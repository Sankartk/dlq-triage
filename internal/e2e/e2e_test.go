//go:build integration

// Package e2e runs the ingest and replay flow against an SQS emulator.
//
//	moto_server -p 5055
//	$env:DLQ_TRIAGE_SQS_ENDPOINT = "http://127.0.0.1:5055"
//	go test -tags integration ./internal/e2e/
package e2e

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"github.com/Sankartk/dlq-triage/internal/domain"
	"github.com/Sankartk/dlq-triage/internal/fingerprint"
	"github.com/Sankartk/dlq-triage/internal/ingest"
	"github.com/Sankartk/dlq-triage/internal/queue"
	"github.com/Sankartk/dlq-triage/internal/replay"
	"github.com/Sankartk/dlq-triage/internal/store"
)

func TestIngestThenReplayOneGroupAgainstSQS(t *testing.T) {
	ep := os.Getenv("DLQ_TRIAGE_SQS_ENDPOINT")
	if ep == "" {
		t.Skip("DLQ_TRIAGE_SQS_ENDPOINT not set")
	}
	t.Setenv("AWS_ACCESS_KEY_ID", "test")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test")
	ctx := context.Background()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))

	q, err := queue.NewSQS(ctx, queue.SQSOptions{Region: "us-east-1", Endpoint: ep, WaitTime: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	cfg, _ := config.LoadDefaultConfig(ctx, config.WithRegion("us-east-1"))
	admin := sqs.NewFromConfig(cfg, func(o *sqs.Options) { o.BaseEndpoint = aws.String(ep) })
	mk := func(name string) string {
		out, err := admin.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String(name)})
		if err != nil {
			t.Fatal(err)
		}
		return aws.ToString(out.QueueUrl)
	}
	suffix := time.Now().UnixNano()
	dlq := mk(fmt.Sprintf("orders-dlq-%d", suffix))
	dest := mk(fmt.Sprintf("orders-%d", suffix))

	send := func(body, errText string, amount string) {
		_, err := admin.SendMessage(ctx, &sqs.SendMessageInput{
			QueueUrl: aws.String(dlq), MessageBody: aws.String(body),
			MessageAttributes: map[string]types.MessageAttributeValue{
				"ErrorMessage": {DataType: aws.String("String"), StringValue: aws.String(errText)},
				"Amount":       {DataType: aws.String("Number"), StringValue: aws.String(amount)},
			},
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	for i := 1; i <= 6; i++ {
		send(fmt.Sprintf(`{"loan":"L%d","amount":%d}`, i, i*100), fmt.Sprintf("Loan %d not found", i), fmt.Sprint(i*100))
	}
	for i := 1; i <= 3; i++ {
		send(fmt.Sprintf(`{"loan":"T%d"}`, i), fmt.Sprintf("Timeout after %d seconds", 30*i), "0")
	}

	st, err := store.OpenSQLite(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	ing := ingest.New(st, q, ingest.Config{
		Fingerprint: fingerprint.Config{ErrorAttributes: []string{"ErrorMessage"}},
		Visibility:  2 * time.Second,
	}, log)
	res, err := ing.Scan(ctx, ingest.Source{Name: "orders", DLQURL: dlq})
	if err != nil || res.Seen != 9 || res.New != 9 {
		t.Fatalf("scan = %+v err=%v", res, err)
	}
	groups, _ := st.ListGroups(ctx, "orders")
	if len(groups) != 2 || groups[0].PendingCount != 6 || groups[1].PendingCount != 3 {
		t.Fatalf("groups = %+v", groups)
	}
	target := groups[0] // "Loan <n> not found"

	n := 0
	eng := replay.New(ctx, st, q, replay.Config{
		Visibility: 2 * time.Second, Patience: 10 * time.Second, PollInterval: 200 * time.Millisecond, TagAttributes: true,
	}, log, func() string { n++; return fmt.Sprintf("job-%d", n) })
	defer eng.Shutdown()

	rq := replay.Queue{Name: "orders", DLQURL: dlq, DestURL: dest}
	dry, err := eng.Start(ctx, replay.Request{Queue: rq, GroupKey: target.Key, Actor: "alice", DryRun: true, RatePerSec: 50, MaxMessages: 100})
	if err != nil || dry.Total != 6 || dry.Status != domain.JobCompleted {
		t.Fatalf("dry run = %+v err=%v", dry, err)
	}
	if d, _ := q.Depth(ctx, dest); d.Visible != 0 {
		t.Fatalf("dry run sent messages: %+v", d)
	}

	// Scanned messages are hidden for the visibility timeout; the engine waits for them.
	job, err := eng.Start(ctx, replay.Request{Queue: rq, GroupKey: target.Key, Actor: "alice", RatePerSec: 50, MaxMessages: 100})
	if err != nil {
		t.Fatal(err)
	}
	eng.Wait()

	got, _ := st.GetJob(ctx, job.ID)
	if got.Status != domain.JobCompleted || got.Replayed != 6 || got.Failed != 0 {
		t.Fatalf("job = %+v", got)
	}

	moved, err := drain(ctx, q, dest)
	if err != nil || len(moved) != 6 {
		t.Fatalf("destination has %d messages err=%v, want 6", len(moved), err)
	}
	for _, m := range moved {
		if a := m.Attributes["Amount"]; a.DataType != "Number" {
			t.Fatalf("Number attribute became %q", a.DataType)
		}
		if m.Attributes["x-dlq-triage-job"].Value != job.ID {
			t.Fatalf("missing trace attribute: %+v", m.Attributes)
		}
	}

	// The other group is untouched, still in the dead-letter queue.
	d, _ := q.Depth(ctx, dlq)
	if d.Visible+d.InFlight != 3 {
		t.Fatalf("dead-letter queue holds %+v, want 3 timeout messages", d)
	}
	if g, _ := st.GetGroup(ctx, "orders", target.Key); g.PendingCount != 0 {
		t.Fatalf("pending after replay = %d", g.PendingCount)
	}
	actions := []string{}
	entries, _ := st.ListAudit(ctx, "orders", 10)
	for i := len(entries) - 1; i >= 0; i-- {
		actions = append(actions, entries[i].Action)
	}
	if fmt.Sprint(actions) != "[replay.dryrun replay.start replay.finish]" {
		t.Fatalf("audit = %v", actions)
	}
}

func drain(ctx context.Context, q queue.Queue, url string) ([]domain.Message, error) {
	var all []domain.Message
	for empty := 0; empty < 2; {
		batch, err := q.Receive(ctx, url, 10, 30*time.Second)
		if err != nil {
			return nil, err
		}
		if len(batch) == 0 {
			empty++
			continue
		}
		all = append(all, batch...)
	}
	return all, nil
}
