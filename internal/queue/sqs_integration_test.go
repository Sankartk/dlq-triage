//go:build integration

package queue

import (
	"context"
	"encoding/base64"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"

	"github.com/Sankartk/dlq-triage/internal/domain"
)

// Run with an SQS emulator, for example:
//
//	moto_server -p 5055
//	$env:DLQ_TRIAGE_SQS_ENDPOINT = "http://127.0.0.1:5055"
//	go test -tags integration ./internal/queue/
func endpoint(t *testing.T) string {
	t.Helper()
	ep := os.Getenv("DLQ_TRIAGE_SQS_ENDPOINT")
	if ep == "" {
		t.Skip("DLQ_TRIAGE_SQS_ENDPOINT not set")
	}
	return ep
}

func newSQS(t *testing.T) (*SQS, *sqs.Client) {
	t.Helper()
	ep := endpoint(t)
	t.Setenv("AWS_ACCESS_KEY_ID", "test")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test")
	ctx := context.Background()
	q, err := NewSQS(ctx, SQSOptions{Region: "us-east-1", Endpoint: ep, WaitTime: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion("us-east-1"))
	if err != nil {
		t.Fatal(err)
	}
	admin := sqs.NewFromConfig(cfg, func(o *sqs.Options) { o.BaseEndpoint = aws.String(ep) })
	return q, admin
}

func createQueue(t *testing.T, admin *sqs.Client, name string, attrs map[string]string) string {
	t.Helper()
	out, err := admin.CreateQueue(context.Background(), &sqs.CreateQueueInput{QueueName: aws.String(name), Attributes: attrs})
	if err != nil {
		t.Fatal(err)
	}
	return aws.ToString(out.QueueUrl)
}

func TestSQSRoundTripPreservesAttributeTypes(t *testing.T) {
	q, admin := newSQS(t)
	ctx := context.Background()
	suffix := time.Now().UnixNano()
	src := createQueue(t, admin, fmt.Sprintf("src-%d", suffix), nil)
	dst := createQueue(t, admin, fmt.Sprintf("dst-%d", suffix), nil)

	binary := []byte{0x00, 0xff, 0x10, 0x80}
	msg := domain.Message{
		Body: `{"loan":"L1"}`,
		Attributes: map[string]domain.Attribute{
			"ErrorMessage": domain.StringAttr("Loan 1 not found"),
			"Amount":       {DataType: "Number", Value: "12345.67"},
			"Blob":         {DataType: "Binary", Value: base64.StdEncoding.EncodeToString(binary)},
			"Custom":       {DataType: "String.loan-id", Value: "L1"},
		},
	}
	if _, err := q.Send(ctx, src, msg, ""); err != nil {
		t.Fatal(err)
	}

	got, err := q.Receive(ctx, src, 10, 30*time.Second)
	if err != nil || len(got) != 1 {
		t.Fatalf("receive: %v %+v", err, got)
	}
	m := got[0]
	if m.Body != msg.Body || m.ID == "" || m.Handle == "" {
		t.Fatalf("message = %+v", m)
	}
	if m.ReceiveCount != 1 {
		t.Errorf("receive count = %d, want 1", m.ReceiveCount)
	}
	if m.SentAt.IsZero() || time.Since(m.SentAt) > time.Hour {
		t.Errorf("sent at = %v", m.SentAt)
	}
	for name, want := range msg.Attributes {
		if got := m.Attributes[name]; got != want {
			t.Errorf("attribute %s = %+v, want %+v", name, got, want)
		}
	}

	// Replay to another queue: data types must survive the second hop too.
	if _, err := q.Send(ctx, dst, m, m.ID); err != nil {
		t.Fatal(err)
	}
	if err := q.Delete(ctx, src, m); err != nil {
		t.Fatal(err)
	}
	moved, err := q.Receive(ctx, dst, 10, 30*time.Second)
	if err != nil || len(moved) != 1 {
		t.Fatalf("receive from destination: %v %+v", err, moved)
	}
	for name, want := range msg.Attributes {
		if got := moved[0].Attributes[name]; got != want {
			t.Errorf("after replay attribute %s = %+v, want %+v", name, got, want)
		}
	}

	d, err := q.Depth(ctx, src)
	if err != nil || d.Visible != 0 || d.InFlight != 0 {
		t.Fatalf("source depth after delete = %+v err=%v", d, err)
	}
}

func TestSQSVisibilityHidesMessagesUntilTimeout(t *testing.T) {
	q, admin := newSQS(t)
	ctx := context.Background()
	url := createQueue(t, admin, fmt.Sprintf("vis-%d", time.Now().UnixNano()), nil)
	for i := 0; i < 3; i++ {
		if _, err := q.Send(ctx, url, domain.Message{Body: fmt.Sprintf("m%d", i)}, ""); err != nil {
			t.Fatal(err)
		}
	}
	first, err := q.Receive(ctx, url, 10, 2*time.Second)
	if err != nil || len(first) != 3 {
		t.Fatalf("first receive = %d err=%v", len(first), err)
	}
	d, _ := q.Depth(ctx, url)
	if d.InFlight != 3 || d.Visible != 0 {
		t.Fatalf("depth while hidden = %+v", d)
	}
	hidden, _ := q.Receive(ctx, url, 10, 2*time.Second)
	if len(hidden) != 0 {
		t.Fatalf("hidden messages were received: %d", len(hidden))
	}
	time.Sleep(2500 * time.Millisecond)
	again, err := q.Receive(ctx, url, 10, 30*time.Second)
	if err != nil || len(again) != 3 {
		t.Fatalf("after timeout got %d err=%v", len(again), err)
	}
	if again[0].ReceiveCount != 2 {
		t.Errorf("receive count = %d, want 2", again[0].ReceiveCount)
	}
}

func TestSQSFifoSendUsesGroupAndDedup(t *testing.T) {
	q, admin := newSQS(t)
	ctx := context.Background()
	url := createQueue(t, admin, fmt.Sprintf("f-%d.fifo", time.Now().UnixNano()), map[string]string{"FifoQueue": "true"})

	msg := domain.Message{Body: "payload", GroupID: "loan-7"}
	if _, err := q.Send(ctx, url, msg, "orig-1"); err != nil {
		t.Fatal(err)
	}
	// Same dedup id inside the dedup window: accepted but not delivered twice.
	if _, err := q.Send(ctx, url, msg, "orig-1"); err != nil {
		t.Fatal(err)
	}
	got, err := q.Receive(ctx, url, 10, 30*time.Second)
	if err != nil || len(got) != 1 {
		t.Fatalf("got %d messages err=%v, want exactly 1 (dedup id honoured)", len(got), err)
	}
	if got[0].GroupID != "loan-7" {
		t.Errorf("group id = %q", got[0].GroupID)
	}

	// A message without a group id still goes through on FIFO queues.
	if _, err := q.Send(ctx, url, domain.Message{Body: "no group"}, "orig-2"); err != nil {
		t.Fatalf("send without group: %v", err)
	}
}

func TestSQSErrorsAreReported(t *testing.T) {
	q, _ := newSQS(t)
	ctx := context.Background()
	if _, err := q.Receive(ctx, "http://127.0.0.1:1/000000000000/does-not-exist", 1, time.Second); err == nil {
		t.Error("expected an error for an unreachable queue")
	}
	if _, err := q.Receive(ctx, "x", 0, time.Second); err == nil {
		t.Error("expected an error for max=0")
	}
	if _, err := q.Receive(ctx, "x", 11, time.Second); err == nil {
		t.Error("expected an error for max=11")
	}
}
