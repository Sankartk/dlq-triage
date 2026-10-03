package replay

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Sankartk/dlq-triage/internal/domain"
	"github.com/Sankartk/dlq-triage/internal/queue"
	"github.com/Sankartk/dlq-triage/internal/store"
)

const (
	dlqURL  = "mem://orders-dlq"
	destURL = "mem://orders"
)

var testQueue = Queue{Name: "orders", DLQURL: dlqURL, DestURL: destURL}

type fixture struct {
	t      *testing.T
	st     *store.SQLite
	mem    *queue.Memory
	eng    *Engine
	clock  *fakeClock
	nextID atomic.Int64
}

type fakeClock struct{ t atomic.Int64 }

func (c *fakeClock) Now() time.Time          { return time.Unix(0, c.t.Load()).UTC() }
func (c *fakeClock) Advance(d time.Duration) { c.t.Add(int64(d)) }

func newFixture(t *testing.T, cfg Config) *fixture {
	t.Helper()
	st, err := store.OpenSQLite(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })

	f := &fixture{t: t, st: st, mem: queue.NewMemory(), clock: &fakeClock{}}
	f.clock.t.Store(time.Date(2026, 10, 3, 12, 0, 0, 0, time.UTC).UnixNano())
	// Real time for queue visibility so engine polling behaves normally.
	if cfg.Visibility == 0 {
		cfg.Visibility = 50 * time.Millisecond
	}
	if cfg.PollInterval == 0 {
		cfg.PollInterval = 5 * time.Millisecond
	}
	if cfg.Patience == 0 {
		cfg.Patience = 300 * time.Millisecond
	}
	cfg.TagAttributes = true
	f.eng = New(context.Background(), st, f.mem, cfg, slog.New(slog.NewTextHandler(io.Discard, nil)),
		func() string { return fmt.Sprintf("job-%d", f.nextID.Add(1)) })
	t.Cleanup(f.eng.Shutdown)
	return f
}

// seed puts n messages in the DLQ and ingests them into the store under group.
func (f *fixture) seed(group string, n int) []string {
	f.t.Helper()
	var ids []string
	for i := 0; i < n; i++ {
		m := domain.Message{Body: fmt.Sprintf(`{"n":%d}`, i), Attributes: map[string]string{"ErrorMessage": "boom"}, ReceiveCount: 5}
		id := f.mem.Seed(dlqURL, m)[0]
		_, err := f.st.UpsertMessage(context.Background(), domain.StoredMessage{
			Queue: "orders", ID: id, GroupKey: group, Body: m.Body, Attributes: m.Attributes,
			ReceiveCount: 5, IngestedAt: f.clock.Now().Add(time.Duration(i) * time.Millisecond),
		}, "boom", "shape")
		if err != nil {
			f.t.Fatal(err)
		}
		ids = append(ids, id)
	}
	return ids
}

func (f *fixture) request(group string) Request {
	return Request{Queue: testQueue, GroupKey: group, Actor: "alice", RatePerSec: 100, MaxMessages: 100}
}

func (f *fixture) job(id string) domain.ReplayJob {
	f.t.Helper()
	j, err := f.st.GetJob(context.Background(), id)
	if err != nil {
		f.t.Fatal(err)
	}
	return j
}

func (f *fixture) audit() []string {
	f.t.Helper()
	entries, err := f.st.ListAudit(context.Background(), "orders", 100)
	if err != nil {
		f.t.Fatal(err)
	}
	var actions []string
	for i := len(entries) - 1; i >= 0; i-- {
		actions = append(actions, entries[i].Action)
	}
	return actions
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestReplayMovesGroupAndLeavesOthers(t *testing.T) {
	f := newFixture(t, Config{})
	a := f.seed("A", 5)
	b := f.seed("B", 3)

	job, err := f.eng.Start(context.Background(), f.request("A"))
	if err != nil {
		t.Fatal(err)
	}
	f.eng.Wait()

	got := f.job(job.ID)
	if got.Status != domain.JobCompleted || got.Total != 5 || got.Replayed != 5 || got.Failed != 0 || got.Skipped != 0 {
		t.Fatalf("job = %+v", got)
	}
	if got.FinishedAt == nil {
		t.Fatal("finished_at not set")
	}
	if n := len(f.mem.Messages(destURL)); n != 5 {
		t.Fatalf("destination has %d messages, want 5", n)
	}
	remaining := f.mem.Messages(dlqURL)
	if len(remaining) != 3 {
		t.Fatalf("DLQ has %d messages, want 3", len(remaining))
	}
	for _, m := range remaining {
		if !contains(b, m.ID) {
			t.Errorf("unexpected message left in DLQ: %s", m.ID)
		}
	}
	for _, id := range a {
		if st, _ := f.st.MessageStatus(context.Background(), "orders", id); st != domain.StatusReplayed {
			t.Errorf("message %s status = %q", id, st)
		}
	}
	for _, id := range b {
		if st, _ := f.st.MessageStatus(context.Background(), "orders", id); st != domain.StatusPending {
			t.Errorf("group B message %s status = %q, want pending", id, st)
		}
	}
	if want := []string{"replay.start", "replay.finish"}; !equalStrings(f.audit(), want) {
		t.Fatalf("audit = %v, want %v", f.audit(), want)
	}
}

func TestReplayTagsAttributesAndKeepsBody(t *testing.T) {
	f := newFixture(t, Config{})
	ids := f.seed("A", 1)
	job, err := f.eng.Start(context.Background(), f.request("A"))
	if err != nil {
		t.Fatal(err)
	}
	f.eng.Wait()

	out := f.mem.Messages(destURL)
	if len(out) != 1 {
		t.Fatalf("destination = %+v", out)
	}
	if out[0].Body != `{"n":0}` || out[0].Attributes["ErrorMessage"] != "boom" {
		t.Fatalf("body or attributes changed: %+v", out[0])
	}
	if out[0].Attributes["x-dlq-triage-original-id"] != ids[0] || out[0].Attributes["x-dlq-triage-job"] != job.ID {
		t.Fatalf("tracing attributes missing: %+v", out[0].Attributes)
	}
}

func TestDryRunChangesNothing(t *testing.T) {
	f := newFixture(t, Config{})
	f.seed("A", 4)
	req := f.request("A")
	req.DryRun = true

	job, err := f.eng.Start(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if job.Status != domain.JobCompleted || job.Total != 4 || !job.DryRun || job.Replayed != 0 {
		t.Fatalf("dry run job = %+v", job)
	}
	if n := len(f.mem.Messages(destURL)); n != 0 {
		t.Fatalf("dry run sent %d messages", n)
	}
	if n := len(f.mem.Messages(dlqURL)); n != 4 {
		t.Fatalf("dry run changed the DLQ: %d messages", n)
	}
	g, _ := f.st.GetGroup(context.Background(), "orders", "A")
	if g.PendingCount != 4 {
		t.Fatalf("pending = %d, want 4", g.PendingCount)
	}
	if want := []string{"replay.dryrun"}; !equalStrings(f.audit(), want) {
		t.Fatalf("audit = %v", f.audit())
	}
}

func TestMaxMessagesLimitsTheJob(t *testing.T) {
	f := newFixture(t, Config{})
	f.seed("A", 6)
	req := f.request("A")
	req.MaxMessages = 2

	job, err := f.eng.Start(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	f.eng.Wait()
	got := f.job(job.ID)
	if got.Total != 2 || got.Replayed != 2 {
		t.Fatalf("job = %+v", got)
	}
	g, _ := f.st.GetGroup(context.Background(), "orders", "A")
	if g.PendingCount != 4 {
		t.Fatalf("pending = %d, want 4", g.PendingCount)
	}
}

func TestSendFailureLeavesMessageInDLQAndFailsJob(t *testing.T) {
	f := newFixture(t, Config{})
	ids := f.seed("A", 4)
	poison := ids[1]
	f.mem.FailSend = func(_ string, m domain.Message) error {
		if m.Attributes["x-dlq-triage-original-id"] == poison {
			return errors.New("destination unavailable")
		}
		return nil
	}

	job, _ := f.eng.Start(context.Background(), f.request("A"))
	f.eng.Wait()

	got := f.job(job.ID)
	if got.Status != domain.JobFailed || got.Replayed != 3 || got.Failed != 1 {
		t.Fatalf("job = %+v", got)
	}
	if st, _ := f.st.MessageStatus(context.Background(), "orders", poison); st != domain.StatusPending {
		t.Fatalf("failed message status = %q, want pending", st)
	}
	left := f.mem.Messages(dlqURL)
	if len(left) != 1 || left[0].ID != poison {
		t.Fatalf("DLQ should hold only the failed message, has %+v", left)
	}
}

func TestReplayedMessagesAreNotSelectedAgain(t *testing.T) {
	f := newFixture(t, Config{})
	ids := f.seed("A", 3)
	// ids[0] was replayed by an earlier run, so it is no longer pending.
	if err := f.st.MarkReplayed(context.Background(), "orders", ids[0], f.clock.Now()); err != nil {
		t.Fatal(err)
	}
	job, err := f.eng.Start(context.Background(), f.request("A"))
	if err != nil {
		t.Fatal(err)
	}
	f.eng.Wait()

	got := f.job(job.ID)
	if got.Total != 2 || got.Replayed != 2 {
		t.Fatalf("job = %+v", got)
	}
	// The job only touches the two pending messages; ids[0] stays untouched.
	left := f.mem.Messages(dlqURL)
	if len(left) != 1 || left[0].ID != ids[0] {
		t.Fatalf("DLQ = %+v", left)
	}
	if n := len(f.mem.Messages(destURL)); n != 2 {
		t.Fatalf("destination has %d, want 2", n)
	}
}

func TestCrashBetweenSendAndDeleteDoesNotDuplicate(t *testing.T) {
	f := newFixture(t, Config{})
	ids := f.seed("A", 2)
	ctx := context.Background()

	// An earlier run sent message 0 and recorded it, but stopped before deleting it.
	f.mem.Send(ctx, destURL, domain.Message{Body: `{"n":0}`}, ids[0])
	f.st.MarkReplayed(ctx, "orders", ids[0], f.clock.Now())

	msgs, _ := f.mem.Receive(ctx, dlqURL, 10, time.Minute)
	var target domain.Message
	for _, m := range msgs {
		if m.ID == ids[0] {
			target = m
		}
	}
	// replayOne is called directly because a recorded message is never selected by Start.
	job := domain.ReplayJob{ID: "manual", Queue: "orders", GroupKey: "A"}
	out := f.eng.replayOne(ctx, f.request("A"), job, target)
	if out != outcomeSkipped {
		t.Fatalf("outcome = %v, want skipped", out)
	}
	if n := len(f.mem.Messages(destURL)); n != 1 {
		t.Fatalf("destination has %d messages, want exactly 1", n)
	}
	for _, m := range f.mem.Messages(dlqURL) {
		if m.ID == ids[0] {
			t.Fatal("replayed message was not removed from the DLQ")
		}
	}
}

func TestDestinationDedupPreventsDoubleSend(t *testing.T) {
	f := newFixture(t, Config{})
	ids := f.seed("A", 1)
	ctx := context.Background()
	// Same original id sent twice (crash after send, before the store was updated).
	f.mem.Send(ctx, destURL, domain.Message{Body: "x"}, ids[0])
	job, _ := f.eng.Start(ctx, f.request("A"))
	f.eng.Wait()
	if got := f.job(job.ID); got.Replayed != 1 {
		t.Fatalf("job = %+v", got)
	}
	if n := len(f.mem.Messages(destURL)); n != 1 {
		t.Fatalf("destination has %d messages, want 1 (dedup id honoured)", n)
	}
}

func TestOverlappingRealRunsAreRejected(t *testing.T) {
	f := newFixture(t, Config{})
	f.seed("A", 3)
	req := f.request("A")
	req.RatePerSec = 1 // slow enough that the first run is still going

	first, err := f.eng.Start(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.eng.Start(context.Background(), req); !errors.Is(err, ErrBusy) {
		t.Fatalf("second start err = %v, want ErrBusy", err)
	}
	// A different group on the same queue is independent.
	f.seed("B", 1)
	if _, err := f.eng.Start(context.Background(), f.request("B")); err != nil {
		t.Fatalf("other group blocked: %v", err)
	}
	if !f.eng.Cancel(first.ID) {
		t.Fatal("expected to cancel a running job")
	}
	f.eng.Wait()
}

func TestCancelStopsJobAndRecordsIt(t *testing.T) {
	f := newFixture(t, Config{})
	f.seed("A", 5)
	req := f.request("A")
	req.RatePerSec = 1

	job, _ := f.eng.Start(context.Background(), req)
	deadline := time.Now().Add(2 * time.Second)
	for len(f.mem.Messages(destURL)) == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	f.eng.Cancel(job.ID)
	f.eng.Wait()

	got := f.job(job.ID)
	if got.Status != domain.JobCancelled || got.FinishedAt == nil {
		t.Fatalf("job = %+v", got)
	}
	if got.Replayed == 0 || got.Replayed >= 5 {
		t.Fatalf("replayed = %d, expected some but not all", got.Replayed)
	}
	// Whatever was replayed is consistent between store, destination and DLQ.
	if len(f.mem.Messages(destURL)) != got.Replayed {
		t.Fatalf("destination=%d replayed=%d", len(f.mem.Messages(destURL)), got.Replayed)
	}
	if len(f.mem.Messages(dlqURL)) != 5-got.Replayed {
		t.Fatalf("DLQ=%d, want %d", len(f.mem.Messages(dlqURL)), 5-got.Replayed)
	}
	// A cancelled group can be run again.
	if _, err := f.eng.Start(context.Background(), f.request("A")); err != nil {
		t.Fatalf("restart after cancel: %v", err)
	}
	f.eng.Wait()
	g, _ := f.st.GetGroup(context.Background(), "orders", "A")
	if g.PendingCount != 0 {
		t.Fatalf("pending after rerun = %d", g.PendingCount)
	}
	if n := len(f.mem.Messages(destURL)); n != 5 {
		t.Fatalf("destination = %d, want 5 with no duplicates", n)
	}
}

func TestRateLimitIsHonoured(t *testing.T) {
	f := newFixture(t, Config{})
	f.seed("A", 4)
	req := f.request("A")
	req.RatePerSec = 20 // 4 messages need at least ~150ms after the first

	start := time.Now()
	f.eng.Start(context.Background(), req)
	f.eng.Wait()
	if elapsed := time.Since(start); elapsed < 120*time.Millisecond {
		t.Fatalf("replay finished in %v, rate limit not applied", elapsed)
	}
}

func TestMissingMessagesFailTheJob(t *testing.T) {
	f := newFixture(t, Config{})
	ids := f.seed("A", 3)
	// Someone else removed a message from the DLQ after it was ingested.
	msgs, _ := f.mem.Receive(context.Background(), dlqURL, 1, time.Hour)
	f.mem.Delete(context.Background(), dlqURL, msgs[0])
	_ = ids

	job, _ := f.eng.Start(context.Background(), f.request("A"))
	f.eng.Wait()
	got := f.job(job.ID)
	if got.Status != domain.JobFailed || got.Replayed != 2 || got.Failed != 1 || got.Error == "" {
		t.Fatalf("job = %+v", got)
	}
}

func TestValidation(t *testing.T) {
	f := newFixture(t, Config{MaxRatePerSec: 50, MaxMessagesLimit: 10})
	f.seed("A", 1)
	ctx := context.Background()
	base := f.request("A")
	base.RatePerSec, base.MaxMessages = 5, 5

	cases := map[string]func(*Request){
		"no actor":      func(r *Request) { r.Actor = "" },
		"no group":      func(r *Request) { r.GroupKey = "" },
		"no queue":      func(r *Request) { r.Queue = Queue{} },
		"no dest":       func(r *Request) { r.Queue.DestURL = "" },
		"zero rate":     func(r *Request) { r.RatePerSec = 0 },
		"rate too high": func(r *Request) { r.RatePerSec = 51 },
		"zero max":      func(r *Request) { r.MaxMessages = 0 },
		"max too high":  func(r *Request) { r.MaxMessages = 11 },
	}
	for name, mutate := range cases {
		req := base
		mutate(&req)
		if _, err := f.eng.Start(ctx, req); !errors.Is(err, ErrInvalid) {
			t.Errorf("%s: err = %v, want ErrInvalid", name, err)
		}
	}
	if _, err := f.eng.Start(ctx, Request{Queue: testQueue, GroupKey: "nope", Actor: "a", RatePerSec: 1, MaxMessages: 1}); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("unknown group err = %v", err)
	}
}

func TestNothingToReplay(t *testing.T) {
	f := newFixture(t, Config{})
	ids := f.seed("A", 1)
	f.st.MarkReplayed(context.Background(), "orders", ids[0], f.clock.Now())
	if _, err := f.eng.Start(context.Background(), f.request("A")); !errors.Is(err, ErrNothingToReplay) {
		t.Fatalf("err = %v, want ErrNothingToReplay", err)
	}
}

func TestShutdownRecordsRunningJob(t *testing.T) {
	f := newFixture(t, Config{})
	f.seed("A", 5)
	req := f.request("A")
	req.RatePerSec = 1
	job, _ := f.eng.Start(context.Background(), req)
	f.eng.Shutdown()
	got := f.job(job.ID)
	if got.Status != domain.JobCancelled || got.Error != "service shutting down" {
		t.Fatalf("job after shutdown = %+v", got)
	}
}

func contains(list []string, s string) bool {
	for _, x := range list {
		if x == s {
			return true
		}
	}
	return false
}
