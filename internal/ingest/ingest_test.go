package ingest

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
	"github.com/Sankartk/dlq-triage/internal/fingerprint"
	"github.com/Sankartk/dlq-triage/internal/queue"
	"github.com/Sankartk/dlq-triage/internal/store"
)

const url = "mem://dlq"

var src = Source{Name: "orders", DLQURL: url}

func setup(t *testing.T, cfg Config) (*Ingester, *queue.Memory, *store.SQLite) {
	t.Helper()
	st, err := store.OpenSQLite(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	mem := queue.NewMemory()
	cfg.Fingerprint = fingerprint.Config{ErrorAttributes: []string{"ErrorMessage"}}
	return New(st, mem, cfg, slog.New(slog.NewTextHandler(io.Discard, nil))), mem, st
}

func failure(errText, body string) domain.Message {
	return domain.Message{Body: body, ReceiveCount: 3,
		Attributes: map[string]domain.Attribute{"ErrorMessage": domain.StringAttr(errText)}}
}

func TestScanGroupsByFailure(t *testing.T) {
	ing, mem, st := setup(t, Config{Visibility: time.Minute})
	for i := 1; i <= 5; i++ {
		mem.Seed(url, failure(fmt.Sprintf("Loan %d not found", i), fmt.Sprintf(`{"loan":"L%d"}`, i)))
	}
	for i := 1; i <= 2; i++ {
		mem.Seed(url, failure(fmt.Sprintf("Timeout after %d seconds", 30*i), `{"loan":"x"}`))
	}

	res, err := ing.Scan(context.Background(), src)
	if err != nil {
		t.Fatal(err)
	}
	if res.Seen != 7 || res.New != 7 {
		t.Fatalf("result = %+v", res)
	}
	groups, _ := st.ListGroups(context.Background(), "orders")
	if len(groups) != 2 || groups[0].PendingCount != 5 || groups[1].PendingCount != 2 {
		t.Fatalf("groups = %+v", groups)
	}
	if groups[0].ErrorSig != "Loan <n> not found" || groups[1].ErrorSig != "Timeout after <n> seconds" {
		t.Fatalf("signatures = %q / %q", groups[0].ErrorSig, groups[1].ErrorSig)
	}
}

func TestScanIsIdempotentAndNeverDeletes(t *testing.T) {
	ing, mem, st := setup(t, Config{Visibility: time.Second})
	var now atomic.Int64
	base := time.Date(2026, 10, 3, 12, 0, 0, 0, time.UTC)
	now.Store(base.UnixNano())
	mem.SetClock(func() time.Time { return time.Unix(0, now.Load()).UTC() })

	mem.Seed(url, failure("boom 1", `{"a":1}`), failure("boom 2", `{"a":2}`))
	first, err := ing.Scan(context.Background(), src)
	if err != nil || first.New != 2 {
		t.Fatalf("first = %+v err=%v", first, err)
	}
	now.Add(int64(2 * time.Second)) // visibility expired
	second, err := ing.Scan(context.Background(), src)
	if err != nil || second.Seen != 2 || second.New != 0 {
		t.Fatalf("second = %+v err=%v", second, err)
	}
	if n := len(mem.Messages(url)); n != 2 {
		t.Fatalf("scan removed messages: %d left", n)
	}
	g, _ := st.ListGroups(context.Background(), "orders")
	if len(g) != 1 || g[0].Count != 2 {
		t.Fatalf("groups = %+v", g)
	}
	// Receive count is refreshed from the queue.
	msgs, _ := st.ListMessages(context.Background(), "orders", g[0].Key, false, 10, 0)
	if msgs[0].ReceiveCount < 4 {
		t.Fatalf("receive count not refreshed: %d", msgs[0].ReceiveCount)
	}
}

func TestScanStopsAtMaxPerScan(t *testing.T) {
	ing, mem, st := setup(t, Config{Visibility: time.Minute, MaxPerScan: 25})
	for i := 0; i < 60; i++ {
		mem.Seed(url, failure("boom", fmt.Sprintf(`{"n":%d}`, i)))
	}
	res, err := ing.Scan(context.Background(), src)
	if err != nil || res.Seen != 25 {
		t.Fatalf("result = %+v err=%v", res, err)
	}
	g, _ := st.ListGroups(context.Background(), "orders")
	if g[0].Count != 25 {
		t.Fatalf("stored %d, want 25", g[0].Count)
	}
}

func TestScanKeepsRepliedStatus(t *testing.T) {
	ing, mem, st := setup(t, Config{Visibility: time.Millisecond})
	ids := mem.Seed(url, failure("boom", `{"a":1}`))
	ing.Scan(context.Background(), src)
	st.MarkReplayed(context.Background(), "orders", ids[0], time.Now())
	time.Sleep(5 * time.Millisecond)
	ing.Scan(context.Background(), src)
	if s, _ := st.MessageStatus(context.Background(), "orders", ids[0]); s != domain.StatusReplayed {
		t.Fatalf("status = %q after re-ingest", s)
	}
}

func TestScanSkipsBusyQueue(t *testing.T) {
	ing, mem, st := setup(t, Config{Visibility: time.Minute})
	mem.Seed(url, failure("boom", `{}`))
	ing.Busy = func(name string) bool { return name == "orders" }
	res, err := ing.Scan(context.Background(), src)
	if err != nil || !res.Skipped || res.Seen != 0 {
		t.Fatalf("result = %+v err=%v", res, err)
	}
	if g, _ := st.ListGroups(context.Background(), "orders"); len(g) != 0 {
		t.Fatalf("busy queue was scanned: %+v", g)
	}
	if d, _ := mem.Depth(context.Background(), url); d.Visible != 1 {
		t.Fatalf("busy queue was touched: %+v", d)
	}
}

type failingQueue struct{ queue.Queue }

func (failingQueue) Receive(context.Context, string, int, time.Duration) ([]domain.Message, error) {
	return nil, errors.New("queue unavailable")
}

func TestScanReportsQueueErrors(t *testing.T) {
	st, _ := store.OpenSQLite(":memory:")
	defer st.Close()
	ing := New(st, failingQueue{queue.NewMemory()}, Config{}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	var got error
	ing.OnScan = func(_ string, _ Result, err error) { got = err }
	if _, err := ing.Scan(context.Background(), src); err == nil {
		t.Fatal("expected error")
	}
	if got == nil {
		t.Fatal("OnScan did not receive the error")
	}
}

func TestRunScansPeriodicallyUntilCancelled(t *testing.T) {
	ing, mem, st := setup(t, Config{Visibility: time.Minute, Interval: 20 * time.Millisecond})
	mem.Seed(url, failure("boom", `{}`))
	var scans atomic.Int32
	ing.OnScan = func(string, Result, error) { scans.Add(1) }

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { ing.Run(ctx, []Source{src}); close(done) }()
	deadline := time.Now().Add(2 * time.Second)
	for scans.Load() < 3 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not stop after cancel")
	}
	if scans.Load() < 3 {
		t.Fatalf("only %d scans ran", scans.Load())
	}
	if g, _ := st.ListGroups(context.Background(), "orders"); len(g) != 1 {
		t.Fatalf("groups = %+v", g)
	}
}
