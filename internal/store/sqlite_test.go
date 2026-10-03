package store

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/Sankartk/dlq-triage/internal/domain"
)

var t0 = time.Date(2026, 10, 3, 12, 0, 0, 0, time.UTC)

func newStore(t *testing.T) *SQLite {
	t.Helper()
	s, err := OpenSQLite(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func msg(id, group string, at time.Time) domain.StoredMessage {
	return domain.StoredMessage{
		Queue: "q", ID: id, GroupKey: group, Body: `{"id":"` + id + `"}`,
		Attributes: map[string]string{"ErrorMessage": "boom"}, ReceiveCount: 5,
		SentAt: at.Add(-time.Hour), IngestedAt: at,
	}
}

func TestUpsertIsIdempotentAndReportsCreation(t *testing.T) {
	ctx := context.Background()
	s := newStore(t)

	created, err := s.UpsertMessage(ctx, msg("m1", "g1", t0), "sig", "shape")
	if err != nil || !created {
		t.Fatalf("first upsert: created=%v err=%v", created, err)
	}
	m2 := msg("m1", "g1", t0.Add(time.Minute))
	m2.ReceiveCount = 9
	created, err = s.UpsertMessage(ctx, m2, "sig", "shape")
	if err != nil || created {
		t.Fatalf("second upsert: created=%v err=%v", created, err)
	}

	g, err := s.GetGroup(ctx, "q", "g1")
	if err != nil {
		t.Fatal(err)
	}
	if g.Count != 1 || g.PendingCount != 1 {
		t.Fatalf("count=%d pending=%d, want 1/1", g.Count, g.PendingCount)
	}
	if !g.FirstSeen.Equal(t0) || !g.LastSeen.Equal(t0.Add(time.Minute)) {
		t.Fatalf("first/last seen = %v / %v", g.FirstSeen, g.LastSeen)
	}
	msgs, _ := s.ListMessages(ctx, "q", "g1", false, 10, 0)
	if len(msgs) != 1 || msgs[0].ReceiveCount != 9 {
		t.Fatalf("receive count not refreshed: %+v", msgs)
	}
	if msgs[0].Attributes["ErrorMessage"] != "boom" || msgs[0].SentAt.IsZero() {
		t.Fatalf("attributes or sent_at lost: %+v", msgs[0])
	}
}

func TestReIngestNeverResetsReplayedStatus(t *testing.T) {
	ctx := context.Background()
	s := newStore(t)
	s.UpsertMessage(ctx, msg("m1", "g1", t0), "sig", "shape")
	if err := s.MarkReplayed(ctx, "q", "m1", t0.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	s.UpsertMessage(ctx, msg("m1", "g1", t0.Add(time.Minute)), "sig", "shape")

	st, err := s.MessageStatus(ctx, "q", "m1")
	if err != nil || st != domain.StatusReplayed {
		t.Fatalf("status = %q err=%v, want replayed", st, err)
	}
	g, _ := s.GetGroup(ctx, "q", "g1")
	if g.PendingCount != 0 || g.Count != 1 {
		t.Fatalf("pending=%d count=%d, want 0/1", g.PendingCount, g.Count)
	}
}

func TestListGroupsOrdersByPending(t *testing.T) {
	ctx := context.Background()
	s := newStore(t)
	s.UpsertMessage(ctx, msg("a1", "small", t0), "s1", "x")
	for i, id := range []string{"b1", "b2", "b3"} {
		s.UpsertMessage(ctx, msg(id, "big", t0.Add(time.Duration(i)*time.Second)), "s2", "x")
	}
	s.UpsertMessage(ctx, domain.StoredMessage{Queue: "other", ID: "z", GroupKey: "g", Body: "{}", IngestedAt: t0}, "s", "x")

	groups, err := s.ListGroups(ctx, "q")
	if err != nil {
		t.Fatal(err)
	}
	if len(groups) != 2 || groups[0].Key != "big" || groups[0].PendingCount != 3 || groups[1].Key != "small" {
		t.Fatalf("unexpected groups: %+v", groups)
	}
	if groups[0].SampleMessage != "b1" {
		t.Fatalf("sample = %q, want b1", groups[0].SampleMessage)
	}
}

func TestPendingMessageIDsAndPagination(t *testing.T) {
	ctx := context.Background()
	s := newStore(t)
	for i, id := range []string{"m1", "m2", "m3", "m4"} {
		s.UpsertMessage(ctx, msg(id, "g", t0.Add(time.Duration(i)*time.Second)), "s", "x")
	}
	s.MarkReplayed(ctx, "q", "m2", t0)

	ids, err := s.PendingMessageIDs(ctx, "q", "g", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != 3 || ids[0] != "m1" || ids[1] != "m3" || ids[2] != "m4" {
		t.Fatalf("pending ids = %v", ids)
	}
	ids, _ = s.PendingMessageIDs(ctx, "q", "g", 2)
	if len(ids) != 2 {
		t.Fatalf("limit not applied: %v", ids)
	}
	page, _ := s.ListMessages(ctx, "q", "g", true, 2, 1)
	if len(page) != 2 || page[0].ID != "m3" {
		t.Fatalf("pagination wrong: %+v", page)
	}
}

func TestNotFoundErrors(t *testing.T) {
	ctx := context.Background()
	s := newStore(t)
	if _, err := s.GetGroup(ctx, "q", "nope"); !errors.Is(err, ErrNotFound) {
		t.Errorf("GetGroup err = %v", err)
	}
	if _, err := s.MessageStatus(ctx, "q", "nope"); !errors.Is(err, ErrNotFound) {
		t.Errorf("MessageStatus err = %v", err)
	}
	if err := s.MarkReplayed(ctx, "q", "nope", t0); !errors.Is(err, ErrNotFound) {
		t.Errorf("MarkReplayed err = %v", err)
	}
	if err := s.SetAISummary(ctx, "q", "nope", "x", t0); !errors.Is(err, ErrNotFound) {
		t.Errorf("SetAISummary err = %v", err)
	}
	if _, err := s.GetJob(ctx, "nope"); !errors.Is(err, ErrNotFound) {
		t.Errorf("GetJob err = %v", err)
	}
	if err := s.UpdateJob(ctx, domain.ReplayJob{ID: "nope"}); !errors.Is(err, ErrNotFound) {
		t.Errorf("UpdateJob err = %v", err)
	}
}

func TestAISummaryRoundTrip(t *testing.T) {
	ctx := context.Background()
	s := newStore(t)
	s.UpsertMessage(ctx, msg("m1", "g", t0), "s", "x")
	if err := s.SetAISummary(ctx, "q", "g", "Missing loan record", t0.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	g, _ := s.GetGroup(ctx, "q", "g")
	if g.AISummary != "Missing loan record" || g.AISummaryAt == nil || !g.AISummaryAt.Equal(t0.Add(time.Hour)) {
		t.Fatalf("summary not stored: %+v", g)
	}
}

func TestJobLifecycle(t *testing.T) {
	ctx := context.Background()
	s := newStore(t)
	j := domain.ReplayJob{
		ID: "j1", Queue: "q", GroupKey: "g", Status: domain.JobRunning, RequestedBy: "alice",
		RatePerSec: 5, MaxMessages: 100, Total: 10, CreatedAt: t0,
	}
	if err := s.CreateJob(ctx, j); err != nil {
		t.Fatal(err)
	}
	if got, err := s.ActiveJob(ctx, "q", "g"); err != nil || got.ID != "j1" {
		t.Fatalf("ActiveJob = %+v err=%v", got, err)
	}

	fin := t0.Add(time.Minute)
	j.Status, j.Replayed, j.Skipped, j.Failed, j.FinishedAt = domain.JobCompleted, 8, 1, 1, &fin
	if err := s.UpdateJob(ctx, j); err != nil {
		t.Fatal(err)
	}
	got, err := s.GetJob(ctx, "j1")
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != domain.JobCompleted || got.Replayed != 8 || got.Skipped != 1 || got.Failed != 1 ||
		got.FinishedAt == nil || got.RequestedBy != "alice" || got.RatePerSec != 5 || got.DryRun {
		t.Fatalf("job round trip: %+v", got)
	}
	if _, err := s.ActiveJob(ctx, "q", "g"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("finished job must not be active, err=%v", err)
	}
}

func TestDryRunJobsAreNeverActive(t *testing.T) {
	ctx := context.Background()
	s := newStore(t)
	s.CreateJob(ctx, domain.ReplayJob{ID: "d1", Queue: "q", GroupKey: "g", DryRun: true, Status: domain.JobRunning, CreatedAt: t0})
	if _, err := s.ActiveJob(ctx, "q", "g"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("dry run must not block real runs, err=%v", err)
	}
}

func TestFailRunningJobs(t *testing.T) {
	ctx := context.Background()
	s := newStore(t)
	s.CreateJob(ctx, domain.ReplayJob{ID: "r", Queue: "q", GroupKey: "g", Status: domain.JobRunning, CreatedAt: t0})
	s.CreateJob(ctx, domain.ReplayJob{ID: "c", Queue: "q", GroupKey: "g", Status: domain.JobCompleted, CreatedAt: t0})

	n, err := s.FailRunningJobs(ctx, "service restarted", t0.Add(time.Hour))
	if err != nil || n != 1 {
		t.Fatalf("n=%d err=%v", n, err)
	}
	r, _ := s.GetJob(ctx, "r")
	c, _ := s.GetJob(ctx, "c")
	if r.Status != domain.JobFailed || r.Error != "service restarted" || r.FinishedAt == nil {
		t.Fatalf("running job not failed: %+v", r)
	}
	if c.Status != domain.JobCompleted {
		t.Fatalf("completed job changed: %+v", c)
	}
}

func TestListJobsNewestFirst(t *testing.T) {
	ctx := context.Background()
	s := newStore(t)
	for i, id := range []string{"j1", "j2", "j3"} {
		s.CreateJob(ctx, domain.ReplayJob{ID: id, Queue: "q", GroupKey: "g", Status: domain.JobCompleted, CreatedAt: t0.Add(time.Duration(i) * time.Minute)})
	}
	jobs, err := s.ListJobs(ctx, "q", 2)
	if err != nil || len(jobs) != 2 || jobs[0].ID != "j3" || jobs[1].ID != "j2" {
		t.Fatalf("jobs = %+v err=%v", jobs, err)
	}
}

func TestAuditAppendOnlyNewestFirst(t *testing.T) {
	ctx := context.Background()
	s := newStore(t)
	for i, a := range []string{"ingest", "replay.start", "replay.finish"} {
		if err := s.AppendAudit(ctx, domain.AuditEntry{At: t0.Add(time.Duration(i) * time.Second), Actor: "alice", Action: a, Queue: "q", GroupKey: "g", JobID: "j1", Detail: "d"}); err != nil {
			t.Fatal(err)
		}
	}
	s.AppendAudit(ctx, domain.AuditEntry{At: t0, Actor: "bob", Action: "x", Queue: "other"})

	got, err := s.ListAudit(ctx, "q", 10)
	if err != nil || len(got) != 3 || got[0].Action != "replay.finish" || got[2].Action != "ingest" {
		t.Fatalf("audit = %+v err=%v", got, err)
	}
	all, _ := s.ListAudit(ctx, "", 10)
	if len(all) != 4 {
		t.Fatalf("all queues: %d entries, want 4", len(all))
	}
}

func TestMigrationsRunOnceAndSurviveReopen(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "dlq.db")

	s, err := OpenSQLite(path)
	if err != nil {
		t.Fatal(err)
	}
	s.UpsertMessage(ctx, msg("m1", "g", t0), "sig", "shape")
	s.Close()

	s, err = OpenSQLite(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer s.Close()
	g, err := s.GetGroup(ctx, "q", "g")
	if err != nil || g.Count != 1 {
		t.Fatalf("data lost on reopen: %+v err=%v", g, err)
	}
	var v int
	if err := s.db.QueryRow(`SELECT COUNT(*) FROM schema_version`).Scan(&v); err != nil || v != len(migrations) {
		t.Fatalf("schema_version rows = %d (err=%v), want %d", v, err, len(migrations))
	}
}
