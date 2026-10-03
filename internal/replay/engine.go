// Package replay moves a group of dead-lettered messages back to a
// destination queue, one controlled job at a time.
//
// Delivery is at-least-once. A message is sent, then marked replayed in the
// store, then deleted from the dead-letter queue. If the process stops between
// send and mark, a later run can send that message again; consumers must be
// idempotent, and the destination queue's deduplication applies on FIFO queues.
package replay

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"golang.org/x/time/rate"

	"github.com/Sankartk/dlq-triage/internal/domain"
	"github.com/Sankartk/dlq-triage/internal/queue"
	"github.com/Sankartk/dlq-triage/internal/store"
)

var (
	// ErrInvalid wraps request validation failures.
	ErrInvalid = errors.New("invalid replay request")
	// ErrBusy is returned when a real replay of the group is already running.
	ErrBusy = errors.New("a replay of this group is already running")
	// ErrNothingToReplay is returned when the group has no pending messages.
	ErrNothingToReplay = errors.New("group has no pending messages")
)

const maxSQSAttributes = 10

// Queue identifies a dead-letter queue and where its messages are replayed to.
type Queue struct {
	Name    string
	DLQURL  string
	DestURL string
}

// Request asks for a replay of one group.
type Request struct {
	Queue       Queue
	GroupKey    string
	Actor       string
	DryRun      bool
	RatePerSec  float64
	MaxMessages int
}

// Config tunes the engine. Zero values get safe defaults.
type Config struct {
	// Visibility hides messages from other receivers while the engine holds them.
	Visibility time.Duration
	// Patience is how long the engine keeps polling for expected messages that
	// are not currently visible (for example, still hidden by an earlier scan).
	Patience time.Duration
	// PollInterval is the wait between empty receives.
	PollInterval time.Duration
	// MaxRatePerSec and MaxMessagesLimit cap what a caller may request.
	MaxRatePerSec    float64
	MaxMessagesLimit int
	// TagAttributes adds x-dlq-triage-* attributes to replayed messages so
	// consumers can trace and de-duplicate them.
	TagAttributes bool
}

func (c Config) withDefaults() Config {
	if c.Visibility <= 0 {
		c.Visibility = 30 * time.Second
	}
	if c.Patience <= 0 {
		c.Patience = 45 * time.Second
	}
	if c.PollInterval <= 0 {
		c.PollInterval = time.Second
	}
	if c.MaxRatePerSec <= 0 {
		c.MaxRatePerSec = 100
	}
	if c.MaxMessagesLimit <= 0 {
		c.MaxMessagesLimit = 5000
	}
	return c
}

// Engine runs replay jobs.
type Engine struct {
	store store.Store
	queue queue.Queue
	cfg   Config
	log   *slog.Logger
	now   func() time.Time
	newID func() string

	baseCtx context.Context
	cancel  context.CancelFunc
	wg      sync.WaitGroup

	mu      sync.Mutex
	running map[string]string             // "queue/group" -> job id
	cancels map[string]context.CancelFunc // job id -> cancel
}

// New builds an engine. Background jobs stop when parent is cancelled or
// Shutdown is called.
func New(parent context.Context, st store.Store, q queue.Queue, cfg Config, log *slog.Logger, newID func() string) *Engine {
	if log == nil {
		log = slog.Default()
	}
	ctx, cancel := context.WithCancel(parent)
	return &Engine{
		store: st, queue: q, cfg: cfg.withDefaults(), log: log,
		now: time.Now, newID: newID,
		baseCtx: ctx, cancel: cancel,
		running: map[string]string{}, cancels: map[string]context.CancelFunc{},
	}
}

// Start validates the request, records the job and, unless it is a dry run,
// begins replaying in the background. It returns the job as created.
func (e *Engine) Start(ctx context.Context, req Request) (domain.ReplayJob, error) {
	if err := e.validate(req); err != nil {
		return domain.ReplayJob{}, err
	}
	g, err := e.store.GetGroup(ctx, req.Queue.Name, req.GroupKey)
	if err != nil {
		return domain.ReplayJob{}, err
	}
	if g.PendingCount == 0 {
		return domain.ReplayJob{}, ErrNothingToReplay
	}
	ids, err := e.store.PendingMessageIDs(ctx, req.Queue.Name, req.GroupKey, req.MaxMessages)
	if err != nil {
		return domain.ReplayJob{}, err
	}

	job := domain.ReplayJob{
		ID: e.newID(), Queue: req.Queue.Name, GroupKey: req.GroupKey, DryRun: req.DryRun,
		Status: domain.JobRunning, RequestedBy: req.Actor, RatePerSec: req.RatePerSec,
		MaxMessages: req.MaxMessages, Total: len(ids), CreatedAt: e.now(),
	}

	if req.DryRun {
		fin := e.now()
		job.Status, job.FinishedAt = domain.JobCompleted, &fin
		if err := e.store.CreateJob(ctx, job); err != nil {
			return job, err
		}
		e.audit(ctx, req.Actor, "replay.dryrun", job, fmt.Sprintf("would replay %d messages", job.Total))
		return job, nil
	}

	key := req.Queue.Name + "/" + req.GroupKey
	e.mu.Lock()
	if _, busy := e.running[key]; busy {
		e.mu.Unlock()
		return domain.ReplayJob{}, ErrBusy
	}
	if _, err := e.store.ActiveJob(ctx, req.Queue.Name, req.GroupKey); err == nil {
		e.mu.Unlock()
		return domain.ReplayJob{}, ErrBusy
	}
	e.running[key] = job.ID
	jobCtx, cancel := context.WithCancel(e.baseCtx)
	e.cancels[job.ID] = cancel
	e.mu.Unlock()

	if err := e.store.CreateJob(ctx, job); err != nil {
		e.release(key, job.ID)
		cancel()
		return job, err
	}
	e.audit(ctx, req.Actor, "replay.start", job,
		fmt.Sprintf("total=%d rate=%.1f/s max=%d", job.Total, job.RatePerSec, job.MaxMessages))

	e.wg.Add(1)
	go func() {
		defer e.wg.Done()
		defer cancel()
		defer e.release(key, job.ID)
		e.execute(jobCtx, req, job, ids)
	}()
	return job, nil
}

// Cancel stops a running job. It reports whether the job was running.
func (e *Engine) Cancel(jobID string) bool {
	e.mu.Lock()
	cancel, ok := e.cancels[jobID]
	e.mu.Unlock()
	if ok {
		cancel()
	}
	return ok
}

// Wait blocks until all background jobs have finished.
func (e *Engine) Wait() { e.wg.Wait() }

// Shutdown cancels running jobs and waits for them to record their state.
func (e *Engine) Shutdown() {
	e.cancel()
	e.wg.Wait()
}

func (e *Engine) release(key, jobID string) {
	e.mu.Lock()
	delete(e.running, key)
	delete(e.cancels, jobID)
	e.mu.Unlock()
}

func (e *Engine) validate(req Request) error {
	switch {
	case req.Queue.Name == "" || req.Queue.DLQURL == "" || req.Queue.DestURL == "":
		return fmt.Errorf("%w: queue is not fully configured", ErrInvalid)
	case req.GroupKey == "":
		return fmt.Errorf("%w: group is required", ErrInvalid)
	case req.Actor == "":
		return fmt.Errorf("%w: actor is required", ErrInvalid)
	case req.RatePerSec <= 0 || req.RatePerSec > e.cfg.MaxRatePerSec:
		return fmt.Errorf("%w: rate must be between 0 and %.0f messages per second", ErrInvalid, e.cfg.MaxRatePerSec)
	case req.MaxMessages < 1 || req.MaxMessages > e.cfg.MaxMessagesLimit:
		return fmt.Errorf("%w: maxMessages must be between 1 and %d", ErrInvalid, e.cfg.MaxMessagesLimit)
	}
	return nil
}

func (e *Engine) execute(ctx context.Context, req Request, job domain.ReplayJob, ids []string) {
	remaining := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		remaining[id] = struct{}{}
	}
	limiter := rate.NewLimiter(rate.Limit(req.RatePerSec), 1)
	lastProgress := e.now()
	receiveErrs := 0
	sinceSave := 0
	var failure string

loop:
	for len(remaining) > 0 {
		if ctx.Err() != nil {
			break
		}
		msgs, err := e.queue.Receive(ctx, req.Queue.DLQURL, 10, e.cfg.Visibility)
		if err != nil {
			if ctx.Err() != nil {
				break
			}
			receiveErrs++
			e.log.Warn("receive from dead-letter queue failed", "job", job.ID, "err", err)
			if receiveErrs >= 3 {
				failure = "could not read the dead-letter queue: " + err.Error()
				break
			}
			if !e.sleep(ctx, e.cfg.PollInterval) {
				break
			}
			continue
		}
		receiveErrs = 0
		if len(msgs) == 0 {
			if e.now().Sub(lastProgress) > e.cfg.Patience {
				break
			}
			if !e.sleep(ctx, e.cfg.PollInterval) {
				break
			}
			continue
		}
		for _, m := range msgs {
			if _, ours := remaining[m.ID]; !ours {
				// Not part of this job; it becomes visible again after Visibility.
				continue
			}
			lastProgress = e.now()
			delete(remaining, m.ID)
			outcome := e.replayOne(ctx, req, job, m)
			switch outcome {
			case outcomeReplayed:
				job.Replayed++
			case outcomeSkipped:
				job.Skipped++
			case outcomeFailed:
				job.Failed++
			case outcomeStopped:
				remaining[m.ID] = struct{}{}
				break loop
			}
			if sinceSave++; sinceSave >= 10 {
				sinceSave = 0
				e.save(job)
			}
			if err := limiter.Wait(ctx); err != nil {
				break loop
			}
		}
	}

	fin := e.now()
	job.FinishedAt = &fin
	switch {
	case ctx.Err() != nil:
		job.Status = domain.JobCancelled
		if e.baseCtx.Err() != nil {
			job.Error = "service shutting down"
		}
	case failure != "":
		job.Status, job.Error = domain.JobFailed, failure
	default:
		job.Status = domain.JobCompleted
		if n := len(remaining); n > 0 {
			job.Failed += n
			job.Status = domain.JobFailed
			job.Error = fmt.Sprintf("%d messages were not found in the dead-letter queue (already removed or still hidden)", n)
		} else if job.Failed > 0 {
			job.Status = domain.JobFailed
			job.Error = fmt.Sprintf("%d messages could not be replayed and remain in the dead-letter queue", job.Failed)
		}
	}
	e.save(job)
	action := map[domain.JobStatus]string{
		domain.JobCompleted: "replay.finish", domain.JobFailed: "replay.finish", domain.JobCancelled: "replay.cancel",
	}[job.Status]
	e.audit(context.WithoutCancel(ctx), req.Actor, action, job,
		fmt.Sprintf("status=%s replayed=%d skipped=%d failed=%d", job.Status, job.Replayed, job.Skipped, job.Failed))
}

type outcome int

const (
	outcomeReplayed outcome = iota
	outcomeSkipped
	outcomeFailed
	outcomeStopped
)

func (e *Engine) replayOne(ctx context.Context, req Request, job domain.ReplayJob, m domain.Message) outcome {
	bg := context.WithoutCancel(ctx) // finish bookkeeping for a message we already sent
	status, err := e.store.MessageStatus(ctx, req.Queue.Name, m.ID)
	if err != nil && !errors.Is(err, store.ErrNotFound) {
		if ctx.Err() != nil {
			return outcomeStopped
		}
		e.log.Error("read message status", "job", job.ID, "message", m.ID, "err", err)
		return outcomeFailed
	}

	if status == domain.StatusReplayed {
		// Sent by an earlier run that stopped before deleting it.
		if err := e.queue.Delete(bg, req.Queue.DLQURL, m); err != nil {
			e.log.Warn("delete already-replayed message", "job", job.ID, "message", m.ID, "err", err)
			return outcomeFailed
		}
		return outcomeSkipped
	}

	out := m
	out.Attributes = copyAttrs(m.Attributes)
	if e.cfg.TagAttributes {
		if len(out.Attributes)+2 <= maxSQSAttributes {
			out.Attributes["x-dlq-triage-original-id"] = m.ID
			out.Attributes["x-dlq-triage-job"] = job.ID
		}
	}
	if _, err := e.queue.Send(ctx, req.Queue.DestURL, out, m.ID); err != nil {
		if ctx.Err() != nil {
			return outcomeStopped
		}
		e.log.Warn("send to destination failed", "job", job.ID, "message", m.ID, "err", err)
		return outcomeFailed
	}
	if err := e.store.MarkReplayed(bg, req.Queue.Name, m.ID, e.now()); err != nil {
		// Sent but not recorded: leave it in the DLQ so a later run can reconcile.
		e.log.Error("record replayed message", "job", job.ID, "message", m.ID, "err", err)
		return outcomeFailed
	}
	if err := e.queue.Delete(bg, req.Queue.DLQURL, m); err != nil {
		// The store already says replayed, so a later run removes it without resending.
		e.log.Warn("delete replayed message from dead-letter queue", "job", job.ID, "message", m.ID, "err", err)
	}
	return outcomeReplayed
}

func (e *Engine) save(job domain.ReplayJob) {
	if err := e.store.UpdateJob(context.WithoutCancel(e.baseCtx), job); err != nil {
		e.log.Error("save job", "job", job.ID, "err", err)
	}
}

func (e *Engine) audit(ctx context.Context, actor, action string, job domain.ReplayJob, detail string) {
	err := e.store.AppendAudit(ctx, domain.AuditEntry{
		At: e.now(), Actor: actor, Action: action, Queue: job.Queue, GroupKey: job.GroupKey, JobID: job.ID, Detail: detail,
	})
	if err != nil {
		e.log.Error("write audit entry", "action", action, "job", job.ID, "err", err)
	}
}

func (e *Engine) sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

func copyAttrs(in map[string]string) map[string]string {
	out := make(map[string]string, len(in)+2)
	for k, v := range in {
		out[k] = v
	}
	return out
}
