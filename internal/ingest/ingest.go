// Package ingest copies dead-lettered messages into the store and groups them.
//
// Ingesting never deletes anything from the dead-letter queue. It receives
// messages with a short visibility timeout, records them, and lets them
// reappear. The side effect is that each scan increments the queue's
// approximate receive count of the messages it sees.
package ingest

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/Sankartk/dlq-triage/internal/domain"
	"github.com/Sankartk/dlq-triage/internal/fingerprint"
	"github.com/Sankartk/dlq-triage/internal/queue"
	"github.com/Sankartk/dlq-triage/internal/store"
)

// Source names a dead-letter queue to watch.
type Source struct {
	Name   string
	DLQURL string
}

// Config tunes scanning.
type Config struct {
	Fingerprint fingerprint.Config
	// Visibility is how long scanned messages stay hidden. It also bounds how
	// soon a replay can receive them, so keep it short.
	Visibility time.Duration
	// MaxPerScan caps messages read in one scan, bounding the receive-count
	// side effect on very deep queues.
	MaxPerScan int
	// Interval is the pause between scans of a queue.
	Interval time.Duration
}

func (c Config) withDefaults() Config {
	if c.Visibility <= 0 {
		c.Visibility = 10 * time.Second
	}
	if c.MaxPerScan <= 0 {
		c.MaxPerScan = 1000
	}
	if c.Interval <= 0 {
		c.Interval = 60 * time.Second
	}
	return c
}

// Result summarises one scan.
type Result struct {
	Seen    int // messages received
	New     int // messages stored for the first time
	Skipped bool
}

// Ingester scans dead-letter queues.
type Ingester struct {
	store store.Store
	queue queue.Queue
	cfg   Config
	log   *slog.Logger
	now   func() time.Time

	// Busy reports whether a queue should be left alone, for example while a
	// replay is moving its messages.
	Busy func(queueName string) bool
	// OnScan is called after every scan with its outcome.
	OnScan func(queueName string, res Result, err error)
}

// New builds an ingester.
func New(st store.Store, q queue.Queue, cfg Config, log *slog.Logger) *Ingester {
	if log == nil {
		log = slog.Default()
	}
	return &Ingester{store: st, queue: q, cfg: cfg.withDefaults(), log: log, now: time.Now}
}

// Scan reads a queue once.
func (i *Ingester) Scan(ctx context.Context, src Source) (Result, error) {
	if i.Busy != nil && i.Busy(src.Name) {
		res := Result{Skipped: true}
		i.report(src, res, nil)
		return res, nil
	}
	var res Result
	empty := 0
	for res.Seen < i.cfg.MaxPerScan {
		if err := ctx.Err(); err != nil {
			return res, err
		}
		batch := 10
		if left := i.cfg.MaxPerScan - res.Seen; left < batch {
			batch = left
		}
		msgs, err := i.queue.Receive(ctx, src.DLQURL, batch, i.cfg.Visibility)
		if err != nil {
			i.report(src, res, err)
			return res, err
		}
		if len(msgs) == 0 {
			// Two consecutive empty receives guard against a single empty
			// response from a sampled short poll.
			if empty++; empty >= 2 {
				break
			}
			continue
		}
		empty = 0
		for _, m := range msgs {
			created, err := i.record(ctx, src, m)
			if err != nil {
				i.report(src, res, err)
				return res, err
			}
			res.Seen++
			if created {
				res.New++
			}
		}
	}
	i.report(src, res, nil)
	return res, nil
}

func (i *Ingester) record(ctx context.Context, src Source, m domain.Message) (bool, error) {
	fp := fingerprint.Compute(m, i.cfg.Fingerprint)
	return i.store.UpsertMessage(ctx, domain.StoredMessage{
		Queue: src.Name, ID: m.ID, GroupKey: fp.Key, Body: m.Body, Attributes: m.Attributes,
		GroupID: m.GroupID, ReceiveCount: m.ReceiveCount, SentAt: m.SentAt, IngestedAt: i.now(),
	}, fp.ErrorSig, fp.Shape)
}

func (i *Ingester) report(src Source, res Result, err error) {
	if err != nil && !errors.Is(err, context.Canceled) {
		i.log.Warn("dead-letter scan failed", "queue", src.Name, "seen", res.Seen, "err", err)
	} else if err == nil && res.New > 0 {
		i.log.Info("dead-letter scan", "queue", src.Name, "seen", res.Seen, "new", res.New)
	}
	if i.OnScan != nil {
		i.OnScan(src.Name, res, err)
	}
}

// Run scans every source immediately and then on each interval until ctx ends.
func (i *Ingester) Run(ctx context.Context, sources []Source) {
	var wg sync.WaitGroup
	for _, src := range sources {
		wg.Add(1)
		go func(src Source) {
			defer wg.Done()
			t := time.NewTimer(0)
			defer t.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-t.C:
					i.Scan(ctx, src)
					t.Reset(i.cfg.Interval)
				}
			}
		}(src)
	}
	wg.Wait()
}
