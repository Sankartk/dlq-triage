package queue

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/Sankartk/dlq-triage/internal/domain"
)

// Memory is an in-process Queue that follows SQS visibility semantics.
// It exists so the engine can be tested without a queue service, and it is
// also used for local demos. It is not a substitute for a real queue.
type Memory struct {
	mu     sync.Mutex
	now    func() time.Time
	queues map[string]*memQueue
	seq    int
	// FailSend, when set, is called before every Send and may return an error.
	FailSend func(url string, msg domain.Message) error
}

type memMessage struct {
	msg          domain.Message
	visibleAt    time.Time
	receiveCount int
	handle       string
}

type memQueue struct {
	messages []*memMessage
	dedup    map[string]string // dedup id -> message id
}

// NewMemory creates an empty in-memory queue service.
func NewMemory() *Memory {
	return &Memory{now: time.Now, queues: map[string]*memQueue{}}
}

// SetClock replaces the clock; tests use it to advance visibility timeouts.
func (m *Memory) SetClock(now func() time.Time) { m.mu.Lock(); m.now = now; m.mu.Unlock() }

func (m *Memory) q(url string) *memQueue {
	q, ok := m.queues[url]
	if !ok {
		q = &memQueue{dedup: map[string]string{}}
		m.queues[url] = q
	}
	return q
}

// Seed adds messages directly, bypassing Send. It returns the ids it assigned
// when a message has no id.
func (m *Memory) Seed(url string, msgs ...domain.Message) []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	q := m.q(url)
	ids := make([]string, 0, len(msgs))
	for _, msg := range msgs {
		if msg.ID == "" {
			m.seq++
			msg.ID = fmt.Sprintf("seed-%d", m.seq)
		}
		q.messages = append(q.messages, &memMessage{msg: msg, receiveCount: msg.ReceiveCount})
		ids = append(ids, msg.ID)
	}
	return ids
}

// Bodies returns the bodies currently stored on a queue regardless of
// visibility, in insertion order. Tests use it to assert what was replayed.
func (m *Memory) Bodies(url string) []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []string
	for _, mm := range m.q(url).messages {
		out = append(out, mm.msg.Body)
	}
	return out
}

// Messages returns copies of every message on a queue regardless of visibility.
func (m *Memory) Messages(url string) []domain.Message {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []domain.Message
	for _, mm := range m.q(url).messages {
		out = append(out, mm.msg)
	}
	return out
}

func (m *Memory) Receive(_ context.Context, url string, max int, visibility time.Duration) ([]domain.Message, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if max <= 0 || max > 10 {
		return nil, errors.New("max must be between 1 and 10")
	}
	now := m.now()
	var out []domain.Message
	for _, mm := range m.q(url).messages {
		if len(out) == max {
			break
		}
		if mm.visibleAt.After(now) {
			continue
		}
		mm.receiveCount++
		m.seq++
		mm.handle = fmt.Sprintf("h-%d", m.seq)
		mm.visibleAt = now.Add(visibility)
		cp := mm.msg
		cp.ReceiveCount = mm.receiveCount
		cp.Handle = mm.handle
		out = append(out, cp)
	}
	return out, nil
}

func (m *Memory) Delete(_ context.Context, url string, msg domain.Message) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	q := m.q(url)
	for i, mm := range q.messages {
		if mm.msg.ID == msg.ID {
			if mm.handle != msg.Handle {
				// SQS reports success for stale handles of a deleted message but
				// rejects them while the message still exists.
				return errors.New("receipt handle is not valid")
			}
			q.messages = append(q.messages[:i], q.messages[i+1:]...)
			return nil
		}
	}
	return nil
}

func (m *Memory) Send(_ context.Context, url string, msg domain.Message, dedupID string) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.FailSend != nil {
		if err := m.FailSend(url, msg); err != nil {
			return "", err
		}
	}
	q := m.q(url)
	if dedupID != "" {
		if id, ok := q.dedup[dedupID]; ok {
			return id, nil
		}
	}
	m.seq++
	id := fmt.Sprintf("msg-%d", m.seq)
	if dedupID != "" {
		q.dedup[dedupID] = id
	}
	cp := msg
	cp.ID = id
	cp.Handle = ""
	cp.ReceiveCount = 0
	q.messages = append(q.messages, &memMessage{msg: cp})
	return id, nil
}

func (m *Memory) Depth(_ context.Context, url string) (domain.QueueDepth, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	now := m.now()
	var d domain.QueueDepth
	for _, mm := range m.q(url).messages {
		if mm.visibleAt.After(now) {
			d.InFlight++
		} else {
			d.Visible++
		}
	}
	return d, nil
}
