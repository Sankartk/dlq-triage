package api

import (
	"sync"
	"time"

	"github.com/Sankartk/dlq-triage/internal/ingest"
)

// ScanStatus is the outcome of the latest scan of a queue.
type ScanStatus struct {
	At      time.Time
	Seen    int
	New     int
	Skipped bool
	Err     string
}

// ScanTracker remembers the latest scan of every queue.
type ScanTracker struct {
	mu   sync.RWMutex
	last map[string]ScanStatus
	now  func() time.Time
}

// NewScanTracker creates an empty tracker.
func NewScanTracker() *ScanTracker {
	return &ScanTracker{last: map[string]ScanStatus{}, now: time.Now}
}

// Record is suitable as an ingest.Ingester OnScan hook.
func (s *ScanTracker) Record(queue string, res ingest.Result, err error) {
	st := ScanStatus{At: s.now(), Seen: res.Seen, New: res.New, Skipped: res.Skipped}
	if err != nil {
		st.Err = err.Error()
	}
	s.mu.Lock()
	s.last[queue] = st
	s.mu.Unlock()
}

// Last returns the latest status for a queue.
func (s *ScanTracker) Last(queue string) (ScanStatus, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	st, ok := s.last[queue]
	return st, ok
}
