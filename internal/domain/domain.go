// Package domain holds the types shared by every other package.
package domain

import "time"

// Message is a dead-lettered message as seen by the service.
type Message struct {
	ID           string            // queue-assigned message id; stable across receives
	Body         string            // raw payload
	Attributes   map[string]string // string message attributes
	GroupID      string            // FIFO message group id, empty for standard queues
	ReceiveCount int               // approximate receive count reported by the queue
	SentAt       time.Time         // when the message was first sent, if the queue reports it
	Handle       string            // receipt handle; only valid for the receive that returned it
}

// Group is a set of dead-lettered messages that failed the same way.
type Group struct {
	Queue         string
	Key           string
	ErrorSig      string
	Shape         string
	Count         int
	PendingCount  int
	FirstSeen     time.Time
	LastSeen      time.Time
	AISummary     string
	AISummaryAt   *time.Time
	SampleMessage string // id of one message in the group, empty if none stored
}

// StoredMessage is a message persisted by the ingester.
type StoredMessage struct {
	Queue        string
	ID           string
	GroupKey     string
	Body         string
	Attributes   map[string]string
	GroupID      string
	ReceiveCount int
	SentAt       time.Time
	IngestedAt   time.Time
	Status       MessageStatus
	ReplayedAt   *time.Time
}

// MessageStatus is the lifecycle state of a stored message.
type MessageStatus string

const (
	StatusPending  MessageStatus = "pending"
	StatusReplayed MessageStatus = "replayed"
)

// JobStatus is the lifecycle state of a replay job.
type JobStatus string

const (
	JobRunning   JobStatus = "running"
	JobCompleted JobStatus = "completed"
	JobFailed    JobStatus = "failed"
	JobCancelled JobStatus = "cancelled"
)

// ReplayJob records one replay (or dry run) of a group.
type ReplayJob struct {
	ID          string
	Queue       string
	GroupKey    string
	DryRun      bool
	Status      JobStatus
	RequestedBy string
	RatePerSec  float64
	MaxMessages int
	Total       int // messages selected for this job
	Replayed    int
	Skipped     int // already replayed earlier; removed from the DLQ without resending
	Failed      int
	Error       string
	CreatedAt   time.Time
	FinishedAt  *time.Time
}

// AuditEntry is an append-only record of a state-changing action.
type AuditEntry struct {
	ID       int64
	At       time.Time
	Actor    string
	Action   string
	Queue    string
	GroupKey string
	JobID    string
	Detail   string
}

// QueueDepth is a point-in-time view of a queue.
type QueueDepth struct {
	Visible  int
	InFlight int
	Delayed  int
}
