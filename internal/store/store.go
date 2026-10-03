// Package store persists messages, groups, replay jobs and the audit log.
package store

import (
	"context"
	"errors"
	"time"

	"github.com/Sankartk/dlq-triage/internal/domain"
)

// ErrNotFound is returned when a requested record does not exist.
var ErrNotFound = errors.New("not found")

// Store is the persistence contract. Implementations must be safe for
// concurrent use.
type Store interface {
	// UpsertMessage records a message and its group. It is idempotent per
	// (queue, message id): re-ingesting a known message only refreshes
	// last_seen and the receive count, and never resets a replayed status.
	// It reports whether the message was new.
	UpsertMessage(ctx context.Context, m domain.StoredMessage, errorSig, shape string) (created bool, err error)

	// ListGroups returns groups for a queue ordered by pending count.
	ListGroups(ctx context.Context, queue string) ([]domain.Group, error)
	GetGroup(ctx context.Context, queue, key string) (domain.Group, error)

	// ListMessages returns messages of a group, optionally only pending ones.
	ListMessages(ctx context.Context, queue, groupKey string, pendingOnly bool, limit, offset int) ([]domain.StoredMessage, error)

	// PendingMessageIDs returns up to limit pending message ids of a group,
	// oldest first.
	PendingMessageIDs(ctx context.Context, queue, groupKey string, limit int) ([]string, error)

	// MessageStatus returns the status of one message.
	MessageStatus(ctx context.Context, queue, id string) (domain.MessageStatus, error)

	// MarkReplayed sets a message to replayed.
	MarkReplayed(ctx context.Context, queue, id string, at time.Time) error

	// SetAISummary stores a generated summary for a group.
	SetAISummary(ctx context.Context, queue, key, summary string, at time.Time) error

	CreateJob(ctx context.Context, j domain.ReplayJob) error
	UpdateJob(ctx context.Context, j domain.ReplayJob) error
	GetJob(ctx context.Context, id string) (domain.ReplayJob, error)
	ListJobs(ctx context.Context, queue string, limit int) ([]domain.ReplayJob, error)

	// ActiveJob returns a running job for the group, or ErrNotFound.
	ActiveJob(ctx context.Context, queue, groupKey string) (domain.ReplayJob, error)

	// FailRunningJobs marks jobs left in the running state as failed. It is
	// called at startup, when no job can still be running.
	FailRunningJobs(ctx context.Context, reason string, at time.Time) (int, error)

	AppendAudit(ctx context.Context, e domain.AuditEntry) error
	ListAudit(ctx context.Context, queue string, limit int) ([]domain.AuditEntry, error)

	Close() error
}
