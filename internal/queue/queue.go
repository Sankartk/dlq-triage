// Package queue defines the minimal queue operations the service needs.
package queue

import (
	"context"
	"time"

	"github.com/Sankartk/dlq-triage/internal/domain"
)

// Queue is the set of operations the ingester and replay engine use.
// Implementations must be safe for concurrent use.
type Queue interface {
	// Receive returns up to max messages. Returned messages are hidden from
	// other receivers for visibility, and their ReceiveCount is incremented.
	Receive(ctx context.Context, url string, max int, visibility time.Duration) ([]domain.Message, error)

	// Delete permanently removes a message using the handle from Receive.
	Delete(ctx context.Context, url string, msg domain.Message) error

	// Send puts a message on a queue and returns the new message id.
	Send(ctx context.Context, url string, msg domain.Message, dedupID string) (string, error)

	// Depth reports approximate message counts.
	Depth(ctx context.Context, url string) (domain.QueueDepth, error)
}
