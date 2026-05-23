package tickr

import (
	"context"
	"time"
)

// Admin is an optional capability that a [Storage] adapter may also
// implement to power the embedded Web UI ([github.com/ndmt1at21/tickr/ui])
// and admin tooling. The methods here are scoped to operator workflows
// (browse, inspect, recover) rather than the producer/consumer hot path,
// so they may use ad-hoc queries.
//
// Adapters that do not implement Admin can still be used by the UI in a
// reduced read-only mode backed by [Storage.ListDead], [Storage.Stats],
// and [Storage.History].
type Admin interface {
	// Get returns a single message by ID, regardless of status. Returns
	// nil, nil if not found.
	Get(ctx context.Context, id MessageID) (*InboundMessage, error)

	// List returns messages matching q ordered by id descending (newest
	// first). Use q.AfterID for cursor-based pagination — pass the id of
	// the last row you saw to fetch the next page.
	List(ctx context.Context, q ListQuery) ([]*InboundMessage, error)

	// Kill moves a non-terminal message straight to DEAD with the given
	// reason recorded in history. No-op (and returns nil) if the message
	// is already terminal.
	Kill(ctx context.Context, id MessageID, reason string) error
}

// ListQuery filters [Admin.List].
type ListQuery struct {
	// Status restricts to a single lifecycle status. Empty means "any".
	Status Status

	// EventType restricts to a single event type. Empty means "any".
	EventType string

	// Search is a case-insensitive substring match against payload and
	// idempotency_key. Empty means "no search".
	Search string

	// AfterID is a cursor: only rows with id < AfterID are returned.
	// Empty means "start from newest".
	AfterID MessageID

	// Limit caps the number of rows returned. Zero defaults to 50; the
	// adapter may clamp the maximum.
	Limit int

	// Since restricts to rows created at or after this time. Zero means
	// "no lower bound".
	Since time.Time
}
