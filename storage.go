package tickr

import (
	"context"
	"time"
)

// Tx is the caller's database transaction handed to Storage.Enqueue. It is
// produced by the concrete adapter's WrapTx helper (e.g.
// postgres.WrapTx(pgx.Tx)). A zero Tx means "no caller transaction" — the
// adapter opens its own short-lived auto-commit for the insert.
type Tx struct{ inner any }

// MakeTx wraps the adapter's native transaction handle. Storage adapter
// implementations call this from their WrapTx helper.
func MakeTx(inner any) Tx { return Tx{inner: inner} }

// Inner returns the adapter-specific transaction handle for the adapter
// to type-assert.
func (t Tx) Inner() any { return t.inner }

// IsZero reports whether Tx is the zero value (no caller transaction).
func (t Tx) IsZero() bool { return t.inner == nil }

// Stats is a point-in-time snapshot of queue depth.
type Stats struct {
	ByStatus    map[Status]int
	ByEventType map[string]map[Status]int
	SampledAt   time.Time
}

// EnqueueParams is the input to Storage.Enqueue.
type EnqueueParams struct {
	EventType      string
	Payload        []byte
	Headers        map[string]string
	IdempotencyKey string
	MaxAttempts    int
	ProcessAt      time.Time
}

// ClaimParams controls one poll cycle.
type ClaimParams struct {
	WorkerID   string
	Batch      int
	Lease      time.Duration
	EventTypes []string
}

// FailParams describes a single failed attempt.
type FailParams struct {
	MessageID   MessageID
	Attempt     int
	WorkerID    string
	Err         string
	NextRetryAt time.Time
	Dead        bool
}

// Storage is the backend-agnostic outbox persistence interface. All methods
// must be safe for concurrent use across goroutines and processes.
type Storage interface {
	Enqueue(ctx context.Context, tx Tx, p EnqueueParams) (*InboundMessage, error)
	EnqueueBatch(ctx context.Context, tx Tx, ps []EnqueueParams) ([]*InboundMessage, error)

	Claim(ctx context.Context, p ClaimParams) ([]*InboundMessage, error)
	Succeed(ctx context.Context, id MessageID, attempt int, workerID string) error
	Fail(ctx context.Context, p FailParams) error
	ReleaseShutdown(ctx context.Context, id MessageID, attempt int, workerID, reason string) error
	Extend(ctx context.Context, id MessageID, workerID string, until time.Time) (bool, error)

	History(ctx context.Context, id MessageID) ([]Transition, error)
	ListDead(ctx context.Context, eventType string, after time.Time, limit int) ([]*InboundMessage, error)
	Requeue(ctx context.Context, id MessageID, processAt time.Time) error

	ReclaimExpired(ctx context.Context, limit int) (int64, error)
	PurgeTerminal(ctx context.Context, before time.Time, limit int) (int64, error)

	Stats(ctx context.Context) (Stats, error)

	ApplyMigrations(ctx context.Context) error
	TryLeaderLock(ctx context.Context, key string) (bool, func(), error)
}

// HistoryPurger is an optional Storage extension that deletes old
// tickr_history rows. Adapters that implement it are picked up by the
// Worker's janitor when RetentionPolicy.History > 0. Adapters that do not
// implement it silently skip history purge — history grows unbounded
// until an operator runs DELETE manually.
type HistoryPurger interface {
	PurgeHistory(ctx context.Context, before time.Time, limit int) (int64, error)
}
