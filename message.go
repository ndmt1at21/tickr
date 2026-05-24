// Package tickr is a reliable async messaging library for microservices
// implementing the outbox pattern. The storage table is the queue: callers
// enqueue messages inside their own DB transaction (transactional outbox),
// and registered handlers run in-process via the worker pool.
package tickr

import "time"

// MessageID identifies a tickr message. Storage adapters use UUIDv7 strings
// so that IDs are lexicographically time-ordered and shard-portable.
type MessageID string

// Status is the lifecycle state of a message.
type Status string

// Lifecycle states. SUCCESS and DEAD are terminal; the others are transient.
const (
	StatusCreated  Status = "CREATED"
	StatusHandling Status = "HANDLING"
	StatusSuccess  Status = "SUCCESS"
	StatusFailed   Status = "FAILED"
	StatusRetrying Status = "RETRYING"
	StatusDead     Status = "DEAD"
)

// Terminal reports whether a status is terminal (no further transitions
// except via admin Requeue).
func (s Status) Terminal() bool {
	return s == StatusSuccess || s == StatusDead
}

// Message is what the caller supplies at enqueue time.
type Message struct {
	// Type is the routing key matched against the HandlerRegistry.
	Type string

	// Payload is the opaque message body. Encoding is the caller's choice.
	Payload []byte

	// Headers carries optional metadata. Trace propagation headers
	// (W3C traceparent / tracestate) are injected here automatically
	// when a Tracer is configured on the Client.
	Headers map[string]string

	// IdempotencyKey, when set, makes the (Type, IdempotencyKey) pair
	// unique across the outbox. A second Enqueue with the same key returns
	// *ErrDuplicate carrying the existing MessageID.
	IdempotencyKey string

	// ScheduledAt makes the message eligible only after this instant.
	// Zero value means eligible immediately.
	ScheduledAt time.Time

	// MaxAttempts overrides the handler default for this single message.
	// Zero means "inherit the handler's configured MaxAttempts".
	MaxAttempts int
}

// InboundMessage is the read-side view of a message handed to a Handler.
type InboundMessage struct {
	ID             MessageID
	Type           string
	Payload        []byte
	Headers        map[string]string
	IdempotencyKey string

	// Attempt is 1-indexed; it is incremented at claim time before dispatch.
	Attempt     int
	MaxAttempts int

	EnqueuedAt  time.Time
	ScheduledAt time.Time

	// LastError is the error string from the previous attempt, or "" on the
	// first attempt.
	LastError string

	// Status is the row's lifecycle status at the time it was read. On the
	// [Handler] hot path it is always [StatusHandling] (set by the claim)
	// and handlers can safely ignore it.
	Status Status
}

// Transition is one row in the append-only message history.
type Transition struct {
	MessageID MessageID
	Seq       int
	From      Status
	To        Status
	Attempt   int
	Error     string
	WorkerID  string
	At        time.Time
}

// Outcome categorises the result of a single handler attempt for metrics.
type Outcome string

// Handler-attempt outcomes used by the Metrics hook.
const (
	OutcomeSuccess  Outcome = "success"
	OutcomeRetry    Outcome = "retry"
	OutcomeDead     Outcome = "dead"
	OutcomeCanceled Outcome = "canceled"
)
