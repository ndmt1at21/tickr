package tickr

import "context"

// Handler processes a single InboundMessage attempt. Returning nil marks
// the attempt SUCCESS. Returning a non-nil error triggers the retry path
// or DLQ depending on attempt count and the error's nature (see DeadLetter,
// RetryAfter, Skip).
//
// Handlers must be safe for concurrent use; the worker may dispatch many
// invocations of the same Handler in parallel up to the configured
// concurrency cap.
type Handler func(ctx context.Context, msg *InboundMessage) error

// BatchHandler processes a slice of InboundMessage of the same event type
// as a single all-or-nothing attempt: returning nil marks every message in
// the batch SUCCESS, while a non-nil error fails every message with the
// same error (each one retries or dead-letters on its own attempt count).
//
// The worker groups same-type messages from one claim cycle and chunks
// them by [WithMaxBatchSize]; each chunk runs in a single goroutine, so a
// BatchHandler must be safe for concurrent use across event types but is
// invoked serially within one batch. Outcome modifiers ([DeadLetter],
// [RetryAfter], [Skip]) apply uniformly to the whole batch.
type BatchHandler func(ctx context.Context, msgs []*InboundMessage) error
