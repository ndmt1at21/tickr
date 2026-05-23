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
