package tickr

import "context"

// Notifier is an optional capability for Storage adapters. When the
// configured Storage also implements Notifier, the Client publishes an
// enqueue notification on every successful insert, and the Worker
// subscribes so it can wake up from its poll backoff immediately.
//
// Adapters that do not implement Notifier degrade gracefully to pure
// polling — there is no behavioural change beyond latency.
//
// Semantics:
//
//   - Notify must be called inside the caller's transaction when present,
//     so that the notification is only observable to subscribers if (and
//     when) that transaction commits. This preserves the transactional
//     outbox guarantee.
//   - Listen returns a channel that fires (coalesced, non-blocking) every
//     time at least one new message has been enqueued since the last read.
//     Receivers must treat the signal as a hint, not as a count.
//   - The cleanup function returned by Listen releases the underlying
//     connection/subscription and must be safe to call multiple times.
type Notifier interface {
	Notify(ctx context.Context, tx Tx, eventType string) error
	Listen(ctx context.Context) (<-chan struct{}, func() error, error)
}
