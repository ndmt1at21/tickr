package tickr

import (
	"context"
	"time"
)

// Logger is the optional structured-logging hook. kv pairs follow slog
// conventions: alternating keys (strings) and values.
type Logger interface {
	Debug(ctx context.Context, msg string, kv ...any)
	Info(ctx context.Context, msg string, kv ...any)
	Warn(ctx context.Context, msg string, kv ...any)
	Error(ctx context.Context, msg string, err error, kv ...any)
}

// Metrics is the optional metrics hook. Implementations must be safe for
// concurrent use. The Prometheus implementation lives in tickr/metrics/prom.
type Metrics interface {
	MessageEnqueued(eventType string)
	HandlerStarted(eventType string, attempt int)
	HandlerCompleted(eventType string, attempt int, duration time.Duration, outcome Outcome)
	MessageDead(eventType string)
	QueueDepth(eventType string, status Status, depth int)
	ClaimBatch(claimed, requested int, duration time.Duration)
	LeaseReclaimed(eventType string, count int)
}

// EndSpanFunc finishes a span started by a Tracer call. Pass the resulting
// error (or nil on success) so the span can record exception attributes.
type EndSpanFunc func(err error)

// Tracer is the optional tracing hook. The OpenTelemetry implementation
// lives in tickr/tracing/otel and handles W3C traceparent propagation
// through Message.Headers automatically.
type Tracer interface {
	// StartEnqueueSpan wraps a producer-side Enqueue and lets the impl
	// inject trace propagation into the returned context. The Client also
	// reads back the propagation headers via InjectHeaders so they can be
	// written to the outbox row.
	StartEnqueueSpan(ctx context.Context, eventType string) (context.Context, EndSpanFunc)

	// StartClaimSpan wraps a worker-side claim batch.
	StartClaimSpan(ctx context.Context) (context.Context, EndSpanFunc)

	// StartHandlerSpan wraps a single handler attempt. Implementations
	// extract the parent span from msg.Headers (W3C traceparent) so a
	// single trace links producer → outbox → consumer across processes.
	StartHandlerSpan(ctx context.Context, msg *InboundMessage) (context.Context, EndSpanFunc)

	// InjectHeaders writes the current trace context from ctx into the
	// given headers map (creating it if nil). The Client calls this before
	// writing the outbox row so downstream handlers can re-attach.
	InjectHeaders(ctx context.Context, headers map[string]string) map[string]string
}

// ErrorKind classifies where an [ErrorEvent] originated.
type ErrorKind string

const (
	// ErrorKindHandler — a handler attempt returned a non-nil error and the
	// message is being retried.
	ErrorKindHandler ErrorKind = "handler"
	// ErrorKindDeadLetter — a message moved to DEAD (max attempts exhausted,
	// DeadLetter sentinel, or WithDeadLetterIf predicate matched).
	ErrorKindDeadLetter ErrorKind = "dead_letter"
	// ErrorKindNoHandler — a claimed message has no registered handler and
	// was dead-lettered.
	ErrorKindNoHandler ErrorKind = "no_handler"
	// ErrorKindClaim — storage.Claim failed.
	ErrorKindClaim ErrorKind = "claim"
	// ErrorKindStorage — a Succeed/Fail/Release write failed after a handler
	// attempt completed.
	ErrorKindStorage ErrorKind = "storage"
	// ErrorKindLease — lease extension errored or was lost mid-attempt.
	ErrorKindLease ErrorKind = "lease"
	// ErrorKindJanitor — the retention janitor failed (leader lock or purge).
	ErrorKindJanitor ErrorKind = "janitor"
	// ErrorKindReclaimer — the lease reclaimer failed (leader lock or reclaim).
	ErrorKindReclaimer ErrorKind = "reclaimer"
)

// ErrorEvent describes a single error worth alerting on. EventType,
// MessageID, and Attempt are zero/empty for core errors not tied to a
// specific message.
type ErrorEvent struct {
	Kind      ErrorKind
	EventType string
	MessageID MessageID
	Attempt   int
	Err       error
}

// Alerter is the optional hook for asynchronous error notifications —
// e.g. post to Slack, send an email, or page via PagerDuty.
//
// OnError is invoked in a fresh goroutine detached from the caller's
// context, so the call neither blocks the engine nor is cancelled by
// shutdown. The supplied ctx carries a 30s timeout. The library recovers
// from panics inside OnError.
//
// Implementations should still be cheap to invoke (or hand off to their
// own buffered worker): a sustained error burst means one goroutine per
// event. Throttle, batch, or coalesce inside OnError if you call out to
// rate-limited services.
type Alerter interface {
	OnError(ctx context.Context, evt ErrorEvent)
}

// nopAlerter is the default when no Alerter is configured.
type nopAlerter struct{}

func (nopAlerter) OnError(context.Context, ErrorEvent) {}

func defaultedAlerter(a Alerter) Alerter {
	if a == nil {
		return nopAlerter{}
	}
	return a
}

// fireAlert dispatches evt to alerter asynchronously. It is a no-op when
// alerter is the nopAlerter, so the hot path pays nothing when alerts are
// not configured.
func fireAlert(alerter Alerter, logger Logger, ctx context.Context, evt ErrorEvent) {
	if _, ok := alerter.(nopAlerter); ok {
		return
	}
	go func() {
		defer func() {
			if r := recover(); r != nil {
				logger.Warn(context.Background(), "tickr: alerter panic",
					"kind", string(evt.Kind), "panic", r)
			}
		}()
		alertCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
		defer cancel()
		alerter.OnError(alertCtx, evt)
	}()
}

// nopLogger is the default when no Logger is configured.
type nopLogger struct{}

func (nopLogger) Debug(context.Context, string, ...any)        {}
func (nopLogger) Info(context.Context, string, ...any)         {}
func (nopLogger) Warn(context.Context, string, ...any)         {}
func (nopLogger) Error(context.Context, string, error, ...any) {}

// nopMetrics is the default when no Metrics is configured.
type nopMetrics struct{}

func (nopMetrics) MessageEnqueued(string)                               {}
func (nopMetrics) HandlerStarted(string, int)                           {}
func (nopMetrics) HandlerCompleted(string, int, time.Duration, Outcome) {}
func (nopMetrics) MessageDead(string)                                   {}
func (nopMetrics) QueueDepth(string, Status, int)                       {}
func (nopMetrics) ClaimBatch(int, int, time.Duration)                   {}
func (nopMetrics) LeaseReclaimed(string, int)                           {}

// nopTracer is the default when no Tracer is configured. Spans are no-ops
// and InjectHeaders returns the headers unchanged.
type nopTracer struct{}

func (nopTracer) StartEnqueueSpan(ctx context.Context, _ string) (context.Context, EndSpanFunc) {
	return ctx, func(error) {}
}
func (nopTracer) StartClaimSpan(ctx context.Context) (context.Context, EndSpanFunc) {
	return ctx, func(error) {}
}
func (nopTracer) StartHandlerSpan(ctx context.Context, _ *InboundMessage) (context.Context, EndSpanFunc) {
	return ctx, func(error) {}
}
func (nopTracer) InjectHeaders(_ context.Context, headers map[string]string) map[string]string {
	return headers
}
