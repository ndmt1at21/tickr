// Package otel provides an OpenTelemetry implementation of tickr.Tracer.
// It uses W3C trace-context propagation through Message.Headers so that
// producer-side spans become the parent of consumer-side handler spans
// even across processes and time.
package otel

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	"github.com/ndmt1at21/tickr"
)

// Tracer implements tickr.Tracer using a configurable otel/trace.Tracer
// and propagator. By default it uses the global TracerProvider and the
// global TextMapPropagator (which apps usually wire to W3C trace-context).
type Tracer struct {
	t          trace.Tracer
	propagator propagation.TextMapPropagator
}

// Option configures a Tracer.
type Option func(*Tracer)

// WithTracer overrides the otel/trace.Tracer used to create spans.
func WithTracer(t trace.Tracer) Option {
	return func(o *Tracer) { o.t = t }
}

// WithPropagator overrides the TextMapPropagator used for context
// injection/extraction through Message.Headers.
func WithPropagator(p propagation.TextMapPropagator) Option {
	return func(o *Tracer) { o.propagator = p }
}

// New constructs a Tracer. Pass options to override the tracer or
// propagator; defaults pick up the global TracerProvider and the global
// TextMapPropagator at call time.
func New(opts ...Option) *Tracer {
	t := &Tracer{
		t:          otel.GetTracerProvider().Tracer("tickr"),
		propagator: otel.GetTextMapPropagator(),
	}
	for _, o := range opts {
		o(t)
	}
	return t
}

// StartEnqueueSpan implements tickr.Tracer.
func (tr *Tracer) StartEnqueueSpan(ctx context.Context, eventType string) (context.Context, tickr.EndSpanFunc) {
	ctx, span := tr.t.Start(ctx, "tickr.enqueue",
		trace.WithSpanKind(trace.SpanKindProducer),
		trace.WithAttributes(
			attribute.String("messaging.system", "tickr"),
			attribute.String("messaging.operation", "publish"),
			attribute.String("messaging.destination.name", eventType),
		),
	)
	return ctx, finisher(span)
}

// StartClaimSpan implements tickr.Tracer.
func (tr *Tracer) StartClaimSpan(ctx context.Context) (context.Context, tickr.EndSpanFunc) {
	ctx, span := tr.t.Start(ctx, "tickr.claim",
		trace.WithSpanKind(trace.SpanKindInternal),
		trace.WithAttributes(attribute.String("messaging.system", "tickr")),
	)
	return ctx, finisher(span)
}

// StartHandlerSpan implements tickr.Tracer. The parent span context is
// restored from msg.Headers (W3C traceparent), so the resulting span links
// to the producer-side enqueue span as its parent.
func (tr *Tracer) StartHandlerSpan(ctx context.Context, msg *tickr.InboundMessage) (context.Context, tickr.EndSpanFunc) {
	parent := tr.propagator.Extract(ctx, mapCarrier(msg.Headers))
	ctx, span := tr.t.Start(parent, "tickr.handler",
		trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithAttributes(
			attribute.String("messaging.system", "tickr"),
			attribute.String("messaging.operation", "process"),
			attribute.String("messaging.destination.name", msg.Type),
			attribute.String("messaging.message.id", string(msg.ID)),
			attribute.Int("tickr.attempt", msg.Attempt),
			attribute.Int("tickr.max_attempts", msg.MaxAttempts),
		),
	)
	return ctx, finisher(span)
}

// InjectHeaders implements tickr.Tracer. Returns the headers map with the
// current trace context injected (W3C traceparent / tracestate).
func (tr *Tracer) InjectHeaders(ctx context.Context, headers map[string]string) map[string]string {
	if headers == nil {
		headers = map[string]string{}
	}
	tr.propagator.Inject(ctx, mapCarrier(headers))
	return headers
}

// mapCarrier adapts map[string]string to otel's TextMapCarrier interface.
type mapCarrier map[string]string

func (m mapCarrier) Get(key string) string { return m[key] }
func (m mapCarrier) Set(key, value string) { m[key] = value }
func (m mapCarrier) Keys() []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

func finisher(span trace.Span) tickr.EndSpanFunc {
	return func(err error) {
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		}
		span.End()
	}
}

// Ensure Tracer satisfies tickr.Tracer.
var _ tickr.Tracer = (*Tracer)(nil)
