// Package otel provides an OpenTelemetry implementation of tickr.Metrics.
// Instruments are created on the global MeterProvider by default; pass
// WithMeter to scope them to a specific Meter.
package otel

import (
	"context"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/ndmt1at21/tickr"
)

const instrumentationName = "github.com/ndmt1at21/tickr"

// Option configures the Metrics adapter.
type Option func(*Metrics)

// WithMeter overrides the otel Meter used to create instruments. Default
// is the global MeterProvider's Meter for the tickr package.
func WithMeter(m metric.Meter) Option {
	return func(o *Metrics) { o.meter = m }
}

// Metrics implements tickr.Metrics on top of OpenTelemetry instruments.
type Metrics struct {
	meter metric.Meter

	enqueued      metric.Int64Counter
	dead          metric.Int64Counter
	attempts      metric.Int64Counter
	duration      metric.Float64Histogram
	claimBatch    metric.Int64Histogram
	claimDuration metric.Float64Histogram
	leasesReclaim metric.Int64Counter

	// async gauges. We hold last-observed values per label set and emit
	// them from a callback registered with the meter.
	mu             sync.Mutex
	queueDepth     map[depthKey]int64
	inflightCounts map[string]int64
	queueDepthGg   metric.Int64ObservableGauge
	inflightGg     metric.Int64ObservableGauge
}

type depthKey struct {
	eventType string
	status    string
}

// New builds and registers the metric instruments and returns the adapter.
func New(opts ...Option) (*Metrics, error) {
	m := &Metrics{
		meter:          otel.GetMeterProvider().Meter(instrumentationName),
		queueDepth:     map[depthKey]int64{},
		inflightCounts: map[string]int64{},
	}
	for _, opt := range opts {
		opt(m)
	}

	var err error
	if m.enqueued, err = m.meter.Int64Counter(
		"tickr.messages.enqueued",
		metric.WithDescription("Total messages enqueued via tickr.Client."),
	); err != nil {
		return nil, err
	}
	if m.dead, err = m.meter.Int64Counter(
		"tickr.messages.dead",
		metric.WithDescription("Total messages that landed in the dead-letter queue."),
	); err != nil {
		return nil, err
	}
	if m.attempts, err = m.meter.Int64Counter(
		"tickr.handler.attempts",
		metric.WithDescription("Total handler attempts categorised by outcome."),
	); err != nil {
		return nil, err
	}
	if m.duration, err = m.meter.Float64Histogram(
		"tickr.handler.duration",
		metric.WithDescription("Handler duration in seconds."),
		metric.WithUnit("s"),
	); err != nil {
		return nil, err
	}
	if m.claimBatch, err = m.meter.Int64Histogram(
		"tickr.claim.batch_size",
		metric.WithDescription("Number of messages returned by each claim query."),
	); err != nil {
		return nil, err
	}
	if m.claimDuration, err = m.meter.Float64Histogram(
		"tickr.claim.duration",
		metric.WithDescription("Time taken by each claim query."),
		metric.WithUnit("s"),
	); err != nil {
		return nil, err
	}
	if m.leasesReclaim, err = m.meter.Int64Counter(
		"tickr.leases.reclaimed",
		metric.WithDescription("Total expired leases reclaimed by the reclaimer."),
	); err != nil {
		return nil, err
	}

	if m.queueDepthGg, err = m.meter.Int64ObservableGauge(
		"tickr.queue.depth",
		metric.WithDescription("Current count of messages per (event_type, status)."),
		metric.WithInt64Callback(func(_ context.Context, o metric.Int64Observer) error {
			m.mu.Lock()
			defer m.mu.Unlock()
			for k, v := range m.queueDepth {
				o.Observe(v,
					metric.WithAttributes(
						attribute.String("event_type", k.eventType),
						attribute.String("status", k.status),
					),
				)
			}
			return nil
		}),
	); err != nil {
		return nil, err
	}
	if m.inflightGg, err = m.meter.Int64ObservableGauge(
		"tickr.handlers.inflight",
		metric.WithDescription("Currently-running handler attempts."),
		metric.WithInt64Callback(func(_ context.Context, o metric.Int64Observer) error {
			m.mu.Lock()
			defer m.mu.Unlock()
			for et, v := range m.inflightCounts {
				o.Observe(v, metric.WithAttributes(attribute.String("event_type", et)))
			}
			return nil
		}),
	); err != nil {
		return nil, err
	}
	return m, nil
}

func eventTypeAttr(eventType string) metric.MeasurementOption {
	return metric.WithAttributes(attribute.String("event_type", eventType))
}

// MessageEnqueued implements tickr.Metrics.
func (m *Metrics) MessageEnqueued(eventType string) {
	m.enqueued.Add(context.Background(), 1, eventTypeAttr(eventType))
}

// HandlerStarted implements tickr.Metrics.
func (m *Metrics) HandlerStarted(eventType string, _ int) {
	m.mu.Lock()
	m.inflightCounts[eventType]++
	m.mu.Unlock()
}

// HandlerCompleted implements tickr.Metrics.
func (m *Metrics) HandlerCompleted(eventType string, _ int, duration time.Duration, outcome tickr.Outcome) {
	m.mu.Lock()
	if v := m.inflightCounts[eventType]; v > 0 {
		m.inflightCounts[eventType] = v - 1
	}
	m.mu.Unlock()

	attrs := metric.WithAttributes(
		attribute.String("event_type", eventType),
		attribute.String("outcome", string(outcome)),
	)
	m.attempts.Add(context.Background(), 1, attrs)
	m.duration.Record(context.Background(), duration.Seconds(), attrs)
}

// MessageDead implements tickr.Metrics.
func (m *Metrics) MessageDead(eventType string) {
	m.dead.Add(context.Background(), 1, eventTypeAttr(eventType))
}

// QueueDepth implements tickr.Metrics.
func (m *Metrics) QueueDepth(eventType string, status tickr.Status, depth int) {
	m.mu.Lock()
	m.queueDepth[depthKey{eventType, string(status)}] = int64(depth)
	m.mu.Unlock()
}

// ClaimBatch implements tickr.Metrics.
func (m *Metrics) ClaimBatch(claimed, _ int, duration time.Duration) {
	m.claimBatch.Record(context.Background(), int64(claimed))
	m.claimDuration.Record(context.Background(), duration.Seconds())
}

// LeaseReclaimed implements tickr.Metrics.
func (m *Metrics) LeaseReclaimed(eventType string, count int) {
	m.leasesReclaim.Add(context.Background(), int64(count), eventTypeAttr(eventType))
}

var _ tickr.Metrics = (*Metrics)(nil)
