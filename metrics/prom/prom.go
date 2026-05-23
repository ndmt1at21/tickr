// Package prom provides a Prometheus implementation of tickr.Metrics.
package prom

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/ndmt1at21/tickr"
)

// Metrics implements tickr.Metrics by emitting Prometheus counters,
// histograms, and gauges. Construct with New(reg) and pass the returned
// value as WorkerConfig.Metrics and ClientConfig.Metrics.
type Metrics struct {
	enqueued      *prometheus.CounterVec
	dead          *prometheus.CounterVec
	attempts      *prometheus.CounterVec
	duration      *prometheus.HistogramVec
	queueDepth    *prometheus.GaugeVec
	claimBatchSz  prometheus.Histogram
	claimDuration prometheus.Histogram
	leaseReclaim  *prometheus.CounterVec
	inflight      *prometheus.GaugeVec
}

// New registers the tickr metrics on reg (or prometheus.DefaultRegisterer
// when reg is nil) and returns the Metrics adapter.
func New(reg prometheus.Registerer) *Metrics {
	if reg == nil {
		reg = prometheus.DefaultRegisterer
	}
	m := &Metrics{
		enqueued: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "tickr_messages_enqueued_total",
			Help: "Total messages enqueued via tickr.Client.",
		}, []string{"event_type"}),

		dead: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "tickr_messages_dead_total",
			Help: "Total messages that landed in the dead-letter queue.",
		}, []string{"event_type"}),

		attempts: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "tickr_handler_attempts_total",
			Help: "Total handler attempts categorised by outcome.",
		}, []string{"event_type", "outcome"}),

		duration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "tickr_handler_duration_seconds",
			Help:    "Handler duration in seconds.",
			Buckets: prometheus.DefBuckets,
		}, []string{"event_type", "outcome"}),

		queueDepth: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "tickr_queue_depth",
			Help: "Current count of messages per (event_type, status).",
		}, []string{"event_type", "status"}),

		claimBatchSz: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "tickr_claim_batch_size",
			Help:    "Number of messages returned by each claim query.",
			Buckets: []float64{0, 1, 5, 10, 25, 50, 100, 200, 500, 1000},
		}),

		claimDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "tickr_claim_duration_seconds",
			Help:    "Time taken by each claim query.",
			Buckets: []float64{0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 5},
		}),

		leaseReclaim: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "tickr_leases_reclaimed_total",
			Help: "Total expired leases reclaimed by the reclaimer.",
		}, []string{"event_type"}),

		inflight: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "tickr_inflight_handlers",
			Help: "Currently-running handler attempts.",
		}, []string{"event_type"}),
	}
	reg.MustRegister(m.enqueued, m.dead, m.attempts, m.duration,
		m.queueDepth, m.claimBatchSz, m.claimDuration,
		m.leaseReclaim, m.inflight)
	return m
}

// MessageEnqueued implements tickr.Metrics.
func (m *Metrics) MessageEnqueued(eventType string) {
	m.enqueued.WithLabelValues(eventType).Inc()
}

// HandlerStarted implements tickr.Metrics.
func (m *Metrics) HandlerStarted(eventType string, _ int) {
	m.inflight.WithLabelValues(eventType).Inc()
}

// HandlerCompleted implements tickr.Metrics.
func (m *Metrics) HandlerCompleted(eventType string, _ int, duration time.Duration, outcome tickr.Outcome) {
	m.inflight.WithLabelValues(eventType).Dec()
	m.attempts.WithLabelValues(eventType, string(outcome)).Inc()
	m.duration.WithLabelValues(eventType, string(outcome)).Observe(duration.Seconds())
}

// MessageDead implements tickr.Metrics.
func (m *Metrics) MessageDead(eventType string) {
	m.dead.WithLabelValues(eventType).Inc()
}

// QueueDepth implements tickr.Metrics.
func (m *Metrics) QueueDepth(eventType string, status tickr.Status, depth int) {
	m.queueDepth.WithLabelValues(eventType, string(status)).Set(float64(depth))
}

// ClaimBatch implements tickr.Metrics.
func (m *Metrics) ClaimBatch(claimed, _ int, duration time.Duration) {
	m.claimBatchSz.Observe(float64(claimed))
	m.claimDuration.Observe(duration.Seconds())
}

// LeaseReclaimed implements tickr.Metrics.
func (m *Metrics) LeaseReclaimed(eventType string, count int) {
	m.leaseReclaim.WithLabelValues(eventType).Add(float64(count))
}

// Ensure Metrics satisfies tickr.Metrics.
var _ tickr.Metrics = (*Metrics)(nil)
