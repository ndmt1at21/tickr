---
id: observability
title: Observability
sidebar_position: 7
---

# Observability

tickr ships first-class hooks for Prometheus, OpenTelemetry, and Grafana. Each integration lives in its own subpackage so its dependencies stay out of the core `go.mod`.

## Prometheus

```bash
go get github.com/ndmt1at21/tickr/metrics/prom
```

```go
import (
    "github.com/prometheus/client_golang/prometheus"
    "github.com/prometheus/client_golang/prometheus/promhttp"
    tprom "github.com/ndmt1at21/tickr/metrics/prom"
)

reg := prometheus.NewRegistry()
m   := tprom.New(reg)

client, _ := tickr.NewClient(tickr.ClientConfig{
    Storage: store,
    Metrics: m,
})
w, _ := tickr.NewWorker(tickr.WorkerConfig{
    Storage:  store,
    Registry: handlerReg,
    Metrics:  m,
    Stats:    tickr.StatsPolicy{Interval: 10 * time.Second}, // enables QueueDepth sampling
})

http.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
```

### Emitted metrics

| Metric | Labels | Notes |
|---|---|---|
| `tickr_messages_enqueued_total` | `event_type` | Counter |
| `tickr_handler_attempts_total` | `event_type`, `outcome` | `outcome ∈ {success, retry, dead, canceled}` |
| `tickr_handler_duration_seconds` | `event_type`, `outcome` | Histogram |
| `tickr_messages_dead_total` | `event_type` | Counter |
| `tickr_queue_depth` | `event_type`, `status` | Gauge — only emitted when `Stats.Interval > 0` |
| `tickr_claim_batch_size` | — | Histogram |
| `tickr_claim_duration_seconds` | — | Histogram |
| `tickr_leases_reclaimed_total` | `event_type` | Counter |
| `tickr_inflight_handlers` | `event_type` | Gauge |

### Recommended alerts

| Alert | PromQL |
|---|---|
| Dead-letter rate spiking | `rate(tickr_messages_dead_total[5m]) > 0.1` |
| Queue backing up | `tickr_queue_depth{status="CREATED"} > 10000` |
| Reclaimer churning (likely worker crashes) | `rate(tickr_leases_reclaimed_total[5m]) > 1` |
| Handler p99 latency | `histogram_quantile(0.99, sum(rate(tickr_handler_duration_seconds_bucket[5m])) by (le, event_type))` |

## OpenTelemetry

```bash
go get github.com/ndmt1at21/tickr/tracing/otel
```

```go
import totel "github.com/ndmt1at21/tickr/tracing/otel"

tracer := totel.New() // picks up the global TracerProvider + propagator

client, _ := tickr.NewClient(tickr.ClientConfig{
    Storage: store,
    Tracer:  tracer,
})
w, _ := tickr.NewWorker(tickr.WorkerConfig{
    Storage:  store,
    Registry: reg,
    Tracer:   tracer,
})
```

### How propagation works

1. At enqueue time, the tracer **injects** W3C `traceparent` / `tracestate` into `Message.Headers` using the global propagator.
2. The adapter stores those headers alongside the row.
3. At dispatch, the worker **extracts** the headers back into `ctx`, parenting the handler span on the producer's span.

Result: **one trace spans producer → outbox → consumer**, even when the consumer runs hours later in a different process.

### Span attributes

Spans follow the [OpenTelemetry messaging semantic conventions](https://opentelemetry.io/docs/specs/semconv/messaging/messaging-spans/):

| Attribute | Value |
|---|---|
| `messaging.system` | `tickr` |
| `messaging.destination.name` | `Message.Type` |
| `messaging.operation` | `publish` / `process` |
| `messaging.message.id` | `MessageID` |

The `messaging.message.id` is the same on producer and consumer spans, so you can deep-link from Tempo / Jaeger directly to the row.

## Grafana

Import [`grafana/tickr-dashboard.json`](https://github.com/ndmt1at21/tickr/blob/main/grafana/tickr-dashboard.json). The dashboard has panels for:

- Throughput (enqueued, success, dead per second)
- Handler latency (p50 / p95 / p99 per event type)
- Queue depth by status
- Lease reclaim activity
- In-flight handlers per event type

See [grafana/README.md](https://github.com/ndmt1at21/tickr/blob/main/grafana/README.md) for scrape config, alert rules, and Tempo/Jaeger deep-links by `messaging.message.id`.

## Logger

Implement the `tickr.Logger` interface to plug in `slog`, `zap`, `zerolog`, etc.:

```go
type Logger interface {
    Debug(ctx context.Context, msg string, kv ...any)
    Info(ctx context.Context, msg string, kv ...any)
    Warn(ctx context.Context, msg string, kv ...any)
    Error(ctx context.Context, msg string, err error, kv ...any)
}
```

A minimal `slog` adapter:

```go
type slogLogger struct{ l *slog.Logger }

func (s slogLogger) Debug(ctx context.Context, msg string, kv ...any) { s.l.DebugContext(ctx, msg, kv...) }
func (s slogLogger) Info(ctx context.Context, msg string, kv ...any)  { s.l.InfoContext(ctx, msg, kv...) }
func (s slogLogger) Warn(ctx context.Context, msg string, kv ...any)  { s.l.WarnContext(ctx, msg, kv...) }
func (s slogLogger) Error(ctx context.Context, msg string, err error, kv ...any) {
    s.l.ErrorContext(ctx, msg, append(kv, "err", err)...)
}
```

## Alerter

`Alerter` receives **background-loop errors** that can't be returned from a synchronous call (reclaimer failures, janitor failures, claim-loop failures). Wire it to PagerDuty / OpsGenie / Slack:

```go
type Alerter interface {
    Alert(ctx context.Context, ev ErrorEvent)
}

type ErrorEvent struct {
    Kind ErrorKind // ErrorKindClaim, ErrorKindReclaimer, ErrorKindJanitor
    Err  error
}
```

For routine handler errors (`error` returned from your handler), use the metrics + structured logs instead — those already carry `event_type` and `attempt`.
