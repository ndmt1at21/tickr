# tickr

[![CI](https://github.com/ndmt1at21/tickr/actions/workflows/ci.yml/badge.svg)](https://github.com/ndmt1at21/tickr/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/ndmt1at21/tickr.svg)](https://pkg.go.dev/github.com/ndmt1at21/tickr)
[![Go Report Card](https://goreportcard.com/badge/github.com/ndmt1at21/tickr)](https://goreportcard.com/report/github.com/ndmt1at21/tickr)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![Release](https://img.shields.io/github/v/release/ndmt1at21/tickr?display_name=tag&sort=semver)](https://github.com/ndmt1at21/tickr/releases)

Reliable async messaging for Go microservices. Implements the **transactional outbox pattern** without an external broker — the storage table *is* the queue, and registered handlers run in-process via a horizontally-scalable worker pool.

- **At-least-once delivery** with idempotent handlers
- **Transactional outbox**: enqueue inside your own DB transaction
- **Exponential backoff with jitter**, configurable per handler
- **Status machine** (`CREATED → HANDLING → SUCCESS / FAILED → RETRYING → DEAD`) with per-message history
- **Delayed messages**, **dead-letter queue**, **idempotency-key dedup**, **per-event-type concurrency limits**
- **Configurable poll interval and retention**
- **First-class observability**: Prometheus metrics, OpenTelemetry tracing with W3C propagation, ready-to-import Grafana dashboard
- **Pluggable storage** — v1 ships a PostgreSQL adapter (pgx/v5, `SELECT … FOR UPDATE SKIP LOCKED`)

Target throughput: **1M messages/minute** across a horizontally scaled fleet.

## Layout

```
tickr/                       core API (Client, Worker, types, Storage interface)
  storage/postgres/          PostgreSQL adapter + embedded migrations
  codec/json/                low-level json codec (most users want tickr.On[T])
  metrics/prom/              Prometheus implementation of tickr.Metrics
  tracing/otel/              OpenTelemetry implementation of tickr.Tracer
  grafana/                   ready-to-import dashboard JSON
  examples/orders/           HTTP service + outbox + worker demo
```

## Producer (write side)

```go
pool, _ := pgxpool.New(ctx, dsn)
store   := pgstore.New(pool)
client, _ := tickr.NewClient(tickr.ClientConfig{Storage: store})

tx, _ := pool.Begin(ctx)
defer tx.Rollback(ctx)

if _, err := tx.Exec(ctx, `INSERT INTO orders …`); err != nil { return err }

payload, _ := tickr.Encode(order)
_, err := client.Enqueue(ctx, pgstore.WrapTx(tx), tickr.Message{
    Type:           "order.created",
    Payload:        payload,
    IdempotencyKey: order.ID,
})
if err != nil && !tickr.IsDuplicate(err) { return err }

return tx.Commit(ctx)
```

## Consumer (worker side)

`tickr.On[T]` registers a typed handler — the payload is JSON-decoded
into `T` before your function runs. A malformed payload is dead-lettered
immediately (it won't decode on retry either).

```go
reg := tickr.NewRegistry()
_ = tickr.On(reg, "order.created",
    func(ctx context.Context, msg *tickr.InboundMessage, body OrderCreated) error {
        return chargeCustomer(ctx, body)
    },
    tickr.WithMaxAttempts(5),
    tickr.WithAttemptTimeout(10*time.Second),
)

w, _ := tickr.NewWorker(tickr.WorkerConfig{
    Storage:  store,
    Registry: reg,
    Stats:    tickr.StatsPolicy{Interval: 10 * time.Second},
})
_ = w.Start(ctx) // blocks until ctx is cancelled
```

Drop down to `reg.On(eventType, tickr.Handler, …)` when you need raw
`[]byte` access or a non-JSON codec.

### Batch handlers

Process same-type messages in groups when the downstream work batches
naturally (bulk inserts, bulk-API calls). `tickr.OnBatch[T]` registers an
all-or-nothing handler: returning `nil` marks every message in the batch
SUCCESS; a non-nil error fails the whole batch with the same error
(each message retries on its own attempt count).

```go
_ = tickr.OnBatch(reg, "order.created",
    func(ctx context.Context, batch []tickr.BatchItem[OrderCreated]) error {
        rows := make([]OrderCreated, len(batch))
        for i, it := range batch {
            rows[i] = it.Body
        }
        return db.BulkInsert(ctx, rows)
    },
    tickr.WithMaxBatchSize(100),
    tickr.WithAttemptTimeout(30*time.Second),
)
```

The worker groups same-type messages from each claim cycle and chunks
them by `WithMaxBatchSize` (zero = the whole group in one call). Use
`reg.OnBatch(eventType, tickr.BatchHandler, …)` for raw `[]byte` access.

### Built-in transport handlers

Two subpackages skip the boilerplate when the handler's only job is to
forward the message to a downstream service. Each lives in its own Go
module so its transport-specific deps stay out of the core `go.mod`.

#### HTTP webhook — [`handlers/http`](handlers/http/)

```go
import httphandler "github.com/ndmt1at21/tickr/handlers/http"

_ = tickr.On(reg, "email.send",
    httphandler.PostJSON[Email](http.DefaultClient, httphandler.Config[Email]{
        URLFunc: func(_ context.Context, _ *tickr.InboundMessage, e Email) string {
            return e.HookURL
        },
    }),
    tickr.WithMaxAttempts(8),
    tickr.WithAttemptTimeout(15*time.Second),
)
```

Defaults: 2xx → success; 4xx (except 408/425/429) → DeadLetter;
408/425/429 + 5xx → retry (Retry-After honored); transport errors →
retry. The message's `Headers` (W3C traceparent included) and
`IdempotencyKey` are forwarded as HTTP headers automatically.
Override classification or header forwarding via [`Config`](handlers/http/handler.go).

#### gRPC unary — [`handlers/grpc`](handlers/grpc/)

```go
import grpchandler "github.com/ndmt1at21/tickr/handlers/grpc"

client := userpb.NewUserServiceClient(conn)

_ = tickr.On(reg, "user.signup",
    grpchandler.Unary(client.Notify, grpchandler.Config[*userpb.NotifyRequest]{}),
    tickr.WithMaxAttempts(5),
    tickr.WithAttemptTimeout(10*time.Second),
)
```

Defaults follow gRPC retry semantics: `UNAVAILABLE`, `DEADLINE_EXCEEDED`,
`RESOURCE_EXHAUSTED`, `ABORTED` retry; `INVALID_ARGUMENT`, `NOT_FOUND`,
`PERMISSION_DENIED`, `UNAUTHENTICATED`, `ALREADY_EXISTS`,
`FAILED_PRECONDITION`, `OUT_OF_RANGE`, `UNIMPLEMENTED` DeadLetter;
`INTERNAL` / `UNKNOWN` / `DATA_LOSS` retry by default (override via
`Config.Classifier`). Trace context and idempotency key are attached as
outgoing gRPC metadata.

## Migrations

```go
m, _ := tickr.NewMigrator(store)
_ = m.Up(ctx)
```

The Postgres adapter embeds its SQL via `embed.FS`; no external migration tool required.

## Status machine

```
CREATED  ──claim──▶  HANDLING ──nil───▶  SUCCESS
                       │
                       ├─ err (attempt<max) ─▶  FAILED ─▶ RETRYING ─▶ HANDLING
                       │
                       ├─ err (attempt==max) ▶  FAILED ─▶ DEAD
                       │
                       ├─ DeadLetter() ──────▶  DEAD
                       │
                       └─ ctx.Canceled (shutdown) ▶ CREATED|RETRYING  (no attempt burn)

DEAD ── admin Requeue ──▶ CREATED                  (manual recovery)
```

Every transition appends a row to `tickr_history`.

## Failure handling

| Scenario | Mechanism |
|---|---|
| Handler returns error (attempt < max) | scheduled `RETRYING` with exponential backoff |
| Handler returns error (attempt == max) | terminates in `DEAD` |
| Handler returns `tickr.DeadLetter(err)` | jumps to `DEAD` without retry |
| Handler returns `tickr.RetryAfter(d, err)` | overrides RetryPolicy with explicit delay |
| Handler returns `tickr.Skip(reason)` | `SUCCESS` without side effects |
| Handler panics | recovered, treated as `error`, normal retry path |
| Handler timeout (`WithAttemptTimeout`) | normal retry path |
| Worker graceful shutdown (SIGTERM) | inflight handlers get `ctx.Done()`, claim released without burning attempt |
| Worker hard crash (SIGKILL) | lease expires → reclaimer transitions back to `RETRYING`, attempt **stays incremented** (poison-pill protection) |

### Lease auto-extension

`WithAttemptTimeout` can exceed the worker's `Lease` safely — the engine extends the storage lease every `Lease/3` while the handler runs. If the lease is lost to another worker, the in-flight attempt's `ctx` is cancelled so duplicate work is avoided.

## Observability

### Prometheus

```go
import (
    "github.com/prometheus/client_golang/prometheus"
    "github.com/prometheus/client_golang/prometheus/promhttp"
    tprom "github.com/ndmt1at21/tickr/metrics/prom"
)

reg := prometheus.NewRegistry()
m   := tprom.New(reg)

// ... pass m as ClientConfig.Metrics and WorkerConfig.Metrics
http.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
```

Metrics emitted:

| Metric | Labels |
|---|---|
| `tickr_messages_enqueued_total` | `event_type` |
| `tickr_handler_attempts_total` | `event_type`, `outcome` |
| `tickr_handler_duration_seconds` | `event_type`, `outcome` |
| `tickr_messages_dead_total` | `event_type` |
| `tickr_queue_depth` | `event_type`, `status` |
| `tickr_claim_batch_size` | — |
| `tickr_claim_duration_seconds` | — |
| `tickr_leases_reclaimed_total` | `event_type` |
| `tickr_inflight_handlers` | `event_type` |

### OpenTelemetry tracing

```go
import totel "github.com/ndmt1at21/tickr/tracing/otel"

tracer := totel.New() // picks up the global TracerProvider + propagator
```

The tracer injects W3C `traceparent` and `tracestate` into `Message.Headers` at enqueue time, and extracts them at handler dispatch — so a single trace spans **producer → outbox → consumer** across processes, even if the consumer runs hours later.

Span attributes follow the OpenTelemetry messaging semantic conventions.

### Grafana

Import `grafana/tickr-dashboard.json`. See [grafana/README.md](grafana/README.md) for scrape config, alert rules, and Tempo/Jaeger deep-links by `messaging.message.id`.

## Throughput

For head-to-head numbers against [River](https://github.com/riverqueue/river),
[Gue](https://github.com/vgarvardt/gue),
[Watermill SQL](https://github.com/ThreeDotsLabs/watermill-sql), and
[Asynq](https://github.com/hibiken/asynq) on identical workloads, see
[BENCHMARKS.md](BENCHMARKS.md). The bench code lives in
[`benchmarks/`](benchmarks/) as a separate Go module.

For 1M msg/min (16.7k/sec) sustained, the recommended baseline configuration is:

| Knob | Value |
|---|---|
| Fleet × claim goroutines | 30 × 4 = 120 |
| `BatchSize` | 200 |
| `PollInterval` | 100 ms with adaptive backoff to 2 s on empty |
| `Lease` | 30 s with handler auto-extension |
| PG pool per instance | 8 connections (240 total — front with PgBouncer) |

The bottleneck is **not** `SKIP LOCKED` itself but autovacuum keeping up with dead tuples on the hot table. Tune:

```sql
ALTER TABLE tickr_messages SET (
    autovacuum_vacuum_scale_factor = 0.02,
    autovacuum_naptime             = 10
);
```

## Run the example

```bash
cd examples/orders
docker compose up --build
```

This brings up Postgres, Prometheus, Grafana, Tempo, and the orders service. Then:

```bash
curl -X POST http://localhost:8080/orders \
  -H 'content-type: application/json' \
  -d '{"order_id":"o-1","customer_id":"c-1","total":42.50}'
```

Observe:
- Logs: handler invocation in the orders container
- Postgres: `tickr_messages` row transitions, `tickr_history` rows
- Grafana (`http://localhost:3000`): throughput, latency, queue depth
- Tempo: end-to-end span from HTTP request → enqueue → handler

## Limitations

Before adopting, read [ARCHITECTURE.md §11](ARCHITECTURE.md#11-limitations).
Highlights:

- Producer and outbox must share a database (the outbox guarantee
  requires a single transaction).
- At-least-once delivery only — handlers must be idempotent.
- Partitioned Postgres scopes idempotency per partition (per month by
  default).
- MySQL and CockroachDB adapters do not implement `LISTEN/NOTIFY` and
  fall back to pure polling (~100 ms typical wake-up).
- `tickr_history` retention is currently manual.

Section 11.3 lists planned follow-ups; contributions welcome.

## Architecture

See [ARCHITECTURE.md](ARCHITECTURE.md) for the design deep-dive: status
machine, claim query, lease auto-extension, partitioning trade-offs,
leader-lock strategies per adapter, and the conformance suite.

## License

Released under the [MIT License](LICENSE).
