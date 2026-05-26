---
title: Home
layout: home
nav_order: 1
---

# tickr

Reliable async messaging for Go microservices. Implements the **transactional outbox pattern** without an external broker — the storage table *is* the queue, and registered handlers run in-process via a horizontally-scalable worker pool.

```bash
go get github.com/ndmt1at21/tickr
go get github.com/ndmt1at21/tickr/storage/postgres
```

## Features

- **At-least-once delivery** with idempotent handlers
- **Transactional outbox**: enqueue inside your own DB transaction
- **Exponential backoff with jitter**, configurable per handler
- **Status machine** (`CREATED → HANDLING → SUCCESS / FAILED → RETRYING → DEAD`) with per-message history
- **Delayed messages**, **dead-letter queue**, **idempotency-key dedup**, **per-event-type concurrency limits**
- **Configurable poll interval and retention**
- **First-class observability**: Prometheus metrics, OpenTelemetry tracing with W3C propagation, ready-to-import Grafana dashboard
- **Pluggable storage** — v1 ships a PostgreSQL adapter (pgx/v5, `SELECT … FOR UPDATE SKIP LOCKED`)

Target throughput: **1M messages/minute** across a horizontally scaled fleet.

## Where to start

| If you want to… | Read |
|---|---|
| Stand up a producer + worker in 10 minutes | [Getting started]({{ '/getting-started/' | relative_url }}) |
| Understand `Client.Enqueue` and the outbox guarantee | [Producer]({{ '/producer/' | relative_url }}) |
| Register handlers and tune the worker pool | [Consumer]({{ '/consumer/' | relative_url }}) |
| Forward messages to HTTP / gRPC without writing a handler | [Built-in handlers]({{ '/handlers/' | relative_url }}) |
| Look up every knob | [Configuration]({{ '/configuration/' | relative_url }}) |
| Wire Prometheus, OpenTelemetry, Grafana | [Observability]({{ '/observability/' | relative_url }}) |

## At a glance

```go
// Producer side — enqueue inside your business transaction.
tx, _ := pool.Begin(ctx)
defer tx.Rollback(ctx)

_, _ = tx.Exec(ctx, `INSERT INTO orders ...`)

payload, _ := tickr.Encode(order)
_, _ = client.Enqueue(ctx, pgstore.WrapTx(tx), tickr.Message{
    Type:           "order.created",
    Payload:        payload,
    IdempotencyKey: order.ID,
})
_ = tx.Commit(ctx)
```

```go
// Consumer side — typed handler, JSON payload decoded for you.
_ = tickr.On(reg, "order.created",
    func(ctx context.Context, msg *tickr.InboundMessage, body OrderCreated) error {
        return chargeCustomer(ctx, body)
    },
    tickr.WithMaxAttempts(5),
)
```

## Links

- **Source code**: <https://github.com/ndmt1at21/tickr>
- **Go reference**: <https://pkg.go.dev/github.com/ndmt1at21/tickr>
- **Architecture deep-dive**: [ARCHITECTURE.md](https://github.com/ndmt1at21/tickr/blob/main/ARCHITECTURE.md)
- **Benchmarks vs River / Gue / Watermill / Asynq**: [BENCHMARKS.md](https://github.com/ndmt1at21/tickr/blob/main/BENCHMARKS.md)
