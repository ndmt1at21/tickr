---
title: Getting started
nav_order: 2
permalink: /getting-started/
---

# Getting started
{: .no_toc }

End-to-end: producer enqueues, worker dispatches, PostgreSQL backs the outbox. ~10 minutes.

<details open markdown="block">
  <summary>Table of contents</summary>
  {: .text-delta }
- TOC
{:toc}
</details>

## 1. Install

```bash
go get github.com/ndmt1at21/tickr
go get github.com/ndmt1at21/tickr/storage/postgres
```

You need PostgreSQL 12+ (uses `SELECT … FOR UPDATE SKIP LOCKED` and `LISTEN/NOTIFY`).

## 2. Run migrations

The Postgres adapter embeds its DDL — no external migration tool required.

```go
pool, err := pgxpool.New(ctx, dsn)
if err != nil { return err }

store := pgstore.New(pool)
mig, _ := tickr.NewMigrator(store)
if err := mig.Up(ctx); err != nil { return err }
```

This creates `tickr_messages`, `tickr_history`, and supporting indexes.

## 3. Producer side

Enqueue **inside your own transaction** so the outbox row is committed atomically with your business state.

```go
client, _ := tickr.NewClient(tickr.ClientConfig{Storage: store})

tx, _ := pool.Begin(ctx)
defer tx.Rollback(ctx)

if _, err := tx.Exec(ctx, `INSERT INTO orders (id, total) VALUES ($1, $2)`,
    order.ID, order.Total); err != nil {
    return err
}

payload, _ := tickr.Encode(order)
_, err := client.Enqueue(ctx, pgstore.WrapTx(tx), tickr.Message{
    Type:           "order.created",
    Payload:        payload,
    IdempotencyKey: order.ID,
})
if err != nil && !tickr.IsDuplicate(err) {
    return err
}

return tx.Commit(ctx)
```

{: .tip }
> Re-running the same idempotency key after a network retry returns `*ErrDuplicate` carrying the existing `MessageID` — treat it as success.

See [Producer]({{ '/producer/' | relative_url }}) for `EnqueueBatch`, delayed messages, and trace propagation.

## 4. Consumer side

Register a typed handler — payload is JSON-decoded into your struct before dispatch.

```go
type OrderCreated struct {
    ID    string  `json:"id"`
    Total float64 `json:"total"`
}

reg := tickr.NewRegistry()

_ = tickr.On(reg, "order.created",
    func(ctx context.Context, msg *tickr.InboundMessage, body OrderCreated) error {
        return chargeCustomer(ctx, body)
    },
    tickr.WithMaxAttempts(5),
    tickr.WithAttemptTimeout(10*time.Second),
)
```

Start the worker:

```go
w, _ := tickr.NewWorker(tickr.WorkerConfig{
    Storage:  store,
    Registry: reg,
    Stats:    tickr.StatsPolicy{Interval: 10 * time.Second},
})

// Blocks until ctx is cancelled; drains in-flight handlers within ShutdownGrace.
_ = w.Start(ctx)
```

A malformed payload is dead-lettered immediately (it won't decode on retry either) — see [Consumer]({{ '/consumer/' | relative_url }}).

## 5. Graceful shutdown

```go
ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
defer cancel()

if err := w.Start(ctx); err != nil {
    log.Fatal(err)
}
```

In-flight handlers receive `ctx.Done()` and the claim is released **without** burning an attempt — the message is requeued for the next worker.

## 6. Run the example

The repo ships a full demo (HTTP service + outbox + worker + Postgres + Prometheus + Grafana + Tempo):

```bash
cd examples/orders
docker compose up --build
```

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

## Next

- [Producer]({{ '/producer/' | relative_url }}) — Client API, batch enqueue, delayed messages
- [Consumer]({{ '/consumer/' | relative_url }}) — Registry, batch handlers, retry / dead-letter / skip
- [Configuration]({{ '/configuration/' | relative_url }}) — every knob, with defaults
- [Observability]({{ '/observability/' | relative_url }}) — Prometheus, OpenTelemetry, Grafana
