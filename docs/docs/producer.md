---
id: producer
title: Producer
sidebar_position: 3
---

# Producer (Client)

`tickr.Client` writes outbox rows through the configured `Storage` adapter, optionally participating in the caller's transaction.

## NewClient

```go
client, err := tickr.NewClient(tickr.ClientConfig{
    Storage: store, // required
})
```

`ClientConfig` fields:

| Field | Type | Default | Purpose |
|---|---|---|---|
| `Storage` | `tickr.Storage` | — (required) | Adapter — typically `pgstore.New(pool)` |
| `DefaultMaxAttempts` | `int` | `10` | Used when a `Message.MaxAttempts` is zero |
| `Logger` | `tickr.Logger` | nop | Structured logger |
| `Metrics` | `tickr.Metrics` | nop | Metrics hook — see [Observability](./observability.md) |
| `Tracer` | `tickr.Tracer` | nop | Tracer hook — see [Observability](./observability.md) |

## Enqueue

```go
func (c *Client) Enqueue(ctx context.Context, tx Tx, msg Message) (MessageID, error)
```

`tx` may be the zero value (no caller transaction) — the adapter does a single auto-commit. To get the outbox guarantee, pass `pgstore.WrapTx(tx)` so the row commits with your business state.

### Message fields

| Field | Required | Notes |
|---|---|---|
| `Type` | yes | Routing key — must match a registered handler |
| `Payload` | no | Opaque body. Encoding is your choice; use `tickr.Encode` for JSON |
| `Headers` | no | `map[string]string`. W3C `traceparent` / `tracestate` injected automatically when a `Tracer` is configured |
| `IdempotencyKey` | no | Makes `(Type, IdempotencyKey)` unique across the outbox |
| `ScheduledAt` | no | Eligible only after this instant. Zero ⇒ immediately |
| `MaxAttempts` | no | Per-message override; zero ⇒ inherit handler default |

### Idempotency

When `(Type, IdempotencyKey)` already exists, `Enqueue` returns `*ErrDuplicate` carrying the existing `MessageID`. Treat it as success:

```go
id, err := client.Enqueue(ctx, tx, msg)
if err != nil && !tickr.IsDuplicate(err) {
    return err
}
// id is the existing or newly-created MessageID either way.
```

### Notify-on-commit

When the storage adapter implements `Notifier` (Postgres does), `Enqueue` fires a best-effort `NOTIFY` so subscribed workers wake up immediately on commit. Notifications **are only delivered after the caller transaction commits** — the outbox guarantee is preserved.

## EnqueueBatch

```go
func (c *Client) EnqueueBatch(ctx context.Context, tx Tx, msgs []Message) ([]MessageID, error)
```

Writes many messages in one round-trip. The returned slice is parallel to `msgs`.

```go
ids, err := client.EnqueueBatch(ctx, pgstore.WrapTx(tx), []tickr.Message{
    {Type: "order.created", Payload: p1, IdempotencyKey: "o-1"},
    {Type: "order.created", Payload: p2, IdempotencyKey: "o-2"},
    {Type: "audit.event",   Payload: p3},
})
```

Duplicates in the batch return their existing IDs alongside non-duplicate inserts — same semantics as the single-message form.

## Delayed messages

Set `ScheduledAt` to defer eligibility:

```go
_, _ = client.Enqueue(ctx, pgstore.WrapTx(tx), tickr.Message{
    Type:        "reminder.send",
    Payload:     payload,
    ScheduledAt: time.Now().Add(24 * time.Hour),
})
```

The claim query filters by `process_at <= now()` — delayed rows simply skip the claim window.

## Trace propagation

When `ClientConfig.Tracer` is set (typically `tracing/otel`), the W3C `traceparent` and `tracestate` headers are injected into `Message.Headers` at enqueue time. The consumer side extracts them at dispatch, so **a single trace spans producer → outbox → consumer across processes**, even if the consumer runs hours later.

See [Observability → OpenTelemetry](./observability.md#opentelemetry).

## Encode helper

```go
func Encode(body any) ([]byte, error)
```

Thin wrapper around `encoding/json.Marshal`. The symmetric `tickr.On[T]` decodes with `encoding/json` — using `Encode` keeps producer and consumer in sync.

```go
payload, err := tickr.Encode(order)
if err != nil { return err }
```

## Errors

| Sentinel / type | Returned when |
|---|---|
| `*ErrDuplicate` | `(Type, IdempotencyKey)` already exists. Check with `tickr.IsDuplicate(err)` |
| `error("Message.Type required")` | `Message.Type == ""` |
| Adapter errors | Wrapped storage errors — unchanged |
