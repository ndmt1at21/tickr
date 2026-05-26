---
title: Consumer
nav_order: 4
permalink: /consumer/
---

# Consumer (Worker + Registry)
{: .no_toc }

The worker side runs a claim loop, dispatches handlers, auto-extends leases, and (when leader) runs the reclaimer / janitor / stats loops.

<details open markdown="block">
  <summary>Table of contents</summary>
  {: .text-delta }
- TOC
{:toc}
</details>

## Registry

A registry maps event types to handlers and their per-event-type options.

```go
reg := tickr.NewRegistry()
```

Mutation **after** `Worker.Start` is not supported — register everything during init.

### tickr.On — typed single-message handler

```go
func On[T any](
    r *HandlerRegistry,
    eventType string,
    h TypedHandler[T],
    opts ...HandlerOption,
) error

type TypedHandler[T any] func(ctx context.Context, msg *InboundMessage, body T) error
```

The payload is decoded with `encoding/json` before your function runs. A malformed payload is dead-lettered immediately — it won't decode on retry either.

```go
_ = tickr.On(reg, "order.created",
    func(ctx context.Context, msg *tickr.InboundMessage, body OrderCreated) error {
        return chargeCustomer(ctx, body)
    },
    tickr.WithMaxAttempts(5),
    tickr.WithAttemptTimeout(10*time.Second),
)
```

Use `tickr.MustOn` to panic on registration error (handy in `init`).

### reg.On — raw `[]byte` handler

When you need a custom codec or raw bytes:

```go
_ = reg.On("order.created",
    func(ctx context.Context, msg *tickr.InboundMessage) error {
        var o OrderCreated
        if err := proto.Unmarshal(msg.Payload, &o); err != nil {
            return tickr.DeadLetter(err)
        }
        return chargeCustomer(ctx, o)
    },
)
```

### tickr.OnBatch — typed batch handler

Process same-type messages in groups. Returning `nil` marks every message `SUCCESS`; a non-nil error fails the whole batch (each retries on its own attempt count).

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

The worker groups same-type messages from each claim cycle and chunks them by `WithMaxBatchSize` (zero = the whole group in one call).

Use `reg.OnBatch(eventType, tickr.BatchHandler, …)` when you need raw bytes.

## HandlerOption

| Option | Default | Purpose |
|---|---|---|
| `WithMaxAttempts(n int)` | `10` | Per-event-type retry budget |
| `WithRetryPolicy(p RetryPolicy)` | `ExponentialBackoff{Base: 1s, Max: 1h, JitterFraction: 0.2}` | Custom backoff |
| `WithAttemptTimeout(d time.Duration)` | `30s` | Per-attempt deadline. May exceed `WorkerConfig.Lease` — engine auto-extends |
| `WithMaxInflight(n int)` | unlimited | Per-event-type concurrency cap **within one worker process** |
| `WithMaxBatchSize(n int)` | whole group | Caps `BatchHandler` invocations (no effect on single-message handlers) |
| `WithDeadLetterIf(pred func(error) bool)` | nil | Predicate that, when true, sends a message straight to `DEAD` |

## Worker

```go
w, err := tickr.NewWorker(tickr.WorkerConfig{
    Storage:  store,    // required
    Registry: reg,      // required
})
if err != nil { return err }

if err := w.Start(ctx); err != nil { // blocks
    return err
}
```

`Worker.Stop(ctx)` cancels the worker's context; the blocking drain happens inside `Start` bounded by `ShutdownGrace`.

See [Configuration → WorkerConfig]({{ '/configuration/#workerconfig' | relative_url }}) for the full knob reference.

## Handler return semantics

```go
type Handler func(ctx context.Context, msg *InboundMessage) error
```

| Return | Effect |
|---|---|
| `nil` | `SUCCESS` |
| Generic `error` (attempt < max) | `FAILED → RETRYING` with backoff |
| Generic `error` (attempt == max) | `FAILED → DEAD` |
| `tickr.DeadLetter(err)` | Straight to `DEAD`, no retry burn |
| `tickr.RetryAfter(d, err)` | Overrides `RetryPolicy` with explicit delay |
| `tickr.Skip(reason)` | `SUCCESS` without side effects; reason logged in history |
| Panic | Recovered, treated as `error` |
| `ctx.Done()` (shutdown) | Claim released, attempt **not** burned |

### Error helpers

```go
return tickr.DeadLetter(fmt.Errorf("payload invalid: %w", err))
return tickr.RetryAfter(30*time.Second, errors.New("rate-limited"))
return tickr.Skip("already processed by legacy job")
```

Inspect with `IsDeadLetter`, `ExtractRetryAfter`, `IsSkip`.

## InboundMessage

```go
type InboundMessage struct {
    ID             MessageID
    Type           string
    Payload        []byte
    Headers        map[string]string
    IdempotencyKey string

    Attempt     int // 1-indexed; incremented at claim time before dispatch
    MaxAttempts int

    EnqueuedAt  time.Time
    ScheduledAt time.Time

    LastError string // previous attempt's error, or "" on first attempt
    Status    Status // always StatusHandling on the hot path
}
```

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

## Lease auto-extension

`WithAttemptTimeout` can safely exceed `WorkerConfig.Lease` — the engine extends the storage lease every `Lease/3` while the handler runs. If the lease is lost to another worker, the in-flight attempt's `ctx` is cancelled so duplicate work is avoided.

## Graceful vs. hard shutdown

| Event | Behaviour |
|---|---|
| `SIGTERM` (ctx cancelled) | In-flight handlers get `ctx.Done()`. Claim released **without** burning the attempt. Bounded by `WorkerConfig.ShutdownGrace` (default 30s) |
| `SIGKILL` / crash | Lease expires → reclaimer transitions back to `RETRYING`. Attempt **stays incremented** (poison-pill protection) |

## Reclaimer, janitor, stats

The worker runs three optional leader-elected background loops:

| Loop | Purpose | Disable |
|---|---|---|
| Reclaimer | Transitions expired-lease messages back to `RETRYING` | `WorkerConfig.DisableReclaimer = true` |
| Janitor | Purges terminal rows older than `RetentionPolicy` | `WorkerConfig.DisableJanitor = true` |
| Stats | Samples queue depth into Prometheus | `WorkerConfig.Stats.Interval = 0` |

Leader election uses `Storage.TryLeaderLock` (Postgres: advisory locks) so only one worker per fleet runs each loop.
