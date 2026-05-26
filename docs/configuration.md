---
title: Configuration
nav_order: 6
permalink: /configuration/
---

# Configuration reference
{: .no_toc }

Every knob, with its default and what it controls.

<details open markdown="block">
  <summary>Table of contents</summary>
  {: .text-delta }
- TOC
{:toc}
</details>

## ClientConfig

```go
client, _ := tickr.NewClient(tickr.ClientConfig{...})
```

| Field | Type | Default | Purpose |
|---|---|---|---|
| `Storage` | `tickr.Storage` | — (required) | Adapter — typically `pgstore.New(pool)` |
| `DefaultMaxAttempts` | `int` | `10` | Fallback when `Message.MaxAttempts` is zero |
| `Logger` | `tickr.Logger` | nop | Structured logger |
| `Metrics` | `tickr.Metrics` | nop | Metrics hook |
| `Tracer` | `tickr.Tracer` | nop | Tracer hook |

## WorkerConfig

```go
w, _ := tickr.NewWorker(tickr.WorkerConfig{...})
```

### Required

| Field | Type | Purpose |
|---|---|---|
| `Storage` | `tickr.Storage` | Adapter — must match the producer's |
| `Registry` | `*HandlerRegistry` | Handlers populated before `Start` |

### Identity

| Field | Type | Default | Purpose |
|---|---|---|---|
| `WorkerID` | `string` | `hostname-pid` | Used in `tickr_history` rows for forensics |

### Claim loop

| Field | Type | Default | Purpose |
|---|---|---|---|
| `PollInterval` | `time.Duration` | `100ms` | Time between claim attempts when no work was found |
| `PollMaxBackoff` | `time.Duration` | `2s` | Adaptive backoff ceiling on consecutive empty polls |
| `BatchSize` | `int` | `100` | Max messages claimed per poll cycle |
| `PoolSize` | `int` | `32` | Goroutines available to run handlers |
| `Lease` | `time.Duration` | `30s` | How long a claimed row is owned. Engine auto-extends every `Lease/3` |
| `ShutdownGrace` | `time.Duration` | `30s` | Max time `Start` blocks draining in-flight handlers |

### Background loops

| Field | Type | Default | Purpose |
|---|---|---|---|
| `ReclaimInterval` | `time.Duration` | `5s` | How often the leader tries to reclaim expired leases |
| `Retention` | `RetentionPolicy` | see below | Janitor settings |
| `Stats.Interval` | `time.Duration` | `0` (disabled) | Queue-depth sampling cadence |
| `DisableReclaimer` | `bool` | `false` | Skip the reclaimer loop entirely |
| `DisableJanitor` | `bool` | `false` | Skip the janitor (purge) loop |
| `DisableNotifier` | `bool` | `false` | Skip subscribing to `LISTEN/NOTIFY` even if the adapter supports it |

### Observability

| Field | Type | Default | Purpose |
|---|---|---|---|
| `Logger` | `tickr.Logger` | nop | Structured logger |
| `Metrics` | `tickr.Metrics` | nop | Metrics hook |
| `Tracer` | `tickr.Tracer` | nop | Tracer hook |
| `Alerter` | `tickr.Alerter` | nop | Background-loop error fan-out (reclaimer / janitor / claim) |

## RetentionPolicy

Controls how long terminal-state rows and history rows are kept.

| Field | Type | Default | Purpose |
|---|---|---|---|
| `Success` | `time.Duration` | `24h` | Age cutoff for purging `SUCCESS` rows |
| `Dead` | `time.Duration` | `30d` | Age cutoff for `DEAD`. **Negative ⇒ never purge** |
| `History` | `time.Duration` | `max(Success, Dead, 30d)` | Age cutoff for `tickr_history` rows. **Negative ⇒ never purge** |
| `PurgeBatch` | `int` | `5000` | Rows deleted per batch (tunes lock duration) |
| `PurgeEvery` | `time.Duration` | `1m` | How often the janitor wakes |

{: .note }
> `History` purging requires the storage adapter to implement `tickr.HistoryPurger`. Adapters that don't implement it skip this phase silently.

## HandlerOption

Per-event-type tuning passed at registration:

```go
tickr.On(reg, "event.type", h,
    tickr.WithMaxAttempts(5),
    tickr.WithAttemptTimeout(10*time.Second),
)
```

| Option | Default | Purpose |
|---|---|---|
| `WithMaxAttempts(n int)` | `10` | Retry budget for this event type |
| `WithRetryPolicy(p RetryPolicy)` | `ExponentialBackoff{Base:1s, Max:1h, JitterFraction:0.2}` | Custom backoff |
| `WithAttemptTimeout(d time.Duration)` | `30s` | Per-attempt deadline. May exceed `Lease` — engine auto-extends |
| `WithMaxInflight(n int)` | unlimited | Concurrency cap per worker process |
| `WithMaxBatchSize(n int)` | whole group | Caps `BatchHandler` invocations |
| `WithDeadLetterIf(pred func(error) bool)` | nil | Predicate that, when true, sends a message straight to `DEAD` |

## RetryPolicy

Implement to customise backoff:

```go
type RetryPolicy interface {
    NextDelay(attempt int, err error) time.Duration
}
```

The default is `ExponentialBackoff{Base: 1s, Max: 1h, JitterFraction: 0.2}`:

```
delay = min(Max, Base * 2^(attempt-1)) * (1 + spread)
```

where `spread ∈ [-JitterFraction, +JitterFraction]`.

| Field | Default | Notes |
|---|---|---|
| `Base` | `1s` | Delay before the second attempt |
| `Max` | `1h` | Caps the computed delay |
| `JitterFraction` | `0.2` | Clamped to `[0, 1]`. Zero disables jitter |
| `Rand` | `math/rand/v2` | Override for deterministic tests |

## 1M msg/min baseline

| Knob | Value |
|---|---|
| Fleet × claim goroutines | 30 × 4 = 120 |
| `BatchSize` | 200 |
| `PollInterval` | 100 ms with adaptive backoff to 2 s on empty |
| `Lease` | 30 s with handler auto-extension |
| PG pool per instance | 8 connections (240 total — front with PgBouncer) |

The bottleneck is **not** `SKIP LOCKED` itself but autovacuum keeping up with dead tuples on the hot table. Tune Postgres:

```sql
ALTER TABLE tickr_messages SET (
    autovacuum_vacuum_scale_factor = 0.02,
    autovacuum_naptime             = 10
);
```
