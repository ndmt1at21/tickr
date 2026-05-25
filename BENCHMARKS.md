# Benchmarks: tickr vs other Go job/outbox libraries

This page tracks how tickr's throughput compares against four other
Go async-job libraries on the same workload. Bench code lives in the
[`benchmarks/`](benchmarks/) module — a separate Go module so the
competitors' dependencies stay out of tickr's `go.sum`.

## Libraries under test

| Library | Storage | Producer batch API | Notes |
|---|---|---|---|
| **tickr** | PostgreSQL (pgx) | `EnqueueBatch` | Transactional outbox, `FOR UPDATE SKIP LOCKED` claim |
| **River** ([riverqueue/river](https://github.com/riverqueue/river)) | PostgreSQL (pgx) | `InsertMany` | Pgx-native, transactional `InsertTx`, listen/notify |
| **Gue** ([vgarvardt/gue](https://github.com/vgarvardt/gue)) | PostgreSQL (pgx or lib/pq) | `EnqueueBatch` | `FOR UPDATE SKIP LOCKED`, predates River |
| **Watermill SQL** ([ThreeDotsLabs/watermill-sql](https://github.com/ThreeDotsLabs/watermill-sql)) | PostgreSQL (database/sql) | `Publish(topic, ...msgs)` | Per-consumer-group offsets table |
| **Asynq** ([hibiken/asynq](https://github.com/hibiken/asynq)) | Redis | one-by-one `Enqueue` (no batch) | Different substrate; included as a reference point |

Only tickr can be enqueued inside the caller's own DB transaction; the
others require their own DB/Redis connection. That difference is **not
captured** by these benchmarks — they measure raw throughput only.

## Scenarios

Both scenarios share `defaultParams()` ([benchmarks/common_test.go](benchmarks/common_test.go)):

- 64-byte JSON payload
- EnqueueBatch chunk: 500
- Consumer batch size: 100 (Postgres adapters)
- Worker pool size: 64
- Poll interval: 10 ms

### Enqueue throughput

Pre-loads `b.N` messages and measures msgs/sec for the producer side
only. No worker runs.

### Drain throughput

Pre-loads `b.N` messages with the timer stopped, then starts a worker
with a no-op handler and measures wall time until all `b.N` reach
SUCCESS. Reclaimer / janitor are disabled in tickr so we measure the
claim → handle → ack loop in isolation.

## Reproducing

The bench files use the `bench` build tag and Testcontainers (Docker
required).

```bash
cd benchmarks
go mod tidy
go test -tags bench -run='^$' -bench='Enqueue$'  -benchtime=20000x ./...
go test -tags bench -run='^$' -bench='Drain$'    -benchtime=20000x ./...
```

`-benchtime=Nx` pins the message count per iteration (here 20k). Bump
it for steady-state numbers; small N is dominated by container startup
and worker spin-up.

To run a single library:

```bash
go test -tags bench -run='^$' -bench='^BenchmarkRiver'   -benchtime=20000x .
go test -tags bench -run='^$' -bench='^BenchmarkGue'     -benchtime=20000x .
go test -tags bench -run='^$' -bench='^BenchmarkTickr'   -benchtime=20000x .
go test -tags bench -run='^$' -bench='^BenchmarkWaterm'  -benchtime=20000x .
go test -tags bench -run='^$' -bench='^BenchmarkAsynq'   -benchtime=20000x .
```

## Results

**Reference host:** Intel i7-1255U (12 cores), 16 GB RAM, Linux 6.6
WSL2, Docker 28.4 (overlayfs).
**Postgres image:** `postgres:16-alpine` (single container, default
config — no PgBouncer, no autovacuum tuning).
**Redis image:** `redis:7-alpine` (single container, default config).
**Iteration size:** `-benchtime=5000x` (5,000 messages per scenario),
`-count=3`. Numbers below are the **median** of three back-to-back
runs after first-iteration container/OS-cache warmup.

### Enqueue (msgs/sec, higher is better)

| Library | msgs/sec | Notes |
|---|---:|---|
| **Watermill SQL** | **106,802** | Single multi-row INSERT per batch; thinnest table |
| Gue | 95,045 | Simple table (no history/metadata cols), pgx batch |
| River | 52,854 | `InsertMany`, rich job-state columns |
| tickr | 39,133 | `EnqueueBatch` writes `tickr_messages` + pg_notify per row |
| Asynq | 4,370 | One-by-one (no batch API), Redis round-trip per msg |

### Drain (msgs/sec, higher is better)

| Library | msgs/sec | Notes |
|---|---:|---|
| **Watermill SQL** | **40,010** | Pub/sub model — advances consumer offset, no per-msg UPDATE |
| River | 5,991 | Notify-based dispatch + pgx-native ack |
| tickr (HistoryOff) | 4,809 | `WithHistoryPolicy(HistoryOff)` — skips per-transition audit writes |
| tickr (default) | 4,259 | UPDATE + history INSERT folded into one CTE ([postgres.go:537](storage/postgres/postgres.go#L537)) |
| Asynq | 2,655 | Redis-backed, per-msg LPOP/ZADD round-trip |
| Gue | 1,835 | Per-msg DELETE on success + pgx-native ack |

### How to read these

- **Watermill leads both lists because it isn't a job queue.** It's
  pub/sub with a consumer-group offsets table — there's no per-message
  state machine, so an ack is one row-update bumping the offset, not
  N row-updates marking jobs SUCCESS. Different guarantee, different
  workload.
- **tickr trails River on drain (~67%)** because each ack still writes
  two rows: the `tickr_messages` UPDATE *and* a row in `tickr_history`
  for the HANDLING→SUCCESS transition. The transitions are folded into
  a single CTE round-trip ([postgres.go:521-657](storage/postgres/postgres.go#L521-L657)),
  but the **write volume** is still ~2× River's. The next lever to
  close this gap is a `HistoryPolicy` knob (`Off` / `AsyncBuffered`)
  for users who don't need a per-message audit log — see [tier 2 in the
  improvement plan](#tier-2--optional-history-for-high-throughput).
- **Gue's drain throughput trails its enqueue** because each success
  does a row-DELETE under `FOR UPDATE SKIP LOCKED`; insert is cheap,
  but the consumer is bound by per-row write amplification.
- **Asynq's enqueue gap** is the no-batch API — every `Enqueue` is one
  Redis round-trip. With pipelining, that number would multiply.
- All numbers are from a **single-host, single-container** setup. On a
  real Postgres cluster with PgBouncer, tuned autovacuum, and
  horizontally-scaled workers, network RTT is meaningfully nonzero —
  so the round-trip-count optimizations (like the CTE merge below)
  matter more there than they do on this loopback bench.

## Optimization log

### 2026-05-25 — Fold ack UPDATE + history INSERT into one CTE

Per-message ack was 2–3 separate `pool.Exec` calls in the Postgres
adapter: an UPDATE on `tickr_messages` plus 1–2 INSERTs into
`tickr_history`. Each was a separate round-trip on the pool. Folded
into single CTE queries in [storage/postgres/postgres.go:537-664](storage/postgres/postgres.go#L537-L664):

- `Succeed`: 2 round-trips → **1**
- `ReleaseShutdown`: 2 round-trips → **1**
- `Fail`: 3 round-trips → **1** (records both the transient FAILED
  state and the resolved RETRYING/DEAD state in one statement)

**Observed gain on this bench:** ~4% on drain (3,825 → 3,978
msgs/sec). Modest, because Docker-loopback round-trips are ~10µs each
— the ack cost is dominated by WAL write amplification, not network.
On a real PG cluster (1–2 ms RTT) the same change would be a
multi-x speedup on the ack path.

**Verified:** all postgres conformance tests pass
(`go test -tags integration ./storage/postgres/...`), including
`succeed`, `fail_retry_then_dead`, and `notifier`.

### 2026-05-25 — `HistoryOff` policy for opt-out audit log

Added `pgstore.WithHistoryPolicy(pgstore.HistoryOff)`
([postgres.go:49-90](storage/postgres/postgres.go#L49-L90)). Under
`HistoryOff`, the adapter skips every `tickr_history` INSERT on
the hot path: the claim-time batch insert, the per-message
HANDLING→SUCCESS row, and the HANDLING→FAILED→{RETRYING,DEAD}
pair on Fail. UPDATE-only variants replace the CTE queries
when the policy is set. Admin recovery (`Requeue`) still records
DEAD→CREATED so manual interventions remain auditable.

**Observed gain on this bench:** drain median 4,259 → 4,809
msgs/sec (+13%) at `-benchtime=20000x`.

**Why not larger?** The per-ack bottleneck on this bench is
**WAL fsync per `Succeed` Exec**, not row volume — see Tier 3
below.

**Verified:** new `TestPostgresHistoryOff` runs the full
conformance suite under `HistoryOff` and asserts that no rows
with `from_status='HANDLING'` ever land in `tickr_history`
([postgres_integration_test.go:54-105](storage/postgres/postgres_integration_test.go#L54-L105)).

## What's next

### Tier 1 — already shipped

- ☑ Fold ack UPDATE + history INSERT into single CTE.

### Tier 2 — optional history (shipped)

- ☑ `WithHistoryPolicy(HistoryOff)` skips all hot-path
  `tickr_history` writes ([postgres.go:49](storage/postgres/postgres.go#L49)).
  Admin recovery (Requeue) still writes history rows by design.

**Observed gain:** median drain 4,259 → 4,809 msgs/sec (+13%).

**Smaller than the "double the writes" math suggests** — turns out
the `tickr_history` INSERT was not the actual bottleneck on this
bench. Each `Succeed` is its own implicit transaction, so the
hot path is dominated by **WAL fsync per ack**, not by row volume.
Removing the history row cuts WAL bytes but not fsync count. On a
real cluster (where network RTT matters more than fsync latency)
the gain will be larger, but the next big lever is batching the
ack itself.

### Tier 3 — batched ack via `pgx.SendBatch`

Today each completed message acks independently in its own
transaction ([engine.go:416](engine.go#L416)) → 1 fsync per ack.
Accumulating completions per claim-cycle and flushing them in one
`pgx.SendBatch` (or one `BEGIN; UPDATE...; UPDATE...; COMMIT`) would
amortize fsync across the batch. This is the **highest-leverage
remaining optimization** — projected to push drain into River
territory (~6k+ msgs/sec) on this bench, more on a real cluster.

## Caveats

- **Single-host Docker** under-counts what a real Postgres cluster
  with PgBouncer + sized autovacuum can do — see [README §Throughput](README.md#throughput).
- **Asynq** runs on Redis, not Postgres. It is included to show the
  order-of-magnitude gap between RDBMS-backed outbox queues and a
  Redis-backed task queue, not as a like-for-like comparison.
- **Watermill SQL** uses a consumer-group offsets table; first-message
  latency is higher because the subscriber polls in batches. The Drain
  number captures steady-state, not p99 single-message latency.
- **River** auto-tunes fetch behavior; `FetchPollInterval` is set to
  match the others for fairness, but its listen/notify path may still
  give it an edge on tail latency.
- Each iteration spins a fresh container — pre-load time is excluded
  from the timer, but container start-up is included in wall time
  unless you run a long `-benchtime`.

## Updating this page

After running the bench, replace the `_TBD_` cells with the `msgs/sec`
custom metric reported by each `go test -bench` invocation. Don't
forget to update the **Reference host** line so future readers know
what hardware produced the numbers.
