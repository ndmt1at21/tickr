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

> Numbers below the rule are from the **2026-05-26 re-run** on Intel
> i5-8300H after the unnest `EnqueueBatch` rewrite. Pre-rewrite rows
> are from the i7-1255U reference host.

| Library | msgs/sec | Notes |
|---|---:|---|
| **Watermill SQL** | **106,802** | Single multi-row INSERT per batch; thinnest table (i7 ref) |
| Gue | 95,045 | Simple table (no history/metadata cols), pgx batch (i7 ref) |
| River | 52,854 | `InsertMany`, rich job-state columns (i7 ref) |
| tickr | 39,133 | Pre-rewrite: N individual INSERTs via `pgx.Batch` (i7 ref) |
| Asynq | 4,370 | One-by-one (no batch API), Redis round-trip per msg |
|---|---|---|
| **tickr (unnest)** | **42,725** | Single `INSERT … SELECT unnest($1[]…)` — 1 round-trip (i5, 2026-05-26) |
| River | 29,649 | Same host for apples-to-apples (i5, 2026-05-26) |

### Drain (msgs/sec, higher is better)

> Numbers below the rule are from the **2026-05-26 re-run** on Intel
> i5-8300H after shipping Tier 3 (batched ack). The pre-Tier-3 rows are
> preserved above the rule for reference (they were measured on i7-1255U;
> direct cross-machine comparison is approximate).

| Library | msgs/sec | Notes |
|---|---:|---|
| **Watermill SQL** | **40,010** | Pub/sub model — advances consumer offset, no per-msg UPDATE |
| River | 5,991 | Notify-based dispatch + pgx-native ack (i7 reference) |
| tickr (HistoryOff) | 4,809 | Pre-Tier-3 (i7 reference) |
| tickr (default) | 4,259 | Pre-Tier-3 (i7 reference) |
| Asynq | 2,655 | Redis-backed, per-msg LPOP/ZADD round-trip |
| Gue | 1,835 | Per-msg DELETE on success + pgx-native ack |
|---|---|---|
| **tickr (HistoryOff + BatchedAck)** | **5,943** | `BatchSucceed` — N acks in 1 tx → 1 WAL fsync (i5, 2026-05-26) |
| River | 5,662 | Same bench, same host for apples-to-apples (i5, 2026-05-26) |
| tickr (default + BatchedAck) | 3,684 | CTE still writes 2 rows/msg; WAL bytes, not fsyncs, are now the cap |

### How to read these

- **Watermill leads both lists because it isn't a job queue.** It's
  pub/sub with a consumer-group offsets table — there's no per-message
  state machine, so an ack is one row-update bumping the offset, not
  N row-updates marking jobs SUCCESS. Different guarantee, different
  workload.
- **tickr (HistoryOff + BatchedAck) now matches River** on the same
  host. The batched-ack path (`BatchAcker` interface + `ackFlusher`
  goroutine) cuts WAL fsyncs from N per claim batch to 1, which was
  the root bottleneck identified in the Tier 3 analysis.
- **tickr default still trails** because each ack writes two rows (the
  UPDATE *and* a `tickr_history` INSERT via CTE). The fsync count is
  now 1 per batch, but WAL *byte* volume is unchanged — that's the
  remaining gap. Users who need the audit log and top throughput should
  combine `BatchedAck` with `AsyncBuffered` history (not yet shipped).
- **Gue's drain throughput trails its enqueue** because each success
  does a row-DELETE under `FOR UPDATE SKIP LOCKED`; insert is cheap,
  but the consumer is bound by per-row write amplification.
- **Asynq's enqueue gap** is the no-batch API — every `Enqueue` is one
  Redis round-trip. With pipelining, that number would multiply.
- **tickr now beats River on enqueue** on the same host after switching
  `EnqueueBatch` from N individual INSERTs via `pgx.Batch` to a single
  `INSERT … SELECT unnest($1::uuid[], …)`. The remaining gap to
  Gue/Watermill is tickr's wider table (headers, idempotency_key, shard,
  max_attempts columns) — inherent to the feature set, not the query path.
- All numbers are from a **single-host, single-container** setup. On a
  real Postgres cluster with PgBouncer, tuned autovacuum, and
  horizontally-scaled workers, network RTT is meaningfully nonzero —
  so the round-trip-count optimizations matter more there than on this
  loopback bench.

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
