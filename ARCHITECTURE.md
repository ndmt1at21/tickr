# Architecture

This document explains the moving parts of tickr — what each component does,
why the design is what it is, and which invariants the implementation relies
on. It is the document to read before you change the engine, the storage
contract, or the adapter for a new database.

For user-facing API, see [README.md](README.md).

---

## 1. Big picture

```
                 ┌──────────────────────────────────────────┐
   producer ───▶ │  Client.Enqueue (in caller's tx)         │
                 │  → Storage.Enqueue                       │
                 │  → Notifier.Notify (optional)            │
                 └──────────────────────────────────────────┘
                                  │ committed row
                                  ▼
                    ┌────────────────────────────┐
                    │  tickr_messages (outbox)   │
                    │  status, attempts, lease   │
                    └────────────────────────────┘
                                  │
                                  ▼
   ┌──────────────────────────────────────────────────────┐
   │                  Worker fleet                        │
   │ ┌──────────┐ ┌──────────────┐ ┌──────────────────┐  │
   │ │  Engine  │ │  Reclaimer   │ │  Janitor / Stats │  │
   │ │  (poll)  │ │  (leader)    │ │   (leader)       │  │
   │ └──────────┘ └──────────────┘ └──────────────────┘  │
   └──────────────────────────────────────────────────────┘
                                  │
                                  ▼
                         user handler (in process)
```

The outbox table doubles as both the durable store and the queue. There is
no broker. A worker polls (with optional `LISTEN/NOTIFY` wake-up) for
eligible rows, claims them with `SELECT … FOR UPDATE SKIP LOCKED`, runs the
registered handler, and writes the outcome back to the same table.

---

## 2. Status machine

```
CREATED  ──claim──▶  HANDLING ──nil───▶  SUCCESS
                       │
                       ├─ err (attempt<max) ─▶  FAILED ─▶ RETRYING ─▶ HANDLING
                       │
                       ├─ err (attempt==max) ▶  FAILED ─▶ DEAD
                       │
                       ├─ DeadLetter()       ─▶  DEAD
                       │
                       └─ ctx.Canceled (shutdown) ▶ CREATED | RETRYING
                                                    (attempt NOT incremented)

DEAD ── admin Requeue ──▶ CREATED                  (manual recovery)
```

Every transition appends a row to `tickr_history`. The history is
append-only and never read on the hot path — it exists for operator forensics.

### Why these states?

`FAILED` exists as a transient label distinct from `RETRYING` so observers
can tell *the most recent attempt errored* from *the message is scheduled
for another attempt*. The two collapse to the same scheduling behaviour but
emit different metrics.

`CREATED` versus `RETRYING` is preserved on shutdown release so the
reclaimer can distinguish "never tried" rows from "previously tried" rows
without inspecting attempt count.

---

## 3. At-least-once delivery

The library guarantees **at-least-once**, not exactly-once.

Three things make that work:

1. **Atomic claim**: the claim query uses `SELECT … FOR UPDATE SKIP LOCKED`
   in a single tx that also updates `status='HANDLING'`. Two workers
   cannot observe the same row.

2. **Idempotent state writes**: every state change is conditional on
   `(id, status='HANDLING', claimed_by=workerID)`. A worker that lost its
   lease cannot accidentally mark a row `SUCCESS` after another worker
   already retried it.

3. **Lease auto-extension**: the engine spawns a goroutine that bumps
   `claimed_until` every `Lease/3` while the handler runs. A handler can
   safely declare `WithAttemptTimeout(10*time.Second)` even if `Lease` is
   only 3 seconds, because the lease will be extended for as long as the
   attempt is alive. If the extender fails to extend (network partition,
   another worker took over), the attempt's context is cancelled so the
   handler stops writing.

### What "at-least-once" means for handlers

Handlers must be idempotent. The library cannot make this easier for you
beyond surfacing `InboundMessage.IdempotencyKey` and offering the message
ID — you build dedup on top using whatever scheme fits your domain.

A common pattern is a side table keyed by `(handler_name, idempotency_key)`
that records "this work has been done" inside the same tx as the side
effects. See the orders example.

---

## 4. The claim query

```sql
UPDATE tickr_messages
   SET status        = 'HANDLING',
       attempt       = attempt + 1,
       claimed_by    = $1,
       claimed_until = now() + $2::interval,
       updated_at    = now()
 WHERE id IN (
   SELECT id FROM tickr_messages
    WHERE status IN ('CREATED','RETRYING')
      AND event_type = ANY($3)
      AND process_at <= now()
    ORDER BY process_at, id
    LIMIT $4
    FOR UPDATE SKIP LOCKED
 )
RETURNING …
```

Three properties that matter:

- **`SKIP LOCKED`** is what makes the claim non-blocking across workers.
  Without it, 30 workers polling the same range would serialise on row
  locks.
- **`ORDER BY process_at, id`** keeps delivery roughly FIFO inside each
  event type. The `(event_type, process_at, id)` partial index is what
  makes the inner SELECT cheap.
- **`attempt = attempt + 1`** happens at claim time, not at fail time.
  This is the **poison-pill rule**: an attempt that crashes the worker
  hard (SIGKILL) still counts. Without this, a single bad message could
  loop a fleet forever.

The Postgres adapter and the MySQL adapter both implement this pattern,
adjusted for dialect. CockroachDB inherits the Postgres implementation
verbatim.

---

## 5. Graceful shutdown semantics

When `Worker.Start`'s context is cancelled:

1. The engine sets `stopping=true` and stops accepting new claims.
2. In-flight handlers see their `ctx.Done()` close (the same context
   passed to the handler).
3. If they return before `ShutdownGrace`, their outcomes commit normally:
   - returned `nil` → `SUCCESS`
   - returned `error` → `RETRYING` with the configured policy
   - returned `ctx.Canceled` *and* the worker is stopping → the row is
     released (`CREATED` or `RETRYING`) with **attempt not incremented**.
4. If a handler exceeds `ShutdownGrace`, `drain()` returns an error and
   the worker exits anyway. The orphaned row stays in `HANDLING` until
   its lease expires; the reclaimer then transitions it to `RETRYING`
   with **attempt staying incremented** — this is the SIGKILL case.

The asymmetry between "clean shutdown" (don't burn an attempt) and "hard
crash" (do burn) is intentional. Clean shutdowns are routine (deploys,
scale-down); hard crashes might be the message itself being poison.

---

## 6. LISTEN / NOTIFY adaptive polling

The engine polls on a configurable interval (default 100 ms) with
exponential backoff up to a cap (default 2 s) when polls return zero rows.

Adapters that implement `tickr.Notifier` cut latency by waking the engine
out of its backoff on every enqueue:

```
Client.Enqueue
  └─ Storage.Enqueue (inside caller tx)
  └─ Notifier.Notify (inside caller tx, only sees the light of day on commit)

Worker boot
  └─ Notifier.Listen → channel
  └─ engine select { time.After(backoff), notifyCh }
```

The Postgres adapter implements it via `LISTEN tickr_msg` on a dedicated
hijacked connection plus `SELECT pg_notify('tickr_msg', event_type)` on
the producer side. Postgres semantics guarantee that the NOTIFY is only
delivered to subscribers when the issuing transaction commits — which is
exactly what the outbox pattern needs. A rolled-back enqueue is invisible.

MySQL and CockroachDB do not support LISTEN/NOTIFY, so their workers
fall back to pure polling. Composition via interface embedding (in the
CockroachDB adapter) ensures it does not accidentally satisfy
`tickr.Notifier` through promoted methods of the embedded Postgres store.

---

## 7. Partitioning (Postgres)

Default Postgres schema is a single table. For workloads beyond ~100M
historical rows, `pgstore.WithPartitioning()` switches to a RANGE-partitioned
schema:

- `tickr_messages` is partitioned by `created_at`.
- `Store.EnsurePartitions(ctx, monthsAhead)` creates monthly partitions
  ahead of time. Run it daily from a leader-elected loop, or once at
  deploy time.
- A `DEFAULT` partition catches inserts outside any explicit range — its
  size should stay near zero and is a warning signal if it doesn't.
- `Store.DropPartitionsBefore(ctx, cutoff)` is O(1) regardless of row
  count, replacing the linear-cost janitor `DELETE` for terminal rows.

### The idempotency trade-off

A Postgres unique index on a partitioned table **must** include the
partition key. The idempotency index therefore becomes
`(event_type, idempotency_key, created_at)`, meaning idempotency keys are
unique **per partition** (per month by default), not globally.

For typical outbox workloads — where duplicate-detection happens within
seconds-to-hours of the original event — this is fine. If you need
cross-partition idempotency, build a side lookup table keyed by
`(event_type, idempotency_key)` and check it before calling Enqueue.

---

## 8. Background loops and leader election

Each worker runs three optional background loops alongside the claim loop:

| Loop       | Cadence (default) | Purpose                                   |
|------------|-------------------|-------------------------------------------|
| Reclaimer  | every 5 s         | Transitions `HANDLING` rows whose lease   |
|            |                   | has expired back to `RETRYING`.           |
| Janitor    | every 60 s        | Deletes terminal rows older than the      |
|            |                   | retention policy.                         |
| Stats      | configurable      | Samples `Stats()` and emits gauge metrics.|

All three are gated by `Storage.TryLeaderLock` so that, across a fleet,
only one worker runs each at any moment.

### Leader-lock strategies

- **Postgres**: `pg_try_advisory_lock(fnv64(key))`. Session-scoped — the
  lock auto-releases when the connection is returned to the pool, so we
  don't need TTL bookkeeping.
- **MySQL**: `GET_LOCK('tickr:<key>', 0)`. Session-scoped. We hold the
  dedicated conn for the duration.
- **CockroachDB**: row-locked lease in `tickr_locks` with a 60 s TTL.
  CockroachDB does not implement `pg_try_advisory_lock`. The TTL exists
  so a worker that dies between acquire and release doesn't permanently
  freeze the lock.

All three return the same `(acquired bool, unlock func(), err error)`
shape to the caller.

---

## 9. Storage adapter contract

`tickr.Storage` is the interface every adapter must satisfy. Beyond the
SQL each method translates to, the contract has a small number of
non-obvious requirements:

1. **`Enqueue` must respect `tx`**. When `tx.IsZero()` is false, the row
   must be inserted *inside* that tx. Notifications go in the same tx.
2. **`Claim` must update `attempt`** atomically with the status change.
   Adapters that can't do this in a single statement (MySQL) wrap a
   SELECT-then-UPDATE inside a tx.
3. **State writes are conditional on lease ownership**. `Succeed`, `Fail`,
   `Extend`, and `ReleaseShutdown` all match on
   `(id, status='HANDLING', claimed_by=workerID)` and silently no-op when
   the lease is gone. Adapters must preserve this behaviour to keep the
   "lost lease → safely cancel" invariant.
4. **`ReclaimExpired` must be safe to run concurrently**, even though
   we gate it on the leader lock. Defence in depth: idempotency by
   construction lets us tolerate accidental concurrent runs.
5. **`ApplyMigrations` is idempotent**. Adapters track applied versions
   in `tickr_schema_version`.
6. **`Notifier` is opt-in**. Adapters that can't implement it MUST NOT
   satisfy the interface at all — workers detect via type assertion.

The conformance suite at `storage/internal/storagetest` runs the same
test battery against any adapter so new backends start with portable
coverage.

---

## 10. Observability

Three pluggable hooks — `Logger`, `Metrics`, `Tracer` — all default to
no-op implementations. They are the only places the engine talks to the
outside world.

### Metrics

Adapters in `metrics/prom` and `metrics/otel` emit the same vocabulary:

| Metric                          | Type      | Labels                  |
|--------------------------------|-----------|-------------------------|
| `tickr.messages.enqueued`       | counter   | `event_type`            |
| `tickr.messages.dead`           | counter   | `event_type`            |
| `tickr.handler.attempts`        | counter   | `event_type`, `outcome` |
| `tickr.handler.duration`        | histogram | `event_type`, `outcome` |
| `tickr.queue.depth`             | gauge     | `event_type`, `status`  |
| `tickr.claim.batch_size`        | histogram | —                       |
| `tickr.claim.duration`          | histogram | —                       |
| `tickr.leases.reclaimed`        | counter   | `event_type`            |
| `tickr.handlers.inflight`       | gauge     | `event_type`            |

(The Prometheus impl uses `tickr_…_…` underscore naming per Prometheus
convention; OTel uses the dotted form which exporters normalise to the
backend's preferred shape.)

### Tracing

`tracing/otel` injects W3C `traceparent` and `tracestate` into
`Message.Headers` at enqueue time, and extracts them at handler dispatch.
A single trace therefore spans **producer → outbox → consumer** across
processes, even when the consumer runs hours later. Span attributes follow
the OpenTelemetry messaging semantic conventions.

---

## 11. Limitations

This section is the canonical list of what tickr does not do, what it
does imperfectly, and what we know we want to improve. If you are
evaluating tickr or planning a contribution, read it.

### 11.1 Intentional non-goals (design decisions)

These are not bugs. They are bounded scope.

- **Cross-DB transactions.** The producer's business write and the
  outbox insert must share a database. If you have a polyglot stack,
  use the broker-forwarder pattern: tickr in your primary DB → bridge
  reader → broker (Kafka/NATS/Redis Streams).
- **Exactly-once delivery.** Out of scope; surface `IdempotencyKey`
  and rely on handler-side dedup.
- **Backpressure on the producer side.** A runaway producer will
  saturate the outbox table; mitigate via Postgres `tablespace`,
  partitioning, and quota at the application layer.
- **Saga orchestration.** Multi-step workflows are not modelled —
  [`examples/saga`](examples/saga) shows the recommended pattern of
  chained enqueues, but tickr does not own state machines, compensation,
  or visualisation. It is the delivery substrate underneath sagas, not
  the saga engine itself.
- **First-class scheduling.** `ScheduledAt` is a single per-message
  delay. We do not store cron expressions or recurring schedules.
- **Storage outside an RDBMS.** Redis, DynamoDB, and similar KV stores
  cannot participate in the producer's business transaction, so they
  cannot serve as the outbox table. They are valid *destinations* via
  forwarder mode but not storage backends.

### 11.2 Backend-specific limitations

#### Postgres — partitioned schema

- **Idempotency is scoped per partition.** Postgres requires a unique
  index on a partitioned table to include the partition key, so the
  idempotency index covers `(event_type, idempotency_key, created_at)`.
  With monthly partitions this means a duplicate produced more than a
  month apart will be admitted as a new row. For typical outbox
  workloads — where duplicate detection happens within minutes-to-hours
  of the original event — this is acceptable. Mitigation: build a
  side lookup table keyed by `(event_type, idempotency_key)` and check
  it before calling Enqueue. (See [`migrations_partitioned/0001_init.sql`](storage/postgres/migrations_partitioned/0001_init.sql).)
- **Default partition is a footgun.** Inserts outside any explicit
  range land in `tickr_messages_default`, which cannot be dropped by
  `DROP PARTITION`. If you stop calling `EnsurePartitions`, retention
  silently breaks. Monitor the default partition size as an alert.
- **Mode is permanent.** Switching between the unpartitioned and
  partitioned schema on a live database is not supported — `ALTER
  TABLE … PARTITION BY` does not exist in Postgres. Plan the choice
  at schema-init time.

#### MySQL

- **No Notifier.** MySQL has no `LISTEN/NOTIFY` equivalent, so workers
  fall back to pure polling. Expect ~poll-interval latency on the
  enqueue → dispatch path (default 100 ms with 2 s backoff).
- **`LIMIT` in `DELETE` / `UPDATE`** is MySQL-only syntax. The MySQL
  adapter uses it for `PurgeTerminal` and `ReclaimExpired`. If you
  port the adapter to MariaDB or another fork, verify support.
- **`isDuplicateErr` is string-matched.** [`storage/mysql/mysql.go`](storage/mysql/mysql.go)
  detects duplicate-key collisions by substring (`"Error 1062"` /
  `"Duplicate entry"`) because we depend on `database/sql` and don't
  want to import the driver's error type. Locale-translated error
  messages will defeat this. Follow-up: use `errors.As` against
  `*mysql.MySQLError{Number: 1062}`.
- **Multi-statement migrations are split naively.** `splitSQL` in the
  MySQL adapter splits on `;`. It is correct for our DDL but will
  mishandle SQL containing semicolons in string literals or comments.
  Keep MySQL migrations DDL-only.
- **`Notifier` interface is not implemented at all** (deliberately).
  Workers detect via type assertion and silently fall back.

#### CockroachDB

- **No Notifier.** CockroachDB does not support `LISTEN/NOTIFY`. The
  adapter is composed (not embedded) such that the inherited Postgres
  Notify/Listen methods are NOT promoted, so type assertions for
  `tickr.Notifier` correctly miss. Workers fall back to polling.
- **Leader lease has a TTL window.** Where Postgres uses
  session-scoped advisory locks (auto-released on disconnect), the
  CockroachDB adapter uses a row in `tickr_locks` with a 60 s TTL. If
  a worker dies holding the lease, no other worker can take over for
  up to 60 s. Reclaimer / janitor are idempotent so this is a latency
  issue, not a correctness one.
- **`EnsurePartitions` and `DropPartitionsBefore` inherit the
  Postgres implementations** but partitioning semantics differ in
  CockroachDB (it has its own partitioning DSL). Do not call them on
  a CockroachDB cluster — they will issue Postgres-only DDL. The
  inheritance is a known sharp edge; an explicit override that errors
  with a clear message is a planned follow-up.
- **Schema migrations assume Postgres-compatible DDL.** Cockroach
  supports most of it (ENUM types, `JSONB`, partial indexes) but does
  not implement `pg_try_advisory_lock`, `pg_notify`, or `LISTEN`. The
  adapter only steps off these explicitly.

#### Cross-cutting

- **`tickrctl drain`** polls `Stats()` for queue depth and exits when
  it reaches zero. It does not signal a specific worker to stop
  claiming. For "drain this pod before terminating" you should set
  `terminationGracePeriodSeconds` and SIGTERM the worker — `Start`
  will run its bounded drain. CLI-driven per-worker drain is a planned
  follow-up.
- **`Storage.Stats()` is O(table-scan)** on most adapters: it
  aggregates over the whole `tickr_messages` table. On multi-100M-row
  unpartitioned schemas, do not sample frequently. Tune
  `StatsPolicy.Interval` accordingly (default is "off").
- **History retention is automatic.** Whenever the janitor runs and the
  storage adapter implements the optional `tickr.HistoryPurger`
  extension, `tickr_history` is purged in the same pass as
  `tickr_messages`. `RetentionPolicy.History == 0` picks a default of
  `max(Success, Dead, 30d)` so history rows outlive their parent
  message; set a positive duration to override or a negative value to
  opt out entirely. The Postgres and MySQL adapters implement
  `HistoryPurger`; the CockroachDB adapter forwards to the embedded
  Postgres adapter.

### 11.3 Known follow-ups

Tracked here so contributors can pick them up:

- [ ] **CockroachDB: explicit override of `EnsurePartitions` and
      `DropPartitionsBefore`** that returns
      `ErrUnsupported("use CRDB-native partitioning")` instead of
      silently issuing Postgres DDL.
- [x] **MySQL: typed duplicate-key detection** via `errors.As` against
      `*mysql.MySQLError{Number: 1062}` instead of substring match.
- [ ] **Unit tests for non-trivial helpers** that have no integration
      coverage today: `parseUpperBound` (Postgres partition listing)
      and `splitSQL` (MySQL migration runner).
- [ ] **`tickrctl drain --worker <id>`** that sends a signal (via a
      `tickr_admin` row or a separate channel) telling one specific
      worker to stop claiming.
- [x] **History retention.** A janitor pass for `tickr_history`
      governed by a separate `RetentionPolicy.History` field.
- [ ] **Stats sampling pushdown.** Replace the full-scan
      `Stats()` with per-status counters maintained by triggers or
      by background sampling of a partitioned subset.
- [ ] **`tickr.Notifier` listener auto-reconnect.** Today, if the
      hijacked Postgres connection drops mid-flight, the listener
      goroutine exits and the worker falls back to polling until
      the next `Worker.Start`. A reconnect loop would keep
      low-latency dispatch alive across transient network drops.
- [ ] **Conformance suite gaps.** [`storagetest.RunSuite`](storage/internal/storagetest/suite.go)
      does not yet cover `Extend`, `EnqueueBatch`, or `ReleaseShutdown`
      directly — these are exercised end-to-end through the engine but
      have no isolated test.
- [ ] **Broker-forwarder mode.** Run tickr as a publisher to an
      external broker (Kafka/NATS) instead of running handlers
      in-process. Mentioned in §11.1 as the recommended polyglot path
      but not yet implemented.

---

## 12. Deployment & operations

This section covers running tickr workers as long-lived processes —
specifically the Kubernetes case, since several questions there are not
obvious to anyone whose mental model of a service is "HTTP-traffic-driven."

### 12.1 The shutdown flow you get for free

A tickr worker that does the usual signal wiring:

```go
ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
defer stop()
_ = w.Start(ctx) // blocks
```

reacts to SIGTERM as follows:

1. `signal.NotifyContext` cancels the context handed to `Worker.Start`.
2. The engine sees `ctx.Done()`, sets `stopping=true`, stops accepting
   new claims.
3. In-flight handlers receive `ctx.Done()` on the per-attempt context.
4. The worker waits up to `WorkerConfig.ShutdownGrace` (default 30 s)
   for handlers to finish, then returns. See [§5](#5-graceful-shutdown-semantics).

The contract: **handlers that finish cleanly within `ShutdownGrace` keep
their normal Succeed/Fail behaviour; handlers that don't get their rows
auto-reclaimed after the lease expires** — at-least-once delivery survives.

### 12.2 Kubernetes: workers are consumers, not servers

The default K8s lifecycle assumptions ("pods get traffic; idle pods can
be terminated") do not apply to outbox workers. Two anti-patterns to
avoid:

- **HPA on CPU/RPS.** Worker CPU usage is bursty (high during a
  handler attempt, ~zero during poll backoff) and there are no
  inbound requests at all. CPU-based HPA will oscillate and
  RPS-based HPA will keep scaling to zero.
- **HTTP liveness probes that hit a `/healthz` you haven't wired up.**
  If the probe fails, K8s kills the pod *without* running its
  shutdown grace flow correctly. A worker without an HTTP server
  needs either no probe or a TCP / exec probe that doesn't depend
  on serving requests.

### 12.3 Safe shutdown manifest

The two settings that matter:

```yaml
spec:
  template:
    spec:
      # Must be >= ShutdownGrace + handler tail. tickr defaults to 30s
      # ShutdownGrace; add 10–15s buffer for the kernel and any preStop
      # hooks. K8s defaults to 30s which will cut handlers off mid-attempt.
      terminationGracePeriodSeconds: 60

      containers:
        - name: worker
          image: ghcr.io/you/your-worker:latest
          env:
            - name: TICKR_DSN
              valueFrom:
                secretKeyRef: { name: tickr-db, key: dsn }

          # No HTTP traffic, but we still want K8s to know "alive" /
          # "ready" without lying. The simplest correct probe is exec:
          # the process exits non-zero if it has crashed, so PID-1 is
          # sufficient liveness. Optional: serve a tiny /healthz that
          # returns 503 once the worker context is cancelled, so the
          # readiness probe drains it from any Service it sits behind.
          livenessProbe:
            exec:
              command: ["sh", "-c", "kill -0 1"]
            periodSeconds: 30
            failureThreshold: 3

          # preStop runs *before* SIGTERM. Use it to give a service
          # mesh / sidecar time to flush connections. Tickr itself
          # does not need this — its drain is triggered by SIGTERM —
          # but a 5–10s sleep here protects against sidecar races.
          lifecycle:
            preStop:
              exec:
                command: ["sh", "-c", "sleep 5"]
```

The accounting:

```
deploy SIGTERM ─▶ preStop sleep 5s ─▶ container SIGTERM
                                       └─▶ tickr drain up to 30s
                                       └─▶ exit
                                                 ▲
                                                 │
                  terminationGracePeriodSeconds (60s) ──┘
```

If `terminationGracePeriodSeconds < preStop + ShutdownGrace`, K8s sends
SIGKILL mid-drain and you get the SIGKILL semantics from
[§5](#5-graceful-shutdown-semantics) — at-least-once still holds, but
attempts get burned that didn't need to.

### 12.4 Autoscaling on queue depth (KEDA)

The right scaler for a tickr worker is **queue depth, not traffic**.
Export `tickr_queue_depth` via the Prometheus adapter and let KEDA scale
on it:

```yaml
apiVersion: keda.sh/v1alpha1
kind: ScaledObject
metadata:
  name: tickr-worker
spec:
  scaleTargetRef:
    name: tickr-worker
  minReplicaCount: 1     # never scale to zero: claim loop must keep running
  maxReplicaCount: 30
  pollingInterval: 15
  cooldownPeriod: 120
  triggers:
    - type: prometheus
      metadata:
        serverAddress: http://prometheus.monitoring:9090
        query: sum(tickr_queue_depth{status=~"CREATED|RETRYING"})
        threshold: "200"    # add a replica per 200 pending messages
```

Notes:

- **`minReplicaCount: 1`** is the answer to "K8s sees no traffic and
  scales to zero." A worker that is scaled to zero stops polling, so
  retries and the reclaimer stall. Keep at least one replica alive.
- The query filters by status so reschedule-pending rows don't
  contribute. Tune the threshold to your handler throughput.
- If you must scale to zero (cost reasons), gate it on a leader-elected
  "wake-up" worker that polls every 5 minutes and scales the fleet
  back up if queue depth > 0. That pattern is fragile; the minReplica=1
  approach is recommended.

### 12.5 Per-pod drain without killing the pod

Sometimes you want a specific pod to stop claiming **without exiting**
— e.g., before a debugging attach, or to safely cordon a node. Today
the recommended path is:

1. `kubectl delete pod <name>` with `--grace-period=60` — K8s SIGTERMs
   the pod and tickr's normal drain runs.

A `tickrctl drain --worker <id>` that signals via a row in the DB
(without killing the process) is on the follow-up list (§11.3).

### 12.6 Health-check endpoint pattern (optional)

If you do want HTTP-based probes — for a service mesh, ingress
visibility, or because your platform requires it — wire a tiny
`/healthz` that mirrors the worker context:

```go
mux := http.NewServeMux()
mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
    select {
    case <-ctx.Done():
        http.Error(w, "shutting down", http.StatusServiceUnavailable)
    default:
        w.WriteHeader(http.StatusOK)
    }
})
go http.ListenAndServe(":8080", mux)
```

`readinessProbe` hits `/healthz` → returns 503 once SIGTERM lands,
which drops the pod out of any Service endpoint within one probe cycle.
The worker keeps draining in-flight handlers behind it. The
`livenessProbe` stays exec-based so a stuck HTTP listener doesn't
trigger a SIGKILL before drain completes.

---

## 13. Layout map

```
tickr/
├── client.go         producer-side Client
├── worker.go         worker shell: lifecycle, background loops
├── engine.go         claim loop, dispatch, lease auto-extension
├── registry.go       handler registry, per-event-type options
├── retry.go          RetryPolicy + ExponentialBackoff (jittered)
├── errors.go         sentinels: ErrDuplicate, DeadLetter, RetryAfter, Skip
├── storage.go        Storage interface
├── notifier.go       Notifier interface (optional capability)
├── message.go        Message, InboundMessage, Status, Outcome types
├── observe.go        Logger, Metrics, Tracer interfaces + no-op impls
├── migrate.go        Migrator entry point
│
├── storage/
│   ├── postgres/     pgx adapter (default + partitioned schemas)
│   ├── mysql/        database/sql adapter (MySQL 8.0+)
│   ├── cockroach/    CockroachDB adapter (embeds postgres, overrides lock)
│   └── internal/storagetest/   portable conformance suite
│
├── codec/
│   ├── json/         encoding/json typed-handler wrapper
│   └── proto/        google.golang.org/protobuf wrapper
│
├── metrics/
│   ├── prom/         Prometheus implementation
│   └── otel/         OpenTelemetry implementation
│
├── tracing/otel/     OpenTelemetry tracer (W3C context propagation)
│
├── cmd/tickrctl/     admin CLI
└── examples/         runnable examples
```
