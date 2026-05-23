-- tickr schema (partitioned variant).
-- Hot table is RANGE-partitioned by created_at. The default partition catches
-- inserts outside any explicit monthly partition; callers should call
-- Store.EnsurePartitions periodically (e.g. daily) so writes stay in real
-- partitions and retention can be implemented by DROP PARTITION.
--
-- IDEMPOTENCY NOTE: Postgres requires a partitioned table's UNIQUE index to
-- include the partition key. The idempotency index therefore covers
-- (event_type, idempotency_key, created_at). This means idempotency keys are
-- unique per partition (per month with the default monthly cadence), not
-- across all of history. For outbox workloads where duplicate detection
-- happens within seconds-to-hours this is acceptable; if you need
-- cross-partition idempotency you should add a side lookup table.

CREATE TYPE tickr_status AS ENUM
    ('CREATED','HANDLING','SUCCESS','FAILED','RETRYING','DEAD');

CREATE TABLE tickr_messages (
    id              UUID         NOT NULL,
    event_type      TEXT         NOT NULL,
    payload         BYTEA        NOT NULL,
    headers         JSONB        NOT NULL DEFAULT '{}'::jsonb,
    idempotency_key TEXT,
    status          tickr_status NOT NULL,
    attempt         INT          NOT NULL DEFAULT 0,
    max_attempts    INT          NOT NULL DEFAULT 10,
    process_at      TIMESTAMPTZ  NOT NULL,
    claimed_until   TIMESTAMPTZ,
    claimed_by      TEXT,
    last_error      TEXT,
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ  NOT NULL DEFAULT now(),
    completed_at    TIMESTAMPTZ,
    PRIMARY KEY (id, created_at)
) PARTITION BY RANGE (created_at);

-- Catch-all partition. Should normally be empty: EnsurePartitions creates
-- monthly partitions ahead of the current time so live writes land in real
-- partitions. Anything that ends up here cannot be dropped via
-- DROP PARTITION (you'd need DELETE) — treat its size as a warning signal.
CREATE TABLE tickr_messages_default PARTITION OF tickr_messages DEFAULT;

CREATE INDEX tickr_msg_claim_idx
    ON tickr_messages (event_type, process_at, id)
    WHERE status IN ('CREATED','RETRYING');

CREATE INDEX tickr_msg_lease_idx
    ON tickr_messages (claimed_until)
    WHERE status = 'HANDLING';

-- Idempotency: must include the partition key. See note at top of file.
CREATE UNIQUE INDEX tickr_msg_idem_idx
    ON tickr_messages (event_type, idempotency_key, created_at)
    WHERE idempotency_key IS NOT NULL;

CREATE INDEX tickr_msg_dead_idx
    ON tickr_messages (event_type, completed_at DESC)
    WHERE status = 'DEAD';

CREATE INDEX tickr_msg_purge_idx
    ON tickr_messages (completed_at)
    WHERE status IN ('SUCCESS','DEAD');

CREATE TABLE tickr_history (
    message_id  UUID         NOT NULL,
    seq         INT          NOT NULL,
    from_status tickr_status,
    to_status   tickr_status NOT NULL,
    attempt     INT          NOT NULL,
    error       TEXT,
    worker_id   TEXT,
    at          TIMESTAMPTZ  NOT NULL DEFAULT now(),
    PRIMARY KEY (message_id, seq)
);

CREATE TABLE tickr_schema_version (
    version    INT         PRIMARY KEY,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
