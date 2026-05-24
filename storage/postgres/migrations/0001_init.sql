-- tickr schema: hot table + append-only history.
-- See storage/postgres/schema.sql for the canonical reference.

CREATE TYPE tickr_status AS ENUM
    ('CREATED','HANDLING','SUCCESS','FAILED','RETRYING','DEAD');

CREATE TABLE tickr_messages (
    id              UUID         PRIMARY KEY,
    event_type      TEXT         NOT NULL,
    payload         BYTEA        NOT NULL,
    headers         JSONB        NOT NULL DEFAULT '{}'::jsonb,
    idempotency_key TEXT,
    status          tickr_status NOT NULL,
    attempt         INT          NOT NULL DEFAULT 0,
    max_attempts    INT          NOT NULL DEFAULT 10,
    process_at      TIMESTAMPTZ  NOT NULL,
    shard           SMALLINT     NOT NULL,
    claimed_until   TIMESTAMPTZ,
    claimed_by      TEXT,
    last_error      TEXT,
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ  NOT NULL DEFAULT now(),
    completed_at    TIMESTAMPTZ
);

-- Partial claim index: only pending rows live in this index, keeping it
-- small even at multi-million-row table sizes. The `shard` leading column
-- splits the B-tree head across N subtrees so a burst of inserts with
-- near-identical process_at values doesn't pile up in one hot leaf — see
-- postgres.go (numClaimShards) for the producer / claim-path counterpart.
CREATE INDEX tickr_msg_claim_idx
    ON tickr_messages (shard, event_type, process_at, id)
    WHERE status IN ('CREATED','RETRYING');

-- Lease-expiry index for the reclaimer.
CREATE INDEX tickr_msg_lease_idx
    ON tickr_messages (claimed_until)
    WHERE status = 'HANDLING';

-- Idempotency: scoped per event_type, partial on non-null keys.
CREATE UNIQUE INDEX tickr_msg_idem_idx
    ON tickr_messages (event_type, idempotency_key)
    WHERE idempotency_key IS NOT NULL;

-- DLQ browsing.
CREATE INDEX tickr_msg_dead_idx
    ON tickr_messages (event_type, completed_at DESC)
    WHERE status = 'DEAD';

-- Retention purge.
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
