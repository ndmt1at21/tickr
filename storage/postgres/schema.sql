-- Canonical reference of the tickr schema. The authoritative copy lives in
-- migrations/0001_init.sql; this file mirrors it for documentation and IDE
-- tooling (e.g. sqlc, ER diagrams). Keep in sync.

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

-- The leading `shard` column splits the head of the claim index across N
-- subtrees. Producers write a random shard per row; the claim path picks one
-- shard per call, falling back to a full scan if that shard is empty.
CREATE INDEX tickr_msg_claim_idx  ON tickr_messages (shard, event_type, process_at, id) WHERE status IN ('CREATED','RETRYING');
CREATE INDEX tickr_msg_lease_idx  ON tickr_messages (claimed_until)              WHERE status = 'HANDLING';
CREATE UNIQUE INDEX tickr_msg_idem_idx ON tickr_messages (event_type, idempotency_key) WHERE idempotency_key IS NOT NULL;
CREATE INDEX tickr_msg_dead_idx   ON tickr_messages (event_type, completed_at DESC) WHERE status = 'DEAD';
CREATE INDEX tickr_msg_purge_idx  ON tickr_messages (completed_at)               WHERE status IN ('SUCCESS','DEAD');

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
