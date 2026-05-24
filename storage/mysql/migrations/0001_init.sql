-- tickr schema for MySQL 8.0+ (requires SELECT ... FOR UPDATE SKIP LOCKED).

CREATE TABLE tickr_messages (
    id              CHAR(36)     NOT NULL PRIMARY KEY,
    event_type      VARCHAR(255) NOT NULL,
    payload         LONGBLOB     NOT NULL,
    headers         JSON         NOT NULL,
    idempotency_key VARCHAR(255),
    status          ENUM('CREATED','HANDLING','SUCCESS','FAILED','RETRYING','DEAD') NOT NULL,
    attempt         INT          NOT NULL DEFAULT 0,
    max_attempts    INT          NOT NULL DEFAULT 10,
    process_at      DATETIME(6)  NOT NULL,
    shard           TINYINT      NOT NULL,
    claimed_until   DATETIME(6),
    claimed_by      VARCHAR(255),
    last_error      TEXT,
    created_at      DATETIME(6)  NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    updated_at      DATETIME(6)  NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    completed_at    DATETIME(6),
    -- The `shard` leading column splits the B-tree head across N subtrees so
    -- a burst of inserts with near-identical process_at values doesn't pile
    -- up in one hot leaf. See mysql.go (numClaimShards).
    INDEX tickr_msg_claim_idx (shard, status, event_type, process_at, id),
    INDEX tickr_msg_lease_idx (status, claimed_until),
    INDEX tickr_msg_dead_idx (status, event_type, completed_at),
    INDEX tickr_msg_purge_idx (status, completed_at),
    -- MySQL treats NULL as distinct in unique indexes — exactly the
    -- behaviour we want for optional idempotency keys.
    UNIQUE KEY tickr_msg_idem_idx (event_type, idempotency_key)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE tickr_history (
    message_id  CHAR(36)     NOT NULL,
    seq         INT          NOT NULL,
    from_status VARCHAR(16),
    to_status   VARCHAR(16)  NOT NULL,
    attempt     INT          NOT NULL,
    error       TEXT,
    worker_id   VARCHAR(255),
    at          DATETIME(6)  NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    PRIMARY KEY (message_id, seq)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
