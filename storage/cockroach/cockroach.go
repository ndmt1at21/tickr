// Package cockroach is the CockroachDB adapter for tickr.
//
// CockroachDB is wire-compatible with PostgreSQL (pgx works as-is) and
// supports the same SQL features tickr relies on — JSONB, partial indexes,
// SELECT ... FOR UPDATE SKIP LOCKED, ENUM types, and prepared statements.
// This adapter therefore reuses the Postgres adapter's queries verbatim
// (via interface embedding) and only overrides the bits that differ:
//
//  1. Leader election. CockroachDB does not implement pg_try_advisory_lock,
//     so this adapter swaps to a tickr_locks row-lock pattern with a
//     60-second TTL.
//
//  2. LISTEN/NOTIFY. CockroachDB does not support LISTEN/NOTIFY at all, so
//     this adapter deliberately does NOT implement tickr.Notifier (workers
//     fall back to pure polling). Interface embedding ensures only the
//     tickr.Storage method set is promoted; the inner Notifier methods on
//     the embedded *postgres.Store are NOT visible through this type.
package cockroach

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ndmt1at21/tickr"
	pgstore "github.com/ndmt1at21/tickr/storage/postgres"
)

// Store is the CockroachDB-backed Storage implementation. Most methods are
// inherited from the embedded tickr.Storage (a *postgres.Store under the
// hood). TryLeaderLock is overridden; Notifier is deliberately not satisfied.
type Store struct {
	tickr.Storage
	pool *pgxpool.Pool
}

// New wraps an existing pgxpool.Pool connected to a CockroachDB cluster.
func New(pool *pgxpool.Pool) *Store {
	return &Store{
		Storage: pgstore.New(pool, pgstore.WithoutNotifier()),
		pool:    pool,
	}
}

// WrapTx adapts a pgx.Tx so it can be passed to Client.Enqueue.
var WrapTx = pgstore.WrapTx

// PurgeHistory forwards to the embedded Postgres adapter. The method is
// declared explicitly because the embedded tickr.Storage interface field
// only promotes methods that are part of tickr.Storage; tickr.HistoryPurger
// is an optional extension that must be re-exposed by the outer type for
// type assertions to succeed.
func (s *Store) PurgeHistory(ctx context.Context, before time.Time, limit int) (int64, error) {
	hp, ok := s.Storage.(tickr.HistoryPurger)
	if !ok {
		return 0, nil
	}
	return hp.PurgeHistory(ctx, before, limit)
}

// ApplyMigrations runs the standard Postgres migrations, then bootstraps the
// tickr_locks table used by the row-lock leader election.
func (s *Store) ApplyMigrations(ctx context.Context) error {
	if err := s.Storage.ApplyMigrations(ctx); err != nil {
		return err
	}
	_, err := s.pool.Exec(ctx, `
        CREATE TABLE IF NOT EXISTS tickr_locks (
            key        STRING       PRIMARY KEY,
            holder     STRING       NOT NULL,
            expires_at TIMESTAMPTZ  NOT NULL
        )`)
	if err != nil {
		return fmt.Errorf("tickr/cockroach: create lock table: %w", err)
	}
	return nil
}

// TryLeaderLock acquires a row-locked lease in tickr_locks. Unlike the
// Postgres advisory lock (auto-released on session close), this lease is
// time-bounded and must be released explicitly via the returned unlock
// func. If a worker crashes between acquire and unlock the lease expires
// after 60 seconds and another worker can take over.
func (s *Store) TryLeaderLock(ctx context.Context, key string) (bool, func(), error) {
	holder := uuid.NewString()
	var got bool
	err := s.pool.QueryRow(ctx, `
        INSERT INTO tickr_locks (key, holder, expires_at)
        VALUES ($1, $2, now() + INTERVAL '60 seconds')
        ON CONFLICT (key) DO UPDATE
          SET holder     = EXCLUDED.holder,
              expires_at = EXCLUDED.expires_at
          WHERE tickr_locks.expires_at < now()
        RETURNING tickr_locks.holder = $2`,
		key, holder).Scan(&got)
	if err != nil {
		// ON CONFLICT DO UPDATE ... WHERE filters out the update when the
		// existing lock is still live, leaving no row to RETURN. Treat that
		// as contention, not an error.
		if errors.Is(err, pgx.ErrNoRows) {
			return false, nil, nil
		}
		return false, nil, fmt.Errorf("tickr/cockroach: try leader lock: %w", err)
	}
	if !got {
		return false, nil, nil
	}
	unlock := func() {
		_, _ = s.pool.Exec(context.Background(),
			`DELETE FROM tickr_locks WHERE key=$1 AND holder=$2`, key, holder)
	}
	return true, unlock, nil
}
