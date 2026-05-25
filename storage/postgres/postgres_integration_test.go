//go:build integration

package postgres_test

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/ndmt1at21/tickr/storage/internal/storagetest"
	pgstore "github.com/ndmt1at21/tickr/storage/postgres"
)

func TestPostgresSuite(t *testing.T) {
	ctx := context.Background()
	container, err := tcpostgres.Run(ctx,
		"postgres:16-alpine",
		tcpostgres.WithDatabase("tickr"),
		tcpostgres.WithUsername("tickr"),
		tcpostgres.WithPassword("tickr"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(60*time.Second),
		),
	)
	if err != nil {
		t.Fatalf("start postgres container: %v", err)
	}
	t.Cleanup(func() { _ = container.Terminate(ctx) })

	dsn, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("connection string: %v", err)
	}
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("pgxpool: %v", err)
	}
	t.Cleanup(pool.Close)

	store := pgstore.New(pool)
	if err := store.ApplyMigrations(ctx); err != nil {
		t.Fatalf("apply migrations: %v", err)
	}

	storagetest.RunSuite(t, store, storagetest.Capabilities{SupportsNotifier: true})
}

// TestPostgresHistoryOff verifies that the HistoryOff policy skips
// tickr_history writes on the hot path while leaving the message state
// machine intact: the existing conformance suite (which only checks
// Status and counts, not history rows) must still pass.
func TestPostgresHistoryOff(t *testing.T) {
	ctx := context.Background()
	container, err := tcpostgres.Run(ctx,
		"postgres:16-alpine",
		tcpostgres.WithDatabase("tickr"),
		tcpostgres.WithUsername("tickr"),
		tcpostgres.WithPassword("tickr"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(60*time.Second),
		),
	)
	if err != nil {
		t.Fatalf("start postgres container: %v", err)
	}
	t.Cleanup(func() { _ = container.Terminate(ctx) })

	dsn, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("connection string: %v", err)
	}
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("pgxpool: %v", err)
	}
	t.Cleanup(pool.Close)

	store := pgstore.New(pool, pgstore.WithHistoryPolicy(pgstore.HistoryOff))
	if err := store.ApplyMigrations(ctx); err != nil {
		t.Fatalf("apply migrations: %v", err)
	}

	// Capability suite verifies Status / counts / transitions still work
	// without history rows.
	storagetest.RunSuite(t, store, storagetest.Capabilities{SupportsNotifier: true})

	// The hot-path transitions (anything originating from HANDLING) must
	// be absent. Admin-path transitions like DEAD→CREATED (Requeue) are
	// still recorded by design — see HistoryPolicy doc.
	var n int
	if err := pool.QueryRow(ctx,
		"SELECT count(*) FROM tickr_history WHERE from_status = 'HANDLING' OR from_status IS NULL",
	).Scan(&n); err != nil {
		t.Fatalf("count hot-path history: %v", err)
	}
	if n != 0 {
		t.Fatalf("HistoryOff still wrote %d hot-path tickr_history rows", n)
	}
}

func TestPostgresPartitionedSuite(t *testing.T) {
	ctx := context.Background()
	container, err := tcpostgres.Run(ctx,
		"postgres:16-alpine",
		tcpostgres.WithDatabase("tickr"),
		tcpostgres.WithUsername("tickr"),
		tcpostgres.WithPassword("tickr"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(60*time.Second),
		),
	)
	if err != nil {
		t.Fatalf("start postgres container: %v", err)
	}
	t.Cleanup(func() { _ = container.Terminate(ctx) })

	dsn, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("connection string: %v", err)
	}
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("pgxpool: %v", err)
	}
	t.Cleanup(pool.Close)

	store := pgstore.New(pool, pgstore.WithPartitioning())
	if err := store.ApplyMigrations(ctx); err != nil {
		t.Fatalf("apply migrations: %v", err)
	}
	if err := store.EnsurePartitions(ctx, 1); err != nil {
		t.Fatalf("ensure partitions: %v", err)
	}

	storagetest.RunSuite(t, store, storagetest.Capabilities{SupportsNotifier: true})
}
