//go:build bench

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

func BenchmarkPostgresDrain(b *testing.B) {
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
		b.Fatalf("start postgres container: %v", err)
	}
	b.Cleanup(func() { _ = container.Terminate(ctx) })

	dsn, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		b.Fatalf("connection string: %v", err)
	}
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		b.Fatalf("pgxpool: %v", err)
	}
	b.Cleanup(pool.Close)

	store := pgstore.New(pool)
	if err := store.ApplyMigrations(ctx); err != nil {
		b.Fatalf("apply migrations: %v", err)
	}

	storagetest.RunDrainBenchmark(b, store, storagetest.BenchOptions{})
}
