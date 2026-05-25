//go:build bench

// Package benchmarks_test compares tickr's drain & enqueue throughput
// against other Go async-job libraries (River, Gue, Watermill SQL, Asynq)
// on the same workload.
//
// All benchmarks share the same parameters via benchParams() so the
// numbers are comparable. Pin the per-iteration message count with
// -benchtime=Nx, e.g. `-benchtime=20000x` drains 20k messages per run.
package benchmarks_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	tcredis "github.com/testcontainers/testcontainers-go/modules/redis"
	"github.com/testcontainers/testcontainers-go/wait"
)

// benchParams are the knobs every adapter under test sees, so a number
// reported for tickr is directly comparable to River/Gue/Watermill/Asynq.
type benchParams struct {
	Payload      []byte
	EnqueueBatch int           // batched insert chunk for pre-load and Enqueue bench
	BatchSize    int           // per-claim batch size on the consumer
	PoolSize     int           // concurrent handler goroutines
	PollInterval time.Duration // for pull-based libraries
	MaxWait      time.Duration // drain timeout per iteration
}

func defaultParams() benchParams {
	return benchParams{
		Payload:      []byte(`{"k":"bench","note":"drain-throughput-default-payload-64b"}`),
		EnqueueBatch: 500,
		BatchSize:    100,
		PoolSize:     64,
		PollInterval: 10 * time.Millisecond,
		MaxWait:      5 * time.Minute,
	}
}

// newPgDSN spins up a Postgres testcontainer and returns its DSN.
// The container is torn down via b.Cleanup.
func newPgDSN(b *testing.B) string {
	b.Helper()
	ctx := context.Background()
	container, err := tcpostgres.Run(ctx,
		"postgres:16-alpine",
		tcpostgres.WithDatabase("bench"),
		tcpostgres.WithUsername("bench"),
		tcpostgres.WithPassword("bench"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(60*time.Second),
		),
	)
	if err != nil {
		b.Fatalf("start postgres: %v", err)
	}
	b.Cleanup(func() { _ = container.Terminate(ctx) })

	dsn, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		b.Fatalf("dsn: %v", err)
	}
	return dsn
}

// newPgPool returns a pgxpool against a freshly-started Postgres
// testcontainer. Adapters that want raw database/sql should call
// newPgDSN instead.
func newPgPool(b *testing.B) *pgxpool.Pool {
	b.Helper()
	dsn := newPgDSN(b)
	pool, err := pgxpool.New(context.Background(), dsn)
	if err != nil {
		b.Fatalf("pgxpool: %v", err)
	}
	b.Cleanup(pool.Close)
	return pool
}

// newRedisAddr spins up a Redis testcontainer and returns its host:port.
func newRedisAddr(b *testing.B) string {
	b.Helper()
	ctx := context.Background()
	container, err := tcredis.Run(ctx, "redis:7-alpine")
	if err != nil {
		b.Fatalf("start redis: %v", err)
	}
	b.Cleanup(func() { _ = container.Terminate(ctx) })

	host, err := container.Host(ctx)
	if err != nil {
		b.Fatalf("redis host: %v", err)
	}
	port, err := container.MappedPort(ctx, "6379/tcp")
	if err != nil {
		b.Fatalf("redis port: %v", err)
	}
	return fmt.Sprintf("%s:%s", host, port.Port())
}

// reportMsgsPerSec records msgs/sec as a custom benchmark metric.
func reportMsgsPerSec(b *testing.B, n int, elapsed time.Duration) {
	b.Helper()
	b.ReportMetric(float64(n)/elapsed.Seconds(), "msgs/sec")
}
