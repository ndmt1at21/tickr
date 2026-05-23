//go:build bench

package cockroach_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	crstore "github.com/ndmt1at21/tickr/storage/cockroach"
	"github.com/ndmt1at21/tickr/storage/internal/storagetest"
)

func BenchmarkCockroachDrain(b *testing.B) {
	ctx := context.Background()
	req := testcontainers.ContainerRequest{
		Image:        "cockroachdb/cockroach:latest-v23.2",
		ExposedPorts: []string{"26257/tcp", "8080/tcp"},
		Cmd:          []string{"start-single-node", "--insecure", "--accept-sql-without-tls"},
		WaitingFor: wait.ForHTTP("/health?ready=1").
			WithPort("8080/tcp").
			WithStartupTimeout(120 * time.Second),
	}
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		b.Fatalf("start cockroach container: %v", err)
	}
	b.Cleanup(func() { _ = container.Terminate(ctx) })

	host, err := container.Host(ctx)
	if err != nil {
		b.Fatalf("host: %v", err)
	}
	port, err := container.MappedPort(ctx, "26257/tcp")
	if err != nil {
		b.Fatalf("port: %v", err)
	}
	dsn := fmt.Sprintf("postgres://root@%s:%s/defaultdb?sslmode=disable", host, port.Port())

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		b.Fatalf("pgxpool: %v", err)
	}
	b.Cleanup(pool.Close)

	store := crstore.New(pool)
	if err := store.ApplyMigrations(ctx); err != nil {
		b.Fatalf("apply migrations: %v", err)
	}

	storagetest.RunDrainBenchmark(b, store, storagetest.BenchOptions{})
}
