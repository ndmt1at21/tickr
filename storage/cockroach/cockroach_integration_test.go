//go:build integration

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

func TestCockroachSuite(t *testing.T) {
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
		t.Fatalf("start cockroach container: %v", err)
	}
	t.Cleanup(func() { _ = container.Terminate(ctx) })

	host, err := container.Host(ctx)
	if err != nil {
		t.Fatalf("host: %v", err)
	}
	port, err := container.MappedPort(ctx, "26257/tcp")
	if err != nil {
		t.Fatalf("port: %v", err)
	}
	dsn := fmt.Sprintf("postgres://root@%s:%s/defaultdb?sslmode=disable", host, port.Port())

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("pgxpool: %v", err)
	}
	t.Cleanup(pool.Close)

	store := crstore.New(pool)
	if err := store.ApplyMigrations(ctx); err != nil {
		t.Fatalf("apply migrations: %v", err)
	}
	// CockroachDB does not support LISTEN/NOTIFY.
	storagetest.RunSuite(t, store, storagetest.Capabilities{SupportsNotifier: false})
}
