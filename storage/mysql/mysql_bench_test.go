//go:build bench

package mysql_test

import (
	"context"
	"database/sql"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/testcontainers/testcontainers-go"
	tcmysql "github.com/testcontainers/testcontainers-go/modules/mysql"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/ndmt1at21/tickr/storage/internal/storagetest"
	mysqlstore "github.com/ndmt1at21/tickr/storage/mysql"
)

func newBenchStore(b *testing.B) *mysqlstore.Store {
	b.Helper()
	ctx := context.Background()
	container, err := tcmysql.Run(ctx,
		"mysql:8.0",
		tcmysql.WithDatabase("tickr"),
		tcmysql.WithUsername("tickr"),
		tcmysql.WithPassword("tickr"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("ready for connections").
				WithOccurrence(2).
				WithStartupTimeout(120*time.Second),
		),
	)
	if err != nil {
		b.Fatalf("start mysql container: %v", err)
	}
	b.Cleanup(func() { _ = container.Terminate(ctx) })

	dsn, err := container.ConnectionString(ctx, "parseTime=true", "multiStatements=true")
	if err != nil {
		b.Fatalf("connection string: %v", err)
	}
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		b.Fatalf("sql.Open: %v", err)
	}
	b.Cleanup(func() { _ = db.Close() })
	// See mysql_integration_test.go for the rationale on the ping retry.
	pingCtx, pingCancel := context.WithTimeout(ctx, 30*time.Second)
	defer pingCancel()
	for {
		if err := db.PingContext(pingCtx); err == nil {
			break
		} else if pingCtx.Err() != nil {
			b.Fatalf("ping: %v", err)
		}
		time.Sleep(500 * time.Millisecond)
	}

	store := mysqlstore.New(db)
	if err := store.ApplyMigrations(ctx); err != nil {
		b.Fatalf("apply migrations: %v", err)
	}
	return store
}

func BenchmarkMySQLDrain(b *testing.B) {
	store := newBenchStore(b)
	storagetest.RunDrainBenchmark(b, store, storagetest.BenchOptions{})
}

func BenchmarkMySQLEnqueue(b *testing.B) {
	store := newBenchStore(b)
	storagetest.RunEnqueueBenchmark(b, store, storagetest.BenchOptions{})
}
