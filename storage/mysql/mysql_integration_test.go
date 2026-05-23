//go:build integration

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

func TestMySQLSuite(t *testing.T) {
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
		t.Fatalf("start mysql container: %v", err)
	}
	t.Cleanup(func() { _ = container.Terminate(ctx) })

	dsn, err := container.ConnectionString(ctx, "parseTime=true", "multiStatements=true")
	if err != nil {
		t.Fatalf("connection string: %v", err)
	}
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	// MySQL containers occasionally accept connections during the "temp
	// init" → "real start" handover and then drop them with EOF; retry
	// until we get several consecutive successful pings on fresh
	// connections so any conns opened during the flaky window are
	// flushed out of the pool before the suite runs.
	pingCtx, pingCancel := context.WithTimeout(ctx, 60*time.Second)
	defer pingCancel()
	const wantStable = 5
	stable := 0
	for stable < wantStable {
		conn, err := db.Conn(pingCtx)
		if err == nil {
			if err := conn.PingContext(pingCtx); err == nil {
				stable++
			} else {
				stable = 0
			}
			_ = conn.Close()
		} else {
			stable = 0
		}
		if pingCtx.Err() != nil {
			t.Fatalf("mysql never stabilized: %v", err)
		}
		if stable < wantStable {
			time.Sleep(500 * time.Millisecond)
		}
	}
	// Drop any idle conns that were opened (and possibly half-broken)
	// during the wait loop. Subsequent operations will open fresh ones.
	db.SetMaxIdleConns(0)
	db.SetMaxIdleConns(2)

	store := mysqlstore.New(db)
	if err := store.ApplyMigrations(ctx); err != nil {
		t.Fatalf("apply migrations: %v", err)
	}
	storagetest.RunSuite(t, store, storagetest.Capabilities{})
}
