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

	mysqlstore "github.com/ndmt1at21/tickr/storage/mysql"
	"github.com/ndmt1at21/tickr/storage/internal/storagetest"
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
	// briefly until the server settles.
	pingCtx, pingCancel := context.WithTimeout(ctx, 30*time.Second)
	defer pingCancel()
	for {
		if err := db.PingContext(pingCtx); err == nil {
			break
		} else if pingCtx.Err() != nil {
			t.Fatalf("ping: %v", err)
		}
		time.Sleep(500 * time.Millisecond)
	}

	store := mysqlstore.New(db)
	if err := store.ApplyMigrations(ctx); err != nil {
		t.Fatalf("apply migrations: %v", err)
	}
	storagetest.RunSuite(t, store, storagetest.Capabilities{})
}
