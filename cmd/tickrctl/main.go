// tickrctl is the admin CLI for tickr.
//
// Usage:
//
//	tickrctl --dsn=postgres://... <subcommand> [flags]
//
// Subcommands:
//
//	migrate                       Apply embedded schema migrations.
//	ensure-partitions             Create monthly partitions ahead of time.
//	stats                         Print queue depth by event type / status.
//	dlq list                      List dead-letter messages.
//	dlq requeue <message-id>      Requeue one dead message (moves DEAD -> CREATED).
//	history <message-id>          Print the full transition history for a message.
//	drain                         Block until queue depth reaches zero (or timeout).
//
// Common flags:
//
//	--dsn          Postgres connection string (or env TICKR_DSN).
//	--partitioned  Use the partitioned schema variant (for migrate / ensure-partitions).
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ndmt1at21/tickr"
	pgstore "github.com/ndmt1at21/tickr/storage/postgres"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "tickrctl:", err)
		os.Exit(1)
	}
}

func run() error {
	root := flag.NewFlagSet("tickrctl", flag.ContinueOnError)
	dsn := root.String("dsn", os.Getenv("TICKR_DSN"), "Postgres DSN (or TICKR_DSN env)")
	partitioned := root.Bool("partitioned", false, "Use partitioned schema variant")
	root.Usage = func() {
		fmt.Fprintln(os.Stderr, "Usage: tickrctl [global flags] <subcommand> [subcommand flags]")
		fmt.Fprintln(os.Stderr, "")
		fmt.Fprintln(os.Stderr, "Global flags:")
		root.PrintDefaults()
		fmt.Fprintln(os.Stderr, "")
		fmt.Fprintln(os.Stderr, "Subcommands:")
		fmt.Fprintln(os.Stderr, "  migrate, ensure-partitions, stats, dlq, history, drain")
	}

	if err := root.Parse(os.Args[1:]); err != nil {
		return err
	}
	args := root.Args()
	if len(args) == 0 {
		root.Usage()
		return errors.New("missing subcommand")
	}
	if *dsn == "" {
		return errors.New("missing --dsn (or TICKR_DSN env)")
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pool, err := pgxpool.New(ctx, *dsn)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer pool.Close()

	var storeOpts []pgstore.Option
	if *partitioned {
		storeOpts = append(storeOpts, pgstore.WithPartitioning())
	}
	store := pgstore.New(pool, storeOpts...)

	sub, subArgs := args[0], args[1:]
	switch sub {
	case "migrate":
		return cmdMigrate(ctx, store)
	case "ensure-partitions":
		return cmdEnsurePartitions(ctx, store, subArgs)
	case "stats":
		return cmdStats(ctx, store, subArgs)
	case "dlq":
		return cmdDLQ(ctx, store, subArgs)
	case "history":
		return cmdHistory(ctx, store, subArgs)
	case "drain":
		return cmdDrain(ctx, store, subArgs)
	default:
		root.Usage()
		return fmt.Errorf("unknown subcommand: %s", sub)
	}
}

var _ tickr.Storage = (*pgstore.Store)(nil)
