package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"text/tabwriter"
	"time"

	"github.com/ndmt1at21/tickr"
	pgstore "github.com/ndmt1at21/tickr/storage/postgres"
)

func cmdMigrate(ctx context.Context, store *pgstore.Store) error {
	if err := store.ApplyMigrations(ctx); err != nil {
		return err
	}
	fmt.Println("migrations applied")
	return nil
}

func cmdEnsurePartitions(ctx context.Context, store *pgstore.Store, args []string) error {
	fs := flag.NewFlagSet("ensure-partitions", flag.ContinueOnError)
	ahead := fs.Int("ahead", 3, "Number of months ahead to pre-create partitions")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if err := store.EnsurePartitions(ctx, *ahead); err != nil {
		return err
	}
	fmt.Printf("ensured partitions for current month + %d months ahead\n", *ahead)
	return nil
}

func cmdStats(ctx context.Context, store *pgstore.Store, args []string) error {
	fs := flag.NewFlagSet("stats", flag.ContinueOnError)
	asJSON := fs.Bool("json", false, "Emit JSON instead of a table")
	if err := fs.Parse(args); err != nil {
		return err
	}
	stats, err := store.Stats(ctx)
	if err != nil {
		return err
	}
	if *asJSON {
		return json.NewEncoder(os.Stdout).Encode(stats)
	}
	tw := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "EVENT_TYPE\tSTATUS\tDEPTH")
	for et, byStatus := range stats.ByEventType {
		for status, depth := range byStatus {
			fmt.Fprintf(tw, "%s\t%s\t%d\n", et, status, depth)
		}
	}
	return tw.Flush()
}

func cmdDLQ(ctx context.Context, store *pgstore.Store, args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("dlq: missing action (list|requeue)")
	}
	switch args[0] {
	case "list":
		return cmdDLQList(ctx, store, args[1:])
	case "requeue":
		return cmdDLQRequeue(ctx, store, args[1:])
	default:
		return fmt.Errorf("dlq: unknown action %q", args[0])
	}
}

func cmdDLQList(ctx context.Context, store *pgstore.Store, args []string) error {
	fs := flag.NewFlagSet("dlq list", flag.ContinueOnError)
	eventType := fs.String("type", "", "Filter by event type (empty = all)")
	limit := fs.Int("limit", 50, "Max rows to list")
	since := fs.String("since", "", "ISO-8601 timestamp; list dead rows newer than this (default: 24h ago)")
	asJSON := fs.Bool("json", false, "Emit JSON instead of a table")
	if err := fs.Parse(args); err != nil {
		return err
	}

	var after time.Time
	if *since != "" {
		t, err := time.Parse(time.RFC3339, *since)
		if err != nil {
			return fmt.Errorf("invalid --since: %w", err)
		}
		after = t
	} else {
		after = time.Now().Add(-24 * time.Hour)
	}

	msgs, err := store.ListDead(ctx, *eventType, after, *limit)
	if err != nil {
		return err
	}
	if *asJSON {
		return json.NewEncoder(os.Stdout).Encode(msgs)
	}
	tw := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "ID\tEVENT_TYPE\tATTEMPT\tENQUEUED_AT\tLAST_ERROR")
	for _, m := range msgs {
		fmt.Fprintf(tw, "%s\t%s\t%d\t%s\t%s\n",
			m.ID, m.Type, m.Attempt,
			m.EnqueuedAt.UTC().Format(time.RFC3339),
			truncate(m.LastError, 80))
	}
	return tw.Flush()
}

func cmdDLQRequeue(ctx context.Context, store *pgstore.Store, args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("dlq requeue: missing message id")
	}
	id := tickr.MessageID(args[0])
	if err := store.Requeue(ctx, id, time.Now()); err != nil {
		return err
	}
	fmt.Printf("requeued %s\n", id)
	return nil
}

func cmdHistory(ctx context.Context, store *pgstore.Store, args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("history: missing message id")
	}
	id := tickr.MessageID(args[0])
	hist, err := store.History(ctx, id)
	if err != nil {
		return err
	}
	tw := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "SEQ\tFROM\tTO\tATTEMPT\tAT\tWORKER\tERROR")
	for _, t := range hist {
		fmt.Fprintf(tw, "%d\t%s\t%s\t%d\t%s\t%s\t%s\n",
			t.Seq, t.From, t.To, t.Attempt,
			t.At.UTC().Format(time.RFC3339),
			t.WorkerID, truncate(t.Error, 80))
	}
	return tw.Flush()
}

func cmdDrain(ctx context.Context, store *pgstore.Store, args []string) error {
	fs := flag.NewFlagSet("drain", flag.ContinueOnError)
	wait := fs.Duration("wait", 5*time.Minute, "Maximum time to wait for queue depth to reach zero")
	poll := fs.Duration("poll", 2*time.Second, "Polling interval")
	if err := fs.Parse(args); err != nil {
		return err
	}
	deadline := time.Now().Add(*wait)
	for {
		stats, err := store.Stats(ctx)
		if err != nil {
			return err
		}
		pending := 0
		for _, byStatus := range stats.ByEventType {
			for status, n := range byStatus {
				if status == tickr.StatusCreated ||
					status == tickr.StatusRetrying ||
					status == tickr.StatusHandling ||
					status == tickr.StatusFailed {
					pending += n
				}
			}
		}
		if pending == 0 {
			fmt.Println("drained")
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("drain timeout: %d messages still pending", pending)
		}
		fmt.Printf("waiting: %d pending\n", pending)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(*poll):
		}
	}
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}
