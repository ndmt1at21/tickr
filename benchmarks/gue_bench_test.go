//go:build bench

package benchmarks_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/vgarvardt/gue/v5"
	"github.com/vgarvardt/gue/v5/adapter/pgxv5"
)

// gueSchema is the standard gue v5 jobs table. Gue does not expose a
// migrator on its public API, so the bench applies the canonical schema
// inline. Keep this in sync with vgarvardt/gue/v5/migrations/schema.sql.
const gueSchema = `
CREATE TABLE IF NOT EXISTS gue_jobs (
    job_id        TEXT        NOT NULL PRIMARY KEY,
    priority      SMALLINT    NOT NULL,
    run_at        TIMESTAMPTZ NOT NULL,
    job_type      TEXT        NOT NULL,
    args          BYTEA       NOT NULL,
    error_count   INTEGER     NOT NULL DEFAULT 0,
    last_error    TEXT,
    queue         TEXT        NOT NULL,
    created_at    TIMESTAMPTZ NOT NULL,
    updated_at    TIMESTAMPTZ NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_gue_jobs_selector ON gue_jobs (queue, run_at, priority);
`

func setupGueClient(b *testing.B) *gue.Client {
	b.Helper()
	ctx := context.Background()
	pool := newPgPool(b)
	if _, err := pool.Exec(ctx, gueSchema); err != nil {
		b.Fatalf("apply gue schema: %v", err)
	}
	c, err := gue.NewClient(pgxv5.NewConnPool(pool))
	if err != nil {
		b.Fatalf("gue.NewClient: %v", err)
	}
	return c
}

func BenchmarkGueEnqueue(b *testing.B) {
	p := defaultParams()
	c := setupGueClient(b)

	ctx, cancel := context.WithTimeout(context.Background(), p.MaxWait)
	defer cancel()

	jobs := make([]*gue.Job, 0, p.EnqueueBatch)

	b.SetBytes(int64(len(p.Payload)))
	b.ResetTimer()
	start := time.Now()

	enqueued := 0
	for enqueued < b.N {
		n := p.EnqueueBatch
		if r := b.N - enqueued; r < n {
			n = r
		}
		jobs = jobs[:0]
		for i := 0; i < n; i++ {
			jobs = append(jobs, &gue.Job{Type: "bench", Args: p.Payload})
		}
		if err := c.EnqueueBatch(ctx, jobs); err != nil {
			b.Fatalf("gue enqueue: %v", err)
		}
		enqueued += n
	}
	elapsed := time.Since(start)
	b.StopTimer()
	reportMsgsPerSec(b, b.N, elapsed)
}

func BenchmarkGueDrain(b *testing.B) {
	p := defaultParams()
	c := setupGueClient(b)

	target := int64(b.N)
	var processed atomic.Int64
	done := make(chan struct{})

	wm := gue.WorkMap{
		"bench": func(_ context.Context, _ *gue.Job) error {
			if processed.Add(1) == target {
				select {
				case <-done:
				default:
					close(done)
				}
			}
			return nil
		},
	}

	wp, err := gue.NewWorkerPool(c, wm, p.PoolSize,
		gue.WithPoolPollInterval(p.PollInterval),
	)
	if err != nil {
		b.Fatalf("gue worker pool: %v", err)
	}

	// Pre-enqueue.
	b.StopTimer()
	setupCtx, setupCancel := context.WithTimeout(context.Background(), p.MaxWait)
	defer setupCancel()

	jobs := make([]*gue.Job, 0, p.EnqueueBatch)
	enqueued := 0
	for enqueued < b.N {
		n := p.EnqueueBatch
		if r := b.N - enqueued; r < n {
			n = r
		}
		jobs = jobs[:0]
		for i := 0; i < n; i++ {
			jobs = append(jobs, &gue.Job{Type: "bench", Args: p.Payload})
		}
		if err := c.EnqueueBatch(setupCtx, jobs); err != nil {
			b.Fatalf("pre-enqueue: %v", err)
		}
		enqueued += n
	}

	runCtx, runCancel := context.WithCancel(context.Background())
	defer runCancel()
	runErr := make(chan error, 1)

	b.SetBytes(int64(len(p.Payload)))
	b.StartTimer()
	start := time.Now()

	go func() { runErr <- wp.Run(runCtx) }()

	select {
	case <-done:
	case <-time.After(p.MaxWait):
		b.StopTimer()
		runCancel()
		<-runErr
		b.Fatalf("drain timeout: %d/%d", processed.Load(), b.N)
	}
	elapsed := time.Since(start)
	b.StopTimer()
	runCancel()
	<-runErr

	reportMsgsPerSec(b, b.N, elapsed)
}
