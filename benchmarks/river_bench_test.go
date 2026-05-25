//go:build bench

package benchmarks_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivermigrate"
)

// benchArgs mirrors the 64-byte JSON payload tickr's harness uses so the
// wire shape is comparable across libraries.
type benchArgs struct {
	K    string `json:"k"`
	Note string `json:"note"`
}

func (benchArgs) Kind() string { return "bench" }

type benchWorker struct {
	river.WorkerDefaults[benchArgs]
	processed *atomic.Int64
	target    int64
	done      chan struct{}
}

func (w *benchWorker) Work(_ context.Context, _ *river.Job[benchArgs]) error {
	if w.processed.Add(1) == w.target {
		select {
		case <-w.done:
		default:
			close(w.done)
		}
	}
	return nil
}

func setupRiverClient(b *testing.B, p benchParams, workers *river.Workers) *river.Client[pgx.Tx] {
	b.Helper()
	ctx := context.Background()
	pool := newPgPool(b)
	driver := riverpgxv5.New(pool)

	migrator, err := rivermigrate.New(driver, nil)
	if err != nil {
		b.Fatalf("river migrator: %v", err)
	}
	if _, err := migrator.Migrate(ctx, rivermigrate.DirectionUp, nil); err != nil {
		b.Fatalf("river migrate: %v", err)
	}

	cfg := &river.Config{
		Queues: map[string]river.QueueConfig{
			river.QueueDefault: {MaxWorkers: p.PoolSize},
		},
		Workers:           workers,
		FetchCooldown:     p.PollInterval,
		FetchPollInterval: p.PollInterval,
	}
	client, err := river.NewClient(driver, cfg)
	if err != nil {
		b.Fatalf("river client: %v", err)
	}
	return client
}

func BenchmarkRiverEnqueue(b *testing.B) {
	p := defaultParams()
	// River requires the job's Kind to be registered on the client's
	// Workers bundle even when we only call InsertMany. Register a no-op.
	workers := river.NewWorkers()
	if err := river.AddWorkerSafely(workers, &benchWorker{
		processed: new(atomic.Int64),
		target:    -1, // never closes done
		done:      make(chan struct{}),
	}); err != nil {
		b.Fatalf("add worker: %v", err)
	}
	client := setupRiverClient(b, p, workers)

	ctx, cancel := context.WithTimeout(context.Background(), p.MaxWait)
	defer cancel()

	args := benchArgs{K: "bench", Note: "drain-throughput-default-payload-64b"}
	params := make([]river.InsertManyParams, 0, p.EnqueueBatch)

	b.SetBytes(int64(len(p.Payload)))
	b.ResetTimer()
	start := time.Now()

	enqueued := 0
	for enqueued < b.N {
		n := p.EnqueueBatch
		if r := b.N - enqueued; r < n {
			n = r
		}
		params = params[:0]
		for i := 0; i < n; i++ {
			params = append(params, river.InsertManyParams{Args: args})
		}
		if _, err := client.InsertMany(ctx, params); err != nil {
			b.Fatalf("insert-many: %v", err)
		}
		enqueued += n
	}
	elapsed := time.Since(start)
	b.StopTimer()
	reportMsgsPerSec(b, b.N, elapsed)
}

func BenchmarkRiverDrain(b *testing.B) {
	p := defaultParams()
	target := int64(b.N)
	var processed atomic.Int64
	done := make(chan struct{})

	workers := river.NewWorkers()
	if err := river.AddWorkerSafely(workers, &benchWorker{
		processed: &processed,
		target:    target,
		done:      done,
	}); err != nil {
		b.Fatalf("add worker: %v", err)
	}
	client := setupRiverClient(b, p, workers)

	ctx, cancel := context.WithTimeout(context.Background(), p.MaxWait)
	defer cancel()

	b.StopTimer()
	args := benchArgs{K: "bench", Note: "drain-throughput-default-payload-64b"}
	params := make([]river.InsertManyParams, 0, p.EnqueueBatch)
	enqueued := 0
	for enqueued < b.N {
		n := p.EnqueueBatch
		if r := b.N - enqueued; r < n {
			n = r
		}
		params = params[:0]
		for i := 0; i < n; i++ {
			params = append(params, river.InsertManyParams{Args: args})
		}
		if _, err := client.InsertMany(ctx, params); err != nil {
			b.Fatalf("pre-enqueue: %v", err)
		}
		enqueued += n
	}

	b.SetBytes(int64(len(p.Payload)))
	b.StartTimer()
	start := time.Now()

	if err := client.Start(ctx); err != nil {
		b.Fatalf("client.Start: %v", err)
	}

	select {
	case <-done:
	case <-time.After(p.MaxWait):
		b.StopTimer()
		shutdownCtx, sc := context.WithTimeout(context.Background(), 10*time.Second)
		defer sc()
		_ = client.Stop(shutdownCtx)
		b.Fatalf("drain timeout: %d/%d", processed.Load(), b.N)
	}
	elapsed := time.Since(start)
	b.StopTimer()

	shutdownCtx, sc := context.WithTimeout(context.Background(), 10*time.Second)
	defer sc()
	_ = client.Stop(shutdownCtx)
	reportMsgsPerSec(b, b.N, elapsed)
}
