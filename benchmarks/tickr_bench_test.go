//go:build bench

package benchmarks_test

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ndmt1at21/tickr"
	pgstore "github.com/ndmt1at21/tickr/storage/postgres"
)

func newTickrStore(b *testing.B, opts ...pgstore.Option) *pgstore.Store {
	b.Helper()
	pool := newPgPool(b)
	store := pgstore.New(pool, opts...)
	if err := store.ApplyMigrations(context.Background()); err != nil {
		b.Fatalf("apply migrations: %v", err)
	}
	return store
}

func BenchmarkTickrEnqueue(b *testing.B) {
	p := defaultParams()
	store := newTickrStore(b)

	eventType := fmt.Sprintf("bench.enqueue.tickr.%d", time.Now().UnixNano())
	ctx, cancel := context.WithTimeout(context.Background(), p.MaxWait)
	defer cancel()

	batch := make([]tickr.EnqueueParams, 0, p.EnqueueBatch)

	b.SetBytes(int64(len(p.Payload)))
	b.ResetTimer()
	start := time.Now()

	enqueued := 0
	for enqueued < b.N {
		n := p.EnqueueBatch
		if r := b.N - enqueued; r < n {
			n = r
		}
		batch = batch[:0]
		now := time.Now()
		for i := 0; i < n; i++ {
			batch = append(batch, tickr.EnqueueParams{
				EventType:   eventType,
				Payload:     p.Payload,
				MaxAttempts: 1,
				ProcessAt:   now,
			})
		}
		if _, err := store.EnqueueBatch(ctx, tickr.Tx{}, batch); err != nil {
			b.Fatalf("enqueue: %v", err)
		}
		enqueued += n
	}
	elapsed := time.Since(start)
	b.StopTimer()
	reportMsgsPerSec(b, b.N, elapsed)
}

func BenchmarkTickrDrain(b *testing.B) {
	runTickrDrain(b, "bench.drain.tickr")
}

// BenchmarkTickrDrainHistoryOff runs the same drain workload as
// BenchmarkTickrDrain but with the Postgres adapter configured to skip
// per-transition writes to tickr_history (HistoryOff). Isolates the
// throughput cost of the audit log on the hot path.
func BenchmarkTickrDrainHistoryOff(b *testing.B) {
	runTickrDrain(b, "bench.drain.tickr.nohist", pgstore.WithHistoryPolicy(pgstore.HistoryOff))
}

func runTickrDrain(b *testing.B, eventPrefix string, opts ...pgstore.Option) {
	b.Helper()
	p := defaultParams()
	store := newTickrStore(b, opts...)
	eventType := fmt.Sprintf("%s.%d", eventPrefix, time.Now().UnixNano())

	b.StopTimer()
	setupCtx, setupCancel := context.WithTimeout(context.Background(), p.MaxWait)
	defer setupCancel()

	batch := make([]tickr.EnqueueParams, 0, p.EnqueueBatch)
	enqueued := 0
	for enqueued < b.N {
		n := p.EnqueueBatch
		if r := b.N - enqueued; r < n {
			n = r
		}
		batch = batch[:0]
		now := time.Now()
		for i := 0; i < n; i++ {
			batch = append(batch, tickr.EnqueueParams{
				EventType:   eventType,
				Payload:     p.Payload,
				MaxAttempts: 1,
				ProcessAt:   now,
			})
		}
		if _, err := store.EnqueueBatch(setupCtx, tickr.Tx{}, batch); err != nil {
			b.Fatalf("pre-enqueue: %v", err)
		}
		enqueued += n
	}

	var processed atomic.Int64
	done := make(chan struct{})
	target := int64(b.N)

	registry := tickr.NewRegistry()
	if err := registry.On(eventType, func(_ context.Context, _ *tickr.InboundMessage) error {
		if processed.Add(1) == target {
			close(done)
		}
		return nil
	}, tickr.WithMaxAttempts(1)); err != nil {
		b.Fatalf("register handler: %v", err)
	}

	worker, err := tickr.NewWorker(tickr.WorkerConfig{
		Storage:          store,
		Registry:         registry,
		WorkerID:         fmt.Sprintf("bench-tickr-%d", time.Now().UnixNano()),
		BatchSize:        p.BatchSize,
		PoolSize:         p.PoolSize,
		PollInterval:     p.PollInterval,
		DisableReclaimer: true,
		DisableJanitor:   true,
	})
	if err != nil {
		b.Fatalf("new worker: %v", err)
	}

	runCtx, runCancel := context.WithCancel(context.Background())
	defer runCancel()
	workerErr := make(chan error, 1)

	b.SetBytes(int64(len(p.Payload)))
	b.StartTimer()
	start := time.Now()

	go func() { workerErr <- worker.Start(runCtx) }()

	select {
	case <-done:
	case <-time.After(p.MaxWait):
		b.StopTimer()
		runCancel()
		<-workerErr
		b.Fatalf("drain timeout after %s: %d/%d", p.MaxWait, processed.Load(), b.N)
	}
	elapsed := time.Since(start)
	b.StopTimer()
	runCancel()
	<-workerErr
	reportMsgsPerSec(b, b.N, elapsed)
}
