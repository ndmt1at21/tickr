package storagetest

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ndmt1at21/tickr"
)

// BenchOptions tunes a drain-throughput benchmark.
//
// All zero-value fields fall back to defaults documented per field.
type BenchOptions struct {
	// Payload is sent verbatim with every message. Defaults to a 64-byte
	// JSON-shaped blob.
	Payload []byte

	// EnqueueBatch is the EnqueueBatch chunk size used when pre-loading the
	// outbox. Defaults to 500. Does not affect what the worker measures.
	EnqueueBatch int

	// BatchSize is forwarded to WorkerConfig.BatchSize (Claim batch size).
	// Defaults to 100.
	BatchSize int

	// PoolSize is forwarded to WorkerConfig.PoolSize (max concurrent
	// handlers). Defaults to 64.
	PoolSize int

	// PollInterval is forwarded to WorkerConfig.PollInterval. Defaults to
	// 10ms so that small drains aren't dominated by poll latency.
	PollInterval time.Duration

	// MaxWait caps how long the harness waits for the drain to complete
	// before failing the benchmark. Defaults to 5 minutes.
	MaxWait time.Duration
}

func (o BenchOptions) withDefaults() BenchOptions {
	if o.Payload == nil {
		o.Payload = []byte(`{"k":"bench","note":"drain-throughput-default-payload-64b"}`)
	}
	if o.EnqueueBatch <= 0 {
		o.EnqueueBatch = 500
	}
	if o.BatchSize <= 0 {
		o.BatchSize = 100
	}
	if o.PoolSize <= 0 {
		o.PoolSize = 64
	}
	if o.PollInterval <= 0 {
		o.PollInterval = 10 * time.Millisecond
	}
	if o.MaxWait <= 0 {
		o.MaxWait = 5 * time.Minute
	}
	return o
}

// RunDrainBenchmark measures end-to-end drain throughput against store.
//
// Each invocation pre-enqueues b.N messages under a unique event type, then
// starts a Worker with a no-op handler and waits for all b.N messages to
// reach SUCCESS. The benchmark timer covers worker start + drain; pre-load
// is excluded via b.StopTimer/StartTimer.
//
// Reports msgs/sec as a custom metric. ns/op is time per message.
//
// Use -benchtime=Nx to pin the message count exactly, e.g. -benchtime=20000x
// to drain 20k messages per iteration.
func RunDrainBenchmark(b *testing.B, store tickr.Storage, opts BenchOptions) {
	b.Helper()
	if b.N == 0 {
		return
	}
	opts = opts.withDefaults()

	eventType := fmt.Sprintf("bench.drain.%d", time.Now().UnixNano())

	b.StopTimer()

	setupCtx, setupCancel := context.WithTimeout(context.Background(), opts.MaxWait)
	defer setupCancel()

	batch := make([]tickr.EnqueueParams, 0, opts.EnqueueBatch)
	enqueued := 0
	for enqueued < b.N {
		n := opts.EnqueueBatch
		if remaining := b.N - enqueued; remaining < n {
			n = remaining
		}
		batch = batch[:0]
		now := time.Now()
		for i := 0; i < n; i++ {
			batch = append(batch, tickr.EnqueueParams{
				EventType:   eventType,
				Payload:     opts.Payload,
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
		WorkerID:         fmt.Sprintf("bench-%d", time.Now().UnixNano()),
		BatchSize:        opts.BatchSize,
		PoolSize:         opts.PoolSize,
		PollInterval:     opts.PollInterval,
		DisableReclaimer: true,
		DisableJanitor:   true,
	})
	if err != nil {
		b.Fatalf("new worker: %v", err)
	}

	runCtx, runCancel := context.WithCancel(context.Background())
	defer runCancel()

	workerErr := make(chan error, 1)

	b.SetBytes(int64(len(opts.Payload)))
	b.StartTimer()
	start := time.Now()

	go func() { workerErr <- worker.Start(runCtx) }()

	select {
	case <-done:
	case <-time.After(opts.MaxWait):
		b.StopTimer()
		runCancel()
		<-workerErr
		b.Fatalf("drain timeout after %s: processed %d / %d", opts.MaxWait, processed.Load(), b.N)
	}
	elapsed := time.Since(start)
	b.StopTimer()

	runCancel()
	<-workerErr

	b.ReportMetric(float64(b.N)/elapsed.Seconds(), "msgs/sec")
}
