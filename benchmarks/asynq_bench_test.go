//go:build bench

package benchmarks_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hibiken/asynq"
)

// Asynq has no native batch-insert; producers call Enqueue once per
// task. We keep the same EnqueueBatch loop shape for symmetry with the
// other suites but issue one Enqueue per iteration of the inner loop.
func BenchmarkAsynqEnqueue(b *testing.B) {
	p := defaultParams()
	addr := newRedisAddr(b)
	client := asynq.NewClient(asynq.RedisClientOpt{Addr: addr})
	b.Cleanup(func() { _ = client.Close() })

	task := asynq.NewTask("bench", p.Payload)

	b.SetBytes(int64(len(p.Payload)))
	b.ResetTimer()
	start := time.Now()

	for i := 0; i < b.N; i++ {
		if _, err := client.Enqueue(task); err != nil {
			b.Fatalf("enqueue: %v", err)
		}
	}
	elapsed := time.Since(start)
	b.StopTimer()
	reportMsgsPerSec(b, b.N, elapsed)
}

func BenchmarkAsynqDrain(b *testing.B) {
	p := defaultParams()
	addr := newRedisAddr(b)
	client := asynq.NewClient(asynq.RedisClientOpt{Addr: addr})
	b.Cleanup(func() { _ = client.Close() })

	target := int64(b.N)
	var processed atomic.Int64
	done := make(chan struct{})

	// Pre-enqueue.
	b.StopTimer()
	task := asynq.NewTask("bench", p.Payload)
	for i := 0; i < b.N; i++ {
		if _, err := client.Enqueue(task); err != nil {
			b.Fatalf("pre-enqueue: %v", err)
		}
	}

	srv := asynq.NewServer(asynq.RedisClientOpt{Addr: addr}, asynq.Config{
		Concurrency: p.PoolSize,
	})

	mux := asynq.NewServeMux()
	mux.HandleFunc("bench", func(_ context.Context, _ *asynq.Task) error {
		if processed.Add(1) == target {
			select {
			case <-done:
			default:
				close(done)
			}
		}
		return nil
	})

	b.SetBytes(int64(len(p.Payload)))
	b.StartTimer()
	start := time.Now()

	if err := srv.Start(mux); err != nil {
		b.Fatalf("asynq start: %v", err)
	}

	select {
	case <-done:
	case <-time.After(p.MaxWait):
		b.StopTimer()
		srv.Shutdown()
		b.Fatalf("drain timeout: %d/%d", processed.Load(), b.N)
	}
	elapsed := time.Since(start)
	b.StopTimer()
	srv.Shutdown()
	reportMsgsPerSec(b, b.N, elapsed)
}
