//go:build bench

package benchmarks_test

import (
	"context"
	dbsql "database/sql"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	wsql "github.com/ThreeDotsLabs/watermill-sql/v3/pkg/sql"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill"
	_ "github.com/jackc/pgx/v5/stdlib"
)

func openSQLDB(b *testing.B) *dbsql.DB {
	b.Helper()
	dsn := newPgDSN(b)
	db, err := dbsql.Open("pgx", dsn)
	if err != nil {
		b.Fatalf("sql.Open pgx: %v", err)
	}
	b.Cleanup(func() { _ = db.Close() })
	if err := db.PingContext(context.Background()); err != nil {
		b.Fatalf("ping: %v", err)
	}
	return db
}

func setupWatermill(b *testing.B) (message.Publisher, message.Subscriber, string) {
	b.Helper()
	db := openSQLDB(b)
	logger := watermill.NopLogger{}

	pub, err := wsql.NewPublisher(db, wsql.PublisherConfig{
		SchemaAdapter:        wsql.DefaultPostgreSQLSchema{},
		AutoInitializeSchema: true,
	}, logger)
	if err != nil {
		b.Fatalf("watermill publisher: %v", err)
	}
	sub, err := wsql.NewSubscriber(db, wsql.SubscriberConfig{
		SchemaAdapter:    wsql.DefaultPostgreSQLSchema{},
		OffsetsAdapter:   wsql.DefaultPostgreSQLOffsetsAdapter{},
		InitializeSchema: true,
		ConsumerGroup:    "bench",
	}, logger)
	if err != nil {
		b.Fatalf("watermill subscriber: %v", err)
	}
	topic := fmt.Sprintf("bench_%d", time.Now().UnixNano())
	b.Cleanup(func() {
		_ = pub.Close()
		_ = sub.Close()
	})
	return pub, sub, topic
}

func buildBatch(p benchParams, n int) []*message.Message {
	msgs := make([]*message.Message, n)
	for i := 0; i < n; i++ {
		msgs[i] = message.NewMessage(watermill.NewUUID(), p.Payload)
	}
	return msgs
}

func BenchmarkWatermillEnqueue(b *testing.B) {
	p := defaultParams()
	pub, _, topic := setupWatermill(b)

	b.SetBytes(int64(len(p.Payload)))
	b.ResetTimer()
	start := time.Now()

	enqueued := 0
	for enqueued < b.N {
		n := p.EnqueueBatch
		if r := b.N - enqueued; r < n {
			n = r
		}
		if err := pub.Publish(topic, buildBatch(p, n)...); err != nil {
			b.Fatalf("publish: %v", err)
		}
		enqueued += n
	}
	elapsed := time.Since(start)
	b.StopTimer()
	reportMsgsPerSec(b, b.N, elapsed)
}

func BenchmarkWatermillDrain(b *testing.B) {
	p := defaultParams()
	pub, sub, topic := setupWatermill(b)

	// Pre-enqueue.
	b.StopTimer()
	enqueued := 0
	for enqueued < b.N {
		n := p.EnqueueBatch
		if r := b.N - enqueued; r < n {
			n = r
		}
		if err := pub.Publish(topic, buildBatch(p, n)...); err != nil {
			b.Fatalf("pre-publish: %v", err)
		}
		enqueued += n
	}

	runCtx, runCancel := context.WithCancel(context.Background())
	defer runCancel()

	msgs, err := sub.Subscribe(runCtx, topic)
	if err != nil {
		b.Fatalf("subscribe: %v", err)
	}

	target := int64(b.N)
	var processed atomic.Int64
	done := make(chan struct{})

	b.SetBytes(int64(len(p.Payload)))
	b.StartTimer()
	start := time.Now()

	go func() {
		for msg := range msgs {
			msg.Ack()
			if processed.Add(1) == target {
				select {
				case <-done:
				default:
					close(done)
				}
				return
			}
		}
	}()

	select {
	case <-done:
	case <-time.After(p.MaxWait):
		b.StopTimer()
		runCancel()
		b.Fatalf("drain timeout: %d/%d", processed.Load(), b.N)
	}
	elapsed := time.Since(start)
	b.StopTimer()
	runCancel()
	reportMsgsPerSec(b, b.N, elapsed)
}
