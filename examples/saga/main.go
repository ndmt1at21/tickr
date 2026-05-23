// saga shows a three-step orchestration on top of tickr: order.created
// triggers a payment.charge, whose success triggers a shipment.create,
// and whose failure triggers an order.refund. Each handler enqueues the
// next message inside the same tx that records the local state change,
// so the workflow is durable end-to-end without an external orchestrator.
//
// Run:
//
//	docker run --rm -p 5432:5432 -e POSTGRES_PASSWORD=tickr -e POSTGRES_USER=tickr -e POSTGRES_DB=tickr postgres:16
//	go run ./examples/saga
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ndmt1at21/tickr"
	jsoncodec "github.com/ndmt1at21/tickr/codec/json"
	pgstore "github.com/ndmt1at21/tickr/storage/postgres"
)

type OrderCreated struct{ OrderID string }
type PaymentCharge struct{ OrderID string }
type ShipmentCreate struct{ OrderID string }
type OrderRefund struct{ OrderID, Reason string }

func main() {
	dsn := envOr("TICKR_DSN", "postgres://tickr:tickr@localhost:5432/tickr?sslmode=disable")
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		log.Fatal(err)
	}
	defer pool.Close()

	store := pgstore.New(pool)
	if err := store.ApplyMigrations(ctx); err != nil {
		log.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
        CREATE TABLE IF NOT EXISTS orders (
            id TEXT PRIMARY KEY,
            status TEXT NOT NULL,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )`); err != nil {
		log.Fatal(err)
	}

	client, _ := tickr.NewClient(tickr.ClientConfig{Storage: store})
	reg := tickr.NewRegistry()

	_ = reg.On("order.created",
		jsoncodec.Wrap(handleOrderCreated(pool, client)),
		tickr.WithMaxAttempts(5))

	_ = reg.On("payment.charge",
		jsoncodec.Wrap(handlePaymentCharge(pool, client)),
		tickr.WithMaxAttempts(5),
		tickr.WithDeadLetterIf(func(err error) bool {
			// Permanent failures (e.g. card declined) shouldn't retry.
			return errors.Is(err, errCardDeclined)
		}))

	_ = reg.On("shipment.create",
		jsoncodec.Wrap(handleShipmentCreate(pool)),
		tickr.WithMaxAttempts(5))

	_ = reg.On("order.refund",
		jsoncodec.Wrap(handleOrderRefund(pool)),
		tickr.WithMaxAttempts(5))

	w, _ := tickr.NewWorker(tickr.WorkerConfig{Storage: store, Registry: reg})

	// Kick off one demo order.
	payload, _ := jsoncodec.Encode(OrderCreated{OrderID: fmt.Sprintf("o-%d", time.Now().UnixNano())})
	if _, err := client.Enqueue(ctx, tickr.Tx{}, tickr.Message{
		Type: "order.created", Payload: payload,
	}); err != nil {
		log.Fatal(err)
	}

	log.Println("worker starting; ^C to stop")
	if err := w.Start(ctx); err != nil {
		log.Fatal(err)
	}
}

func handleOrderCreated(pool *pgxpool.Pool, client *tickr.Client) jsoncodec.Handler[OrderCreated] {
	return func(ctx context.Context, _ *tickr.InboundMessage, body OrderCreated) error {
		return withTx(ctx, pool, func(tx pgx.Tx) error {
			if _, err := tx.Exec(ctx, `
                INSERT INTO orders (id, status) VALUES ($1, 'created')
                ON CONFLICT (id) DO UPDATE SET status='created', updated_at=now()`,
				body.OrderID); err != nil {
				return err
			}
			payload, _ := jsoncodec.Encode(PaymentCharge{OrderID: body.OrderID})
			_, err := client.Enqueue(ctx, pgstore.WrapTx(tx), tickr.Message{
				Type:           "payment.charge",
				Payload:        payload,
				IdempotencyKey: "charge:" + body.OrderID,
			})
			if err != nil && !tickr.IsDuplicate(err) {
				return err
			}
			log.Printf("[order.created] %s → enqueued payment.charge", body.OrderID)
			return nil
		})
	}
}

var errCardDeclined = errors.New("card declined")

func handlePaymentCharge(pool *pgxpool.Pool, client *tickr.Client) jsoncodec.Handler[PaymentCharge] {
	return func(ctx context.Context, msg *tickr.InboundMessage, body PaymentCharge) error {
		// Simulate flaky upstream: 20% transient errors, 5% card declines.
		switch n := rand.Intn(100); {
		case n < 5:
			return errCardDeclined
		case n < 25:
			return errors.New("payment processor 503 (transient)")
		}
		return withTx(ctx, pool, func(tx pgx.Tx) error {
			if _, err := tx.Exec(ctx, `UPDATE orders SET status='paid', updated_at=now() WHERE id=$1`, body.OrderID); err != nil {
				return err
			}
			payload, _ := jsoncodec.Encode(ShipmentCreate{OrderID: body.OrderID})
			_, err := client.Enqueue(ctx, pgstore.WrapTx(tx), tickr.Message{
				Type:           "shipment.create",
				Payload:        payload,
				IdempotencyKey: "ship:" + body.OrderID,
			})
			if err != nil && !tickr.IsDuplicate(err) {
				return err
			}
			log.Printf("[payment.charge] %s → paid, enqueued shipment.create (attempt %d)", body.OrderID, msg.Attempt)
			return nil
		})
	}
}

func handleShipmentCreate(pool *pgxpool.Pool) jsoncodec.Handler[ShipmentCreate] {
	return func(ctx context.Context, _ *tickr.InboundMessage, body ShipmentCreate) error {
		_, err := pool.Exec(ctx, `UPDATE orders SET status='shipped', updated_at=now() WHERE id=$1`, body.OrderID)
		if err == nil {
			log.Printf("[shipment.create] %s → shipped", body.OrderID)
		}
		return err
	}
}

func handleOrderRefund(pool *pgxpool.Pool) jsoncodec.Handler[OrderRefund] {
	return func(ctx context.Context, _ *tickr.InboundMessage, body OrderRefund) error {
		_, err := pool.Exec(ctx, `UPDATE orders SET status='refunded', updated_at=now() WHERE id=$1`, body.OrderID)
		if err == nil {
			log.Printf("[order.refund] %s → refunded (%s)", body.OrderID, body.Reason)
		}
		return err
	}
}

func withTx(ctx context.Context, pool *pgxpool.Pool, fn func(pgx.Tx) error) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := fn(tx); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
