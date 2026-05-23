// email-fanout demonstrates outbox-driven webhook delivery: a producer
// inserts notify rows inside its own business tx, and a worker dispatches
// them to a downstream HTTP endpoint with exponential backoff, treating
// 4xx as permanent (dead-letter) and 5xx/network errors as retryable.
//
// Run:
//
//	docker run --rm -p 5432:5432 -e POSTGRES_PASSWORD=tickr -e POSTGRES_USER=tickr -e POSTGRES_DB=tickr postgres:16
//	go run ./examples/email-fanout
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ndmt1at21/tickr"
	jsoncodec "github.com/ndmt1at21/tickr/codec/json"
	pgstore "github.com/ndmt1at21/tickr/storage/postgres"
)

type EmailNotification struct {
	To      string `json:"to"`
	Subject string `json:"subject"`
	Body    string `json:"body"`
	HookURL string `json:"hook_url"`
}

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

	client, _ := tickr.NewClient(tickr.ClientConfig{Storage: store})

	reg := tickr.NewRegistry()
	_ = reg.On("email.send",
		jsoncodec.Wrap(deliverEmail),
		tickr.WithMaxAttempts(8),
		tickr.WithAttemptTimeout(15*time.Second),
	)

	w, _ := tickr.NewWorker(tickr.WorkerConfig{
		Storage:  store,
		Registry: reg,
		Stats:    tickr.StatsPolicy{Interval: 10 * time.Second},
	})

	// Enqueue one demo email — substitute for whatever your producer does.
	payload, _ := jsoncodec.Encode(EmailNotification{
		To:      "user@example.com",
		Subject: "Welcome",
		Body:    "Hello from tickr",
		HookURL: "https://httpbin.org/status/200",
	})
	if _, err := client.Enqueue(ctx, tickr.Tx{}, tickr.Message{
		Type:           "email.send",
		Payload:        payload,
		IdempotencyKey: "demo-1",
	}); err != nil {
		log.Fatal(err)
	}

	log.Println("worker starting; ^C to stop")
	if err := w.Start(ctx); err != nil {
		log.Fatal(err)
	}
}

// deliverEmail POSTs the payload to the registered webhook. The retry
// classification is the interesting bit: 4xx is permanent (DeadLetter),
// 5xx and transport errors are retryable, and a Retry-After hint from the
// server overrides the local RetryPolicy.
func deliverEmail(ctx context.Context, _ *tickr.InboundMessage, body EmailNotification) error {
	buf, _ := json.Marshal(body)
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, body.HookURL, bytes.NewReader(buf))
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err // transport error — retry per RetryPolicy
	}
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 512))

	switch {
	case resp.StatusCode >= 200 && resp.StatusCode < 300:
		return nil

	case resp.StatusCode == http.StatusTooManyRequests, resp.StatusCode == http.StatusServiceUnavailable:
		if d, ok := parseRetryAfter(resp.Header.Get("Retry-After")); ok {
			return tickr.RetryAfter(d, fmt.Errorf("server asked us to back off: %s", resp.Status))
		}
		return fmt.Errorf("server busy: %s: %s", resp.Status, respBody)

	case resp.StatusCode >= 400 && resp.StatusCode < 500:
		// 4xx is a client error — retrying won't help.
		return tickr.DeadLetter(fmt.Errorf("permanent: %s: %s", resp.Status, respBody))

	default:
		return fmt.Errorf("transient: %s: %s", resp.Status, respBody)
	}
}

func parseRetryAfter(h string) (time.Duration, bool) {
	if h == "" {
		return 0, false
	}
	// Spec allows seconds or HTTP-date — handle only the seconds form here.
	var secs int
	if _, err := fmt.Sscanf(h, "%d", &secs); err == nil && secs >= 0 {
		return time.Duration(secs) * time.Second, true
	}
	return 0, false
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
