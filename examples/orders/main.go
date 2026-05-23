// orders is an end-to-end demo of the tickr library: an HTTP service
// accepts order POSTs, writes them transactionally to an orders table
// together with an outbox row via tickr.Client.Enqueue, and a worker
// pool in the same binary picks up those outbox rows and runs the
// "order.created" handler.
//
// Observability:
//   - Prometheus metrics on :8080/metrics
//   - OpenTelemetry traces exported via OTLP/HTTP to Tempo at $OTEL_EXPORTER_OTLP_ENDPOINT
//   - traceparent propagation through tickr message headers
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"

	"github.com/ndmt1at21/tickr"
	tprom "github.com/ndmt1at21/tickr/metrics/prom"
	pgstore "github.com/ndmt1at21/tickr/storage/postgres"
	totel "github.com/ndmt1at21/tickr/tracing/otel"
	"github.com/ndmt1at21/tickr/ui"
)

type orderCreated struct {
	OrderID    string  `json:"order_id"`
	CustomerID string  `json:"customer_id"`
	Total      float64 `json:"total"`
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	log := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	// --- OpenTelemetry ----------------------------------------------------
	shutdownTracing, err := initTracing(ctx)
	if err != nil {
		log.Error("init tracing", "err", err)
		os.Exit(1)
	}
	defer func() { _ = shutdownTracing(context.Background()) }()

	// --- Prometheus --------------------------------------------------------
	promReg := prometheus.NewRegistry()
	tickrMetrics := tprom.New(promReg)
	tickrTracer := totel.New()

	// --- Postgres ----------------------------------------------------------
	dsn := envOr("DATABASE_URL", "postgres://postgres:postgres@postgres:5432/postgres?sslmode=disable")
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		log.Error("connect postgres", "err", err)
		os.Exit(1)
	}
	defer pool.Close()

	store := pgstore.New(pool)
	if _, err := tickr.NewMigrator(store); err != nil {
		log.Error("new migrator", "err", err)
		os.Exit(1)
	}
	if err := store.ApplyMigrations(ctx); err != nil {
		log.Error("apply migrations", "err", err)
		os.Exit(1)
	}
	if err := ensureOrdersTable(ctx, pool); err != nil {
		log.Error("ensure orders table", "err", err)
		os.Exit(1)
	}

	// --- tickr Client + Worker --------------------------------------------
	client, err := tickr.NewClient(tickr.ClientConfig{
		Storage: store,
		Logger:  &slogAdapter{l: log},
		Metrics: tickrMetrics,
		Tracer:  tickrTracer,
	})
	if err != nil {
		log.Error("new client", "err", err)
		os.Exit(1)
	}

	registry := tickr.NewRegistry()
	if err := tickr.On(registry, "order.created",
		func(ctx context.Context, msg *tickr.InboundMessage, body orderCreated) error {
			log.InfoContext(ctx, "handling order.created",
				"message_id", msg.ID, "attempt", msg.Attempt,
				"order_id", body.OrderID, "total", body.Total)
			// Simulate occasional transient failures so retries are visible
			// in the dashboard.
			if msg.Attempt == 1 && body.Total > 1000 {
				return errors.New("simulated downstream blip")
			}
			return nil
		},
		tickr.WithMaxAttempts(5),
		tickr.WithAttemptTimeout(10*time.Second),
	); err != nil {
		log.Error("register handler", "err", err)
		os.Exit(1)
	}

	worker, err := tickr.NewWorker(tickr.WorkerConfig{
		Storage:  store,
		Registry: registry,
		Logger:   &slogAdapter{l: log},
		Metrics:  tickrMetrics,
		Tracer:   tickrTracer,
		Stats:    tickr.StatsPolicy{Interval: 10 * time.Second},
	})
	if err != nil {
		log.Error("new worker", "err", err)
		os.Exit(1)
	}

	// --- HTTP --------------------------------------------------------------
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(promReg, promhttp.HandlerOpts{}))
	// Embedded admin dashboard — browse messages, retry/kill, view history.
	mux.Handle("/tickr/", http.StripPrefix("/tickr", ui.Handler(store, ui.Options{Title: "orders"})))
	mux.HandleFunc("/orders", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var body orderCreated
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, "bad json: "+err.Error(), http.StatusBadRequest)
			return
		}
		if body.OrderID == "" {
			http.Error(w, "order_id required", http.StatusBadRequest)
			return
		}
		// Transactionally insert the order + enqueue the outbox row.
		tx, err := pool.Begin(r.Context())
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		defer func() { _ = tx.Rollback(context.Background()) }()

		if _, err := tx.Exec(r.Context(),
			`INSERT INTO orders (id, customer_id, total) VALUES ($1, $2, $3)
			 ON CONFLICT (id) DO NOTHING`,
			body.OrderID, body.CustomerID, body.Total); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		payload, _ := tickr.Encode(body)
		id, err := client.Enqueue(r.Context(), pgstore.WrapTx(tx), tickr.Message{
			Type:           "order.created",
			Payload:        payload,
			IdempotencyKey: body.OrderID,
		})
		if err != nil && !tickr.IsDuplicate(err) {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		if err := tx.Commit(r.Context()); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"order_id": body.OrderID, "message_id": id})
	})

	srv := &http.Server{
		Addr:              ":8080",
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	// Run worker + HTTP server in parallel; either exiting tears down the other.
	errCh := make(chan error, 2)
	go func() { errCh <- worker.Start(ctx) }()
	go func() {
		log.Info("serving", "addr", srv.Addr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
	}()

	<-ctx.Done()
	log.Info("shutting down")
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer shutdownCancel()
	_ = srv.Shutdown(shutdownCtx)
	_ = worker.Stop(shutdownCtx)
	<-errCh
	log.Info("shutdown complete")
}

// ---------------------------------------------------------------------------

func initTracing(ctx context.Context) (func(context.Context) error, error) {
	endpoint := os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT")
	if endpoint == "" {
		// No exporter configured; return a no-op shutdown.
		return func(context.Context) error { return nil }, nil
	}
	exp, err := otlptracehttp.New(ctx, otlptracehttp.WithInsecure())
	if err != nil {
		return nil, fmt.Errorf("otlp exporter: %w", err)
	}
	res, _ := resource.New(ctx,
		resource.WithAttributes(semconv.ServiceName(envOr("OTEL_SERVICE_NAME", "tickr-orders"))),
	)
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exp),
		sdktrace.WithResource(res),
	)
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{}, propagation.Baggage{},
	))
	return tp.Shutdown, nil
}

func ensureOrdersTable(ctx context.Context, pool *pgxpool.Pool) error {
	_, err := pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS orders (
			id          TEXT        PRIMARY KEY,
			customer_id TEXT        NOT NULL,
			total       NUMERIC(10,2) NOT NULL,
			created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
		)`)
	return err
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// slogAdapter bridges slog.Logger to tickr.Logger.
type slogAdapter struct{ l *slog.Logger }

func (a *slogAdapter) Debug(ctx context.Context, msg string, kv ...any) {
	a.l.DebugContext(ctx, msg, kv...)
}
func (a *slogAdapter) Info(ctx context.Context, msg string, kv ...any) {
	a.l.InfoContext(ctx, msg, kv...)
}
func (a *slogAdapter) Warn(ctx context.Context, msg string, kv ...any) {
	a.l.WarnContext(ctx, msg, kv...)
}
func (a *slogAdapter) Error(ctx context.Context, msg string, err error, kv ...any) {
	a.l.ErrorContext(ctx, msg, append([]any{"err", err}, kv...)...)
}
