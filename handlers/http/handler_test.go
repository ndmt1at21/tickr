package httphandler_test

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ndmt1at21/tickr"
	httphandler "github.com/ndmt1at21/tickr/handlers/http"
)

type order struct {
	ID    string `json:"id"`
	Total int    `json:"total"`
}

func newMsg(id, key string, headers map[string]string) *tickr.InboundMessage {
	return &tickr.InboundMessage{
		ID:             tickr.MessageID(id),
		Type:           "order.created",
		IdempotencyKey: key,
		Headers:        headers,
	}
}

func TestPostJSON_Success(t *testing.T) {
	var (
		gotMethod, gotPath, gotIdem, gotTrace, gotCT string
		gotBody                                      []byte
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotPath = r.URL.Path
		gotIdem = r.Header.Get("Idempotency-Key")
		gotTrace = r.Header.Get("traceparent")
		gotCT = r.Header.Get("Content-Type")
		gotBody, _ = io.ReadAll(r.Body)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	h := httphandler.PostJSON[order](nil, httphandler.Config[order]{
		URL: srv.URL + "/orders",
	})
	msg := newMsg("00000000-0000-0000-0000-000000000001", "idem-7",
		map[string]string{"traceparent": "00-trace-span-01"})

	if err := h(context.Background(), msg, order{ID: "o-1", Total: 42}); err != nil {
		t.Fatalf("handler returned error: %v", err)
	}
	if gotMethod != http.MethodPost {
		t.Errorf("method = %q, want POST", gotMethod)
	}
	if gotPath != "/orders" {
		t.Errorf("path = %q", gotPath)
	}
	if gotCT != "application/json" {
		t.Errorf("Content-Type = %q", gotCT)
	}
	if gotIdem != "idem-7" {
		t.Errorf("Idempotency-Key = %q, want idem-7", gotIdem)
	}
	if gotTrace != "00-trace-span-01" {
		t.Errorf("traceparent = %q, want propagated", gotTrace)
	}
	if want := `{"id":"o-1","total":42}`; string(gotBody) != want {
		t.Errorf("body = %q, want %s", gotBody, want)
	}
}

func TestPostJSON_URLFunc(t *testing.T) {
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	h := httphandler.PostJSON[order](nil, httphandler.Config[order]{
		URLFunc: func(_ context.Context, _ *tickr.InboundMessage, _ order) string {
			return srv.URL + "/dyn"
		},
	})
	if err := h(context.Background(), newMsg("id", "", nil), order{}); err != nil {
		t.Fatalf("err: %v", err)
	}
	if hits.Load() != 1 {
		t.Fatalf("server hit count = %d, want 1", hits.Load())
	}
}

// 4xx (other than 408/425/429) is a permanent client error → DeadLetter.
func TestPostJSON_4xxDeadLetters(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "bad payload", http.StatusBadRequest)
	}))
	defer srv.Close()

	h := httphandler.PostJSON[order](nil, httphandler.Config[order]{URL: srv.URL})
	err := h(context.Background(), newMsg("id", "", nil), order{})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !tickr.IsDeadLetter(err) {
		t.Fatalf("expected DeadLetter, got %T: %v", err, err)
	}
}

// 5xx → retry (no DeadLetter wrap).
func TestPostJSON_5xxRetries(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "upstream down", http.StatusBadGateway)
	}))
	defer srv.Close()

	h := httphandler.PostJSON[order](nil, httphandler.Config[order]{URL: srv.URL})
	err := h(context.Background(), newMsg("id", "", nil), order{})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if tickr.IsDeadLetter(err) {
		t.Fatalf("5xx should retry, got DeadLetter: %v", err)
	}
	if _, ok := tickr.ExtractRetryAfter(err); ok {
		t.Fatalf("no Retry-After header → should not be RetryAfter")
	}
}

// 429 + Retry-After honored as a RetryAfter override.
func TestPostJSON_429RetryAfter(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Retry-After", "5")
		http.Error(w, "slow down", http.StatusTooManyRequests)
	}))
	defer srv.Close()

	h := httphandler.PostJSON[order](nil, httphandler.Config[order]{URL: srv.URL})
	err := h(context.Background(), newMsg("id", "", nil), order{})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	d, ok := tickr.ExtractRetryAfter(err)
	if !ok {
		t.Fatalf("expected RetryAfter, got %v", err)
	}
	if d != 5*time.Second {
		t.Errorf("retry-after = %v, want 5s", d)
	}
}

// Transport errors (server unreachable) → retry (no DeadLetter).
func TestPostJSON_TransportErrorRetries(t *testing.T) {
	// Bind a server then immediately close it so the URL is unreachable.
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	srv.Close()

	h := httphandler.PostJSON[order](nil, httphandler.Config[order]{URL: srv.URL})
	err := h(context.Background(), newMsg("id", "", nil), order{})
	if err == nil {
		t.Fatal("expected transport error, got nil")
	}
	if tickr.IsDeadLetter(err) {
		t.Fatalf("transport error should retry, got DeadLetter: %v", err)
	}
}

// Static headers + HeaderFunc merged; trace context still wins.
func TestPostJSON_HeaderPrecedence(t *testing.T) {
	var gotAuth, gotCustom, gotTrace string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		gotCustom = r.Header.Get("X-Per-Msg")
		gotTrace = r.Header.Get("traceparent")
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	h := httphandler.PostJSON[order](nil, httphandler.Config[order]{
		URL:           srv.URL,
		StaticHeaders: map[string]string{"Authorization": "Bearer abc", "traceparent": "should-be-overridden"},
		HeaderFunc: func(_ context.Context, _ *tickr.InboundMessage, o order) map[string]string {
			return map[string]string{"X-Per-Msg": o.ID}
		},
	})
	err := h(context.Background(), newMsg("id", "",
		map[string]string{"traceparent": "00-real-trace-01"}), order{ID: "o-1"})
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if gotAuth != "Bearer abc" {
		t.Errorf("Authorization = %q", gotAuth)
	}
	if gotCustom != "o-1" {
		t.Errorf("X-Per-Msg = %q", gotCustom)
	}
	if gotTrace != "00-real-trace-01" {
		t.Errorf("traceparent = %q — must come from msg.Headers, not StaticHeaders", gotTrace)
	}
}

// Custom classifier overrides defaults entirely.
func TestPostJSON_CustomClassifier(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "boom", http.StatusInternalServerError)
	}))
	defer srv.Close()

	sentinel := errors.New("treat-as-dead")
	h := httphandler.PostJSON[order](nil, httphandler.Config[order]{
		URL: srv.URL,
		Classifier: func(resp *http.Response, body []byte, _ error) error {
			if resp != nil && resp.StatusCode >= 500 {
				return tickr.DeadLetter(sentinel)
			}
			return nil
		},
	})
	err := h(context.Background(), newMsg("id", "", nil), order{})
	if !tickr.IsDeadLetter(err) {
		t.Fatalf("expected DeadLetter from custom classifier, got %v", err)
	}
	if !strings.Contains(err.Error(), "treat-as-dead") {
		t.Errorf("error doesn't wrap sentinel: %v", err)
	}
}

// No URL + no URLFunc → DeadLetter at handler time (config bug, won't fix on retry).
func TestPostJSON_MissingURL(t *testing.T) {
	h := httphandler.PostJSON[order](nil, httphandler.Config[order]{})
	err := h(context.Background(), newMsg("id", "", nil), order{})
	if !tickr.IsDeadLetter(err) {
		t.Fatalf("expected DeadLetter for missing URL, got %v", err)
	}
}
