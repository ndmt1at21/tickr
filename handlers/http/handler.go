// Package httphandler provides a built-in tickr handler that posts the
// message payload to an HTTP endpoint and classifies the response into
// tickr's retry / dead-letter outcomes.
//
//	_ = tickr.On(reg, "order.created",
//	    httphandler.PostJSON[OrderCreated]("https://api.example.com/orders", http.DefaultClient),
//	    tickr.WithMaxAttempts(8),
//	    tickr.WithAttemptTimeout(15*time.Second),
//	)
//
// The defaults match the conventions documented at
// [RFC 9110 §15] and the [Retry-After header]:
//
//   - 2xx → success
//   - 408 Request Timeout, 425 Too Early, 429 Too Many Requests → retry (Retry-After honored)
//   - other 4xx → DeadLetter (client error won't fix on retry)
//   - 5xx → retry (Retry-After honored)
//   - transport / context errors → retry
//
// All headers in [tickr.InboundMessage.Headers] are forwarded as HTTP
// headers — this includes the W3C traceparent / tracestate that the
// OpenTelemetry tracer injects at enqueue time, so distributed traces
// span producer → outbox → downstream service. The message's
// IdempotencyKey, if non-empty, is forwarded as "Idempotency-Key"
// (override via [WithIdempotencyHeader]).
//
// [RFC 9110 §15]: https://www.rfc-editor.org/rfc/rfc9110#section-15
// [Retry-After header]: https://www.rfc-editor.org/rfc/rfc9110#name-retry-after
package httphandler

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/ndmt1at21/tickr"
)

// DefaultIdempotencyHeader is the canonical HTTP header for forwarding
// [tickr.InboundMessage.IdempotencyKey]. It matches the IETF draft
// "The Idempotency-Key HTTP Header Field".
const DefaultIdempotencyHeader = "Idempotency-Key"

// Config configures a JSON HTTP handler. Generic over the decoded
// payload type T so URLFunc / HeaderFunc / Classifier can access typed
// fields.
type Config[T any] struct {
	// Method is the HTTP verb. Defaults to POST.
	Method string

	// URL is the static destination URL. Ignored when URLFunc is set.
	URL string

	// URLFunc, if set, derives the destination URL from the message —
	// useful when the target is encoded in the payload (e.g. a webhook
	// URL).
	URLFunc func(ctx context.Context, msg *tickr.InboundMessage, body T) string

	// StaticHeaders are added to every request. User-supplied entries
	// for "traceparent" / "tracestate" are silently overridden by the
	// message's own trace context so distributed tracing stays
	// coherent.
	StaticHeaders map[string]string

	// HeaderFunc, if set, returns per-message headers merged on top of
	// StaticHeaders. Same trace-context override applies.
	HeaderFunc func(ctx context.Context, msg *tickr.InboundMessage, body T) map[string]string

	// IdempotencyHeader is the header name used to forward the
	// message's IdempotencyKey. Defaults to "Idempotency-Key" — set to
	// "" to disable the auto-forward entirely.
	IdempotencyHeader string

	// Classifier maps an HTTP response (or transport error) to a tickr
	// outcome. Defaults to [DefaultClassifier]. Custom classifiers can
	// inspect the body (already read for them) to make finer
	// decisions.
	Classifier func(resp *http.Response, body []byte, transportErr error) error
}

// PostJSON returns a [tickr.TypedHandler] that JSON-encodes the typed
// body and POSTs it to the configured URL. Register with [tickr.On]:
//
//	_ = tickr.On(reg, "email.send",
//	    httphandler.PostJSON[Email](http.DefaultClient, httphandler.Config[Email]{
//	        URLFunc: func(_ context.Context, _ *tickr.InboundMessage, e Email) string { return e.HookURL },
//	    }),
//	    tickr.WithMaxAttempts(8),
//	)
//
// If client is nil, [http.DefaultClient] is used. Per-attempt timeout
// should be configured via [tickr.WithAttemptTimeout] on the
// registration — that timeout flows through ctx into the http.Request
// and the underlying transport.
func PostJSON[T any](client *http.Client, cfg Config[T]) tickr.TypedHandler[T] {
	if client == nil {
		client = http.DefaultClient
	}
	if cfg.Method == "" {
		cfg.Method = http.MethodPost
	}
	if cfg.IdempotencyHeader == "" {
		cfg.IdempotencyHeader = DefaultIdempotencyHeader
	}
	if cfg.Classifier == nil {
		cfg.Classifier = DefaultClassifier
	}

	return func(ctx context.Context, msg *tickr.InboundMessage, body T) error {
		url := cfg.URL
		if cfg.URLFunc != nil {
			url = cfg.URLFunc(ctx, msg, body)
		}
		if url == "" {
			return tickr.DeadLetter(errors.New("httphandler: no URL configured (set Config.URL or Config.URLFunc)"))
		}

		// Re-encode rather than forwarding msg.Payload verbatim: the
		// typed wrapper has already paid the unmarshal cost, and
		// downstream services rely on canonical UTF-8 JSON regardless
		// of how the producer encoded it.
		payload, err := tickr.Encode(body)
		if err != nil {
			return tickr.DeadLetter(fmt.Errorf("httphandler: encode body: %w", err))
		}

		req, err := http.NewRequestWithContext(ctx, cfg.Method, url, bytes.NewReader(payload))
		if err != nil {
			// Bad URL / method — won't recover on retry.
			return tickr.DeadLetter(fmt.Errorf("httphandler: build request: %w", err))
		}
		applyHeaders(req, msg, body, cfg)

		resp, transportErr := client.Do(req)
		if resp != nil {
			defer func() { _, _ = io.Copy(io.Discard, resp.Body); _ = resp.Body.Close() }()
		}

		// Read body once so the classifier can use it.
		var respBody []byte
		if resp != nil {
			respBody, _ = io.ReadAll(io.LimitReader(resp.Body, 1<<16))
		}

		return cfg.Classifier(resp, respBody, transportErr)
	}
}

func applyHeaders[T any](req *http.Request, msg *tickr.InboundMessage, body T, cfg Config[T]) {
	req.Header.Set("Content-Type", "application/json")

	for k, v := range cfg.StaticHeaders {
		req.Header.Set(k, v)
	}
	if cfg.HeaderFunc != nil {
		for k, v := range cfg.HeaderFunc(req.Context(), msg, body) {
			req.Header.Set(k, v)
		}
	}
	// Trace context overrides any user header so spans stay coherent.
	for k, v := range msg.Headers {
		req.Header.Set(k, v)
	}
	// Idempotency last so it always wins over StaticHeaders.
	if cfg.IdempotencyHeader != "" && msg.IdempotencyKey != "" {
		req.Header.Set(cfg.IdempotencyHeader, msg.IdempotencyKey)
	}
}

// DefaultClassifier implements the standard webhook retry policy
// documented at the top of the package. Exported so users can wrap or
// extend it.
func DefaultClassifier(resp *http.Response, body []byte, transportErr error) error {
	if transportErr != nil {
		// Transport / context errors are virtually always retryable —
		// the request never reached the server, so it's safe to retry.
		return fmt.Errorf("httphandler: transport: %w", transportErr)
	}
	switch {
	case resp.StatusCode >= 200 && resp.StatusCode < 300:
		return nil
	case resp.StatusCode == http.StatusRequestTimeout, // 408
		resp.StatusCode == http.StatusTooEarly,       // 425
		resp.StatusCode == http.StatusTooManyRequests: // 429
		if d, ok := parseRetryAfter(resp.Header.Get("Retry-After")); ok {
			return tickr.RetryAfter(d, fmt.Errorf("httphandler: HTTP %d: %s", resp.StatusCode, snip(body)))
		}
		return fmt.Errorf("httphandler: HTTP %d: %s", resp.StatusCode, snip(body))
	case resp.StatusCode >= 400 && resp.StatusCode < 500:
		// Client error other than the retryable trio — payload or
		// request is wrong and won't fix on retry.
		return tickr.DeadLetter(fmt.Errorf("httphandler: HTTP %d: %s", resp.StatusCode, snip(body)))
	case resp.StatusCode >= 500:
		if d, ok := parseRetryAfter(resp.Header.Get("Retry-After")); ok {
			return tickr.RetryAfter(d, fmt.Errorf("httphandler: HTTP %d: %s", resp.StatusCode, snip(body)))
		}
		return fmt.Errorf("httphandler: HTTP %d: %s", resp.StatusCode, snip(body))
	default:
		// 1xx / 3xx — shouldn't happen for a posted JSON body in
		// normal use. Treat as retry-worthy "unknown" until proven
		// otherwise.
		return fmt.Errorf("httphandler: unexpected HTTP %d", resp.StatusCode)
	}
}

// parseRetryAfter parses both forms allowed by RFC 9110:
//   - delta-seconds (an integer)
//   - HTTP-date
func parseRetryAfter(v string) (time.Duration, bool) {
	v = strings.TrimSpace(v)
	if v == "" {
		return 0, false
	}
	if secs, err := strconv.Atoi(v); err == nil && secs >= 0 {
		return time.Duration(secs) * time.Second, true
	}
	if t, err := http.ParseTime(v); err == nil {
		if d := time.Until(t); d > 0 {
			return d, true
		}
		return 0, true // header in the past → retry immediately
	}
	return 0, false
}

// snip caps the response body shown in error strings so logs stay sane.
func snip(b []byte) string {
	const max = 200
	if len(b) <= max {
		return string(b)
	}
	return string(b[:max]) + "…"
}
