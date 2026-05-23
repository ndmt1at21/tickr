package tickr

import (
	"fmt"
	"sync"
	"time"
)

// HandlerRegistry maps event types to Handlers and their options. It is
// safe for concurrent use; new handlers may be registered before the
// Worker starts, but mutation after Start is not supported.
type HandlerRegistry struct {
	mu sync.RWMutex
	m  map[string]registration
}

type registration struct {
	handler Handler
	cfg     handlerConfig
}

type handlerConfig struct {
	maxAttempts    int
	retry          RetryPolicy
	maxInflight    int                // 0 = no per-event-type cap
	attemptTimeout time.Duration
	deadLetterIf   func(error) bool
}

// HandlerOption tunes the per-event-type handler behaviour.
type HandlerOption func(*handlerConfig)

// WithMaxAttempts overrides the global default for one event type.
func WithMaxAttempts(n int) HandlerOption {
	return func(c *handlerConfig) { c.maxAttempts = n }
}

// WithRetryPolicy sets a custom RetryPolicy for one event type.
func WithRetryPolicy(p RetryPolicy) HandlerOption {
	return func(c *handlerConfig) { c.retry = p }
}

// WithMaxInflight caps the number of concurrently-running attempts of one
// event type within a single worker process. Fleet-wide limit ≈ this value
// times the number of worker instances.
func WithMaxInflight(n int) HandlerOption {
	return func(c *handlerConfig) { c.maxInflight = n }
}

// WithAttemptTimeout sets the per-attempt deadline for a handler. Long
// timeouts are safe — the engine auto-extends the storage lease for the
// duration of each attempt.
func WithAttemptTimeout(d time.Duration) HandlerOption {
	return func(c *handlerConfig) { c.attemptTimeout = d }
}

// WithDeadLetterIf sends the message straight to DEAD whenever the
// predicate returns true for the handler's returned error. Useful for
// permanent failures (malformed payloads, business-rule violations) that
// shouldn't burn the retry budget.
func WithDeadLetterIf(pred func(error) bool) HandlerOption {
	return func(c *handlerConfig) { c.deadLetterIf = pred }
}

// NewRegistry creates an empty HandlerRegistry.
func NewRegistry() *HandlerRegistry {
	return &HandlerRegistry{m: map[string]registration{}}
}

// On registers a handler for one event type. Registering the same type
// twice is an error.
func (r *HandlerRegistry) On(eventType string, h Handler, opts ...HandlerOption) error {
	if eventType == "" {
		return fmt.Errorf("tickr: event type must be non-empty")
	}
	if h == nil {
		return fmt.Errorf("tickr: handler must be non-nil")
	}
	cfg := handlerConfig{
		maxAttempts:    10,
		retry:          DefaultRetryPolicy(),
		attemptTimeout: 30 * time.Second,
	}
	for _, o := range opts {
		o(&cfg)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.m[eventType]; exists {
		return fmt.Errorf("tickr: handler already registered for event type %q", eventType)
	}
	r.m[eventType] = registration{handler: h, cfg: cfg}
	return nil
}

// EventTypes returns the registered event-type names in unspecified order.
// Used by the worker to scope the claim query.
func (r *HandlerRegistry) EventTypes() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]string, 0, len(r.m))
	for k := range r.m {
		out = append(out, k)
	}
	return out
}

// lookup returns the registration for an event type and whether it exists.
// It is used by the engine via the lookupAdapter glue.
func (r *HandlerRegistry) lookup(eventType string) (registration, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	reg, ok := r.m[eventType]
	return reg, ok
}
