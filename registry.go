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
	// Exactly one of handler or batchHandler is non-nil.
	handler      Handler
	batchHandler BatchHandler
	cfg          handlerConfig
}

type handlerConfig struct {
	maxAttempts    int
	retry          RetryPolicy
	maxInflight    int // 0 = no per-event-type cap
	maxBatchSize   int // 0 = pass the whole claimed group; only applies to BatchHandler
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

// WithMaxBatchSize caps the number of messages passed to a [BatchHandler]
// in a single invocation. Zero (the default) means "use the whole group
// claimed for this event type in one poll cycle" — typically up to
// [WorkerConfig.BatchSize]. Has no effect on single-message handlers.
func WithMaxBatchSize(n int) HandlerOption {
	return func(c *handlerConfig) { c.maxBatchSize = n }
}

// NewRegistry creates an empty HandlerRegistry.
func NewRegistry() *HandlerRegistry {
	return &HandlerRegistry{m: map[string]registration{}}
}

// On registers a handler for one event type. Registering the same type
// twice is an error.
func (r *HandlerRegistry) On(eventType string, h Handler, opts ...HandlerOption) error {
	if h == nil {
		return fmt.Errorf("tickr: handler must be non-nil")
	}
	return r.register(eventType, registration{handler: h}, opts)
}

// OnBatch registers a [BatchHandler] for one event type. The worker groups
// same-type messages from each claim cycle and invokes the handler once
// per chunk (see [WithMaxBatchSize]). Registering the same event type
// twice — as single or batch — is an error.
func (r *HandlerRegistry) OnBatch(eventType string, h BatchHandler, opts ...HandlerOption) error {
	if h == nil {
		return fmt.Errorf("tickr: batch handler must be non-nil")
	}
	return r.register(eventType, registration{batchHandler: h}, opts)
}

func (r *HandlerRegistry) register(eventType string, reg registration, opts []HandlerOption) error {
	if eventType == "" {
		return fmt.Errorf("tickr: event type must be non-empty")
	}
	reg.cfg = handlerConfig{
		maxAttempts:    10,
		retry:          DefaultRetryPolicy(),
		attemptTimeout: 30 * time.Second,
	}
	for _, o := range opts {
		o(&reg.cfg)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.m[eventType]; exists {
		return fmt.Errorf("tickr: handler already registered for event type %q", eventType)
	}
	r.m[eventType] = reg
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
