package tickr

import (
	"context"
	"fmt"
	"time"
)

// ClientConfig configures the producer-side Client.
type ClientConfig struct {
	Storage Storage

	// DefaultMaxAttempts is the value used for messages whose
	// Message.MaxAttempts is zero. Defaults to 10.
	DefaultMaxAttempts int

	Logger  Logger
	Metrics Metrics
	Tracer  Tracer
}

// Client is the producer-side entry point. It writes outbox rows through
// the configured Storage adapter, optionally participating in the caller's
// transaction.
type Client struct {
	cfg ClientConfig
	log Logger
	mx  Metrics
	tr  Tracer
}

// NewClient builds a Client. Storage is required.
func NewClient(cfg ClientConfig) (*Client, error) {
	if cfg.Storage == nil {
		return nil, fmt.Errorf("tickr: ClientConfig.Storage is required")
	}
	if cfg.DefaultMaxAttempts <= 0 {
		cfg.DefaultMaxAttempts = 10
	}
	return &Client{
		cfg: cfg,
		log: defaultedLogger(cfg.Logger),
		mx:  defaultedMetrics(cfg.Metrics),
		tr:  defaultedTracer(cfg.Tracer),
	}, nil
}

func defaultedLogger(l Logger) Logger {
	if l == nil {
		return nopLogger{}
	}
	return l
}

func defaultedMetrics(m Metrics) Metrics {
	if m == nil {
		return nopMetrics{}
	}
	return m
}

func defaultedTracer(t Tracer) Tracer {
	if t == nil {
		return nopTracer{}
	}
	return t
}

// Enqueue writes a single message. tx may be the zero value (no caller
// transaction) — in that case the adapter performs a single auto-commit.
//
// Returns *ErrDuplicate (still with a usable existing-ID) when the
// (Type, IdempotencyKey) pair already exists.
func (c *Client) Enqueue(ctx context.Context, tx Tx, msg Message) (MessageID, error) {
	if msg.Type == "" {
		return "", fmt.Errorf("tickr: Message.Type required")
	}

	ctx, end := c.tr.StartEnqueueSpan(ctx, msg.Type)
	// Inject propagation headers into the message so consumers can rejoin
	// the trace. The Tracer impl is no-op-safe when nothing is configured.
	msg.Headers = c.tr.InjectHeaders(ctx, msg.Headers)

	maxAttempts := msg.MaxAttempts
	if maxAttempts <= 0 {
		maxAttempts = c.cfg.DefaultMaxAttempts
	}
	processAt := msg.ScheduledAt
	if processAt.IsZero() {
		processAt = time.Now()
	}

	out, err := c.cfg.Storage.Enqueue(ctx, tx, EnqueueParams{
		EventType:      msg.Type,
		Payload:        msg.Payload,
		Headers:        msg.Headers,
		IdempotencyKey: msg.IdempotencyKey,
		MaxAttempts:    maxAttempts,
		ProcessAt:      processAt,
	})
	if err != nil && !IsDuplicate(err) {
		end(err)
		return "", err
	}
	end(nil)

	if out == nil {
		return "", err
	}
	c.mx.MessageEnqueued(msg.Type)
	// Best-effort notification — never fails the enqueue. Notifications
	// are only delivered to subscribers after the caller transaction
	// commits, so the outbox guarantee is preserved.
	if !IsDuplicate(err) {
		if n, ok := c.cfg.Storage.(Notifier); ok {
			if nerr := n.Notify(ctx, tx, msg.Type); nerr != nil {
				c.log.Warn(ctx, "tickr: notify failed", "event_type", msg.Type, "err", nerr)
			}
		}
	}
	return out.ID, err // err may be *ErrDuplicate
}

// EnqueueBatch writes many messages in one round-trip. The returned slice
// is parallel to msgs; entries may be nil only when an error occurred for
// that index.
func (c *Client) EnqueueBatch(ctx context.Context, tx Tx, msgs []Message) ([]MessageID, error) {
	if len(msgs) == 0 {
		return nil, nil
	}
	params := make([]EnqueueParams, len(msgs))
	for i, msg := range msgs {
		if msg.Type == "" {
			return nil, fmt.Errorf("tickr: batch item %d: Message.Type required", i)
		}
		ctxIter, end := c.tr.StartEnqueueSpan(ctx, msg.Type)
		msg.Headers = c.tr.InjectHeaders(ctxIter, msg.Headers)
		end(nil)
		max := msg.MaxAttempts
		if max <= 0 {
			max = c.cfg.DefaultMaxAttempts
		}
		processAt := msg.ScheduledAt
		if processAt.IsZero() {
			processAt = time.Now()
		}
		params[i] = EnqueueParams{
			EventType:      msg.Type,
			Payload:        msg.Payload,
			Headers:        msg.Headers,
			IdempotencyKey: msg.IdempotencyKey,
			MaxAttempts:    max,
			ProcessAt:      processAt,
		}
	}
	out, err := c.cfg.Storage.EnqueueBatch(ctx, tx, params)
	ids := make([]MessageID, len(out))
	notifier, hasNotifier := c.cfg.Storage.(Notifier)
	seen := map[string]struct{}{}
	for i, m := range out {
		if m != nil {
			ids[i] = m.ID
			c.mx.MessageEnqueued(m.Type)
			if hasNotifier {
				if _, dup := seen[m.Type]; !dup {
					seen[m.Type] = struct{}{}
					if nerr := notifier.Notify(ctx, tx, m.Type); nerr != nil {
						c.log.Warn(ctx, "tickr: notify failed", "event_type", m.Type, "err", nerr)
					}
				}
			}
		}
	}
	return ids, err
}
