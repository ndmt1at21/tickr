package tickr

import (
	"context"
	"encoding/json"
	"fmt"
)

// TypedHandler is a handler whose payload has already been decoded to T.
// It's the ergonomic equivalent of [Handler] when payloads are JSON-encoded
// Go structs, which is the common case.
type TypedHandler[T any] func(ctx context.Context, msg *InboundMessage, body T) error

// On registers a typed handler in one line, eliminating the
// jsoncodec.Wrap boilerplate. The payload is decoded with encoding/json
// before the handler runs; a decode failure is returned as a permanent
// dead-letter error (the same message will never decode) so the message
// goes straight to DEAD without burning the retry budget.
//
// Example:
//
//	tickr.On(reg, "order.created",
//	    func(ctx context.Context, msg *tickr.InboundMessage, body OrderCreated) error {
//	        return chargeCustomer(ctx, body)
//	    },
//	    tickr.WithMaxAttempts(5),
//	)
//
// Use the lower-level [HandlerRegistry.On] when you need a custom codec or
// raw []byte access.
func On[T any](r *HandlerRegistry, eventType string, h TypedHandler[T], opts ...HandlerOption) error {
	if h == nil {
		return fmt.Errorf("tickr: handler must be non-nil")
	}
	return r.On(eventType, wrapJSON(h), opts...)
}

// MustOn is like [On] but panics on error. Convenient for handler
// registration during program init.
func MustOn[T any](r *HandlerRegistry, eventType string, h TypedHandler[T], opts ...HandlerOption) {
	if err := On(r, eventType, h, opts...); err != nil {
		panic(err)
	}
}

// Encode JSON-encodes body for use as [Message.Payload]. The companion to
// [On]: producers should encode with this so the symmetry with the
// consumer side is explicit.
func Encode(body any) ([]byte, error) {
	b, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("tickr: encode payload: %w", err)
	}
	return b, nil
}

func wrapJSON[T any](h TypedHandler[T]) Handler {
	return func(ctx context.Context, msg *InboundMessage) error {
		var body T
		if len(msg.Payload) > 0 {
			if err := json.Unmarshal(msg.Payload, &body); err != nil {
				// Permanent: a malformed payload will never decode on retry.
				return DeadLetter(fmt.Errorf("tickr: decode payload: %w", err))
			}
		}
		return h(ctx, msg, body)
	}
}
