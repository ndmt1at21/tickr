// Package json provides typed-handler wrappers that automatically encode
// and decode message payloads using encoding/json.
package json

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/ndmt1at21/tickr"
)

// Handler is the typed handler signature.
type Handler[T any] func(ctx context.Context, msg *tickr.InboundMessage, body T) error

// Wrap converts a typed Handler[T] into a tickr.Handler that decodes
// msg.Payload via encoding/json before invoking h.
func Wrap[T any](h Handler[T]) tickr.Handler {
	return func(ctx context.Context, msg *tickr.InboundMessage) error {
		var body T
		if len(msg.Payload) > 0 {
			if err := json.Unmarshal(msg.Payload, &body); err != nil {
				return fmt.Errorf("tickr/codec/json: decode payload: %w", err)
			}
		}
		return h(ctx, msg, body)
	}
}

// Encode marshals body to JSON and returns the bytes ready for
// tickr.Message.Payload. Convenience wrapper around encoding/json.Marshal.
func Encode(body any) ([]byte, error) {
	b, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("tickr/codec/json: encode payload: %w", err)
	}
	return b, nil
}
