// Package proto provides typed-handler wrappers that encode and decode
// message payloads using google.golang.org/protobuf.
//
// Usage:
//
//	import (
//	    tickr "github.com/ndmt1at21/tickr"
//	    protocodec "github.com/ndmt1at21/tickr/codec/proto"
//	    pb "myapp/gen/events"
//	)
//
//	reg.On("order.created",
//	    protocodec.Wrap(func(ctx context.Context, msg *tickr.InboundMessage, body *pb.OrderCreated) error {
//	        return chargeCustomer(ctx, body)
//	    }))
//
//	payload, _ := protocodec.Encode(&pb.OrderCreated{...})
//	_, _ = client.Enqueue(ctx, tx, tickr.Message{Type: "order.created", Payload: payload})
package proto

import (
	"context"
	"fmt"
	"reflect"

	"google.golang.org/protobuf/proto"

	"github.com/ndmt1at21/tickr"
)

// Handler is the typed handler signature. T must be a pointer to a generated
// protobuf message type (e.g. *pb.OrderCreated).
type Handler[T proto.Message] func(ctx context.Context, msg *tickr.InboundMessage, body T) error

// Wrap converts a typed Handler[T] into a tickr.Handler. It allocates a
// fresh T per invocation via reflection so handlers are safe to run
// concurrently without sharing buffers.
func Wrap[T proto.Message](h Handler[T]) tickr.Handler {
	// Determine the concrete element type once at registration time.
	var zero T
	elemType := reflect.TypeOf(zero).Elem()
	return func(ctx context.Context, msg *tickr.InboundMessage) error {
		body := reflect.New(elemType).Interface().(T)
		if len(msg.Payload) > 0 {
			if err := proto.Unmarshal(msg.Payload, body); err != nil {
				return fmt.Errorf("tickr/codec/proto: decode payload: %w", err)
			}
		}
		return h(ctx, msg, body)
	}
}

// Encode marshals a protobuf message and returns the bytes ready for
// tickr.Message.Payload.
func Encode(body proto.Message) ([]byte, error) {
	b, err := proto.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("tickr/codec/proto: encode payload: %w", err)
	}
	return b, nil
}
