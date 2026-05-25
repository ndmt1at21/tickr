// Package grpchandler provides a built-in tickr handler that invokes a
// unary gRPC method and classifies the response status into tickr's
// retry / dead-letter outcomes.
//
//	conn, _ := grpc.NewClient(target, grpc.WithTransportCredentials(insecure.NewCredentials()))
//	client := userpb.NewUserServiceClient(conn)
//
//	_ = tickr.On(reg, "user.signup",
//	    grpchandler.Unary[*userpb.NotifyRequest, *userpb.NotifyResponse](
//	        client.Notify, // generated method
//	        grpchandler.Config[*userpb.NotifyRequest]{},
//	    ),
//	    tickr.WithMaxAttempts(5),
//	    tickr.WithAttemptTimeout(10*time.Second),
//	)
//
// Default classification follows the [gRPC retry semantics] common in
// the ecosystem:
//
//   - OK → success
//   - UNAVAILABLE, DEADLINE_EXCEEDED, RESOURCE_EXHAUSTED, ABORTED → retry
//   - INVALID_ARGUMENT, NOT_FOUND, PERMISSION_DENIED, UNAUTHENTICATED,
//     ALREADY_EXISTS, FAILED_PRECONDITION, OUT_OF_RANGE, UNIMPLEMENTED → DeadLetter
//   - INTERNAL, UNKNOWN, DATA_LOSS, CANCELLED → retry (with caveat: app
//     code may mean otherwise — supply Config.Classifier to override)
//
// The message's W3C trace context ([tickr.InboundMessage.Headers]) and
// IdempotencyKey are attached as outgoing gRPC metadata so distributed
// traces span producer → outbox → downstream service and the callee can
// dedup safely.
//
// [gRPC retry semantics]: https://grpc.io/docs/guides/retry/
package grpchandler

import (
	"context"
	"errors"
	"fmt"

	"github.com/ndmt1at21/tickr"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// DefaultIdempotencyMetadata is the canonical outgoing metadata key for
// forwarding [tickr.InboundMessage.IdempotencyKey]. gRPC metadata keys
// are lowercased on the wire (HTTP/2 convention).
const DefaultIdempotencyMetadata = "x-idempotency-key"

// UnaryInvoke matches the signature of a generated unary client method
// (the kind protoc-gen-go-grpc emits on a service client). Pass the
// bound method directly:
//
//	grpchandler.Unary(userClient.Notify, ...)
type UnaryInvoke[Req, Resp any] func(ctx context.Context, req Req, opts ...grpc.CallOption) (Resp, error)

// Config configures a unary gRPC handler. Generic over the request type
// so MetadataFunc / Classifier can access typed fields.
type Config[Req any] struct {
	// StaticMetadata is appended to every outgoing call. User-supplied
	// entries for known trace-context keys are overridden by the
	// message's own headers so spans stay coherent.
	StaticMetadata map[string]string

	// MetadataFunc, if set, returns per-message metadata merged on top
	// of StaticMetadata. Same trace-context override applies.
	MetadataFunc func(ctx context.Context, msg *tickr.InboundMessage, req Req) map[string]string

	// IdempotencyMetadata is the metadata key used to forward the
	// message's IdempotencyKey. Defaults to "x-idempotency-key" — set
	// to "" to disable the auto-forward entirely.
	IdempotencyMetadata string

	// CallOpts are passed through to every invoke call (auth creds,
	// codec selection, etc.).
	CallOpts []grpc.CallOption

	// Classifier maps a gRPC status error to a tickr outcome. Defaults
	// to [DefaultClassifier]. Custom classifiers can inspect status
	// details to make per-service decisions.
	Classifier func(err error) error
}

// Unary returns a [tickr.TypedHandler] that calls invoke for each
// message. Use the generated client method directly:
//
//	_ = tickr.On(reg, "user.signup",
//	    grpchandler.Unary(client.Notify, grpchandler.Config[*pb.NotifyReq]{}),
//	    tickr.WithMaxAttempts(5),
//	)
//
// Per-attempt timeout should be configured via [tickr.WithAttemptTimeout]
// on the registration — it flows into ctx and the gRPC call inherits
// it.
func Unary[Req, Resp any](invoke UnaryInvoke[Req, Resp], cfg Config[Req]) tickr.TypedHandler[Req] {
	if invoke == nil {
		// Constructing without an invoke fn is a programmer error;
		// surface it loudly at handler-call time as a permanent fault.
		return func(_ context.Context, _ *tickr.InboundMessage, _ Req) error {
			return tickr.DeadLetter(errors.New("grpchandler: nil invoke"))
		}
	}
	if cfg.IdempotencyMetadata == "" {
		cfg.IdempotencyMetadata = DefaultIdempotencyMetadata
	}
	if cfg.Classifier == nil {
		cfg.Classifier = DefaultClassifier
	}

	return func(ctx context.Context, msg *tickr.InboundMessage, req Req) error {
		ctx = applyMetadata(ctx, msg, req, cfg)
		_, err := invoke(ctx, req, cfg.CallOpts...)
		return cfg.Classifier(err)
	}
}

func applyMetadata[Req any](ctx context.Context, msg *tickr.InboundMessage, req Req, cfg Config[Req]) context.Context {
	md := metadata.MD{}
	for k, v := range cfg.StaticMetadata {
		md.Set(k, v)
	}
	if cfg.MetadataFunc != nil {
		for k, v := range cfg.MetadataFunc(ctx, msg, req) {
			md.Set(k, v)
		}
	}
	// Trace context wins so distributed traces stay coherent.
	for k, v := range msg.Headers {
		md.Set(k, v)
	}
	if cfg.IdempotencyMetadata != "" && msg.IdempotencyKey != "" {
		md.Set(cfg.IdempotencyMetadata, msg.IdempotencyKey)
	}
	if len(md) == 0 {
		return ctx
	}
	return metadata.NewOutgoingContext(ctx, md)
}

// DefaultClassifier implements the standard gRPC retry policy
// documented at the top of the package. Exported so users can wrap or
// extend it.
func DefaultClassifier(err error) error {
	if err == nil {
		return nil
	}
	st, ok := status.FromError(err)
	if !ok {
		// Non-status error — likely a transport / context issue. Retry.
		return fmt.Errorf("grpchandler: %w", err)
	}
	switch st.Code() {
	case codes.OK:
		return nil
	case codes.InvalidArgument,
		codes.NotFound,
		codes.AlreadyExists,
		codes.PermissionDenied,
		codes.Unauthenticated,
		codes.FailedPrecondition,
		codes.OutOfRange,
		codes.Unimplemented:
		return tickr.DeadLetter(err)
	default:
		// UNAVAILABLE, DEADLINE_EXCEEDED, RESOURCE_EXHAUSTED, ABORTED,
		// INTERNAL, UNKNOWN, DATA_LOSS, CANCELLED — retry by default.
		// Users with stricter policies (e.g. INTERNAL as permanent)
		// supply Config.Classifier.
		return err
	}
}
