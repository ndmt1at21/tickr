package grpchandler_test

import (
	"context"
	"errors"
	"testing"

	"github.com/ndmt1at21/tickr"
	grpchandler "github.com/ndmt1at21/tickr/handlers/grpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type notifyReq struct {
	UserID string
}
type notifyResp struct{}

// stubInvoke captures the call's ctx + req so tests can assert on
// outgoing metadata, and returns whatever err / resp the test pre-seeds.
type stubInvoke struct {
	gotCtx context.Context
	gotReq *notifyReq
	err    error
}

func (s *stubInvoke) call(ctx context.Context, req *notifyReq, _ ...grpc.CallOption) (*notifyResp, error) {
	s.gotCtx = ctx
	s.gotReq = req
	if s.err != nil {
		return nil, s.err
	}
	return &notifyResp{}, nil
}

func newMsg(idem string, headers map[string]string) *tickr.InboundMessage {
	return &tickr.InboundMessage{
		ID:             "00000000-0000-0000-0000-000000000001",
		Type:           "user.signup",
		IdempotencyKey: idem,
		Headers:        headers,
	}
}

func TestUnary_SuccessAndMetadataPropagation(t *testing.T) {
	stub := &stubInvoke{}
	h := grpchandler.Unary(stub.call, grpchandler.Config[*notifyReq]{})

	msg := newMsg("idem-42", map[string]string{
		"traceparent": "00-trace-span-01",
		"tracestate":  "vendor=foo",
	})
	if err := h(context.Background(), msg, &notifyReq{UserID: "u-7"}); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}

	md, ok := metadata.FromOutgoingContext(stub.gotCtx)
	if !ok {
		t.Fatal("no outgoing metadata attached")
	}
	if got := md.Get("traceparent"); len(got) != 1 || got[0] != "00-trace-span-01" {
		t.Errorf("traceparent metadata = %v", got)
	}
	if got := md.Get("tracestate"); len(got) != 1 || got[0] != "vendor=foo" {
		t.Errorf("tracestate metadata = %v", got)
	}
	if got := md.Get(grpchandler.DefaultIdempotencyMetadata); len(got) != 1 || got[0] != "idem-42" {
		t.Errorf("idempotency metadata = %v", got)
	}
	if stub.gotReq == nil || stub.gotReq.UserID != "u-7" {
		t.Errorf("req not forwarded: %+v", stub.gotReq)
	}
}

// Static + per-msg metadata get merged; trace context still wins.
func TestUnary_MetadataPrecedence(t *testing.T) {
	stub := &stubInvoke{}
	h := grpchandler.Unary(stub.call, grpchandler.Config[*notifyReq]{
		StaticMetadata: map[string]string{
			"authorization": "Bearer abc",
			"traceparent":   "should-be-overridden",
		},
		MetadataFunc: func(_ context.Context, _ *tickr.InboundMessage, r *notifyReq) map[string]string {
			return map[string]string{"x-user-id": r.UserID}
		},
	})
	msg := newMsg("", map[string]string{"traceparent": "00-real-01"})
	if err := h(context.Background(), msg, &notifyReq{UserID: "u-9"}); err != nil {
		t.Fatalf("err: %v", err)
	}
	md, _ := metadata.FromOutgoingContext(stub.gotCtx)
	if got := md.Get("authorization"); len(got) != 1 || got[0] != "Bearer abc" {
		t.Errorf("authorization = %v", got)
	}
	if got := md.Get("x-user-id"); len(got) != 1 || got[0] != "u-9" {
		t.Errorf("x-user-id = %v", got)
	}
	if got := md.Get("traceparent"); len(got) != 1 || got[0] != "00-real-01" {
		t.Errorf("traceparent = %v — must come from msg.Headers", got)
	}
}

// Permanent gRPC codes become DeadLetter.
func TestUnary_PermanentCodesDeadLetter(t *testing.T) {
	cases := []codes.Code{
		codes.InvalidArgument,
		codes.NotFound,
		codes.AlreadyExists,
		codes.PermissionDenied,
		codes.Unauthenticated,
		codes.FailedPrecondition,
		codes.OutOfRange,
		codes.Unimplemented,
	}
	for _, c := range cases {
		t.Run(c.String(), func(t *testing.T) {
			stub := &stubInvoke{err: status.Error(c, "nope")}
			h := grpchandler.Unary(stub.call, grpchandler.Config[*notifyReq]{})
			err := h(context.Background(), newMsg("", nil), &notifyReq{})
			if !tickr.IsDeadLetter(err) {
				t.Fatalf("code %s should DeadLetter, got: %v", c, err)
			}
		})
	}
}

// Retryable codes do NOT DeadLetter.
func TestUnary_RetryableCodes(t *testing.T) {
	cases := []codes.Code{
		codes.Unavailable,
		codes.DeadlineExceeded,
		codes.ResourceExhausted,
		codes.Aborted,
		codes.Internal,
		codes.Unknown,
		codes.DataLoss,
	}
	for _, c := range cases {
		t.Run(c.String(), func(t *testing.T) {
			stub := &stubInvoke{err: status.Error(c, "transient")}
			h := grpchandler.Unary(stub.call, grpchandler.Config[*notifyReq]{})
			err := h(context.Background(), newMsg("", nil), &notifyReq{})
			if err == nil {
				t.Fatal("expected error, got nil")
			}
			if tickr.IsDeadLetter(err) {
				t.Fatalf("code %s should retry, got DeadLetter: %v", c, err)
			}
		})
	}
}

// Non-status error (e.g. transport-level): retry.
func TestUnary_NonStatusErrorRetries(t *testing.T) {
	stub := &stubInvoke{err: errors.New("connection refused")}
	h := grpchandler.Unary(stub.call, grpchandler.Config[*notifyReq]{})
	err := h(context.Background(), newMsg("", nil), &notifyReq{})
	if err == nil {
		t.Fatal("expected err")
	}
	if tickr.IsDeadLetter(err) {
		t.Fatalf("transport error should retry, got DeadLetter: %v", err)
	}
}

// Custom classifier overrides defaults: treat INTERNAL as DeadLetter.
func TestUnary_CustomClassifier(t *testing.T) {
	stub := &stubInvoke{err: status.Error(codes.Internal, "boom")}
	h := grpchandler.Unary(stub.call, grpchandler.Config[*notifyReq]{
		Classifier: func(err error) error {
			st, ok := status.FromError(err)
			if ok && st.Code() == codes.Internal {
				return tickr.DeadLetter(err)
			}
			return err
		},
	})
	err := h(context.Background(), newMsg("", nil), &notifyReq{})
	if !tickr.IsDeadLetter(err) {
		t.Fatalf("custom classifier should DeadLetter INTERNAL, got: %v", err)
	}
}

// Custom IdempotencyMetadata key replaces the default; the default key
// must not also appear.
func TestUnary_CustomIdempotencyKey(t *testing.T) {
	stub := &stubInvoke{}
	h := grpchandler.Unary(stub.call, grpchandler.Config[*notifyReq]{
		IdempotencyMetadata: "x-custom-idem",
	})
	if err := h(context.Background(), newMsg("abc", nil), &notifyReq{}); err != nil {
		t.Fatalf("err: %v", err)
	}
	md, _ := metadata.FromOutgoingContext(stub.gotCtx)
	if got := md.Get("x-custom-idem"); len(got) != 1 || got[0] != "abc" {
		t.Errorf("custom idem header = %v", got)
	}
	if got := md.Get(grpchandler.DefaultIdempotencyMetadata); len(got) != 0 {
		t.Errorf("default header should be absent when custom is set: %v", got)
	}
}

// nil invoke fn → DeadLetter at handler-call time (programmer error).
func TestUnary_NilInvoke(t *testing.T) {
	h := grpchandler.Unary[*notifyReq, *notifyResp](nil, grpchandler.Config[*notifyReq]{})
	err := h(context.Background(), newMsg("", nil), &notifyReq{})
	if !tickr.IsDeadLetter(err) {
		t.Fatalf("nil invoke should DeadLetter, got: %v", err)
	}
}
