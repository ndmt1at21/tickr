package tickr

import (
	"context"
	"testing"
	"time"
)

func TestRegistry_RegisterAndLookup(t *testing.T) {
	r := NewRegistry()
	called := false
	h := func(ctx context.Context, msg *InboundMessage) error {
		called = true
		return nil
	}
	if err := r.On("evt", h, WithMaxAttempts(3), WithAttemptTimeout(time.Second)); err != nil {
		t.Fatalf("register: %v", err)
	}

	reg, ok := r.lookup("evt")
	if !ok {
		t.Fatal("lookup miss")
	}
	if reg.cfg.maxAttempts != 3 || reg.cfg.attemptTimeout != time.Second {
		t.Errorf("options not applied: %+v", reg.cfg)
	}
	if err := reg.handler(context.Background(), &InboundMessage{}); err != nil {
		t.Fatalf("handler returned error: %v", err)
	}
	if !called {
		t.Error("handler not invoked")
	}
}

func TestRegistry_DuplicateRegister(t *testing.T) {
	r := NewRegistry()
	h := func(ctx context.Context, msg *InboundMessage) error { return nil }
	if err := r.On("evt", h); err != nil {
		t.Fatalf("first register: %v", err)
	}
	if err := r.On("evt", h); err == nil {
		t.Error("expected duplicate register to error")
	}
}

func TestRegistry_EmptyValidation(t *testing.T) {
	r := NewRegistry()
	h := func(ctx context.Context, msg *InboundMessage) error { return nil }
	if err := r.On("", h); err == nil {
		t.Error("expected empty event type to error")
	}
	if err := r.On("evt", nil); err == nil {
		t.Error("expected nil handler to error")
	}
}

func TestRegistry_EventTypes(t *testing.T) {
	r := NewRegistry()
	h := func(ctx context.Context, msg *InboundMessage) error { return nil }
	_ = r.On("a", h)
	_ = r.On("b", h)
	types := r.EventTypes()
	if len(types) != 2 {
		t.Fatalf("expected 2 event types, got %d: %v", len(types), types)
	}
}
