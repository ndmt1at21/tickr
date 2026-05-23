package tickr

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
)

type fooBody struct {
	Name  string `json:"name"`
	Count int    `json:"count"`
}

func TestOnTyped_DecodesPayload(t *testing.T) {
	reg := NewRegistry()
	got := fooBody{}
	if err := On(reg, "foo.created",
		func(_ context.Context, _ *InboundMessage, body fooBody) error {
			got = body
			return nil
		}); err != nil {
		t.Fatalf("On: %v", err)
	}
	regEntry, ok := reg.lookup("foo.created")
	if !ok {
		t.Fatal("handler not registered")
	}
	payload, _ := json.Marshal(fooBody{Name: "alice", Count: 7})
	if err := regEntry.handler(context.Background(),
		&InboundMessage{Type: "foo.created", Payload: payload}); err != nil {
		t.Fatalf("handler: %v", err)
	}
	if got.Name != "alice" || got.Count != 7 {
		t.Fatalf("decoded = %+v, want {alice 7}", got)
	}
}

func TestOnTyped_BadJSONIsDeadLetter(t *testing.T) {
	reg := NewRegistry()
	if err := On(reg, "foo.bad",
		func(_ context.Context, _ *InboundMessage, _ fooBody) error { return nil }); err != nil {
		t.Fatalf("On: %v", err)
	}
	regEntry, _ := reg.lookup("foo.bad")
	err := regEntry.handler(context.Background(),
		&InboundMessage{Type: "foo.bad", Payload: []byte("not json")})
	if err == nil {
		t.Fatal("want decode error, got nil")
	}
	if !IsDeadLetter(err) {
		t.Fatalf("want dead-letter error, got %v", err)
	}
}

func TestOnTyped_EmptyPayload(t *testing.T) {
	reg := NewRegistry()
	called := false
	if err := On(reg, "foo.empty",
		func(_ context.Context, _ *InboundMessage, body fooBody) error {
			called = true
			if body != (fooBody{}) {
				t.Fatalf("want zero body, got %+v", body)
			}
			return nil
		}); err != nil {
		t.Fatalf("On: %v", err)
	}
	regEntry, _ := reg.lookup("foo.empty")
	if err := regEntry.handler(context.Background(), &InboundMessage{Type: "foo.empty"}); err != nil {
		t.Fatalf("handler: %v", err)
	}
	if !called {
		t.Fatal("handler not invoked")
	}
}

func TestOnTyped_HandlerErrorPropagates(t *testing.T) {
	reg := NewRegistry()
	sentinel := errors.New("boom")
	if err := On(reg, "foo.err",
		func(_ context.Context, _ *InboundMessage, _ fooBody) error { return sentinel }); err != nil {
		t.Fatalf("On: %v", err)
	}
	regEntry, _ := reg.lookup("foo.err")
	err := regEntry.handler(context.Background(),
		&InboundMessage{Type: "foo.err", Payload: []byte(`{"name":"x"}`)})
	if !errors.Is(err, sentinel) {
		t.Fatalf("err = %v, want sentinel", err)
	}
}

func TestEncode_RoundTrip(t *testing.T) {
	in := fooBody{Name: "bob", Count: 42}
	b, err := Encode(in)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	var out fooBody
	if err := json.Unmarshal(b, &out); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if out != in {
		t.Fatalf("round-trip mismatch: %+v vs %+v", out, in)
	}
}

func TestMustOn_NilPanics(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic")
		}
	}()
	reg := NewRegistry()
	MustOn[fooBody](reg, "x", nil)
}
