package tickr

import (
	"errors"
	"testing"
	"time"
)

func TestRetryAfter(t *testing.T) {
	cause := errors.New("downstream blip")
	err := RetryAfter(5*time.Second, cause)

	d, ok := ExtractRetryAfter(err)
	if !ok {
		t.Fatal("expected ExtractRetryAfter to find a delay")
	}
	if d != 5*time.Second {
		t.Errorf("expected 5s, got %s", d)
	}
	if !errors.Is(err, cause) {
		t.Error("expected RetryAfter to wrap the cause")
	}
}

func TestDeadLetter(t *testing.T) {
	cause := errors.New("permanently broken")
	err := DeadLetter(cause)
	if !IsDeadLetter(err) {
		t.Error("expected IsDeadLetter to be true")
	}
	if !errors.Is(err, cause) {
		t.Error("expected DeadLetter to wrap the cause")
	}
}

func TestSkip(t *testing.T) {
	err := Skip("already done")
	if !IsSkip(err) {
		t.Error("expected IsSkip to be true")
	}
}

func TestIsDuplicate(t *testing.T) {
	err := &ErrDuplicate{ExistingID: "x", EventType: "e", Key: "k"}
	if !IsDuplicate(err) {
		t.Error("expected IsDuplicate to be true")
	}
	if IsDuplicate(errors.New("nope")) {
		t.Error("expected non-duplicate to be false")
	}
}
