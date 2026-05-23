package tickr

import (
	"testing"
	"time"
)

func TestExponentialBackoff_Growth(t *testing.T) {
	b := ExponentialBackoff{Base: time.Second, Max: time.Hour, JitterFraction: 0, Rand: func() float64 { return 0.5 }}
	cases := []struct {
		attempt int
		want    time.Duration
	}{
		{1, time.Second},
		{2, 2 * time.Second},
		{3, 4 * time.Second},
		{4, 8 * time.Second},
		{5, 16 * time.Second},
	}
	for _, c := range cases {
		got := b.NextDelay(c.attempt, nil)
		if got != c.want {
			t.Errorf("attempt %d: got %s, want %s", c.attempt, got, c.want)
		}
	}
}

func TestExponentialBackoff_Cap(t *testing.T) {
	b := ExponentialBackoff{Base: time.Second, Max: 10 * time.Second, JitterFraction: 0, Rand: func() float64 { return 0.5 }}
	got := b.NextDelay(20, nil) // 2^19 seconds far exceeds cap
	if got != 10*time.Second {
		t.Errorf("expected cap at 10s, got %s", got)
	}
}

func TestExponentialBackoff_Jitter(t *testing.T) {
	b := ExponentialBackoff{Base: 10 * time.Second, Max: time.Hour, JitterFraction: 0.5, Rand: func() float64 { return 1.0 }}
	// With rand=1.0 and jitter=0.5, spread = (2*1-1)*0.5 = +0.5, so 10s * 1.5 = 15s
	got := b.NextDelay(1, nil)
	if got != 15*time.Second {
		t.Errorf("expected 15s with max jitter, got %s", got)
	}
	b.Rand = func() float64 { return 0.0 }
	// With rand=0, spread = -0.5, so 10s * 0.5 = 5s
	got = b.NextDelay(1, nil)
	if got != 5*time.Second {
		t.Errorf("expected 5s with min jitter, got %s", got)
	}
}

func TestExponentialBackoff_Defaults(t *testing.T) {
	b := ExponentialBackoff{}
	got := b.NextDelay(1, nil)
	if got <= 0 || got > time.Hour {
		t.Errorf("default delay out of expected range: %s", got)
	}
}
