package tickr

import (
	"math"
	"math/rand/v2"
	"time"
)

// RetryPolicy computes the delay before the next attempt after a failure.
// Implementations must be safe for concurrent use.
type RetryPolicy interface {
	// NextDelay returns the delay before the (attempt+1)-th attempt. attempt
	// is the 1-indexed number of attempts already made. err is the failure
	// from the most recent attempt and may be nil.
	NextDelay(attempt int, err error) time.Duration
}

// ExponentialBackoff is the default RetryPolicy: delay = min(Max, Base * 2^(attempt-1))
// with multiplicative full-jitter in the range [1-JitterFraction, 1+JitterFraction].
type ExponentialBackoff struct {
	// Base is the delay before the second attempt. Default 1s when zero.
	Base time.Duration
	// Max caps the delay. Default 1h when zero.
	Max time.Duration
	// JitterFraction is the random spread applied to the computed delay,
	// clamped to [0, 1]. Default 0.2.
	JitterFraction float64
	// Rand is an optional random source for deterministic tests. Default
	// uses math/rand/v2's package-level functions.
	Rand func() float64
}

// NextDelay implements RetryPolicy.
func (e ExponentialBackoff) NextDelay(attempt int, _ error) time.Duration {
	base := e.Base
	if base <= 0 {
		base = time.Second
	}
	maxDelay := e.Max
	if maxDelay <= 0 {
		maxDelay = time.Hour
	}
	jitter := e.JitterFraction
	if jitter < 0 {
		jitter = 0
	}
	if jitter > 1 {
		jitter = 1
	}
	if attempt < 1 {
		attempt = 1
	}

	// 2^(attempt-1) with overflow guard.
	exp := math.Pow(2, float64(attempt-1))
	d := time.Duration(float64(base) * exp)
	if d <= 0 || d > maxDelay {
		d = maxDelay
	}

	if jitter > 0 {
		var r float64
		if e.Rand != nil {
			r = e.Rand()
		} else {
			r = rand.Float64()
		}
		spread := (2*r - 1) * jitter // in [-jitter, +jitter]
		d = time.Duration(float64(d) * (1 + spread))
		d = max(d, 0)
		d = min(d, maxDelay)
	}
	return d
}

// DefaultRetryPolicy returns the default ExponentialBackoff used when a
// handler is registered without WithRetryPolicy.
func DefaultRetryPolicy() RetryPolicy {
	return ExponentialBackoff{Base: time.Second, Max: time.Hour, JitterFraction: 0.2}
}
