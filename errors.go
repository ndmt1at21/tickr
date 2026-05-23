package tickr

import (
	"errors"
	"fmt"
	"time"
)

// ErrDuplicate is returned by Client.Enqueue when a message with the same
// (Type, IdempotencyKey) already exists. The caller can treat duplicates as
// success by checking errors.As.
type ErrDuplicate struct {
	ExistingID MessageID
	EventType  string
	Key        string
}

func (e *ErrDuplicate) Error() string {
	return fmt.Sprintf("tickr: duplicate idempotency key %q for event type %q (existing id %s)",
		e.Key, e.EventType, e.ExistingID)
}

// IsDuplicate reports whether err (or anything it wraps) is *ErrDuplicate.
func IsDuplicate(err error) bool {
	var d *ErrDuplicate
	return errors.As(err, &d)
}

// retryAfterErr forces a specific next-attempt delay regardless of the
// handler's RetryPolicy.
type retryAfterErr struct {
	delay time.Duration
	err   error
}

func (e *retryAfterErr) Error() string {
	if e.err == nil {
		return fmt.Sprintf("tickr: retry after %s", e.delay)
	}
	return fmt.Sprintf("tickr: retry after %s: %v", e.delay, e.err)
}

func (e *retryAfterErr) Unwrap() error { return e.err }

// RetryAfter wraps err and instructs the engine to delay the next attempt
// by exactly delay (bypassing the configured RetryPolicy). If attempts are
// exhausted, the message still moves to DEAD.
func RetryAfter(delay time.Duration, err error) error {
	if err == nil {
		err = errors.New("retry requested")
	}
	return &retryAfterErr{delay: delay, err: err}
}

// ExtractRetryAfter returns the explicit retry delay if err was produced
// by RetryAfter, else (0, false).
func ExtractRetryAfter(err error) (time.Duration, bool) {
	var r *retryAfterErr
	if errors.As(err, &r) {
		return r.delay, true
	}
	return 0, false
}

// deadLetterErr asks the engine to terminate retries and move the message
// straight to DEAD.
type deadLetterErr struct{ err error }

func (e *deadLetterErr) Error() string {
	if e.err == nil {
		return "tickr: dead-letter requested"
	}
	return "tickr: dead-letter: " + e.err.Error()
}

func (e *deadLetterErr) Unwrap() error { return e.err }

// DeadLetter wraps err and instructs the engine to move the message to DEAD
// without consuming further retry budget. Use this for permanent failures
// the handler can detect (malformed payload, business rule violations).
func DeadLetter(err error) error {
	if err == nil {
		err = errors.New("dead-lettered")
	}
	return &deadLetterErr{err: err}
}

// IsDeadLetter reports whether err was produced by DeadLetter.
func IsDeadLetter(err error) bool {
	var d *deadLetterErr
	return errors.As(err, &d)
}

// skipErr marks the attempt as SUCCESS without doing work. Useful when the
// handler detects the work is already done (idempotent no-op).
type skipErr struct{ reason string }

func (e *skipErr) Error() string {
	if e.reason == "" {
		return "tickr: skipped"
	}
	return "tickr: skipped: " + e.reason
}

// Skip marks the current attempt as SUCCESS without running side effects.
// reason is recorded in history for auditability.
func Skip(reason string) error { return &skipErr{reason: reason} }

// IsSkip reports whether err was produced by Skip.
func IsSkip(err error) bool {
	var s *skipErr
	return errors.As(err, &s)
}
