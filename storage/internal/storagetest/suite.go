// Package storagetest provides a portable conformance suite that exercises
// every method of the tickr.Storage interface. Each adapter package
// instantiates its concrete Store against a container-managed database
// and calls RunSuite to verify behaviour.
package storagetest

import (
	"context"
	"testing"
	"time"

	"github.com/ndmt1at21/tickr"
)

// Capabilities describes adapter capabilities so tests that depend on
// optional features (e.g. Notifier) can be skipped where unsupported.
type Capabilities struct {
	SupportsNotifier bool
}

// RunSuite runs the full conformance suite against store. The caller is
// responsible for setting up the storage (e.g. ApplyMigrations) and
// truncating between top-level test runs if needed.
func RunSuite(t *testing.T, store tickr.Storage, caps Capabilities) {
	t.Helper()
	t.Run("enqueue_and_claim", func(t *testing.T) { testEnqueueAndClaim(t, store) })
	t.Run("idempotency", func(t *testing.T) { testIdempotency(t, store) })
	t.Run("succeed", func(t *testing.T) { testSucceed(t, store) })
	t.Run("fail_retry_then_dead", func(t *testing.T) { testFailRetryDead(t, store) })
	t.Run("requeue_from_dead", func(t *testing.T) { testRequeueFromDead(t, store) })
	t.Run("reclaim_expired", func(t *testing.T) { testReclaimExpired(t, store) })
	t.Run("purge_terminal", func(t *testing.T) { testPurgeTerminal(t, store) })
	t.Run("stats", func(t *testing.T) { testStats(t, store) })
	t.Run("leader_lock_exclusion", func(t *testing.T) { testLeaderLock(t, store) })
	if caps.SupportsNotifier {
		t.Run("notifier", func(t *testing.T) { testNotifier(t, store) })
	}
}

func ctx(t *testing.T) context.Context {
	t.Helper()
	c, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	return c
}

func mustEnqueue(t *testing.T, store tickr.Storage, p tickr.EnqueueParams) *tickr.InboundMessage {
	t.Helper()
	if p.ProcessAt.IsZero() {
		p.ProcessAt = time.Now()
	}
	if p.MaxAttempts == 0 {
		p.MaxAttempts = 3
	}
	m, err := store.Enqueue(ctx(t), tickr.Tx{}, p)
	if err != nil && !tickr.IsDuplicate(err) {
		t.Fatalf("enqueue: %v", err)
	}
	return m
}

func testEnqueueAndClaim(t *testing.T, store tickr.Storage) {
	m := mustEnqueue(t, store, tickr.EnqueueParams{
		EventType: "test.enqueue_claim",
		Payload:   []byte(`{"hello":"world"}`),
	})
	if m.ID == "" {
		t.Fatal("enqueue: empty ID")
	}
	msgs, err := store.Claim(ctx(t), tickr.ClaimParams{
		WorkerID: "w1", Batch: 10, Lease: 30 * time.Second,
		EventTypes: []string{"test.enqueue_claim"},
	})
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if len(msgs) == 0 {
		t.Fatal("claim returned no messages")
	}
	found := false
	for _, c := range msgs {
		if c.ID == m.ID {
			found = true
			if c.Attempt != 1 {
				t.Errorf("claim: want attempt=1 got %d", c.Attempt)
			}
		}
	}
	if !found {
		t.Errorf("claim: enqueued message %s not in batch", m.ID)
	}
}

func testIdempotency(t *testing.T, store tickr.Storage) {
	key := "idem-" + time.Now().Format("150405.000000000")
	first := mustEnqueue(t, store, tickr.EnqueueParams{
		EventType:      "test.idem",
		Payload:        []byte(`{}`),
		IdempotencyKey: key,
	})
	_, err := store.Enqueue(ctx(t), tickr.Tx{}, tickr.EnqueueParams{
		EventType:      "test.idem",
		Payload:        []byte(`{}`),
		IdempotencyKey: key,
	})
	if !tickr.IsDuplicate(err) {
		t.Fatalf("second enqueue: want IsDuplicate, got %v", err)
	}
	var dup *tickr.ErrDuplicate
	if as := tickr.IsDuplicate(err); !as {
		t.Fatalf("expected ErrDuplicate")
	}
	_ = dup
	_ = first
}

func testSucceed(t *testing.T, store tickr.Storage) {
	m := mustEnqueue(t, store, tickr.EnqueueParams{EventType: "test.success", Payload: []byte(`{}`)})
	claimed := claimUntil(t, store, "test.success", m.ID, "w-success")
	if err := store.Succeed(ctx(t), claimed.ID, claimed.Attempt, "w-success"); err != nil {
		t.Fatalf("succeed: %v", err)
	}
}

func testFailRetryDead(t *testing.T, store tickr.Storage) {
	m := mustEnqueue(t, store, tickr.EnqueueParams{
		EventType:   "test.fail",
		Payload:     []byte(`{}`),
		MaxAttempts: 2,
	})
	claimed := claimUntil(t, store, "test.fail", m.ID, "w-fail")
	if err := store.Fail(ctx(t), tickr.FailParams{
		MessageID: claimed.ID, Attempt: claimed.Attempt, WorkerID: "w-fail",
		Err: "boom", NextRetryAt: time.Now(),
	}); err != nil {
		t.Fatalf("fail-retry: %v", err)
	}
	// Second attempt: claim again, fail with Dead.
	claimed2 := claimUntil(t, store, "test.fail", m.ID, "w-fail")
	if err := store.Fail(ctx(t), tickr.FailParams{
		MessageID: claimed2.ID, Attempt: claimed2.Attempt, WorkerID: "w-fail",
		Err: "boom-final", Dead: true,
	}); err != nil {
		t.Fatalf("fail-dead: %v", err)
	}
	dead, err := store.ListDead(ctx(t), "test.fail", time.Now().Add(-time.Hour), 10)
	if err != nil {
		t.Fatalf("list dead: %v", err)
	}
	found := false
	for _, d := range dead {
		if d.ID == m.ID {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("dead message %s not listed", m.ID)
	}
}

func testRequeueFromDead(t *testing.T, store tickr.Storage) {
	m := mustEnqueue(t, store, tickr.EnqueueParams{
		EventType:   "test.requeue",
		Payload:     []byte(`{}`),
		MaxAttempts: 1,
	})
	claimed := claimUntil(t, store, "test.requeue", m.ID, "w-rq")
	if err := store.Fail(ctx(t), tickr.FailParams{
		MessageID: claimed.ID, Attempt: claimed.Attempt, WorkerID: "w-rq",
		Err: "boom", Dead: true,
	}); err != nil {
		t.Fatalf("fail-dead: %v", err)
	}
	if err := store.Requeue(ctx(t), m.ID, time.Now()); err != nil {
		t.Fatalf("requeue: %v", err)
	}
	claimUntil(t, store, "test.requeue", m.ID, "w-rq-2")
}

func testReclaimExpired(t *testing.T, store tickr.Storage) {
	m := mustEnqueue(t, store, tickr.EnqueueParams{EventType: "test.reclaim", Payload: []byte(`{}`)})
	// Claim with a -1s lease so it's already expired.
	_, err := store.Claim(ctx(t), tickr.ClaimParams{
		WorkerID: "w-died", Batch: 10, Lease: -1 * time.Second,
		EventTypes: []string{"test.reclaim"},
	})
	if err != nil {
		t.Fatalf("claim w/ expired lease: %v", err)
	}
	n, err := store.ReclaimExpired(ctx(t), 100)
	if err != nil {
		t.Fatalf("reclaim: %v", err)
	}
	if n < 1 {
		t.Errorf("expected at least 1 reclaim, got %d", n)
	}
	_ = m
}

func testPurgeTerminal(t *testing.T, store tickr.Storage) {
	m := mustEnqueue(t, store, tickr.EnqueueParams{EventType: "test.purge", Payload: []byte(`{}`)})
	claimed := claimUntil(t, store, "test.purge", m.ID, "w-p")
	if err := store.Succeed(ctx(t), claimed.ID, claimed.Attempt, "w-p"); err != nil {
		t.Fatalf("succeed: %v", err)
	}
	n, err := store.PurgeTerminal(ctx(t), time.Now().Add(time.Hour), 100)
	if err != nil {
		t.Fatalf("purge: %v", err)
	}
	if n < 1 {
		t.Errorf("expected at least 1 row purged, got %d", n)
	}
}

func testStats(t *testing.T, store tickr.Storage) {
	mustEnqueue(t, store, tickr.EnqueueParams{EventType: "test.stats", Payload: []byte(`{}`)})
	s, err := store.Stats(ctx(t))
	if err != nil {
		t.Fatalf("stats: %v", err)
	}
	if len(s.ByEventType) == 0 {
		t.Fatal("stats: no event types reported")
	}
}

func testLeaderLock(t *testing.T, store tickr.Storage) {
	got1, unlock1, err := store.TryLeaderLock(ctx(t), "test-leader")
	if err != nil {
		t.Fatalf("first acquire: %v", err)
	}
	if !got1 {
		t.Fatal("first acquire returned false")
	}
	got2, _, err := store.TryLeaderLock(ctx(t), "test-leader")
	if err != nil {
		t.Fatalf("second acquire: %v", err)
	}
	if got2 {
		t.Fatal("second acquire should have failed while first holds")
	}
	unlock1()
	got3, unlock3, err := store.TryLeaderLock(ctx(t), "test-leader")
	if err != nil {
		t.Fatalf("third acquire: %v", err)
	}
	if !got3 {
		t.Fatal("third acquire should succeed after first unlocked")
	}
	unlock3()
}

func testNotifier(t *testing.T, store tickr.Storage) {
	n, ok := store.(tickr.Notifier)
	if !ok {
		t.Skip("storage does not implement Notifier")
	}
	listenCtx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	ch, cleanup, err := n.Listen(listenCtx)
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	t.Cleanup(func() { _ = cleanup() })

	// Drain the channel.
	select {
	case <-ch:
	default:
	}

	mustEnqueue(t, store, tickr.EnqueueParams{EventType: "test.notify", Payload: []byte(`{}`)})
	// Some adapters send notify inside the storage tx (auto-commit) — wait
	// up to 2s.
	select {
	case <-ch:
		// got it.
	case <-time.After(2 * time.Second):
		t.Fatal("did not receive notification within 2s of enqueue")
	}
}

func claimUntil(t *testing.T, store tickr.Storage, eventType string, want tickr.MessageID, workerID string) *tickr.InboundMessage {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		msgs, err := store.Claim(ctx(t), tickr.ClaimParams{
			WorkerID: workerID, Batch: 50, Lease: 30 * time.Second,
			EventTypes: []string{eventType},
		})
		if err != nil {
			t.Fatalf("claim: %v", err)
		}
		for _, m := range msgs {
			if m.ID == want {
				return m
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("claim: timed out waiting for %s", want)
	return nil
}
