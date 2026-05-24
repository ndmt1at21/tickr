package tickr

import (
	"context"
	"errors"
	"sort"
	"sync"
	"testing"
	"time"
)

// fakeEngineStore is a minimal Storage covering only what the engine's
// dispatch + commit paths exercise. Methods not called by these tests
// return zero values.
type fakeEngineStore struct {
	mu       sync.Mutex
	toClaim  []*InboundMessage
	claimed  bool
	succeed  []MessageID
	failed   []FailParams
}

func (f *fakeEngineStore) Claim(_ context.Context, _ ClaimParams) ([]*InboundMessage, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.claimed {
		return nil, nil
	}
	f.claimed = true
	return f.toClaim, nil
}

func (f *fakeEngineStore) Succeed(_ context.Context, id MessageID, _ int, _ string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.succeed = append(f.succeed, id)
	return nil
}

func (f *fakeEngineStore) Fail(_ context.Context, p FailParams) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.failed = append(f.failed, p)
	return nil
}

func (f *fakeEngineStore) Extend(_ context.Context, _ MessageID, _ string, _ time.Time) (bool, error) {
	return true, nil
}

func (f *fakeEngineStore) Enqueue(context.Context, Tx, EnqueueParams) (*InboundMessage, error) {
	return nil, nil
}
func (f *fakeEngineStore) EnqueueBatch(context.Context, Tx, []EnqueueParams) ([]*InboundMessage, error) {
	return nil, nil
}
func (f *fakeEngineStore) ReleaseShutdown(context.Context, MessageID, int, string, string) error {
	return nil
}
func (f *fakeEngineStore) History(context.Context, MessageID) ([]Transition, error) { return nil, nil }
func (f *fakeEngineStore) ListDead(context.Context, string, time.Time, int) ([]*InboundMessage, error) {
	return nil, nil
}
func (f *fakeEngineStore) Requeue(context.Context, MessageID, time.Time) error    { return nil }
func (f *fakeEngineStore) ReclaimExpired(context.Context, int) (int64, error)     { return 0, nil }
func (f *fakeEngineStore) PurgeTerminal(context.Context, time.Time, int) (int64, error) {
	return 0, nil
}
func (f *fakeEngineStore) Stats(context.Context) (Stats, error)         { return Stats{}, nil }
func (f *fakeEngineStore) ApplyMigrations(context.Context) error        { return nil }
func (f *fakeEngineStore) TryLeaderLock(context.Context, string) (bool, func(), error) {
	return false, func() {}, nil
}

func newTestEngine(store Storage, reg *HandlerRegistry) *engine {
	return newEngine(engineConfig{
		storage:  store,
		registry: reg,
		workerID: "test",
		logger:   nopLogger{},
		metrics:  nopMetrics{},
		tracer:   nopTracer{},
	})
}

func msg(id, eventType string) *InboundMessage {
	return &InboundMessage{ID: MessageID(id), Type: eventType, MaxAttempts: 5, Attempt: 1}
}

func TestEngine_BatchHandler_AllSucceed(t *testing.T) {
	store := &fakeEngineStore{toClaim: []*InboundMessage{
		msg("a", "batch.evt"),
		msg("b", "batch.evt"),
		msg("c", "batch.evt"),
	}}
	reg := NewRegistry()
	var (
		mu        sync.Mutex
		invocations [][]MessageID
	)
	if err := reg.OnBatch("batch.evt", func(_ context.Context, msgs []*InboundMessage) error {
		mu.Lock()
		defer mu.Unlock()
		ids := make([]MessageID, len(msgs))
		for i, m := range msgs {
			ids[i] = m.ID
		}
		invocations = append(invocations, ids)
		return nil
	}); err != nil {
		t.Fatalf("OnBatch: %v", err)
	}

	e := newTestEngine(store, reg)
	e.pollOnce(context.Background())
	e.inflight.Wait()

	if len(invocations) != 1 {
		t.Fatalf("got %d invocations, want 1", len(invocations))
	}
	if len(invocations[0]) != 3 {
		t.Fatalf("batch size = %d, want 3", len(invocations[0]))
	}
	if len(store.succeed) != 3 {
		t.Fatalf("succeed count = %d, want 3 (failed: %+v)", len(store.succeed), store.failed)
	}
}

func TestEngine_BatchHandler_ChunkedByMaxBatchSize(t *testing.T) {
	var claim []*InboundMessage
	for i := range 10 {
		claim = append(claim, msg(string(rune('a'+i)), "batch.evt"))
	}
	store := &fakeEngineStore{toClaim: claim}
	reg := NewRegistry()
	var (
		mu    sync.Mutex
		sizes []int
	)
	if err := reg.OnBatch("batch.evt",
		func(_ context.Context, msgs []*InboundMessage) error {
			mu.Lock()
			defer mu.Unlock()
			sizes = append(sizes, len(msgs))
			return nil
		},
		WithMaxBatchSize(3),
	); err != nil {
		t.Fatalf("OnBatch: %v", err)
	}

	e := newTestEngine(store, reg)
	e.pollOnce(context.Background())
	e.inflight.Wait()

	sort.Ints(sizes)
	want := []int{1, 3, 3, 3}
	if len(sizes) != len(want) {
		t.Fatalf("got chunks %v, want %v", sizes, want)
	}
	for i, s := range sizes {
		if s != want[i] {
			t.Fatalf("got chunks %v, want %v", sizes, want)
		}
	}
	if len(store.succeed) != 10 {
		t.Fatalf("succeed = %d, want 10", len(store.succeed))
	}
}

func TestEngine_BatchHandler_ErrorFailsEveryMessage(t *testing.T) {
	store := &fakeEngineStore{toClaim: []*InboundMessage{
		msg("a", "batch.evt"),
		msg("b", "batch.evt"),
	}}
	reg := NewRegistry()
	boom := errors.New("boom")
	if err := reg.OnBatch("batch.evt", func(_ context.Context, _ []*InboundMessage) error {
		return boom
	}); err != nil {
		t.Fatalf("OnBatch: %v", err)
	}

	e := newTestEngine(store, reg)
	e.pollOnce(context.Background())
	e.inflight.Wait()

	if len(store.succeed) != 0 {
		t.Fatalf("succeed should be empty, got %v", store.succeed)
	}
	if len(store.failed) != 2 {
		t.Fatalf("fail count = %d, want 2", len(store.failed))
	}
	for _, f := range store.failed {
		if f.Err != "boom" {
			t.Errorf("fail err = %q, want \"boom\"", f.Err)
		}
		if f.Dead {
			t.Errorf("attempt 1 of 5 should not dead-letter")
		}
	}
}

func TestEngine_BatchHandler_DeadLetterFailsAllAsDead(t *testing.T) {
	store := &fakeEngineStore{toClaim: []*InboundMessage{
		msg("a", "batch.evt"),
		msg("b", "batch.evt"),
	}}
	reg := NewRegistry()
	if err := reg.OnBatch("batch.evt", func(_ context.Context, _ []*InboundMessage) error {
		return DeadLetter(errors.New("permanent"))
	}); err != nil {
		t.Fatalf("OnBatch: %v", err)
	}

	e := newTestEngine(store, reg)
	e.pollOnce(context.Background())
	e.inflight.Wait()

	if len(store.failed) != 2 {
		t.Fatalf("fail count = %d, want 2", len(store.failed))
	}
	for _, f := range store.failed {
		if !f.Dead {
			t.Errorf("expected Dead=true for DeadLetter outcome, got %+v", f)
		}
	}
}

func TestEngine_PartitionsSingleAndBatchPerType(t *testing.T) {
	store := &fakeEngineStore{toClaim: []*InboundMessage{
		msg("s1", "single.evt"),
		msg("b1", "batch.evt"),
		msg("b2", "batch.evt"),
		msg("s2", "single.evt"),
	}}
	reg := NewRegistry()
	var (
		mu          sync.Mutex
		singleCalls int
		batchSizes  []int
	)
	if err := reg.On("single.evt", func(_ context.Context, _ *InboundMessage) error {
		mu.Lock()
		defer mu.Unlock()
		singleCalls++
		return nil
	}); err != nil {
		t.Fatalf("On: %v", err)
	}
	if err := reg.OnBatch("batch.evt", func(_ context.Context, msgs []*InboundMessage) error {
		mu.Lock()
		defer mu.Unlock()
		batchSizes = append(batchSizes, len(msgs))
		return nil
	}); err != nil {
		t.Fatalf("OnBatch: %v", err)
	}

	e := newTestEngine(store, reg)
	e.pollOnce(context.Background())
	e.inflight.Wait()

	if singleCalls != 2 {
		t.Errorf("single handler calls = %d, want 2", singleCalls)
	}
	if len(batchSizes) != 1 || batchSizes[0] != 2 {
		t.Errorf("batch invocations = %v, want [2]", batchSizes)
	}
	if len(store.succeed) != 4 {
		t.Errorf("succeed count = %d, want 4", len(store.succeed))
	}
}
