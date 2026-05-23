package ui

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ndmt1at21/tickr"
)

// fakeStore is a minimal in-memory implementation of tickr.Storage +
// tickr.Admin sufficient for exercising the UI handler.
type fakeStore struct {
	mu      sync.Mutex
	msgs    map[tickr.MessageID]*tickr.InboundMessage
	hist    map[tickr.MessageID][]tickr.Transition
	order   []tickr.MessageID // newest last
	killed  []tickr.MessageID
	requeue []tickr.MessageID
}

func newFake() *fakeStore {
	return &fakeStore{
		msgs: map[tickr.MessageID]*tickr.InboundMessage{},
		hist: map[tickr.MessageID][]tickr.Transition{},
	}
}

func (f *fakeStore) add(m *tickr.InboundMessage) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.msgs[m.ID] = m
	f.order = append(f.order, m.ID)
}

// --- tickr.Storage ---------------------------------------------------------

func (f *fakeStore) Enqueue(context.Context, tickr.Tx, tickr.EnqueueParams) (*tickr.InboundMessage, error) {
	return nil, nil
}
func (f *fakeStore) EnqueueBatch(context.Context, tickr.Tx, []tickr.EnqueueParams) ([]*tickr.InboundMessage, error) {
	return nil, nil
}
func (f *fakeStore) Claim(context.Context, tickr.ClaimParams) ([]*tickr.InboundMessage, error) {
	return nil, nil
}
func (f *fakeStore) Succeed(context.Context, tickr.MessageID, int, string) error { return nil }
func (f *fakeStore) Fail(context.Context, tickr.FailParams) error                { return nil }
func (f *fakeStore) ReleaseShutdown(context.Context, tickr.MessageID, int, string, string) error {
	return nil
}
func (f *fakeStore) Extend(context.Context, tickr.MessageID, string, time.Time) (bool, error) {
	return true, nil
}
func (f *fakeStore) History(_ context.Context, id tickr.MessageID) ([]tickr.Transition, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]tickr.Transition(nil), f.hist[id]...), nil
}
func (f *fakeStore) ListDead(context.Context, string, time.Time, int) ([]*tickr.InboundMessage, error) {
	return nil, nil
}
func (f *fakeStore) Requeue(_ context.Context, id tickr.MessageID, _ time.Time) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.requeue = append(f.requeue, id)
	if m, ok := f.msgs[id]; ok {
		m.Status = tickr.StatusCreated
	}
	return nil
}
func (f *fakeStore) ReclaimExpired(context.Context, int) (int64, error)         { return 0, nil }
func (f *fakeStore) PurgeTerminal(context.Context, time.Time, int) (int64, error) { return 0, nil }
func (f *fakeStore) Stats(context.Context) (tickr.Stats, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := tickr.Stats{
		ByStatus:    map[tickr.Status]int{},
		ByEventType: map[string]map[tickr.Status]int{},
		SampledAt:   time.Now(),
	}
	for _, m := range f.msgs {
		out.ByStatus[m.Status]++
		if out.ByEventType[m.Type] == nil {
			out.ByEventType[m.Type] = map[tickr.Status]int{}
		}
		out.ByEventType[m.Type][m.Status]++
	}
	return out, nil
}
func (f *fakeStore) ApplyMigrations(context.Context) error { return nil }
func (f *fakeStore) TryLeaderLock(context.Context, string) (bool, func(), error) {
	return false, func() {}, nil
}

// --- tickr.Admin -----------------------------------------------------------

func (f *fakeStore) Get(_ context.Context, id tickr.MessageID) (*tickr.InboundMessage, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.msgs[id], nil
}

func (f *fakeStore) List(_ context.Context, q tickr.ListQuery) ([]*tickr.InboundMessage, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	limit := q.Limit
	if limit <= 0 {
		limit = 50
	}
	out := []*tickr.InboundMessage{}
	for i := len(f.order) - 1; i >= 0 && len(out) < limit; i-- {
		m := f.msgs[f.order[i]]
		if q.Status != "" && m.Status != q.Status {
			continue
		}
		if q.EventType != "" && m.Type != q.EventType {
			continue
		}
		if q.Search != "" && !strings.Contains(strings.ToLower(m.IdempotencyKey+" "+string(m.Payload)), strings.ToLower(q.Search)) {
			continue
		}
		if q.AfterID != "" && m.ID >= q.AfterID {
			continue
		}
		out = append(out, m)
	}
	return out, nil
}

func (f *fakeStore) Kill(_ context.Context, id tickr.MessageID, _ string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.killed = append(f.killed, id)
	if m, ok := f.msgs[id]; ok {
		m.Status = tickr.StatusDead
	}
	return nil
}

// --- tests -----------------------------------------------------------------

func TestUI_IndexServesHTML(t *testing.T) {
	srv := httptest.NewServer(Handler(newFake(), Options{Title: "demo"}))
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	ct := resp.Header.Get("Content-Type")
	if !strings.HasPrefix(ct, "text/html") {
		t.Fatalf("content-type = %q", ct)
	}
}

func TestUI_StatsRespectsAdmin(t *testing.T) {
	fake := newFake()
	fake.add(&tickr.InboundMessage{ID: "a", Type: "t", Status: tickr.StatusCreated})
	fake.add(&tickr.InboundMessage{ID: "b", Type: "t", Status: tickr.StatusDead})

	srv := httptest.NewServer(Handler(fake))
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/api/stats")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var got statsResponse
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatal(err)
	}
	if got.ByStatus["CREATED"] != 1 || got.ByStatus["DEAD"] != 1 {
		t.Fatalf("counts = %+v", got.ByStatus)
	}
}

func TestUI_ListFiltersByStatus(t *testing.T) {
	fake := newFake()
	fake.add(&tickr.InboundMessage{ID: "1", Type: "t", Status: tickr.StatusCreated})
	fake.add(&tickr.InboundMessage{ID: "2", Type: "t", Status: tickr.StatusDead})
	fake.add(&tickr.InboundMessage{ID: "3", Type: "t", Status: tickr.StatusDead})

	srv := httptest.NewServer(Handler(fake))
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/api/messages?status=DEAD")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var got listResponse
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatal(err)
	}
	if len(got.Messages) != 2 {
		t.Fatalf("len = %d, want 2", len(got.Messages))
	}
	for _, m := range got.Messages {
		if m.Status != "DEAD" {
			t.Fatalf("status = %q", m.Status)
		}
	}
}

func TestUI_RequeueAndKill(t *testing.T) {
	fake := newFake()
	fake.add(&tickr.InboundMessage{ID: "a", Type: "t", Status: tickr.StatusDead})
	fake.add(&tickr.InboundMessage{ID: "b", Type: "t", Status: tickr.StatusFailed})

	srv := httptest.NewServer(Handler(fake))
	defer srv.Close()

	// requeue
	resp, err := http.Post(srv.URL+"/api/messages/a/requeue", "", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("requeue status = %d", resp.StatusCode)
	}
	if len(fake.requeue) != 1 || fake.requeue[0] != "a" {
		t.Fatalf("requeue calls = %v", fake.requeue)
	}

	// kill
	resp, err = http.Post(srv.URL+"/api/messages/b/kill", "", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("kill status = %d", resp.StatusCode)
	}
	if len(fake.killed) != 1 || fake.killed[0] != "b" {
		t.Fatalf("kill calls = %v", fake.killed)
	}
}

func TestUI_ReadOnlyRejectsMutations(t *testing.T) {
	fake := newFake()
	fake.add(&tickr.InboundMessage{ID: "a", Type: "t", Status: tickr.StatusDead})

	srv := httptest.NewServer(Handler(fake, Options{ReadOnly: true}))
	defer srv.Close()

	resp, err := http.Post(srv.URL+"/api/messages/a/requeue", "", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("status = %d, want 403", resp.StatusCode)
	}
	if len(fake.requeue) != 0 {
		t.Fatalf("requeue should not have run")
	}
}

func TestUI_GetMessageReturnsHistory(t *testing.T) {
	fake := newFake()
	fake.add(&tickr.InboundMessage{ID: "x", Type: "t", Status: tickr.StatusDead, Payload: []byte(`{"k":1}`)})
	fake.hist["x"] = []tickr.Transition{
		{MessageID: "x", Seq: 1, From: "", To: tickr.StatusCreated, At: time.Now()},
		{MessageID: "x", Seq: 2, From: tickr.StatusCreated, To: tickr.StatusDead, Attempt: 1, Error: "boom", At: time.Now()},
	}

	srv := httptest.NewServer(Handler(fake))
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/api/messages/x")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var got messageDetail
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatal(err)
	}
	if got.ID != "x" || got.Status != "DEAD" {
		t.Fatalf("got = %+v", got)
	}
	if len(got.History) != 2 {
		t.Fatalf("history len = %d", len(got.History))
	}
	if got.PayloadPreview != `{"k":1}` {
		t.Fatalf("payload preview = %q", got.PayloadPreview)
	}
}

func TestUI_UnknownStatusIs400(t *testing.T) {
	srv := httptest.NewServer(Handler(newFake()))
	defer srv.Close()
	resp, err := http.Get(srv.URL + "/api/messages?status=nope")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d", resp.StatusCode)
	}
}
