// Package ui serves an embeddable admin dashboard for a tickr outbox.
//
// Mount it on any [http.ServeMux]:
//
//	mux.Handle("/tickr/", http.StripPrefix("/tickr", ui.Handler(store)))
//
// The dashboard lists messages filtered by status and event type, shows
// per-message history, and exposes retry / kill / requeue actions. It is
// transport-agnostic — pair it with your own auth middleware before
// exposing it on a public URL.
//
// The handler requires the underlying [tickr.Storage] to also implement
// [tickr.Admin] (the bundled PostgreSQL adapter does).
package ui

import (
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/ndmt1at21/tickr"
)

//go:embed assets/*
var assetsFS embed.FS

// Options configures the UI handler.
type Options struct {
	// Title is shown in the page header. Defaults to "tickr".
	Title string

	// ReadOnly disables mutating endpoints (requeue, kill). Useful when
	// exposing the dashboard to viewers who shouldn't be able to take
	// recovery actions.
	ReadOnly bool
}

// Handler returns an [http.Handler] serving the dashboard at "/" and JSON
// APIs under "/api/". store must implement [tickr.Admin]; if it doesn't,
// the handler returns 501 Not Implemented from list/get endpoints.
func Handler(store tickr.Storage, opts ...Options) http.Handler {
	var o Options
	if len(opts) > 0 {
		o = opts[0]
	}
	if o.Title == "" {
		o.Title = "tickr"
	}

	admin, _ := store.(tickr.Admin)
	srv := &server{store: store, admin: admin, opts: o}

	mux := http.NewServeMux()
	mux.HandleFunc("/api/stats", srv.handleStats)
	mux.HandleFunc("/api/messages", srv.handleListMessages)
	mux.HandleFunc("/api/messages/", srv.handleMessageByID)
	mux.HandleFunc("/api/event-types", srv.handleEventTypes)

	sub, _ := fs.Sub(assetsFS, "assets")
	mux.Handle("/static/", http.StripPrefix("/static/", http.FileServer(http.FS(sub))))
	mux.HandleFunc("/", srv.handleIndex)

	return mux
}

type server struct {
	store tickr.Storage
	admin tickr.Admin
	opts  Options
}

// --- HTML ------------------------------------------------------------------

func (s *server) handleIndex(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	b, err := fs.ReadFile(assetsFS, "assets/index.html")
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	html := strings.ReplaceAll(string(b), "{{TITLE}}", s.opts.Title)
	html = strings.ReplaceAll(html, "{{READONLY}}", strconv.FormatBool(s.opts.ReadOnly))
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write([]byte(html))
}

// --- /api/stats ------------------------------------------------------------

type statsResponse struct {
	ByStatus    map[string]int            `json:"by_status"`
	ByEventType map[string]map[string]int `json:"by_event_type"`
	SampledAt   time.Time                 `json:"sampled_at"`
}

func (s *server) handleStats(w http.ResponseWriter, r *http.Request) {
	stats, err := s.store.Stats(r.Context())
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	out := statsResponse{
		ByStatus:    make(map[string]int, len(stats.ByStatus)),
		ByEventType: make(map[string]map[string]int, len(stats.ByEventType)),
		SampledAt:   stats.SampledAt,
	}
	for k, v := range stats.ByStatus {
		out.ByStatus[string(k)] = v
	}
	for et, byStatus := range stats.ByEventType {
		m := make(map[string]int, len(byStatus))
		for k, v := range byStatus {
			m[string(k)] = v
		}
		out.ByEventType[et] = m
	}
	writeJSON(w, http.StatusOK, out)
}

// --- /api/event-types ------------------------------------------------------

func (s *server) handleEventTypes(w http.ResponseWriter, r *http.Request) {
	stats, err := s.store.Stats(r.Context())
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	types := make([]string, 0, len(stats.ByEventType))
	for t := range stats.ByEventType {
		types = append(types, t)
	}
	writeJSON(w, http.StatusOK, types)
}

// --- /api/messages ---------------------------------------------------------

type listResponse struct {
	Messages []messageDTO `json:"messages"`
	NextID   string       `json:"next_id,omitempty"`
}

type messageDTO struct {
	ID             string            `json:"id"`
	Type           string            `json:"type"`
	Status         string            `json:"status"`
	Attempt        int               `json:"attempt"`
	MaxAttempts    int               `json:"max_attempts"`
	EnqueuedAt     time.Time         `json:"enqueued_at"`
	ScheduledAt    time.Time         `json:"scheduled_at"`
	IdempotencyKey string            `json:"idempotency_key,omitempty"`
	LastError      string            `json:"last_error,omitempty"`
	Headers        map[string]string `json:"headers,omitempty"`
	PayloadPreview string            `json:"payload_preview,omitempty"`
	PayloadSize    int               `json:"payload_size"`
}

func (s *server) handleListMessages(w http.ResponseWriter, r *http.Request) {
	if s.admin == nil {
		writeErr(w, http.StatusNotImplemented,
			errors.New("storage adapter does not implement tickr.Admin"))
		return
	}
	q := r.URL.Query()
	limit, _ := strconv.Atoi(q.Get("limit"))
	if limit <= 0 {
		limit = 50
	}
	statusFilter := tickr.Status(strings.ToUpper(q.Get("status")))
	if statusFilter != "" && !knownStatus(statusFilter) {
		writeErr(w, http.StatusBadRequest, fmt.Errorf("unknown status %q", statusFilter))
		return
	}
	msgs, err := s.admin.List(r.Context(), tickr.ListQuery{
		Status:    statusFilter,
		EventType: q.Get("event_type"),
		Search:    q.Get("q"),
		AfterID:   tickr.MessageID(q.Get("after_id")),
		Limit:     limit,
	})
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	resp := listResponse{Messages: make([]messageDTO, 0, len(msgs))}
	for _, m := range msgs {
		resp.Messages = append(resp.Messages, toDTO(m, string(m.Status)))
	}
	if len(msgs) == limit {
		resp.NextID = string(msgs[len(msgs)-1].ID)
	}
	writeJSON(w, http.StatusOK, resp)
}

func toDTO(m *tickr.InboundMessage, status string) messageDTO {
	dto := messageDTO{
		ID:             string(m.ID),
		Type:           m.Type,
		Status:         status,
		Attempt:        m.Attempt,
		MaxAttempts:    m.MaxAttempts,
		EnqueuedAt:     m.EnqueuedAt,
		ScheduledAt:    m.ScheduledAt,
		IdempotencyKey: m.IdempotencyKey,
		LastError:      m.LastError,
		Headers:        m.Headers,
		PayloadSize:    len(m.Payload),
	}
	if len(m.Payload) > 0 {
		dto.PayloadPreview = previewPayload(m.Payload)
	}
	return dto
}

func previewPayload(b []byte) string {
	const max = 512
	s := string(b)
	if len(s) > max {
		return s[:max] + "…"
	}
	return s
}

// --- /api/messages/{id}[/action] -------------------------------------------

func (s *server) handleMessageByID(w http.ResponseWriter, r *http.Request) {
	if s.admin == nil {
		writeErr(w, http.StatusNotImplemented,
			errors.New("storage adapter does not implement tickr.Admin"))
		return
	}
	rest := strings.TrimPrefix(r.URL.Path, "/api/messages/")
	id, action, _ := strings.Cut(rest, "/")
	if id == "" {
		http.NotFound(w, r)
		return
	}
	mid := tickr.MessageID(id)

	switch action {
	case "":
		s.getMessage(w, r, mid)
	case "requeue":
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		s.requeueMessage(w, r, mid)
	case "kill":
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		s.killMessage(w, r, mid)
	default:
		http.NotFound(w, r)
	}
}

type messageDetail struct {
	messageDTO
	History []transitionDTO `json:"history"`
}

type transitionDTO struct {
	Seq      int       `json:"seq"`
	From     string    `json:"from"`
	To       string    `json:"to"`
	Attempt  int       `json:"attempt"`
	Error    string    `json:"error,omitempty"`
	WorkerID string    `json:"worker_id,omitempty"`
	At       time.Time `json:"at"`
}

func (s *server) getMessage(w http.ResponseWriter, r *http.Request, id tickr.MessageID) {
	msg, err := s.admin.Get(r.Context(), id)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	if msg == nil {
		http.NotFound(w, r)
		return
	}
	hist, err := s.store.History(r.Context(), id)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	// The history's last "to" status is the current status.
	cur := ""
	if len(hist) > 0 {
		cur = string(hist[len(hist)-1].To)
	}
	out := messageDetail{messageDTO: toDTO(msg, cur)}
	for _, t := range hist {
		out.History = append(out.History, transitionDTO{
			Seq: t.Seq, From: string(t.From), To: string(t.To),
			Attempt: t.Attempt, Error: t.Error, WorkerID: t.WorkerID, At: t.At,
		})
	}
	writeJSON(w, http.StatusOK, out)
}

func (s *server) requeueMessage(w http.ResponseWriter, r *http.Request, id tickr.MessageID) {
	if s.opts.ReadOnly {
		writeErr(w, http.StatusForbidden, errors.New("dashboard is read-only"))
		return
	}
	if err := s.store.Requeue(r.Context(), id, time.Now()); err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "requeued"})
}

func (s *server) killMessage(w http.ResponseWriter, r *http.Request, id tickr.MessageID) {
	if s.opts.ReadOnly {
		writeErr(w, http.StatusForbidden, errors.New("dashboard is read-only"))
		return
	}
	reason := r.URL.Query().Get("reason")
	if err := s.admin.Kill(r.Context(), id, reason); err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "killed"})
}

// --- helpers ---------------------------------------------------------------

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, code int, err error) {
	writeJSON(w, code, map[string]string{"error": err.Error()})
}

func knownStatus(s tickr.Status) bool {
	switch s {
	case tickr.StatusCreated, tickr.StatusHandling, tickr.StatusSuccess,
		tickr.StatusFailed, tickr.StatusRetrying, tickr.StatusDead:
		return true
	}
	return false
}

