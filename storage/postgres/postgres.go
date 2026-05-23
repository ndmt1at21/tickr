// Package postgres is the PostgreSQL adapter for tickr. It implements
// tickr.Storage on top of pgx/v5 using SELECT ... FOR UPDATE SKIP LOCKED
// for non-blocking parallel claims.
package postgres

import (
	"context"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io/fs"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ndmt1at21/tickr"
)

//go:embed migrations/*.sql
var migrationsFS embed.FS

// Store is the Postgres-backed Storage implementation.
type Store struct {
	pool *pgxpool.Pool
}

// New wraps an existing pgxpool.Pool. Callers own the pool lifecycle.
func New(pool *pgxpool.Pool) *Store { return &Store{pool: pool} }

// --- transaction wrapper ----------------------------------------------------

// WrapTx adapts a pgx.Tx so it can be passed to Client.Enqueue.
func WrapTx(tx pgx.Tx) tickr.Tx { return tickr.MakeTx(tx) }

// querier returns a pgx.Tx if one was supplied, else a wrapper around the
// pool that satisfies the same query/exec surface.
func (s *Store) querier(tx tickr.Tx) querier {
	if tx.IsZero() {
		return poolQuerier{pool: s.pool}
	}
	pgtx, ok := tx.Inner().(pgx.Tx)
	if !ok {
		// Defensive: another adapter's Tx was passed in.
		return poolQuerier{pool: s.pool}
	}
	return txQuerier{tx: pgtx}
}

// querier abstracts over *pgxpool.Pool and pgx.Tx for the methods we use.
type querier interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	SendBatch(ctx context.Context, b *pgx.Batch) pgx.BatchResults
}

type poolQuerier struct{ pool *pgxpool.Pool }

func (q poolQuerier) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	return q.pool.Exec(ctx, sql, args...)
}
func (q poolQuerier) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	return q.pool.Query(ctx, sql, args...)
}
func (q poolQuerier) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	return q.pool.QueryRow(ctx, sql, args...)
}
func (q poolQuerier) SendBatch(ctx context.Context, b *pgx.Batch) pgx.BatchResults {
	return q.pool.SendBatch(ctx, b)
}

type txQuerier struct{ tx pgx.Tx }

func (q txQuerier) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	return q.tx.Exec(ctx, sql, args...)
}
func (q txQuerier) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	return q.tx.Query(ctx, sql, args...)
}
func (q txQuerier) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	return q.tx.QueryRow(ctx, sql, args...)
}
func (q txQuerier) SendBatch(ctx context.Context, b *pgx.Batch) pgx.BatchResults {
	return q.tx.SendBatch(ctx, b)
}

// --- helpers ---------------------------------------------------------------

func newID() uuid.UUID {
	// UUIDv7: time-ordered, no sequence contention, shard-portable.
	id, err := uuid.NewV7()
	if err != nil {
		// NewV7 only errors when the system clock is unreadable; fall back
		// to v4 so enqueues keep working.
		return uuid.New()
	}
	return id
}

func encodeHeaders(h map[string]string) []byte {
	if len(h) == 0 {
		return []byte("{}")
	}
	b, _ := json.Marshal(h)
	return b
}

func decodeHeaders(b []byte) map[string]string {
	if len(b) == 0 {
		return nil
	}
	m := map[string]string{}
	_ = json.Unmarshal(b, &m)
	if len(m) == 0 {
		return nil
	}
	return m
}

// scanMessage reads a tickr_messages row in the canonical column order used
// across all queries returning messages.
func scanMessage(row pgx.Row) (*tickr.InboundMessage, error) {
	var (
		id          uuid.UUID
		eventType   string
		payload     []byte
		headersJSON []byte
		idemKey     *string
		status      string
		attempt     int
		maxAttempts int
		processAt   time.Time
		createdAt   time.Time
		lastErr     *string
	)
	err := row.Scan(&id, &eventType, &payload, &headersJSON, &idemKey, &status,
		&attempt, &maxAttempts, &processAt, &createdAt, &lastErr)
	if err != nil {
		return nil, err
	}
	out := &tickr.InboundMessage{
		ID:          tickr.MessageID(id.String()),
		Type:        eventType,
		Payload:     payload,
		Headers:     decodeHeaders(headersJSON),
		Attempt:     attempt,
		MaxAttempts: maxAttempts,
		EnqueuedAt:  createdAt,
		ScheduledAt: processAt,
	}
	if idemKey != nil {
		out.IdempotencyKey = *idemKey
	}
	if lastErr != nil {
		out.LastError = *lastErr
	}
	return out, nil
}

const messageReturnCols = `id, event_type, payload, headers, idempotency_key,
	status, attempt, max_attempts, process_at, created_at, last_error`

// --- Enqueue ----------------------------------------------------------------

const enqueueSQL = `
INSERT INTO tickr_messages
    (id, event_type, payload, headers, idempotency_key, status,
     attempt, max_attempts, process_at, created_at, updated_at)
VALUES ($1, $2, $3, $4, $5, 'CREATED',
        0, $6, $7, now(), now())
ON CONFLICT (event_type, idempotency_key) WHERE idempotency_key IS NOT NULL
    DO NOTHING
RETURNING ` + messageReturnCols

// Enqueue implements tickr.Storage.
func (s *Store) Enqueue(ctx context.Context, tx tickr.Tx, p tickr.EnqueueParams) (*tickr.InboundMessage, error) {
	q := s.querier(tx)
	id := newID()
	processAt := p.ProcessAt
	if processAt.IsZero() {
		processAt = time.Now()
	}
	maxAttempts := p.MaxAttempts
	if maxAttempts <= 0 {
		maxAttempts = 10
	}

	var idemArg any
	if p.IdempotencyKey != "" {
		idemArg = p.IdempotencyKey
	}

	row := q.QueryRow(ctx, enqueueSQL,
		id, p.EventType, p.Payload, encodeHeaders(p.Headers),
		idemArg, maxAttempts, processAt)
	msg, err := scanMessage(row)
	if err == nil {
		return msg, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return nil, fmt.Errorf("tickr/postgres: enqueue: %w", err)
	}
	// Duplicate idempotency key — fetch the existing row.
	if p.IdempotencyKey == "" {
		return nil, fmt.Errorf("tickr/postgres: enqueue returned no rows but no idempotency key was set")
	}
	existing, err := s.findByIdempotency(ctx, q, p.EventType, p.IdempotencyKey)
	if err != nil {
		return nil, err
	}
	return existing, &tickr.ErrDuplicate{
		ExistingID: existing.ID,
		EventType:  p.EventType,
		Key:        p.IdempotencyKey,
	}
}

const findByIdemSQL = `
SELECT ` + messageReturnCols + `
FROM tickr_messages
WHERE event_type = $1 AND idempotency_key = $2`

func (s *Store) findByIdempotency(ctx context.Context, q querier, eventType, key string) (*tickr.InboundMessage, error) {
	row := q.QueryRow(ctx, findByIdemSQL, eventType, key)
	msg, err := scanMessage(row)
	if err != nil {
		return nil, fmt.Errorf("tickr/postgres: fetch by idempotency key: %w", err)
	}
	return msg, nil
}

// EnqueueBatch implements tickr.Storage. Uses pgx.Batch to pipeline
// inserts in one round-trip.
func (s *Store) EnqueueBatch(ctx context.Context, tx tickr.Tx, ps []tickr.EnqueueParams) ([]*tickr.InboundMessage, error) {
	if len(ps) == 0 {
		return nil, nil
	}
	q := s.querier(tx)
	batch := &pgx.Batch{}
	for _, p := range ps {
		id := newID()
		processAt := p.ProcessAt
		if processAt.IsZero() {
			processAt = time.Now()
		}
		maxAttempts := p.MaxAttempts
		if maxAttempts <= 0 {
			maxAttempts = 10
		}
		var idemArg any
		if p.IdempotencyKey != "" {
			idemArg = p.IdempotencyKey
		}
		batch.Queue(enqueueSQL,
			id, p.EventType, p.Payload, encodeHeaders(p.Headers),
			idemArg, maxAttempts, processAt)
	}
	br := q.SendBatch(ctx, batch)
	defer br.Close()

	out := make([]*tickr.InboundMessage, 0, len(ps))
	var firstErr error
	for i, p := range ps {
		row := br.QueryRow()
		msg, err := scanMessage(row)
		if err == nil {
			out = append(out, msg)
			continue
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			if firstErr == nil {
				firstErr = fmt.Errorf("tickr/postgres: enqueue batch item %d: %w", i, err)
			}
			out = append(out, nil)
			continue
		}
		// Duplicate idempotency key for this row.
		if p.IdempotencyKey == "" {
			if firstErr == nil {
				firstErr = fmt.Errorf("tickr/postgres: enqueue batch item %d: no rows but no idempotency key", i)
			}
			out = append(out, nil)
			continue
		}
		// Defer the lookup until after we've drained the batch.
		out = append(out, nil)
	}
	if err := br.Close(); err != nil && firstErr == nil {
		firstErr = err
	}
	if firstErr != nil {
		return out, firstErr
	}
	// Resolve duplicates (post-batch) so we can report ErrDuplicate.
	var dupErr error
	for i, p := range ps {
		if out[i] != nil || p.IdempotencyKey == "" {
			continue
		}
		existing, err := s.findByIdempotency(ctx, q, p.EventType, p.IdempotencyKey)
		if err != nil {
			return out, err
		}
		out[i] = existing
		if dupErr == nil {
			dupErr = &tickr.ErrDuplicate{
				ExistingID: existing.ID,
				EventType:  p.EventType,
				Key:        p.IdempotencyKey,
			}
		}
	}
	return out, dupErr
}

// --- Claim ------------------------------------------------------------------

const claimSQL = `
WITH eligible AS (
    SELECT id
    FROM   tickr_messages
    WHERE  status IN ('CREATED','RETRYING')
      AND  process_at <= now()
      AND  ($1::text[] IS NULL OR event_type = ANY($1))
    ORDER BY process_at, id
    LIMIT  $2
    FOR UPDATE SKIP LOCKED
)
UPDATE tickr_messages m
SET    status        = 'HANDLING',
       attempt       = m.attempt + 1,
       claimed_by    = $3,
       claimed_until = now() + ($4 || ' seconds')::interval,
       updated_at    = now()
FROM   eligible e
WHERE  m.id = e.id
RETURNING ` + messageReturnCols

func (s *Store) Claim(ctx context.Context, p tickr.ClaimParams) ([]*tickr.InboundMessage, error) {
	if p.Batch <= 0 {
		p.Batch = 1
	}
	leaseSec := int(p.Lease.Seconds())
	if leaseSec <= 0 {
		leaseSec = 30
	}
	var eventTypes any
	if len(p.EventTypes) > 0 {
		eventTypes = p.EventTypes
	}

	rows, err := s.pool.Query(ctx, claimSQL, eventTypes, p.Batch, p.WorkerID, fmt.Sprintf("%d", leaseSec))
	if err != nil {
		return nil, fmt.Errorf("tickr/postgres: claim: %w", err)
	}
	defer rows.Close()

	out := make([]*tickr.InboundMessage, 0, p.Batch)
	for rows.Next() {
		msg, err := scanMessage(rows)
		if err != nil {
			return nil, fmt.Errorf("tickr/postgres: claim scan: %w", err)
		}
		// Append HANDLING history row inline (best-effort; not in same tx
		// intentionally so a hang here doesn't block the claim).
		out = append(out, msg)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("tickr/postgres: claim rows: %w", err)
	}

	// Append a CREATED/RETRYING → HANDLING history row per claimed message.
	if len(out) > 0 {
		if err := s.appendHistoryBatch(ctx, out, tickr.StatusHandling, "", p.WorkerID); err != nil {
			// History best-effort; log via returned err — engine logs it.
			return out, fmt.Errorf("tickr/postgres: claim history: %w", err)
		}
	}
	return out, nil
}

// --- Succeed / Fail / Release / Extend -------------------------------------

const succeedSQL = `
UPDATE tickr_messages
SET    status       = 'SUCCESS',
       claimed_until = NULL,
       claimed_by    = NULL,
       last_error    = NULL,
       completed_at  = now(),
       updated_at    = now()
WHERE  id = $1
  AND  status = 'HANDLING'
  AND  attempt = $2`

func (s *Store) Succeed(ctx context.Context, id tickr.MessageID, attempt int, workerID string) error {
	mid, err := uuid.Parse(string(id))
	if err != nil {
		return fmt.Errorf("tickr/postgres: succeed: bad id: %w", err)
	}
	tag, err := s.pool.Exec(ctx, succeedSQL, mid, attempt)
	if err != nil {
		return fmt.Errorf("tickr/postgres: succeed: %w", err)
	}
	if tag.RowsAffected() == 0 {
		// Lease lost or stale attempt — silently ignore.
		return nil
	}
	return s.appendHistory(ctx, id, tickr.StatusHandling, tickr.StatusSuccess, attempt, "", workerID)
}

const failRetrySQL = `
UPDATE tickr_messages
SET    status        = 'RETRYING',
       claimed_until = NULL,
       claimed_by    = NULL,
       last_error    = $3,
       process_at    = $4,
       updated_at    = now()
WHERE  id = $1
  AND  status = 'HANDLING'
  AND  attempt = $2`

const failDeadSQL = `
UPDATE tickr_messages
SET    status        = 'DEAD',
       claimed_until = NULL,
       claimed_by    = NULL,
       last_error    = $3,
       completed_at  = now(),
       updated_at    = now()
WHERE  id = $1
  AND  status = 'HANDLING'
  AND  attempt = $2`

func (s *Store) Fail(ctx context.Context, p tickr.FailParams) error {
	mid, err := uuid.Parse(string(p.MessageID))
	if err != nil {
		return fmt.Errorf("tickr/postgres: fail: bad id: %w", err)
	}
	var (
		tag  pgconn.CommandTag
		next tickr.Status
	)
	// History records the transient FAILED state, then the resolved state.
	if err := s.appendHistory(ctx, p.MessageID, tickr.StatusHandling, tickr.StatusFailed, p.Attempt, p.Err, p.WorkerID); err != nil {
		return err
	}
	if p.Dead {
		tag, err = s.pool.Exec(ctx, failDeadSQL, mid, p.Attempt, p.Err)
		next = tickr.StatusDead
	} else {
		tag, err = s.pool.Exec(ctx, failRetrySQL, mid, p.Attempt, p.Err, p.NextRetryAt)
		next = tickr.StatusRetrying
	}
	if err != nil {
		return fmt.Errorf("tickr/postgres: fail: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return nil // stale lease
	}
	return s.appendHistory(ctx, p.MessageID, tickr.StatusFailed, next, p.Attempt, p.Err, p.WorkerID)
}

const releaseShutdownSQL = `
UPDATE tickr_messages
SET    status        = 'RETRYING',
       claimed_until = NULL,
       claimed_by    = NULL,
       attempt       = GREATEST(0, attempt - 1),
       process_at    = now(),
       last_error    = $3,
       updated_at    = now()
WHERE  id = $1
  AND  status = 'HANDLING'
  AND  attempt = $2`

func (s *Store) ReleaseShutdown(ctx context.Context, id tickr.MessageID, attempt int, workerID, reason string) error {
	mid, err := uuid.Parse(string(id))
	if err != nil {
		return fmt.Errorf("tickr/postgres: release: bad id: %w", err)
	}
	tag, err := s.pool.Exec(ctx, releaseShutdownSQL, mid, attempt, reason)
	if err != nil {
		return fmt.Errorf("tickr/postgres: release: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return nil
	}
	return s.appendHistory(ctx, id, tickr.StatusHandling, tickr.StatusRetrying, attempt, reason, workerID)
}

const extendSQL = `
UPDATE tickr_messages
SET    claimed_until = $3,
       updated_at    = now()
WHERE  id = $1
  AND  status = 'HANDLING'
  AND  claimed_by = $2`

func (s *Store) Extend(ctx context.Context, id tickr.MessageID, workerID string, until time.Time) (bool, error) {
	mid, err := uuid.Parse(string(id))
	if err != nil {
		return false, fmt.Errorf("tickr/postgres: extend: bad id: %w", err)
	}
	tag, err := s.pool.Exec(ctx, extendSQL, mid, workerID, until)
	if err != nil {
		return false, fmt.Errorf("tickr/postgres: extend: %w", err)
	}
	return tag.RowsAffected() > 0, nil
}

// --- History ----------------------------------------------------------------

const appendHistorySQL = `
INSERT INTO tickr_history (message_id, seq, from_status, to_status, attempt, error, worker_id)
SELECT $1, COALESCE(MAX(seq), 0) + 1, $2, $3, $4, NULLIF($5, ''), NULLIF($6, '')
FROM   tickr_history WHERE message_id = $1`

func (s *Store) appendHistory(ctx context.Context, id tickr.MessageID, from, to tickr.Status, attempt int, errStr, workerID string) error {
	mid, err := uuid.Parse(string(id))
	if err != nil {
		return fmt.Errorf("tickr/postgres: history: bad id: %w", err)
	}
	var fromArg any
	if from != "" {
		fromArg = string(from)
	}
	_, err = s.pool.Exec(ctx, appendHistorySQL, mid, fromArg, string(to), attempt, errStr, workerID)
	if err != nil {
		return fmt.Errorf("tickr/postgres: history insert: %w", err)
	}
	return nil
}

func (s *Store) appendHistoryBatch(ctx context.Context, msgs []*tickr.InboundMessage, to tickr.Status, errStr, workerID string) error {
	if len(msgs) == 0 {
		return nil
	}
	batch := &pgx.Batch{}
	for _, m := range msgs {
		mid, err := uuid.Parse(string(m.ID))
		if err != nil {
			continue
		}
		// from is implied (CREATED or RETRYING — we don't track which here;
		// engine logs the prior status via msg.Attempt > 0 if needed).
		batch.Queue(appendHistorySQL, mid, nil, string(to), m.Attempt, errStr, workerID)
	}
	br := s.pool.SendBatch(ctx, batch)
	defer br.Close()
	for range msgs {
		if _, err := br.Exec(); err != nil {
			return err
		}
	}
	return nil
}

const historySQL = `
SELECT message_id, seq, from_status, to_status, attempt, error, worker_id, at
FROM   tickr_history
WHERE  message_id = $1
ORDER BY seq`

func (s *Store) History(ctx context.Context, id tickr.MessageID) ([]tickr.Transition, error) {
	mid, err := uuid.Parse(string(id))
	if err != nil {
		return nil, fmt.Errorf("tickr/postgres: history: bad id: %w", err)
	}
	rows, err := s.pool.Query(ctx, historySQL, mid)
	if err != nil {
		return nil, fmt.Errorf("tickr/postgres: history query: %w", err)
	}
	defer rows.Close()
	var out []tickr.Transition
	for rows.Next() {
		var (
			messageID uuid.UUID
			seq       int
			from      *string
			to        string
			attempt   int
			errStr    *string
			workerID  *string
			at        time.Time
		)
		if err := rows.Scan(&messageID, &seq, &from, &to, &attempt, &errStr, &workerID, &at); err != nil {
			return nil, err
		}
		t := tickr.Transition{
			MessageID: tickr.MessageID(messageID.String()),
			Seq:       seq,
			To:        tickr.Status(to),
			Attempt:   attempt,
			At:        at,
		}
		if from != nil {
			t.From = tickr.Status(*from)
		}
		if errStr != nil {
			t.Error = *errStr
		}
		if workerID != nil {
			t.WorkerID = *workerID
		}
		out = append(out, t)
	}
	return out, rows.Err()
}

// --- ListDead / Requeue ----------------------------------------------------

const listDeadSQL = `
SELECT ` + messageReturnCols + `
FROM   tickr_messages
WHERE  status = 'DEAD'
  AND  ($1 = '' OR event_type = $1)
  AND  completed_at > $2
ORDER BY completed_at DESC
LIMIT  $3`

func (s *Store) ListDead(ctx context.Context, eventType string, after time.Time, limit int) ([]*tickr.InboundMessage, error) {
	if limit <= 0 {
		limit = 100
	}
	rows, err := s.pool.Query(ctx, listDeadSQL, eventType, after, limit)
	if err != nil {
		return nil, fmt.Errorf("tickr/postgres: list dead: %w", err)
	}
	defer rows.Close()
	var out []*tickr.InboundMessage
	for rows.Next() {
		m, err := scanMessage(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, m)
	}
	return out, rows.Err()
}

const requeueSQL = `
UPDATE tickr_messages
SET    status        = 'CREATED',
       attempt       = 0,
       process_at    = $2,
       completed_at  = NULL,
       last_error    = NULL,
       updated_at    = now()
WHERE  id = $1 AND status = 'DEAD'`

func (s *Store) Requeue(ctx context.Context, id tickr.MessageID, processAt time.Time) error {
	mid, err := uuid.Parse(string(id))
	if err != nil {
		return fmt.Errorf("tickr/postgres: requeue: bad id: %w", err)
	}
	if processAt.IsZero() {
		processAt = time.Now()
	}
	tag, err := s.pool.Exec(ctx, requeueSQL, mid, processAt)
	if err != nil {
		return fmt.Errorf("tickr/postgres: requeue: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("tickr/postgres: requeue: message not found or not DEAD")
	}
	return s.appendHistory(ctx, id, tickr.StatusDead, tickr.StatusCreated, 0, "manual requeue", "")
}

// --- Reclaim / Purge --------------------------------------------------------

const reclaimSQL = `
UPDATE tickr_messages
SET    status        = 'RETRYING',
       claimed_until = NULL,
       claimed_by    = NULL,
       last_error    = COALESCE(last_error, '') || ' [lease expired]',
       process_at    = now(),
       updated_at    = now()
WHERE  id IN (
    SELECT id FROM tickr_messages
    WHERE  status = 'HANDLING' AND claimed_until < now()
    LIMIT  $1
    FOR UPDATE SKIP LOCKED
)
RETURNING 1`

func (s *Store) ReclaimExpired(ctx context.Context, limit int) (int64, error) {
	if limit <= 0 {
		limit = 500
	}
	tag, err := s.pool.Exec(ctx, reclaimSQL, limit)
	if err != nil {
		return 0, fmt.Errorf("tickr/postgres: reclaim: %w", err)
	}
	return tag.RowsAffected(), nil
}

const purgeSQL = `
DELETE FROM tickr_messages
WHERE id IN (
    SELECT id FROM tickr_messages
    WHERE  status IN ('SUCCESS','DEAD') AND completed_at < $1
    ORDER BY completed_at
    LIMIT  $2
)`

func (s *Store) PurgeTerminal(ctx context.Context, before time.Time, limit int) (int64, error) {
	if limit <= 0 {
		limit = 5000
	}
	tag, err := s.pool.Exec(ctx, purgeSQL, before, limit)
	if err != nil {
		return 0, fmt.Errorf("tickr/postgres: purge: %w", err)
	}
	return tag.RowsAffected(), nil
}

// --- Stats ------------------------------------------------------------------

const statsSQL = `
SELECT event_type, status, count(*)::int
FROM   tickr_messages
GROUP BY event_type, status`

func (s *Store) Stats(ctx context.Context) (tickr.Stats, error) {
	rows, err := s.pool.Query(ctx, statsSQL)
	if err != nil {
		return tickr.Stats{}, fmt.Errorf("tickr/postgres: stats: %w", err)
	}
	defer rows.Close()
	out := tickr.Stats{
		ByStatus:    map[tickr.Status]int{},
		ByEventType: map[string]map[tickr.Status]int{},
		SampledAt:   time.Now(),
	}
	for rows.Next() {
		var (
			eventType string
			status    string
			count     int
		)
		if err := rows.Scan(&eventType, &status, &count); err != nil {
			return tickr.Stats{}, err
		}
		st := tickr.Status(status)
		out.ByStatus[st] += count
		if _, ok := out.ByEventType[eventType]; !ok {
			out.ByEventType[eventType] = map[tickr.Status]int{}
		}
		out.ByEventType[eventType][st] = count
	}
	return out, rows.Err()
}

// --- Migrations & Leader Lock ----------------------------------------------

func (s *Store) ApplyMigrations(ctx context.Context) error {
	entries, err := fs.ReadDir(migrationsFS, "migrations")
	if err != nil {
		return fmt.Errorf("tickr/postgres: read migrations: %w", err)
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Name() < entries[j].Name() })

	// Ensure version table exists. Use a bootstrap statement that doesn't
	// rely on prior migration order.
	if _, err := s.pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS tickr_schema_version (
			version    INT PRIMARY KEY,
			applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
		)`); err != nil {
		return fmt.Errorf("tickr/postgres: bootstrap version table: %w", err)
	}

	applied := map[int]bool{}
	rows, err := s.pool.Query(ctx, `SELECT version FROM tickr_schema_version`)
	if err != nil {
		return fmt.Errorf("tickr/postgres: read versions: %w", err)
	}
	for rows.Next() {
		var v int
		if err := rows.Scan(&v); err != nil {
			rows.Close()
			return err
		}
		applied[v] = true
	}
	rows.Close()

	for _, e := range entries {
		name := e.Name()
		if !strings.HasSuffix(name, ".sql") {
			continue
		}
		var version int
		_, _ = fmt.Sscanf(name, "%04d_", &version)
		if version == 0 {
			continue
		}
		if applied[version] {
			continue
		}
		b, err := fs.ReadFile(migrationsFS, "migrations/"+name)
		if err != nil {
			return fmt.Errorf("tickr/postgres: read migration %s: %w", name, err)
		}
		tx, err := s.pool.Begin(ctx)
		if err != nil {
			return fmt.Errorf("tickr/postgres: begin migration %s: %w", name, err)
		}
		if _, err := tx.Exec(ctx, string(b)); err != nil {
			_ = tx.Rollback(ctx)
			return fmt.Errorf("tickr/postgres: apply migration %s: %w", name, err)
		}
		if _, err := tx.Exec(ctx, `INSERT INTO tickr_schema_version (version) VALUES ($1) ON CONFLICT DO NOTHING`, version); err != nil {
			_ = tx.Rollback(ctx)
			return fmt.Errorf("tickr/postgres: record migration %s: %w", name, err)
		}
		if err := tx.Commit(ctx); err != nil {
			return fmt.Errorf("tickr/postgres: commit migration %s: %w", name, err)
		}
	}
	return nil
}

// TryLeaderLock acquires a session-scoped pg_try_advisory_lock keyed by
// a stable hash of key. The returned unlock func releases the lock and
// returns the connection to the pool.
func (s *Store) TryLeaderLock(ctx context.Context, key string) (bool, func(), error) {
	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		return false, nil, fmt.Errorf("tickr/postgres: acquire conn: %w", err)
	}
	h := fnv.New64a()
	_, _ = h.Write([]byte(key))
	lockKey := int64(h.Sum64()) //nolint:gosec // pg_try_advisory_lock takes signed int64
	var ok bool
	if err := conn.QueryRow(ctx, `SELECT pg_try_advisory_lock($1)`, lockKey).Scan(&ok); err != nil {
		conn.Release()
		return false, nil, fmt.Errorf("tickr/postgres: try advisory lock: %w", err)
	}
	if !ok {
		conn.Release()
		return false, nil, nil
	}
	unlock := func() {
		// Best-effort release; ignore errors.
		_, _ = conn.Exec(context.Background(), `SELECT pg_advisory_unlock($1)`, lockKey)
		conn.Release()
	}
	return true, unlock, nil
}

// Ensure Store satisfies tickr.Storage at compile time.
var _ tickr.Storage = (*Store)(nil)
