// Package mysql is the MySQL 8.0+ adapter for tickr. It implements
// tickr.Storage on top of database/sql + github.com/go-sql-driver/mysql,
// using SELECT ... FOR UPDATE SKIP LOCKED for non-blocking parallel claims.
//
// MySQL has no LISTEN/NOTIFY equivalent, so this adapter does NOT implement
// tickr.Notifier — workers fall back to pure polling. Leader election uses
// GET_LOCK/RELEASE_LOCK on a held connection (session-scoped).
package mysql

import (
	"context"
	"database/sql"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/ndmt1at21/tickr"
)

//go:embed migrations/*.sql
var migrationsFS embed.FS

// Store is the MySQL-backed Storage implementation.
type Store struct {
	db *sql.DB
}

// New wraps an existing *sql.DB. Callers own the lifecycle.
func New(db *sql.DB) *Store { return &Store{db: db} }

// WrapTx adapts a *sql.Tx so it can be passed to Client.Enqueue.
func WrapTx(tx *sql.Tx) tickr.Tx { return tickr.MakeTx(tx) }

type execer interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

func (s *Store) execer(tx tickr.Tx) execer {
	if tx.IsZero() {
		return s.db
	}
	sqltx, ok := tx.Inner().(*sql.Tx)
	if !ok {
		return s.db
	}
	return sqltx
}

func newID() string {
	id, err := uuid.NewV7()
	if err != nil {
		id = uuid.New()
	}
	return id.String()
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

const messageReturnCols = `id, event_type, payload, headers, idempotency_key,
    status, attempt, max_attempts, process_at, created_at, last_error`

type rowScanner interface {
	Scan(dest ...any) error
}

func scanMessage(row rowScanner) (*tickr.InboundMessage, error) {
	var (
		id          string
		eventType   string
		payload     []byte
		headersJSON []byte
		idemKey     sql.NullString
		status      string
		attempt     int
		maxAttempts int
		processAt   time.Time
		createdAt   time.Time
		lastErr     sql.NullString
	)
	if err := row.Scan(&id, &eventType, &payload, &headersJSON, &idemKey, &status,
		&attempt, &maxAttempts, &processAt, &createdAt, &lastErr); err != nil {
		return nil, err
	}
	out := &tickr.InboundMessage{
		ID:          tickr.MessageID(id),
		Type:        eventType,
		Payload:     payload,
		Headers:     decodeHeaders(headersJSON),
		Attempt:     attempt,
		MaxAttempts: maxAttempts,
		EnqueuedAt:  createdAt,
		ScheduledAt: processAt,
		IdempotencyKey: func() string {
			if idemKey.Valid {
				return idemKey.String
			}
			return ""
		}(),
		LastError: func() string {
			if lastErr.Valid {
				return lastErr.String
			}
			return ""
		}(),
	}
	return out, nil
}

// --- Enqueue ----------------------------------------------------------------

const enqueueSQL = `
INSERT INTO tickr_messages
    (id, event_type, payload, headers, idempotency_key, status,
     attempt, max_attempts, process_at, created_at, updated_at)
VALUES (?, ?, ?, ?, ?, 'CREATED',
        0, ?, ?, CURRENT_TIMESTAMP(6), CURRENT_TIMESTAMP(6))`

const findByIdemSQL = `
SELECT ` + messageReturnCols + `
FROM tickr_messages
WHERE event_type = ? AND idempotency_key = ?`

const findByIDSQL = `
SELECT ` + messageReturnCols + `
FROM tickr_messages
WHERE id = ?`

// Enqueue implements tickr.Storage.
func (s *Store) Enqueue(ctx context.Context, tx tickr.Tx, p tickr.EnqueueParams) (*tickr.InboundMessage, error) {
	ex := s.execer(tx)
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

	_, err := ex.ExecContext(ctx, enqueueSQL,
		id, p.EventType, p.Payload, encodeHeaders(p.Headers),
		idemArg, maxAttempts, processAt)
	if err != nil {
		if isDuplicateErr(err) && p.IdempotencyKey != "" {
			existing, ferr := s.findByIdempotency(ctx, ex, p.EventType, p.IdempotencyKey)
			if ferr != nil {
				return nil, ferr
			}
			return existing, &tickr.ErrDuplicate{
				ExistingID: existing.ID,
				EventType:  p.EventType,
				Key:        p.IdempotencyKey,
			}
		}
		return nil, fmt.Errorf("tickr/mysql: enqueue: %w", err)
	}
	return s.findByID(ctx, ex, tickr.MessageID(id))
}

// EnqueueBatch implements tickr.Storage.
func (s *Store) EnqueueBatch(ctx context.Context, tx tickr.Tx, ps []tickr.EnqueueParams) ([]*tickr.InboundMessage, error) {
	out := make([]*tickr.InboundMessage, 0, len(ps))
	var firstErr error
	for _, p := range ps {
		msg, err := s.Enqueue(ctx, tx, p)
		if err != nil && !tickr.IsDuplicate(err) {
			if firstErr == nil {
				firstErr = err
			}
			out = append(out, nil)
			continue
		}
		out = append(out, msg)
	}
	return out, firstErr
}

func (s *Store) findByIdempotency(ctx context.Context, ex execer, eventType, key string) (*tickr.InboundMessage, error) {
	row := ex.QueryRowContext(ctx, findByIdemSQL, eventType, key)
	msg, err := scanMessage(row)
	if err != nil {
		return nil, fmt.Errorf("tickr/mysql: fetch by idempotency key: %w", err)
	}
	return msg, nil
}

func (s *Store) findByID(ctx context.Context, ex execer, id tickr.MessageID) (*tickr.InboundMessage, error) {
	row := ex.QueryRowContext(ctx, findByIDSQL, string(id))
	return scanMessage(row)
}

// isDuplicateErr matches the MySQL "Duplicate entry" error (errno 1062).
func isDuplicateErr(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), "Error 1062") ||
		strings.Contains(err.Error(), "Duplicate entry")
}

// --- Claim ------------------------------------------------------------------

// claimSQL: select candidate rows non-blockingly (SKIP LOCKED), then update
// them in the same tx. database/sql doesn't give us a portable RETURNING,
// so we use a two-step (SELECT FOR UPDATE SKIP LOCKED → UPDATE) within a tx.
func (s *Store) Claim(ctx context.Context, p tickr.ClaimParams) ([]*tickr.InboundMessage, error) {
	if p.Batch <= 0 {
		p.Batch = 100
	}
	if len(p.EventTypes) == 0 {
		return nil, nil
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("tickr/mysql: begin claim tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	placeholders := strings.Repeat("?,", len(p.EventTypes))
	placeholders = placeholders[:len(placeholders)-1]
	args := make([]any, 0, len(p.EventTypes)+2)
	for _, et := range p.EventTypes {
		args = append(args, et)
	}
	args = append(args, time.Now(), p.Batch)

	selectSQL := fmt.Sprintf(`
        SELECT id FROM tickr_messages
        WHERE status IN ('CREATED','RETRYING')
          AND event_type IN (%s)
          AND process_at <= ?
        ORDER BY process_at, id
        LIMIT ?
        FOR UPDATE SKIP LOCKED`, placeholders)

	rows, err := tx.QueryContext(ctx, selectSQL, args...)
	if err != nil {
		return nil, fmt.Errorf("tickr/mysql: claim select: %w", err)
	}
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return nil, err
		}
		ids = append(ids, id)
	}
	rows.Close()
	if len(ids) == 0 {
		return nil, tx.Commit()
	}

	until := time.Now().Add(p.Lease)
	updatePlaceholders := strings.Repeat("?,", len(ids))
	updatePlaceholders = updatePlaceholders[:len(updatePlaceholders)-1]
	updateArgs := make([]any, 0, len(ids)+2)
	updateArgs = append(updateArgs, p.WorkerID, until)
	for _, id := range ids {
		updateArgs = append(updateArgs, id)
	}
	updateSQL := fmt.Sprintf(`
        UPDATE tickr_messages
        SET status = 'HANDLING',
            attempt = attempt + 1,
            claimed_by = ?,
            claimed_until = ?,
            updated_at = CURRENT_TIMESTAMP(6)
        WHERE id IN (%s)`, updatePlaceholders)
	if _, err := tx.ExecContext(ctx, updateSQL, updateArgs...); err != nil {
		return nil, fmt.Errorf("tickr/mysql: claim update: %w", err)
	}

	fetchSQL := fmt.Sprintf(`SELECT %s FROM tickr_messages WHERE id IN (%s)`,
		messageReturnCols, updatePlaceholders)
	fetchArgs := make([]any, len(ids))
	for i, id := range ids {
		fetchArgs[i] = id
	}
	fetchRows, err := tx.QueryContext(ctx, fetchSQL, fetchArgs...)
	if err != nil {
		return nil, fmt.Errorf("tickr/mysql: claim fetch: %w", err)
	}
	defer fetchRows.Close()
	var out []*tickr.InboundMessage
	for fetchRows.Next() {
		msg, err := scanMessage(fetchRows)
		if err != nil {
			return nil, err
		}
		out = append(out, msg)
		appendHistory(ctx, tx, msg.ID, "HANDLING", "HANDLING", msg.Attempt, "", p.WorkerID)
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("tickr/mysql: claim commit: %w", err)
	}
	return out, nil
}

func appendHistory(ctx context.Context, ex execer, id tickr.MessageID, from, to string, attempt int, errStr, workerID string) {
	const sqlText = `
        INSERT INTO tickr_history (message_id, seq, from_status, to_status, attempt, error, worker_id)
        SELECT ?, COALESCE(MAX(seq), 0) + 1, ?, ?, ?, NULLIF(?, ''), ?
        FROM tickr_history WHERE message_id = ?`
	_, _ = ex.ExecContext(ctx, sqlText, string(id), from, to, attempt, errStr, workerID, string(id))
}

// --- Succeed / Fail / Release / Extend --------------------------------------

func (s *Store) Succeed(ctx context.Context, id tickr.MessageID, attempt int, workerID string) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("tickr/mysql: succeed begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(ctx, `
        UPDATE tickr_messages
        SET status='SUCCESS', completed_at=CURRENT_TIMESTAMP(6), updated_at=CURRENT_TIMESTAMP(6)
        WHERE id=? AND status='HANDLING' AND claimed_by=?`,
		string(id), workerID); err != nil {
		return fmt.Errorf("tickr/mysql: succeed update: %w", err)
	}
	appendHistory(ctx, tx, id, "HANDLING", "SUCCESS", attempt, "", workerID)
	return tx.Commit()
}

func (s *Store) Fail(ctx context.Context, p tickr.FailParams) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("tickr/mysql: fail begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	if p.Dead {
		if _, err := tx.ExecContext(ctx, `
            UPDATE tickr_messages
            SET status='DEAD', completed_at=CURRENT_TIMESTAMP(6),
                last_error=?, updated_at=CURRENT_TIMESTAMP(6)
            WHERE id=? AND status='HANDLING' AND claimed_by=?`,
			p.Err, string(p.MessageID), p.WorkerID); err != nil {
			return fmt.Errorf("tickr/mysql: fail->dead: %w", err)
		}
		appendHistory(ctx, tx, p.MessageID, "HANDLING", "DEAD", p.Attempt, p.Err, p.WorkerID)
	} else {
		if _, err := tx.ExecContext(ctx, `
            UPDATE tickr_messages
            SET status='RETRYING',
                process_at=?,
                last_error=?,
                claimed_by=NULL, claimed_until=NULL,
                updated_at=CURRENT_TIMESTAMP(6)
            WHERE id=? AND status='HANDLING' AND claimed_by=?`,
			p.NextRetryAt, p.Err, string(p.MessageID), p.WorkerID); err != nil {
			return fmt.Errorf("tickr/mysql: fail->retry: %w", err)
		}
		appendHistory(ctx, tx, p.MessageID, "HANDLING", "RETRYING", p.Attempt, p.Err, p.WorkerID)
	}
	return tx.Commit()
}

func (s *Store) ReleaseShutdown(ctx context.Context, id tickr.MessageID, attempt int, workerID, reason string) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(ctx, `
        UPDATE tickr_messages
        SET status = IF(attempt > 1, 'RETRYING', 'CREATED'),
            attempt = GREATEST(attempt - 1, 0),
            claimed_by = NULL, claimed_until = NULL,
            updated_at = CURRENT_TIMESTAMP(6)
        WHERE id = ? AND status = 'HANDLING' AND claimed_by = ?`,
		string(id), workerID); err != nil {
		return fmt.Errorf("tickr/mysql: release-shutdown: %w", err)
	}
	appendHistory(ctx, tx, id, "HANDLING", "RETRYING", attempt, reason, workerID)
	return tx.Commit()
}

func (s *Store) Extend(ctx context.Context, id tickr.MessageID, workerID string, until time.Time) (bool, error) {
	res, err := s.db.ExecContext(ctx, `
        UPDATE tickr_messages
        SET claimed_until=?, updated_at=CURRENT_TIMESTAMP(6)
        WHERE id=? AND status='HANDLING' AND claimed_by=?`,
		until, string(id), workerID)
	if err != nil {
		return false, fmt.Errorf("tickr/mysql: extend: %w", err)
	}
	n, _ := res.RowsAffected()
	return n > 0, nil
}

// --- History / DLQ / Requeue ------------------------------------------------

func (s *Store) History(ctx context.Context, id tickr.MessageID) ([]tickr.Transition, error) {
	rows, err := s.db.QueryContext(ctx, `
        SELECT message_id, seq, from_status, to_status, attempt, error, worker_id, at
        FROM tickr_history WHERE message_id=? ORDER BY seq`, string(id))
	if err != nil {
		return nil, fmt.Errorf("tickr/mysql: history: %w", err)
	}
	defer rows.Close()
	var out []tickr.Transition
	for rows.Next() {
		var (
			t        tickr.Transition
			mid      string
			from, to sql.NullString
			errStr   sql.NullString
			worker   sql.NullString
		)
		if err := rows.Scan(&mid, &t.Seq, &from, &to, &t.Attempt, &errStr, &worker, &t.At); err != nil {
			return nil, err
		}
		t.MessageID = tickr.MessageID(mid)
		if from.Valid {
			t.From = tickr.Status(from.String)
		}
		if to.Valid {
			t.To = tickr.Status(to.String)
		}
		if errStr.Valid {
			t.Error = errStr.String
		}
		if worker.Valid {
			t.WorkerID = worker.String
		}
		out = append(out, t)
	}
	return out, rows.Err()
}

func (s *Store) ListDead(ctx context.Context, eventType string, after time.Time, limit int) ([]*tickr.InboundMessage, error) {
	if limit <= 0 {
		limit = 50
	}
	var (
		rows *sql.Rows
		err  error
	)
	if eventType == "" {
		rows, err = s.db.QueryContext(ctx, fmt.Sprintf(`
            SELECT %s FROM tickr_messages
            WHERE status='DEAD' AND completed_at >= ?
            ORDER BY completed_at DESC LIMIT ?`, messageReturnCols),
			after, limit)
	} else {
		rows, err = s.db.QueryContext(ctx, fmt.Sprintf(`
            SELECT %s FROM tickr_messages
            WHERE status='DEAD' AND event_type=? AND completed_at >= ?
            ORDER BY completed_at DESC LIMIT ?`, messageReturnCols),
			eventType, after, limit)
	}
	if err != nil {
		return nil, fmt.Errorf("tickr/mysql: list dead: %w", err)
	}
	defer rows.Close()
	var out []*tickr.InboundMessage
	for rows.Next() {
		msg, err := scanMessage(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, msg)
	}
	return out, rows.Err()
}

func (s *Store) Requeue(ctx context.Context, id tickr.MessageID, processAt time.Time) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(ctx, `
        UPDATE tickr_messages
        SET status='CREATED', attempt=0,
            claimed_by=NULL, claimed_until=NULL, completed_at=NULL,
            process_at=?, last_error=NULL,
            updated_at=CURRENT_TIMESTAMP(6)
        WHERE id=? AND status='DEAD'`, processAt, string(id)); err != nil {
		return fmt.Errorf("tickr/mysql: requeue: %w", err)
	}
	appendHistory(ctx, tx, id, "DEAD", "CREATED", 0, "admin requeue", "")
	return tx.Commit()
}

// --- Reclaimer / Purger / Stats ---------------------------------------------

func (s *Store) ReclaimExpired(ctx context.Context, limit int) (int64, error) {
	if limit <= 0 {
		limit = 500
	}
	res, err := s.db.ExecContext(ctx, `
        UPDATE tickr_messages
        SET status='RETRYING',
            process_at=CURRENT_TIMESTAMP(6),
            claimed_by=NULL, claimed_until=NULL,
            last_error=COALESCE(last_error, 'lease expired'),
            updated_at=CURRENT_TIMESTAMP(6)
        WHERE status='HANDLING' AND claimed_until < CURRENT_TIMESTAMP(6)
        LIMIT ?`, limit)
	if err != nil {
		return 0, fmt.Errorf("tickr/mysql: reclaim: %w", err)
	}
	n, _ := res.RowsAffected()
	return n, nil
}

func (s *Store) PurgeTerminal(ctx context.Context, before time.Time, limit int) (int64, error) {
	if limit <= 0 {
		limit = 5000
	}
	res, err := s.db.ExecContext(ctx, `
        DELETE FROM tickr_messages
        WHERE status IN ('SUCCESS','DEAD')
          AND completed_at < ?
        LIMIT ?`, before, limit)
	if err != nil {
		return 0, fmt.Errorf("tickr/mysql: purge: %w", err)
	}
	n, _ := res.RowsAffected()
	return n, nil
}

func (s *Store) Stats(ctx context.Context) (tickr.Stats, error) {
	rows, err := s.db.QueryContext(ctx, `
        SELECT event_type, status, COUNT(*)
        FROM tickr_messages GROUP BY event_type, status`)
	if err != nil {
		return tickr.Stats{}, fmt.Errorf("tickr/mysql: stats: %w", err)
	}
	defer rows.Close()
	out := tickr.Stats{
		ByStatus:    map[tickr.Status]int{},
		ByEventType: map[string]map[tickr.Status]int{},
		SampledAt:   time.Now(),
	}
	for rows.Next() {
		var (
			et, st string
			n      int
		)
		if err := rows.Scan(&et, &st, &n); err != nil {
			return tickr.Stats{}, err
		}
		s := tickr.Status(st)
		out.ByStatus[s] += n
		if out.ByEventType[et] == nil {
			out.ByEventType[et] = map[tickr.Status]int{}
		}
		out.ByEventType[et][s] = n
	}
	return out, rows.Err()
}

// --- Migrations & Leader Lock ----------------------------------------------

func (s *Store) ApplyMigrations(ctx context.Context) error {
	entries, err := fs.ReadDir(migrationsFS, "migrations")
	if err != nil {
		return fmt.Errorf("tickr/mysql: read migrations: %w", err)
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Name() < entries[j].Name() })

	if _, err := s.db.ExecContext(ctx, `
        CREATE TABLE IF NOT EXISTS tickr_schema_version (
            version    INT NOT NULL PRIMARY KEY,
            applied_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`); err != nil {
		return fmt.Errorf("tickr/mysql: bootstrap version table: %w", err)
	}

	applied := map[int]bool{}
	rows, err := s.db.QueryContext(ctx, `SELECT version FROM tickr_schema_version`)
	if err != nil {
		return err
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
		if version == 0 || applied[version] {
			continue
		}
		b, err := fs.ReadFile(migrationsFS, "migrations/"+name)
		if err != nil {
			return fmt.Errorf("tickr/mysql: read %s: %w", name, err)
		}
		// MySQL doesn't allow multiple statements in one Exec by default —
		// split on ; (naive, but our migrations are simple DDL).
		for _, stmt := range splitSQL(string(b)) {
			if strings.TrimSpace(stmt) == "" {
				continue
			}
			if _, err := s.db.ExecContext(ctx, stmt); err != nil {
				return fmt.Errorf("tickr/mysql: apply %s: %w", name, err)
			}
		}
		if _, err := s.db.ExecContext(ctx,
			`INSERT IGNORE INTO tickr_schema_version (version) VALUES (?)`, version); err != nil {
			return fmt.Errorf("tickr/mysql: record %s: %w", name, err)
		}
	}
	return nil
}

func splitSQL(s string) []string {
	out := strings.Split(s, ";")
	for i, stmt := range out {
		out[i] = strings.TrimSpace(stmt)
	}
	return out
}

// TryLeaderLock uses GET_LOCK. The lock is session-scoped, so we hold a
// dedicated connection until unlock is called.
func (s *Store) TryLeaderLock(ctx context.Context, key string) (bool, func(), error) {
	conn, err := s.db.Conn(ctx)
	if err != nil {
		return false, nil, fmt.Errorf("tickr/mysql: acquire conn: %w", err)
	}
	var got sql.NullInt64
	if err := conn.QueryRowContext(ctx, `SELECT GET_LOCK(?, 0)`, "tickr:"+key).Scan(&got); err != nil {
		_ = conn.Close()
		return false, nil, fmt.Errorf("tickr/mysql: get_lock: %w", err)
	}
	if !got.Valid || got.Int64 != 1 {
		_ = conn.Close()
		return false, nil, nil
	}
	unlock := func() {
		_, _ = conn.ExecContext(context.Background(), `SELECT RELEASE_LOCK(?)`, "tickr:"+key)
		_ = conn.Close()
	}
	return true, unlock, nil
}

var _ tickr.Storage = (*Store)(nil)

// errAdapter satisfies errors.As checks where appropriate.
var _ error = (*tickr.ErrDuplicate)(nil)

func wrapErr(prefix string, err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, sql.ErrNoRows) {
		return err
	}
	return fmt.Errorf("%s: %w", prefix, err)
}

var _ = wrapErr // keep helper available
