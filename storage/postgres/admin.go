package postgres

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/ndmt1at21/tickr"
)

// Get implements tickr.Admin.
func (s *Store) Get(ctx context.Context, id tickr.MessageID) (*tickr.InboundMessage, error) {
	mid, err := uuid.Parse(string(id))
	if err != nil {
		return nil, fmt.Errorf("tickr/postgres: get: bad id: %w", err)
	}
	row := s.pool.QueryRow(ctx,
		`SELECT `+messageReturnCols+` FROM tickr_messages WHERE id = $1`, mid)
	msg, err := scanMessage(row)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("tickr/postgres: get: %w", err)
	}
	return msg, nil
}

// List implements tickr.Admin.
func (s *Store) List(ctx context.Context, q tickr.ListQuery) ([]*tickr.InboundMessage, error) {
	limit := q.Limit
	if limit <= 0 {
		limit = 50
	}
	if limit > 500 {
		limit = 500
	}

	var (
		conds []string
		args  []any
	)
	add := func(cond string, val any) {
		args = append(args, val)
		conds = append(conds, fmt.Sprintf(cond, len(args)))
	}
	if q.Status != "" {
		add("status = $%d", string(q.Status))
	}
	if q.EventType != "" {
		add("event_type = $%d", q.EventType)
	}
	if q.AfterID != "" {
		afterID, err := uuid.Parse(string(q.AfterID))
		if err != nil {
			return nil, fmt.Errorf("tickr/postgres: list: bad cursor: %w", err)
		}
		add("id < $%d", afterID)
	}
	if !q.Since.IsZero() {
		add("created_at >= $%d", q.Since)
	}
	if q.Search != "" {
		// Case-insensitive substring on payload (treated as text) and the
		// idempotency key. Payloads are bytea — convert_from is safe for
		// UTF-8 text; binary payloads will simply not match, which is fine
		// for an admin search UI.
		add("(idempotency_key ILIKE $%d", "%"+q.Search+"%")
		conds[len(conds)-1] += fmt.Sprintf(" OR convert_from(payload, 'UTF8') ILIKE $%d)", len(args))
		args = append(args, "%"+q.Search+"%")
	}

	where := ""
	if len(conds) > 0 {
		where = "WHERE " + strings.Join(conds, " AND ")
	}
	args = append(args, limit)
	sql := fmt.Sprintf(
		`SELECT %s FROM tickr_messages %s ORDER BY id DESC LIMIT $%d`,
		messageReturnCols, where, len(args))

	rows, err := s.pool.Query(ctx, sql, args...)
	if err != nil {
		return nil, fmt.Errorf("tickr/postgres: list: %w", err)
	}
	defer rows.Close()

	out := make([]*tickr.InboundMessage, 0, limit)
	for rows.Next() {
		m, err := scanMessage(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, m)
	}
	return out, rows.Err()
}

const killSQL = `
UPDATE tickr_messages
SET    status        = 'DEAD',
       claimed_until = NULL,
       claimed_by    = NULL,
       last_error    = COALESCE(last_error, '') ||
                       CASE WHEN last_error IS NULL OR last_error = '' THEN '' ELSE ' | ' END ||
                       'killed: ' || $2,
       completed_at  = now(),
       updated_at    = now()
WHERE  id = $1
  AND  status NOT IN ('SUCCESS','DEAD')
RETURNING status::text, attempt`

// Kill implements tickr.Admin.
func (s *Store) Kill(ctx context.Context, id tickr.MessageID, reason string) error {
	mid, err := uuid.Parse(string(id))
	if err != nil {
		return fmt.Errorf("tickr/postgres: kill: bad id: %w", err)
	}
	if reason == "" {
		reason = "admin kill"
	}
	var (
		prevStatus string
		attempt    int
	)
	if err := s.pool.QueryRow(ctx, killSQL, mid, reason).
		Scan(&prevStatus, &attempt); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			// Either not found or already terminal — both are no-ops.
			return nil
		}
		return fmt.Errorf("tickr/postgres: kill: %w", err)
	}
	_ = prevStatus
	// Best-effort history row.
	if err := s.appendHistory(ctx, id, tickr.Status(prevStatus), tickr.StatusDead, attempt, "killed: "+reason, ""); err != nil {
		return fmt.Errorf("tickr/postgres: kill history: %w", err)
	}
	return nil
}

// compile-time check: the postgres adapter satisfies tickr.Admin.
var _ tickr.Admin = (*Store)(nil)
