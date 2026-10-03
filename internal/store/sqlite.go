package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	_ "modernc.org/sqlite" // pure-Go SQLite driver

	"github.com/Sankartk/dlq-triage/internal/domain"
)

// SQLite is a Store backed by a single SQLite database file.
type SQLite struct {
	db *sql.DB
}

var migrations = []string{
	`CREATE TABLE groups (
		queue TEXT NOT NULL,
		key TEXT NOT NULL,
		error_sig TEXT NOT NULL,
		shape TEXT NOT NULL,
		first_seen TEXT NOT NULL,
		last_seen TEXT NOT NULL,
		ai_summary TEXT NOT NULL DEFAULT '',
		ai_summary_at TEXT,
		PRIMARY KEY (queue, key)
	)`,
	`CREATE TABLE messages (
		queue TEXT NOT NULL,
		id TEXT NOT NULL,
		group_key TEXT NOT NULL,
		body TEXT NOT NULL,
		attrs_json TEXT NOT NULL,
		group_id TEXT NOT NULL DEFAULT '',
		receive_count INTEGER NOT NULL,
		sent_at TEXT,
		ingested_at TEXT NOT NULL,
		status TEXT NOT NULL DEFAULT 'pending',
		replayed_at TEXT,
		PRIMARY KEY (queue, id)
	)`,
	`CREATE INDEX idx_messages_group ON messages (queue, group_key, status, ingested_at)`,
	`CREATE TABLE replay_jobs (
		id TEXT PRIMARY KEY,
		queue TEXT NOT NULL,
		group_key TEXT NOT NULL,
		dry_run INTEGER NOT NULL,
		status TEXT NOT NULL,
		requested_by TEXT NOT NULL,
		rate_per_sec REAL NOT NULL,
		max_messages INTEGER NOT NULL,
		total INTEGER NOT NULL DEFAULT 0,
		replayed INTEGER NOT NULL DEFAULT 0,
		skipped INTEGER NOT NULL DEFAULT 0,
		failed INTEGER NOT NULL DEFAULT 0,
		error TEXT NOT NULL DEFAULT '',
		created_at TEXT NOT NULL,
		finished_at TEXT
	)`,
	`CREATE INDEX idx_jobs_queue ON replay_jobs (queue, created_at)`,
	`CREATE TABLE audit (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		at TEXT NOT NULL,
		actor TEXT NOT NULL,
		action TEXT NOT NULL,
		queue TEXT NOT NULL,
		group_key TEXT NOT NULL DEFAULT '',
		job_id TEXT NOT NULL DEFAULT '',
		detail TEXT NOT NULL DEFAULT ''
	)`,
	`CREATE INDEX idx_audit_queue ON audit (queue, id)`,
}

// OpenSQLite opens (and migrates) a database. Use ":memory:" in tests, which
// is pinned to one connection because each connection gets its own database.
func OpenSQLite(path string) (*SQLite, error) {
	dsn := path
	if path == ":memory:" {
		dsn = "file::memory:?_pragma=foreign_keys(1)"
	} else {
		dsn = "file:" + path + "?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)&_pragma=foreign_keys(1)"
	}
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	// One connection: SQLite allows a single writer, and ":memory:" databases
	// are per-connection.
	db.SetMaxOpenConns(1)
	s := &SQLite{db: db}
	if err := s.migrate(context.Background()); err != nil {
		db.Close()
		return nil, err
	}
	return s, nil
}

func (s *SQLite) migrate(ctx context.Context) error {
	if _, err := s.db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS schema_version (version INTEGER NOT NULL)`); err != nil {
		return err
	}
	var current int
	err := s.db.QueryRowContext(ctx, `SELECT COALESCE(MAX(version), 0) FROM schema_version`).Scan(&current)
	if err != nil {
		return err
	}
	for i := current; i < len(migrations); i++ {
		tx, err := s.db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, migrations[i]); err != nil {
			tx.Rollback()
			return fmt.Errorf("migration %d: %w", i+1, err)
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO schema_version (version) VALUES (?)`, i+1); err != nil {
			tx.Rollback()
			return err
		}
		if err := tx.Commit(); err != nil {
			return err
		}
	}
	return nil
}

func (s *SQLite) Close() error { return s.db.Close() }

func (s *SQLite) Ping(ctx context.Context) error { return s.db.PingContext(ctx) }

const timeFmt = time.RFC3339Nano

func ts(t time.Time) string { return t.UTC().Format(timeFmt) }

func parseTS(s string) time.Time {
	t, _ := time.Parse(timeFmt, s)
	return t
}

func parseOptTS(ns sql.NullString) *time.Time {
	if !ns.Valid || ns.String == "" {
		return nil
	}
	t := parseTS(ns.String)
	return &t
}

func (s *SQLite) UpsertMessage(ctx context.Context, m domain.StoredMessage, errorSig, shape string) (bool, error) {
	attrs, err := json.Marshal(m.Attributes)
	if err != nil {
		return false, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()

	now := ts(m.IngestedAt)
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO groups (queue, key, error_sig, shape, first_seen, last_seen)
		VALUES (?, ?, ?, ?, ?, ?)
		ON CONFLICT (queue, key) DO UPDATE SET last_seen = excluded.last_seen`,
		m.Queue, m.GroupKey, errorSig, shape, now, now); err != nil {
		return false, err
	}

	var sentAt any
	if !m.SentAt.IsZero() {
		sentAt = ts(m.SentAt)
	}
	var existing int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM messages WHERE queue = ? AND id = ?`, m.Queue, m.ID).Scan(&existing); err != nil {
		return false, err
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO messages (queue, id, group_key, body, attrs_json, group_id, receive_count, sent_at, ingested_at, status)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending')
		ON CONFLICT (queue, id) DO UPDATE SET receive_count = excluded.receive_count`,
		m.Queue, m.ID, m.GroupKey, m.Body, string(attrs), m.GroupID, m.ReceiveCount, sentAt, now); err != nil {
		return false, err
	}
	if err := tx.Commit(); err != nil {
		return false, err
	}
	return existing == 0, nil
}

const groupSelect = `
	SELECT g.queue, g.key, g.error_sig, g.shape, g.first_seen, g.last_seen, g.ai_summary, g.ai_summary_at,
	       (SELECT COUNT(*) FROM messages m WHERE m.queue = g.queue AND m.group_key = g.key) AS total,
	       (SELECT COUNT(*) FROM messages m WHERE m.queue = g.queue AND m.group_key = g.key AND m.status = 'pending') AS pending,
	       COALESCE((SELECT m.id FROM messages m WHERE m.queue = g.queue AND m.group_key = g.key ORDER BY m.ingested_at, m.id LIMIT 1), '') AS sample
	FROM groups g`

func scanGroup(sc interface{ Scan(...any) error }) (domain.Group, error) {
	var g domain.Group
	var first, last string
	var aiAt sql.NullString
	if err := sc.Scan(&g.Queue, &g.Key, &g.ErrorSig, &g.Shape, &first, &last, &g.AISummary, &aiAt, &g.Count, &g.PendingCount, &g.SampleMessage); err != nil {
		return g, err
	}
	g.FirstSeen, g.LastSeen = parseTS(first), parseTS(last)
	g.AISummaryAt = parseOptTS(aiAt)
	return g, nil
}

func (s *SQLite) ListGroups(ctx context.Context, queue string) ([]domain.Group, error) {
	rows, err := s.db.QueryContext(ctx, groupSelect+` WHERE g.queue = ? ORDER BY pending DESC, g.last_seen DESC, g.key`, queue)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []domain.Group
	for rows.Next() {
		g, err := scanGroup(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, g)
	}
	return out, rows.Err()
}

func (s *SQLite) GetGroup(ctx context.Context, queue, key string) (domain.Group, error) {
	g, err := scanGroup(s.db.QueryRowContext(ctx, groupSelect+` WHERE g.queue = ? AND g.key = ?`, queue, key))
	if errors.Is(err, sql.ErrNoRows) {
		return g, ErrNotFound
	}
	return g, err
}

func (s *SQLite) ListMessages(ctx context.Context, queue, groupKey string, pendingOnly bool, limit, offset int) ([]domain.StoredMessage, error) {
	q := `SELECT queue, id, group_key, body, attrs_json, group_id, receive_count, sent_at, ingested_at, status, replayed_at
	      FROM messages WHERE queue = ? AND group_key = ?`
	args := []any{queue, groupKey}
	if pendingOnly {
		q += ` AND status = 'pending'`
	}
	q += ` ORDER BY ingested_at, id LIMIT ? OFFSET ?`
	args = append(args, limit, offset)
	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []domain.StoredMessage
	for rows.Next() {
		var m domain.StoredMessage
		var attrs, ingested, status string
		var sent, replayed sql.NullString
		if err := rows.Scan(&m.Queue, &m.ID, &m.GroupKey, &m.Body, &attrs, &m.GroupID, &m.ReceiveCount, &sent, &ingested, &status, &replayed); err != nil {
			return nil, err
		}
		_ = json.Unmarshal([]byte(attrs), &m.Attributes)
		if t := parseOptTS(sent); t != nil {
			m.SentAt = *t
		}
		m.IngestedAt = parseTS(ingested)
		m.Status = domain.MessageStatus(status)
		m.ReplayedAt = parseOptTS(replayed)
		out = append(out, m)
	}
	return out, rows.Err()
}

func (s *SQLite) PendingMessageIDs(ctx context.Context, queue, groupKey string, limit int) ([]string, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT id FROM messages WHERE queue = ? AND group_key = ? AND status = 'pending' ORDER BY ingested_at, id LIMIT ?`,
		queue, groupKey, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

func (s *SQLite) MessageStatus(ctx context.Context, queue, id string) (domain.MessageStatus, error) {
	var st string
	err := s.db.QueryRowContext(ctx, `SELECT status FROM messages WHERE queue = ? AND id = ?`, queue, id).Scan(&st)
	if errors.Is(err, sql.ErrNoRows) {
		return "", ErrNotFound
	}
	return domain.MessageStatus(st), err
}

func (s *SQLite) MarkReplayed(ctx context.Context, queue, id string, at time.Time) error {
	res, err := s.db.ExecContext(ctx, `UPDATE messages SET status = 'replayed', replayed_at = ? WHERE queue = ? AND id = ?`, ts(at), queue, id)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return ErrNotFound
	}
	return nil
}

func (s *SQLite) SetAISummary(ctx context.Context, queue, key, summary string, at time.Time) error {
	res, err := s.db.ExecContext(ctx, `UPDATE groups SET ai_summary = ?, ai_summary_at = ? WHERE queue = ? AND key = ?`, summary, ts(at), queue, key)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return ErrNotFound
	}
	return nil
}

func (s *SQLite) CreateJob(ctx context.Context, j domain.ReplayJob) error {
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO replay_jobs (id, queue, group_key, dry_run, status, requested_by, rate_per_sec, max_messages,
		                         total, replayed, skipped, failed, error, created_at, finished_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		j.ID, j.Queue, j.GroupKey, boolInt(j.DryRun), string(j.Status), j.RequestedBy, j.RatePerSec, j.MaxMessages,
		j.Total, j.Replayed, j.Skipped, j.Failed, j.Error, ts(j.CreatedAt), optTS(j.FinishedAt))
	return err
}

func (s *SQLite) UpdateJob(ctx context.Context, j domain.ReplayJob) error {
	res, err := s.db.ExecContext(ctx, `
		UPDATE replay_jobs SET status = ?, total = ?, replayed = ?, skipped = ?, failed = ?, error = ?, finished_at = ?
		WHERE id = ?`,
		string(j.Status), j.Total, j.Replayed, j.Skipped, j.Failed, j.Error, optTS(j.FinishedAt), j.ID)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return ErrNotFound
	}
	return nil
}

const jobCols = `id, queue, group_key, dry_run, status, requested_by, rate_per_sec, max_messages,
	total, replayed, skipped, failed, error, created_at, finished_at`

func scanJob(sc interface{ Scan(...any) error }) (domain.ReplayJob, error) {
	var j domain.ReplayJob
	var dry int
	var status, created string
	var finished sql.NullString
	if err := sc.Scan(&j.ID, &j.Queue, &j.GroupKey, &dry, &status, &j.RequestedBy, &j.RatePerSec, &j.MaxMessages,
		&j.Total, &j.Replayed, &j.Skipped, &j.Failed, &j.Error, &created, &finished); err != nil {
		return j, err
	}
	j.DryRun = dry != 0
	j.Status = domain.JobStatus(status)
	j.CreatedAt = parseTS(created)
	j.FinishedAt = parseOptTS(finished)
	return j, nil
}

func (s *SQLite) GetJob(ctx context.Context, id string) (domain.ReplayJob, error) {
	j, err := scanJob(s.db.QueryRowContext(ctx, `SELECT `+jobCols+` FROM replay_jobs WHERE id = ?`, id))
	if errors.Is(err, sql.ErrNoRows) {
		return j, ErrNotFound
	}
	return j, err
}

func (s *SQLite) ListJobs(ctx context.Context, queue string, limit int) ([]domain.ReplayJob, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT `+jobCols+` FROM replay_jobs WHERE queue = ? ORDER BY created_at DESC, id LIMIT ?`, queue, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []domain.ReplayJob
	for rows.Next() {
		j, err := scanJob(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, j)
	}
	return out, rows.Err()
}

func (s *SQLite) ActiveJob(ctx context.Context, queue, groupKey string) (domain.ReplayJob, error) {
	j, err := scanJob(s.db.QueryRowContext(ctx,
		`SELECT `+jobCols+` FROM replay_jobs WHERE queue = ? AND group_key = ? AND status = 'running' AND dry_run = 0 LIMIT 1`, queue, groupKey))
	if errors.Is(err, sql.ErrNoRows) {
		return j, ErrNotFound
	}
	return j, err
}

func (s *SQLite) FailRunningJobs(ctx context.Context, reason string, at time.Time) (int, error) {
	res, err := s.db.ExecContext(ctx,
		`UPDATE replay_jobs SET status = 'failed', error = ?, finished_at = ? WHERE status = 'running'`, reason, ts(at))
	if err != nil {
		return 0, err
	}
	n, _ := res.RowsAffected()
	return int(n), nil
}

func (s *SQLite) AppendAudit(ctx context.Context, e domain.AuditEntry) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO audit (at, actor, action, queue, group_key, job_id, detail) VALUES (?, ?, ?, ?, ?, ?, ?)`,
		ts(e.At), e.Actor, e.Action, e.Queue, e.GroupKey, e.JobID, e.Detail)
	return err
}

func (s *SQLite) ListAudit(ctx context.Context, queue string, limit int) ([]domain.AuditEntry, error) {
	q := `SELECT id, at, actor, action, queue, group_key, job_id, detail FROM audit`
	var args []any
	if strings.TrimSpace(queue) != "" {
		q += ` WHERE queue = ?`
		args = append(args, queue)
	}
	q += ` ORDER BY id DESC LIMIT ?`
	args = append(args, limit)
	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []domain.AuditEntry
	for rows.Next() {
		var e domain.AuditEntry
		var at string
		if err := rows.Scan(&e.ID, &at, &e.Actor, &e.Action, &e.Queue, &e.GroupKey, &e.JobID, &e.Detail); err != nil {
			return nil, err
		}
		e.At = parseTS(at)
		out = append(out, e)
	}
	return out, rows.Err()
}

func boolInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

func optTS(t *time.Time) any {
	if t == nil {
		return nil
	}
	return ts(*t)
}
