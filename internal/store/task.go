package store

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/Parag953/perf-agent/internal/model"
)

const taskSchema = `
CREATE TABLE IF NOT EXISTS task (
    id          TEXT PRIMARY KEY,
    service     TEXT NOT NULL,
    alert       TEXT,
    phase       TEXT NOT NULL,
    vm_id       TEXT,
    signature   TEXT,
    outcome     TEXT,
    pr_url      TEXT,
    memory_id   INTEGER,
    error       TEXT,
    payload     TEXT,
    analysis    TEXT,
    summary     TEXT,
    enqueued_at TEXT NOT NULL,
    started_at  TEXT,
    finished_at TEXT
);
CREATE INDEX IF NOT EXISTS idx_task_enqueued ON task(enqueued_at);

CREATE TABLE IF NOT EXISTS task_event (
    id      INTEGER PRIMARY KEY AUTOINCREMENT,
    task_id TEXT NOT NULL,
    seq     INTEGER NOT NULL,
    at      TEXT NOT NULL,
    kind    TEXT NOT NULL,
    tool    TEXT,
    text    TEXT
);
CREATE INDEX IF NOT EXISTS idx_event_task ON task_event(task_id, seq);
`

const evLayout = time.RFC3339Nano

func (s *Store) SaveTask(t model.Task) error {
	_, err := s.db.Exec(
		`INSERT INTO task
		   (id, service, alert, phase, vm_id, signature, outcome, pr_url, memory_id, error,
		    payload, analysis, summary, enqueued_at, started_at, finished_at)
		 VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
		 ON CONFLICT(id) DO UPDATE SET
		   service=excluded.service, alert=excluded.alert, phase=excluded.phase,
		   vm_id=excluded.vm_id, signature=excluded.signature, outcome=excluded.outcome,
		   pr_url=excluded.pr_url, memory_id=excluded.memory_id, error=excluded.error,
		   payload=excluded.payload, analysis=excluded.analysis, summary=excluded.summary,
		   enqueued_at=excluded.enqueued_at, started_at=excluded.started_at,
		   finished_at=excluded.finished_at`,
		t.ID, t.Service, t.Alert, string(t.Phase), t.VMID, t.Signature, string(t.Outcome),
		t.PRURL, t.MemoryID, t.Error, string(t.Payload), t.Analysis, t.Summary,
		t.EnqueuedAt.UTC().Format(evLayout), nullTime(t.StartedAt), nullTime(t.FinishedAt),
	)
	if err != nil {
		return fmt.Errorf("save task: %w", err)
	}
	return nil
}

func (s *Store) LoadTasks(limit int) ([]model.Task, error) {
	if limit <= 0 {
		limit = 200
	}
	rows, err := s.db.Query(
		`SELECT id, service, alert, phase, vm_id, signature, outcome, pr_url, memory_id, error,
		        payload, analysis, summary, enqueued_at, started_at, finished_at
		   FROM task ORDER BY enqueued_at DESC LIMIT ?`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var desc []model.Task
	for rows.Next() {
		var (
			t                                      model.Task
			alert, vmID, sig, outcome, pr, errText sql.NullString
			payload, analysis, summary             sql.NullString
			memID                                  sql.NullInt64
			enqueued                               string
			started, finished                      sql.NullString
		)
		if err := rows.Scan(&t.ID, &t.Service, &alert, &t.Phase, &vmID, &sig, &outcome, &pr,
			&memID, &errText, &payload, &analysis, &summary, &enqueued, &started, &finished); err != nil {
			return nil, err
		}
		t.Alert, t.VMID, t.Signature = alert.String, vmID.String, sig.String
		t.Outcome, t.PRURL, t.Error = model.Outcome(outcome.String), pr.String, errText.String
		t.MemoryID = memID.Int64
		t.Analysis, t.Summary = analysis.String, summary.String
		if payload.String != "" {
			t.Payload = json.RawMessage(payload.String)
		}
		t.EnqueuedAt, _ = time.Parse(evLayout, enqueued)
		t.StartedAt = parseTime(started)
		t.FinishedAt = parseTime(finished)
		desc = append(desc, t)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	out := make([]model.Task, 0, len(desc))
	for i := len(desc) - 1; i >= 0; i-- {
		out = append(out, desc[i])
	}
	return out, nil
}

func (s *Store) AppendEvent(e model.AgentEvent) error {
	_, err := s.db.Exec(
		`INSERT INTO task_event (task_id, seq, at, kind, tool, text) VALUES (?,?,?,?,?,?)`,
		e.TaskID, e.Seq, e.At.UTC().Format(evLayout), string(e.Kind), e.Tool, e.Text)
	if err != nil {
		return fmt.Errorf("append event: %w", err)
	}
	return nil
}

func (s *Store) Events(taskID string) ([]model.AgentEvent, error) {
	rows, err := s.db.Query(
		`SELECT task_id, seq, at, kind, tool, text FROM task_event WHERE task_id = ? ORDER BY seq`, taskID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := []model.AgentEvent{}
	for rows.Next() {
		var (
			e         model.AgentEvent
			at        string
			tool, txt sql.NullString
		)
		if err := rows.Scan(&e.TaskID, &e.Seq, &at, &e.Kind, &tool, &txt); err != nil {
			return nil, err
		}
		e.At, _ = time.Parse(evLayout, at)
		e.Tool, e.Text = tool.String, txt.String
		out = append(out, e)
	}
	return out, rows.Err()
}

func nullTime(t *time.Time) any {
	if t == nil {
		return nil
	}
	return t.UTC().Format(evLayout)
}

func parseTime(s sql.NullString) *time.Time {
	if !s.Valid || s.String == "" {
		return nil
	}
	t, err := time.Parse(evLayout, s.String)
	if err != nil {
		return nil
	}
	return &t
}
