// Package store is the memory layer: one SQLite row per resolved issue,
// written from an agent's response. The dashboard reads the same DB.
package store

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/Parag953/perf-agent/internal/model"

	_ "modernc.org/sqlite" // pure-Go driver, no cgo (good for the aarch64 host)
)

const schema = `
CREATE TABLE IF NOT EXISTS memory (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    signature     TEXT NOT NULL,
    service       TEXT NOT NULL,
    symptom       TEXT,
    root_cause    TEXT,
    fix_summary   TEXT,
    pr_url        TEXT,
    full_analysis TEXT,
    created_at    TEXT NOT NULL,
    last_seen     TEXT NOT NULL,
    hit_count     INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_memory_service ON memory(service);
`

const tsLayout = time.RFC3339

type Store struct {
	db *sql.DB
}

// Open opens (or creates) the SQLite DB at path and ensures the schema exists.
func Open(path string) (*Store, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}
	db.SetMaxOpenConns(1) // sqlite: serialize writers, avoids "database is locked"
	if _, err := db.Exec(schema + taskSchema); err != nil {
		return nil, fmt.Errorf("init schema: %w", err)
	}
	return &Store{db: db}, nil
}

func (s *Store) Close() error { return s.db.Close() }

// Insert writes a new memory row and returns its id. created_at/last_seen are
// set to now; hit_count starts at 0.
func (s *Store) Insert(m model.Memory) (int64, error) {
	now := time.Now().UTC().Format(tsLayout)
	res, err := s.db.Exec(
		`INSERT INTO memory
		   (signature, service, symptom, root_cause, fix_summary, pr_url, full_analysis, created_at, last_seen, hit_count)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 0)`,
		m.Signature, m.Service, m.Symptom, m.RootCause, m.FixSummary, m.PRURL, m.FullAnalysis, now, now,
	)
	if err != nil {
		return 0, fmt.Errorf("insert memory: %w", err)
	}
	return res.LastInsertId()
}

// BumpHit records another sighting of a known issue: hit_count++ and last_seen=now.
func (s *Store) BumpHit(id int64) error {
	now := time.Now().UTC().Format(tsLayout)
	_, err := s.db.Exec(`UPDATE memory SET hit_count = hit_count + 1, last_seen = ? WHERE id = ?`, now, id)
	return err
}

// CandidatesByService returns the prefiltered set the matcher hands to Haiku.
func (s *Store) CandidatesByService(service string) ([]model.Memory, error) {
	rows, err := s.db.Query(
		`SELECT id, signature, service, symptom, root_cause, fix_summary, pr_url, full_analysis, created_at, last_seen, hit_count
		   FROM memory WHERE service = ? ORDER BY last_seen DESC LIMIT 50`, service)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanRows(rows)
}

// Get returns one full memory row (for the drill-down endpoint).
func (s *Store) Get(id int64) (model.Memory, error) {
	row := s.db.QueryRow(
		`SELECT id, signature, service, symptom, root_cause, fix_summary, pr_url, full_analysis, created_at, last_seen, hit_count
		   FROM memory WHERE id = ?`, id)
	return scanRow(row)
}

// Summaries returns trimmed rows for the dashboard list, most-recently-seen first.
func (s *Store) Summaries(limit int) ([]model.MemorySummary, error) {
	if limit <= 0 {
		limit = 100
	}
	rows, err := s.db.Query(
		`SELECT id, signature, service, symptom, root_cause, fix_summary, pr_url, full_analysis, created_at, last_seen, hit_count
		   FROM memory ORDER BY last_seen DESC LIMIT ?`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	mems, err := scanRows(rows)
	if err != nil {
		return nil, err
	}
	out := make([]model.MemorySummary, 0, len(mems))
	for _, m := range mems {
		out = append(out, m.Summary())
	}
	return out, nil
}

type scanner interface {
	Scan(dest ...any) error
}

func scanRow(sc scanner) (model.Memory, error) {
	var (
		m                          model.Memory
		symptom, fix, pr, analysis sql.NullString
		created, seen              string
	)
	err := sc.Scan(&m.ID, &m.Signature, &m.Service, &symptom, &m.RootCause, &fix, &pr, &analysis, &created, &seen, &m.HitCount)
	if err != nil {
		return m, err
	}
	m.Symptom, m.FixSummary, m.PRURL, m.FullAnalysis = symptom.String, fix.String, pr.String, analysis.String
	m.CreatedAt, _ = time.Parse(tsLayout, created)
	m.LastSeen, _ = time.Parse(tsLayout, seen)
	return m, nil
}

func scanRows(rows *sql.Rows) ([]model.Memory, error) {
	var out []model.Memory
	for rows.Next() {
		m, err := scanRow(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, m)
	}
	return out, rows.Err()
}
