// Package model holds the shared types that flow between the orchestrator,
// the memory store, the poold client, and the dashboard API.
package model

import (
	"encoding/json"
	"time"
)

// Phase is where a task sits in its lifecycle. The dashboard renders these as
// status pills; see the design doc's "Task phases" section.
type Phase string

const (
	PhaseQueued     Phase = "queued"
	PhaseWaiting    Phase = "waiting"
	PhaseMatching   Phase = "matching"
	PhaseLeasing    Phase = "leasing"
	PhaseDispatched Phase = "dispatched"
	PhaseRunning    Phase = "running"
	PhaseCollecting Phase = "collecting"
	PhasePRCheck    Phase = "pr_check"
	PhaseDistilling Phase = "distilling"
	PhaseDone       Phase = "done"
	PhaseError      Phase = "error"
)

// Outcome is what a finished task resulted in.
type Outcome string

const (
	OutcomePROpened     Outcome = "pr_opened"
	OutcomeAnalysisOnly Outcome = "analysis_only"
	OutcomeMemoryHit    Outcome = "memory_hit"
	OutcomeNeedQueries  Outcome = "need_queries"
	OutcomeError        Outcome = "error"
)

// VMState is a pool VM's lifecycle state, owned by poold.
type VMState string

const (
	VMReady     VMState = "ready"     // clean & provisioned, leasable
	VMFree      VMState = "free"      // idle in pool, leasable
	VMAllocated VMState = "allocated" // leased, running its one task
	VMDegraded  VMState = "degraded"  // dirty after its task, awaiting recycle
)

// Trigger is what arrives on the webhook — an alert to investigate.
type Trigger struct {
	Service string          `json:"service"`
	Alert   string          `json:"alert"`
	Payload json.RawMessage `json:"payload,omitempty"`
}

// Task is one investigation as it moves through the orchestrator.
type Task struct {
	ID         string          `json:"task_id"`
	Service    string          `json:"service"`
	Alert      string          `json:"alert"`
	Phase      Phase           `json:"phase"`
	VMID       string          `json:"vm_id,omitempty"`
	Signature  string          `json:"signature,omitempty"`
	Outcome    Outcome         `json:"outcome,omitempty"`
	PRURL      string          `json:"pr_url,omitempty"`
	MemoryID   int64           `json:"memory_id,omitempty"`
	Error      string          `json:"error,omitempty"`
	EnqueuedAt time.Time       `json:"enqueued_at"`
	StartedAt  *time.Time      `json:"started_at,omitempty"`
	FinishedAt *time.Time      `json:"finished_at,omitempty"`
	QueuePos   int             `json:"queue_pos,omitempty"`
	Payload    json.RawMessage `json:"-"`
	Analysis   string          `json:"-"`
	Summary    string          `json:"-"`
}

type EventKind string

const (
	EventSystem     EventKind = "system"
	EventBootstrap  EventKind = "bootstrap"
	EventThinking   EventKind = "thinking"
	EventText       EventKind = "text"
	EventTool       EventKind = "tool"
	EventToolResult EventKind = "tool_result"
	EventResult     EventKind = "result"
	EventFailure    EventKind = "failure"
)

type AgentEvent struct {
	TaskID string    `json:"task_id"`
	Seq    int       `json:"seq"`
	At     time.Time `json:"at"`
	Kind   EventKind `json:"kind"`
	Tool   string    `json:"tool,omitempty"`
	Text   string    `json:"text,omitempty"`
}

type TaskDetail struct {
	Task
	Payload  json.RawMessage `json:"payload,omitempty"`
	Analysis string          `json:"analysis,omitempty"`
	Summary  string          `json:"summary,omitempty"`
	Events   []AgentEvent    `json:"events"`
}

// Clone returns a value copy safe to hand to the API layer.
func (t *Task) Clone() Task {
	c := *t
	c.Payload = nil
	c.Analysis = ""
	c.Summary = ""
	return c
}

func (t *Task) Detail(events []AgentEvent) TaskDetail {
	if events == nil {
		events = []AgentEvent{}
	}
	return TaskDetail{Task: t.Clone(), Payload: t.Payload, Analysis: t.Analysis, Summary: t.Summary, Events: events}
}

// VM is a pool VM as reported by poold.Status.
type VM struct {
	VMID    string     `json:"vm_id"`
	Name    string     `json:"name,omitempty"`
	State   VMState    `json:"state"`
	Host    string     `json:"host,omitempty"`
	TaskID  string     `json:"task_id,omitempty"`
	Service string     `json:"service,omitempty"`
	Alert   string     `json:"alert,omitempty"`
	Since   *time.Time `json:"since,omitempty"`
	Reason  string     `json:"degraded_reason,omitempty"`
	Prepare *Prepare   `json:"prepare,omitempty"`
}

type Prepare struct {
	Step    string  `json:"step,omitempty"`
	Attempt int     `json:"attempt,omitempty"`
	Elapsed float64 `json:"elapsed_s,omitempty"`
	Sample  string  `json:"gate_sample,omitempty"`
}

// Memory is one row of what we've learned — written from an agent's response.
type Memory struct {
	ID           int64     `json:"id"`
	Signature    string    `json:"signature"`
	Service      string    `json:"service"`
	Symptom      string    `json:"symptom,omitempty"`
	RootCause    string    `json:"root_cause"`
	FixSummary   string    `json:"fix_summary,omitempty"`
	PRURL        string    `json:"pr_url,omitempty"`
	FullAnalysis string    `json:"full_analysis,omitempty"`
	CreatedAt    time.Time `json:"created_at"`
	LastSeen     time.Time `json:"last_seen"`
	HitCount     int       `json:"hit_count"`
}

// Summary is the trimmed memory row the dashboard list shows (no full_analysis).
func (m Memory) Summary() MemorySummary {
	return MemorySummary{
		ID:        m.ID,
		Signature: m.Signature,
		Service:   m.Service,
		RootCause: m.RootCause,
		PRURL:     m.PRURL,
		HitCount:  m.HitCount,
		LastSeen:  m.LastSeen,
	}
}

// MemorySummary is what /api/state carries for each known issue.
type MemorySummary struct {
	ID        int64     `json:"id"`
	Signature string    `json:"signature"`
	Service   string    `json:"service"`
	RootCause string    `json:"root_cause"`
	PRURL     string    `json:"pr_url,omitempty"`
	HitCount  int       `json:"hit_count"`
	LastSeen  time.Time `json:"last_seen"`
}

// Stats are the headline numbers for the dashboard's stat tiles.
type Stats struct {
	VMsTotal     int     `json:"vms_total"`
	VMsReady     int     `json:"vms_ready"`
	VMsAllocated int     `json:"vms_allocated"`
	VMsDegraded  int     `json:"vms_degraded"`
	QueueDepth   int     `json:"queue_depth"`
	Running      int     `json:"running"`
	Completed    int     `json:"completed"`
	TasksTotal   int     `json:"tasks_total"`
	CacheHits    int     `json:"cache_hits"`
	HitRate      float64 `json:"hit_rate"`
	AvgTaskSecs  int     `json:"avg_task_secs"`
}

// State is the full snapshot served by GET /api/state and streamed on /api/stream.
type State struct {
	Stats  Stats           `json:"stats"`
	VMs    []VM            `json:"vms"`
	Tasks  []Task          `json:"tasks"`
	Memory []MemorySummary `json:"memory"`
}
