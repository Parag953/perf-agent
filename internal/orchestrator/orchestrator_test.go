package orchestrator

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Parag953/perf-agent/internal/config"
	"github.com/Parag953/perf-agent/internal/dispatch"
	"github.com/Parag953/perf-agent/internal/model"
	"github.com/Parag953/perf-agent/internal/poold"
	"github.com/Parag953/perf-agent/internal/store"
)

// fakeLLM answers the three orchestrator prompts deterministically, keyed on the
// system prompt, so we can exercise both the miss and the hit path without a
// real model.
type fakeLLM struct{}

var idRe = regexp.MustCompile(`id=(\d+)`)

func (fakeLLM) Backend() string { return "fake" }

func (fakeLLM) Complete(_ context.Context, system, user string) (string, error) {
	switch {
	case strings.Contains(system, "fingerprint"):
		return "tachyon/scope-map-churn", nil
	case strings.Contains(system, "decide whether"):
		if m := idRe.FindStringSubmatch(user); len(m) == 2 {
			return fmt.Sprintf(`{"match": %s}`, m[1]), nil
		}
		return `{"match": null}`, nil
	case strings.Contains(system, "compress"):
		return `{"symptom":"heap churn","root_cause":"resources cache rebuilt per request","fix_summary":"memoize per request"}`, nil
	}
	return "", nil
}

// fakePoold is a one-VM pool that always leases successfully.
type fakePoold struct {
	mu    sync.Mutex
	state model.VMState
}

func (p *fakePoold) Lease(context.Context) (*poold.Lease, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.state = model.VMAllocated
	return &poold.Lease{VMID: "VM101", Host: "10.0.0.1", SSHTarget: "agent@10.0.0.1", State: "allocated"}, nil
}

func (p *fakePoold) Release(context.Context, string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.state = model.VMDegraded
	return nil
}

func (p *fakePoold) Status(context.Context) ([]model.VM, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	st := p.state
	if st == "" {
		st = model.VMReady
	}
	return []model.VM{{VMID: "VM101", State: st, Host: "10.0.0.1"}}, nil
}

func waitDone(t *testing.T, o *Orchestrator, taskID string) model.Task {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		for _, task := range o.Snapshot().Tasks {
			if task.ID == taskID && (task.Phase == model.PhaseDone || task.Phase == model.PhaseError) {
				return task
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("task %s did not finish in time", taskID)
	return model.Task{}
}

func TestPipeline_MissThenHit(t *testing.T) {
	dir := t.TempDir()
	mem, err := store.Open(filepath.Join(dir, "memory.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer mem.Close()

	cfg := config.Config{Workers: 1, DispatchMode: "mock", ClaudeTimeout: 5 * time.Second}
	disp := dispatch.New(cfg)
	logger := log.New(io.Discard, "", 0)
	o := New(cfg, &fakePoold{}, mem, fakeLLM{}, disp, nil, logger)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	o.Start(ctx)

	trigger := model.Trigger{Service: "tachyon", Alert: "memory > 40%", Payload: json.RawMessage(`{}`)}

	// First firing: miss → agent runs → PR opened → memory written.
	first := o.Submit(trigger)
	done := waitDone(t, o, first.ID)
	if done.Outcome != model.OutcomePROpened {
		t.Fatalf("first task outcome = %q, want pr_opened", done.Outcome)
	}
	if done.PRURL == "" {
		t.Fatal("first task should have captured a PR url")
	}
	if got := len(mustSummaries(t, mem)); got != 1 {
		t.Fatalf("memory rows = %d, want 1", got)
	}

	// Second firing of the same issue: memory hit, no new row, no VM outcome.
	second := o.Submit(trigger)
	done2 := waitDone(t, o, second.ID)
	if done2.Outcome != model.OutcomeMemoryHit {
		t.Fatalf("second task outcome = %q, want memory_hit", done2.Outcome)
	}
	if got := len(mustSummaries(t, mem)); got != 1 {
		t.Fatalf("memory rows after hit = %d, want 1 (no new row)", got)
	}
	if hits := o.Snapshot().Stats.CacheHits; hits != 1 {
		t.Fatalf("cache_hits = %d, want 1", hits)
	}

	// The hit should have bumped the row's hit_count.
	row, _ := mem.Get(done.MemoryID)
	if row.HitCount != 1 {
		t.Fatalf("hit_count = %d, want 1", row.HitCount)
	}
}

func mustSummaries(t *testing.T, s *store.Store) []model.MemorySummary {
	t.Helper()
	out, err := s.Summaries(100)
	if err != nil {
		t.Fatal(err)
	}
	return out
}
