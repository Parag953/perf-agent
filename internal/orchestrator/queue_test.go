package orchestrator

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/Parag953/perf-agent/internal/config"
	"github.com/Parag953/perf-agent/internal/dispatch"
	"github.com/Parag953/perf-agent/internal/model"
	"github.com/Parag953/perf-agent/internal/poold"
	"github.com/Parag953/perf-agent/internal/store"
)

type capPoold struct {
	mu     sync.Mutex
	free   []string
	inUse  map[string]bool
	leases int
}

func newCapPoold(n int) *capPoold {
	p := &capPoold{inUse: map[string]bool{}}
	for i := 0; i < n; i++ {
		p.free = append(p.free, fmt.Sprintf("VM10%d", i+1))
	}
	return p
}

func (p *capPoold) Lease(context.Context) (*poold.Lease, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.free) == 0 {
		return nil, poold.ErrNoCapacity
	}
	id := p.free[0]
	p.free = p.free[1:]
	p.inUse[id] = true
	p.leases++
	return &poold.Lease{VMID: id, Host: "10.0.0.1", SSHTarget: "agent@10.0.0.1", State: "allocated"}, nil
}

func (p *capPoold) Release(_ context.Context, vmID string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.inUse[vmID] {
		delete(p.inUse, vmID)
		p.free = append(p.free, vmID)
	}
	return nil
}

func (p *capPoold) Status(context.Context) ([]model.VM, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	var out []model.VM
	for _, id := range p.free {
		out = append(out, model.VM{VMID: id, State: model.VMReady})
	}
	for id := range p.inUse {
		out = append(out, model.VM{VMID: id, State: model.VMAllocated})
	}
	return out, nil
}

func (p *capPoold) leaseCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.leases
}

func newTestOrch(t *testing.T, pool poold.Client) (*Orchestrator, *store.Store) {
	return newPacedOrch(t, pool, 0)
}

func newPacedOrch(t *testing.T, pool poold.Client, step time.Duration) (*Orchestrator, *store.Store) {
	t.Helper()
	mem, err := store.Open(filepath.Join(t.TempDir(), "memory.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { mem.Close() })
	cfg := config.Config{
		DispatchMode:  "mock",
		ClaudeTimeout: 30 * time.Second,
		QueuePoll:     20 * time.Millisecond,
		MockStepDelay: step,
	}
	o := New(cfg, pool, mem, fakeLLM{}, dispatch.New(cfg), nil, nil, log.New(io.Discard, "", 0))
	return o, mem
}

func findTask(o *Orchestrator, id string) (model.Task, bool) {
	for _, t := range o.Snapshot().Tasks {
		if t.ID == id {
			return t, true
		}
	}
	return model.Task{}, false
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func TestSecondTaskQueuesWhenTheOnlyVMIsBusyThenRunsWhenItFrees(t *testing.T) {
	pool := newCapPoold(1)
	o, _ := newPacedOrch(t, pool, 60*time.Millisecond)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	o.Start(ctx)

	trigger := model.Trigger{Service: "tachyon", Alert: "heap growth", Payload: json.RawMessage(`{}`)}
	first := o.Submit(trigger)
	second := o.Submit(model.Trigger{Service: "kepler", Alert: "cpu high", Payload: json.RawMessage(`{}`)})

	waitFor(t, "second task to be queued behind the first", func() bool {
		tk, ok := findTask(o, second.ID)
		return ok && tk.Phase == model.PhaseQueued && tk.QueuePos == 1
	})

	if tk, _ := findTask(o, second.ID); tk.Phase == model.PhaseError {
		t.Fatalf("queued task must not fail when the pool is exhausted: %s", tk.Error)
	}

	waitFor(t, "both tasks to finish", func() bool {
		a, okA := findTask(o, first.ID)
		b, okB := findTask(o, second.ID)
		return okA && okB && isTerminal(a.Phase) && isTerminal(b.Phase)
	})

	a, _ := findTask(o, first.ID)
	b, _ := findTask(o, second.ID)
	if a.Phase == model.PhaseError || b.Phase == model.PhaseError {
		t.Fatalf("tasks errored: first=%q second=%q", a.Error, b.Error)
	}
	if pool.leaseCount() != 2 {
		t.Errorf("lease count = %d, want 2", pool.leaseCount())
	}
}

func TestQueueDispatchesInSubmissionOrder(t *testing.T) {
	pool := newCapPoold(1)
	o, _ := newTestOrch(t, pool)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	o.Start(ctx)

	var ids []string
	for _, svc := range []string{"alpha", "bravo", "charlie"} {
		ids = append(ids, o.Submit(model.Trigger{Service: svc, Alert: "a", Payload: json.RawMessage(`{}`)}).ID)
	}

	waitFor(t, "all three tasks to finish", func() bool {
		for _, id := range ids {
			tk, ok := findTask(o, id)
			if !ok || !isTerminal(tk.Phase) {
				return false
			}
		}
		return true
	})

	var starts []time.Time
	for _, id := range ids {
		tk, _ := findTask(o, id)
		if tk.StartedAt == nil {
			t.Fatalf("task %s has no start time", id)
		}
		starts = append(starts, *tk.StartedAt)
	}
	for i := 1; i < len(starts); i++ {
		if starts[i].Before(starts[i-1]) {
			t.Errorf("task %s started before %s — queue is not FIFO", ids[i], ids[i-1])
		}
	}
}

func TestQueueDepthCountsWaitingTasks(t *testing.T) {
	pool := newCapPoold(1)
	o, _ := newPacedOrch(t, pool, 60*time.Millisecond)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	o.Start(ctx)

	for _, svc := range []string{"alpha", "bravo", "charlie"} {
		o.Submit(model.Trigger{Service: svc, Alert: "a", Payload: json.RawMessage(`{}`)})
	}

	waitFor(t, "queue depth to reach 2", func() bool {
		return o.Snapshot().Stats.QueueDepth == 2
	})
}

func TestExhaustedPoolNeverFailsTheTask(t *testing.T) {
	pool := newCapPoold(0)
	o, _ := newTestOrch(t, pool)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	o.Start(ctx)

	task := o.Submit(model.Trigger{Service: "tachyon", Alert: "heap", Payload: json.RawMessage(`{}`)})

	waitFor(t, "task to reach the queue", func() bool {
		tk, ok := findTask(o, task.ID)
		return ok && tk.Phase == model.PhaseQueued
	})
	time.Sleep(300 * time.Millisecond)

	tk, _ := findTask(o, task.ID)
	if tk.Phase != model.PhaseQueued {
		t.Errorf("phase = %s, want it to stay queued with no capacity", tk.Phase)
	}
	if tk.Error != "" {
		t.Errorf("error = %q, want none", tk.Error)
	}
}

func TestRecoverFailsOrphanedTasksAndReleasesTheirVMs(t *testing.T) {
	pool := newCapPoold(1)
	o, mem := newTestOrch(t, pool)

	if _, err := pool.Lease(context.Background()); err != nil {
		t.Fatal(err)
	}
	started := time.Now().UTC()
	err := mem.SaveTask(model.Task{
		ID: "t-orphan", Service: "tachyon", Alert: "heap", Phase: model.PhaseRunning,
		VMID: "VM101", EnqueuedAt: started, StartedAt: &started,
	})
	if err != nil {
		t.Fatal(err)
	}

	o.Recover(context.Background())

	tk, ok := findTask(o, "t-orphan")
	if !ok {
		t.Fatal("recovered task missing from the registry")
	}
	if tk.Phase != model.PhaseError {
		t.Errorf("phase = %s, want error", tk.Phase)
	}
	if tk.Error == "" {
		t.Error("recovered task should record why it was failed")
	}
	if len(pool.free) != 1 {
		t.Errorf("orphaned VM was not released: free = %v", pool.free)
	}
}

func TestRecoverKeepsFinishedTasksIntact(t *testing.T) {
	o, mem := newTestOrch(t, newCapPoold(1))
	fin := time.Now().UTC()
	err := mem.SaveTask(model.Task{
		ID: "t-done", Service: "tachyon", Phase: model.PhaseDone, Outcome: model.OutcomePROpened,
		PRURL: "https://x/pull/7", EnqueuedAt: fin, FinishedAt: &fin,
	})
	if err != nil {
		t.Fatal(err)
	}

	o.Recover(context.Background())

	tk, ok := findTask(o, "t-done")
	if !ok {
		t.Fatal("finished task missing after recovery")
	}
	if tk.Phase != model.PhaseDone || tk.PRURL != "https://x/pull/7" {
		t.Errorf("finished task altered by recovery: %+v", tk)
	}
}

func TestAgentEventsArePersistedAndServedWithTaskDetail(t *testing.T) {
	o, _ := newTestOrch(t, newCapPoold(1))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	o.Start(ctx)

	task := o.Submit(model.Trigger{
		Service: "tachyon", Alert: "heap growth",
		Payload: json.RawMessage(`{"tags":["kube_deployment:tachyon"]}`),
	})
	waitFor(t, "task to finish", func() bool {
		tk, ok := findTask(o, task.ID)
		return ok && isTerminal(tk.Phase)
	})

	detail, err := o.TaskDetail(task.ID)
	if err != nil {
		t.Fatalf("task detail: %v", err)
	}
	if len(detail.Events) == 0 {
		t.Fatal("expected the agent transcript to be persisted")
	}
	var kinds []model.EventKind
	for _, e := range detail.Events {
		kinds = append(kinds, e.Kind)
	}
	if !containsKind(kinds, model.EventTool) || !containsKind(kinds, model.EventText) {
		t.Errorf("transcript kinds = %v, want tool and text events", kinds)
	}
	if string(detail.Payload) != `{"tags":["kube_deployment:tachyon"]}` {
		t.Errorf("detail payload = %s, want the original trigger body", detail.Payload)
	}
	if detail.Analysis == "" {
		t.Error("detail should carry the agent's full analysis")
	}
}

func containsKind(kinds []model.EventKind, want model.EventKind) bool {
	for _, k := range kinds {
		if k == want {
			return true
		}
	}
	return false
}

func TestSubscribeReceivesLiveEventsForARunningTask(t *testing.T) {
	o, _ := newTestOrch(t, newCapPoold(1))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	o.Start(ctx)

	task := o.Submit(model.Trigger{Service: "tachyon", Alert: "heap", Payload: json.RawMessage(`{}`)})
	ch, unsubscribe := o.SubscribeTask(task.ID)
	defer unsubscribe()

	select {
	case e := <-ch:
		if e.TaskID != task.ID {
			t.Errorf("event task id = %q, want %q", e.TaskID, task.ID)
		}
	case <-time.After(8 * time.Second):
		t.Fatal("no live event arrived for the running task")
	}
}
