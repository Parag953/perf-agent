// Package orchestrator is app.py's successor: it takes a trigger, fingerprints
// the issue, checks memory, and on a miss leases a VM, dispatches the agent,
// distills the result back into memory, and releases the VM. It also serves the
// live state the dashboard reads.
package orchestrator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/Parag953/perf-agent/internal/config"
	"github.com/Parag953/perf-agent/internal/dispatch"
	"github.com/Parag953/perf-agent/internal/llm"
	"github.com/Parag953/perf-agent/internal/model"
	"github.com/Parag953/perf-agent/internal/poold"
	"github.com/Parag953/perf-agent/internal/store"
)

// Broadcaster is how the orchestrator notifies the SSE hub that state changed.
type Broadcaster interface {
	Broadcast([]byte)
}

type Orchestrator struct {
	cfg  config.Config
	pool poold.Client
	mem  *store.Store
	llm  llm.Client
	disp dispatch.Dispatcher
	hub  Broadcaster
	log  *log.Logger

	queue chan *model.Task

	mu       sync.Mutex
	tasks    map[string]*model.Task
	order    []string // task ids, insertion order
	seq      int64
	total    int // cumulative tasks submitted (survives eviction)
	hits     int
	durSum   time.Duration
	durCount int

	vmMu   sync.Mutex
	vmSnap []model.VM // last good poold status
}

func New(cfg config.Config, pool poold.Client, mem *store.Store, l llm.Client, disp dispatch.Dispatcher, hub Broadcaster, logger *log.Logger) *Orchestrator {
	return &Orchestrator{
		cfg:   cfg,
		pool:  pool,
		mem:   mem,
		llm:   l,
		disp:  disp,
		hub:   hub,
		log:   logger,
		queue: make(chan *model.Task, 256),
		tasks: make(map[string]*model.Task),
	}
}

// Start launches the worker pool and a background poller for poold status.
func (o *Orchestrator) Start(ctx context.Context) {
	for i := 0; i < o.cfg.Workers; i++ {
		go o.worker(ctx, i)
	}
	go o.pollStatus(ctx)
}

// Submit enqueues a new investigation and returns the created task.
func (o *Orchestrator) Submit(t model.Trigger) model.Task {
	o.mu.Lock()
	o.seq++
	o.total++
	id := fmt.Sprintf("t-%d", o.seq)
	task := &model.Task{
		ID:         id,
		Service:    t.Service,
		Alert:      t.Alert,
		Phase:      model.PhaseQueued,
		Payload:    t.Payload,
		EnqueuedAt: time.Now().UTC(),
	}
	o.tasks[id] = task
	o.order = append(o.order, id)
	snap := task.Clone()
	o.mu.Unlock()

	o.broadcast()
	select {
	case o.queue <- task:
	default:
		o.log.Printf("queue full, dropping task %s", id)
	}
	return snap
}

func (o *Orchestrator) worker(ctx context.Context, n int) {
	for {
		select {
		case <-ctx.Done():
			return
		case task := <-o.queue:
			o.handle(ctx, task)
		}
	}
}

func (o *Orchestrator) handle(ctx context.Context, task *model.Task) {
	trigger := model.Trigger{Service: task.Service, Alert: task.Alert, Payload: task.Payload}
	now := time.Now().UTC()
	o.update(task, func(t *model.Task) { t.StartedAt = &now })

	// 1. Fingerprint + memory lookup.
	o.setPhase(task, model.PhaseMatching)
	sig, err := llm.Signature(ctx, o.llm, trigger)
	if err != nil {
		o.log.Printf("task %s: signature failed: %v", task.ID, err)
	} else {
		o.update(task, func(t *model.Task) { t.Signature = sig })
	}

	if sig != "" {
		candidates, err := o.mem.CandidatesByService(task.Service)
		if err != nil {
			o.log.Printf("task %s: candidates failed: %v", task.ID, err)
		}
		matchID, err := llm.Match(ctx, o.llm, sig, candidates)
		if err != nil {
			o.log.Printf("task %s: match failed: %v", task.ID, err)
		}
		if matchID > 0 {
			if err := o.mem.BumpHit(matchID); err != nil {
				o.log.Printf("task %s: bump hit failed: %v", task.ID, err)
			}
			mem, _ := o.mem.Get(matchID)
			o.mu.Lock()
			o.hits++
			o.mu.Unlock()
			o.finish(task, model.OutcomeMemoryHit, func(t *model.Task) {
				t.MemoryID = matchID
				t.PRURL = mem.PRURL
			})
			o.log.Printf("task %s: memory HIT (id=%d, sig=%s) — no VM spent", task.ID, matchID, sig)
			return
		}
	}

	// 2. Lease a VM (retry while the pool is exhausted).
	o.setPhase(task, model.PhaseLeasing)
	lease, err := o.leaseWithWait(ctx, task)
	if err != nil {
		o.fail(task, fmt.Errorf("lease: %w", err))
		return
	}
	o.update(task, func(t *model.Task) { t.VMID = lease.VMID })
	defer o.release(lease.VMID)

	// 3. Dispatch the agent.
	o.setPhase(task, model.PhaseDispatched)
	prior := o.closestMemory(task.Service, sig)
	prompt := dispatch.BuildInvestigationPrompt(o.cfg, trigger, prior)
	o.setPhase(task, model.PhaseRunning)

	runCtx, cancel := context.WithTimeout(ctx, o.cfg.ClaudeTimeout)
	analysis, err := o.disp.Run(runCtx, lease.SSHTarget, prompt)
	cancel()
	if err != nil {
		o.fail(task, fmt.Errorf("dispatch: %w", err))
		return
	}

	// 4. Collect + PR check.
	o.setPhase(task, model.PhaseCollecting)
	if q := dispatch.ExtractNeedQueries(analysis); q != "" {
		// The agent paused for DB data. The human-in-the-loop resume is future
		// work; for now the task ends here without a memory write.
		o.finish(task, model.OutcomeNeedQueries, nil)
		o.log.Printf("task %s: agent needs DB queries — parked (resume is future work)", task.ID)
		return
	}
	o.setPhase(task, model.PhasePRCheck)
	prURL := dispatch.ExtractPRURL(analysis)

	// 5. Distill into memory.
	o.setPhase(task, model.PhaseDistilling)
	mem, err := llm.Distill(ctx, o.llm, trigger, sig, analysis, prURL)
	if err != nil {
		o.log.Printf("task %s: distill failed: %v", task.ID, err)
	}
	memID, err := o.mem.Insert(mem)
	if err != nil {
		o.log.Printf("task %s: memory insert failed: %v", task.ID, err)
	}

	outcome := model.OutcomeAnalysisOnly
	if prURL != "" {
		outcome = model.OutcomePROpened
	}
	o.finish(task, outcome, func(t *model.Task) {
		t.PRURL = prURL
		t.MemoryID = memID
	})
	o.log.Printf("task %s: done (outcome=%s, pr=%q, mem=%d)", task.ID, outcome, prURL, memID)
}

// leaseWithWait retries Lease while poold reports no capacity, backing off and
// keeping the task visible as "leasing" the whole time.
func (o *Orchestrator) leaseWithWait(ctx context.Context, task *model.Task) (*poold.Lease, error) {
	backoff := 2 * time.Second
	for {
		lease, err := o.pool.Lease(ctx)
		if err == nil {
			return lease, nil
		}
		if !errors.Is(err, poold.ErrNoCapacity) {
			return nil, err
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(backoff):
		}
		if backoff < 15*time.Second {
			backoff += 2 * time.Second
		}
	}
}

func (o *Orchestrator) release(vmID string) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := o.pool.Release(ctx, vmID); err != nil {
		o.log.Printf("release %s failed: %v", vmID, err)
	}
	o.refreshStatus(ctx)
}

// closestMemory returns a compact hint from the most-recently-seen memory for
// this service, injected into the agent prompt on near-misses (design layer 2).
func (o *Orchestrator) closestMemory(service, _ string) string {
	cands, err := o.mem.CandidatesByService(service)
	if err != nil || len(cands) == 0 {
		return ""
	}
	m := cands[0]
	hint := fmt.Sprintf("signature=%s\nroot_cause=%s", m.Signature, m.RootCause)
	if m.FixSummary != "" {
		hint += "\nprior_fix=" + m.FixSummary
	}
	if m.PRURL != "" {
		hint += "\nprior_pr=" + m.PRURL
	}
	return hint
}

// ---- task state helpers (all broadcast) ----

func (o *Orchestrator) update(task *model.Task, mut func(*model.Task)) {
	o.mu.Lock()
	if mut != nil {
		mut(task)
	}
	o.mu.Unlock()
	o.broadcast()
}

func (o *Orchestrator) setPhase(task *model.Task, p model.Phase) {
	o.update(task, func(t *model.Task) { t.Phase = p })
}

func (o *Orchestrator) finish(task *model.Task, outcome model.Outcome, mut func(*model.Task)) {
	fin := time.Now().UTC()
	o.mu.Lock()
	task.Phase = model.PhaseDone
	task.Outcome = outcome
	task.FinishedAt = &fin
	if task.StartedAt != nil {
		o.durSum += fin.Sub(*task.StartedAt)
		o.durCount++
	}
	if mut != nil {
		mut(task)
	}
	o.evictLocked()
	o.mu.Unlock()
	o.broadcast()
}

func (o *Orchestrator) fail(task *model.Task, err error) {
	o.log.Printf("task %s FAILED: %v", task.ID, err)
	fin := time.Now().UTC()
	o.mu.Lock()
	task.Phase = model.PhaseError
	task.Outcome = model.OutcomeError
	task.Error = err.Error()
	task.FinishedAt = &fin
	o.evictLocked()
	o.mu.Unlock()
	o.broadcast()
}

// maxTerminalTasks bounds how many finished (done/error) tasks we retain in the
// live registry. All in-flight tasks are always kept; the cumulative count is
// tracked separately in o.total, so stats.tasks_total is unaffected by eviction.
const maxTerminalTasks = 50

// evictLocked drops the oldest terminal tasks beyond maxTerminalTasks so the
// registry (and the /api/state payload) can't grow without bound. Caller holds o.mu.
func (o *Orchestrator) evictLocked() {
	terminal := 0
	for _, id := range o.order {
		if isTerminal(o.tasks[id].Phase) {
			terminal++
		}
	}
	remove := terminal - maxTerminalTasks
	if remove <= 0 {
		return
	}
	kept := make([]string, 0, len(o.order)-remove)
	for _, id := range o.order {
		if remove > 0 && isTerminal(o.tasks[id].Phase) {
			delete(o.tasks, id)
			remove--
			continue
		}
		kept = append(kept, id)
	}
	o.order = kept
}

func isTerminal(p model.Phase) bool {
	return p == model.PhaseDone || p == model.PhaseError
}

// ---- poold status polling ----

func (o *Orchestrator) pollStatus(ctx context.Context) {
	t := time.NewTicker(5 * time.Second)
	defer t.Stop()
	o.refreshStatus(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			o.refreshStatus(ctx)
		}
	}
}

func (o *Orchestrator) refreshStatus(ctx context.Context) {
	vms, err := o.pool.Status(ctx)
	if err != nil {
		return // keep last good snapshot
	}
	o.vmMu.Lock()
	o.vmSnap = vms
	o.vmMu.Unlock()
	o.broadcast()
}

// ---- dashboard snapshot ----

func (o *Orchestrator) Snapshot() model.State {
	o.vmMu.Lock()
	// Always a non-nil slice: the dashboard contract promises arrays, and a nil
	// slice would marshal to JSON null and break consumers doing state.vms.map(...).
	vms := make([]model.VM, 0, len(o.vmSnap))
	vms = append(vms, o.vmSnap...)
	o.vmMu.Unlock()

	o.mu.Lock()
	tasks := make([]model.Task, 0, len(o.order))
	queueDepth := 0
	for _, id := range o.order {
		t := o.tasks[id]
		tasks = append(tasks, t.Clone())
		if t.Phase == model.PhaseQueued {
			queueDepth++
		}
	}
	stats := model.Stats{
		QueueDepth: queueDepth,
		TasksTotal: o.total,
		CacheHits:  o.hits,
	}
	if stats.TasksTotal > 0 {
		stats.HitRate = round2(float64(o.hits) / float64(stats.TasksTotal))
	}
	if o.durCount > 0 {
		stats.AvgTaskSecs = int((o.durSum / time.Duration(o.durCount)).Seconds())
	}
	o.mu.Unlock()

	for _, vm := range vms {
		stats.VMsTotal++
		switch vm.State {
		case model.VMReady, model.VMFree:
			stats.VMsReady++
		case model.VMAllocated:
			stats.VMsAllocated++
		case model.VMDegraded:
			stats.VMsDegraded++
		}
	}

	mems, err := o.mem.Summaries(100)
	if err != nil {
		o.log.Printf("snapshot: memory summaries failed: %v", err)
	}
	if mems == nil {
		mems = []model.MemorySummary{} // never null in the contract
	}

	return model.State{Stats: stats, VMs: vms, Tasks: tasks, Memory: mems}
}

// GetMemory backs GET /api/memory/:id.
func (o *Orchestrator) GetMemory(id int64) (model.Memory, error) {
	return o.mem.Get(id)
}

func (o *Orchestrator) broadcast() {
	if o.hub == nil {
		return
	}
	b, err := json.Marshal(o.Snapshot())
	if err != nil {
		return
	}
	o.hub.Broadcast(b)
}

func round2(f float64) float64 {
	return float64(int(f*100+0.5)) / 100
}
