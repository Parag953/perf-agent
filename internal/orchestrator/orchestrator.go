// Package orchestrator is app.py's successor: it takes a trigger, fingerprints
// the issue, checks memory, and on a miss queues the task for a VM, dispatches
// the agent, distills the result back into memory, and releases the VM. It also
// serves the live state the dashboard reads.
//
// Concurrency model:
//   - Submit → intake channel.
//   - matcher goroutines: signature + memory match. Hit → done (no VM). Miss →
//     append to the wait-queue (visible as phase "waiting").
//   - one scheduler goroutine: whenever a VM might be free (a release, a poold
//     poll tick, or a new task queued) it leases VMs and dispatches queued tasks
//     in FIFO order. If poold reports no capacity the task stays queued.
//   - each dispatch runs in its own goroutine; the leased VM gates concurrency.
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

	intake chan *model.Task
	wake   chan struct{} // nudges the scheduler (release / poll / new queued task)

	mu       sync.Mutex
	tasks    map[string]*model.Task
	order    []string // task ids, insertion order
	logs     map[string][]model.AgentEvent
	seq      int64
	total    int // cumulative tasks submitted (survives eviction)
	hits     int
	durSum   time.Duration
	durCount int

	waitMu    sync.Mutex
	waitQueue []*model.Task // FIFO of tasks that missed memory and await a VM

	vmMu   sync.Mutex
	vmSnap []model.VM // last good poold status
}

func New(cfg config.Config, pool poold.Client, mem *store.Store, l llm.Client, disp dispatch.Dispatcher, hub Broadcaster, logger *log.Logger) *Orchestrator {
	return &Orchestrator{
		cfg:    cfg,
		pool:   pool,
		mem:    mem,
		llm:    l,
		disp:   disp,
		hub:    hub,
		log:    logger,
		intake: make(chan *model.Task, 256),
		wake:   make(chan struct{}, 1),
		tasks:  make(map[string]*model.Task),
		logs:   make(map[string][]model.AgentEvent),
	}
}

// Start launches the matcher pool, the scheduler, and the poold poller.
func (o *Orchestrator) Start(ctx context.Context) {
	for range max(o.cfg.Workers, 1) {
		go o.matcher(ctx)
	}
	go o.scheduler(ctx)
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
	case o.intake <- task:
	default:
		o.log.Printf("intake full, dropping task %s", id)
		o.fail(task, fmt.Errorf("intake queue full"))
	}
	return snap
}

// ---- matcher: fingerprint + memory lookup ----

func (o *Orchestrator) matcher(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case task := <-o.intake:
			o.match(ctx, task)
		}
	}
}

func (o *Orchestrator) match(ctx context.Context, task *model.Task) {
	trigger := model.Trigger{Service: task.Service, Alert: task.Alert, Payload: task.Payload}
	now := time.Now().UTC()
	o.update(task, func(t *model.Task) { t.StartedAt = &now })

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
			o.appendLog(task.ID, model.AgentEvent{Kind: "result", Summary: "memory hit — reused a prior analysis, no VM spent"})
			o.finish(task, model.OutcomeMemoryHit, func(t *model.Task) {
				t.MemoryID = matchID
				t.PRURL = mem.PRURL
			})
			o.log.Printf("task %s: memory HIT (id=%d, sig=%s) — no VM spent", task.ID, matchID, sig)
			return
		}
	}

	// Miss: join the wait-queue for a VM.
	o.enqueueWaiting(task)
}

// ---- wait-queue ----

func (o *Orchestrator) enqueueWaiting(task *model.Task) {
	o.setPhase(task, model.PhaseWaiting)
	o.waitMu.Lock()
	o.waitQueue = append(o.waitQueue, task)
	o.waitMu.Unlock()
	o.signalWake()
}

func (o *Orchestrator) dequeueWaiting() *model.Task {
	o.waitMu.Lock()
	defer o.waitMu.Unlock()
	if len(o.waitQueue) == 0 {
		return nil
	}
	t := o.waitQueue[0]
	o.waitQueue = o.waitQueue[1:]
	return t
}

func (o *Orchestrator) requeueFront(task *model.Task) {
	o.waitMu.Lock()
	o.waitQueue = append([]*model.Task{task}, o.waitQueue...)
	o.waitMu.Unlock()
}

func (o *Orchestrator) queueDepth() int {
	o.waitMu.Lock()
	defer o.waitMu.Unlock()
	return len(o.waitQueue)
}

func (o *Orchestrator) signalWake() {
	select {
	case o.wake <- struct{}{}:
	default:
	}
}

// ---- scheduler: lease VMs for queued tasks, FIFO ----

func (o *Orchestrator) scheduler(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-o.wake:
		}
		for {
			task := o.dequeueWaiting()
			if task == nil {
				break
			}
			o.setPhase(task, model.PhaseLeasing)
			lease, err := o.pool.Lease(ctx)
			if errors.Is(err, poold.ErrNoCapacity) {
				// No VM free — put it back at the front and wait for the next wake.
				o.requeueFront(task)
				o.setPhase(task, model.PhaseWaiting)
				break
			}
			if err != nil {
				o.fail(task, fmt.Errorf("lease: %w", err))
				continue
			}
			o.update(task, func(t *model.Task) { t.VMID = lease.VMID })
			go o.investigate(ctx, task, lease)
			o.refreshStatus(ctx) // reflect the fresh allocation in the VM grid promptly

		}
	}
}

// investigate runs one task on its leased VM and always releases it.
func (o *Orchestrator) investigate(ctx context.Context, task *model.Task, lease *poold.Lease) {
	defer o.release(lease.VMID)

	trigger := model.Trigger{Service: task.Service, Alert: task.Alert, Payload: task.Payload}

	o.setPhase(task, model.PhaseDispatched)
	prior := o.closestMemory(task.Service, task.Signature)
	prompt := dispatch.BuildInvestigationPrompt(o.cfg, trigger, prior)
	o.setPhase(task, model.PhaseRunning)

	emit := func(ev model.AgentEvent) { o.appendLog(task.ID, ev) }
	runCtx, cancel := context.WithTimeout(ctx, o.cfg.ClaudeTimeout)
	analysis, err := o.disp.Run(runCtx, lease.SSHTarget, prompt, emit)
	cancel()
	if err != nil {
		o.appendLog(task.ID, model.AgentEvent{Kind: "error", Summary: truncate(err.Error(), 300)})
		o.fail(task, fmt.Errorf("dispatch: %w", err))
		return
	}

	o.setPhase(task, model.PhaseCollecting)
	if q := dispatch.ExtractNeedQueries(analysis); q != "" {
		o.finish(task, model.OutcomeNeedQueries, nil)
		o.log.Printf("task %s: agent needs DB queries — parked (resume is future work)", task.ID)
		return
	}
	o.setPhase(task, model.PhasePRCheck)
	prURL := dispatch.ExtractPRURL(analysis)

	o.setPhase(task, model.PhaseDistilling)
	mem, err := llm.Distill(ctx, o.llm, trigger, task.Signature, analysis, prURL)
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

func (o *Orchestrator) release(vmID string) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := o.pool.Release(ctx, vmID); err != nil {
		o.log.Printf("release %s failed: %v", vmID, err)
	}
	o.refreshStatus(ctx)
	o.signalWake() // a VM just freed — let the scheduler pull the next queued task
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

// ---- per-task activity log ----

const maxLogEvents = 400

func (o *Orchestrator) appendLog(id string, ev model.AgentEvent) {
	o.mu.Lock()
	if ev.TS.IsZero() {
		ev.TS = time.Now().UTC()
	}
	l := o.logs[id]
	ev.Seq = len(l)
	l = append(l, ev)
	if len(l) > maxLogEvents {
		l = l[len(l)-maxLogEvents:]
	}
	o.logs[id] = l
	o.mu.Unlock()
	o.broadcast()
}

// TaskLog backs GET /api/logs/:id.
func (o *Orchestrator) TaskLog(id string) []model.AgentEvent {
	o.mu.Lock()
	defer o.mu.Unlock()
	src := o.logs[id]
	out := make([]model.AgentEvent, len(src))
	copy(out, src)
	return out
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
			delete(o.logs, id)
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
			o.signalWake() // capacity may have freed up out-of-band
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
	// Map each VM to the task currently holding it, so the UI can show what an
	// allocated VM is running (poold's own task_id can be null).
	vmTask := map[string]string{}
	for _, id := range o.order {
		t := o.tasks[id]
		tasks = append(tasks, t.Clone())
		if t.VMID != "" && !isTerminal(t.Phase) {
			vmTask[t.VMID] = t.ID
		}
	}
	stats := model.Stats{
		QueueDepth: o.queueDepth(),
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

	for i := range vms {
		if id, ok := vmTask[vms[i].VMID]; ok && vms[i].TaskID == "" {
			vms[i].TaskID = id
		}
		stats.VMsTotal++
		switch vms[i].State {
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

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}
