// Package orchestrator takes a trigger, fingerprints the issue, checks memory,
// and on a miss queues it for a pool VM, dispatches the agent, streams the run
// back to the dashboard, distills the result into memory, and releases the VM.
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

const (
	maxTerminalTasks = 50
	maxRingEvents    = 400
	subChanBuffer    = 512
)

type Orchestrator struct {
	cfg  config.Config
	pool poold.Client
	mem  *store.Store
	llm  llm.Client
	disp dispatch.Dispatcher
	hub  Broadcaster
	log  *log.Logger

	mu       sync.Mutex
	tasks    map[string]*model.Task
	order    []string
	queue    []string
	seq      int64
	total    int
	hits     int
	durSum   time.Duration
	durCount int

	wake chan struct{}
	gate chan struct{}

	vmMu   sync.Mutex
	vmSnap []model.VM

	evMu  sync.Mutex
	ring  map[string][]model.AgentEvent
	evSeq map[string]int
	subs  map[string]map[chan model.AgentEvent]struct{}
}

func New(cfg config.Config, pool poold.Client, mem *store.Store, l llm.Client, disp dispatch.Dispatcher, hub Broadcaster, logger *log.Logger) *Orchestrator {
	workers := cfg.Workers
	if workers <= 0 {
		workers = 4
	}
	return &Orchestrator{
		cfg:   cfg,
		pool:  pool,
		mem:   mem,
		llm:   l,
		disp:  disp,
		hub:   hub,
		log:   logger,
		tasks: make(map[string]*model.Task),
		wake:  make(chan struct{}, 1),
		gate:  make(chan struct{}, workers),
		ring:  make(map[string][]model.AgentEvent),
		evSeq: make(map[string]int),
		subs:  make(map[string]map[chan model.AgentEvent]struct{}),
	}
}

func (o *Orchestrator) Start(ctx context.Context) {
	o.Recover(ctx)
	go o.scheduler(ctx)
	go o.pollStatus(ctx)
}

// Recover rebuilds the task registry from the store after a restart. Anything
// left mid-flight lost its agent when the process died, so it is failed and the
// VM it held is handed back to the pool.
func (o *Orchestrator) Recover(ctx context.Context) {
	tasks, err := o.mem.LoadTasks(maxTerminalTasks)
	if err != nil {
		o.log.Printf("recover: load tasks failed: %v", err)
		return
	}
	var orphans []*model.Task
	o.mu.Lock()
	for i := range tasks {
		t := tasks[i]
		if _, exists := o.tasks[t.ID]; exists {
			continue
		}
		if n := taskSeq(t.ID); n > o.seq {
			o.seq = n
		}
		o.total++
		if !isTerminal(t.Phase) {
			fin := time.Now().UTC()
			t.Phase = model.PhaseError
			t.Outcome = model.OutcomeError
			t.Error = "orchestrator restarted while this task was in flight"
			t.FinishedAt = &fin
			orphans = append(orphans, &t)
		}
		o.tasks[t.ID] = &t
		o.order = append(o.order, t.ID)
	}
	o.mu.Unlock()

	for _, t := range orphans {
		o.persist(t)
		if t.VMID != "" {
			if err := o.pool.Release(ctx, t.VMID); err != nil {
				o.log.Printf("recover: release %s failed: %v", t.VMID, err)
			} else {
				o.log.Printf("recover: released %s stranded by task %s", t.VMID, t.ID)
			}
		}
	}
	if len(tasks) > 0 {
		o.log.Printf("recover: restored %d tasks (%d were in flight)", len(tasks), len(orphans))
	}
	o.broadcast()
}

func taskSeq(id string) int64 {
	var n int64
	if _, err := fmt.Sscanf(id, "t-%d", &n); err != nil {
		return 0
	}
	return n
}

// Submit accepts a trigger. Fingerprinting and the memory lookup happen off the
// queue, so a known issue resolves without ever waiting for a VM.
func (o *Orchestrator) Submit(t model.Trigger) model.Task {
	o.mu.Lock()
	o.seq++
	o.total++
	id := fmt.Sprintf("t-%d", o.seq)
	task := &model.Task{
		ID:         id,
		Service:    t.Service,
		Alert:      t.Alert,
		Phase:      model.PhaseMatching,
		Payload:    t.Payload,
		EnqueuedAt: time.Now().UTC(),
	}
	o.tasks[id] = task
	o.order = append(o.order, id)
	snap := task.Clone()
	o.mu.Unlock()

	o.persist(task)
	o.broadcast()
	go o.intake(task)
	return snap
}

// intake fingerprints the trigger and checks memory. A hit finishes the task
// immediately; a miss puts it in the VM queue.
func (o *Orchestrator) intake(task *model.Task) {
	ctx := context.Background()
	o.gate <- struct{}{}
	defer func() { <-o.gate }()

	trigger := model.Trigger{Service: task.Service, Alert: task.Alert, Payload: task.Payload}
	sig, err := llm.Signature(ctx, o.llm, trigger)
	if err != nil && llm.Unavailable(err) {
		o.fail(task, fmt.Errorf("model unavailable, refusing to spend a VM: %w", err))
		return
	}
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
				t.Summary = mem.FixSummary
				t.Analysis = mem.FullAnalysis
			})
			o.log.Printf("task %s: memory HIT (id=%d, sig=%s) — no VM spent", task.ID, matchID, sig)
			return
		}
	}
	o.enqueue(task)
}

// enqueue inserts by submission sequence, not arrival: fingerprinting runs in a
// goroutine per task, so tasks can finish matching out of order and a plain
// append would let a later alert overtake an earlier one.
func (o *Orchestrator) enqueue(task *model.Task) {
	o.mu.Lock()
	seq := taskSeq(task.ID)
	at := len(o.queue)
	for i, id := range o.queue {
		if taskSeq(id) > seq {
			at = i
			break
		}
	}
	o.queue = append(o.queue, "")
	copy(o.queue[at+1:], o.queue[at:])
	o.queue[at] = task.ID
	task.Phase = model.PhaseQueued
	o.mu.Unlock()
	o.persist(task)
	o.broadcast()
	o.nudge()
}

func (o *Orchestrator) nudge() {
	select {
	case o.wake <- struct{}{}:
	default:
	}
}

// scheduler is the only goroutine that leases. It walks the queue head-first,
// so waiting tasks are served in submission order rather than racing.
func (o *Orchestrator) scheduler(ctx context.Context) {
	interval := o.cfg.QueuePoll
	if interval <= 0 {
		interval = 5 * time.Second
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		o.drainQueue(ctx)
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		case <-o.wake:
		}
	}
}

func (o *Orchestrator) drainQueue(ctx context.Context) {
	for {
		o.mu.Lock()
		if len(o.queue) == 0 {
			o.mu.Unlock()
			return
		}
		headID := o.queue[0]
		head := o.tasks[headID]
		o.mu.Unlock()
		if head == nil {
			o.mu.Lock()
			o.queue = o.queue[1:]
			o.mu.Unlock()
			continue
		}

		lease, err := o.pool.Lease(ctx)
		if err != nil {
			if !errors.Is(err, poold.ErrNoCapacity) {
				o.log.Printf("lease failed, task %s stays queued: %v", headID, err)
			}
			return
		}

		o.mu.Lock()
		o.queue = o.queue[1:]
		o.mu.Unlock()
		o.update(head, func(t *model.Task) {
			t.Phase = model.PhaseDispatched
			t.VMID = lease.VMID
		})
		go o.run(ctx, head, lease)
	}
}

func (o *Orchestrator) run(ctx context.Context, task *model.Task, lease *poold.Lease) {
	defer func() {
		o.release(lease.VMID)
		o.nudge()
	}()

	now := time.Now().UTC()
	o.update(task, func(t *model.Task) { t.StartedAt = &now })

	trigger := model.Trigger{Service: task.Service, Alert: task.Alert, Payload: task.Payload}
	prior := o.closestMemory(task.Service)
	prompt := dispatch.BuildInvestigationPrompt(o.cfg, trigger, prior)

	runCtx, cancel := context.WithTimeout(ctx, o.cfg.ClaudeTimeout)
	analysis, err := o.disp.Run(runCtx, lease.SSHTarget, prompt, func(e model.AgentEvent) {
		o.record(task, e)
	})
	cancel()
	if err != nil {
		o.record(task, model.AgentEvent{Kind: model.EventFailure, Text: err.Error()})
		o.fail(task, fmt.Errorf("dispatch: %w", err))
		return
	}

	o.setPhase(task, model.PhaseCollecting)
	o.update(task, func(t *model.Task) { t.Analysis = analysis })
	if q := dispatch.ExtractNeedQueries(analysis); q != "" {
		o.finish(task, model.OutcomeNeedQueries, func(t *model.Task) { t.Summary = q })
		o.log.Printf("task %s: agent needs DB queries — parked", task.ID)
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
		t.Summary = mem.FixSummary
	})
	o.log.Printf("task %s: done (outcome=%s, pr=%q, mem=%d)", task.ID, outcome, prURL, memID)
}

func (o *Orchestrator) release(vmID string) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := o.pool.Release(ctx, vmID); err != nil {
		o.log.Printf("release %s failed: %v", vmID, err)
	}
	o.refreshStatus(ctx)
}

func (o *Orchestrator) closestMemory(service string) string {
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

// ---- agent event stream ----

func (o *Orchestrator) record(task *model.Task, e model.AgentEvent) {
	o.evMu.Lock()
	o.evSeq[task.ID]++
	e.TaskID = task.ID
	e.Seq = o.evSeq[task.ID]
	if e.At.IsZero() {
		e.At = time.Now().UTC()
	}
	r := append(o.ring[task.ID], e)
	if len(r) > maxRingEvents {
		r = r[len(r)-maxRingEvents:]
	}
	o.ring[task.ID] = r
	subs := make([]chan model.AgentEvent, 0, len(o.subs[task.ID]))
	for ch := range o.subs[task.ID] {
		subs = append(subs, ch)
	}
	o.evMu.Unlock()

	for _, ch := range subs {
		select {
		case ch <- e:
		default:
		}
	}
	if err := o.mem.AppendEvent(e); err != nil {
		o.log.Printf("task %s: append event failed: %v", task.ID, err)
	}

	o.mu.Lock()
	promote := e.Kind != model.EventBootstrap && task.Phase == model.PhaseDispatched
	o.mu.Unlock()
	if promote {
		o.setPhase(task, model.PhaseRunning)
	}
}

// SubscribeTask returns a channel of agent events for one task, pre-loaded with
// what has already happened so a dashboard opening mid-run sees the whole run.
func (o *Orchestrator) SubscribeTask(taskID string) (<-chan model.AgentEvent, func()) {
	ch := make(chan model.AgentEvent, subChanBuffer)
	o.evMu.Lock()
	for _, e := range o.ring[taskID] {
		select {
		case ch <- e:
		default:
		}
	}
	if o.subs[taskID] == nil {
		o.subs[taskID] = map[chan model.AgentEvent]struct{}{}
	}
	o.subs[taskID][ch] = struct{}{}
	o.evMu.Unlock()

	var once sync.Once
	return ch, func() {
		once.Do(func() {
			o.evMu.Lock()
			if set := o.subs[taskID]; set != nil {
				delete(set, ch)
				if len(set) == 0 {
					delete(o.subs, taskID)
				}
			}
			o.evMu.Unlock()
			close(ch)
		})
	}
}

// ---- task state helpers ----

func (o *Orchestrator) persist(task *model.Task) {
	o.mu.Lock()
	snap := *task
	o.mu.Unlock()
	if err := o.mem.SaveTask(snap); err != nil {
		o.log.Printf("task %s: persist failed: %v", task.ID, err)
	}
}

func (o *Orchestrator) update(task *model.Task, mut func(*model.Task)) {
	o.mu.Lock()
	if mut != nil {
		mut(task)
	}
	o.mu.Unlock()
	o.persist(task)
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
	o.persist(task)
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
	o.persist(task)
	o.broadcast()
}

// evictLocked bounds retained terminal tasks in the live registry; the store
// keeps the full history. Caller holds o.mu.
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
		return
	}
	o.vmMu.Lock()
	o.vmSnap = vms
	o.vmMu.Unlock()
	o.broadcast()
}

// ---- dashboard snapshot ----

func (o *Orchestrator) Snapshot() model.State {
	o.vmMu.Lock()
	vms := make([]model.VM, 0, len(o.vmSnap))
	vms = append(vms, o.vmSnap...)
	o.vmMu.Unlock()

	o.mu.Lock()
	pos := make(map[string]int, len(o.queue))
	for i, id := range o.queue {
		pos[id] = i + 1
	}
	byVM := make(map[string]*model.Task)
	tasks := make([]model.Task, 0, len(o.order))
	var running, completed int
	for _, id := range o.order {
		t := o.tasks[id]
		c := t.Clone()
		c.QueuePos = pos[id]
		tasks = append(tasks, c)
		if isTerminal(t.Phase) {
			completed++
		} else if t.Phase != model.PhaseQueued && t.Phase != model.PhaseMatching {
			running++
			if t.VMID != "" {
				byVM[t.VMID] = t
			}
		}
	}
	stats := model.Stats{
		QueueDepth: len(o.queue),
		Running:    running,
		Completed:  completed,
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
		stats.VMsTotal++
		switch vms[i].State {
		case model.VMReady, model.VMFree:
			stats.VMsReady++
		case model.VMAllocated:
			stats.VMsAllocated++
		case model.VMDegraded:
			stats.VMsDegraded++
		}
		if t := byVM[vms[i].VMID]; t != nil {
			vms[i].TaskID = t.ID
			vms[i].Service = t.Service
			vms[i].Alert = t.Alert
		}
	}

	mems, err := o.mem.Summaries(100)
	if err != nil {
		o.log.Printf("snapshot: memory summaries failed: %v", err)
	}
	if mems == nil {
		mems = []model.MemorySummary{}
	}

	return model.State{Stats: stats, VMs: vms, Tasks: tasks, Memory: mems}
}

// TaskDetail backs GET /api/task/:id — the trigger body, the agent transcript
// and the final analysis, none of which ride along in the list snapshot.
func (o *Orchestrator) TaskDetail(id string) (model.TaskDetail, error) {
	o.mu.Lock()
	task, ok := o.tasks[id]
	var snap model.Task
	if ok {
		snap = *task
	}
	o.mu.Unlock()

	if !ok {
		stored, err := o.mem.LoadTasks(1000)
		if err != nil {
			return model.TaskDetail{}, err
		}
		for _, t := range stored {
			if t.ID == id {
				snap, ok = t, true
				break
			}
		}
		if !ok {
			return model.TaskDetail{}, fmt.Errorf("no such task %q", id)
		}
	}

	events, err := o.mem.Events(id)
	if err != nil {
		o.log.Printf("task %s: load events failed: %v", id, err)
	}
	if len(events) == 0 {
		o.evMu.Lock()
		events = append([]model.AgentEvent{}, o.ring[id]...)
		o.evMu.Unlock()
	}
	return snap.Detail(events), nil
}

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
