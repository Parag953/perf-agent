package orchestrator

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Parag953/perf-agent/internal/config"
	"github.com/Parag953/perf-agent/internal/dispatch"
	"github.com/Parag953/perf-agent/internal/model"
	"github.com/Parag953/perf-agent/internal/store"
)

// A cold snapshot (no poold status yet, empty memory) must still marshal every
// collection as a JSON array, never null — consumers do state.vms.map(...).
func TestSnapshot_NeverNull(t *testing.T) {
	dir := t.TempDir()
	mem, err := store.Open(filepath.Join(dir, "memory.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer mem.Close()

	cfg := config.Config{Workers: 1, DispatchMode: "mock"}
	o := New(cfg, &fakePoold{}, mem, fakeLLM{}, dispatch.New(cfg), nil, nil, log.New(io.Discard, "", 0))

	snap := o.Snapshot()
	if snap.VMs == nil || snap.Tasks == nil || snap.Memory == nil {
		t.Fatalf("snapshot slices must be non-nil: vms=%v tasks=%v memory=%v", snap.VMs == nil, snap.Tasks == nil, snap.Memory == nil)
	}
	b, err := json.Marshal(snap)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{`"vms":[]`, `"tasks":[]`, `"memory":[]`} {
		if !strings.Contains(string(b), want) {
			t.Fatalf("expected %s in %s", want, b)
		}
	}
}

// evictLocked bounds retained terminal tasks while keeping all in-flight ones,
// and the cumulative total is unaffected.
func TestEvictLocked(t *testing.T) {
	o := &Orchestrator{tasks: map[string]*model.Task{}}
	for i := 0; i < maxTerminalTasks+25; i++ {
		id := fmt.Sprintf("done-%d", i)
		o.tasks[id] = &model.Task{ID: id, Phase: model.PhaseDone}
		o.order = append(o.order, id)
	}
	for i := 0; i < 3; i++ { // in-flight tasks must survive eviction
		id := fmt.Sprintf("run-%d", i)
		o.tasks[id] = &model.Task{ID: id, Phase: model.PhaseRunning}
		o.order = append(o.order, id)
	}

	o.evictLocked()

	terminal, running := 0, 0
	for _, id := range o.order {
		switch o.tasks[id].Phase {
		case model.PhaseDone:
			terminal++
		case model.PhaseRunning:
			running++
		}
	}
	if terminal != maxTerminalTasks {
		t.Fatalf("retained terminal = %d, want %d", terminal, maxTerminalTasks)
	}
	if running != 3 {
		t.Fatalf("in-flight tasks must all survive: running = %d, want 3", running)
	}
	if len(o.tasks) != maxTerminalTasks+3 {
		t.Fatalf("registry size = %d, want %d", len(o.tasks), maxTerminalTasks+3)
	}
	// Oldest terminal tasks are the ones dropped.
	if _, ok := o.tasks["done-0"]; ok {
		t.Fatal("oldest terminal task should have been evicted")
	}
}
