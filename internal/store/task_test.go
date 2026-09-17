package store

import (
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	"github.com/Parag953/perf-agent/internal/model"
)

func open(t *testing.T) *Store {
	t.Helper()
	s, err := Open(filepath.Join(t.TempDir(), "t.db"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func TestSaveTaskRoundTripsPayloadAnalysisAndSummary(t *testing.T) {
	s := open(t)
	started := time.Now().UTC().Truncate(time.Second)
	task := model.Task{
		ID:         "t-1",
		Service:    "tachyon",
		Alert:      "heap growth",
		Phase:      model.PhaseDone,
		VMID:       "VM101",
		Outcome:    model.OutcomePROpened,
		PRURL:      "https://example.test/pull/1",
		EnqueuedAt: started,
		StartedAt:  &started,
		Payload:    json.RawMessage(`{"tags":["kube_deployment:tachyon"]}`),
		Analysis:   "full analysis text",
		Summary:    "memoize the cache",
	}
	if err := s.SaveTask(task); err != nil {
		t.Fatalf("save: %v", err)
	}

	got, err := s.LoadTasks(10)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("got %d tasks, want 1", len(got))
	}
	g := got[0]
	if g.ID != "t-1" || g.Service != "tachyon" || g.Phase != model.PhaseDone {
		t.Errorf("identity: %+v", g)
	}
	if g.Analysis != "full analysis text" {
		t.Errorf("analysis = %q", g.Analysis)
	}
	if g.Summary != "memoize the cache" {
		t.Errorf("summary = %q", g.Summary)
	}
	if string(g.Payload) != `{"tags":["kube_deployment:tachyon"]}` {
		t.Errorf("payload = %s", g.Payload)
	}
	if g.StartedAt == nil || !g.StartedAt.Equal(started) {
		t.Errorf("started_at = %v, want %v", g.StartedAt, started)
	}
}

func TestSaveTaskUpdatesInPlace(t *testing.T) {
	s := open(t)
	task := model.Task{ID: "t-1", Service: "tachyon", Phase: model.PhaseQueued, EnqueuedAt: time.Now().UTC()}
	if err := s.SaveTask(task); err != nil {
		t.Fatalf("save: %v", err)
	}
	task.Phase = model.PhaseRunning
	task.VMID = "VM102"
	if err := s.SaveTask(task); err != nil {
		t.Fatalf("resave: %v", err)
	}

	got, err := s.LoadTasks(10)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("got %d tasks, want 1 after update", len(got))
	}
	if got[0].Phase != model.PhaseRunning || got[0].VMID != "VM102" {
		t.Errorf("phase=%s vm=%s", got[0].Phase, got[0].VMID)
	}
}

func TestLoadTasksReturnsOldestFirstWithinLimit(t *testing.T) {
	s := open(t)
	base := time.Now().UTC()
	for i, id := range []string{"t-1", "t-2", "t-3"} {
		err := s.SaveTask(model.Task{ID: id, Service: "s", Phase: model.PhaseDone, EnqueuedAt: base.Add(time.Duration(i) * time.Minute)})
		if err != nil {
			t.Fatalf("save %s: %v", id, err)
		}
	}
	got, err := s.LoadTasks(2)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("got %d tasks, want 2", len(got))
	}
	if got[0].ID != "t-2" || got[1].ID != "t-3" {
		t.Errorf("order = %s,%s want t-2,t-3", got[0].ID, got[1].ID)
	}
}

func TestAppendEventReadsBackInSequenceOrder(t *testing.T) {
	s := open(t)
	now := time.Now().UTC().Truncate(time.Second)
	for _, e := range []model.AgentEvent{
		{TaskID: "t-1", Seq: 1, At: now, Kind: model.EventText, Text: "looking at the heap"},
		{TaskID: "t-1", Seq: 2, At: now, Kind: model.EventTool, Tool: "Bash", Text: "rg memoize"},
		{TaskID: "t-2", Seq: 1, At: now, Kind: model.EventText, Text: "other task"},
	} {
		if err := s.AppendEvent(e); err != nil {
			t.Fatalf("append: %v", err)
		}
	}

	got, err := s.Events("t-1")
	if err != nil {
		t.Fatalf("events: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("got %d events for t-1, want 2", len(got))
	}
	if got[0].Seq != 1 || got[0].Kind != model.EventText || got[0].Text != "looking at the heap" {
		t.Errorf("first = %+v", got[0])
	}
	if got[1].Tool != "Bash" {
		t.Errorf("second tool = %q", got[1].Tool)
	}
}

func TestEventsIsEmptyForUnknownTask(t *testing.T) {
	s := open(t)
	got, err := s.Events("nope")
	if err != nil {
		t.Fatalf("events: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("got %d events, want 0", len(got))
	}
}
