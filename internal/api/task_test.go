package api

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Parag953/perf-agent/internal/model"
)

type fakeEngine struct {
	detail model.TaskDetail
	found  bool
	events chan model.AgentEvent
}

func (f *fakeEngine) Submit(model.Trigger) model.Task { return model.Task{} }
func (f *fakeEngine) Snapshot() model.State           { return model.State{} }
func (f *fakeEngine) GetMemory(int64) (model.Memory, error) {
	return model.Memory{}, fmt.Errorf("nope")
}

func (f *fakeEngine) TaskDetail(id string) (model.TaskDetail, error) {
	if !f.found {
		return model.TaskDetail{}, fmt.Errorf("no such task %q", id)
	}
	return f.detail, nil
}

func (f *fakeEngine) SubscribeTask(string) (<-chan model.AgentEvent, func()) {
	return f.events, func() {}
}

func newTestServer(t *testing.T, eng *fakeEngine) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(NewServer(eng, NewHub(), log.New(io.Discard, "", 0)).Handler())
	t.Cleanup(srv.Close)
	return srv
}

func TestTaskDetailEndpointReturnsPayloadAnalysisAndEvents(t *testing.T) {
	eng := &fakeEngine{
		found: true,
		detail: model.TaskDetail{
			Task:     model.Task{ID: "t-1", Service: "tachyon", Phase: model.PhaseDone, PRURL: "https://x/pull/3"},
			Payload:  json.RawMessage(`{"tags":["kube_deployment:tachyon"]}`),
			Analysis: "root cause: unbounded cache",
			Summary:  "memoize it",
			Events: []model.AgentEvent{
				{TaskID: "t-1", Seq: 1, Kind: model.EventTool, Tool: "Bash", Text: "rg cache"},
			},
		},
	}
	srv := newTestServer(t, eng)

	resp, err := http.Get(srv.URL + "/api/task/t-1")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	var got model.TaskDetail
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatal(err)
	}
	if got.ID != "t-1" || got.PRURL != "https://x/pull/3" {
		t.Errorf("task fields = %+v", got.Task)
	}
	if got.Analysis != "root cause: unbounded cache" {
		t.Errorf("analysis = %q", got.Analysis)
	}
	if string(got.Payload) != `{"tags":["kube_deployment:tachyon"]}` {
		t.Errorf("payload = %s", got.Payload)
	}
	if len(got.Events) != 1 || got.Events[0].Tool != "Bash" {
		t.Errorf("events = %+v", got.Events)
	}
}

func TestTaskDetailEndpointIs404ForUnknownTask(t *testing.T) {
	srv := newTestServer(t, &fakeEngine{found: false})
	resp, err := http.Get(srv.URL + "/api/task/nope")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("status = %d, want 404", resp.StatusCode)
	}
}

func TestTaskStreamEndpointPushesAgentEventsAsSSE(t *testing.T) {
	events := make(chan model.AgentEvent, 4)
	eng := &fakeEngine{found: true, events: events}
	srv := newTestServer(t, eng)

	events <- model.AgentEvent{TaskID: "t-1", Seq: 1, Kind: model.EventThinking, Text: "checking allocations"}

	req, _ := http.NewRequest(http.MethodGet, srv.URL+"/api/task/t-1/stream", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if ct := resp.Header.Get("content-type"); !strings.HasPrefix(ct, "text/event-stream") {
		t.Fatalf("content-type = %q, want text/event-stream", ct)
	}

	line := make(chan string, 1)
	go func() {
		sc := bufio.NewScanner(resp.Body)
		for sc.Scan() {
			if strings.HasPrefix(sc.Text(), "data: ") {
				line <- strings.TrimPrefix(sc.Text(), "data: ")
				return
			}
		}
	}()

	select {
	case l := <-line:
		var e model.AgentEvent
		if err := json.Unmarshal([]byte(l), &e); err != nil {
			t.Fatalf("frame is not an agent event: %v (%s)", err, l)
		}
		if e.Kind != model.EventThinking || e.Text != "checking allocations" {
			t.Errorf("event = %+v", e)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("no SSE frame arrived")
	}
}
