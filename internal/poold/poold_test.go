package poold

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Parag953/perf-agent/internal/model"
)

const statusBody = `{"vms":[
  {"vm_id":"VM101","name":"andro-a","state":"ready","degraded_reason":null,
   "host":"100.75.158.42","ssh_target":"andro-1@100.75.158.42","since":"2026-09-17T11:59:33Z"},
  {"vm_id":"VM102","name":"andro-b","state":"degraded","degraded_reason":"preparing",
   "host":"100.73.230.48","ssh_target":"andro-2@100.73.230.48","since":"2026-09-17T11:46:56Z"}
]}`

const boxesBody = `[
  {"name":"andro-a","vmid":101,"state":"ready"},
  {"name":"andro-b","vmid":102,"state":"preparing",
   "prepare":{"step":"gate","attempt":1,"elapsed_s":118.4},
   "gate":{"sample":"deploys=13 pods_not_ready=2 helm_bad=0","sampled_at":1758100000.0}}
]`

func serve(t *testing.T, boxes string) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("/poold/status", func(w http.ResponseWriter, _ *http.Request) {
		w.Write([]byte(statusBody))
	})
	mux.HandleFunc("/boxes", func(w http.ResponseWriter, _ *http.Request) {
		if boxes == "" {
			http.Error(w, "boom", http.StatusInternalServerError)
			return
		}
		w.Write([]byte(boxes))
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

func TestStatusCarriesNameAndDegradedReason(t *testing.T) {
	srv := serve(t, boxesBody)
	vms, err := New(srv.URL).Status(context.Background())
	if err != nil {
		t.Fatalf("status: %v", err)
	}
	if len(vms) != 2 {
		t.Fatalf("got %d vms, want 2", len(vms))
	}
	if vms[0].Name != "andro-a" {
		t.Errorf("name = %q, want andro-a", vms[0].Name)
	}
	if vms[1].State != model.VMDegraded {
		t.Errorf("state = %q, want degraded", vms[1].State)
	}
	if vms[1].Reason != "preparing" {
		t.Errorf("degraded_reason = %q, want preparing", vms[1].Reason)
	}
}

func TestStatusMergesPrepareProgressFromBoxes(t *testing.T) {
	srv := serve(t, boxesBody)
	vms, err := New(srv.URL).Status(context.Background())
	if err != nil {
		t.Fatalf("status: %v", err)
	}
	if vms[0].Prepare != nil {
		t.Errorf("a ready box should carry no prepare progress, got %+v", vms[0].Prepare)
	}
	p := vms[1].Prepare
	if p == nil {
		t.Fatal("preparing box should carry prepare progress")
	}
	if p.Step != "gate" {
		t.Errorf("step = %q, want gate", p.Step)
	}
	if p.Elapsed != 118.4 {
		t.Errorf("elapsed = %v, want 118.4", p.Elapsed)
	}
	if p.Sample != "deploys=13 pods_not_ready=2 helm_bad=0" {
		t.Errorf("gate sample = %q", p.Sample)
	}
}

func TestStatusStillWorksWhenBoxesEndpointFails(t *testing.T) {
	srv := serve(t, "")
	vms, err := New(srv.URL).Status(context.Background())
	if err != nil {
		t.Fatalf("status must not fail when /boxes is unavailable: %v", err)
	}
	if len(vms) != 2 {
		t.Fatalf("got %d vms, want 2", len(vms))
	}
	if vms[1].Prepare != nil {
		t.Errorf("no prepare detail expected, got %+v", vms[1].Prepare)
	}
}
