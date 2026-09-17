// Package api serves the webhook (trigger intake) and the read-only dashboard
// contract: GET /api/state, GET /api/stream (SSE), GET /api/memory/:id.
package api

import (
	_ "embed"
	"encoding/json"
	"log"
	"net/http"
	"regexp"
	"strconv"
	"strings"

	"github.com/Parag953/perf-agent/internal/model"
)

// Engine is what the API needs from the orchestrator.
type Engine interface {
	Submit(model.Trigger) model.Task
	Snapshot() model.State
	GetMemory(id int64) (model.Memory, error)
}

//go:embed dashboard.html
var dashboardHTML []byte

type Server struct {
	eng Engine
	hub *Hub
	log *log.Logger
}

func NewServer(eng Engine, hub *Hub, logger *log.Logger) *Server {
	return &Server{eng: eng, hub: hub, log: logger}
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/webhook", s.handleWebhook)
	mux.HandleFunc("/api/state", s.handleState)
	mux.HandleFunc("/api/stream", s.handleStream)
	mux.HandleFunc("/api/memory/", s.handleMemory)
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { w.Write([]byte("ok")) })
	mux.HandleFunc("/", s.handleRoot)
	return mux
}

func (s *Server) handleRoot(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("content-type", "text/html; charset=utf-8")
	w.Write(dashboardHTML)
}

// handleWebhook accepts a trigger. It takes either our own {service, alert,
// payload} shape or a raw Datadog-style body, resolving the service from it.
func (s *Server) handleWebhook(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", http.StatusMethodNotAllowed)
		return
	}
	defer r.Body.Close()
	var raw map[string]json.RawMessage
	if err := json.NewDecoder(r.Body).Decode(&raw); err != nil {
		http.Error(w, "bad json: "+err.Error(), http.StatusBadRequest)
		return
	}

	trigger := model.Trigger{
		Service: extractService(raw),
		Alert:   extractAlert(raw),
	}
	if p, ok := raw["payload"]; ok {
		trigger.Payload = p
	} else {
		b, _ := json.Marshal(raw)
		trigger.Payload = b
	}

	task := s.eng.Submit(trigger)
	s.log.Printf("accepted trigger: service=%s alert=%q -> %s", trigger.Service, trigger.Alert, task.ID)
	writeJSON(w, http.StatusAccepted, map[string]string{"task_id": task.ID, "service": trigger.Service})
}

func (s *Server) handleState(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, s.eng.Snapshot())
}

func (s *Server) handleStream(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}
	w.Header().Set("content-type", "text/event-stream")
	w.Header().Set("cache-control", "no-cache")
	w.Header().Set("connection", "keep-alive")
	w.Header().Set("access-control-allow-origin", "*")

	ch := s.hub.Subscribe()
	defer s.hub.Unsubscribe(ch)

	// Send the current snapshot immediately so a fresh client isn't blank.
	if b, err := json.Marshal(s.eng.Snapshot()); err == nil {
		writeSSE(w, b)
		flusher.Flush()
	}

	for {
		select {
		case <-r.Context().Done():
			return
		case b, ok := <-ch:
			if !ok {
				return
			}
			writeSSE(w, b)
			flusher.Flush()
		}
	}
}

func (s *Server) handleMemory(w http.ResponseWriter, r *http.Request) {
	idStr := strings.TrimPrefix(r.URL.Path, "/api/memory/")
	id, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil {
		http.Error(w, "bad id", http.StatusBadRequest)
		return
	}
	mem, err := s.eng.GetMemory(id)
	if err != nil {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	writeJSON(w, http.StatusOK, mem)
}

// ---- trigger parsing ----

var kubeDeployRe = regexp.MustCompile(`kube_deployment:([A-Za-z0-9_.-]+)`)
var deploymentRe = regexp.MustCompile(`deployment ([A-Za-z0-9_.-]+)`)

// extractService resolves the affected service from a webhook body, mirroring
// the resolution order the original app.py used.
func extractService(raw map[string]json.RawMessage) string {
	if v := asString(raw["service"]); v != "" {
		return v
	}
	// tags may be a list or a comma string; scan for kube_deployment:.
	blob := string(raw["tags"]) + " " + string(raw["body"]) + " " + string(raw["title"])
	if m := kubeDeployRe.FindStringSubmatch(blob); len(m) == 2 {
		return m[1]
	}
	if m := deploymentRe.FindStringSubmatch(blob); len(m) == 2 {
		return m[1]
	}
	return "unknown"
}

func extractAlert(raw map[string]json.RawMessage) string {
	for _, k := range []string{"alert", "title", "alert_title", "event_title"} {
		if v := asString(raw[k]); v != "" {
			return v
		}
	}
	return "alert"
}

func asString(r json.RawMessage) string {
	if len(r) == 0 {
		return ""
	}
	var s string
	if err := json.Unmarshal(r, &s); err == nil {
		return strings.TrimSpace(s)
	}
	return ""
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("content-type", "application/json")
	w.Header().Set("access-control-allow-origin", "*")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

func writeSSE(w http.ResponseWriter, b []byte) {
	w.Write([]byte("data: "))
	w.Write(b)
	w.Write([]byte("\n\n"))
}
