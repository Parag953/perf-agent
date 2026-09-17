// Command mock-poold stands in for the real cluster pool manager (Poold) until
// it exists. It implements the /poold/lease, /poold/release, and /poold/status
// contract over an in-memory pool that runs the ready → allocated → degraded →
// (recycle) → ready lifecycle.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"
)

type vm struct {
	ID      string
	State   string // ready | allocated | degraded
	Host    string
	SSH     string
	Since   time.Time
	dirtyAt time.Time
}

type pool struct {
	mu      sync.Mutex
	vms     []*vm
	recycle time.Duration
}

func newPool(n int, recycle time.Duration) *pool {
	p := &pool{recycle: recycle}
	for i := 0; i < n; i++ {
		id := fmt.Sprintf("VM%d", 101+i)
		host := fmt.Sprintf("10.4.1.%d", 101+i)
		p.vms = append(p.vms, &vm{ID: id, State: "ready", Host: host, SSH: "agent@" + host, Since: time.Now()})
	}
	return p
}

// recycler flips degraded VMs back to ready after the recycle delay.
func (p *pool) recycler() {
	for range time.Tick(time.Second) {
		p.mu.Lock()
		for _, v := range p.vms {
			if v.State == "degraded" && time.Since(v.dirtyAt) >= p.recycle {
				v.State = "ready"
				v.Since = time.Now()
			}
		}
		p.mu.Unlock()
	}
}

func (p *pool) lease() (*vm, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, v := range p.vms {
		if v.State == "ready" || v.State == "free" {
			v.State = "allocated"
			v.Since = time.Now()
			return v, true
		}
	}
	return nil, false
}

func (p *pool) release(id string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, v := range p.vms {
		if v.ID == id {
			v.State = "degraded" // one task dirties the VM
			v.dirtyAt = time.Now()
			v.Since = time.Now()
			return true
		}
	}
	return false
}

func (p *pool) status() []map[string]any {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]map[string]any, 0, len(p.vms))
	for _, v := range p.vms {
		out = append(out, map[string]any{
			"vm_id": v.ID, "state": v.State, "host": v.Host,
			"since": v.Since.UTC().Format(time.RFC3339),
		})
	}
	return out
}

func main() {
	addr := flag.String("addr", ":9090", "listen address")
	size := flag.Int("size", 8, "pool size")
	recycle := flag.Duration("recycle", 10*time.Second, "degraded → ready delay")
	flag.Parse()

	p := newPool(*size, *recycle)
	go p.recycler()

	mux := http.NewServeMux()
	mux.HandleFunc("/poold/lease", func(w http.ResponseWriter, r *http.Request) {
		v, ok := p.lease()
		if !ok {
			writeJSON(w, http.StatusConflict, map[string]string{"error": "no_capacity"})
			return
		}
		log.Printf("lease -> %s", v.ID)
		writeJSON(w, http.StatusOK, map[string]string{
			"vm_id": v.ID, "host": v.Host, "ssh_target": v.SSH, "state": v.State,
		})
	})
	mux.HandleFunc("/poold/release", func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			VMID string `json:"vm_id"`
		}
		json.NewDecoder(r.Body).Decode(&body)
		if !p.release(body.VMID) {
			writeJSON(w, http.StatusNotFound, map[string]string{"error": "unknown_vm"})
			return
		}
		log.Printf("release <- %s (now degraded)", body.VMID)
		writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
	})
	mux.HandleFunc("/poold/status", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{"vms": p.status()})
	})

	log.Printf("mock-poold listening on %s (pool=%d, recycle=%s)", *addr, *size, *recycle)
	log.Fatal(http.ListenAndServe(*addr, mux))
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("content-type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}
