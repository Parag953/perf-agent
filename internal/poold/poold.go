// Package poold is the client for the cluster pool manager (Poold). Poold owns
// the VMs and their lifecycle; the orchestrator only leases, releases, and reads
// status. The real service is mocked by cmd/mock-poold for now — this client
// depends only on the HTTP contract, so it swaps in unchanged later.
package poold

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/Parag953/perf-agent/internal/model"
)

// ErrNoCapacity is returned by Lease when the pool has no leasable VM (HTTP 409).
// The orchestrator treats it as "wait and retry", not a hard failure.
var ErrNoCapacity = errors.New("poold: no capacity")

// Lease is what Poold hands back for one leased VM.
type Lease struct {
	VMID      string `json:"vm_id"`
	Host      string `json:"host"`
	SSHTarget string `json:"ssh_target"`
	State     string `json:"state"`
}

// Client is the pool manager contract the orchestrator depends on.
type Client interface {
	Lease(ctx context.Context) (*Lease, error)
	Release(ctx context.Context, vmID string) error
	Status(ctx context.Context) ([]model.VM, error)
}

type httpClient struct {
	base string
	hc   *http.Client
}

// New returns an HTTP-backed Poold client rooted at base (e.g. http://localhost:9090).
func New(base string) Client {
	return &httpClient{base: base, hc: &http.Client{Timeout: 10 * time.Second}}
}

func (c *httpClient) Lease(ctx context.Context) (*Lease, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.base+"/poold/lease", nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.hc.Do(req)
	if err != nil {
		return nil, fmt.Errorf("poold lease: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusConflict {
		return nil, ErrNoCapacity
	}
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("poold lease: status %d: %s", resp.StatusCode, string(body))
	}
	var l Lease
	if err := json.NewDecoder(resp.Body).Decode(&l); err != nil {
		return nil, fmt.Errorf("poold lease decode: %w", err)
	}
	return &l, nil
}

func (c *httpClient) Release(ctx context.Context, vmID string) error {
	body, _ := json.Marshal(map[string]string{"vm_id": vmID})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.base+"/poold/release", bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("content-type", "application/json")
	resp, err := c.hc.Do(req)
	if err != nil {
		return fmt.Errorf("poold release: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("poold release: status %d: %s", resp.StatusCode, string(b))
	}
	return nil
}

func (c *httpClient) Status(ctx context.Context) ([]model.VM, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.base+"/poold/status", nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.hc.Do(req)
	if err != nil {
		return nil, fmt.Errorf("poold status: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("poold status: %d: %s", resp.StatusCode, string(b))
	}
	var out struct {
		VMs []model.VM `json:"vms"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, fmt.Errorf("poold status decode: %w", err)
	}
	return out.VMs, nil
}
