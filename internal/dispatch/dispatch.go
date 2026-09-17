// Package dispatch runs the `claude -p` investigation on a leased VM and parses
// what comes back. In production the SSH dispatcher ships the command to the VM;
// the mock dispatcher synthesizes a plausible analysis for local demos and tests.
package dispatch

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os/exec"
	"strings"

	"github.com/Parag953/perf-agent/internal/config"
	"github.com/Parag953/perf-agent/internal/model"
)

// Dispatcher runs the investigation prompt against a VM and returns the agent's
// analysis text (its stdout `result`).
type Dispatcher interface {
	Run(ctx context.Context, sshTarget, prompt string) (string, error)
	Mode() string
}

// New selects a dispatcher from config: "ssh" or "mock".
func New(cfg config.Config) Dispatcher {
	if cfg.DispatchMode == "mock" {
		return mockDispatcher{}
	}
	return sshDispatcher{
		key:          cfg.SSHKey,
		claudeBin:    cfg.ClaudeBin,
		mcpConfig:    cfg.MCPConfig,
		allowedTools: cfg.AllowedTools,
	}
}

// ---- SSH dispatcher (production) ----

type sshDispatcher struct {
	key          string
	claudeBin    string
	mcpConfig    string
	allowedTools string
}

func (d sshDispatcher) Mode() string { return "ssh" }

func (d sshDispatcher) Run(ctx context.Context, sshTarget, prompt string) (string, error) {
	if sshTarget == "" {
		return "", fmt.Errorf("ssh dispatch: empty ssh target")
	}
	// Remote command: claude reads the prompt from stdin (no prompt arg), so we
	// pipe the (large) prompt over ssh stdin rather than into argv.
	remote := fmt.Sprintf("%s -p --output-format json --mcp-config %s --allowedTools %s",
		d.claudeBin, shellQuote(d.mcpConfig), shellQuote(d.allowedTools))

	args := []string{}
	if d.key != "" {
		args = append(args, "-i", d.key)
	}
	args = append(args,
		"-o", "BatchMode=yes",
		"-o", "StrictHostKeyChecking=accept-new",
		sshTarget, remote,
	)
	cmd := exec.CommandContext(ctx, "ssh", args...)
	cmd.Stdin = strings.NewReader(prompt)
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("ssh dispatch: %v: %s", err, strings.TrimSpace(stderr.String()))
	}
	return parseClaudeJSON(stdout.Bytes()), nil
}

// parseClaudeJSON pulls `.result` out of `claude --output-format json`, falling
// back to raw stdout if it isn't the expected envelope.
func parseClaudeJSON(b []byte) string {
	var out struct {
		Result string `json:"result"`
	}
	if err := json.Unmarshal(b, &out); err == nil && out.Result != "" {
		return out.Result
	}
	return strings.TrimSpace(string(b))
}

func shellQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
}

// ---- Mock dispatcher (demo / tests) ----

type mockDispatcher struct{}

func (mockDispatcher) Mode() string { return "mock" }

func (mockDispatcher) Run(_ context.Context, _, prompt string) (string, error) {
	// Return a plausible, self-consistent analysis so the full pipeline
	// (collect → pr_check → distill → memory) can be exercised without a VM.
	return "Root cause: the resources cache is rebuilt on every request instead of " +
		"being reused across the request lifetime, so heap allocations churn under load. " +
		"Fix: memoize the query result for the duration of the request.\n" +
		"PR_URL: https://github.com/andromedasec/voyager/pull/9999", nil
}

// ---- Output parsing ----

// ExtractPRURL finds a PR link the agent opened, via the PR_URL: marker or a
// raw github pull URL anywhere in the reply.
func ExtractPRURL(response string) string {
	for _, ln := range strings.Split(response, "\n") {
		s := strings.TrimSpace(ln)
		if v, ok := strings.CutPrefix(s, "PR_URL:"); ok {
			return strings.TrimSpace(v)
		}
	}
	for _, f := range strings.Fields(response) {
		if strings.Contains(f, "github.com/") && strings.Contains(f, "/pull/") {
			return strings.Trim(f, "()<>\"'.,")
		}
	}
	return ""
}

// ExtractNeedQueries returns everything after a NEED_QUERIES: marker, if the
// agent paused to ask for DB data, else "".
func ExtractNeedQueries(response string) string {
	const marker = "NEED_QUERIES:"
	if i := strings.Index(response, marker); i >= 0 {
		return strings.TrimSpace(response[i+len(marker):])
	}
	return ""
}

// BuildInvestigationPrompt briefs the agent as an on-call engineer and lists the
// resources it may reach (paths are on the VM). Ported from the original app.py.
// priorMemory, when non-empty, is the closest known issue injected as a hint.
func BuildInvestigationPrompt(cfg config.Config, t model.Trigger, priorMemory string) string {
	payload := string(t.Payload)
	if payload == "" {
		payload = "{}"
	}
	prior := ""
	if priorMemory != "" {
		prior = fmt.Sprintf(`
PRIOR KNOWLEDGE — a similar issue was investigated before. Confirm whether it
applies here before assuming it does; do not blindly trust it:
%s
`, priorMemory)
	}

	return fmt.Sprintf(`You are an on-call Go engineer for the Voyager platform. A Datadog alert just fired.
Investigate and resolve it the way a developer would: form hypotheses, gather ONLY the
evidence you actually need, and drive to a concrete root cause. Do not gather data you don't need.

Alert: %s
Service: %s
Alert payload:
%s
%s
You have access to the following. Use each ONLY if your investigation calls for it:

1. SOURCE CODE — the full Voyager monorepo is checked out locally at %s
   (this service: %s/services/%s). Use your Bash tool to grep and read files
   (rg, cat, ls, git log, etc.).

2. pprof PROFILES — production heap/goroutine captures live in S3, one folder per capture:
       s3://%s/%s/<TIMESTAMP>/heap.dump       (Go heap profile, protobuf)
       s3://%s/%s/<TIMESTAMP>/memstats.json   (runtime.MemStats)
       s3://%s/%s/<TIMESTAMP>/stacktrace      (full goroutine dump)
   You have AWS credentials and the `+"`aws`"+` and `+"`go`"+` CLIs. Fetch a profile ONLY if warranted:
       aws s3 ls s3://%s/%s/
       aws s3 cp s3://%s/%s/<TIMESTAMP>/heap.dump /tmp/heap.dump
       go tool pprof -top -sample_index=inuse_space /tmp/heap.dump
   Pick the most recent capture unless the alert points elsewhere.

3. DATADOG — via MCP tools (metrics, logs, traces). ANDROMEDA PLATFORM — via
   mcp__andromeda_gateway__* tools for tenant/provider/platform data.

4. DATABASES — you CANNOT query PostgreSQL or Neo4j directly. If you need data from them,
   output a block that starts with NEED_QUERIES: containing the exact SQL and Cypher, then STOP.

Deliver a clear root-cause analysis with concrete, code-level fixes.

FIX & PR:
If — and only if — you are confident in a concrete, well-scoped code fix, implement it and
open a DRAFT pull request (do NOT merge it yourself):
  cd %s
  git checkout -b agent/%s-<short-slug>
  git add -A && git commit -m "<clear message explaining the fix>"
  git push -u origin HEAD
  gh pr create --draft --repo %s --base main --title "<concise title>" --body "<root cause + fix>"
On the LAST line of your reply, output the PR link as:
PR_URL: <the URL gh printed>
If you are not confident enough to write a fix, do not open a PR — just report the analysis.`,
		t.Alert, t.Service, payload, prior,
		cfg.VoyagerPath, cfg.VoyagerPath, t.Service,
		cfg.S3Bucket, t.Service, cfg.S3Bucket, t.Service, cfg.S3Bucket, t.Service,
		cfg.S3Bucket, t.Service, cfg.S3Bucket, t.Service,
		cfg.VoyagerPath, t.Service, cfg.VoyagerRepo,
	)
}
