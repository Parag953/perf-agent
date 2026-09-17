// Package dispatch runs the `claude -p` investigation on a leased VM and parses
// what comes back. In production the SSH dispatcher ships the command to the VM;
// the mock dispatcher synthesizes a plausible analysis for local demos and tests.
package dispatch

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"sort"
	"strings"
	"time"

	"github.com/Parag953/perf-agent/internal/config"
	"github.com/Parag953/perf-agent/internal/model"
)

// Emit receives each agent step as it happens, for the live dashboard view. It
// may be nil (no streaming consumer).
type Emit func(model.AgentEvent)

// Dispatcher runs the investigation prompt against a VM, streaming each step to
// emit, and returns the agent's final analysis text (its `result`).
type Dispatcher interface {
	Run(ctx context.Context, sshTarget, prompt string, emit Emit) (string, error)
	Mode() string
}

func fire(emit Emit, kind, tool, summary string) {
	if emit != nil {
		emit(model.AgentEvent{TS: time.Now().UTC(), Kind: kind, Tool: tool, Summary: summary})
	}
}

// New selects a dispatcher from config: "ssh" or "mock".
func New(cfg config.Config) Dispatcher {
	if cfg.DispatchMode == "mock" {
		return mockDispatcher{}
	}
	// Read the mcp.json to ship to the VM from the box's local filesystem. The
	// snapshot the pool VMs boot from has no mcp.json, so the orchestrator writes
	// it during bootstrap. Missing/unreadable => fall back to cfg.MCPConfig path.
	var mcpContent string
	if b, err := os.ReadFile(cfg.MCPConfigSrc); err == nil {
		mcpContent = string(b)
	}

	return sshDispatcher{
		key:          cfg.SSHKey,
		claudeBin:    cfg.ClaudeBin,
		mcpConfig:    cfg.MCPConfig,
		mcpContent:   mcpContent,
		allowedTools: cfg.AllowedTools,
		bootstrap:    cfg.VMBootstrap,
		ghVersion:    cfg.GHVersion,
		stream:       cfg.AgentStream,
		secretEnv:    secretEnv(cfg),
	}
}

// ---- SSH dispatcher (production) ----

// remoteCredsPath / remoteMCPPath are where the bootstrap step writes the secrets
// file and mcp.json on the VM. They use $HOME so they resolve in the non-login
// shell ssh spawns; both are double-quoted for embedding in the remote command.
const (
	remoteCredsPath = `"$HOME/.config/agent/env"`
	remoteMCPPath   = `"$HOME/.config/agent/mcp.json"`
)

type sshDispatcher struct {
	key          string
	claudeBin    string
	mcpConfig    string
	mcpContent   string // mcp.json to write on the VM; "" => use mcpConfig path as-is
	allowedTools string

	bootstrap bool              // provision the VM before the run
	ghVersion string            // gh version for the tarball fallback
	stream    bool              // stream-json (live steps) vs buffered json
	secretEnv map[string]string // forwarded into the run; never logged
}

func (d sshDispatcher) Mode() string { return "ssh" }

func (d sshDispatcher) Run(ctx context.Context, sshTarget, prompt string, emit Emit) (string, error) {
	if sshTarget == "" {
		return "", fmt.Errorf("ssh dispatch: empty ssh target")
	}

	// 1. Provision the (blank) VM: install claude/gh if missing and drop the
	//    secrets file. Idempotent — a pre-baked image makes this a fast no-op.
	if d.bootstrap {
		fire(emit, "provision", "", "provisioning VM (claude/gh/aws + secrets)…")
		if err := d.doBootstrap(ctx, sshTarget); err != nil {
			return "", err
		}
		fire(emit, "provision", "", "VM provisioned")
	}

	// 2. Investigation run. Source the secrets file (so CLAUDE_CODE_OAUTH_TOKEN,
	//    GH_TOKEN, AWS_*, DD_* are in the agent's env — claude expands the
	//    ${DD_API_KEY} refs in mcp.json from there), put ~/.local/bin on PATH so
	//    freshly-installed claude/gh/aws resolve, then run. claude reads the
	//    (large) prompt from stdin, so we pipe it over ssh stdin, not argv.
	format := "json"
	if d.stream {
		format = "stream-json --verbose"
	}
	mcpArg := shellQuote(d.mcpConfig)
	if d.mcpContent != "" {
		mcpArg = remoteMCPPath // bootstrap wrote it here; already double-quoted
	}
	remote := fmt.Sprintf(
		`set -a; [ -f %s ] && . %s; set +a; export PATH="$HOME/.local/bin:$PATH"; %s -p --output-format %s --mcp-config %s --allowedTools %s`,
		remoteCredsPath, remoteCredsPath, d.claudeBin, format, mcpArg, shellQuote(d.allowedTools))

	if !d.stream {
		stdout, stderr, err := d.runSSH(ctx, sshTarget, remote, prompt)
		if err != nil {
			return "", fmt.Errorf("ssh dispatch: %v: %s", err, strings.TrimSpace(stderr))
		}
		return parseClaudeJSON([]byte(stdout)), nil
	}
	return d.runSSHStream(ctx, sshTarget, remote, prompt, emit)
}

// runSSH executes one remote command over ssh, piping stdin to it, and returns
// its stdout/stderr. stdin carries either the bootstrap script or the prompt —
// never a secret in argv (secrets travel in the bootstrap script's stdin).
func (d sshDispatcher) runSSH(ctx context.Context, sshTarget, remote, stdin string) (string, string, error) {
	cmd := d.sshCmd(ctx, sshTarget, remote, stdin)
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	err := cmd.Run()
	return stdout.String(), stderr.String(), err
}

// runSSHStream runs claude with --output-format stream-json, parsing each NDJSON
// line into an AgentEvent as it arrives (emit) and returning the final result.
func (d sshDispatcher) runSSHStream(ctx context.Context, sshTarget, remote, stdin string, emit Emit) (string, error) {
	cmd := d.sshCmd(ctx, sshTarget, remote, stdin)
	stdoutPipe, err := cmd.StdoutPipe()
	if err != nil {
		return "", err
	}
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Start(); err != nil {
		return "", err
	}

	var result string
	sc := bufio.NewScanner(stdoutPipe)
	sc.Buffer(make([]byte, 0, 64*1024), 8*1024*1024) // tool_result lines can be large
	for sc.Scan() {
		evs, res, gotResult := streamEvents(sc.Bytes())
		for _, ev := range evs {
			if emit != nil {
				ev.TS = time.Now().UTC()
				emit(ev)
			}
		}
		if gotResult {
			result = res
		}
	}
	if err := sc.Err(); err != nil {
		fire(emit, "error", "", "stream read error: "+truncate(err.Error(), 200))
	}
	if err := cmd.Wait(); err != nil {
		return "", fmt.Errorf("ssh dispatch: %v: %s", err, strings.TrimSpace(stderr.String()))
	}
	return result, nil
}

func (d sshDispatcher) sshCmd(ctx context.Context, sshTarget, remote, stdin string) *exec.Cmd {
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
	cmd.Stdin = strings.NewReader(stdin)
	return cmd
}

// doBootstrap ships the provisioning script to the VM over ssh stdin (so the
// secrets it carries never appear in argv or the process list).
func (d sshDispatcher) doBootstrap(ctx context.Context, sshTarget string) error {
	stdout, stderr, err := d.runSSH(ctx, sshTarget, "bash -s", d.bootstrapScript())
	if err != nil {
		return fmt.Errorf("ssh bootstrap: %v: %s", err, strings.TrimSpace(stderr))
	}
	if !strings.Contains(stdout, "BOOTSTRAP_OK") {
		return fmt.Errorf("ssh bootstrap: incomplete: %s", strings.TrimSpace(stderr))
	}
	return nil
}

// bootstrapScript builds the idempotent provisioning script: write the secrets
// to a 0600 env file, install claude and gh if absent, and wire gh as git's
// credential helper. Every install is guarded by `command -v` so a VM that
// already has the tooling pays only the cost of the checks.
func (d sshDispatcher) bootstrapScript() string {
	var env strings.Builder
	keys := make([]string, 0, len(d.secretEnv))
	for k := range d.secretEnv {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		fmt.Fprintf(&env, "export %s='%s'\n", k, strings.ReplaceAll(d.secretEnv[k], "'", `'\''`))
	}

	// mcp.json: the snapshot lacks it, so write it (env refs like ${DD_API_KEY}
	// stay literal — claude expands them at run time from the sourced env).
	mcpBlock := ""
	if d.mcpContent != "" {
		mcpBlock = fmt.Sprintf(`
cat > "$HOME/.config/agent/mcp.json" <<'AGENT_MCP_EOF'
%s
AGENT_MCP_EOF
chmod 600 "$HOME/.config/agent/mcp.json"
`, strings.TrimRight(d.mcpContent, "\n"))
	}

	return fmt.Sprintf(`set -e
export PATH="$HOME/.local/bin:$PATH"

# 1. secrets -> a 0600 env file the investigation run sources
mkdir -p "$HOME/.config/agent"
umask 077
cat > "$HOME/.config/agent/env" <<'AGENT_ENV_EOF'
%sAGENT_ENV_EOF
chmod 600 "$HOME/.config/agent/env"
%s
# 2. claude CLI (idempotent)
if ! command -v claude >/dev/null 2>&1; then
  curl -fsSL https://claude.ai/install.sh | bash >/dev/null 2>&1 || true
fi

# 3. gh CLI: package manager first, then a pinned tarball into ~/.local/bin
if ! command -v gh >/dev/null 2>&1; then
  (sudo -n dnf install -y gh || sudo -n yum install -y gh || sudo -n bash -c 'apt-get update && apt-get install -y gh') >/dev/null 2>&1 || true
fi
if ! command -v gh >/dev/null 2>&1; then
  arch=$(uname -m); case "$arch" in x86_64) a=amd64;; aarch64|arm64) a=arm64;; *) a=amd64;; esac
  url="https://github.com/cli/cli/releases/download/v%s/gh_%s_linux_${a}.tar.gz"
  tmp=$(mktemp -d)
  if curl -fsSL "$url" -o "$tmp/gh.tgz"; then
    tar -xzf "$tmp/gh.tgz" -C "$tmp" && mkdir -p "$HOME/.local/bin" && cp "$tmp"/gh_*/bin/gh "$HOME/.local/bin/gh" 2>/dev/null || true
  fi
  rm -rf "$tmp"
fi

# 4. aws CLI v2 (snapshot lacks it) -> ~/.local/bin, no sudo for aws itself
if ! command -v aws >/dev/null 2>&1; then
  arch=$(uname -m)
  tmp=$(mktemp -d)
  if curl -fsSL "https://awscli.amazonaws.com/awscli-exe-linux-${arch}.zip" -o "$tmp/aws.zip"; then
    command -v unzip >/dev/null 2>&1 || (sudo -n dnf install -y unzip || sudo -n yum install -y unzip || sudo -n bash -c 'apt-get update && apt-get install -y unzip') >/dev/null 2>&1 || true
    unzip -q "$tmp/aws.zip" -d "$tmp" && "$tmp/aws/install" --bin-dir "$HOME/.local/bin" --install-dir "$HOME/.local/aws-cli" --update >/dev/null 2>&1 || true
  fi
  rm -rf "$tmp"
fi

# 5. let gh serve as git's credential helper for the push (needs GH_TOKEN)
set -a; . "$HOME/.config/agent/env" 2>/dev/null; set +a
command -v gh >/dev/null 2>&1 && gh auth setup-git >/dev/null 2>&1 || true

echo BOOTSTRAP_OK
`, env.String(), mcpBlock, d.ghVersion, d.ghVersion)
}

// secretEnv collects the non-empty secrets to forward into the VM's run.
func secretEnv(cfg config.Config) map[string]string {
	m := map[string]string{}
	put := func(k, v string) {
		if v != "" {
			m[k] = v
		}
	}
	put("CLAUDE_CODE_OAUTH_TOKEN", cfg.ClaudeOAuthToken)
	put("ANTHROPIC_API_KEY", cfg.AnthropicAPIKey)
	put("GH_TOKEN", cfg.GHToken)
	put("GITHUB_TOKEN", cfg.GHToken)
	put("DD_API_KEY", cfg.DDApiKey)
	put("DD_APP_KEY", cfg.DDAppKey)
	put("AWS_ACCESS_KEY_ID", cfg.AWSAccessKeyID)
	put("AWS_SECRET_ACCESS_KEY", cfg.AWSSecretAccessKey)
	put("AWS_SESSION_TOKEN", cfg.AWSSessionToken)
	put("AWS_REGION", cfg.AWSRegion)
	put("AWS_DEFAULT_REGION", cfg.AWSRegion)
	return m
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

func (mockDispatcher) Run(ctx context.Context, _, prompt string, emit Emit) (string, error) {
	// Emit a scripted, self-consistent investigation so the live UI and the full
	// pipeline (collect → pr_check → distill → memory) can be exercised with no VM.
	steps := []model.AgentEvent{
		{Kind: "system", Summary: "session started (claude-sonnet, tools: Bash, Read, Edit, gh)"},
		{Kind: "text", Summary: "Reproducing: heap grows under sustained query load. Checking the hot path."},
		{Kind: "tool", Tool: "Bash", Summary: "rg -n 'ResourcesQuery' services/tachyon"},
		{Kind: "tool_result", Summary: "resolve.go:214  q := NewResourcesQuery(scope)  // rebuilt per request"},
		{Kind: "tool", Tool: "Read", Summary: "services/tachyon/resolve.go"},
		{Kind: "text", Summary: "Found it: the scope map is rebuilt on every request instead of memoized."},
		{Kind: "tool", Tool: "Bash", Summary: "aws s3 cp s3://as-live-heap-dump/tachyon/latest/heap.dump /tmp/ && go tool pprof -top"},
		{Kind: "tool_result", Summary: "flat  cum   ResourcesQuery.build  41.2%  inuse_space — dominates retained heap"},
		{Kind: "tool", Tool: "Edit", Summary: "services/tachyon/resolve.go — memoize per request context"},
		{Kind: "tool", Tool: "Bash", Summary: "git checkout -b agent/tachyon-memoize-scope && gh pr create --draft"},
		{Kind: "tool_result", Summary: "https://github.com/andromedasec/voyager/pull/9999"},
		{Kind: "result", Summary: "Root cause confirmed; draft PR opened."},
	}
	for _, ev := range steps {
		fire(emit, ev.Kind, ev.Tool, ev.Summary)
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-time.After(150 * time.Millisecond):
		}
	}
	return "Root cause: the resources cache is rebuilt on every request instead of " +
		"being reused across the request lifetime, so heap allocations churn under load. " +
		"Fix: memoize the query result for the duration of the request.\n" +
		"PR_URL: https://github.com/andromedasec/voyager/pull/9999", nil
}

// ---- stream-json parsing ----

// streamEvents maps one line of `claude -p --output-format stream-json` into zero
// or more AgentEvents, and pulls out the final result text when present.
func streamEvents(line []byte) (evs []model.AgentEvent, result string, gotResult bool) {
	line = bytes.TrimSpace(line)
	if len(line) == 0 || line[0] != '{' {
		return nil, "", false
	}
	var m struct {
		Type    string `json:"type"`
		Subtype string `json:"subtype"`
		Result  string `json:"result"`
		Model   string `json:"model"`
		Message struct {
			Content []struct {
				Type    string          `json:"type"`
				Text    string          `json:"text"`
				Name    string          `json:"name"`
				Input   json.RawMessage `json:"input"`
				Content json.RawMessage `json:"content"`
			} `json:"content"`
		} `json:"message"`
	}
	if json.Unmarshal(line, &m) != nil {
		return nil, "", false
	}

	switch m.Type {
	case "system":
		if m.Subtype == "init" {
			s := "session started"
			if m.Model != "" {
				s += " (" + m.Model + ")"
			}
			evs = append(evs, model.AgentEvent{Kind: "system", Summary: s})
		}
	case "assistant":
		for _, c := range m.Message.Content {
			switch c.Type {
			case "text":
				if t := strings.TrimSpace(c.Text); t != "" {
					evs = append(evs, model.AgentEvent{Kind: "text", Summary: truncate(t, 600)})
				}
			case "tool_use":
				evs = append(evs, model.AgentEvent{Kind: "tool", Tool: c.Name, Summary: toolSummary(c.Input)})
			}
		}
	case "user":
		for _, c := range m.Message.Content {
			if c.Type == "tool_result" {
				if s := resultSummary(c.Content); s != "" {
					evs = append(evs, model.AgentEvent{Kind: "tool_result", Summary: s})
				}
			}
		}
	case "result":
		result, gotResult = m.Result, true
		s := "run complete"
		if m.Subtype != "" && m.Subtype != "success" {
			s = "run ended: " + m.Subtype
		}
		evs = append(evs, model.AgentEvent{Kind: "result", Summary: s})
	}
	return evs, result, gotResult
}

// toolSummary turns a tool_use input into a compact one-liner, favouring the
// field that says what the agent actually did.
func toolSummary(input json.RawMessage) string {
	var m map[string]any
	_ = json.Unmarshal(input, &m)
	pick := func(keys ...string) string {
		for _, k := range keys {
			if v, ok := m[k].(string); ok && v != "" {
				return v
			}
		}
		return ""
	}
	if s := pick("command", "file_path", "path", "pattern", "query", "url"); s != "" {
		return oneLine(truncate(s, 300))
	}
	return oneLine(truncate(string(input), 200))
}

// resultSummary flattens a tool_result content (string or content blocks) into a
// single truncated line.
func resultSummary(raw json.RawMessage) string {
	if len(raw) == 0 {
		return ""
	}
	var s string
	if json.Unmarshal(raw, &s) == nil {
		return oneLine(truncate(s, 300))
	}
	var blocks []struct {
		Text string `json:"text"`
	}
	if json.Unmarshal(raw, &blocks) == nil {
		var b strings.Builder
		for _, bl := range blocks {
			b.WriteString(bl.Text)
			b.WriteString(" ")
		}
		return oneLine(truncate(b.String(), 300))
	}
	return oneLine(truncate(string(raw), 200))
}

func oneLine(s string) string {
	return strings.Join(strings.Fields(strings.ReplaceAll(s, "\n", " ")), " ")
}

func truncate(s string, n int) string {
	s = strings.TrimSpace(s)
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
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
