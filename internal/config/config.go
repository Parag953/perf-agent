// Package config loads orchestrator settings from the environment.
package config

import (
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	// HTTP
	WebhookAddr string // listen address for the webhook + dashboard API

	// Poold (VM pool manager) — mocked by cmd/mock-poold for now.
	PooldURL string

	// LLM (Haiku) — used for signature / match / distill.
	// Backend selection: LLM_BACKEND explicitly picks "api", "cli", or "stub".
	// When unset, "api" is used if AnthropicAPIKey is present, else "cli"
	// (shell out to the local `claude`, reusing the agents' subscription token).
	// "stub" is a dependency-free demo/test backend.
	AnthropicAPIKey string
	HaikuModel      string
	ClaudeBin       string
	llmBackend      string

	// Dispatch — how the investigation command reaches a VM.
	//   "ssh"  : ssh the `claude -p` job to the leased VM (production)
	//   "mock" : synthesize a plausible analysis locally (demo / tests)
	DispatchMode string
	SSHKey       string // -i identity file for ssh dispatch

	// VM bootstrap — a leased VM comes back blank, so before `claude -p` runs the
	// orchestrator provisions it: install the tooling (claude, gh) if missing and
	// inject the secrets the agent needs. Idempotent, so a pre-baked image no-ops.
	VMBootstrap bool   // run the bootstrap step before dispatch (default true)
	GHVersion   string // gh CLI version for the tarball fallback install
	AgentStream bool   // stream `claude -p` step-by-step (stream-json) for the live UI

	// Secrets forwarded into the VM's investigation run (never logged, written to
	// a 0600 env file on the VM). All optional — set the ones the agent needs.
	ClaudeOAuthToken   string // CLAUDE_CODE_OAUTH_TOKEN — headless claude auth
	GHToken            string // GH_TOKEN/GITHUB_TOKEN — git push + gh pr create
	DDApiKey           string // DD_API_KEY — datadog MCP (referenced by mcp.json)
	DDAppKey           string // DD_APP_KEY — datadog MCP
	AWSAccessKeyID     string
	AWSSecretAccessKey string
	AWSSessionToken    string
	AWSRegion          string

	// Agent runtime (paths are on the VM, not the orchestrator).
	VoyagerPath   string
	VoyagerRepo   string
	S3Bucket      string
	MCPConfig     string // path claude --mcp-config reads on the VM (fallback if no content shipped)
	MCPConfigSrc  string // path on the BOX to read mcp.json from; its content is shipped to the VM
	AllowedTools  string
	ClaudeTimeout time.Duration

	// Memory
	DBPath string

	// Concurrency — number of workers; each holds at most one leased VM.
	Workers int
}

func Load() Config {
	c := Config{
		WebhookAddr:     env("WEBHOOK_ADDR", ":8080"),
		PooldURL:        env("POOLD_URL", "http://localhost:9090"),
		AnthropicAPIKey: env("ANTHROPIC_API_KEY", ""),
		HaikuModel:      env("HAIKU_MODEL", "claude-haiku-4-5-20251001"),
		ClaudeBin:       env("CLAUDE_BIN", "claude"),
		llmBackend:      env("LLM_BACKEND", ""),
		DispatchMode:    env("DISPATCH_MODE", "ssh"),
		SSHKey:          env("SSH_KEY", ""),

		VMBootstrap:        envBool("VM_BOOTSTRAP", true),
		GHVersion:          env("GH_VERSION", "2.63.2"),
		AgentStream:        envBool("AGENT_STREAM", true),
		ClaudeOAuthToken:   env("CLAUDE_CODE_OAUTH_TOKEN", ""),
		GHToken:            env("GH_TOKEN", env("GITHUB_TOKEN", "")),
		DDApiKey:           env("DD_API_KEY", ""),
		DDAppKey:           env("DD_APP_KEY", ""),
		AWSAccessKeyID:     env("AWS_ACCESS_KEY_ID", ""),
		AWSSecretAccessKey: env("AWS_SECRET_ACCESS_KEY", ""),
		AWSSessionToken:    env("AWS_SESSION_TOKEN", ""),
		AWSRegion:          env("AWS_REGION", env("AWS_DEFAULT_REGION", "us-west-2")),

		VoyagerPath:   env("VOYAGER_PATH", "/opt/agent/voyager"),
		VoyagerRepo:   env("VOYAGER_REPO", "andromedasec/voyager"),
		S3Bucket:      env("S3_BUCKET", "as-live-heap-dump"),
		MCPConfig:     env("MCP_CONFIG", "/opt/agent/mcp.json"),
		MCPConfigSrc:  env("MCP_CONFIG_SRC", env("MCP_CONFIG", "/opt/agent/mcp.json")),
		AllowedTools:  env("ALLOWED_TOOLS", "Bash,Edit,Write,mcp__datadog__*,mcp__andromeda_gateway__*"),
		ClaudeTimeout: time.Duration(envInt("CLAUDE_TIMEOUT", 900)) * time.Second,
		DBPath:        env("DB_PATH", "/opt/agent/memory.db"),
		Workers:       envInt("WORKERS", 4),
	}
	return c
}

// LLMBackend reports which Haiku backend to use: an explicit LLM_BACKEND wins,
// otherwise "api" when a key is present, else "cli".
func (c Config) LLMBackend() string {
	if c.llmBackend != "" {
		return c.llmBackend
	}
	if c.AnthropicAPIKey != "" {
		return "api"
	}
	return "cli"
}

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func envInt(k string, def int) int {
	if v := os.Getenv(k); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

// envBool reads a boolean env var. "0", "false", "no", "off" (any case) are false;
// any other non-empty value is true; unset falls back to def.
func envBool(k string, def bool) bool {
	v := strings.TrimSpace(strings.ToLower(os.Getenv(k)))
	if v == "" {
		return def
	}
	switch v {
	case "0", "false", "no", "off":
		return false
	default:
		return true
	}
}
