// Package config loads orchestrator settings from the environment.
package config

import (
	"os"
	"strconv"
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

	// Agent runtime (paths are on the VM, not the orchestrator).
	VoyagerPath   string
	VoyagerRepo   string
	S3Bucket      string
	MCPConfig     string
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
		VoyagerPath:     env("VOYAGER_PATH", "/opt/agent/voyager"),
		VoyagerRepo:     env("VOYAGER_REPO", "andromedasec/voyager"),
		S3Bucket:        env("S3_BUCKET", "as-live-heap-dump"),
		MCPConfig:       env("MCP_CONFIG", "/opt/agent/mcp.json"),
		AllowedTools:    env("ALLOWED_TOOLS", "Bash,Edit,Write,mcp__datadog__*,mcp__andromeda_gateway__*"),
		ClaudeTimeout:   time.Duration(envInt("CLAUDE_TIMEOUT", 900)) * time.Second,
		DBPath:          env("DB_PATH", "/opt/agent/memory.db"),
		Workers:         envInt("WORKERS", 4),
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
