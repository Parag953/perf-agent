package dispatch

import (
	"strings"
	"testing"

	"github.com/Parag953/perf-agent/internal/config"
)

// The bootstrap script must carry every configured secret (quoted), install
// claude/gh only when absent, pin the gh version, and end with the sentinel the
// dispatcher checks for.
func TestBootstrapScript(t *testing.T) {
	d := New(config.Config{
		DispatchMode:     "ssh",
		VMBootstrap:      true,
		GHVersion:        "2.63.2",
		ClaudeBin:        "claude",
		ClaudeOAuthToken: "sk-ant-oat-secret",
		GHToken:          "ghp_secret",
		DDApiKey:         "dd-api-secret",
		DDAppKey:         "dd-app-secret",
		AWSAccessKeyID:   "AKIAEXAMPLE",
		MCPConfigSrc:     "/no/such/file", // unreadable => no mcp block
	}).(sshDispatcher)

	s := d.bootstrapScript()

	for _, want := range []string{
		"export CLAUDE_CODE_OAUTH_TOKEN='sk-ant-oat-secret'",
		"export GH_TOKEN='ghp_secret'",
		"export GITHUB_TOKEN='ghp_secret'",
		"export DD_API_KEY='dd-api-secret'",
		"export DD_APP_KEY='dd-app-secret'",
		"export AWS_ACCESS_KEY_ID='AKIAEXAMPLE'",
		"command -v claude", // guarded install
		"command -v gh",
		"command -v aws",                             // aws v2 install
		"awscli-exe-linux",                           // aws installer url
		"releases/download/v2.63.2/gh_2.63.2_linux_", // pinned gh tarball fallback
		"gh auth setup-git",
		"chmod 600",
		"echo BOOTSTRAP_OK",
	} {
		if !strings.Contains(s, want) {
			t.Errorf("bootstrap script missing %q", want)
		}
	}
	// An unset secret must not appear at all.
	if strings.Contains(s, "AWS_SESSION_TOKEN") {
		t.Error("unset secret AWS_SESSION_TOKEN should be omitted")
	}
}

// When mcp.json content is present it must be written on the VM with its
// ${DD_API_KEY} refs left literal (claude expands them at run time), and the run
// must point --mcp-config at the written file.
func TestBootstrapShipsMCP(t *testing.T) {
	d := sshDispatcher{
		claudeBin:  "claude",
		mcpConfig:  "/opt/agent/mcp.json",
		mcpContent: `{"mcpServers":{"datadog":{"headers":{"DD_API_KEY":"${DD_API_KEY}"}}}}`,
		ghVersion:  "2.63.2",
		secretEnv:  map[string]string{"DD_API_KEY": "z"},
	}
	s := d.bootstrapScript()
	if !strings.Contains(s, "AGENT_MCP_EOF") {
		t.Error("mcp.json heredoc missing")
	}
	if !strings.Contains(s, `"DD_API_KEY":"${DD_API_KEY}"`) {
		t.Error("mcp.json env ref should be written literally, not expanded")
	}
}

// A single quote in a secret must be shell-escaped so the env file still sources.
func TestSecretEnvEscaping(t *testing.T) {
	d := New(config.Config{
		DispatchMode: "ssh",
		GHToken:      `pa'ss`,
	}).(sshDispatcher)

	s := d.bootstrapScript()
	if !strings.Contains(s, `export GH_TOKEN='pa'\''ss'`) {
		t.Errorf("single quote not escaped in:\n%s", s)
	}
}

// Bootstrap disabled or mock mode must not produce an ssh bootstrap path.
func TestSecretEnvOmitsEmpty(t *testing.T) {
	got := secretEnv(config.Config{AWSRegion: "us-west-2"})
	if _, ok := got["CLAUDE_CODE_OAUTH_TOKEN"]; ok {
		t.Error("empty token should be omitted")
	}
	if got["AWS_REGION"] != "us-west-2" || got["AWS_DEFAULT_REGION"] != "us-west-2" {
		t.Errorf("region not forwarded to both keys: %v", got)
	}
}
