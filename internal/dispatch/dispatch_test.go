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
		AWSAccessKeyID:   "AKIAEXAMPLE",
	}).(sshDispatcher)

	s := d.bootstrapScript()

	for _, want := range []string{
		"export CLAUDE_CODE_OAUTH_TOKEN='sk-ant-oat-secret'",
		"export GH_TOKEN='ghp_secret'",
		"export GITHUB_TOKEN='ghp_secret'",
		"export AWS_ACCESS_KEY_ID='AKIAEXAMPLE'",
		"command -v claude", // guarded install
		"command -v gh",
		"releases/download/v2.63.2/gh_2.63.2_linux_", // pinned tarball fallback
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
