package dispatch

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/Parag953/perf-agent/internal/config"
	"github.com/Parag953/perf-agent/internal/model"
)

func promptFor(service string) string {
	cfg := config.Config{
		VoyagerPath: "$HOME/voyager",
		VoyagerRepo: "andromedasec/voyager",
		S3Bucket:    "as-live-heap-dump",
	}
	return BuildInvestigationPrompt(cfg, model.Trigger{
		Service: service,
		Alert:   "memory > 60%",
		Payload: json.RawMessage(`{}`),
	}, "")
}

func TestPromptDoesNotAssertAServiceDirectoryBuiltFromTheDeploymentName(t *testing.T) {
	p := promptFor("ingester-realtime-logs")
	if strings.Contains(p, "services/ingester-realtime-logs") {
		t.Errorf("prompt asserts services/<deployment> as a real path; the deployment name is not the source directory")
	}
}

func TestPromptWarnsTheDeploymentNameMayNotMatchTheDirectory(t *testing.T) {
	p := strings.ToLower(promptFor("ingester-realtime-logs"))
	if !strings.Contains(p, "may not match") && !strings.Contains(p, "does not always match") {
		t.Errorf("prompt should warn that the alert's service name may not match the source directory")
	}
}

func TestPromptGivesAConcreteWayToLocateTheServiceDirectory(t *testing.T) {
	p := promptFor("ingester-realtime-logs")
	if !strings.Contains(p, "ls ") && !strings.Contains(p, "find ") {
		t.Errorf("prompt should hand the agent a concrete command to locate the service directory")
	}
}

func TestPromptUsesTheConfiguredRepoRoot(t *testing.T) {
	p := promptFor("tachyon")
	if !strings.Contains(p, "$HOME/voyager") {
		t.Errorf("prompt should reference the configured repo root")
	}
}

func TestVoyagerPathDefaultsToTheRepoOnTheLeasedVM(t *testing.T) {
	t.Setenv("VOYAGER_PATH", "")
	got := config.Load().VoyagerPath
	if got != "$HOME/voyager" {
		t.Errorf("VoyagerPath default = %q, want $HOME/voyager (the path on the pool VM, not on the orchestrator box)", got)
	}
}
