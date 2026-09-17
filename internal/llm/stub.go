package llm

import (
	"context"
	"fmt"
	"regexp"
	"strings"
)

// stubClient is a dependency-free backend for local demos and tests. It answers
// the three orchestrator prompts with deterministic heuristics — no model call —
// so the full pipeline (including memory hits) runs without an API key or the
// claude CLI. Not for production: swap in the api/cli backend there.
type stubClient struct{}

func NewStubClient() Client { return stubClient{} }

func (stubClient) Backend() string { return "stub" }

var (
	stubNewSig    = regexp.MustCompile(`New signature:\s*(.+)`)
	stubCandidate = regexp.MustCompile(`id=(\d+) signature="([^"]*)"`)
	stubKebab     = regexp.MustCompile(`[^a-z0-9]+`)
)

func (stubClient) Complete(_ context.Context, system, user string) (string, error) {
	switch {
	case strings.Contains(system, "fingerprint"):
		// Deterministic "<service>/<kebab-alert>" so identical alerts collapse.
		service, alert := "unknown", user
		for _, ln := range strings.Split(user, "\n") {
			if v, ok := strings.CutPrefix(ln, "Service:"); ok {
				service = strings.TrimSpace(v)
			}
			if v, ok := strings.CutPrefix(ln, "Alert:"); ok {
				alert = strings.TrimSpace(v)
			}
		}
		slug := stubKebab.ReplaceAllString(strings.ToLower(alert), "-")
		slug = strings.Trim(slug, "-")
		if len(slug) > 40 {
			slug = slug[:40]
		}
		return fmt.Sprintf("%s/%s", service, slug), nil

	case strings.Contains(system, "decide whether"):
		// Exact-signature match against the candidate list.
		newSig := ""
		if m := stubNewSig.FindStringSubmatch(user); len(m) == 2 {
			newSig = strings.TrimSpace(m[1])
		}
		for _, m := range stubCandidate.FindAllStringSubmatch(user, -1) {
			if strings.EqualFold(strings.TrimSpace(m[2]), newSig) {
				return fmt.Sprintf(`{"match": %s}`, m[1]), nil
			}
		}
		return `{"match": null}`, nil

	case strings.Contains(system, "compress"):
		return `{"symptom":"observed condition","root_cause":"synthesized by stub backend from the agent analysis","fix_summary":"see PR if opened"}`, nil
	}
	return "", nil
}
