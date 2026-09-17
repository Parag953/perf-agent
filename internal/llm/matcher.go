package llm

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/Parag953/perf-agent/internal/model"
)

// Signature reduces an alert to a canonical fingerprint like "tachyon/scope-map-churn".
// It is stable across cosmetically different firings of the same underlying issue.
func Signature(ctx context.Context, c Client, t model.Trigger) (string, error) {
	const system = `You fingerprint production alerts for a Go platform. Given an alert, ` +
		`return ONE canonical signature of the form "<service>/<short-kebab-symptom>" ` +
		`(e.g. "tachyon/scope-map-churn", "kepler/log-buffer-growth"). ` +
		`The symptom should name the underlying condition, not the exact numbers, ` +
		`so the same issue firing twice yields the same signature. ` +
		`Reply with the signature ONLY — no prose, no quotes, no code fence.`
	user := fmt.Sprintf("Service: %s\nAlert: %s\nPayload: %s", t.Service, t.Alert, string(t.Payload))
	out, err := c.Complete(ctx, system, user)
	if err != nil {
		return "", err
	}
	sig := strings.TrimSpace(firstLine(out))
	sig = strings.Trim(sig, "`\"' ")
	if sig == "" {
		return "", fmt.Errorf("empty signature")
	}
	return sig, nil
}

// Match asks Haiku whether the incoming signature is the same issue as any of
// the candidate memories. It returns the matched memory id, or 0 for no match.
func Match(ctx context.Context, c Client, sig string, candidates []model.Memory) (int64, error) {
	if len(candidates) == 0 {
		return 0, nil
	}
	var b strings.Builder
	for _, m := range candidates {
		fmt.Fprintf(&b, "- id=%d signature=%q root_cause=%q\n", m.ID, m.Signature, truncate(m.RootCause, 200))
	}
	const system = `You decide whether a new incident matches a previously seen one. ` +
		`You are given a new signature and a list of known issues (id, signature, root_cause). ` +
		`If the new signature describes the SAME underlying problem as one of them, return that id. ` +
		`Be conservative: only match when you are confident it is the same root cause, not merely the same service. ` +
		`Reply with strict JSON only: {"match": <id or null>}.`
	user := fmt.Sprintf("New signature: %s\n\nKnown issues:\n%s", sig, b.String())
	out, err := c.Complete(ctx, system, user)
	if err != nil {
		return 0, err
	}
	var parsed struct {
		Match *int64 `json:"match"`
	}
	if err := json.Unmarshal([]byte(extractJSON(out)), &parsed); err != nil {
		return 0, fmt.Errorf("decode match: %w (raw: %q)", err, truncate(out, 120))
	}
	if parsed.Match == nil {
		return 0, nil
	}
	return *parsed.Match, nil
}

// Distill turns an agent's freeform analysis into the structured fields of a
// memory row. The signature is passed through (already computed at intake).
func Distill(ctx context.Context, c Client, t model.Trigger, sig, analysis, prURL string) (model.Memory, error) {
	const system = `You compress an engineer's root-cause analysis into a compact memory record. ` +
		`Reply with strict JSON only, no code fence: ` +
		`{"symptom": "...", "root_cause": "...", "fix_summary": "..."}. ` +
		`symptom: the observable condition in a few words. ` +
		`root_cause: the underlying cause in 1-2 sentences. ` +
		`fix_summary: the fix in one sentence, or "" if none was proposed.`
	user := fmt.Sprintf("Service: %s\nAlert: %s\n\nAnalysis:\n%s", t.Service, t.Alert, analysis)
	out, err := c.Complete(ctx, system, user)
	m := model.Memory{Signature: sig, Service: t.Service, PRURL: prURL, FullAnalysis: analysis}
	if err != nil {
		return m, err
	}
	var parsed struct {
		Symptom    string `json:"symptom"`
		RootCause  string `json:"root_cause"`
		FixSummary string `json:"fix_summary"`
	}
	if err := json.Unmarshal([]byte(extractJSON(out)), &parsed); err != nil {
		// Degrade gracefully: keep the raw analysis as the root cause.
		m.RootCause = truncate(analysis, 500)
		return m, nil
	}
	m.Symptom, m.RootCause, m.FixSummary = parsed.Symptom, parsed.RootCause, parsed.FixSummary
	return m, nil
}

func firstLine(s string) string {
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		return s[:i]
	}
	return s
}

func truncate(s string, n int) string {
	s = strings.TrimSpace(s)
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}

// extractJSON pulls the first {...} object out of a reply that may be wrapped
// in prose or a ```json fence.
func extractJSON(s string) string {
	start := strings.IndexByte(s, '{')
	end := strings.LastIndexByte(s, '}')
	if start >= 0 && end > start {
		return s[start : end+1]
	}
	return s
}
