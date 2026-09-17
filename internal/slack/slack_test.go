package slack

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestNewDisabledWhenUnconfigured(t *testing.T) {
	if New("", "", nil) != nil {
		t.Fatal("expected nil Notifier when token and channel are empty")
	}
	if New("xoxb-tok", "", nil) != nil {
		t.Fatal("expected nil Notifier when channel is empty")
	}
	if New("", "C123", nil) != nil {
		t.Fatal("expected nil Notifier when token is empty")
	}
	if New("xoxb-tok", "C123", nil) == nil {
		t.Fatal("expected a Notifier when both are set")
	}
}

func TestNilNotifierPostIsNoop(t *testing.T) {
	var n *Notifier // nil
	// Must not panic.
	n.PostResult(context.Background(), Result{Service: "svc"})
}

func TestTruncate(t *testing.T) {
	if got := truncate("hello", 10); got != "hello" {
		t.Fatalf("short string changed: %q", got)
	}
	got := truncate("hello world", 5)
	if !strings.HasSuffix(got, "…") {
		t.Fatalf("expected ellipsis, got %q", got)
	}
	// Multibyte runes must be counted, not bytes (no broken UTF-8).
	if got := truncate("héllo wörld", 4); !strings.HasSuffix(got, "…") || len([]rune(got)) > 5 {
		t.Fatalf("multibyte truncate wrong: %q", got)
	}
}

func TestMrkdwnEscaping(t *testing.T) {
	got := mrkdwn("a < b && c > d")
	for _, want := range []string{"&lt;", "&amp;", "&gt;"} {
		if !strings.Contains(got, want) {
			t.Fatalf("mrkdwn(%q) = %q, missing %q", "a < b && c > d", got, want)
		}
	}
}

func TestBlocksPROpened(t *testing.T) {
	n := &Notifier{}
	r := Result{
		TaskID:     "t-7",
		Service:    "ingester-realtime-logs",
		Alert:      "CPU > 90%",
		Outcome:    "pr_opened",
		Symptom:    "sustained CPU saturation",
		RootCause:  "unbounded goroutine fan-out per batch",
		FixSummary: "bound concurrency with a worker pool",
		PRURL:      "https://github.com/andromedasec/voyager/pull/42",
		Elapsed:    92 * time.Second,
	}
	blocks := n.blocks(r)

	// Must marshal to valid JSON (what we send to Slack).
	raw, err := json.Marshal(blocks)
	if err != nil {
		t.Fatalf("marshal blocks: %v", err)
	}
	s := string(raw)
	for _, want := range []string{
		"Fix PR opened", ":white_check_mark:", "ingester-realtime-logs",
		"Root cause", "unbounded goroutine fan-out", "pull/42", "t-7",
	} {
		if !strings.Contains(s, want) {
			t.Fatalf("blocks JSON missing %q\n%s", want, s)
		}
	}

	// Fallback text carries the headline + PR link for notifications.
	fb := n.fallbackText(r)
	if !strings.Contains(fb, "Fix PR opened") || !strings.Contains(fb, "pull/42") {
		t.Fatalf("fallback text wrong: %q", fb)
	}
}

func TestBlocksAnalysisOnly(t *testing.T) {
	n := &Notifier{}
	r := Result{TaskID: "t-8", Service: "tachyon", Outcome: "analysis_only", RootCause: "GC pressure"}
	raw, _ := json.Marshal(n.blocks(r))
	s := string(raw)
	if !strings.Contains(s, "Analysis complete") || !strings.Contains(s, ":mag:") {
		t.Fatalf("expected analysis header, got %s", s)
	}
	if strings.Contains(s, "Pull request") {
		t.Fatalf("no PR expected in analysis-only message: %s", s)
	}
}
