// Package slack posts a finished investigation's result into a Slack channel via
// chat.postMessage. It is optional: New returns nil when no bot token/channel is
// configured, and every method is nil-safe, so callers need no guard.
package slack

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"time"
)

const postMessageURL = "https://slack.com/api/chat.postMessage"

// Notifier posts messages to one Slack channel as the perf-agent bot.
type Notifier struct {
	token   string
	channel string
	hc      *http.Client
	log     *log.Logger
}

// New builds a Notifier, or returns nil (notifications disabled) when either the
// bot token or the channel is unset. A nil *Notifier is safe to call.
func New(token, channel string, logger *log.Logger) *Notifier {
	if token == "" || channel == "" {
		return nil
	}
	return &Notifier{
		token:   token,
		channel: channel,
		hc:      &http.Client{Timeout: 15 * time.Second},
		log:     logger,
	}
}

// Result is what a finished investigation produced, formatted into a message.
type Result struct {
	TaskID     string
	Service    string
	Alert      string
	Outcome    string // model.Outcome as a string (e.g. "pr_opened", "analysis_only")
	Symptom    string
	RootCause  string
	FixSummary string
	PRURL      string
	Elapsed    time.Duration
}

// PostResult posts one investigation result. It is best-effort: failures are
// logged, never returned, so notification never blocks or fails the task. Safe
// to call on a nil *Notifier (no-op).
func (n *Notifier) PostResult(ctx context.Context, r Result) {
	if n == nil {
		return
	}
	payload := map[string]any{
		"channel": n.channel,
		"text":    n.fallbackText(r), // notification + accessibility fallback
		"blocks":  n.blocks(r),
	}
	body, err := json.Marshal(payload)
	if err != nil {
		n.logf("marshal message: %v", err)
		return
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, postMessageURL, bytes.NewReader(body))
	if err != nil {
		n.logf("build request: %v", err)
		return
	}
	req.Header.Set("Authorization", "Bearer "+n.token)
	req.Header.Set("Content-Type", "application/json; charset=utf-8")

	resp, err := n.hc.Do(req)
	if err != nil {
		n.logf("post to slack: %v", err)
		return
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)

	// chat.postMessage returns HTTP 200 with {"ok":false,"error":"..."} on
	// logical failures (bad token, not_in_channel, ...), so inspect the body.
	var out struct {
		OK    bool   `json:"ok"`
		Error string `json:"error"`
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		n.logf("decode slack response (http %d): %v", resp.StatusCode, err)
		return
	}
	if !out.OK {
		n.logf("slack rejected message: %s (http %d)", out.Error, resp.StatusCode)
		return
	}
	n.logf("posted %s result for task %s", r.Outcome, r.TaskID)
}

func (n *Notifier) fallbackText(r Result) string {
	head := "Analysis complete"
	if r.Outcome == "pr_opened" {
		head = "Fix PR opened"
	}
	svc := r.Service
	if svc == "" {
		svc = "unknown service"
	}
	if r.PRURL != "" {
		return fmt.Sprintf("%s for %s — %s", head, svc, r.PRURL)
	}
	return fmt.Sprintf("%s for %s", head, svc)
}

// blocks renders the Block Kit message. Section text is capped so a long
// root-cause or fix never trips Slack's 3000-char per-block limit.
func (n *Notifier) blocks(r Result) []map[string]any {
	header, emoji := "Analysis complete", ":mag:"
	if r.Outcome == "pr_opened" {
		header, emoji = "Fix PR opened", ":white_check_mark:"
	}

	blocks := []map[string]any{
		{
			"type": "header",
			"text": map[string]any{"type": "plain_text", "text": fmt.Sprintf("%s %s", emoji, header), "emoji": true},
		},
	}

	// Service / alert / elapsed context line.
	var ctx []string
	if r.Service != "" {
		ctx = append(ctx, "*Service:* `"+r.Service+"`")
	}
	if r.Alert != "" {
		ctx = append(ctx, "*Alert:* "+mrkdwn(r.Alert))
	}
	if r.Elapsed > 0 {
		ctx = append(ctx, "*Took:* "+r.Elapsed.Round(time.Second).String())
	}
	if len(ctx) > 0 {
		blocks = append(blocks, section(strings.Join(ctx, "   ")))
	}

	if r.Symptom != "" {
		blocks = append(blocks, section("*Symptom*\n"+mrkdwn(truncate(r.Symptom, 600))))
	}
	if r.RootCause != "" {
		blocks = append(blocks, section("*Root cause*\n"+mrkdwn(truncate(r.RootCause, 1200))))
	}
	if r.FixSummary != "" {
		blocks = append(blocks, section("*Fix*\n"+mrkdwn(truncate(r.FixSummary, 1200))))
	}
	if r.PRURL != "" {
		blocks = append(blocks, section("*Pull request*\n<"+r.PRURL+">"))
	}

	// Footer with the task id for cross-referencing the dashboard.
	if r.TaskID != "" {
		blocks = append(blocks, map[string]any{
			"type":     "context",
			"elements": []map[string]any{{"type": "mrkdwn", "text": "perf-agent · task `" + r.TaskID + "`"}},
		})
	}
	return blocks
}

func section(text string) map[string]any {
	return map[string]any{"type": "section", "text": map[string]any{"type": "mrkdwn", "text": text}}
}

func (n *Notifier) logf(format string, args ...any) {
	if n.log != nil {
		n.log.Printf("slack: "+format, args...)
	}
}

// truncate trims s to at most max characters, appending an ellipsis when it cut.
func truncate(s string, max int) string {
	r := []rune(s)
	if len(r) <= max {
		return s
	}
	return strings.TrimSpace(string(r[:max])) + "…"
}

// mrkdwn escapes the three characters Slack treats specially in mrkdwn text so
// agent-authored analysis renders literally rather than as broken markup.
func mrkdwn(s string) string {
	r := strings.NewReplacer("&", "&amp;", "<", "&lt;", ">", "&gt;")
	return r.Replace(s)
}
