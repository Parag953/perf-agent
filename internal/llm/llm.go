// Package llm wraps the small Haiku calls the orchestrator makes on its own:
// fingerprinting an alert, deciding whether it matches a known issue, and
// distilling an agent's response into a memory row.
//
// Two backends are provided. When an Anthropic API key is present the "api"
// backend calls the Messages API directly; otherwise the "cli" backend shells
// out to the local `claude` binary, reusing the same subscription OAuth token
// the agents authenticate with.
package llm

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os/exec"
	"strings"
	"time"
)

// Client is a single-shot completion: system + user prompt in, text out.
type Client interface {
	Complete(ctx context.Context, system, user string) (string, error)
	Backend() string
}

// ---- Anthropic Messages API backend ----

type apiClient struct {
	key   string
	model string
	hc    *http.Client
}

func NewAPIClient(key, model string) Client {
	return &apiClient{key: key, model: model, hc: &http.Client{Timeout: 60 * time.Second}}
}

func (c *apiClient) Backend() string { return "api" }

func (c *apiClient) Complete(ctx context.Context, system, user string) (string, error) {
	body, _ := json.Marshal(map[string]any{
		"model":      c.model,
		"max_tokens": 1024,
		"system":     system,
		"messages":   []map[string]string{{"role": "user", "content": user}},
	})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "https://api.anthropic.com/v1/messages", bytes.NewReader(body))
	if err != nil {
		return "", err
	}
	req.Header.Set("x-api-key", c.key)
	req.Header.Set("anthropic-version", "2023-06-01")
	req.Header.Set("content-type", "application/json")

	resp, err := c.hc.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("anthropic api %d: %s", resp.StatusCode, strings.TrimSpace(string(raw)))
	}
	var out struct {
		Content []struct {
			Type string `json:"type"`
			Text string `json:"text"`
		} `json:"content"`
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		return "", fmt.Errorf("decode anthropic response: %w", err)
	}
	var sb strings.Builder
	for _, b := range out.Content {
		if b.Type == "text" {
			sb.WriteString(b.Text)
		}
	}
	return sb.String(), nil
}

// ---- claude CLI backend ----

type cliClient struct {
	bin   string
	model string
}

func NewCLIClient(bin, model string) Client {
	return &cliClient{bin: bin, model: model}
}

func (c *cliClient) Backend() string { return "cli" }

func (c *cliClient) Complete(ctx context.Context, system, user string) (string, error) {
	// The CLI has no separate system channel here; fold it into the prompt.
	prompt := user
	if system != "" {
		prompt = system + "\n\n" + user
	}
	cmd := exec.CommandContext(ctx, c.bin, "-p",
		"--model", c.model,
		"--output-format", "json",
	)
	cmd.Stdin = strings.NewReader(prompt)
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("claude cli: %v: %s", err, strings.TrimSpace(stderr.String()))
	}
	var out struct {
		Result string `json:"result"`
	}
	if err := json.Unmarshal(stdout.Bytes(), &out); err != nil {
		// Fall back to raw stdout if it wasn't JSON.
		return strings.TrimSpace(stdout.String()), nil
	}
	return out.Result, nil
}
