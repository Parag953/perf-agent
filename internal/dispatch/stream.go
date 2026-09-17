package dispatch

import (
	"bufio"
	"encoding/json"
	"io"
	"strings"
	"time"

	"github.com/Parag953/perf-agent/internal/model"
)

const maxEventText = 600

type streamEnvelope struct {
	Type    string `json:"type"`
	Subtype string `json:"subtype"`
	Model   string `json:"model"`
	Result  string `json:"result"`
	Message struct {
		Content []struct {
			Type     string          `json:"type"`
			Text     string          `json:"text"`
			Thinking string          `json:"thinking"`
			Name     string          `json:"name"`
			Input    json.RawMessage `json:"input"`
			Content  json.RawMessage `json:"content"`
			IsError  bool            `json:"is_error"`
		} `json:"content"`
	} `json:"message"`
}

func DecodeStreamLine(b []byte) ([]model.AgentEvent, string) {
	line := strings.TrimSpace(string(b))
	if line == "" || !strings.HasPrefix(line, "{") {
		return nil, ""
	}
	var env streamEnvelope
	if err := json.Unmarshal([]byte(line), &env); err != nil {
		return nil, ""
	}

	switch env.Type {
	case "system":
		if env.Subtype != "init" {
			return nil, ""
		}
		text := "agent session started"
		if env.Model != "" {
			text += " on " + env.Model
		}
		return []model.AgentEvent{{Kind: model.EventSystem, Text: text}}, ""

	case "result":
		text := clip(env.Result)
		if text == "" {
			text = "run finished"
		}
		kind := model.EventResult
		if env.Subtype != "" && env.Subtype != "success" {
			kind = model.EventFailure
		}
		return []model.AgentEvent{{Kind: kind, Text: text}}, env.Result

	case "assistant", "user":
		var out []model.AgentEvent
		for _, c := range env.Message.Content {
			switch c.Type {
			case "text":
				if t := strings.TrimSpace(c.Text); t != "" {
					out = append(out, model.AgentEvent{Kind: model.EventText, Text: clip(t)})
				}
			case "thinking":
				if t := strings.TrimSpace(c.Thinking); t != "" {
					out = append(out, model.AgentEvent{Kind: model.EventThinking, Text: clip(t)})
				}
			case "tool_use":
				out = append(out, model.AgentEvent{Kind: model.EventTool, Tool: c.Name, Text: clip(toolArg(c.Input))})
			case "tool_result":
				kind := model.EventToolResult
				if c.IsError {
					kind = model.EventFailure
				}
				if t := strings.TrimSpace(flatten(c.Content)); t != "" {
					out = append(out, model.AgentEvent{Kind: kind, Text: clip(t)})
				}
			}
		}
		return out, ""
	}
	return nil, ""
}

func toolArg(raw json.RawMessage) string {
	if len(raw) == 0 {
		return ""
	}
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil {
		return ""
	}
	for _, k := range []string{"command", "file_path", "pattern", "path", "url", "query", "prompt"} {
		if v, ok := m[k].(string); ok && strings.TrimSpace(v) != "" {
			return v
		}
	}
	b, err := json.Marshal(m)
	if err != nil {
		return ""
	}
	return string(b)
}

func flatten(raw json.RawMessage) string {
	if len(raw) == 0 {
		return ""
	}
	var s string
	if err := json.Unmarshal(raw, &s); err == nil {
		return s
	}
	var blocks []struct {
		Type string `json:"type"`
		Text string `json:"text"`
	}
	if err := json.Unmarshal(raw, &blocks); err == nil {
		parts := make([]string, 0, len(blocks))
		for _, b := range blocks {
			if b.Text != "" {
				parts = append(parts, b.Text)
			}
		}
		return strings.Join(parts, "\n")
	}
	return string(raw)
}

func clip(s string) string {
	s = strings.TrimSpace(s)
	r := []rune(s)
	if len(r) <= maxEventText {
		return s
	}
	return strings.TrimSpace(string(r[:maxEventText])) + "…"
}

func scanStream(r io.Reader, emit func(model.AgentEvent)) (string, error) {
	br := bufio.NewReaderSize(r, 64*1024)
	seq := 0
	final := ""
	for {
		line, err := br.ReadString('\n')
		if len(line) > 0 {
			evs, f := DecodeStreamLine([]byte(line))
			for _, e := range evs {
				seq++
				e.Seq = seq
				e.At = time.Now().UTC()
				emit(e)
			}
			if f != "" {
				final = f
			}
		}
		if err != nil {
			if err == io.EOF {
				return final, nil
			}
			return final, err
		}
	}
}
