package dispatch

import (
	"strings"
	"testing"

	"github.com/Parag953/perf-agent/internal/model"
)

func TestDecodeStreamLineEmitsTextFromAssistantMessage(t *testing.T) {
	line := `{"type":"assistant","message":{"content":[{"type":"text","text":"The heap grows because the cache is rebuilt."}]}}`
	evs, final := DecodeStreamLine([]byte(line))
	if final != "" {
		t.Errorf("final = %q, want empty", final)
	}
	if len(evs) != 1 {
		t.Fatalf("got %d events, want 1", len(evs))
	}
	if evs[0].Kind != model.EventText {
		t.Errorf("kind = %s, want text", evs[0].Kind)
	}
	if evs[0].Text != "The heap grows because the cache is rebuilt." {
		t.Errorf("text = %q", evs[0].Text)
	}
}

func TestDecodeStreamLineEmitsThinkingSeparatelyFromText(t *testing.T) {
	line := `{"type":"assistant","message":{"content":[` +
		`{"type":"thinking","thinking":"Let me check the allocation path first."},` +
		`{"type":"text","text":"Checking the allocation path."}]}}`
	evs, _ := DecodeStreamLine([]byte(line))
	if len(evs) != 2 {
		t.Fatalf("got %d events, want 2", len(evs))
	}
	if evs[0].Kind != model.EventThinking || evs[0].Text != "Let me check the allocation path first." {
		t.Errorf("first = %+v", evs[0])
	}
	if evs[1].Kind != model.EventText {
		t.Errorf("second kind = %s, want text", evs[1].Kind)
	}
}

func TestDecodeStreamLineSummarizesBashToolCall(t *testing.T) {
	line := `{"type":"assistant","message":{"content":[{"type":"tool_use","name":"Bash","input":{"command":"rg -n memoize services/tachyon","description":"search"}}]}}`
	evs, _ := DecodeStreamLine([]byte(line))
	if len(evs) != 1 {
		t.Fatalf("got %d events, want 1", len(evs))
	}
	if evs[0].Kind != model.EventTool {
		t.Errorf("kind = %s, want tool", evs[0].Kind)
	}
	if evs[0].Tool != "Bash" {
		t.Errorf("tool = %q", evs[0].Tool)
	}
	if evs[0].Text != "rg -n memoize services/tachyon" {
		t.Errorf("text = %q, want the command", evs[0].Text)
	}
}

func TestDecodeStreamLineUsesFilePathForReadToolCall(t *testing.T) {
	line := `{"type":"assistant","message":{"content":[{"type":"tool_use","name":"Read","input":{"file_path":"/opt/agent/voyager/services/tachyon/main.go"}}]}}`
	evs, _ := DecodeStreamLine([]byte(line))
	if len(evs) != 1 {
		t.Fatalf("got %d events, want 1", len(evs))
	}
	if evs[0].Text != "/opt/agent/voyager/services/tachyon/main.go" {
		t.Errorf("text = %q, want the file path", evs[0].Text)
	}
}

func TestDecodeStreamLineEmitsToolResultTruncated(t *testing.T) {
	long := strings.Repeat("x", 900)
	line := `{"type":"user","message":{"content":[{"type":"tool_result","content":"` + long + `"}]}}`
	evs, _ := DecodeStreamLine([]byte(line))
	if len(evs) != 1 {
		t.Fatalf("got %d events, want 1", len(evs))
	}
	if evs[0].Kind != model.EventToolResult {
		t.Errorf("kind = %s, want tool_result", evs[0].Kind)
	}
	if len(evs[0].Text) > maxEventText+3 {
		t.Errorf("text length = %d, want truncated to %d", len(evs[0].Text), maxEventText)
	}
	if !strings.HasSuffix(evs[0].Text, "…") {
		t.Errorf("truncated text should end with an ellipsis, got %q", evs[0].Text[len(evs[0].Text)-10:])
	}
}

func TestDecodeStreamLineReturnsFinalResult(t *testing.T) {
	line := `{"type":"result","subtype":"success","result":"Root cause: unbounded cache.\nPR_URL: https://x/pull/1"}`
	evs, final := DecodeStreamLine([]byte(line))
	if final != "Root cause: unbounded cache.\nPR_URL: https://x/pull/1" {
		t.Errorf("final = %q", final)
	}
	if len(evs) != 1 || evs[0].Kind != model.EventResult {
		t.Errorf("events = %+v, want one result event", evs)
	}
}

func TestDecodeStreamLineEmitsSystemInitWithModel(t *testing.T) {
	line := `{"type":"system","subtype":"init","model":"claude-opus-5","tools":["Bash","Read"]}`
	evs, _ := DecodeStreamLine([]byte(line))
	if len(evs) != 1 {
		t.Fatalf("got %d events, want 1", len(evs))
	}
	if evs[0].Kind != model.EventSystem {
		t.Errorf("kind = %s, want system", evs[0].Kind)
	}
	if !strings.Contains(evs[0].Text, "claude-opus-5") {
		t.Errorf("text = %q, want it to name the model", evs[0].Text)
	}
}

func TestDecodeStreamLineIgnoresBlankAndNonJSONLines(t *testing.T) {
	for _, line := range []string{"", "   ", "not json at all", "{}"} {
		evs, final := DecodeStreamLine([]byte(line))
		if len(evs) != 0 || final != "" {
			t.Errorf("line %q produced events=%+v final=%q, want nothing", line, evs, final)
		}
	}
}

func TestDecodeStreamLineSkipsEmptyTextBlocks(t *testing.T) {
	line := `{"type":"assistant","message":{"content":[{"type":"text","text":"   "}]}}`
	evs, _ := DecodeStreamLine([]byte(line))
	if len(evs) != 0 {
		t.Errorf("got %d events, want 0 for whitespace-only text", len(evs))
	}
}

func TestScanStreamEmitsEventsInOrderAndReturnsFinalResult(t *testing.T) {
	in := strings.Join([]string{
		`{"type":"system","subtype":"init","model":"claude-opus-5"}`,
		`{"type":"assistant","message":{"content":[{"type":"tool_use","name":"Bash","input":{"command":"ls"}}]}}`,
		`{"type":"assistant","message":{"content":[{"type":"text","text":"done looking"}]}}`,
		`{"type":"result","subtype":"success","result":"the answer"}`,
	}, "\n")

	var got []model.AgentEvent
	final, err := scanStream(strings.NewReader(in), func(e model.AgentEvent) { got = append(got, e) })
	if err != nil {
		t.Fatalf("scanStream: %v", err)
	}
	if final != "the answer" {
		t.Errorf("final = %q", final)
	}
	kinds := []model.EventKind{}
	for _, e := range got {
		kinds = append(kinds, e.Kind)
	}
	want := []model.EventKind{model.EventSystem, model.EventTool, model.EventText, model.EventResult}
	if len(kinds) != len(want) {
		t.Fatalf("kinds = %v, want %v", kinds, want)
	}
	for i := range want {
		if kinds[i] != want[i] {
			t.Errorf("kind[%d] = %s, want %s", i, kinds[i], want[i])
		}
	}
}

func TestScanStreamHandlesLinesLargerThanTheDefaultScannerBuffer(t *testing.T) {
	huge := strings.Repeat("y", 200_000)
	in := `{"type":"user","message":{"content":[{"type":"tool_result","content":"` + huge + `"}]}}` + "\n" +
		`{"type":"result","subtype":"success","result":"survived"}`

	var got []model.AgentEvent
	final, err := scanStream(strings.NewReader(in), func(e model.AgentEvent) { got = append(got, e) })
	if err != nil {
		t.Fatalf("scanStream: %v", err)
	}
	if final != "survived" {
		t.Errorf("final = %q, want the result after the huge line", final)
	}
	if len(got) != 2 {
		t.Fatalf("got %d events, want 2 (tool_result + result)", len(got))
	}
	if got[0].Kind != model.EventToolResult {
		t.Errorf("first kind = %s, want tool_result", got[0].Kind)
	}
}

func TestScanStreamAssignsIncreasingSequenceNumbers(t *testing.T) {
	in := strings.Join([]string{
		`{"type":"assistant","message":{"content":[{"type":"text","text":"one"}]}}`,
		`{"type":"assistant","message":{"content":[{"type":"text","text":"two"},{"type":"text","text":"three"}]}}`,
	}, "\n")

	var seqs []int
	if _, err := scanStream(strings.NewReader(in), func(e model.AgentEvent) { seqs = append(seqs, e.Seq) }); err != nil {
		t.Fatalf("scanStream: %v", err)
	}
	if len(seqs) != 3 {
		t.Fatalf("got %d events, want 3", len(seqs))
	}
	for i, s := range seqs {
		if s != i+1 {
			t.Errorf("seq[%d] = %d, want %d", i, s, i+1)
		}
	}
}
