package llm

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func fakeClaude(t *testing.T, script string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "claude")
	if err := os.WriteFile(p, []byte("#!/bin/sh\n"+script+"\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestCLIErrorIncludesStdoutWhereClaudeReportsSessionLimits(t *testing.T) {
	bin := fakeClaude(t, `echo "You've hit your session limit · resets 1:50pm (UTC)"; exit 1`)
	_, err := NewCLIClient(bin, "m").Complete(context.Background(), "s", "u")
	if err == nil {
		t.Fatal("expected an error")
	}
	if !strings.Contains(err.Error(), "session limit") {
		t.Errorf("error = %q, want it to carry the CLI's stdout message", err)
	}
}

func TestCLIErrorStillIncludesStderr(t *testing.T) {
	bin := fakeClaude(t, `echo "boom on stderr" 1>&2; exit 1`)
	_, err := NewCLIClient(bin, "m").Complete(context.Background(), "s", "u")
	if err == nil {
		t.Fatal("expected an error")
	}
	if !strings.Contains(err.Error(), "boom on stderr") {
		t.Errorf("error = %q, want it to carry stderr too", err)
	}
}

func TestUnavailableDetectsAnExhaustedOrUnusableModel(t *testing.T) {
	for _, msg := range []string{
		"claude cli: exit status 1: You've hit your session limit · resets 1:50pm (UTC)",
		"claude cli: exit status 1: Usage limit reached",
		"anthropic api 401: authentication_error",
		"anthropic api 429: rate_limit_error",
		"claude cli: exec: \"claude\": executable file not found in $PATH",
		"anthropic api 529: overloaded_error",
	} {
		if !Unavailable(errFrom(msg)) {
			t.Errorf("Unavailable(%q) = false, want true", msg)
		}
	}
}

func TestUnavailableIgnoresOrdinaryFailures(t *testing.T) {
	for _, msg := range []string{
		"decode anthropic response: unexpected end of JSON input",
		"claude cli: exit status 1: could not parse the prompt",
		"context deadline exceeded",
	} {
		if Unavailable(errFrom(msg)) {
			t.Errorf("Unavailable(%q) = true, want false", msg)
		}
	}
}

func TestUnavailableIsFalseForNoError(t *testing.T) {
	if Unavailable(nil) {
		t.Error("Unavailable(nil) = true, want false")
	}
}

type strErr string

func (e strErr) Error() string { return string(e) }

func errFrom(s string) error { return strErr(s) }
