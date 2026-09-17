package workflowhelper

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// The `wefty run` flags are one caller of this package; Event and Write are
// exported, so they are a contract of their own. These cover what a caller
// reaching past the flags can still get wrong — and what the agent would then
// have to refuse, after the caller had already been told the event was written.

func TestValidateRefusesAPayloadThatOnlyClaimsToBeJSON(t *testing.T) {
	event := Event{Kind: KindResult, PayloadFormat: PayloadJSON, Payload: []byte("{not-json}")}
	err := event.Validate()
	if err == nil {
		t.Fatal("a json payload that is not JSON was accepted")
	}
	if !strings.Contains(err.Error(), "marked json") {
		t.Fatalf("refused with %q, want it to name the mismatch", err)
	}
	// The agent decodes a json payload before nesting it, and its refusal
	// costs the whole event. Caught here, it costs a message.
	var usage UsageError
	if !errors.As(err, &usage) {
		t.Fatalf("refused with %T, want a usage error the CLI can render", err)
	}
}

func TestValidateAcceptsRealJSONAndDowngradesATruncatedPayload(t *testing.T) {
	valid := Event{Kind: KindResult, PayloadFormat: PayloadJSON, Payload: []byte(`{"passed":true}`)}
	if err := valid.Validate(); err != nil {
		t.Fatalf("a JSON payload was refused: %v", err)
	}
	if valid.PayloadFormat != PayloadJSON {
		t.Fatalf("format is %q, want json", valid.PayloadFormat)
	}

	// A truncated JSON document is not a JSON document. Refusing it would lose
	// the verdict to a large payload, so the format is downgraded and the
	// truncation is marked instead.
	oversize := Event{Kind: KindResult, PayloadFormat: PayloadJSON,
		Payload: append([]byte(`{"detail":"`), make([]byte, maxPayloadBytes)...)}
	if err := oversize.Validate(); err != nil {
		t.Fatalf("an oversize payload was refused: %v", err)
	}
	if oversize.PayloadFormat != PayloadText {
		t.Fatalf("format is %q, want text after truncation", oversize.PayloadFormat)
	}
	if !strings.Contains(string(oversize.Payload), "truncated by wefty run") {
		t.Fatal("a truncated payload carries no marker")
	}
}

func TestValidateRefusesAnIdentifierRatherThanRewritingIt(t *testing.T) {
	// The agent rewrites an over-long identifier, and a rewritten key is a
	// different idempotency identity: the same event published twice would
	// become two documents. Refusing is the only answer that keeps the
	// caller's identity the caller's.
	for _, test := range []struct {
		name  string
		event Event
		flag  string
	}{
		{"step", Event{Kind: KindEnvelope, Step: strings.Repeat("s", 256)}, "--step"},
		{"name", Event{Kind: KindGate, Name: strings.Repeat("n", 256), Outcome: "pass"}, "--name"},
		{"key", Event{Kind: KindEnvelope, Step: "build", Key: strings.Repeat("k", 256)}, "--key"},
		{"summary", Event{Kind: KindEnvelope, Step: "build", Summary: strings.Repeat("m", 2049)}, "--summary"},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := test.event.Validate()
			if err == nil {
				t.Fatal("an over-long value was accepted")
			}
			if !strings.Contains(err.Error(), test.flag) || !strings.Contains(err.Error(), "bounds it at") {
				t.Fatalf("refused with %q, want the flag and its bound", err)
			}
		})
	}
}

func TestWriteRefusesAMailboxDirectoryThatIsALink(t *testing.T) {
	directory := t.TempDir()
	elsewhere := filepath.Join(directory, "elsewhere")
	if err := os.Mkdir(elsewhere, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(elsewhere, filepath.Join(directory, "tmp")); err != nil {
		t.Fatal(err)
	}
	if _, err := Write(directory, Event{Kind: KindGate, Name: "vet", Outcome: "pass"}); err == nil {
		t.Fatal("a symlinked staging directory was accepted")
	} else if !strings.Contains(err.Error(), "is not a directory") {
		t.Fatalf("refused with %q, want it to name the substituted directory", err)
	}
}

func TestWritePublishesA0600RegularFile(t *testing.T) {
	directory := t.TempDir()
	path, err := Write(directory, Event{Kind: KindGate, Name: "vet", Outcome: "pass", Summary: "clean"})
	if err != nil {
		t.Fatalf("write: %v", err)
	}
	info, err := os.Lstat(path)
	if err != nil {
		t.Fatal(err)
	}
	if !info.Mode().IsRegular() {
		t.Fatalf("published %s", info.Mode())
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("published mode %s, want 0600", info.Mode().Perm())
	}
	// Nothing is left staged: the rename is the only "done writing" signal.
	staged, err := os.ReadDir(filepath.Join(directory, stagingDirectory))
	if err != nil {
		t.Fatal(err)
	}
	if len(staged) != 0 {
		t.Fatalf("%d files left in tmp/", len(staged))
	}
}
