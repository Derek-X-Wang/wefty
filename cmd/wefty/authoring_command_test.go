package main

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Derek-X-Wang/wefty/internal/workflowhelper"
)

// TestAuthoringCommandsRunWithoutAFabric is the property that matters at this
// seam. `wefty run` is what a job calls from inside a node, where it has no
// cluster identity, no endpoint and no credential — that is the whole reason
// the run mailbox exists. So these commands must be dispatched before the CLI
// opens a Fabric connection, exactly like `wefty node`. Routing them through
// the ordinary command path would make reporting depend on the authority the
// mailbox was built to remove.
func TestAuthoringCommandsRunWithoutAFabric(t *testing.T) {
	directory := t.TempDir()
	t.Setenv(workflowhelper.RunDirEnv, directory)
	t.Setenv("WEFTY_NODE_CONFIG", filepath.Join(t.TempDir(), "absent.json"))

	var stdout, stderr bytes.Buffer
	// No --fabric, no --l1, no --l3: a job has none of them.
	err := run(context.Background(), []string{"--json", "run", "gate", "--name", "vet", "--outcome", "pass"}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("wefty run gate: %v\n%s", err, stderr.String())
	}
	if !strings.Contains(stdout.String(), `"event"`) {
		t.Fatalf("--json printed %q, want the written event path", stdout.String())
	}
	entries, err := os.ReadDir(filepath.Join(directory, "events"))
	if err != nil || len(entries) != 1 {
		t.Fatalf("read the mailbox: %d entries, %v", len(entries), err)
	}

	workspace := t.TempDir()
	stdout.Reset()
	stderr.Reset()
	err = run(context.Background(),
		[]string{"workflow", "init", "demo", "--dir", filepath.Join(workspace, "workflows")}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("wefty workflow init: %v\n%s", err, stderr.String())
	}
	if _, err := os.Stat(filepath.Join(workspace, "workflows", "demo", "demo.sh")); err != nil {
		t.Fatalf("the scaffold wrote no starter: %v", err)
	}
}

// TestAuthoringCommandsReportUsageMistakes keeps an authoring error readable:
// a workflow author is the user of this surface, and a flag typo inside a job
// must say what is wrong rather than fail the run with a bare exit code.
func TestAuthoringCommandsReportUsageMistakes(t *testing.T) {
	t.Setenv(workflowhelper.RunDirEnv, t.TempDir())
	t.Setenv("WEFTY_NODE_CONFIG", filepath.Join(t.TempDir(), "absent.json"))
	cases := []struct {
		args    []string
		message string
	}{
		{[]string{"run"}, "subcommand is required"},
		{[]string{"run", "gate", "--name", "vet"}, "requires --outcome"},
		{[]string{"run", "gate", "--name", "vet", "--outcome", "maybe"}, "is not one of"},
		{[]string{"run", "envelope"}, "requires --step"},
		{[]string{"run", "nonsense"}, "unknown wefty run subcommand"},
		{[]string{"workflow", "init"}, "usage: wefty workflow init"},
		{[]string{"workflow", "init", "demo", "--lang", "ts"}, "needs a bundle step"},
		{[]string{"workflow", "init", "demo", "--lang", "perl"}, "is not bash"},
		{[]string{"workflow", "init", "9lives"}, "must start with a letter"},
	}
	for _, test := range cases {
		var stdout, stderr bytes.Buffer
		err := run(context.Background(), test.args, &stdout, &stderr)
		if err == nil {
			t.Fatalf("wefty %s succeeded", strings.Join(test.args, " "))
		}
		if !strings.Contains(err.Error(), test.message) {
			t.Fatalf("wefty %s failed with %q, want it to mention %q",
				strings.Join(test.args, " "), err, test.message)
		}
		if _, ok := err.(usageError); !ok {
			t.Fatalf("wefty %s returned %T, want a usage error", strings.Join(test.args, " "), err)
		}
	}
}

// TestAuthoringHelpAnswersWithoutAMailbox covers the case a person actually
// hits: reading the flags at a desk, where there is no run to report against.
// Answering "this job has no run mailbox" to a request for help is useless.
func TestAuthoringHelpAnswersWithoutAMailbox(t *testing.T) {
	t.Setenv(workflowhelper.RunDirEnv, "")
	t.Setenv("WEFTY_NODE_CONFIG", filepath.Join(t.TempDir(), "absent.json"))
	for _, args := range [][]string{
		{"run", "--help"},
		{"run", "gate", "--help"},
		{"run", "params", "-h"},
		{"workflow", "--help"},
		{"workflow", "init", "--help"},
	} {
		var stdout, stderr bytes.Buffer
		if err := run(context.Background(), args, &stdout, &stderr); err != nil {
			t.Fatalf("wefty %s: %v", strings.Join(args, " "), err)
		}
		if !strings.Contains(stdout.String(), "Usage: wefty ") {
			t.Fatalf("wefty %s printed %q, want usage", strings.Join(args, " "), stdout.String())
		}
	}
}

// TestRunSubcommandsRefuseAnEventTheAgentWouldHaveToTruncate keeps a bound the
// author can act on. An oversize header pushes the file past the contract's
// 64 KiB event bound, the agent truncates it before the "--" separator, and the
// parser then refuses the whole event — so the evidence is lost after the CLI
// has already reported success.
func TestRunSubcommandsRefuseAnEventTheAgentWouldHaveToTruncate(t *testing.T) {
	directory := t.TempDir()
	t.Setenv(workflowhelper.RunDirEnv, directory)
	t.Setenv("WEFTY_NODE_CONFIG", filepath.Join(t.TempDir(), "absent.json"))
	var stdout, stderr bytes.Buffer
	err := run(context.Background(), []string{"run", "envelope", "--step", "build",
		"--summary", strings.Repeat("x", 4096)}, &stdout, &stderr)
	if err == nil {
		t.Fatal("an oversize summary was accepted")
	}
	if !strings.Contains(err.Error(), "--summary") || !strings.Contains(err.Error(), "bounds it at") {
		t.Fatalf("refused with %q, want the flag and its bound", err)
	}
	entries, _ := os.ReadDir(filepath.Join(directory, "events"))
	if len(entries) != 0 {
		t.Fatalf("a refused event still published %d files", len(entries))
	}
}

// TestRunResultKeepsTheWholeDocumentInTheHandoffDirectory separates the two
// bounds that apply to a result. The handoff copy is what an operator reads
// off the node, so it is written whole; bounding it at the event size turned
// an ordinary large result into a truncated file that still looked valid. The
// event's payload is bounded, because one event has to fit the protocol.
func TestRunResultKeepsTheWholeDocumentInTheHandoffDirectory(t *testing.T) {
	directory := t.TempDir()
	handoff := t.TempDir()
	t.Setenv(workflowhelper.RunDirEnv, directory)
	t.Setenv(workflowhelper.HandoffDirEnv, handoff)
	t.Setenv("WEFTY_NODE_CONFIG", filepath.Join(t.TempDir(), "absent.json"))

	// 200 KiB of perfectly valid JSON: far past one event, nowhere near a
	// reason to refuse a result document.
	document, err := json.Marshal(map[string]string{"schema_version": "1", "detail": strings.Repeat("d", 200<<10)})
	if err != nil {
		t.Fatal(err)
	}
	source := filepath.Join(t.TempDir(), "result.json")
	if err := os.WriteFile(source, document, 0o600); err != nil {
		t.Fatal(err)
	}

	var stdout, stderr bytes.Buffer
	if err := run(context.Background(), []string{"run", "result", "--file", source, "--status", "succeeded"}, &stdout, &stderr); err != nil {
		t.Fatalf("wefty run result: %v\n%s", err, stderr.String())
	}

	copied, err := os.ReadFile(filepath.Join(handoff, workflowhelper.ResultFileName))
	if err != nil {
		t.Fatalf("read the handoff result: %v", err)
	}
	if !bytes.Equal(copied, document) {
		t.Fatalf("the handoff copy is %d bytes, want the whole %d byte document", len(copied), len(document))
	}
	if !json.Valid(copied) {
		t.Fatal("the handoff copy is not a JSON document")
	}

	events, err := os.ReadDir(filepath.Join(directory, "events"))
	if err != nil || len(events) != 1 {
		t.Fatalf("read the mailbox: %d entries, %v", len(events), err)
	}
	raw, err := os.ReadFile(filepath.Join(directory, "events", events[0].Name()))
	if err != nil {
		t.Fatal(err)
	}
	if len(raw) > 64<<10 {
		t.Fatalf("the event is %d bytes; one event must fit the protocol bound", len(raw))
	}
	// The event says text, not json: a truncated JSON document is not one, and
	// an event that claims json and fails to decode is refused outright.
	if !strings.Contains(string(raw), "\npayload: text\n") {
		t.Fatalf("the truncated event does not declare a text payload:\n%s", raw[:512])
	}
	if !strings.Contains(string(raw), "truncated by wefty run") {
		t.Fatal("the truncated event carries no truncation marker")
	}
}

// TestRunSubcommandsRefuseASubstitutedMailboxDirectory covers the mailbox
// being a directory the workload owns: `events` can be a symlink by the time
// the helper runs, and O_EXCL on the leaf would not notice, because it is the
// ancestor that was replaced.
func TestRunSubcommandsRefuseASubstitutedMailboxDirectory(t *testing.T) {
	directory := t.TempDir()
	elsewhere := filepath.Join(directory, "elsewhere")
	if err := os.Mkdir(elsewhere, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(elsewhere, filepath.Join(directory, "events")); err != nil {
		t.Fatal(err)
	}
	t.Setenv(workflowhelper.RunDirEnv, directory)
	t.Setenv("WEFTY_NODE_CONFIG", filepath.Join(t.TempDir(), "absent.json"))

	var stdout, stderr bytes.Buffer
	err := run(context.Background(), []string{"run", "gate", "--name", "vet", "--outcome", "pass"}, &stdout, &stderr)
	if err == nil {
		t.Fatal("a symlinked events directory was accepted")
	}
	if !strings.Contains(err.Error(), "is not a directory") {
		t.Fatalf("refused with %q, want it to name the substituted directory", err)
	}
	entries, _ := os.ReadDir(elsewhere)
	if len(entries) != 0 {
		t.Fatalf("the redirected directory received %d files", len(entries))
	}
}

// TestRootUsageNamesTheAuthoringCommands keeps the surface discoverable: a
// workflow author who never reads the contract should find `wefty run` by
// typing `wefty help`.
func TestRootUsageNamesTheAuthoringCommands(t *testing.T) {
	for _, fragment := range []string{"run <envelope|step|gate|result|params>", "workflow init NAME"} {
		if !strings.Contains(rootUsage, fragment) {
			t.Fatalf("wefty's root usage does not mention %q", fragment)
		}
	}
}
