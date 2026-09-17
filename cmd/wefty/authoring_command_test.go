package main

import (
	"bytes"
	"context"
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
