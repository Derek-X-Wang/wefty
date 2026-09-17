package agent

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/internal/workflowhelper"
)

// The run mailbox is a file protocol, not a library call: the agent parses
// what a workload wrote, and the `wefty run` subcommands are only the
// recommended producer of it. That makes the two sides independently
// implementable — which is the point, because an OCI image without the wefty
// binary must be able to report with a few lines of shell — and it makes them
// independently wrong. These tests live here, next to the parser that decides,
// rather than beside the producer that hopes.
//
// The parser is authority; nothing below changes it.

// createdAtLine is normalized before two producers' bytes are compared: both
// stamp the second they ran in, and two processes need not share one.
var createdAtLine = regexp.MustCompile(`(?m)^created-at: .*$`)

var (
	weftyBinaryOnce sync.Once
	weftyBinaryPath string
	weftyBinaryErr  error
)

// weftyBinary builds the CLI once per test binary. These tests drive the real
// executable rather than calling the package in process, because the thing
// being proved is what a workflow's `wefty run` invocations leave on disk —
// and every one of those is a separate process with its own in-memory state.
// Calling the package five times in one process would share a sequence counter
// no real workflow has.
func weftyBinary(t *testing.T) string {
	t.Helper()
	weftyBinaryOnce.Do(func() {
		directory, err := os.MkdirTemp("", "wefty-conformance-")
		if err != nil {
			weftyBinaryErr = err
			return
		}
		weftyBinaryPath = filepath.Join(directory, "wefty")
		build := exec.Command("go", "build", "-o", weftyBinaryPath, "../cmd/wefty")
		if output, err := build.CombinedOutput(); err != nil {
			weftyBinaryErr = fmt.Errorf("go build ./cmd/wefty: %w\n%s", err, output)
		}
	})
	if weftyBinaryErr != nil {
		t.Fatalf("build the wefty CLI: %v", weftyBinaryErr)
	}
	return weftyBinaryPath
}

// runWefty invokes one `wefty run` subcommand as its own process, the way a
// workflow does.
func runWefty(t *testing.T, directory string, arguments ...string) string {
	t.Helper()
	command := exec.Command(weftyBinary(t), append([]string{"run"}, arguments...)...)
	command.Env = append(os.Environ(),
		workflowhelper.RunDirEnv+"="+directory,
		workflowhelper.HandoffDirEnv+"="+t.TempDir(),
	)
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("wefty run %s: %v\n%s", strings.Join(arguments, " "), err, output)
	}
	return string(output)
}

// TestRunSubcommandsProduceEventsTheAgentMailboxAccepts drives each `wefty
// run` subcommand and feeds what it wrote to the agent's own parser, then
// builds the ledger document from the parsed event and validates it against
// the v1 protocol schemas. A helper that writes a file the agent refuses would
// silently cost a run its evidence.
func TestRunSubcommandsProduceEventsTheAgentMailboxAccepts(t *testing.T) {
	directory := t.TempDir()

	evidence := filepath.Join(t.TempDir(), "evidence.txt")
	if err := os.WriteFile(evidence, []byte("two tests failed\n"), 0o600); err != nil {
		t.Fatalf("write evidence: %v", err)
	}
	result := filepath.Join(t.TempDir(), "result.json")
	if err := os.WriteFile(result, []byte(`{"schema_version":1,"passed":false}`), 0o600); err != nil {
		t.Fatalf("write result: %v", err)
	}
	detail := filepath.Join(t.TempDir(), "detail.txt")
	if err := os.WriteFile(detail, []byte("built 12 packages\n"), 0o600); err != nil {
		t.Fatalf("write detail: %v", err)
	}

	commands := [][]string{
		{"step", "--name", "build", "--summary", "compiling"},
		{"envelope", "--step", "build", "--status", "succeeded", "--summary", "built", "--payload-file", detail},
		{"step", "--name", "build", "--end", "--summary", "compiled"},
		{"gate", "--name", "vet", "--outcome", "fail", "--summary", "vet failed", "--evidence-file", evidence},
		{"result", "--file", result, "--status", "failed", "--summary", "one gate failed"},
	}
	// Each of these is a separate process, so the published order below is the
	// order the file names produce and not an artifact of one process's
	// counter.
	for _, command := range commands {
		runWefty(t, directory, command...)
	}

	mailbox := &runMailbox{runID: "run_conformance", attemptID: "attempt_conformance"}
	events := readMailboxEvents(t, directory)
	if len(events) != len(commands) {
		t.Fatalf("wrote %d events, want %d", len(events), len(commands))
	}
	kinds := []string{}
	for _, entry := range events {
		event, err := parseRunMailboxEvent(entry.raw)
		if err != nil {
			t.Fatalf("the agent refused %s: %v\n%s", entry.name, err, entry.raw)
		}
		kinds = append(kinds, event.kind)
		collection, document, err := mailbox.document(event, entry.name)
		if err != nil {
			t.Fatalf("the agent could not build a document from %s: %v", entry.name, err)
		}
		// The agent omits attempt_id on purpose: only L3 knows which attempt
		// its run token is bound to, and it binds the field before validating.
		// The test binds it the same way so the schema sees the document L3
		// actually validates.
		bound := bindAttemptID(t, document, mailbox.attemptID)
		switch collection {
		case runLedgerGateCollection:
			if err := contract.ValidateGateResultJSON(bound); err != nil {
				t.Fatalf("%s produced an invalid gate result: %v\n%s", entry.name, err, bound)
			}
		default:
			if err := contract.ValidateEnvelopeJSON(bound); err != nil {
				t.Fatalf("%s produced an invalid envelope: %v\n%s", entry.name, err, bound)
			}
		}
	}
	// Order is the producer's only ordering signal: the agent publishes a
	// sweep in lexical file-name order, so the subcommands must name events so
	// that "step started" cannot arrive after "step ended".
	want := []string{"step", "envelope", "step", "gate", "result"}
	if strings.Join(kinds, ",") != strings.Join(want, ",") {
		t.Fatalf("published order %v, want %v", kinds, want)
	}

	// The specifics the ledger reader actually sees.
	gate, err := parseRunMailboxEvent(events[3].raw)
	if err != nil {
		t.Fatalf("parse the gate: %v", err)
	}
	if gate.name != "vet" || gate.outcome != "fail" || gate.step != "vet" {
		t.Fatalf("gate is %+v, want name/step vet with outcome fail", gate)
	}
	if string(gate.body) != "two tests failed\n" {
		t.Fatalf("gate evidence is %q, want the evidence file verbatim", gate.body)
	}
	envelope, err := parseRunMailboxEvent(events[1].raw)
	if err != nil {
		t.Fatalf("parse the envelope: %v", err)
	}
	if envelope.step != "build" || envelope.status != string(contract.EnvelopeSucceeded) {
		t.Fatalf("envelope is %+v, want step build succeeded", envelope)
	}
	// A result document is JSON, and the agent must be able to nest it rather
	// than escape it as text.
	resultEvent, err := parseRunMailboxEvent(events[4].raw)
	if err != nil {
		t.Fatalf("parse the result: %v", err)
	}
	if !resultEvent.payloadJSON {
		t.Fatal("wefty run result did not mark a JSON result document as a json payload")
	}
}

// TestInlineBashWriterProducesByteIdenticalEvents holds the claim the scaffold
// makes to every OCI author: the ~25 lines of shell it emits for an image
// without the wefty binary are not a second, subtly different protocol.
func TestInlineBashWriterProducesByteIdenticalEvents(t *testing.T) {
	bashPath, err := exec.LookPath("bash")
	if err != nil {
		if os.Getenv("GITHUB_ACTIONS") == "true" {
			t.Fatalf("bash is required in CI: %v", err)
		}
		t.Skip("bash is required to exercise the inline writer")
	}
	payload := filepath.Join(t.TempDir(), "payload.txt")
	if err := os.WriteFile(payload, []byte("line one\nline two\n"), 0o600); err != nil {
		t.Fatalf("write payload: %v", err)
	}

	cases := []struct {
		name string
		// command is the `wefty run` form; inline is the equivalent call to
		// the inline writer's wefty_event.
		command []string
		inline  []string
	}{
		{
			name:    "step",
			command: []string{"step", "--name", "build", "--summary", "compiling"},
			inline:  []string{"step", "build", "", "started", "", "compiling", "text", "", ""},
		},
		{
			name:    "envelope",
			command: []string{"envelope", "--step", "build", "--status", "partial", "--summary", "half done", "--payload-file", payload},
			inline:  []string{"envelope", "", "build", "partial", "", "half done", "text", payload, ""},
		},
		{
			name:    "gate",
			command: []string{"gate", "--name", "vet", "--outcome", "error", "--summary", "vet could not run", "--evidence-file", payload},
			inline:  []string{"gate", "vet", "", "", "error", "vet could not run", "text", payload, ""},
		},
		{
			// A summary carrying a newline and a tab is the case a hand-rolled
			// writer gets wrong: an unfolded value becomes a bogus header line
			// and the agent refuses the whole event.
			name:    "folded summary and explicit key",
			command: []string{"envelope", "--step", "test", "--summary", "  two\tfailures\nsee the log  ", "--key", "test.1"},
			inline:  []string{"envelope", "", "test", "succeeded", "", "  two\tfailures\nsee the log  ", "text", "", "test.1"},
		},
	}

	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			fromCommand := writeThroughSubcommand(t, test.command)
			fromInline := writeThroughInlineWriter(t, bashPath, test.inline)
			if !bytes.Equal(normalizeCreatedAt(fromCommand), normalizeCreatedAt(fromInline)) {
				t.Fatalf("the two producers disagree.\nwefty run:\n%s\ninline writer:\n%s", fromCommand, fromInline)
			}
			// Both must still be events the agent accepts, not two copies of
			// the same mistake.
			if _, err := parseRunMailboxEvent(fromInline); err != nil {
				t.Fatalf("the agent refused the inline writer's event: %v\n%s", err, fromInline)
			}
			if !createdAtLine.Match(fromInline) {
				t.Fatalf("the inline writer stamped no created-at:\n%s", fromInline)
			}
		})
	}
}

// TestWorkflowInitProducesARunnableStarter scaffolds a workflow and runs it the
// way a node would: a mailbox directory, a handoff directory, params on disk
// and no wefty binary on PATH. The starter has to report without any of the
// things it cannot have.
func TestWorkflowInitProducesARunnableStarter(t *testing.T) {
	bashPath, err := exec.LookPath("bash")
	if err != nil {
		if os.Getenv("GITHUB_ACTIONS") == "true" {
			t.Fatalf("bash is required in CI: %v", err)
		}
		t.Skip("bash is required to run a scaffolded starter")
	}
	workspace := t.TempDir()
	var stdout bytes.Buffer
	if err := workflowhelper.ExecuteWorkflow(
		[]string{"init", "demo", "--dir", filepath.Join(workspace, "workflows")}, false, &stdout); err != nil {
		t.Fatalf("wefty workflow init: %v", err)
	}
	starter := filepath.Join(workspace, "workflows", "demo", "demo.sh")
	for _, expected := range []string{starter,
		filepath.Join(workspace, "workflows", "demo", "README.md"),
		filepath.Join(workspace, "workflows", "demo", "demo_integration_test.go")} {
		if _, err := os.Stat(expected); err != nil {
			t.Fatalf("the scaffold did not write %s: %v", expected, err)
		}
	}
	if output, err := exec.Command(bashPath, "-n", starter).CombinedOutput(); err != nil {
		t.Fatalf("bash -n %s: %v\n%s", starter, err, output)
	}

	runDir := filepath.Join(workspace, "mailbox")
	handoffDir := filepath.Join(workspace, "handoff")
	if err := os.MkdirAll(runDir, 0o700); err != nil {
		t.Fatalf("create the mailbox: %v", err)
	}
	if err := os.WriteFile(filepath.Join(runDir, "params.json"), []byte(`{"subject":"wefty"}`), 0o600); err != nil {
		t.Fatalf("write params.json: %v", err)
	}
	command := exec.Command(bashPath, starter)
	// A system PATH is what makes this the inline-writer path: an image
	// without the wefty binary is the case the fallback exists for.
	command.Env = append(os.Environ(),
		workflowhelper.RunDirEnv+"="+runDir,
		workflowhelper.HandoffDirEnv+"="+handoffDir,
		"PATH=/usr/bin:/bin:/usr/sbin:/sbin",
	)
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("run %s: %v\n%s", starter, err, output)
	}

	mailbox := &runMailbox{runID: "run_scaffold", attemptID: "attempt_scaffold"}
	kinds := []string{}
	for _, entry := range readMailboxEvents(t, runDir) {
		event, err := parseRunMailboxEvent(entry.raw)
		if err != nil {
			t.Fatalf("the agent refused the starter's %s: %v\n%s", entry.name, err, entry.raw)
		}
		if _, _, err := mailbox.document(event, entry.name); err != nil {
			t.Fatalf("the agent could not build a document from %s: %v", entry.name, err)
		}
		kinds = append(kinds, event.kind)
	}
	sorted := append([]string(nil), kinds...)
	sort.Strings(sorted)
	want := "envelope,gate,result,step,step"
	if strings.Join(sorted, ",") != want {
		t.Fatalf("the starter reported %v, want %s\n%s", kinds, want, output)
	}
	// The result convention: an operator reads this off the node when a
	// failing attempt retains its handoff directory.
	if _, err := os.Stat(filepath.Join(handoffDir, workflowhelper.ResultFileName)); err != nil {
		t.Fatalf("the starter left no %s in the handoff directory: %v", workflowhelper.ResultFileName, err)
	}
	// The starter read its params rather than guessing.
	if !bytes.Contains(output, []byte("wefty")) {
		t.Fatalf("the starter did not use the submitted subject:\n%s", output)
	}
}

// TestRunSubcommandsRefuseWithoutARunDir covers the job kinds that receive no
// mailbox — an OCI job today. Reporting must fail loudly there, naming what is
// missing, rather than write into a directory nobody publishes from.
func TestRunSubcommandsRefuseWithoutARunDir(t *testing.T) {
	t.Setenv(workflowhelper.RunDirEnv, "")
	commands := [][]string{
		{"envelope", "--step", "build"},
		{"step", "--name", "build"},
		{"gate", "--name", "vet", "--outcome", "pass"},
		{"result", "--file", "/dev/null"},
		{"params"},
		{"params", "ref"},
	}
	for _, command := range commands {
		var stdout, stderr bytes.Buffer
		err := workflowhelper.ExecuteRun(command, false, &stdout, &stderr)
		if err == nil {
			t.Fatalf("wefty run %s succeeded without %s", strings.Join(command, " "), workflowhelper.RunDirEnv)
		}
		message := err.Error()
		for _, fragment := range []string{workflowhelper.RunDirEnv, "run mailbox", "run-execution-context.md"} {
			if !strings.Contains(message, fragment) {
				t.Fatalf("wefty run %s refused with %q, which does not mention %q",
					strings.Join(command, " "), message, fragment)
			}
		}
		if stdout.Len() != 0 {
			t.Fatalf("wefty run %s printed %q while refusing", strings.Join(command, " "), stdout.String())
		}
	}
}

// bindAttemptID does what L3 does before validation: it fills in the field the
// agent deliberately leaves out.
func bindAttemptID(t *testing.T, document []byte, attemptID string) []byte {
	t.Helper()
	fields := map[string]any{}
	if err := json.Unmarshal(document, &fields); err != nil {
		t.Fatalf("decode the agent's document: %v", err)
	}
	fields["attempt_id"] = attemptID
	bound, err := json.Marshal(fields)
	if err != nil {
		t.Fatalf("encode the bound document: %v", err)
	}
	return bound
}

type mailboxEntry struct {
	name string
	raw  []byte
}

// readMailboxEvents returns the published events in the lexical order the
// agent sweeps them in.
func readMailboxEvents(t *testing.T, directory string) []mailboxEntry {
	t.Helper()
	entries, err := os.ReadDir(filepath.Join(directory, "events"))
	if err != nil {
		t.Fatalf("read the mailbox: %v", err)
	}
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		names = append(names, entry.Name())
	}
	sort.Strings(names)
	events := make([]mailboxEntry, 0, len(names))
	for _, name := range names {
		raw, err := os.ReadFile(filepath.Join(directory, "events", name))
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
		events = append(events, mailboxEntry{name: name, raw: raw})
	}
	return events
}

// writeThroughSubcommand runs one `wefty run` subcommand against a fresh
// mailbox and returns the single event it wrote.
func writeThroughSubcommand(t *testing.T, command []string) []byte {
	t.Helper()
	directory := t.TempDir()
	runWefty(t, directory, command...)
	events := readMailboxEvents(t, directory)
	if len(events) != 1 {
		t.Fatalf("wefty run %s wrote %d events, want 1", strings.Join(command, " "), len(events))
	}
	return events[0].raw
}

// writeThroughInlineWriter sources the scaffold's inline writer verbatim and
// calls wefty_event with the equivalent arguments.
func writeThroughInlineWriter(t *testing.T, bashPath string, arguments []string) []byte {
	t.Helper()
	directory := t.TempDir()
	script := filepath.Join(t.TempDir(), "inline.sh")
	body := "#!/usr/bin/env bash\nset -u\n" + workflowhelper.InlineBashWriter() + "\nwefty_event \"$@\"\n"
	if err := os.WriteFile(script, []byte(body), 0o700); err != nil {
		t.Fatalf("write the inline writer harness: %v", err)
	}
	command := exec.Command(bashPath, append([]string{script}, arguments...)...)
	command.Env = append(os.Environ(), workflowhelper.RunDirEnv+"="+directory)
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("inline writer %v: %v\n%s", arguments, err, output)
	}
	events := readMailboxEvents(t, directory)
	if len(events) != 1 {
		t.Fatalf("the inline writer wrote %d events, want 1", len(events))
	}
	return events[0].raw
}

func normalizeCreatedAt(raw []byte) []byte {
	headers, body, separated := bytes.Cut(raw, []byte("\n--\n"))
	if !separated {
		return createdAtLine.ReplaceAll(raw, []byte("created-at: <normalized>"))
	}
	normalized := createdAtLine.ReplaceAll(headers, []byte("created-at: <normalized>"))
	return append(append(normalized, []byte("\n--\n")...), body...)
}
