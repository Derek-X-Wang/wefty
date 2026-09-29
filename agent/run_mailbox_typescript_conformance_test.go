package agent

import (
	"bytes"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/internal/workflowhelper"
)

// The TypeScript scaffold inlines its own run-mailbox writer rather than
// depending on a package, which makes it a third producer of the file
// protocol. These tests hold it to the same bar as the other two: for every
// call it has a `wefty run` equivalent for, it writes the same bytes, and
// whatever it writes, the agent's parser accepts.
//
// The writer is run exactly as it ships, through Node's type stripping, so no
// build step and no network stand between the test and the source. Node 22.6
// or later is needed for that; without it these skip, unless
// WEFTY_REQUIRE_NODE=1 -- which the dogfood-workflow CI job sets -- turns the
// skip into a failure.

// typeScriptHarness is appended to the writer to drive one call from a JSON
// description. It is plain JavaScript on purpose: it is test plumbing, not
// part of what ships.
const typeScriptHarness = `
const spec = JSON.parse(process.argv[2]);
const options = {};
for (const field of ["summary", "key", "evidence"]) {
  if (spec[field] !== undefined) options[field] = spec[field];
}
if (spec.detailFile !== undefined) options.detail = readFileSync(spec.detailFile, "utf8");
if (spec.detailJSON !== undefined) options.detail = spec.detailJSON;
if (spec.evidenceFile !== undefined) options.evidence = readFileSync(spec.evidenceFile, "utf8");
switch (spec.call) {
  case "step": reportStep(spec.name, spec.status, options); break;
  case "envelope": reportEnvelope(spec.step, spec.status, options); break;
  case "gate": reportGate(spec.name, spec.outcome, options); break;
  case "result": reportResult(spec.document, spec.status, options); break;
  case "param": process.stdout.write(runParam(spec.name)); break;
  default: throw new Error("unknown call " + spec.call);
}
`

// requireNodeOrSkip skips when Node is unavailable, except where the caller's
// environment says Node is part of the lane.
func requireNodeOrSkip(t *testing.T, reason string) {
	t.Helper()
	if os.Getenv("WEFTY_REQUIRE_NODE") == "1" {
		t.Fatalf("WEFTY_REQUIRE_NODE=1: %s", reason)
	}
	t.Skip(reason)
}

// typeScriptNode returns the argv prefix that runs a .mts file with Node's
// type stripping: on by default from Node 23.6 and 22.18, behind a flag from
// 22.6.
func typeScriptNode(t *testing.T) []string {
	t.Helper()
	nodePath, err := exec.LookPath("node")
	if err != nil {
		requireNodeOrSkip(t, "node is required to run the TypeScript writer")
	}
	probe := filepath.Join(t.TempDir(), "probe.mts")
	if err := os.WriteFile(probe, []byte("const answer: number = 42;\nif (answer !== 42) process.exit(1);\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	for _, prefix := range [][]string{{nodePath}, {nodePath, "--experimental-strip-types"}} {
		if err := exec.Command(prefix[0], append(prefix[1:], probe)...).Run(); err == nil {
			return prefix
		}
	}
	version, _ := exec.Command(nodePath, "--version").Output()
	requireNodeOrSkip(t, "node "+strings.TrimSpace(string(version))+" cannot strip TypeScript types; Node 22.6 or later is needed")
	return nil
}

// runTypeScriptWriter runs one call through the inline writer and returns the
// mailbox and handoff directories it wrote into.
func runTypeScriptWriter(t *testing.T, node []string, spec map[string]any) (string, string, []byte, error) {
	t.Helper()
	harness := filepath.Join(t.TempDir(), "harness.mts")
	if err := os.WriteFile(harness, []byte(workflowhelper.InlineTypeScriptWriter()+typeScriptHarness), 0o600); err != nil {
		t.Fatalf("write the TypeScript writer harness: %v", err)
	}
	encoded, err := json.Marshal(spec)
	if err != nil {
		t.Fatal(err)
	}
	runDir := t.TempDir()
	handoff := t.TempDir()
	command := exec.Command(node[0], append(node[1:], harness, string(encoded))...)
	command.Env = append(os.Environ(),
		workflowhelper.RunDirEnv+"="+runDir,
		workflowhelper.HandoffDirEnv+"="+handoff,
	)
	output, err := command.CombinedOutput()
	return runDir, handoff, output, err
}

// writeThroughTypeScriptWriter runs one call and returns the single event it
// wrote, and the handoff directory.
func writeThroughTypeScriptWriter(t *testing.T, node []string, spec map[string]any) ([]byte, string) {
	t.Helper()
	runDir, handoff, output, err := runTypeScriptWriter(t, node, spec)
	if err != nil {
		t.Fatalf("TypeScript writer %v: %v\n%s", spec, err, output)
	}
	events := readMailboxEvents(t, runDir)
	if len(events) != 1 {
		t.Fatalf("the TypeScript writer wrote %d events, want 1", len(events))
	}
	return events[0].raw, handoff
}

// writeThroughSubcommandWithHandoff is writeThroughSubcommand with the handoff
// directory kept, so a result's handoff copy can be compared too.
func writeThroughSubcommandWithHandoff(t *testing.T, command []string) ([]byte, string) {
	t.Helper()
	directory := t.TempDir()
	handoff := t.TempDir()
	run := exec.Command(weftyBinary(t), append([]string{"run"}, command...)...)
	run.Env = append(os.Environ(),
		workflowhelper.RunDirEnv+"="+directory,
		workflowhelper.HandoffDirEnv+"="+handoff,
	)
	if output, err := run.CombinedOutput(); err != nil {
		t.Fatalf("wefty run %s: %v\n%s", strings.Join(command, " "), err, output)
	}
	events := readMailboxEvents(t, directory)
	if len(events) != 1 {
		t.Fatalf("wefty run %s wrote %d events, want 1", strings.Join(command, " "), len(events))
	}
	return events[0].raw, handoff
}

// assertAgentAcceptsEvent feeds one event to the agent's own parser, builds the
// ledger document from it and validates that against the v1 schema.
func assertAgentAcceptsEvent(t *testing.T, raw []byte) runMailboxEvent {
	t.Helper()
	event, err := parseRunMailboxEvent(raw)
	if err != nil {
		t.Fatalf("the agent refused the event: %v\n%s", err, raw)
	}
	mailbox := &runMailbox{runID: "run_typescript", attemptID: "attempt_typescript"}
	collection, document, err := mailbox.document(event, "typescript-event")
	if err != nil {
		t.Fatalf("the agent could not build a document from the event: %v\n%s", err, raw)
	}
	bound := bindAttemptID(t, document, mailbox.attemptID)
	if collection == runLedgerGateCollection {
		err = contract.ValidateGateResultJSON(bound)
	} else {
		err = contract.ValidateEnvelopeJSON(bound)
	}
	if err != nil {
		t.Fatalf("the event produced an invalid document: %v\n%s", err, bound)
	}
	return event
}

// TestInlineTypeScriptWriterMatchesRunByteForByte is the claim the TypeScript
// scaffold makes: its writer is not a second, subtly different protocol. Unlike
// the POSIX writer it can parse JSON and name a bad argument, so there is no
// carve-out -- every call below, the result's json payload included, must
// produce exactly the bytes `wefty run` produces.
func TestInlineTypeScriptWriterMatchesRunByteForByte(t *testing.T) {
	node := typeScriptNode(t)
	directory := t.TempDir()
	text := filepath.Join(directory, "detail.txt")
	if err := os.WriteFile(text, []byte("line one\nline two\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	// JSON.stringify's compact form, so the file and the object are the same
	// document byte for byte. The objects below are raw JSON because a Go map
	// would reorder their keys on the way to the harness.
	jsonDetail := filepath.Join(directory, "detail.json")
	if err := os.WriteFile(jsonDetail, []byte(`{"built":12,"packages":["a","b"]}`), 0o600); err != nil {
		t.Fatal(err)
	}
	result := filepath.Join(directory, "result.json")
	if err := os.WriteFile(result, []byte(`{"schema_version":1,"passed":false}`), 0o600); err != nil {
		t.Fatal(err)
	}

	cases := []struct {
		name       string
		command    []string
		typescript map[string]any
	}{
		{
			name:       "step started",
			command:    []string{"step", "--name", "build", "--summary", "compiling"},
			typescript: map[string]any{"call": "step", "name": "build", "status": "started", "summary": "compiling"},
		},
		{
			name:       "step ended",
			command:    []string{"step", "--name", "build", "--end", "--summary", "compiled"},
			typescript: map[string]any{"call": "step", "name": "build", "status": "ended", "summary": "compiled"},
		},
		{
			name:       "envelope with a text detail",
			command:    []string{"envelope", "--step", "build", "--status", "partial", "--summary", "half done", "--payload-file", text},
			typescript: map[string]any{"call": "envelope", "step": "build", "status": "partial", "summary": "half done", "detailFile": text},
		},
		{
			name:       "envelope with a JSON detail",
			command:    []string{"envelope", "--step", "build", "--status", "succeeded", "--payload-json-file", jsonDetail},
			typescript: map[string]any{"call": "envelope", "step": "build", "status": "succeeded", "detailJSON": json.RawMessage(`{"built":12,"packages":["a","b"]}`)},
		},
		{
			name:       "gate with evidence",
			command:    []string{"gate", "--name", "vet", "--outcome", "error", "--summary", "vet could not run", "--evidence-file", text},
			typescript: map[string]any{"call": "gate", "name": "vet", "outcome": "error", "summary": "vet could not run", "evidenceFile": text},
		},
		{
			// Folding is where a hand-rolled writer goes wrong: an unfolded
			// newline becomes a bogus header and the agent refuses the event.
			// The control byte is dropped and the non-ASCII text kept, byte for
			// byte.
			name:       "folded summary and explicit key",
			command:    []string{"envelope", "--step", "test", "--summary", "  two\tfailures\x01 — café\nsee the log  ", "--key", "test.1"},
			typescript: map[string]any{"call": "envelope", "step": "test", "status": "succeeded", "summary": "  two\tfailures\x01 — café\nsee the log  ", "key": "test.1"},
		},
		{
			name:       "result",
			command:    []string{"result", "--file", result, "--status", "failed", "--summary", "one gate failed"},
			typescript: map[string]any{"call": "result", "document": json.RawMessage(`{"schema_version":1,"passed":false}`), "status": "failed", "summary": "one gate failed"},
		},
	}

	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			fromCommand, commandHandoff := writeThroughSubcommandWithHandoff(t, test.command)
			fromTypeScript, typeScriptHandoff := writeThroughTypeScriptWriter(t, node, test.typescript)
			if !bytes.Equal(normalizeCreatedAt(fromCommand), normalizeCreatedAt(fromTypeScript)) {
				t.Fatalf("the two producers disagree.\nwefty run:\n%s\nTypeScript writer:\n%s", fromCommand, fromTypeScript)
			}
			if !createdAtLine.Match(fromTypeScript) {
				t.Fatalf("the TypeScript writer stamped no created-at:\n%s", fromTypeScript)
			}
			event := assertAgentAcceptsEvent(t, fromTypeScript)
			if test.typescript["call"] != "result" {
				return
			}
			if !event.payloadJSON {
				t.Fatal("the TypeScript writer did not mark a JSON result document as a json payload")
			}
			fromCommandCopy, err := os.ReadFile(filepath.Join(commandHandoff, workflowhelper.ResultFileName))
			if err != nil {
				t.Fatal(err)
			}
			fromTypeScriptCopy, err := os.ReadFile(filepath.Join(typeScriptHandoff, workflowhelper.ResultFileName))
			if err != nil {
				t.Fatalf("the TypeScript writer left no %s: %v", workflowhelper.ResultFileName, err)
			}
			if !bytes.Equal(fromCommandCopy, fromTypeScriptCopy) {
				t.Fatalf("the handoff copies differ: %q vs %q", fromCommandCopy, fromTypeScriptCopy)
			}
		})
	}
}

// TestInlineTypeScriptWriterBoundsLikeRun covers the places a producer can
// cost a run its evidence after reporting success: an identifier or summary the
// agent would rewrite is refused with its name, and a payload past the event
// bound is truncated rather than refused, so the verdict still lands.
func TestInlineTypeScriptWriterBoundsLikeRun(t *testing.T) {
	node := typeScriptNode(t)

	t.Run("an over-long summary is refused and nothing is published", func(t *testing.T) {
		runDir, _, output, err := runTypeScriptWriter(t, node, map[string]any{
			"call": "gate", "name": "vet", "outcome": "fail", "summary": strings.Repeat("x", 4096)})
		if err == nil {
			t.Fatal("an over-long summary was accepted")
		}
		if !bytes.Contains(output, []byte("summary is 4096 bytes")) || !bytes.Contains(output, []byte("bounds it at 2048")) {
			t.Fatalf("refused with %s, want the field and its bound", output)
		}
		if entries, _ := os.ReadDir(filepath.Join(runDir, "events")); len(entries) != 0 {
			t.Fatalf("a refused event still published %d files", len(entries))
		}
	})

	t.Run("an over-long key is refused", func(t *testing.T) {
		_, _, output, err := runTypeScriptWriter(t, node, map[string]any{
			"call": "step", "name": "build", "status": "started", "key": strings.Repeat("k", 256)})
		if err == nil || !bytes.Contains(output, []byte("key is 256 bytes")) {
			t.Fatalf("an over-long key: %v\n%s", err, output)
		}
	})

	t.Run("oversize evidence is truncated, not lost", func(t *testing.T) {
		raw, _ := writeThroughTypeScriptWriter(t, node, map[string]any{
			"call": "gate", "name": "vet", "outcome": "fail", "evidence": strings.Repeat("e", 100<<10)})
		if len(raw) > workflowhelper.MaxEventBytes {
			t.Fatalf("the TypeScript writer produced a %d byte event", len(raw))
		}
		event := assertAgentAcceptsEvent(t, raw)
		if event.outcome != "fail" || !bytes.Contains(event.body, []byte("truncated by the inline run-mailbox writer")) {
			t.Fatalf("the truncated gate is %+v", event)
		}
	})

	t.Run("an oversize result keeps the whole document in the handoff directory", func(t *testing.T) {
		detail := strings.Repeat("d", 100<<10)
		raw, handoff := writeThroughTypeScriptWriter(t, node, map[string]any{
			"call": "result", "document": map[string]any{"detail": detail}, "status": "succeeded"})
		event := assertAgentAcceptsEvent(t, raw)
		if event.payloadJSON {
			t.Fatal("a truncated result still claims a json payload; the agent would refuse it")
		}
		copied, err := os.ReadFile(filepath.Join(handoff, workflowhelper.ResultFileName))
		if err != nil {
			t.Fatal(err)
		}
		if !json.Valid(copied) || !bytes.Contains(copied, []byte(detail)) {
			t.Fatalf("the handoff copy is %d bytes and not the whole document", len(copied))
		}
	})

	t.Run("no mailbox is a refusal that names it", func(t *testing.T) {
		harness := filepath.Join(t.TempDir(), "harness.mts")
		if err := os.WriteFile(harness, []byte(workflowhelper.InlineTypeScriptWriter()+typeScriptHarness), 0o600); err != nil {
			t.Fatal(err)
		}
		command := exec.Command(node[0], append(node[1:], harness, `{"call":"gate","name":"vet","outcome":"pass"}`)...)
		command.Env = append(os.Environ(), workflowhelper.RunDirEnv+"=")
		output, err := command.CombinedOutput()
		if err == nil || !bytes.Contains(output, []byte("WEFTY_RUN_DIR is not set")) {
			t.Fatalf("reporting without a mailbox: %v\n%s", err, output)
		}
	})
}

// TestInlineTypeScriptWriterReadsParamsLikeRun keeps `runParam` and
// `wefty run params NAME` answering the same question the same way.
func TestInlineTypeScriptWriterReadsParamsLikeRun(t *testing.T) {
	node := typeScriptNode(t)
	harness := filepath.Join(t.TempDir(), "harness.mts")
	if err := os.WriteFile(harness, []byte(workflowhelper.InlineTypeScriptWriter()+typeScriptHarness), 0o600); err != nil {
		t.Fatal(err)
	}
	runDir := t.TempDir()
	params := `{"ref":"feature/x \"quoted\"","count":3,"nested":{"ref":"no"}}`
	if err := os.WriteFile(filepath.Join(runDir, "params.json"), []byte(params), 0o600); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"ref", "count", "nested", "absent"} {
		command := exec.Command(node[0], append(node[1:], harness, `{"call":"param","name":"`+name+`"}`)...)
		command.Env = append(os.Environ(), workflowhelper.RunDirEnv+"="+runDir)
		fromTypeScript, err := command.Output()
		if err != nil {
			t.Fatalf("runParam(%q): %v", name, err)
		}
		helper := exec.Command(weftyBinary(t), "run", "params", name)
		helper.Env = append(os.Environ(), workflowhelper.RunDirEnv+"="+runDir)
		fromCommand, err := helper.Output()
		if err != nil {
			t.Fatalf("wefty run params %s: %v", name, err)
		}
		// The CLI ends its line; the function returns the value.
		if string(fromTypeScript) != strings.TrimSuffix(string(fromCommand), "\n") {
			t.Fatalf("param %s: runParam gave %q, wefty run params gave %q", name, fromTypeScript, fromCommand)
		}
	}
}

// TestTypeScriptWorkflowInitProducesAStarterTheAgentAccepts scaffolds a
// TypeScript workflow and runs its source the way the conformance tests run
// the writer: every event the starter reports must be one the agent accepts.
// The bundle a node actually runs is exercised end to end, through a real
// stack, by the WEFTY_TS_SCAFFOLD_EXERCISE test in cmd/wefty.
func TestTypeScriptWorkflowInitProducesAStarterTheAgentAccepts(t *testing.T) {
	node := typeScriptNode(t)
	workspace := t.TempDir()
	var stdout bytes.Buffer
	if err := workflowhelper.ExecuteWorkflow(
		[]string{"init", "demo", "--lang", "ts", "--dir", filepath.Join(workspace, "workflows")}, false, &stdout); err != nil {
		t.Fatalf("wefty workflow init --lang ts: %v", err)
	}
	source, err := os.ReadFile(filepath.Join(workspace, "workflows", "demo", "src", "demo.ts"))
	if err != nil {
		t.Fatal(err)
	}
	// .mts, because Node strips types only by extension.
	starter := filepath.Join(t.TempDir(), "demo.mts")
	if err := os.WriteFile(starter, source, 0o600); err != nil {
		t.Fatal(err)
	}
	runDir := t.TempDir()
	handoff := t.TempDir()
	if err := os.WriteFile(filepath.Join(runDir, "params.json"), []byte(`{"subject":"wefty"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	command := exec.Command(node[0], append(node[1:], starter)...)
	command.Env = append(os.Environ(),
		workflowhelper.RunDirEnv+"="+runDir,
		workflowhelper.HandoffDirEnv+"="+handoff,
	)
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("run the starter: %v\n%s", err, output)
	}
	kinds := []string{}
	for _, entry := range readMailboxEvents(t, runDir) {
		event := assertAgentAcceptsEvent(t, entry.raw)
		kinds = append(kinds, event.kind)
	}
	// Lexical file-name order is publication order, and one process's reports
	// must keep the order it made them in.
	if strings.Join(kinds, ",") != "step,envelope,step,gate,result" {
		t.Fatalf("the starter published %v\n%s", kinds, output)
	}
	raw, err := os.ReadFile(filepath.Join(handoff, workflowhelper.ResultFileName))
	if err != nil {
		t.Fatalf("the starter left no %s: %v", workflowhelper.ResultFileName, err)
	}
	if !bytes.Contains(raw, []byte(`"subject":"wefty"`)) || !bytes.Contains(raw, []byte(`"passed":true`)) {
		t.Fatalf("the starter's result is %s, want the submitted subject and a pass", raw)
	}
}
