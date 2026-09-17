//go:build darwin || linux

package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/l1"
	processrunner "github.com/Derek-X-Wang/wefty/runner/process"
)

const (
	mailboxTestRunID    = "run_mailbox"
	mailboxTestToken    = "wrun_mailbox_secret"
	mailboxTestAttempt  = "attempt-handoff"
	mailboxTestEndpoint = "http://127.0.0.1:9/"
)

type recordedDocument struct {
	collection string
	runToken   string
	runID      string
	body       []byte
	// handoffPresent records whether the run's handoff directory still existed
	// at the instant the agent published. The final sweep must run before the
	// handoff lifecycle can remove it.
	handoffPresent bool
}

type recordingAppender struct {
	mu        sync.Mutex
	documents []recordedDocument
	handoff   string
	fail      func(count int) error
	published chan struct{}
}

func newRecordingAppender(handoff string) *recordingAppender {
	return &recordingAppender{handoff: handoff, published: make(chan struct{}, 64)}
}

func (a *recordingAppender) appendRunDocument(_ context.Context, runToken, runID, collection string, body []byte) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.fail != nil {
		if err := a.fail(len(a.documents)); err != nil {
			return err
		}
	}
	present := false
	if a.handoff != "" {
		if _, err := os.Stat(a.handoff); err == nil {
			present = true
		}
	}
	a.documents = append(a.documents, recordedDocument{
		collection: collection, runToken: runToken, runID: runID,
		body: append([]byte(nil), body...), handoffPresent: present,
	})
	select {
	case a.published <- struct{}{}:
	default:
	}
	return nil
}

func (a *recordingAppender) snapshot() []recordedDocument {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]recordedDocument(nil), a.documents...)
}

func mailboxClaim(runID, handoff string) l1.Claim {
	claim := handoffClaim(runID, handoff, nil)
	claim.Job.Spec.Execution.Env = map[string]string{
		contract.EnvRunID:         runID,
		contract.EnvL3Endpoint:    mailboxTestEndpoint,
		contract.EnvHandoffDir:    handoff,
		contract.EnvRunParamsJSON: `{"ref":"main"}`,
	}
	claim.Job.Spec.Execution.SensitiveEnv = map[string]string{contract.EnvRunToken: mailboxTestToken}
	return claim
}

// mailboxRunner is a workload that writes mailbox events and optionally waits
// for the agent to publish them before it exits.
type mailboxRunner struct {
	write      func(runDirectory string) error
	waitFor    func() bool
	runDir     string
	sawRunDir  string
	sawParams  string
	runnerErr  error
	exitCode   int
	waitBudget time.Duration
}

func (r *mailboxRunner) Run(_ context.Context, request processrunner.Request, _ processrunner.OutputSink) (contract.ProcessResult, error) {
	if request.Started != nil {
		request.Started()
	}
	r.sawRunDir = request.Execution.Env[contract.EnvRunDir]
	r.runDir = r.sawRunDir
	if r.runDir != "" {
		if payload, err := os.ReadFile(filepath.Join(r.runDir, runMailboxParamsFileName)); err == nil {
			r.sawParams = string(payload)
		}
	}
	if r.write != nil {
		if err := r.write(r.runDir); err != nil {
			r.runnerErr = err
		}
	}
	if r.waitFor != nil {
		budget := r.waitBudget
		if budget == 0 {
			budget = 5 * time.Second
		}
		deadline := time.Now().Add(budget)
		for !r.waitFor() {
			if time.Now().After(deadline) {
				r.runnerErr = fmt.Errorf("workload timed out waiting for the agent to publish")
				break
			}
			time.Sleep(time.Millisecond)
		}
	}
	exitCode := r.exitCode
	return contract.ProcessResult{ExitCode: &exitCode}, nil
}

func writeMailboxEvent(runDirectory, name, content string) error {
	staging := filepath.Join(runDirectory, runMailboxStagingDirectoryName, name)
	if err := os.WriteFile(staging, []byte(content), 0o600); err != nil {
		return err
	}
	return os.Rename(staging, filepath.Join(runDirectory, runMailboxEventsDirectoryName, name))
}

func decodeEnvelope(t *testing.T, body []byte) contract.Envelope {
	t.Helper()
	var envelope contract.Envelope
	if err := json.Unmarshal(body, &envelope); err != nil {
		t.Fatalf("decode envelope %s: %v", body, err)
	}
	if err := contract.ValidateEnvelopeJSON(body); err != nil {
		t.Fatalf("envelope %s does not satisfy the v1 schema: %v", body, err)
	}
	return envelope
}

func TestMailboxEventsArePublishedWhileTheAttemptRuns(t *testing.T) {
	root := filepath.Join(t.TempDir(), "handoffs")
	handoff := filepath.Join(root, mailboxTestRunID)
	appender := newRecordingAppender(handoff)
	runner := &mailboxRunner{
		write: func(runDirectory string) error {
			return writeMailboxEvent(runDirectory, "0001-phase-clone",
				"wefty-protocol: 1\nkind: phase\nname: clone\nstatus: started\n--\n")
		},
		waitFor: func() bool { return len(appender.snapshot()) > 0 },
	}
	a := &Agent{
		registration: contract.NodeRegistration{NodeID: "node-1"},
		runtimes:     testRuntimeSet(runner),
		handoffs:     newHandoffManager(root, time.Hour),
		runLedger:    appender,
		mailboxPoll:  5 * time.Millisecond,
	}
	claim := mailboxClaim(mailboxTestRunID, handoff)
	result, err := a.runWorkload(context.Background(), claim)
	if err != nil || result.ExitCode == nil || *result.ExitCode != 0 {
		t.Fatalf("runWorkload() = (%#v, %v)", result, err)
	}
	if runner.runnerErr != nil {
		t.Fatalf("workload: %v", runner.runnerErr)
	}
	if want := filepath.Join(handoff, runMailboxDirectoryName); runner.sawRunDir != want {
		t.Fatalf("workload %s = %q, want %q", contract.EnvRunDir, runner.sawRunDir, want)
	}
	if strings.TrimSpace(runner.sawParams) != `{"ref":"main"}` {
		t.Fatalf("workload params = %q, want the dispatched run params", runner.sawParams)
	}
	documents := appender.snapshot()
	if len(documents) != 1 {
		t.Fatalf("published %d documents, want 1", len(documents))
	}
	if documents[0].collection != runLedgerEnvelopeCollection || documents[0].runToken != mailboxTestToken {
		t.Fatalf("published %+v, want an envelope carrying the agent-held run token", documents[0])
	}
	envelope := decodeEnvelope(t, documents[0].body)
	if envelope.StepID != "phase:clone" || envelope.Status != contract.EnvelopePartial {
		t.Fatalf("phase envelope = %+v, want a partial phase:clone envelope", envelope)
	}
	if envelope.RunID != mailboxTestRunID || envelope.AttemptID != mailboxTestAttempt {
		t.Fatalf("phase envelope identity = (%q, %q)", envelope.RunID, envelope.AttemptID)
	}
}

func TestMailboxReplayAfterAgentRestartDoesNotDuplicateEnvelopes(t *testing.T) {
	handoff := filepath.Join(t.TempDir(), mailboxTestRunID)
	if err := os.MkdirAll(handoff, 0o700); err != nil {
		t.Fatal(err)
	}
	appender := newRecordingAppender(handoff)
	// The first agent publishes the event but is lost before the cursor rename.
	first, err := prepareRunMailbox(mailboxClaim(mailboxTestRunID, handoff).Job.Spec, mailboxTestAttempt, appender, time.Hour, nil)
	if err != nil {
		t.Fatal(err)
	}
	event := "wefty-protocol: 1\nkind: gate\nname: vet\noutcome: pass\n--\nok\n"
	if err := writeMailboxEvent(first.directory, "0007-gate-vet", event); err != nil {
		t.Fatal(err)
	}
	first.retireCheckpoint = func() bool { return false }
	first.sweep(context.Background())
	if len(appender.snapshot()) != 1 {
		t.Fatalf("first sweep published %d documents, want 1", len(appender.snapshot()))
	}

	// A restarted agent re-reads the same file and republishes it. The bytes
	// must be identical so L3 replays the original document instead of
	// accepting a second one.
	second, err := prepareRunMailbox(mailboxClaim(mailboxTestRunID, handoff).Job.Spec, mailboxTestAttempt, appender, time.Hour, nil)
	if err != nil {
		t.Fatal(err)
	}
	second.sweep(context.Background())
	documents := appender.snapshot()
	if len(documents) != 2 {
		t.Fatalf("republished %d documents, want the same event twice", len(documents))
	}
	if string(documents[0].body) != string(documents[1].body) {
		t.Fatalf("republished document differs:\n%s\n%s", documents[0].body, documents[1].body)
	}
	var gate contract.GateResult
	if err := json.Unmarshal(documents[1].body, &gate); err != nil {
		t.Fatal(err)
	}
	if err := contract.ValidateGateResultJSON(documents[1].body); err != nil {
		t.Fatalf("gate does not satisfy the v1 schema: %v", err)
	}
	if gate.IdempotencyKey == "" || gate.Outcome != contract.GatePass || gate.Name != "vet" {
		t.Fatalf("gate = %+v", gate)
	}
	if len(gate.Evidence) != 1 || gate.Evidence[0].Value != "ok" {
		t.Fatalf("gate evidence = %+v, want the raw payload", gate.Evidence)
	}
	// A third sweep finds nothing: the event was retired under the cursor.
	second.sweep(context.Background())
	if len(appender.snapshot()) != 2 {
		t.Fatalf("retired event republished: %d documents", len(appender.snapshot()))
	}
}

func TestOversizeMailboxEventIsTruncatedNotDropped(t *testing.T) {
	handoff := filepath.Join(t.TempDir(), mailboxTestRunID)
	if err := os.MkdirAll(handoff, 0o700); err != nil {
		t.Fatal(err)
	}
	appender := newRecordingAppender(handoff)
	mailbox, err := prepareRunMailbox(mailboxClaim(mailboxTestRunID, handoff).Job.Spec, mailboxTestAttempt, appender, time.Hour, nil)
	if err != nil {
		t.Fatal(err)
	}
	header := "wefty-protocol: 1\nkind: gate\nname: test\noutcome: fail\n--\n"
	body := strings.Repeat("F", MaxRunMailboxEventBytes*2)
	if err := writeMailboxEvent(mailbox.directory, "0002-gate-test", header+body); err != nil {
		t.Fatal(err)
	}
	mailbox.sweep(context.Background())
	documents := appender.snapshot()
	if len(documents) != 1 {
		t.Fatalf("published %d documents, want the oversize event to survive truncation", len(documents))
	}
	var gate contract.GateResult
	if err := json.Unmarshal(documents[0].body, &gate); err != nil {
		t.Fatal(err)
	}
	if gate.Outcome != contract.GateFail {
		t.Fatalf("truncated gate outcome = %q, want the verdict preserved", gate.Outcome)
	}
	if len(gate.Evidence) != 1 {
		t.Fatalf("truncated gate evidence = %+v", gate.Evidence)
	}
	if !strings.Contains(gate.Evidence[0].Value, "truncated by the node agent") {
		t.Fatalf("truncated gate evidence does not say so: %.120q", gate.Evidence[0].Value)
	}
	if len(gate.Evidence[0].Value) > MaxRunMailboxEventBytes+len(runMailboxTruncationNotice) {
		t.Fatalf("truncated gate evidence is %d bytes, want at most the size bound", len(gate.Evidence[0].Value))
	}
	if err := contract.ValidateGateResultJSON(documents[0].body); err != nil {
		t.Fatalf("truncated gate does not satisfy the v1 schema: %v", err)
	}
}

func TestMailboxFinalSweepCompletesBeforeHandoffRemoval(t *testing.T) {
	root := filepath.Join(t.TempDir(), "handoffs")
	handoff := filepath.Join(root, mailboxTestRunID)
	appender := newRecordingAppender(handoff)
	runner := &mailboxRunner{
		write: func(runDirectory string) error {
			return writeMailboxEvent(runDirectory, "0009-result-verdict",
				"wefty-protocol: 1\nkind: result\nname: result.json\n--\n{\"passed\":true}\n")
		},
	}
	manager := newHandoffManager(root, time.Hour)
	a := &Agent{
		registration: contract.NodeRegistration{NodeID: "node-1"},
		runtimes:     testRuntimeSet(runner),
		handoffs:     manager,
		runLedger:    appender,
		// Long enough that no poll can fire: only the final sweep can publish.
		mailboxPoll: time.Hour,
	}
	claim := mailboxClaim(mailboxTestRunID, handoff)
	result, err := a.runWorkload(context.Background(), claim)
	if err != nil || result.ExitCode == nil || *result.ExitCode != 0 {
		t.Fatalf("runWorkload() = (%#v, %v)", result, err)
	}
	documents := appender.snapshot()
	if len(documents) != 1 {
		t.Fatalf("published %d documents, want the final sweep to publish the result", len(documents))
	}
	if !documents[0].handoffPresent {
		t.Fatal("the result was published after the handoff directory was removed")
	}
	envelope := decodeEnvelope(t, documents[0].body)
	if envelope.StepID != "result.json" || envelope.Status != contract.EnvelopeSucceeded {
		t.Fatalf("result envelope = %+v", envelope)
	}
	if err := manager.finish(claim.Job.Spec, "node-1", true); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(handoff); !os.IsNotExist(err) {
		t.Fatalf("successful handoff directory still exists: %v", err)
	}
}

func TestRunMailboxEventParsing(t *testing.T) {
	for _, testCase := range []struct {
		name    string
		raw     string
		wantErr string
		assert  func(*testing.T, runMailboxEvent)
	}{
		{
			name: "gate with a raw payload",
			raw:  "wefty-protocol: 1\nkind: gate\nname: gofmt\noutcome: fail\nstep: gofmt\n--\nmain.go\nutil.go\n",
			assert: func(t *testing.T, event runMailboxEvent) {
				if event.kind != runMailboxKindGate || event.outcome != "fail" || event.name != "gofmt" {
					t.Fatalf("event = %+v", event)
				}
				if string(event.body) != "main.go\nutil.go\n" {
					t.Fatalf("body = %q, want the payload verbatim", event.body)
				}
			},
		},
		{
			name: "envelope defaults to succeeded and mirrors name into step",
			raw:  "wefty-protocol: 1\nkind: envelope\nname: implement\n--\n",
			assert: func(t *testing.T, event runMailboxEvent) {
				if event.status != string(contract.EnvelopeSucceeded) || event.step != "implement" {
					t.Fatalf("event = %+v", event)
				}
			},
		},
		{
			name: "phase defaults to started",
			raw:  "wefty-protocol: 1\nkind: phase\nname: clone\n--\n",
			assert: func(t *testing.T, event runMailboxEvent) {
				if event.status != "started" {
					t.Fatalf("phase status = %q", event.status)
				}
			},
		},
		{
			name: "header-only event without a trailing newline",
			raw:  "wefty-protocol: 1\nkind: phase\nname: clone\nstatus: ended\n--",
			assert: func(t *testing.T, event runMailboxEvent) {
				if event.status != "ended" || len(event.body) != 0 {
					t.Fatalf("event = %+v", event)
				}
			},
		},
		{
			name: "a payload may contain the separator",
			raw:  "wefty-protocol: 1\nkind: gate\nname: vet\noutcome: pass\n--\nfirst\n--\nsecond\n",
			assert: func(t *testing.T, event runMailboxEvent) {
				if string(event.body) != "first\n--\nsecond\n" {
					t.Fatalf("body = %q", event.body)
				}
			},
		},
		{
			name:    "the protocol header must come first",
			raw:     "kind: gate\nwefty-protocol: 1\nname: vet\noutcome: pass\n--\n",
			wantErr: "must start with",
		},
		{
			name:    "an unsupported protocol version is refused",
			raw:     "wefty-protocol: 2\nkind: gate\nname: vet\noutcome: pass\n--\n",
			wantErr: "protocol version",
		},
		{
			name:    "a missing separator is refused",
			raw:     "wefty-protocol: 1\nkind: gate\nname: vet\noutcome: pass\n",
			wantErr: "payload separator",
		},
		{
			name:    "an unknown header is refused",
			raw:     "wefty-protocol: 1\nkind: gate\nname: vet\noutcome: pass\nrun-id: run_other\n--\n",
			wantErr: "not part of the protocol",
		},
		{
			name:    "a repeated header is refused",
			raw:     "wefty-protocol: 1\nkind: gate\nname: vet\nname: other\noutcome: pass\n--\n",
			wantErr: "appears twice",
		},
		{
			name:    "an unknown kind is refused",
			raw:     "wefty-protocol: 1\nkind: telemetry\nname: vet\n--\n",
			wantErr: "kind \"telemetry\"",
		},
		{
			name:    "a gate requires a known outcome",
			raw:     "wefty-protocol: 1\nkind: gate\nname: vet\noutcome: maybe\n--\n",
			wantErr: "outcome \"maybe\"",
		},
		{
			name:    "a gate requires a name",
			raw:     "wefty-protocol: 1\nkind: gate\noutcome: pass\n--\n",
			wantErr: "gate requires a name",
		},
		{
			name:    "only a gate carries an outcome",
			raw:     "wefty-protocol: 1\nkind: envelope\nname: implement\noutcome: pass\n--\n",
			wantErr: "does not carry an outcome",
		},
		{
			name:    "an unknown envelope status is refused",
			raw:     "wefty-protocol: 1\nkind: envelope\nname: implement\nstatus: maybe\n--\n",
			wantErr: "status \"maybe\"",
		},
		{
			name:    "an unknown payload format is refused",
			raw:     "wefty-protocol: 1\nkind: envelope\nname: implement\npayload: yaml\n--\n",
			wantErr: "payload format",
		},
		{
			name:    "a malformed created-at is refused",
			raw:     "wefty-protocol: 1\nkind: envelope\nname: implement\ncreated-at: yesterday\n--\n",
			wantErr: "created-at",
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			event, err := parseRunMailboxEvent([]byte(testCase.raw))
			if testCase.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), testCase.wantErr) {
					t.Fatalf("parse error = %v, want it to mention %q", err, testCase.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("parse: %v", err)
			}
			testCase.assert(t, event)
		})
	}
}

func TestMalformedMailboxEventIsReportedAndRetired(t *testing.T) {
	handoff := filepath.Join(t.TempDir(), mailboxTestRunID)
	if err := os.MkdirAll(handoff, 0o700); err != nil {
		t.Fatal(err)
	}
	appender := newRecordingAppender(handoff)
	mailbox, err := prepareRunMailbox(mailboxClaim(mailboxTestRunID, handoff).Job.Spec, mailboxTestAttempt, appender, time.Hour, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := writeMailboxEvent(mailbox.directory, "0001-broken", "not a protocol file\n"); err != nil {
		t.Fatal(err)
	}
	mailbox.sweep(context.Background())
	documents := appender.snapshot()
	if len(documents) != 1 {
		t.Fatalf("published %d documents, want one rejection report", len(documents))
	}
	envelope := decodeEnvelope(t, documents[0].body)
	if envelope.Status != contract.EnvelopeFailed || envelope.StepID != "mailbox" {
		t.Fatalf("rejection envelope = %+v", envelope)
	}
	if _, err := os.Stat(filepath.Join(mailbox.events, "0001-broken")); !os.IsNotExist(err) {
		t.Fatalf("malformed event was not retired: %v", err)
	}
}
