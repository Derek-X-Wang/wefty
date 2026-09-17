//go:build darwin || linux

package agent

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/fabric/plain"
	"github.com/Derek-X-Wang/wefty/l1"
	"github.com/Derek-X-Wang/wefty/l3"
	processrunner "github.com/Derek-X-Wang/wefty/runner/process"
)

const (
	mailboxTestToken   = "wrun_mailbox_secret"
	mailboxTestAttempt = "attempt-handoff"
)

var mailboxTestClockOrigin = time.Date(2026, 9, 17, 10, 0, 0, 0, time.UTC)

// --------------------------------------------------------------------------
// Harness
// --------------------------------------------------------------------------

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
	fail      error
	published chan struct{}
}

func newRecordingAppender(handoff string) *recordingAppender {
	return &recordingAppender{handoff: handoff, published: make(chan struct{}, 64)}
}

func (a *recordingAppender) appendRunDocument(_ context.Context, runToken, runID, collection string, body []byte) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.fail != nil {
		return a.fail
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

func (a *recordingAppender) failWith(err error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.fail = err
}

// mailboxSpec builds the job spec L3 would dispatch for a process run.
func mailboxSpec(runID, handoff, params string) contract.JobSpec {
	labels := map[string]string{"run_id": runID}
	if params != "" {
		labels[l3.RunParamsLabel] = params
	}
	return contract.JobSpec{
		Kind:   contract.JobKindProcess,
		Class:  contract.JobClassOneShot,
		Labels: labels,
		Execution: contract.ExecutionSpec{
			HandoffDirectory: handoff,
			Env: map[string]string{
				contract.EnvRunID:      runID,
				contract.EnvL3Endpoint: "http://127.0.0.1:9/",
				contract.EnvHandoffDir: handoff,
			},
			SensitiveEnv: map[string]string{contract.EnvRunToken: mailboxTestToken},
		},
	}
}

func mailboxClaim(runID, handoff, params string) l1.Claim {
	return l1.Claim{
		Job:   l1.Job{Spec: mailboxSpec(runID, handoff, params)},
		Lease: l1.AttemptLease{AttemptID: mailboxTestAttempt},
	}
}

// newTestMailbox prepares a mailbox over a fresh handoff directory with a
// manual clock, so observation timestamps are deterministic.
func newTestMailbox(t *testing.T, appender runLedgerAppender, params string) (*runMailbox, string, *manualClock) {
	t.Helper()
	runID := "run_mailbox"
	handoff := filepath.Join(t.TempDir(), runID)
	if err := os.MkdirAll(handoff, 0o700); err != nil {
		t.Fatal(err)
	}
	clock := newManualClock(mailboxTestClockOrigin)
	mailbox, err := prepareRunMailbox(mailboxSpec(runID, handoff, params), mailboxTestAttempt, appender, time.Hour, clock, t.Logf)
	if err != nil {
		t.Fatalf("prepare run mailbox: %v", err)
	}
	t.Cleanup(mailbox.close)
	return mailbox, handoff, clock
}

// mailboxRunner is a workload that writes mailbox events and optionally waits
// for the agent to publish them before it exits.
type mailboxRunner struct {
	write     func(runDirectory string) error
	waitFor   <-chan struct{}
	sawRunDir string
	sawParams string
	runnerErr error
	exitCode  int
}

func (r *mailboxRunner) Run(ctx context.Context, request processrunner.Request, _ processrunner.OutputSink) (contract.ProcessResult, error) {
	if request.Started != nil {
		request.Started()
	}
	r.sawRunDir = request.Execution.Env[contract.EnvRunDir]
	if r.sawRunDir != "" {
		if payload, err := os.ReadFile(filepath.Join(r.sawRunDir, runMailboxParamsFileName)); err == nil {
			r.sawParams = string(payload)
		}
	}
	if r.write != nil {
		if err := r.write(r.sawRunDir); err != nil {
			r.runnerErr = err
		}
	}
	if r.waitFor != nil {
		select {
		case <-r.waitFor:
		case <-ctx.Done():
			r.runnerErr = ctx.Err()
		case <-time.After(30 * time.Second):
			r.runnerErr = errors.New("workload timed out waiting for the agent to publish")
		}
	}
	exitCode := r.exitCode
	return contract.ProcessResult{ExitCode: &exitCode}, nil
}

func writeMailboxEvent(t *testing.T, runDirectory, name, content string) {
	t.Helper()
	staging := filepath.Join(runDirectory, runMailboxStagingDirectoryName, name)
	if err := os.WriteFile(staging, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(staging, filepath.Join(runDirectory, runMailboxEventsDirectoryName, name)); err != nil {
		t.Fatal(err)
	}
}

func decodeEnvelope(t *testing.T, body []byte) contract.Envelope {
	t.Helper()
	var envelope contract.Envelope
	if err := json.Unmarshal(body, &envelope); err != nil {
		t.Fatalf("decode envelope %s: %v", body, err)
	}
	return envelope
}

// realLedger is an in-process L3 — real store, real HTTP server, real token
// minting through a real dispatch — reachable over a plain Fabric network.
type realLedger struct {
	store       *l3.Store
	agentFabric fabric.Fabric
	address     string
	runID       string
	spec        contract.JobSpec
}

type capturingJobClient struct {
	mu   sync.Mutex
	spec contract.JobSpec
}

func (c *capturingJobClient) SubmitJob(_ context.Context, spec contract.JobSpec) (l1.Job, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.spec = spec
	return l1.Job{JobID: "job_" + spec.Labels["run_id"], Spec: spec, State: contract.JobQueued}, nil
}

func (c *capturingJobClient) GetJob(_ context.Context, jobID string) (l1.Job, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return l1.Job{JobID: jobID, Spec: c.spec, State: contract.JobRunning}, nil
}

// startRealLedger dispatches one run through the real reconciler, so the run
// token and the params label the agent later uses are exactly the ones
// production produces.
func startRealLedger(t *testing.T, handoff, params string) *realLedger {
	t.Helper()
	network := plain.NewNetwork()
	ledgerFabric := network.NewFabric(fabric.Identity{NodeID: "run-ledger"})
	agentFabric := network.NewFabric(fabric.Identity{NodeID: "agent-node"})
	store, err := l3.OpenStore(filepath.Join(t.TempDir(), "l3.sqlite"), l3.StoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	jobs := &capturingJobClient{}
	reconciler, err := l3.NewReconciler(store, jobs, l3.ReconcilerConfig{Interval: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	server, err := l3.NewServer(ledgerFabric, store, l3.ServerConfig{Jobs: jobs})
	if err != nil {
		t.Fatal(err)
	}
	listener, err := ledgerFabric.Listen("tcp", l3.DefaultL3Address)
	if err != nil {
		t.Fatal(err)
	}
	serveContext, cancelServe := context.WithCancel(context.Background())
	serveDone := make(chan error, 1)
	go func() { serveDone <- server.Serve(serveContext, listener) }()
	t.Cleanup(func() {
		cancelServe()
		if err := <-serveDone; err != nil {
			t.Errorf("serve real L3: %v", err)
		}
	})

	content := "exit 0\n"
	digest := sha256.Sum256([]byte(content))
	record, _, err := store.CreateRun(context.Background(), l3.CreateRunInput{
		IdempotencyKey: "mailbox-run",
		Actor:          "tester",
		Request: l3.CreateRunRequest{
			InlineScript: &l3.InlineScriptInput{Content: content, SHA256: hex.EncodeToString(digest[:]), Interpreter: []string{"/bin/sh"}},
			Params:       json.RawMessage(params),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := reconciler.ReconcileOnce(context.Background()); err != nil {
		t.Fatalf("dispatch the run: %v", err)
	}
	jobs.mu.Lock()
	spec := jobs.spec
	jobs.mu.Unlock()
	if spec.Execution.SensitiveEnv[contract.EnvRunToken] == "" {
		t.Fatal("dispatch produced no run token")
	}
	spec.Execution.HandoffDirectory = handoff
	spec.Execution.Env[contract.EnvHandoffDir] = handoff
	return &realLedger{store: store, agentFabric: agentFabric, address: l3.DefaultL3Address, runID: record.RunID, spec: spec}
}

func (l *realLedger) appender() runLedgerAppender {
	return newFabricRunLedgerAppender(l.agentFabric, l.address)
}

func (l *realLedger) mailbox(t *testing.T, attemptID string, clock Clock) *runMailbox {
	t.Helper()
	mailbox, err := prepareRunMailbox(l.spec, attemptID, l.appender(), time.Hour, clock, t.Logf)
	if err != nil {
		t.Fatalf("prepare run mailbox: %v", err)
	}
	t.Cleanup(mailbox.close)
	return mailbox
}

// --------------------------------------------------------------------------
// Real-L3 acceptance
// --------------------------------------------------------------------------

func TestMailboxEvidenceReachesRealL3WithoutAGivenAttemptIdentity(t *testing.T) {
	handoff := filepath.Join(t.TempDir(), "handoff")
	if err := os.MkdirAll(handoff, 0o700); err != nil {
		t.Fatal(err)
	}
	ledger := startRealLedger(t, handoff, `{"ref":"main"}`)
	mailbox := ledger.mailbox(t, mailboxTestAttempt, newManualClock(mailboxTestClockOrigin))

	// B3: the submitted parameters reach the job as a file, carried by the
	// dispatch label rather than by anything the workload could confuse with
	// the reserved execution context.
	params, err := os.ReadFile(filepath.Join(mailbox.directory, runMailboxParamsFileName))
	if err != nil {
		t.Fatal(err)
	}
	if strings.TrimSpace(string(params)) != `{"ref":"main"}` {
		t.Fatalf("params.json = %q, want the submitted params", params)
	}
	for _, name := range []string{contract.EnvRunToken, "WEFTY_RUN_PARAMS_JSON"} {
		if _, present := ledger.spec.Execution.Env[name]; present {
			t.Fatalf("%s reached the public job environment", name)
		}
	}

	writeMailboxEvent(t, mailbox.directory, "0001-step-clone",
		"wefty-protocol: 1\nkind: step\nname: clone\n--\n")
	writeMailboxEvent(t, mailbox.directory, "0002-gate-vet",
		"wefty-protocol: 1\nkind: gate\nname: vet\noutcome: pass\n--\nok\n")
	if !mailbox.sweep(context.Background()) {
		t.Fatal("sweep reported a transport failure against real L3")
	}

	envelopes, err := ledger.store.ListEnvelopes(context.Background(), ledger.runID)
	if err != nil {
		t.Fatal(err)
	}
	if len(envelopes) != 1 {
		t.Fatalf("real L3 accepted %d envelopes, want 1", len(envelopes))
	}
	if envelopes[0].StepID != "clone" || envelopes[0].Status != contract.EnvelopePartial {
		t.Fatalf("accepted envelope = %+v", envelopes[0])
	}
	// L3 bound the attempt identity itself; the agent never claimed one.
	if !strings.HasPrefix(envelopes[0].AttemptID, "runattempt") {
		t.Fatalf("envelope attempt_id = %q, want the ledger's own attempt identity", envelopes[0].AttemptID)
	}
	gates, err := ledger.store.ListGateResults(context.Background(), ledger.runID)
	if err != nil {
		t.Fatal(err)
	}
	if len(gates) != 1 || gates[0].Outcome != contract.GatePass || gates[0].Name != "vet" {
		t.Fatalf("real L3 accepted gates %+v", gates)
	}
	if len(gates[0].Evidence) != 1 || gates[0].Evidence[0].Value != "ok" {
		t.Fatalf("gate evidence = %+v, want the raw payload", gates[0].Evidence)
	}
	if gates[0].AttemptID != envelopes[0].AttemptID {
		t.Fatalf("gate attempt_id = %q, envelope attempt_id = %q", gates[0].AttemptID, envelopes[0].AttemptID)
	}
}

func TestMailboxReplayAfterAgentRestartDoesNotDuplicateEnvelopes(t *testing.T) {
	handoff := filepath.Join(t.TempDir(), "handoff")
	if err := os.MkdirAll(handoff, 0o700); err != nil {
		t.Fatal(err)
	}
	ledger := startRealLedger(t, handoff, `{}`)
	clock := newManualClock(mailboxTestClockOrigin)
	first := ledger.mailbox(t, mailboxTestAttempt, clock)
	writeMailboxEvent(t, first.directory, "0007-envelope-implement",
		"wefty-protocol: 1\nkind: envelope\nname: implement\nsummary: done\n--\n")
	// The agent is lost between the accepted publication and the cursor rename.
	first.retireCheckpoint = func() bool { return false }
	if !first.sweep(context.Background()) {
		t.Fatal("first sweep failed against real L3")
	}

	// A restarted agent re-reads the same file. Its document must be
	// byte-identical — including the timestamp, which is pinned at first
	// observation rather than read from the file now — so L3 replays the
	// original instead of accepting a second envelope.
	clock.Advance(90 * time.Second)
	second := ledger.mailbox(t, mailboxTestAttempt, clock)
	if !second.sweep(context.Background()) {
		t.Fatal("republication failed against real L3")
	}
	envelopes, err := ledger.store.ListEnvelopes(context.Background(), ledger.runID)
	if err != nil {
		t.Fatal(err)
	}
	if len(envelopes) != 1 {
		t.Fatalf("real L3 holds %d envelopes after republication, want 1", len(envelopes))
	}
	if !envelopes[0].CreatedAt.Equal(mailboxTestClockOrigin) {
		t.Fatalf("envelope created_at = %s, want the first observation", envelopes[0].CreatedAt)
	}
	// The retired event is gone, so a third sweep is a no-op.
	if !second.sweep(context.Background()) {
		t.Fatal("third sweep failed")
	}
	if envelopes, _ := ledger.store.ListEnvelopes(context.Background(), ledger.runID); len(envelopes) != 1 {
		t.Fatalf("retired event republished: %d envelopes", len(envelopes))
	}
}

// --------------------------------------------------------------------------
// Lifecycle
// --------------------------------------------------------------------------

func TestMailboxEventsArePublishedWhileTheAttemptRuns(t *testing.T) {
	root := filepath.Join(t.TempDir(), "handoffs")
	runID := "run_mailbox"
	handoff := filepath.Join(root, runID)
	appender := newRecordingAppender(handoff)
	runner := &mailboxRunner{
		write: func(runDirectory string) error {
			writeMailboxEvent(t, runDirectory, "0001-step-clone",
				"wefty-protocol: 1\nkind: step\nname: clone\n--\n")
			return nil
		},
		// The workload does not exit until the agent has published, so a pass
		// can only mean publication happened mid-attempt.
		waitFor: appender.published,
	}
	a := &Agent{
		registration: contract.NodeRegistration{NodeID: "node-1"},
		runtimes:     testRuntimeSet(runner),
		handoffs:     newHandoffManager(root, time.Hour),
		runLedger:    appender,
		mailboxPoll:  5 * time.Millisecond,
	}
	result, err := a.runWorkload(context.Background(), mailboxClaim(runID, handoff, `{"ref":"main"}`))
	if err != nil || result.ExitCode == nil || *result.ExitCode != 0 {
		t.Fatalf("runWorkload() = (%#v, %v)", result, err)
	}
	if runner.runnerErr != nil {
		t.Fatalf("workload: %v", runner.runnerErr)
	}
	if want := filepath.Join(handoff, runMailboxDirectoryName, runID); runner.sawRunDir != want {
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
	if envelope.StepID != "clone" || envelope.Status != contract.EnvelopePartial {
		t.Fatalf("step envelope = %+v, want a partial clone envelope", envelope)
	}
	if envelope.AttemptID != "" {
		t.Fatalf("published attempt_id = %q, want the ledger to bind it", envelope.AttemptID)
	}
}

func TestMailboxFinalSweepCompletesBeforeHandoffRemoval(t *testing.T) {
	root := filepath.Join(t.TempDir(), "handoffs")
	runID := "run_mailbox"
	handoff := filepath.Join(root, runID)
	appender := newRecordingAppender(handoff)
	runner := &mailboxRunner{
		write: func(runDirectory string) error {
			writeMailboxEvent(t, runDirectory, "0009-result-verdict",
				"wefty-protocol: 1\nkind: result\nname: result.json\n--\n{\"passed\":true}\n")
			return nil
		},
	}
	a := &Agent{
		registration: contract.NodeRegistration{NodeID: "node-1"},
		runtimes:     testRuntimeSet(runner),
		handoffs:     newHandoffManager(root, time.Hour),
		runLedger:    appender,
		// Long enough that no poll can fire: only the final sweep can publish.
		mailboxPoll: time.Hour,
	}
	claim := mailboxClaim(runID, handoff, "")
	lifecycle := a.newAttemptLifecycle()
	result, runErr := lifecycle.runWorkload(context.Background(), claim)
	if runErr != nil || result.ExitCode == nil || *result.ExitCode != 0 {
		t.Fatalf("runWorkload() = (%#v, %v)", result, runErr)
	}
	documents := appender.snapshot()
	if len(documents) != 1 {
		t.Fatalf("published %d documents, want the final sweep to publish the result", len(documents))
	}
	if !documents[0].handoffPresent {
		t.Fatal("the result was published after the handoff directory was removed")
	}
	if _, err := lifecycle.finishCompletedAttempt(context.Background(), claim, result, runErr); err != nil {
		t.Fatalf("finishCompletedAttempt: %v", err)
	}
	if _, err := os.Stat(handoff); !os.IsNotExist(err) {
		t.Fatalf("successful handoff directory still exists: %v", err)
	}
}

func TestUnpublishedMailboxEvidenceRetainsTheHandoffOnSuccess(t *testing.T) {
	root := filepath.Join(t.TempDir(), "handoffs")
	runID := "run_mailbox"
	handoff := filepath.Join(root, runID)
	appender := newRecordingAppender(handoff)
	appender.failWith(errors.New("run ledger is unreachable"))
	runner := &mailboxRunner{
		write: func(runDirectory string) error {
			writeMailboxEvent(t, runDirectory, "0001-gate-test",
				"wefty-protocol: 1\nkind: gate\nname: test\noutcome: fail\n--\nboom\n")
			return nil
		},
	}
	a := &Agent{
		registration: contract.NodeRegistration{NodeID: "node-1"},
		runtimes:     testRuntimeSet(runner),
		handoffs:     newHandoffManager(root, time.Hour),
		runLedger:    appender,
		mailboxPoll:  time.Hour,
	}
	claim := mailboxClaim(runID, handoff, "")
	lifecycle := a.newAttemptLifecycle()
	result, runErr := lifecycle.runWorkload(context.Background(), claim)
	if runErr != nil || result.ExitCode == nil || *result.ExitCode != 0 {
		t.Fatalf("runWorkload() = (%#v, %v)", result, runErr)
	}
	if !lifecycle.mailbox.Load().publicationIncomplete() {
		t.Fatal("an unreachable ledger did not mark publication incomplete")
	}
	if _, err := lifecycle.finishCompletedAttempt(context.Background(), claim, result, runErr); err != nil {
		t.Fatalf("finishCompletedAttempt: %v", err)
	}
	if _, err := os.Stat(handoff); err != nil {
		t.Fatalf("unpublished evidence was deleted with the handoff directory: %v", err)
	}
	events := filepath.Join(handoff, runMailboxDirectoryName, runID, runMailboxEventsDirectoryName, "0001-gate-test")
	if _, err := os.Stat(events); err != nil {
		t.Fatalf("the unpublished event did not survive: %v", err)
	}
}

func TestAuthorityLossFencesMailboxPublication(t *testing.T) {
	appender := newRecordingAppender("")
	mailbox, _, _ := newTestMailbox(t, appender, "")
	writeMailboxEvent(t, mailbox.directory, "0001-gate-test",
		"wefty-protocol: 1\nkind: gate\nname: test\noutcome: pass\n--\n")
	mailbox.fence(errors.New("authority watchdog"))
	mailbox.finalize(context.Background())
	if documents := appender.snapshot(); len(documents) != 0 {
		t.Fatalf("a fenced mailbox published %d documents", len(documents))
	}
	if !mailbox.publicationIncomplete() {
		t.Fatal("a fenced mailbox with pending evidence did not report incompleteness")
	}
	// Sweeping again cannot resurrect publication: the fence is permanent.
	if mailbox.sweep(context.Background()) {
		t.Fatal("a fenced mailbox reported a successful sweep")
	}
	if documents := appender.snapshot(); len(documents) != 0 {
		t.Fatalf("a fenced mailbox published %d documents after a later sweep", len(documents))
	}
}

func TestColdRerunDoesNotInheritAnotherRunsMailbox(t *testing.T) {
	handoff := filepath.Join(t.TempDir(), "handoff")
	if err := os.MkdirAll(handoff, 0o700); err != nil {
		t.Fatal(err)
	}
	appender := newRecordingAppender("")
	source, err := prepareRunMailbox(mailboxSpec("run_source", handoff, ""), mailboxTestAttempt, appender, time.Hour, nil, t.Logf)
	if err != nil {
		t.Fatal(err)
	}
	defer source.close()
	writeMailboxEvent(t, source.directory, "0001-gate-test",
		"wefty-protocol: 1\nkind: gate\nname: test\noutcome: fail\n--\n")

	// A cold rerun reuses the same handoff directory under a new run ID.
	rerun, err := prepareRunMailbox(mailboxSpec("run_rerun", handoff, ""), "attempt-rerun", appender, time.Hour, nil, t.Logf)
	if err != nil {
		t.Fatal(err)
	}
	defer rerun.close()
	if !rerun.sweep(context.Background()) {
		t.Fatal("rerun sweep failed")
	}
	if documents := appender.snapshot(); len(documents) != 0 {
		t.Fatalf("the rerun published %d documents from the source run's mailbox", len(documents))
	}
	if rerun.directory == source.directory {
		t.Fatal("the rerun shares the source run's mailbox directory")
	}
}

// --------------------------------------------------------------------------
// Filesystem boundary
// --------------------------------------------------------------------------

func TestRunMailboxRefusesWorkloadPlantedFilesystemObjects(t *testing.T) {
	t.Run("a symlinked event is refused and its target untouched", func(t *testing.T) {
		appender := newRecordingAppender("")
		mailbox, _, _ := newTestMailbox(t, appender, "")
		secret := filepath.Join(t.TempDir(), "secret")
		if err := os.WriteFile(secret, []byte("wefty-protocol: 1\nkind: gate\nname: stolen\noutcome: pass\n--\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(secret, filepath.Join(mailbox.directory, runMailboxEventsDirectoryName, "0001-link")); err != nil {
			t.Fatal(err)
		}
		mailbox.sweep(context.Background())
		if documents := appender.snapshot(); len(documents) != 0 {
			t.Fatalf("a symlinked event published %d documents", len(documents))
		}
		if _, err := os.Lstat(secret); err != nil {
			t.Fatalf("the symlink target was disturbed: %v", err)
		}
	})

	t.Run("a FIFO event cannot block finalization", func(t *testing.T) {
		appender := newRecordingAppender("")
		mailbox, _, _ := newTestMailbox(t, appender, "")
		fifo := filepath.Join(mailbox.directory, runMailboxEventsDirectoryName, "0001-fifo")
		if err := syscall.Mkfifo(fifo, 0o600); err != nil {
			t.Skipf("this filesystem cannot create a FIFO: %v", err)
		}
		done := make(chan struct{})
		go func() {
			defer close(done)
			mailbox.finalize(context.Background())
		}()
		select {
		case <-done:
		case <-time.After(20 * time.Second):
			t.Fatal("a FIFO in the events directory blocked finalization")
		}
		if documents := appender.snapshot(); len(documents) != 0 {
			t.Fatalf("a FIFO event published %d documents", len(documents))
		}
	})

	t.Run("a symlinked events directory is refused at preparation", func(t *testing.T) {
		handoff := filepath.Join(t.TempDir(), "handoff")
		elsewhere := t.TempDir()
		if err := os.MkdirAll(filepath.Join(handoff, runMailboxDirectoryName, "run_mailbox"), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(elsewhere, filepath.Join(handoff, runMailboxDirectoryName, "run_mailbox", runMailboxEventsDirectoryName)); err != nil {
			t.Fatal(err)
		}
		_, err := prepareRunMailbox(mailboxSpec("run_mailbox", handoff, ""), mailboxTestAttempt, newRecordingAppender(""), time.Hour, nil, t.Logf)
		if err == nil || !strings.Contains(err.Error(), "not a directory") {
			t.Fatalf("prepare error = %v, want a refusal of the symlinked events directory", err)
		}
	})

	t.Run("params are not written through a planted symlink", func(t *testing.T) {
		handoff := filepath.Join(t.TempDir(), "handoff")
		mailboxDirectory := filepath.Join(handoff, runMailboxDirectoryName, "run_mailbox")
		if err := os.MkdirAll(mailboxDirectory, 0o700); err != nil {
			t.Fatal(err)
		}
		target := filepath.Join(t.TempDir(), "outside.json")
		if err := os.WriteFile(target, []byte("untouched"), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(target, filepath.Join(mailboxDirectory, runMailboxParamsFileName)); err != nil {
			t.Fatal(err)
		}
		mailbox, err := prepareRunMailbox(mailboxSpec("run_mailbox", handoff, `{"ref":"main"}`), mailboxTestAttempt, newRecordingAppender(""), time.Hour, nil, t.Logf)
		if err != nil {
			t.Fatal(err)
		}
		defer mailbox.close()
		outside, err := os.ReadFile(target)
		if err != nil || string(outside) != "untouched" {
			t.Fatalf("params were written through the symlink: %q, %v", outside, err)
		}
		written, err := os.ReadFile(filepath.Join(mailboxDirectory, runMailboxParamsFileName))
		if err != nil || strings.TrimSpace(string(written)) != `{"ref":"main"}` {
			t.Fatalf("params.json = %q, %v", written, err)
		}
	})
}

// --------------------------------------------------------------------------
// Bounds
// --------------------------------------------------------------------------

func TestOversizeMailboxEventIsTruncatedNotDropped(t *testing.T) {
	appender := newRecordingAppender("")
	mailbox, _, _ := newTestMailbox(t, appender, "")
	header := "wefty-protocol: 1\nkind: gate\nname: test\noutcome: fail\npayload: json\n--\n"
	writeMailboxEvent(t, mailbox.directory, "0002-gate-test", header+`{"failures":"`+strings.Repeat("F", MaxRunMailboxEventBytes*2)+`"}`)
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
	// A truncated JSON payload is no longer decodable, so it is preserved as
	// marked text rather than discarded.
	if gate.Evidence[0].Kind != "text" || !strings.Contains(gate.Evidence[0].Value, "truncated by the node agent") {
		t.Fatalf("truncated evidence = %q (kind %q)", gate.Evidence[0].Value[:64], gate.Evidence[0].Kind)
	}
	if len(gate.Evidence[0].Value) > MaxRunMailboxEventBytes+len(runMailboxTruncationNotice) {
		t.Fatalf("truncated gate evidence is %d bytes, want at most the size bound", len(gate.Evidence[0].Value))
	}
}

func TestRunMailboxEnforcesItsPublicationBounds(t *testing.T) {
	gate := func(name string) string {
		return "wefty-protocol: 1\nkind: gate\nname: " + name + "\noutcome: pass\n--\n"
	}

	t.Run("the event count bound stops publication", func(t *testing.T) {
		appender := newRecordingAppender("")
		mailbox, _, _ := newTestMailbox(t, appender, "")
		mailbox.maxEvents = 2
		for index := range 5 {
			writeMailboxEvent(t, mailbox.directory, fmt.Sprintf("%04d-gate", index), gate(fmt.Sprintf("g%d", index)))
		}
		mailbox.sweep(context.Background())
		if documents := appender.snapshot(); len(documents) != 2 {
			t.Fatalf("published %d documents, want the count bound to hold at 2", len(documents))
		}
		// Bounded events are retired, so they cannot be retried forever.
		if names, err := mailbox.scanEvents(); err != nil || len(names) != 0 {
			t.Fatalf("events remaining after the bound: %v, %v", names, err)
		}
	})

	t.Run("the byte bound counts the document about to be published", func(t *testing.T) {
		appender := newRecordingAppender("")
		mailbox, _, _ := newTestMailbox(t, appender, "")
		mailbox.maxBytes = 600
		writeMailboxEvent(t, mailbox.directory, "0001-gate", gate("small"))
		writeMailboxEvent(t, mailbox.directory, "0002-gate",
			"wefty-protocol: 1\nkind: gate\nname: large\noutcome: pass\n--\n"+strings.Repeat("x", 1024))
		mailbox.sweep(context.Background())
		documents := appender.snapshot()
		if len(documents) != 1 {
			t.Fatalf("published %d documents, want the prospective document to be refused", len(documents))
		}
		if mailbox.state.Bytes > mailbox.maxBytes {
			t.Fatalf("accounted %d bytes, want at most %d", mailbox.state.Bytes, mailbox.maxBytes)
		}
	})

	t.Run("accounting survives a reconstructed mailbox", func(t *testing.T) {
		appender := newRecordingAppender("")
		mailbox, handoff, _ := newTestMailbox(t, appender, "")
		mailbox.maxEvents = 1
		writeMailboxEvent(t, mailbox.directory, "0001-gate", gate("first"))
		mailbox.sweep(context.Background())

		restarted, err := prepareRunMailbox(mailboxSpec("run_mailbox", handoff, ""), mailboxTestAttempt, appender, time.Hour, nil, t.Logf)
		if err != nil {
			t.Fatal(err)
		}
		defer restarted.close()
		restarted.maxEvents = 1
		writeMailboxEvent(t, restarted.directory, "0002-gate", gate("second"))
		restarted.sweep(context.Background())
		if documents := appender.snapshot(); len(documents) != 1 {
			t.Fatalf("published %d documents, want the reconstructed mailbox to remember the bound", len(documents))
		}
	})

	t.Run("one sweep enumerates a bounded number of events", func(t *testing.T) {
		appender := newRecordingAppender("")
		mailbox, _, _ := newTestMailbox(t, appender, "")
		mailbox.maxScan = 1
		for index := range 3 {
			writeMailboxEvent(t, mailbox.directory, fmt.Sprintf("%04d-gate", index), gate(fmt.Sprintf("g%d", index)))
		}
		mailbox.sweep(context.Background())
		if documents := appender.snapshot(); len(documents) != 1 {
			t.Fatalf("one sweep published %d documents, want the scan bound to hold at 1", len(documents))
		}
		mailbox.sweep(context.Background())
		if documents := appender.snapshot(); len(documents) != 2 {
			t.Fatalf("two sweeps published %d documents", len(documents))
		}
	})
}

func TestMalformedMailboxEventIsReportedAndRetired(t *testing.T) {
	appender := newRecordingAppender("")
	mailbox, _, _ := newTestMailbox(t, appender, "")
	writeMailboxEvent(t, mailbox.directory, "0001-broken", "not a protocol file\n")
	mailbox.sweep(context.Background())
	documents := appender.snapshot()
	if len(documents) != 1 {
		t.Fatalf("published %d documents, want one rejection report", len(documents))
	}
	envelope := decodeEnvelope(t, documents[0].body)
	if envelope.Status != contract.EnvelopeFailed || envelope.StepID != "mailbox" {
		t.Fatalf("rejection envelope = %+v", envelope)
	}
	if names, err := mailbox.scanEvents(); err != nil || len(names) != 0 {
		t.Fatalf("malformed event was not retired: %v, %v", names, err)
	}
}

// --------------------------------------------------------------------------
// Parser
// --------------------------------------------------------------------------

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
			name: "a step defaults to started",
			raw:  "wefty-protocol: 1\nkind: step\nname: clone\n--\n",
			assert: func(t *testing.T, event runMailboxEvent) {
				if event.status != "started" {
					t.Fatalf("step status = %q", event.status)
				}
			},
		},
		{
			name: "header-only event without a trailing newline",
			raw:  "wefty-protocol: 1\nkind: step\nname: clone\nstatus: ended\n--",
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
			name:    "an unknown step status is refused",
			raw:     "wefty-protocol: 1\nkind: step\nname: clone\nstatus: halfway\n--\n",
			wantErr: "step status",
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
