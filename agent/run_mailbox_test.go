//go:build darwin || linux

package agent

import (
	"bytes"
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
	"sync/atomic"
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
		// Mailbox eligibility follows L1's record of who submitted the job, so
		// the fixture carries the submitter a real L3 dispatch always has.
		Job:   l1.Job{Spec: mailboxSpec(runID, handoff, params), OriginatingSubmitter: contract.DefaultRunLedgerNodeID},
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

// observingAppender wraps the real HTTP appender so a test can see exactly
// what was sent and how the ledger answered.
type observingAppender struct {
	inner runLedgerAppender
	mu    sync.Mutex
	sent  [][]byte
	errs  []error
}

func (a *observingAppender) appendRunDocument(ctx context.Context, runToken, runID, collection string, body []byte) error {
	err := a.inner.appendRunDocument(ctx, runToken, runID, collection, body)
	a.mu.Lock()
	defer a.mu.Unlock()
	a.sent = append(a.sent, append([]byte(nil), body...))
	a.errs = append(a.errs, err)
	return err
}

func (a *observingAppender) observed() ([][]byte, []error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([][]byte(nil), a.sent...), append([]error(nil), a.errs...)
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
	observer := &observingAppender{inner: ledger.appender()}
	clock := newManualClock(mailboxTestClockOrigin)
	first, err := prepareRunMailbox(ledger.spec, mailboxTestAttempt, observer, time.Hour, clock, t.Logf)
	if err != nil {
		t.Fatal(err)
	}
	defer first.close()
	writeMailboxEvent(t, first.directory, "0007-envelope-implement",
		"wefty-protocol: 1\nkind: envelope\nname: implement\nsummary: done\n--\n")
	// The agent is lost between the accepted publication and its retirement.
	first.retireCheckpoint = func() bool { return false }
	if !first.sweep(context.Background()) {
		t.Fatal("first sweep failed against real L3")
	}

	// A restarted agent re-reads the same file. Its document must be
	// byte-identical — including the timestamp, which is pinned at first
	// observation rather than read from the file now — so L3 replays the
	// original instead of accepting a second envelope or refusing a conflict.
	clock.Advance(90 * time.Second)
	second, err := prepareRunMailbox(ledger.spec, mailboxTestAttempt, observer, time.Hour, clock, t.Logf)
	if err != nil {
		t.Fatal(err)
	}
	defer second.close()
	if !second.sweep(context.Background()) {
		t.Fatal("republication failed against real L3")
	}

	sent, errs := observer.observed()
	if len(sent) != 2 {
		t.Fatalf("sent %d documents, want the same event twice", len(sent))
	}
	if !bytes.Equal(sent[0], sent[1]) {
		t.Fatalf("republished document differs:\n%s\n%s", sent[0], sent[1])
	}
	// A conflict would come back as a rejection, which the mailbox retires
	// silently; only a genuine replay returns no error at all.
	if errs[0] != nil || errs[1] != nil {
		t.Fatalf("real L3 answers = (%v, %v), want the republication accepted as a replay", errs[0], errs[1])
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
	// The stored document is the one that was sent, with only the attempt
	// identity L3 bound itself added.
	var stored, resent map[string]any
	storedBody, err := json.Marshal(envelopes[0])
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(storedBody, &stored); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(sent[1], &resent); err != nil {
		t.Fatal(err)
	}
	for field, value := range resent {
		if fmt.Sprint(stored[field]) != fmt.Sprint(value) {
			t.Fatalf("stored %s = %v, resent %v", field, stored[field], value)
		}
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
		if names, _, err := mailbox.scanEvents(); err != nil || len(names) != 0 {
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
		if documents := appender.snapshot(); len(documents) != 0 {
			t.Fatalf("a capped listing published %d documents without knowing lexical order", len(documents))
		}
		mailbox.finalize(context.Background())
		if !mailbox.publicationIncomplete() || !mailbox.pending() {
			t.Fatal("a capped listing must retain its evidence")
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
	if names, _, err := mailbox.scanEvents(); err != nil || len(names) != 0 {
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

// --------------------------------------------------------------------------
// Post-preparation directory replacement, fencing, and durable bookkeeping
// --------------------------------------------------------------------------

func TestRunMailboxIgnoresAPostPreparationEventsDirectoryReplacement(t *testing.T) {
	t.Run("replaced by a FIFO", func(t *testing.T) {
		appender := newRecordingAppender("")
		mailbox, _, _ := newTestMailbox(t, appender, "")
		writeMailboxEvent(t, mailbox.directory, "0001-gate-test",
			"wefty-protocol: 1\nkind: gate\nname: test\noutcome: pass\n--\n")
		events := filepath.Join(mailbox.directory, runMailboxEventsDirectoryName)
		replaced := filepath.Join(mailbox.directory, "events.moved")
		if err := os.Rename(events, replaced); err != nil {
			t.Fatal(err)
		}
		if err := syscall.Mkfifo(events, 0o600); err != nil {
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
			t.Fatal("a FIFO in place of the events directory blocked finalization")
		}
		// The directory opened at preparation is the one the agent reads, so
		// the event it already held is published and the FIFO is irrelevant.
		if documents := appender.snapshot(); len(documents) != 1 {
			t.Fatalf("published %d documents through the retained directory", len(documents))
		}
	})

	t.Run("replaced by a symlink to the staging directory", func(t *testing.T) {
		appender := newRecordingAppender("")
		mailbox, _, _ := newTestMailbox(t, appender, "")
		staging := filepath.Join(mailbox.directory, runMailboxStagingDirectoryName)
		// A half-written event, which must never be published.
		if err := os.WriteFile(filepath.Join(staging, "0001-partial"),
			[]byte("wefty-protocol: 1\nkind: gate\nname: partial\noutcome: pass\n--\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		events := filepath.Join(mailbox.directory, runMailboxEventsDirectoryName)
		if err := os.Rename(events, filepath.Join(mailbox.directory, "events.moved")); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(staging, events); err != nil {
			t.Fatal(err)
		}
		mailbox.sweep(context.Background())
		if documents := appender.snapshot(); len(documents) != 0 {
			t.Fatalf("a redirected events directory published %d staged documents", len(documents))
		}
	})
}

func TestParentCancellationFencesBeforeTheAttemptContextEnds(t *testing.T) {
	parent, cancelParent := context.WithCancel(context.Background())
	var fencedFirst atomic.Bool
	var fencedCount atomic.Int32
	attempt, cancelAttempt, release := fencedAttemptContext(parent, func(error) {
		fencedCount.Add(1)
		fencedFirst.Store(true)
	})
	defer release()
	defer cancelAttempt(nil)

	if attempt.Err() != nil {
		t.Fatal("the attempt context started cancelled")
	}
	cancelParent()
	<-attempt.Done()
	// Reaching Done is only possible after the fence ran to completion: the
	// gate between the parent and the attempt is cancelled by the same
	// goroutine, after fence returns.
	if !fencedFirst.Load() {
		t.Fatal("the attempt context was cancelled without fencing publication")
	}
	if !errors.Is(context.Cause(attempt), context.Canceled) {
		t.Fatalf("attempt cause = %v", context.Cause(attempt))
	}
	if fencedCount.Load() != 1 {
		t.Fatalf("fenced %d times, want once", fencedCount.Load())
	}
}

func TestAFailedRejectionReportLeavesTheEventPending(t *testing.T) {
	appender := newRecordingAppender("")
	appender.failWith(errors.New("run ledger is unreachable"))
	mailbox, _, _ := newTestMailbox(t, appender, "")
	writeMailboxEvent(t, mailbox.directory, "0001-broken", "not a protocol file\n")
	mailbox.finalize(context.Background())
	if !mailbox.publicationIncomplete() {
		t.Fatal("an undeliverable rejection report did not mark publication incomplete")
	}
	names, _, err := mailbox.scanEvents()
	if err != nil || len(names) != 1 {
		t.Fatalf("events remaining = %v (%v), want the event kept for retry", names, err)
	}
}

func TestCorruptMailboxBookkeepingFailsClosed(t *testing.T) {
	appender := newRecordingAppender("")
	mailbox, handoff, _ := newTestMailbox(t, appender, "")
	writeMailboxEvent(t, mailbox.directory, "0001-gate-test",
		"wefty-protocol: 1\nkind: gate\nname: test\noutcome: pass\n--\n")
	state := filepath.Join(mailbox.directory, runMailboxPublishedDirectoryName, runMailboxStateFileName)
	if err := os.WriteFile(state, []byte("{not json"), 0o600); err != nil {
		t.Fatal(err)
	}

	restarted, err := prepareRunMailbox(mailboxSpec("run_mailbox", handoff, ""), mailboxTestAttempt, appender, time.Hour, nil, t.Logf)
	if err != nil {
		t.Fatal(err)
	}
	defer restarted.close()
	restarted.finalize(context.Background())
	if documents := appender.snapshot(); len(documents) != 0 {
		t.Fatalf("a mailbox with untrustworthy bookkeeping published %d documents", len(documents))
	}
	if !restarted.publicationIncomplete() {
		t.Fatal("corrupt bookkeeping did not mark publication incomplete")
	}
	names, _, err := restarted.scanEvents()
	if err != nil || len(names) != 1 {
		t.Fatalf("events remaining = %v (%v), want the evidence retained", names, err)
	}
}

func TestJunkEntriesCannotHideLaterMailboxEvents(t *testing.T) {
	appender := newRecordingAppender("")
	mailbox, _, _ := newTestMailbox(t, appender, "")
	events := filepath.Join(mailbox.directory, runMailboxEventsDirectoryName)
	// Enumerate the whole listing regardless of filesystem creation order.
	// The removable junk sorts ahead of the valid event.
	if err := os.Mkdir(filepath.Join(events, "0001-directory"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(t.TempDir(), filepath.Join(events, "0002-symlink")); err != nil {
		t.Fatal(err)
	}
	writeMailboxEvent(t, mailbox.directory, "0003-gate-test",
		"wefty-protocol: 1\nkind: gate\nname: test\noutcome: pass\n--\n")

	mailbox.finalize(context.Background())
	documents := appender.snapshot()
	if len(documents) != 1 {
		t.Fatalf("published %d documents, want the hidden event to be reached", len(documents))
	}
	var gate contract.GateResult
	if err := json.Unmarshal(documents[0].body, &gate); err != nil {
		t.Fatal(err)
	}
	if gate.Name != "test" {
		t.Fatalf("published gate = %+v", gate)
	}
	if mailbox.publicationIncomplete() {
		t.Fatal("a drained mailbox reported incomplete publication")
	}
}

func TestAnExhaustedScanListingCountsAsPending(t *testing.T) {
	appender := newRecordingAppender("")
	mailbox, _, _ := newTestMailbox(t, appender, "")
	mailbox.maxScan = 1
	for index := range 2 {
		writeMailboxEvent(t, mailbox.directory, fmt.Sprintf("%04d-gate", index),
			fmt.Sprintf("wefty-protocol: 1\nkind: gate\nname: g%d\noutcome: pass\n--\n", index))
	}
	// The capped listing is incomplete, so it must not claim to be drained.
	if mailbox.sweep(context.Background()) {
		t.Fatal("a capped listing reported a drained mailbox")
	}
	if !mailbox.pending() {
		t.Fatal("a mailbox with unseen entries reported nothing pending")
	}
}

// mailboxAppenderFunc lets concurrency tests observe the actual appender entry.
type mailboxAppenderFunc func(context.Context, string, string, string, []byte) error

func (f mailboxAppenderFunc) appendRunDocument(ctx context.Context, token, run, collection string, body []byte) error {
	return f(ctx, token, run, collection, body)
}

func TestMailboxFenceJoinsAppendAdmissionAndInflightCalls(t *testing.T) {
	for _, raw := range []string{
		"wefty-protocol: 1\nkind: gate\nname: test\noutcome: pass\n--\n",
		"malformed event\n",
	} {
		for _, beforeEntry := range []bool{true, false} {
			t.Run(fmt.Sprintf("rejection=%t/before-entry=%t", strings.HasPrefix(raw, "malformed"), beforeEntry), func(t *testing.T) {
				entered := make(chan struct{})
				cancelled := make(chan struct{})
				release := make(chan struct{})
				var releaseOnce sync.Once
				unblock := func() { releaseOnce.Do(func() { close(release) }) }
				defer unblock()
				pause := func(ctx context.Context) {
					close(entered)
					<-ctx.Done()
					close(cancelled)
					<-release
				}
				var calls atomic.Int32
				appender := mailboxAppenderFunc(func(ctx context.Context, _, _, _ string, _ []byte) error {
					calls.Add(1)
					if !beforeEntry {
						pause(ctx)
					}
					return ctx.Err()
				})
				mailbox, _, _ := newTestMailbox(t, appender, "")
				if beforeEntry {
					mailbox.appendCheckpoint = pause
				}
				writeMailboxEvent(t, mailbox.directory, "0001-event", raw)
				swept := make(chan struct{})
				go func() { defer close(swept); mailbox.finalize(context.Background()) }()
				select {
				case <-entered:
				case <-time.After(5 * time.Second):
					t.Fatal("append did not reach the controlled pause")
				}
				fenced := make(chan struct{}, 2)
				// Every fence caller must join, including a concurrent second one.
				for range 2 {
					go func() { mailbox.fence(context.Canceled); fenced <- struct{}{} }()
				}
				select {
				case <-cancelled:
				case <-time.After(5 * time.Second):
					t.Fatal("fence did not cancel the admitted append")
				}
				select {
				case <-fenced:
					t.Fatal("fence returned with an append still admitted/in flight")
				default:
				}
				unblock()
				for range 2 {
					select {
					case <-fenced:
					case <-time.After(5 * time.Second):
						t.Fatal("fence did not join the cancelled append")
					}
				}
				<-swept
				wantCalls := int32(1)
				if beforeEntry {
					wantCalls = 0
				}
				if calls.Load() != wantCalls {
					t.Fatalf("appender calls = %d, want %d", calls.Load(), wantCalls)
				}
				mailbox.finalize(context.Background())
				mailbox.sweep(context.Background())
				if calls.Load() != wantCalls || !mailbox.publicationIncomplete() {
					t.Fatal("publication crossed the returned fence or lost retained evidence")
				}
			})
		}
	}
}

func TestFencedAttemptPreservesComputerShutdownDeadline(t *testing.T) {
	deadline := time.Now().Add(time.Minute)
	parent, cancelParent := context.WithDeadline(context.Background(), deadline)
	defer cancelParent()
	entered, releaseFence := make(chan struct{}), make(chan struct{})
	var once sync.Once
	attempt, cancelAttempt, release := fencedAttemptContext(parent, func(error) {
		once.Do(func() { close(entered) })
		<-releaseFence
	})
	defer release()
	defer cancelAttempt(nil)
	if got, ok := attempt.Deadline(); !ok || !got.Equal(deadline) {
		t.Fatalf("attempt deadline = %v, %t; want %v", got, ok, deadline)
	}
	cancelParent()
	<-entered
	if attempt.Err() != nil {
		t.Fatal("attempt cancelled before the fence completed")
	}
	operation, cancelOperation := newComputerPublicationOperation(attempt, func(ctx context.Context) (context.Context, context.CancelFunc) {
		return context.WithTimeout(ctx, time.Hour)
	})
	defer cancelOperation()
	if got, ok := operation.Deadline(); !ok || !got.Equal(deadline) {
		t.Fatalf("Computer shutdown deadline = %v, %t; want %v", got, ok, deadline)
	}
	close(releaseFence)
	<-attempt.Done()
}

// mailboxManualDeadline expires only when the test closes done, so appender
// entry never races a wall-clock deadline. The real timer remains a watchdog.
type mailboxManualDeadline struct {
	context.Context
	done chan struct{}
}

func (ctx mailboxManualDeadline) Done() <-chan struct{} { return ctx.done }
func (ctx mailboxManualDeadline) Err() error {
	select {
	case <-ctx.done:
		return context.DeadlineExceeded
	default:
		return nil
	}
}

func TestMailboxFinalizationDeadlineBoundsPollerJoinAndFinalSweep(t *testing.T) {
	for _, polling := range []bool{false, true} {
		t.Run(fmt.Sprintf("polling=%t", polling), func(t *testing.T) {
			entered, exited := make(chan struct{}), make(chan struct{})
			var calls atomic.Int32
			appender := mailboxAppenderFunc(func(ctx context.Context, _, _, _ string, _ []byte) error {
				calls.Add(1)
				close(entered)
				defer close(exited)
				<-ctx.Done()
				return ctx.Err()
			})
			mailbox, _, _ := newTestMailbox(t, appender, "")
			writeMailboxEvent(t, mailbox.directory, "0001-event", "wefty-protocol: 1\nkind: envelope\n--\n")
			if polling {
				// Hold the poller join until finalization has transferred cleanup.
				mailbox.cancel = func() { close(entered) }
			}
			ctx := mailboxManualDeadline{Context: context.Background(), done: make(chan struct{})}
			done := make(chan struct{})
			go func() { defer close(done); mailbox.finalize(ctx) }()
			waitMailboxSignal(t, entered)
			close(ctx.done)
			waitMailboxSignal(t, done)
			if !mailbox.publicationIncomplete() || !mailbox.fenced.Load() || mailbox.publicationContext.Err() == nil {
				t.Fatal("expired finalization did not stop publication and retain evidence")
			}
			mailbox.fence(context.DeadlineExceeded)
			before := calls.Load()
			mailbox.sweep(context.Background())
			if calls.Load() != before || !mailbox.pending() {
				t.Fatal("deadline allowed a later append or discarded the event")
			}
			if polling {
				close(mailbox.finished)
				waitMailboxRootsClosed(t, mailbox)
			} else {
				waitMailboxSignal(t, exited)
			}
		})
	}
}

func waitMailboxSignal(t *testing.T, signal <-chan struct{}) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(5 * time.Second):
		t.Fatal("mailbox handshake did not complete")
	}
}

func waitMailboxRootsClosed(t *testing.T, mailbox *runMailbox) {
	t.Helper()
	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		if _, err := mailbox.root.Stat("."); errors.Is(err, os.ErrClosed) {
			return
		}
		select {
		case <-timer.C:
			t.Fatal("detached cleanup did not close roots")
		case <-ticker.C:
		}
	}
}

func TestMailboxFinalizationLeavesTreesAndFIFOsAndPublishesLaterEvents(t *testing.T) {
	appender := newRecordingAppender("")
	mailbox, _, _ := newTestMailbox(t, appender, "")
	tree := filepath.Join(mailbox.directory, runMailboxEventsDirectoryName, "0001-tree")
	for i := range 64 {
		child := filepath.Join(tree, fmt.Sprintf("branch-%03d", i), "nested")
		if err := os.MkdirAll(child, 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(child, "evidence"), []byte("keep"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	fifo := filepath.Join(mailbox.directory, runMailboxEventsDirectoryName, "0002-fifo")
	if err := syscall.Mkfifo(fifo, 0o600); err != nil {
		t.Fatal(err)
	}
	writeMailboxEvent(t, mailbox.directory, "0003-valid", "wefty-protocol: 1\nkind: envelope\n--\n")
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	mailbox.finalize(ctx)
	if !mailbox.publicationIncomplete() || len(appender.snapshot()) != 1 {
		t.Fatal("unremovable junk either hid the valid event or lost handoff retention")
	}
	for i := range 64 {
		payload, err := os.ReadFile(filepath.Join(tree, fmt.Sprintf("branch-%03d", i), "nested", "evidence"))
		if err != nil || string(payload) != "keep" {
			t.Fatalf("completion traversed or removed the workload tree: %q, %v", payload, err)
		}
	}
	if info, err := os.Lstat(fifo); err != nil || info.Mode()&os.ModeNamedPipe == 0 {
		t.Fatalf("FIFO was removed: %v, %v", info, err)
	}
}

func TestMailboxLexicalOrderAcrossFormerPageBoundary(t *testing.T) {
	appender := newRecordingAppender("")
	mailbox, _, _ := newTestMailbox(t, appender, "")
	// Reverse creation order crosses the old 256-entry page boundary.
	for i := 299; i >= 0; i-- {
		name := fmt.Sprintf("gate-%04d", i)
		writeMailboxEvent(t, mailbox.directory, name, "wefty-protocol: 1\nkind: gate\nname: "+name+"\noutcome: pass\n--\n")
	}
	if !mailbox.sweep(context.Background()) {
		t.Fatal("complete listing was not drained")
	}
	documents := appender.snapshot()
	if len(documents) != 300 {
		t.Fatalf("published %d events, want 300", len(documents))
	}
	for i, document := range documents {
		var gate contract.GateResult
		if err := json.Unmarshal(document.body, &gate); err != nil {
			t.Fatal(err)
		}
		if gate.Name != fmt.Sprintf("gate-%04d", i) {
			t.Fatalf("publication %d = %s, not lexical order", i, gate.Name)
		}
	}
}

func TestMailboxValidJSONStateIsClampedAndFailsClosed(t *testing.T) {
	for _, test := range []struct {
		name  string
		forge func(*runMailboxState)
	}{
		{"negative count", func(s *runMailboxState) { s.Count = -1 }},
		{"excess count", func(s *runMailboxState) { s.Count = MaxRunMailboxEvents + 1 }},
		{"negative bytes", func(s *runMailboxState) { s.Bytes = -1 }},
		{"excess bytes", func(s *runMailboxState) { s.Bytes = MaxRunMailboxTotalBytes + 1 }},
		{"negative rejections", func(s *runMailboxState) { s.Rejections = -1 }},
		{"excess rejections", func(s *runMailboxState) { s.Rejections = MaxRunMailboxRejections + 1 }},
		{"zero timestamp", func(s *runMailboxState) { s.Events["0001-event"] = runMailboxEventState{} }},
		{"future timestamp", func(s *runMailboxState) {
			s.Events["0001-event"] = runMailboxEventState{ObservedAt: mailboxTestClockOrigin.Add(runMailboxObservationClockTolerance + time.Second)}
		}},
		{"invalid name", func(s *runMailboxState) {
			s.Events["../outside"] = runMailboxEventState{ObservedAt: mailboxTestClockOrigin}
		}},
		{"unaccounted reservation", func(s *runMailboxState) {
			s.Events["0001-event"] = runMailboxEventState{ObservedAt: mailboxTestClockOrigin, ChargedBytes: 1}
		}},
		{"negative reservation", func(s *runMailboxState) {
			s.Events["0001-event"] = runMailboxEventState{ObservedAt: mailboxTestClockOrigin, ChargedBytes: -1}
		}},
		{"unaccounted rejection", func(s *runMailboxState) {
			s.Events["0001-event"] = runMailboxEventState{ObservedAt: mailboxTestClockOrigin, RejectionReserved: true}
		}},
		{"unaccounted refusal", func(s *runMailboxState) {
			s.Events["0001-event"] = runMailboxEventState{ObservedAt: mailboxTestClockOrigin, Refused: &runMailboxRefusal{Status: 400}}
		}},
		{"invalid refusal status", func(s *runMailboxState) {
			s.Rejections = 1
			s.Events["0001-event"] = runMailboxEventState{ObservedAt: mailboxTestClockOrigin, RejectionReserved: true, Refused: &runMailboxRefusal{Status: 500}}
		}},
		{"oversized refusal reason", func(s *runMailboxState) {
			s.Rejections = 1
			s.Events["0001-event"] = runMailboxEventState{ObservedAt: mailboxTestClockOrigin, RejectionReserved: true, Refused: &runMailboxRefusal{Status: 400, Reason: strings.Repeat("x", runMailboxRefusalReasonBytes+1)}}
		}},
		{"excess entries", func(s *runMailboxState) {
			for i := range MaxRunMailboxScanEntries + 1 {
				s.Events[fmt.Sprintf("event-%04d", i)] = runMailboxEventState{ObservedAt: mailboxTestClockOrigin}
			}
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			appender := newRecordingAppender("")
			mailbox, handoff, clock := newTestMailbox(t, appender, "")
			writeMailboxEvent(t, mailbox.directory, "0001-event", "wefty-protocol: 1\nkind: envelope\n--\n")
			state := runMailboxState{AttemptID: mailboxTestAttempt, Events: map[string]runMailboxEventState{}}
			test.forge(&state)
			payload, err := json.Marshal(state)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(mailbox.directory, runMailboxPublishedDirectoryName, runMailboxStateFileName), payload, 0o600); err != nil {
				t.Fatal(err)
			}
			restarted, err := prepareRunMailbox(mailboxSpec("run_mailbox", handoff, ""), mailboxTestAttempt, appender, time.Hour, clock, t.Logf)
			if err != nil {
				t.Fatal(err)
			}
			defer restarted.close()
			restarted.finalize(context.Background())
			if !restarted.corrupt || !restarted.publicationIncomplete() || !restarted.pending() || len(appender.snapshot()) != 0 {
				t.Fatal("forged state did not stop publication and retain the evidence")
			}
			got := restarted.state
			if got.Count < 0 || got.Count > MaxRunMailboxEvents || got.Bytes < 0 || got.Bytes > MaxRunMailboxTotalBytes || got.Rejections < 0 || got.Rejections > MaxRunMailboxRejections || len(got.Events) > MaxRunMailboxScanEntries {
				t.Fatal("loaded counters or map were not clamped")
			}
			for name, event := range got.Events {
				if !validRunMailboxEventName(name) || event.ObservedAt.Before(time.Unix(0, 0)) || event.ObservedAt.After(clock.Now()) || event.ChargedBytes < 0 || event.ChargedBytes > MaxRunMailboxTotalBytes {
					t.Fatalf("loaded event was not clamped: %s %+v", name, event)
				}
			}
		})
	}
}

func TestMailboxRejectionBudgetAndPendingReservationSurviveRestart(t *testing.T) {
	appender := newRecordingAppender("")
	mailbox, handoff, clock := newTestMailbox(t, appender, "")
	for i := range MaxRunMailboxRejections - 1 {
		writeMailboxEvent(t, mailbox.directory, fmt.Sprintf("%04d-bad", i), "not protocol\n")
	}
	mailbox.sweep(context.Background())
	appender.failWith(errors.New("response lost"))
	writeMailboxEvent(t, mailbox.directory, "0007-bad", "not protocol\n")
	mailbox.sweep(context.Background())
	if mailbox.state.Rejections != MaxRunMailboxRejections {
		t.Fatal("last rejection was not reserved before publication")
	}
	restarted, err := prepareRunMailbox(mailboxSpec("run_mailbox", handoff, ""), mailboxTestAttempt, appender, time.Hour, clock, t.Logf)
	if err != nil {
		t.Fatal(err)
	}
	defer restarted.close()
	appender.failWith(nil)
	writeMailboxEvent(t, mailbox.directory, "0008-bad", "not protocol\n")
	restarted.finalize(context.Background())
	if restarted.publicationIncomplete() || len(appender.snapshot()) != MaxRunMailboxRejections || restarted.state.Rejections != MaxRunMailboxRejections {
		t.Fatalf("restart rejection budget = %d, published %d, incomplete=%t", restarted.state.Rejections, len(appender.snapshot()), restarted.publicationIncomplete())
	}
}

func TestFencedAttemptMailboxInstallationOrders(t *testing.T) {
	for _, beforeStore := range []bool{false, true} {
		t.Run(fmt.Sprintf("cancel-before-store=%t", beforeStore), func(t *testing.T) {
			appender := newRecordingAppender("")
			mailbox, _, _ := newTestMailbox(t, appender, "")
			writeMailboxEvent(t, mailbox.directory, "0001-event", "wefty-protocol: 1\nkind: envelope\n--\n")
			lifecycle := &attemptLifecycle{}
			parent, cancelParent := context.WithCancel(context.Background())
			defer cancelParent()
			attempt, cancelAttempt, release := fencedAttemptContext(parent, lifecycle.fenceMailbox)
			defer release()
			defer cancelAttempt(nil)
			if !beforeStore {
				lifecycle.storeMailbox(mailbox)
			}
			cancelParent()
			waitMailboxSignal(t, attempt.Done())
			if beforeStore {
				lifecycle.storeMailbox(mailbox)
			}
			mailbox.finalize(context.WithoutCancel(attempt))
			if !mailbox.fenced.Load() || len(appender.snapshot()) != 0 || !mailbox.publicationIncomplete() || !mailbox.pending() {
				t.Fatal("late mailbox installation lost cancellation or published after authority loss")
			}
		})
	}
}

func TestFencedAttemptNormalCancellationAndWatcherRelease(t *testing.T) {
	entered, unblock := make(chan struct{}), make(chan struct{})
	attempt, cancel, release := fencedAttemptContext(context.Background(), func(error) {
		close(entered)
		<-unblock
	})
	release()
	if attempt.Err() != nil {
		t.Fatal("watcher release cancelled the attempt without a fence")
	}
	done := make(chan struct{})
	go func() { cancel(nil); close(done) }()
	waitMailboxSignal(t, entered)
	if attempt.Err() != nil {
		t.Fatal("normal cancellation preceded the fence")
	}
	close(unblock)
	waitMailboxSignal(t, done)
	if !errors.Is(attempt.Err(), context.Canceled) {
		t.Fatal("normal cancellation did not cancel the attempt")
	}
}

func TestMailboxLedgerRefusalRetainsEvidence(t *testing.T) {
	for _, malformed := range []bool{false, true} {
		for _, status := range []int{400, 401, 409} {
			t.Run(fmt.Sprintf("malformed=%t/status=%d", malformed, status), func(t *testing.T) {
				accepted := newRecordingAppender("")
				calls := 0
				appender := mailboxAppenderFunc(func(ctx context.Context, token, run, collection string, body []byte) error {
					calls++
					if calls == 1 {
						return &runLedgerRejection{statusCode: status, body: "refusal reason"}
					}
					return accepted.appendRunDocument(ctx, token, run, collection, body)
				})
				mailbox, handoff, clock := newTestMailbox(t, appender, "")
				var logs strings.Builder
				mailbox.logf = func(format string, args ...any) { fmt.Fprintf(&logs, format, args...) }
				raw := "wefty-protocol: 1\nkind: envelope\n--\n"
				if malformed {
					raw = "not protocol\n"
				}
				writeMailboxEvent(t, mailbox.directory, "0001-event", raw)
				writeMailboxEvent(t, mailbox.directory, "0002-later", "wefty-protocol: 1\nkind: gate\nname: later\noutcome: pass\n--\n")
				// Stop before finalization to model a crash after the refusal.
				mailbox.sweep(context.Background())
				if calls != 2 || len(accepted.snapshot()) != 1 {
					t.Fatal("refusal blocked a later event in the same sweep")
				}
				if !mailbox.publicationIncomplete() || !mailbox.pending() || !strings.Contains(logs.String(), "refusal reason") {
					t.Fatal("refused document lost its source, retention, or refusal log")
				}
				for range 3 {
					mailbox.sweep(context.Background())
				}
				if calls != 2 {
					t.Fatal("repeated sweeps resent a terminal refusal")
				}
				mailbox.close()
				restarted, err := prepareRunMailbox(mailboxSpec("run_mailbox", handoff, ""), mailboxTestAttempt, appender, time.Hour, clock, t.Logf)
				if err != nil {
					t.Fatal(err)
				}
				defer restarted.close()
				if restarted.corrupt || !restarted.publicationIncomplete() {
					t.Fatal("reopening before finalization lost the durable refusal retention marker")
				}
				if refused := restarted.state.Events["0001-event"].Refused; refused == nil || refused.Status != status || refused.Reason != "refusal reason" {
					t.Fatalf("reopening lost refusal status or reason: %+v", refused)
				}
				restarted.finalize(context.Background())
				if calls != 2 || !restarted.pending() {
					t.Fatal("reopening resent the refusal or lost its source")
				}
				if source, err := os.ReadFile(filepath.Join(restarted.directory, runMailboxEventsDirectoryName, "0001-event")); err != nil || string(source) != raw {
					t.Fatalf("refused source was not retained intact: %v", err)
				}
			})
		}
	}
}

func TestFencedAttemptWaitingBeforeMailboxTransferReturnsWithoutAppend(t *testing.T) {
	entered, unblock := make(chan struct{}), make(chan struct{})
	var once sync.Once
	release := func() { once.Do(func() { close(unblock) }) }
	defer release()
	appender := mailboxAppenderFunc(func(ctx context.Context, _, _, _ string, _ []byte) error {
		close(entered)
		<-unblock // deliberately ignores cancellation
		return ctx.Err()
	})
	mailbox, _, _ := newTestMailbox(t, appender, "")
	writeMailboxEvent(t, mailbox.directory, "0001-event", "wefty-protocol: 1\nkind: envelope\n--\n")
	ctx := mailboxManualDeadline{Context: context.Background(), done: make(chan struct{})}
	finalized := make(chan struct{})
	go func() { defer close(finalized); mailbox.finalize(ctx) }()
	waitMailboxSignal(t, entered)
	attempt, cancelAttempt, releaseWatcher := fencedAttemptContext(context.Background(), mailbox.fence)
	defer releaseWatcher()
	fenced := make(chan struct{})
	go func() { defer close(fenced); cancelAttempt(context.Canceled) }()
	// Only the pre-transfer fence can cancel publication at this point.
	waitMailboxSignal(t, mailbox.publicationContext.Done())
	if mailbox.detached.Load() || attempt.Err() != nil {
		t.Fatal("fence did not wait for the admitted append before transfer")
	}
	close(ctx.done)
	waitMailboxSignal(t, finalized)
	waitMailboxSignal(t, fenced)
	if !mailbox.detached.Load() || !errors.Is(attempt.Err(), context.Canceled) {
		t.Fatal("ownership transfer did not release the fence and attempt cancellation")
	}
	select {
	case <-unblock:
		t.Fatal("appender was released before the fence returned")
	default:
	}
	if _, err := mailbox.root.Stat("."); err != nil {
		t.Fatalf("fence closed the detached worker's roots: %v", err)
	}
	release()
	waitMailboxRootsClosed(t, mailbox)
}

func TestMailboxExhaustedRejectionBudgetRetiresWithLog(t *testing.T) {
	appender := newRecordingAppender("")
	mailbox, _, _ := newTestMailbox(t, appender, "")
	mailbox.state.Rejections = MaxRunMailboxRejections
	var logs strings.Builder
	mailbox.logf = func(format string, args ...any) { fmt.Fprintf(&logs, format, args...) }
	writeMailboxEvent(t, mailbox.directory, "0009-bad", "not protocol\n")
	mailbox.finalize(context.Background())
	if mailbox.pending() || len(appender.snapshot()) != 0 || !strings.Contains(logs.String(), "rejection-report budget exhausted; retiring") {
		t.Fatal("budget-exhausted rejection was not retired with a log")
	}
}

func TestMailboxFinalizationExpiryTransfersRootsUntilWorkerUnwinds(t *testing.T) {
	for _, inAppend := range []bool{false, true} {
		t.Run(fmt.Sprintf("in-append=%t", inAppend), func(t *testing.T) {
			entered, unblock := make(chan struct{}), make(chan struct{})
			var once sync.Once
			release := func() { once.Do(func() { close(unblock) }) }
			defer release()
			appender := mailboxAppenderFunc(func(ctx context.Context, _, _, _ string, _ []byte) error {
				if inAppend {
					close(entered)
					<-unblock // deliberately ignores cancellation
					return ctx.Err()
				}
				return nil
			})
			mailbox, _, _ := newTestMailbox(t, appender, "")
			if !inAppend {
				mailbox.retireCheckpoint = func() bool {
					close(entered)
					<-unblock
					return true // state write must still have live roots
				}
			}
			writeMailboxEvent(t, mailbox.directory, "0001-event", "wefty-protocol: 1\nkind: envelope\n--\n")
			ctx := mailboxManualDeadline{Context: context.Background(), done: make(chan struct{})}
			done := make(chan struct{})
			go func() { defer close(done); mailbox.finalize(ctx) }()
			waitMailboxSignal(t, entered)
			close(ctx.done)
			waitMailboxSignal(t, done)
			if !mailbox.detached.Load() || !mailbox.publicationIncomplete() {
				t.Fatal("blocked worker was not detached with incomplete evidence")
			}
			teardown := make(chan struct{})
			go func() { mailbox.fence(nil); mailbox.close(); close(teardown) }()
			waitMailboxSignal(t, teardown)
			for _, root := range []*os.Root{mailbox.root, mailbox.events, mailbox.published} {
				if _, err := root.Stat("."); err != nil {
					t.Fatalf("teardown closed roots under an admitted worker: %v", err)
				}
			}
			release()
			waitMailboxRootsClosed(t, mailbox)
			mailbox.mu.Lock()
			corrupt := mailbox.corrupt
			mailbox.mu.Unlock()
			if corrupt {
				t.Fatal("state write resumed against closed roots")
			}
		})
	}
}

func TestMailboxExpiredFinalizationWithoutPendingEvidence(t *testing.T) {
	mailbox, _, _ := newTestMailbox(t, newRecordingAppender(""), "")
	ctx := mailboxManualDeadline{Context: context.Background(), done: make(chan struct{})}
	close(ctx.done) // a slow reap consumed the caller's entire budget
	mailbox.finalize(ctx)
	if mailbox.publicationIncomplete() || mailbox.detached.Load() {
		t.Fatal("empty mailbox retained solely because the caller's deadline expired")
	}
}

func TestMailboxObservationSurvivesSmallBackwardsClockStep(t *testing.T) {
	appender := newRecordingAppender("")
	mailbox, handoff, _ := newTestMailbox(t, appender, "")
	mailbox.retireCheckpoint = func() bool { return false }
	writeMailboxEvent(t, mailbox.directory, "0001-event", "wefty-protocol: 1\nkind: envelope\n--\n")
	mailbox.sweep(context.Background())
	clock := newManualClock(mailboxTestClockOrigin.Add(-runMailboxObservationClockTolerance))
	restarted, err := prepareRunMailbox(mailboxSpec("run_mailbox", handoff, ""), mailboxTestAttempt, appender, time.Hour, clock, t.Logf)
	if err != nil {
		t.Fatal(err)
	}
	defer restarted.close()
	restarted.finalize(context.Background())
	documents := appender.snapshot()
	if restarted.corrupt || restarted.publicationIncomplete() || len(documents) != 2 || !bytes.Equal(documents[0].body, documents[1].body) {
		t.Fatal("small backwards clock correction broke byte-identical replay")
	}
}
