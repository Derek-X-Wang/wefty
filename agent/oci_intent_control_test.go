package agent

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/l1"
	workloadrunner "github.com/Derek-X-Wang/wefty/runner"
	"github.com/Derek-X-Wang/wefty/runner/lima"
	"github.com/Derek-X-Wang/wefty/runner/ocicontrol"
)

func TestStopOCIRuntimeSuppressesAndJoinsOnlyOCIResidents(t *testing.T) {
	capabilities := newCapabilityState(map[string]bool{
		"kind:process": true, "kind:oci": true,
	}, nil, systemClock{}, time.Second)
	session := newAgentSession(nil, contract.NodeRegistration{}, capabilities, time.Second, time.Second, systemClock{}, newLifecycleObserver(systemClock{}), nil, 1, 1)
	ociContext, cancelOCI := context.WithCancelCause(context.Background())
	processContext, cancelProcess := context.WithCancelCause(context.Background())
	defer cancelOCI(nil)
	defer cancelProcess(nil)
	ociDone := make(chan struct{})
	ociReaped := make(chan runtimeReapOutcome, 1)
	go func() {
		<-ociContext.Done()
		ociReaped <- runtimeReapOutcome{receipt: workloadrunner.ReapReceipt{RuntimeQuiesced: true, Evidence: workloadrunner.ReapEvidenceOCIRuntimeSweep}}
		close(ociDone)
	}()
	session.resident["oci-job"] = &residentAttempt{kind: contract.JobKindOCI, class: contract.JobClassService, cancel: cancelOCI, done: ociDone, runtimeReaped: ociReaped}
	session.resident["process-job"] = &residentAttempt{kind: contract.JobKindProcess, class: contract.JobClassService, cancel: cancelProcess, done: make(chan struct{})}
	session.residentKind["oci-job"] = contract.JobKindOCI
	session.residentJobID["oci-job"] = struct{}{}
	if err := session.stopOCIRuntime(t.Context()); err != nil {
		t.Fatal(err)
	}
	if context.Cause(ociContext) != errOCIIntentDisabled {
		t.Fatalf("OCI cancel cause=%v", context.Cause(ociContext))
	}
	if processContext.Err() != nil {
		t.Fatalf("process resident was canceled: %v", context.Cause(processContext))
	}
	snapshot := capabilities.capabilitySnapshot()
	if snapshot.Capabilities[contract.JobKindOCI] || snapshot.Capabilities["kind:oci"] ||
		snapshot.ReasonCode != contract.CapabilityReasonOCIIntentDisabled {
		t.Fatalf("restrictive snapshot=%+v", snapshot)
	}
}

func TestStopOCIRuntimeRejectsMissingReapProof(t *testing.T) {
	capabilities := newCapabilityState(map[string]bool{"kind:process": true, "kind:oci": true}, nil, systemClock{}, time.Second)
	session := newAgentSession(nil, contract.NodeRegistration{}, capabilities, time.Second, time.Second, systemClock{}, newLifecycleObserver(systemClock{}), nil, 1, 1)
	ctx, cancel := context.WithCancelCause(context.Background())
	done := make(chan struct{})
	go func() { <-ctx.Done(); close(done) }()
	session.resident["oci-job"] = &residentAttempt{kind: contract.JobKindOCI, cancel: cancel, done: done, runtimeReaped: make(chan runtimeReapOutcome, 1)}
	session.residentKind["oci-job"] = contract.JobKindOCI
	session.residentJobID["oci-job"] = struct{}{}
	if err := session.stopOCIRuntime(t.Context()); err == nil {
		t.Fatal("stop reported quiescence without a positive reap receipt")
	}
}

func TestStopOCIRuntimeJoinsClaimBeforeResidentPublication(t *testing.T) {
	capabilities := newCapabilityState(map[string]bool{"kind:process": true, "kind:oci": true}, nil, systemClock{}, time.Second)
	session := newAgentSession(nil, contract.NodeRegistration{}, capabilities, time.Second, time.Second, systemClock{}, newLifecycleObserver(systemClock{}), nil, 1, 1)
	session.residentKind["just-claimed"] = contract.JobKindOCI
	session.residentJobID["just-claimed"] = struct{}{}
	started := make(chan struct{})
	go func() {
		session.claimMu.Lock()
		ctx, cancel := context.WithCancelCause(context.Background())
		done := make(chan struct{})
		reaped := make(chan runtimeReapOutcome, 1)
		resident := &residentAttempt{kind: contract.JobKindOCI, cancel: cancel, done: done, runtimeReaped: reaped}
		session.resident["just-claimed"] = resident
		session.notifyResidentChangedLocked()
		session.claimMu.Unlock()
		close(started)
		<-ctx.Done()
		reaped <- runtimeReapOutcome{receipt: workloadrunner.ReapReceipt{RuntimeQuiesced: true, Evidence: workloadrunner.ReapEvidenceOCIRuntimeSweep}}
		close(done)
	}()
	if err := session.stopOCIRuntime(t.Context()); err != nil {
		t.Fatal(err)
	}
	<-started
}

func TestControllerStopReportsResidentSuppressionFailureCreatedDuringTeardown(t *testing.T) {
	intentPath := filepath.Join(t.TempDir(), "oci-intent.json")
	if _, err := lima.InitializeOCIIntent(intentPath, time.Now()); err != nil {
		t.Fatal(err)
	}
	intentSource := lima.FileIntentSource{Path: intentPath}
	gate := &ociIntentCompletionGate{
		observe: func(ctx context.Context) (OCIIntentObservation, error) {
			intent, err := intentSource.ReadIntent(ctx)
			return OCIIntentObservation{Enabled: intent.Enabled, Revision: intent.Revision}, err
		},
		suppressionTimeout: time.Second,
	}
	outbox, err := newEvidenceOutbox(t.TempDir(), "teardown-suppression-node", 1<<20, systemClock{}, 8, time.Hour, time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	defer outbox.Close()
	claim := l1.Claim{Job: l1.Job{JobID: "teardown-suppression-job", Spec: contract.JobSpec{
		Kind: contract.JobKindOCI, Class: contract.JobClassService,
	}}, Lease: l1.AttemptLease{AttemptID: "teardown-suppression-attempt", FencingToken: "fence"}}
	if err := outbox.ensureAttempt(t.Context(), claim); err != nil {
		t.Fatal(err)
	}
	exitCode := 7
	result := l1.ProcessResult{ExitCode: &exitCode}
	if err := outbox.storeCompletion(t.Context(), claim.Lease.AttemptID, result, time.Now(), l1.RuntimeQuiescenceAttempt); err != nil {
		t.Fatal(err)
	}

	capabilities := newCapabilityState(map[string]bool{"kind:process": true, "kind:oci": true}, nil, systemClock{}, time.Second)
	session := newAgentSession(nil, contract.NodeRegistration{}, capabilities, time.Second, time.Second, systemClock{}, newLifecycleObserver(systemClock{}), nil, 1, 1)
	session.residentKind[claim.Job.JobID] = contract.JobKindOCI
	session.residentJobID[claim.Job.JobID] = struct{}{}
	if !session.gates[workloadClassService].tryAcquire() {
		t.Fatal("acquire service gate for resident")
	}
	session.attempts.Add(1)
	lifecycle := newAttemptLifecycle(attemptLifecycleDependencies{
		outbox: outbox, ociIntentGate: gate, clock: systemClock{}, completionRetry: time.Millisecond,
	})
	residentStarted := make(chan struct{})
	residentDone := make(chan error, 1)
	go func() {
		_, executeErr := session.executeResident(t.Context(), workloadClassService, claim, time.Now(), func(ctx context.Context, _ l1.Claim, _ time.Time) (errorDestination, error) {
			close(residentStarted)
			<-ctx.Done()
			session.recordRuntimeReap(claim.Job.JobID, workloadrunner.ReapReceipt{
				RuntimeQuiesced: true, Evidence: workloadrunner.ReapEvidenceAttempt,
			}, nil)
			failure := lifecycle.completeWithRetry(ctx, claim, l1.CompletionRequest{
				FencingToken: claim.Lease.FencingToken, IdempotencyKey: "completion:" + claim.Lease.AttemptID, Result: result,
			})
			return failure.destination, failure.err
		})
		residentDone <- executeErr
	}()
	<-residentStarted

	connectionBlocker, err := outbox.spool.db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := connectionBlocker.ExecContext(t.Context(), `UPDATE spool_attempts SET job_id=job_id WHERE attempt_id=?`, claim.Lease.AttemptID); err != nil {
		t.Fatal(err)
	}
	controller, err := ocicontrol.NewController(ocicontrol.ControllerConfig{
		IntentPath: intentPath,
		Runtime:    &Agent{session: session, ociIntentGate: gate},
	})
	if err != nil {
		t.Fatal(err)
	}
	stopContext, cancelStop := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancelStop()
	response, stopErr := controller.Stop(stopContext, ocicontrol.IntentMutationRequest{ExpectedRevision: 1})
	var persistenceErr *OCIIntentSuppressionPersistenceError
	if !errors.As(stopErr, &persistenceErr) || persistenceErr.AttemptID != claim.Lease.AttemptID ||
		persistenceErr.IntentRevision != 2 || !errors.Is(persistenceErr, context.DeadlineExceeded) {
		_ = connectionBlocker.Rollback()
		t.Fatalf("controller stop response=%+v error=%T %v, want typed resident suppression failure", response, stopErr, stopErr)
	}
	if response.RuntimeQuiesced {
		_ = connectionBlocker.Rollback()
		t.Fatalf("controller stop reported quiesced after resident suppression failure: %+v", response)
	}
	if err := <-residentDone; !errors.As(err, &persistenceErr) {
		_ = connectionBlocker.Rollback()
		t.Fatalf("resident completion error=%T %v, want typed suppression failure", err, err)
	}
	if err := connectionBlocker.Rollback(); err != nil {
		t.Fatal(err)
	}
	receipt := outbox.spool.inspectCompletion(t.Context(), claim.Lease.AttemptID)
	if receipt.State != "durable_completion" || receipt.Result.ExitCode == nil || *receipt.Result.ExitCode != exitCode {
		t.Fatalf("teardown suppression failure lost completion evidence: %+v", receipt)
	}
}

func TestOCIIntentCompletionFencePredicateIsExplicit(t *testing.T) {
	tests := []struct {
		kind, class string
		want        bool
	}{
		{kind: contract.JobKindOCI, class: contract.JobClassService, want: true},
		{kind: legacyUnclassifiedKind, class: contract.JobClassService, want: true},
		{kind: contract.JobKindProcess, class: contract.JobClassService, want: false},
		{kind: "future-kind", class: contract.JobClassService, want: false},
		{kind: contract.JobKindOCI, class: contract.JobClassOneShot, want: false},
	}
	for _, test := range tests {
		if got := requiresOCIIntentFence(test.kind, test.class); got != test.want {
			t.Fatalf("requiresOCIIntentFence(%q, %q)=%t want %t", test.kind, test.class, got, test.want)
		}
	}
}
