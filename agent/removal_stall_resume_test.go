package agent

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/l1"
	workloadrunner "github.com/Derek-X-Wang/wefty/runner"
	"github.com/Derek-X-Wang/wefty/runner/ocihelper"
)

// stalledRemovalFault is the staged Mac Computer lane fault: every cleanup
// step the helper performs is refused until the operator clears it, and the
// node under test is never restarted.
type stalledRemovalFault struct{ standing bool }

func (fault *stalledRemovalFault) refusal() error {
	return &ocihelper.RPCError{
		Code: ocihelper.CodeEngineFailure, Message: "OCI engine operation failed",
		EngineFailure: &ocihelper.EngineFailureFact{
			Operation: ocihelper.MethodDeleteBackup, Reason: ocihelper.EngineFailurePermissionDenied,
		},
	}
}

// stalledRemovalHarness drives one declared-stalled removal against the real
// durable spool, so the retry cadence, the refusal streak and the stall
// accounting are the shipped ones rather than test doubles.
type stalledRemovalHarness struct {
	spool        *logSpool
	name         string
	controller   *removalController
	removal      localRemoval
	directive    l1.RemovalDirective
	fault        *stalledRemovalFault
	now          time.Time
	logs         []string
	acknowledged int
	finished     int
	reaped       int
	copiesPruned int
	cleared      int
}

func newStalledRemovalHarness(t *testing.T, name string) *stalledRemovalHarness {
	t.Helper()
	spool := openTestLogSpool(t, t.TempDir(), name, 1024)
	t.Cleanup(func() { spool.Close() })
	removal := testRuntimeRemoval(name + "-job")
	manifest := testRuntimeResourceManifest(removal.jobID, "attempt")
	manifest.NodeID = name
	prepared := time.Date(2026, 9, 22, 6, 18, 26, 0, time.UTC)
	if err := spool.storeRuntimeResourceManifest(t.Context(), manifest, prepared); err != nil {
		t.Fatal(err)
	}
	if err := spool.beginRemoval(t.Context(), removal, prepared); err != nil {
		t.Fatal(err)
	}
	harness := &stalledRemovalHarness{
		spool: spool, removal: removal, name: name, fault: &stalledRemovalFault{standing: true},
		now: prepared,
	}
	harness.controller = harness.newController("boot-1")
	harness.directive = l1.RemovalDirective{
		JobID: removal.jobID, BoundNodeID: name, Kind: contract.JobKindOCI,
		RemovalGeneration: removal.generation, CleanupFence: removal.cleanupFence,
		RootInstanceID: removal.rootInstanceID,
		ComputerBackupCopies: &l1.ComputerBackupCopyClaims{Copies: []l1.ComputerBackupPruneDirective{{
			BackupID: name + "-backup", CopyID: name + "-copy", ComputerID: name + "-computer",
			StorageID: name + "-storage", StorageGeneration: 1, AllocatedSize: 1 << 20,
			BoundNodeID: name, RootInstanceID: removal.rootInstanceID,
			OperationRevision: 1, CleanupFence: removal.cleanupFence,
		}}},
	}
	return harness
}

// newController is one agent boot over the harness's durable spool. A restart
// is exactly this: the same durable state, a new boot session, nothing carried
// over in memory.
func (harness *stalledRemovalHarness) newController(bootSessionID string) *removalController {
	spool := harness.spool
	controller := &removalController{
		nodeID: harness.name, bootSessionID: bootSessionID, stallBound: l1.DefaultRemovalStallBound,
		managed: &recordingResumeResource{resume: func() {}}, outbox: &evidenceOutbox{},
		inflight: make(map[string]struct{}),
		now:      func() time.Time { return harness.now },
		logf: func(format string, args ...any) {
			harness.logs = append(harness.logs, fmt.Sprintf(format, args...))
		},
	}
	controller.beginRemoval = func(context.Context, localRemoval) error { return nil }
	controller.loadRuntimeRemoval = spool.runtimeRemoval
	controller.removalStartedAt = spool.removalStartedAt
	controller.recordRemovalFailure = func(ctx context.Context, target localRemoval, code, detail string) error {
		return spool.recordRuntimeRemovalFailure(ctx, target, code, detail, controller.bootSessionID, harness.now)
	}
	controller.recordUntypedFailure = func(ctx context.Context, target localRemoval) error {
		return spool.recordRuntimeRemovalUntypedFailure(ctx, target, controller.bootSessionID, harness.now)
	}
	controller.freezeStall = func(ctx context.Context, target localRemoval, declaration []byte, key string) ([]byte, string, error) {
		return spool.freezeRuntimeRemovalStallDeclaration(ctx, target, declaration, key)
	}
	controller.recordStallDeclared = func(ctx context.Context, target localRemoval) error {
		return spool.recordRuntimeRemovalStallDeclared(ctx, target, harness.now)
	}
	controller.ackRemovalStall = func(context.Context, localRemoval, runtimeRemovalRecord) error { return nil }
	// Deleting the Computer's Backup copies and reaping the runtime are the
	// two helper steps the staged fault refuses; both clear together when the
	// operator clears it.
	controller.removeBackupCopies = func(context.Context, []l1.ComputerBackupPruneDirective) error {
		if harness.fault.standing {
			return harness.fault.refusal()
		}
		harness.copiesPruned++
		return nil
	}
	controller.reapService = func(context.Context, string, string, []workloadrunner.RuntimeResourceManifest) (workloadrunner.ReapReceipt, error) {
		if harness.fault.standing {
			return workloadrunner.ReapReceipt{}, harness.fault.refusal()
		}
		harness.reaped++
		return workloadrunner.ReapReceipt{RuntimeQuiesced: true, Evidence: workloadrunner.ReapEvidenceAttempt,
			BootSessionID: bootSessionID}, nil
	}
	controller.recordRuntimeQuiesced = func(ctx context.Context, target localRemoval, receipt workloadrunner.ReapReceipt) error {
		return spool.recordRuntimeQuiesced(ctx, target, receipt, harness.now)
	}
	controller.recordRuntimeAttested = func(ctx context.Context, target localRemoval, attestation workloadrunner.RuntimeRemovalAttestation) error {
		return spool.recordRuntimeAttested(ctx, target, attestation, harness.now)
	}
	controller.deleteRuntimeData = func(context.Context, workloadrunner.RuntimeRemovalProofRequest) error { return nil }
	controller.attestRuntimeRemoval = func(_ context.Context, request workloadrunner.RuntimeRemovalProofRequest) (workloadrunner.RuntimeRemovalAttestation, error) {
		return testRuntimeRemovalAttestation(runtimeRemovalManifest{Version: 1, JobID: request.JobID,
			RemovalGeneration: request.RemovalGeneration, Attempts: request.Attempts}), nil
	}
	controller.purgeJob = spool.purgeJob
	controller.removeResource = func(context.Context, localRemoval) error { return nil }
	controller.releaseImagePin = func(context.Context, string) error { return nil }
	controller.ackRemoval = func(context.Context, localRemoval) error {
		harness.acknowledged++
		return nil
	}
	controller.finishRemoval = func(ctx context.Context, target localRemoval) error {
		harness.finished++
		return spool.completeRemoval(ctx, target)
	}
	controller.clearReap = func(string) { harness.cleared++ }
	controller.listRuntimeRemovals = spool.pendingRuntimeRemovals
	return controller
}

// declareStall runs the refusals the declaration needs and advances the clock
// past the bound, exactly as a heartbeat-driven node would.
func (harness *stalledRemovalHarness) declareStall(t *testing.T) {
	t.Helper()
	for attempt := 0; attempt < l1.MinimumServiceRemovalStallAttempts; attempt++ {
		if attempt == l1.MinimumServiceRemovalStallAttempts-1 {
			harness.now = harness.now.Add(l1.DefaultRemovalStallBound)
		}
		harness.tick(t)
	}
	record := harness.record(t)
	if record.stallDeclaredAt == nil {
		t.Fatalf("removal was not declared stalled after %d refusals past the bound", l1.MinimumServiceRemovalStallAttempts)
	}
}

// tick is one standing-directive redispatch: the heartbeat hands the same
// directive back and the controller decides whether its own cadence is due.
func (harness *stalledRemovalHarness) tick(t *testing.T) {
	t.Helper()
	failures := make(chan destinationError, 1)
	harness.controller.enqueue(t.Context(), harness.directive, failures)
	harness.controller.wait()
	select {
	case failure := <-failures:
		t.Fatalf("a stalled removal's retry reached the node session fence: %+v", failure)
	default:
	}
}

func (harness *stalledRemovalHarness) record(t *testing.T) runtimeRemovalRecord {
	t.Helper()
	record, found, err := harness.spool.runtimeRemoval(t.Context(), harness.removal.jobID)
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Fatalf("durable removal record for %q disappeared", harness.removal.jobID)
	}
	return record
}

// advanceToNextRetry moves the injected clock to this removal's own next
// bounded retry deadline and returns the delay it waited.
func (harness *stalledRemovalHarness) advanceToNextRetry(t *testing.T) time.Duration {
	t.Helper()
	record := harness.record(t)
	if record.lastAttemptedAt == nil {
		t.Fatal("a declared removal has no durable last-attempt time to back off from")
	}
	before := *record.lastAttemptedAt
	for delay := declaredRemovalRetryBase; ; {
		harness.now = before.Add(delay)
		if !harness.controller.declaredRemovalRetryDeferred(record) {
			harness.now = before.Add(delay - time.Second)
			if !harness.controller.declaredRemovalRetryDeferred(record) {
				t.Fatalf("retry ran a second before its own %s delay", delay)
			}
			harness.now = before.Add(delay)
			return delay
		}
		if delay == declaredRemovalRetryMax {
			t.Fatalf("no delay up to the %s cap released this retry", declaredRemovalRetryMax)
		}
		if delay *= 2; delay > declaredRemovalRetryMax {
			delay = declaredRemovalRetryMax
		}
	}
}

// TestStalledRemovalFinishesOnItsOwnCadenceWithoutARestart is the #514
// regression from attended Mac Computer lane run 2. The removal correctly
// reached `stalled_cleanup_unverified` while the staged refusal stood; the
// operator then cleared the fault and the node that had declared the stall --
// still running, still receiving the standing directive on every heartbeat --
// never finished the cleanup. Only restarting the agent did.
//
// The node that declared a stall retries it on its own bounded cadence, and
// the first retry after the refusal clears completes cleanup exactly as a
// returning node would.
func TestStalledRemovalFinishesOnItsOwnCadenceWithoutARestart(t *testing.T) {
	harness := newStalledRemovalHarness(t, "self-resume-node")
	harness.declareStall(t)
	bootSessionID := harness.controller.bootSessionID

	// One refused retry after the declaration: the cadence is running and the
	// outcome has not changed.
	harness.advanceToNextRetry(t)
	harness.tick(t)
	if harness.acknowledged != 0 || harness.copiesPruned != 0 || harness.reaped != 0 {
		t.Fatalf("a retry under the standing fault completed cleanup: ack %d copies %d reaps %d",
			harness.acknowledged, harness.copiesPruned, harness.reaped)
	}

	harness.fault.standing = false
	// A redispatch before the cadence is due changes nothing: the removal owns
	// its own schedule, not the heartbeat's.
	harness.tick(t)
	if harness.acknowledged != 0 {
		t.Fatal("a stalled removal retried before its own bounded delay had elapsed")
	}
	harness.advanceToNextRetry(t)
	harness.tick(t)

	if harness.copiesPruned != 1 || harness.reaped != 1 {
		t.Fatalf("cleanup steps after the fault cleared = %d Backup copies, %d reaps, want 1 and 1",
			harness.copiesPruned, harness.reaped)
	}
	if harness.acknowledged != 1 || harness.finished != 1 || harness.cleared != 1 {
		t.Fatalf("completion on the running node = ack %d finish %d clear %d, want 1 each",
			harness.acknowledged, harness.finished, harness.cleared)
	}
	if harness.controller.bootSessionID != bootSessionID {
		t.Fatal("the test completed the removal across a restart, not on the running node")
	}
	if _, found, err := harness.spool.runtimeRemoval(t.Context(), harness.removal.jobID); err != nil || found {
		t.Fatalf("durable removal record after completion found=%t err=%v, want released", found, err)
	}
	succeeded := false
	for _, line := range harness.logs {
		succeeded = succeeded || strings.Contains(line, "cleanup succeeded after its declared stall")
	}
	if !succeeded {
		t.Fatalf("agent logs = %v, want the completion after the declared stall named", harness.logs)
	}
}

// TestStalledRemovalRetriesStayBoundedWhileItsRefusalStands keeps the other
// half of the contract. A cadence that finishes the job when the refusal
// clears must not become a hot loop while it stands, and the removal's
// permanent outcome must not move: the declaration is made once and nothing
// claims any part of cleanup succeeded.
func TestStalledRemovalRetriesStayBoundedWhileItsRefusalStands(t *testing.T) {
	harness := newStalledRemovalHarness(t, "bounded-retry-node")
	declarations := 0
	harness.controller.ackRemovalStall = func(context.Context, localRemoval, runtimeRemovalRecord) error {
		declarations++
		return nil
	}
	harness.declareStall(t)
	if declarations != 1 {
		t.Fatalf("stall declarations = %d, want exactly one", declarations)
	}

	delays := []time.Duration{}
	for retry := 0; retry < 6; retry++ {
		delays = append(delays, harness.advanceToNextRetry(t))
		harness.tick(t)
	}
	want := []time.Duration{15 * time.Second, 30 * time.Second, time.Minute, 2 * time.Minute,
		declaredRemovalRetryMax, declaredRemovalRetryMax}
	for index, delay := range delays {
		if delay != want[index] {
			t.Fatalf("retry %d waited %s, want %s (bounded 15s->3m cadence)", index+1, delay, want[index])
		}
	}
	record := harness.record(t)
	if record.stallRetryAttempts != len(delays) {
		t.Fatalf("durable retry cadence counter = %d, want %d", record.stallRetryAttempts, len(delays))
	}
	if declarations != 1 || harness.acknowledged != 0 || harness.copiesPruned != 0 || harness.reaped != 0 {
		t.Fatalf("outcome moved under a standing refusal: declarations %d ack %d copies %d reaps %d",
			declarations, harness.acknowledged, harness.copiesPruned, harness.reaped)
	}
	if record.stallDeclaredAt == nil || record.completedAt != nil {
		t.Fatal("the removal's stalled outcome did not stay put while its refusal stood")
	}
}

// TestARestartMidCadenceDoesNotAcknowledgeTwice covers the overlap the
// contract deliberately allows: the declaring node and a returning node both
// retry, and whichever finishes first completes cleanup. The loser must find
// nothing left to acknowledge.
func TestARestartMidCadenceDoesNotAcknowledgeTwice(t *testing.T) {
	harness := newStalledRemovalHarness(t, "restart-mid-cadence-node")
	harness.declareStall(t)
	harness.advanceToNextRetry(t)
	harness.tick(t)

	harness.fault.standing = false
	harness.advanceToNextRetry(t)
	harness.tick(t)
	if harness.acknowledged != 1 {
		t.Fatalf("acknowledgements after the running node finished = %d, want 1", harness.acknowledged)
	}

	// The returning node: a new boot over the same durable spool, resuming
	// whatever the previous boot left behind.
	returning := harness.newController("boot-2")
	if err := returning.resume(t.Context()); err != nil {
		t.Fatalf("returning boot resume after a completed removal = %v", err)
	}
	returning.enqueue(t.Context(), harness.directive, make(chan destinationError, 1))
	returning.wait()
	if harness.acknowledged != 1 || harness.finished != 1 {
		t.Fatalf("returning boot re-acknowledged a completed removal: ack %d finish %d",
			harness.acknowledged, harness.finished)
	}
}

// singleUseReapRuntime is the helper's real answer shape. A reap that
// succeeds spends the adapter's tracking entry and its single-use sweep
// evidence, so asking again for the same attempt authority comes back
// `unauthorized_attempt` with no tracked fallback left. A refusal spends
// nothing.
type singleUseReapRuntime struct {
	asked    int
	refusing bool
	consumed map[workloadrunner.AttemptAuthority]struct{}
}

func newSingleUseReapRuntime(refusing bool) *singleUseReapRuntime {
	return &singleUseReapRuntime{refusing: refusing, consumed: make(map[workloadrunner.AttemptAuthority]struct{})}
}

func (runtime *singleUseReapRuntime) reap(_ context.Context, kind string, authority workloadrunner.AttemptAuthority) (workloadrunner.ReapReceipt, error) {
	runtime.asked++
	if kind != contract.JobKindOCI {
		return workloadrunner.ReapReceipt{}, fmt.Errorf("unexpected reap kind %q", kind)
	}
	if _, spent := runtime.consumed[authority]; spent {
		return workloadrunner.ReapReceipt{}, &ocihelper.RPCError{
			Code: ocihelper.CodeUnauthorizedAttempt, Message: "attempt authority does not match a live attempt",
		}
	}
	if runtime.refusing {
		return workloadrunner.ReapReceipt{}, &ocihelper.RPCError{Code: ocihelper.CodeEngineFailure, Message: "still refused"}
	}
	runtime.consumed[authority] = struct{}{}
	return workloadrunner.ReapReceipt{RuntimeQuiesced: true, Evidence: workloadrunner.ReapEvidenceAttempt,
		BootSessionID: authority.BootSessionID}, nil
}

// latchedFailureRemoval seeds what the attempt lifecycle recorded while the
// helper was lost, plus the frozen attempt authority the removal carries.
func latchedFailureRemoval(t *testing.T, session *agentSession, jobID string) []workloadrunner.RuntimeResourceManifest {
	t.Helper()
	manifest := testRuntimeResourceManifest(jobID, "attempt")
	manifest.NodeID = session.registration.NodeID
	manifest.BootSessionID = session.registration.BootSessionID
	session.claimMu.Lock()
	session.serviceReaps[jobID] = runtimeReapOutcome{
		err: errors.New("workload runtime lost during operation: OCI boot barrier has not completed"),
	}
	session.serviceBoots[jobID] = session.registration.BootSessionID
	session.claimMu.Unlock()
	return []workloadrunner.RuntimeResourceManifest{manifest}
}

// TestRemovalReapAsksTheRuntimeAgainAfterALatchedFailure is the root cause of
// #514 at the seam it lives on. The session latches the one reap outcome an
// attempt produced and replays it for the rest of the boot. That is right for
// a receipt -- quiescence, once proven, stays proven -- and wrong for a
// failure: run 2's removal replayed a runtime loss recorded while its helper
// was down, so no retry after the fault cleared ever touched the runtime, and
// only a restart (which empties the map) could finish cleanup.
func TestRemovalReapAsksTheRuntimeAgainAfterALatchedFailure(t *testing.T) {
	node := newStalledRemovalNode(t, "reask-reap-node", true)
	session := node.agent.session
	jobID := node.removal.jobID
	attempts := latchedFailureRemoval(t, session, jobID)
	runtime := newSingleUseReapRuntime(true)
	session.reapRemovalAttempt = runtime.reap

	_, err := session.reapServiceForRemoval(t.Context(), jobID, contract.JobKindOCI, attempts)
	if err == nil || !strings.Contains(err.Error(), "still refused") {
		t.Fatalf("reap while the fault stands = %v, want this attempt's own refusal", err)
	}
	if strings.Contains(fmt.Sprint(err), "boot barrier") {
		t.Fatalf("reap replayed the latched failure instead of asking again: %v", err)
	}
	runtime.refusing = false
	receipt, err := session.reapServiceForRemoval(t.Context(), jobID, contract.JobKindOCI, attempts)
	if err != nil || !receipt.RuntimeQuiesced || receipt.Evidence != workloadrunner.ReapEvidenceAttempt {
		t.Fatalf("reap after the fault cleared = %+v err %v, want a positive receipt on the running node", receipt, err)
	}
	if runtime.asked != 2 {
		t.Fatalf("runtime reap asked %d times, want one per removal attempt", runtime.asked)
	}

	// The receipt it earned is the job's answer now: proven quiescence is
	// replayed, never re-asked. The runtime would refuse a second consumption
	// of the same authority, exactly as the helper does.
	replayed, err := session.reapServiceForRemoval(t.Context(), jobID, contract.JobKindOCI, attempts)
	if err != nil || replayed != receipt {
		t.Fatalf("second reap = %+v err %v, want the earned receipt replayed", replayed, err)
	}
	if runtime.asked != 2 {
		t.Fatalf("runtime reap asked %d times, want a proven receipt replayed without asking", runtime.asked)
	}
}

// TestAnEarnedReapReceiptSurvivesAFailedQuiescenceWrite is the narrow window
// the re-ask opens and must close. Asking spends the adapter's evidence for
// that authority, so a receipt earned and then lost -- one transient spool
// error between the answer and its durable write -- would leave the next
// retry asking against evidence that no longer exists, wedged for the rest of
// the boot having already proven the thing it needed.
func TestAnEarnedReapReceiptSurvivesAFailedQuiescenceWrite(t *testing.T) {
	node := newStalledRemovalNode(t, "retained-receipt-node", true)
	session := node.agent.session
	jobID := node.removal.jobID
	attempts := latchedFailureRemoval(t, session, jobID)
	runtime := newSingleUseReapRuntime(false)
	session.reapRemovalAttempt = runtime.reap

	// The retry that finally earns a receipt, whose durable write then fails.
	earned, err := session.reapServiceForRemoval(t.Context(), jobID, contract.JobKindOCI, attempts)
	if err != nil {
		t.Fatalf("the retry that cleared the fault = %v, want a positive receipt", err)
	}
	persistErr := errors.New("record runtime quiescence: disk I/O error")

	// The next cadence tick, after the transient write failure.
	retried, err := session.reapServiceForRemoval(t.Context(), jobID, contract.JobKindOCI, attempts)
	if err != nil {
		t.Fatalf("the tick after %v = %v, want the proof this node already earned", persistErr, err)
	}
	if retried != earned {
		t.Fatalf("retried receipt = %+v, want the earned one %+v", retried, earned)
	}
	if runtime.asked != 1 {
		t.Fatalf("runtime reap asked %d times, want the earned proof reused rather than re-consumed", runtime.asked)
	}
}

// TestAnEarnedReapReceiptIsNotKeptForALaterAttempt keeps the retained receipt
// inside the attempt it speaks for. A new attempt of the same job has its own
// runtime to prove absent.
func TestAnEarnedReapReceiptIsNotKeptForALaterAttempt(t *testing.T) {
	node := newStalledRemovalNode(t, "readmitted-job-node", true)
	session := node.agent.session
	jobID := node.removal.jobID
	attempts := latchedFailureRemoval(t, session, jobID)
	runtime := newSingleUseReapRuntime(false)
	session.reapRemovalAttempt = runtime.reap

	earned, err := session.reapServiceForRemoval(t.Context(), jobID, contract.JobKindOCI, attempts)
	if err != nil {
		t.Fatal(err)
	}

	// The narrow race the guard closes: the job is claimed again between the
	// answer and its retention. executeResident drops the entry when that
	// attempt starts, and a retention arriving afterwards must not put it back.
	session.claimMu.Lock()
	session.residentJobID[jobID] = struct{}{}
	delete(session.serviceReaps, jobID)
	session.claimMu.Unlock()
	session.retainRemovalReap(jobID, earned)
	session.claimMu.Lock()
	_, kept := session.serviceReaps[jobID]
	session.claimMu.Unlock()
	if kept {
		t.Fatal("a receipt from an earlier attempt was retained for a job this node has admitted again")
	}
}

// TestStalledRemovalRetryAndPruneSuppressionDoNotFight keeps #513 and this
// retry on the same side. The ordinary prune list stays suppressed for the
// copies a stalled removal's standing directive names, and that removal's own
// cadence is the one thing that deletes them -- one accounting for one step.
func TestStalledRemovalRetryAndPruneSuppressionDoNotFight(t *testing.T) {
	node := newStalledRemovalNode(t, "retry-vs-prune-node", true)
	session := node.agent.session
	if err := session.processStandingDirectives(t.Context(), node.response); err != nil {
		t.Fatalf("standing directives while the removal stands stalled = %v", err)
	}
	if attempted := node.runtime.attempted(); len(attempted) != 0 {
		t.Fatalf("the prune list reconciled a stalled removal's copies = %v, want none", attempted)
	}

	// The removal's own retry is what carries those copies to the helper.
	record, found, err := session.removals.outbox.spool.runtimeRemoval(t.Context(), node.removal.jobID)
	if err != nil || !found {
		t.Fatalf("seeded removal record found=%t err=%v", found, err)
	}
	if record.stallDeclaredAt == nil {
		t.Fatal("the seeded removal was not declared stalled")
	}
	session.removals.now = func() time.Time {
		return record.lastAttemptedAt.Add(declaredRemovalRetryMax + time.Second)
	}
	err = session.removals.reconcile(t.Context(), node.response.RemovalDirectives[0])
	if err == nil || !strings.Contains(err.Error(), string(ocihelper.CodeEngineFailure)) {
		t.Fatalf("the stalled removal's own retry = %v, want it to carry the helper's refusal", err)
	}
	attempted := node.runtime.attempted()
	if len(attempted) != 1 {
		t.Fatalf("Backup-copy deletions attempted by the removal's own retry = %v, want exactly one "+
			"(one accounting for one step of one removal)", attempted)
	}
	if !slices.Contains(node.copyIDs, attempted[0]) {
		t.Fatalf("the removal's retry deleted %q, want one of its own copies %v", attempted[0], node.copyIDs)
	}
}
