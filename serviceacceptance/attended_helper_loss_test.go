package serviceacceptance

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/runner/ocihelper"
)

// ---------------------------------------------------------------------------
// fakes
// ---------------------------------------------------------------------------

// fakeBarrier models the real BootBarrier's own state machine: a receipt is
// available only while the barrier is prepared, Invalidate drops that, Ensure
// is the single step that acquires and completes the verified sweep, and the
// capability reason is written by an Ensure outcome and by nothing else --
// which is the whole reason the loss driver has to re-Ensure to get one.
type fakeBarrier struct {
	session     attendedSession
	receipts    []ocihelper.VerifiedSweepReceipt
	prepared    bool
	ensures     int
	invalidated int
	// refuseEnsure is the fault window. While it answers true, Ensure fails
	// with refusal and records refusalReason, exactly as the real barrier's
	// recordCapabilityReason does on a failed takeover.
	refuseEnsure  func() bool
	refusal       error
	refusalReason contract.CapabilityReasonCode
	// reason is the last Ensure outcome, never set by anything else.
	reason    contract.CapabilityReasonCode
	ensureErr error
}

func (fake *fakeBarrier) Ensure(context.Context) error {
	if fake.ensureErr != nil {
		fake.reason = fake.refusalReason
		fake.prepared = false
		return fake.ensureErr
	}
	if fake.refuseEnsure != nil && fake.refuseEnsure() {
		fake.reason = fake.refusalReason
		fake.prepared = false
		return fake.refusal
	}
	fake.ensures++
	fake.prepared = true
	fake.reason = ""
	return nil
}

func (fake *fakeBarrier) Invalidate() {
	fake.invalidated++
	fake.prepared = false
}

func (fake *fakeBarrier) SweepReceipt() (ocihelper.VerifiedSweepReceipt, bool) {
	if len(fake.receipts) == 0 || !fake.prepared {
		return ocihelper.VerifiedSweepReceipt{}, false
	}
	index := min(fake.ensures, len(fake.receipts)-1)
	return fake.receipts[index], true
}

// fakeProbe answers the way ocirunner.Adapter.Probe does: it cannot run at all
// without a prepared barrier, and the refusal is the barrier's own wording.
func (fake *fakeBarrier) fakeProbe(context.Context) error {
	if !fake.prepared {
		return errors.New("OCI boot barrier has not completed")
	}
	return nil
}

func (fake *fakeBarrier) CapabilityReasonCode() contract.CapabilityReasonCode { return fake.reason }

func (fake *fakeBarrier) Session() (attendedSession, error) { return fake.session, nil }

type fakeInjector struct {
	injected, restored int
	injectErr          error
	onInject           func()
	onRestore          func()
}

func (fake *fakeInjector) inject(context.Context) ([]string, error) {
	fake.injected++
	if fake.onInject != nil {
		fake.onInject()
	}
	return []string{"fake inject"}, fake.injectErr
}

func (fake *fakeInjector) restore(context.Context) ([]string, error) {
	fake.restored++
	if fake.onRestore != nil {
		fake.onRestore()
	}
	return []string{"fake restore"}, nil
}

func helperSession(id string, generation uint64) ocihelper.HelperSession {
	return ocihelper.HelperSession{HelperInstanceID: id, SessionGeneration: generation}
}

func lossIdentity(t *testing.T, config attendedConfig, kind faultKind) ocihelper.ResourceIdentity {
	t.Helper()
	identity, err := ocihelper.DeterministicResourceIdentity(config.authority(contract.JobClassService, "loss-"+string(kind)))
	if err != nil {
		t.Fatal(err)
	}
	return identity
}

func healthyObservation(t *testing.T) lossObservation {
	t.Helper()
	config := testConfig()
	identity := lossIdentity(t, config, faultHelperLoss)
	sweepAt := time.Unix(1700000000, 0)
	return lossObservation{
		kind: faultHelperLoss, identity: identity,
		preGeneration: helperSession("helper-a", 4), postGeneration: helperSession("helper-b", 5),
		preInventory:        ocihelper.ResourceInventory{Containers: []string{identity.ContainerID}},
		postInventory:       ocihelper.ResourceInventory{},
		sweptInventory:      ocihelper.ResourceInventory{Containers: []string{identity.ContainerID}},
		verifiedAbsent:      true,
		controlStreamFailed: "helper session lost", oldTunnelRefused: "connection refused",
		newClaimRefused:         "runtime unavailable",
		preSweepProbeRefused:    "OCI boot barrier has not completed",
		preSweepBarrierPrepared: false,
		withdrawalEnsureRefused: "dial oci helper: connect: connection refused",
		sweepObservedAt:         sweepAt, probeObservedAt: sweepAt.Add(3 * time.Second),
		withdrawn: contract.CapabilityObservation{Revision: 1, ReasonCode: contract.CapabilityReasonHelperUnitUnavailable},
		reopened:  contract.CapabilityObservation{Revision: 2},
	}
}

// ---------------------------------------------------------------------------
// checkLossTransition
// ---------------------------------------------------------------------------

func TestCheckLossTransitionAcceptsACompleteRecovery(t *testing.T) {
	if err := checkLossTransition(healthyObservation(t)); err != nil {
		t.Fatalf("a complete recovery must pass: %v", err)
	}
}

func TestCheckLossTransitionRejectsIncompleteRecoveries(t *testing.T) {
	for name, mutate := range map[string]func(*lossObservation){
		"old control stream survived": func(o *lossObservation) { o.controlStreamFailed = "" },
		"old tunnel still reachable":  func(o *lossObservation) { o.oldTunnelRefused = "" },
		"new claim admitted":          func(o *lossObservation) { o.newClaimRefused = "" },
		"generation did not advance":  func(o *lossObservation) { o.postGeneration = o.preGeneration },
		"no pre-fault generation":     func(o *lossObservation) { o.preGeneration = ocihelper.HelperSession{} },
		"absence not verified":        func(o *lossObservation) { o.verifiedAbsent = false },
		"old resources neither swept nor absent": func(o *lossObservation) {
			o.sweptInventory = ocihelper.ResourceInventory{}
			o.postInventory = ocihelper.ResourceInventory{Containers: []string{o.identity.ContainerID}}
		},
		"pre-fault workload never observed live": func(o *lossObservation) {
			o.preInventory = ocihelper.ResourceInventory{}
		},
		"probe never passed":             func(o *lossObservation) { o.probeObservedAt = time.Time{} },
		"capability did not reopen":      func(o *lossObservation) { o.reopened = contract.CapabilityObservation{} },
		"withdrawal reason not in vocab": func(o *lossObservation) { o.withdrawn.ReasonCode = "invented_reason" },
		"withdrawal carries no reason":   func(o *lossObservation) { o.withdrawn.ReasonCode = "" },
		"in-window re-Ensure was accepted": func(o *lossObservation) {
			o.withdrawalEnsureRefused = ""
		},
	} {
		observation := healthyObservation(t)
		mutate(&observation)
		if err := checkLossTransition(observation); err == nil {
			t.Fatalf("%s must fail the loss row", name)
		}
	}
}

// A VM that came back with an already-clean runtime sweeps nothing, which is
// admissible only because the recovering generation still verified absence.
func TestCheckLossTransitionAcceptsAnAlreadyCleanRuntimeAfterVMLoss(t *testing.T) {
	observation := healthyObservation(t)
	observation.kind = faultVMLoss
	observation.sweptInventory = ocihelper.ResourceInventory{}
	observation.postInventory = ocihelper.ResourceInventory{}
	if err := checkLossTransition(observation); err != nil {
		t.Fatalf("a verified-absent recovery with nothing left to sweep must pass: %v", err)
	}
	observation.verifiedAbsent = false
	if err := checkLossTransition(observation); err == nil {
		t.Fatal("that branch must not be admissible without the independent absence verification")
	}
}

// ---------------------------------------------------------------------------
// checkSweepOrdering
// ---------------------------------------------------------------------------

func TestCheckSweepOrdering(t *testing.T) {
	if err := checkSweepOrdering(healthyObservation(t)); err != nil {
		t.Fatalf("a sweep that precedes the probe must pass: %v", err)
	}

	probedEarly := healthyObservation(t)
	probedEarly.preSweepProbeRefused = ""
	if err := checkSweepOrdering(probedEarly); err == nil {
		t.Fatal("a probe that passed before the verified sweep must fail the row")
	}

	unrelated := healthyObservation(t)
	unrelated.preSweepProbeRefused = "pinned local OCI image is unavailable"
	if err := checkSweepOrdering(unrelated); err == nil {
		t.Fatal("a pre-sweep failure for an unrelated reason must not satisfy the ordering claim")
	}

	stillPrepared := healthyObservation(t)
	stillPrepared.preSweepBarrierPrepared = true
	if err := checkSweepOrdering(stillPrepared); err == nil {
		t.Fatal("a barrier that still held a sweep receipt must fail the ordering claim")
	}

	inverted := healthyObservation(t)
	inverted.sweepObservedAt = inverted.probeObservedAt.Add(time.Second)
	if err := checkSweepOrdering(inverted); err == nil {
		t.Fatal("a sweep observed after the probe must fail the row")
	}

	missing := healthyObservation(t)
	missing.probeObservedAt = time.Time{}
	if err := checkSweepOrdering(missing); err == nil {
		t.Fatal("an unobserved probe must fail the row")
	}
}

// ---------------------------------------------------------------------------
// capability ledger
// ---------------------------------------------------------------------------

func TestCapabilityLedgerAdvancesOnlyOnObservedTransitions(t *testing.T) {
	moment := time.Unix(1700000000, 0)
	ledger := newCapabilityLedger(func() time.Time { moment = moment.Add(time.Second); return moment })
	withdrawn := ledger.withdraw(contract.CapabilityReasonLimaStopped)
	reopened := ledger.reopen()
	if withdrawn.Revision != 1 || reopened.Revision != 2 {
		t.Fatalf("revisions = %d then %d, want a monotonic pair", withdrawn.Revision, reopened.Revision)
	}
	if withdrawn.Capabilities["kind:oci"] || !reopened.Capabilities["kind:oci"] {
		t.Fatalf("observations = %+v then %+v, want restrictive then reopened", withdrawn, reopened)
	}
	if !withdrawn.ReasonCode.ValidOCIRestriction() {
		t.Fatalf("withdrawal reason %q must be a valid OCI restriction", withdrawn.ReasonCode)
	}
	second := ledger.withdraw(contract.CapabilityReasonHelperUnreachable)
	if second.Revision != 3 {
		t.Fatalf("a second execution must continue the same monotonic ledger, got %d", second.Revision)
	}
	if len(ledger.observations) != 3 {
		t.Fatalf("ledger recorded %d observations, want one per transition", len(ledger.observations))
	}
}

// ---------------------------------------------------------------------------
// lossRow
// ---------------------------------------------------------------------------

func TestLossRowCarriesTheGateTransitionEvidence(t *testing.T) {
	config := testConfig()
	row := lossRow(config, healthyObservation(t), checkLossTransition, "recovery proven", "")
	if row.Status != "PASS" || row.ExitCode != 0 {
		t.Fatalf("row = %+v, want PASS", row)
	}
	if len(row.HelperGenerations) < 2 || len(row.CapabilityRevisions) < 2 || len(row.Inventories) < 2 {
		t.Fatalf("row = %+v, want at least two generations, revisions and inventories", row)
	}
	if row.SessionID != config.SessionID || len(row.Command) == 0 {
		t.Fatalf("row = %+v, want its own session id and command", row)
	}
	if !strings.Contains(row.Reason, capabilityRevisionNote) {
		t.Fatalf("row reason must say the revisions are local observations: %s", row.Reason)
	}
	if !strings.Contains(row.Reason, "fault execution") {
		t.Fatalf("row reason must name the fault execution it came from: %s", row.Reason)
	}
}

func TestLossRowNeverPassesOnAFailedCheck(t *testing.T) {
	observation := healthyObservation(t)
	observation.verifiedAbsent = false
	row := lossRow(testConfig(), observation, checkLossTransition, "recovery proven", "")
	if row.Status == "PASS" || row.ExitCode == 0 {
		t.Fatalf("row = %+v, want FAIL", row)
	}
	if len(row.Inventories) < 2 {
		t.Fatalf("a failed row must still carry the evidence it observed: %+v", row)
	}
}

func TestLossRowsFromOneExecutionShareTheEvidenceAndDifferInClaim(t *testing.T) {
	config := testConfig()
	observation := healthyObservation(t)
	recovery := lossRow(config, observation, checkLossTransition, "recovery", "")
	ordering := lossRow(config, observation, checkSweepOrdering, "ordering", sweepOrderingNote)
	if recovery.Status != "PASS" || ordering.Status != "PASS" {
		t.Fatalf("both rows from one fault execution must pass: %+v %+v", recovery, ordering)
	}
	if len(recovery.HelperGenerations) != len(ordering.HelperGenerations) {
		t.Fatal("both rows must carry the same observed generations")
	}
	if recovery.SessionID != ordering.SessionID || len(ordering.Command) == 0 || ordering.ExitCode != 0 {
		t.Fatalf("each row carries its own session_id, command and exit_code: %+v", ordering)
	}
	if !strings.Contains(ordering.Reason, sweepOrderingNote) {
		t.Fatalf("the ordering row must say the claim is structural, not a timing race: %s", ordering.Reason)
	}
	if !strings.Contains(recovery.Reason, restrictiveObservationNote) {
		t.Fatalf("the recovery row must qualify its restrictive observations: %s", recovery.Reason)
	}
}

// ---------------------------------------------------------------------------
// driveLossSequence end to end, with fakes
// ---------------------------------------------------------------------------

func lossFakes(t *testing.T, config attendedConfig, tunnelSurvives bool) (*fakeBarrier, *fakeSession, *fakeInjector) {
	t.Helper()
	origin := echoOrigin(t)
	identity := lossIdentity(t, config, faultHelperLoss)
	faulted := false
	session := &fakeSession{
		handshake: ocihelper.AcquireSessionResponse{MaximumAttemptDeadman: time.Minute},
	}
	session.runFunc = func(ocihelper.RunRequest) (ocihelper.RunResponse, error) {
		if faulted {
			return ocihelper.RunResponse{}, errors.New("OCI helper runtime lost")
		}
		response := startedResponse()
		response.Endpoints = map[string]uint16{"service": 18080}
		return response, nil
	}
	session.verifyResp = ocihelper.VerifyResponse{
		Inventory: ocihelper.ResourceInventory{Containers: []string{identity.ContainerID}},
	}
	session.dialAttempt = func(ocihelper.DialAttemptPortRequest) (net.Conn, error) {
		if faulted && !tunnelSurvives {
			return nil, errors.New("OCI helper runtime lost")
		}
		return net.Dial("tcp", origin.Addr().String())
	}
	barrier := &fakeBarrier{
		session: session, prepared: true,
		// While the fault stands, a re-Ensure cannot reach the helper. That
		// refusal is what gives the barrier a typed reason at all, and the
		// reason is one BootBarrier.recordCapabilityReason actually emits: it
		// only ever yields helper_unit_unavailable, helper_handshake_stalled or
		// boot_sweep_failed, so pinning anything else would be a fake the real
		// barrier could not produce.
		refuseEnsure:  func() bool { return faulted },
		refusal:       errors.New("dial oci helper: connect: connection refused"),
		refusalReason: contract.CapabilityReasonHelperUnitUnavailable,
		receipts: []ocihelper.VerifiedSweepReceipt{
			{HelperSession: helperSession("helper-a", 4),
				VerifiedInventory: ocihelper.ResourceInventory{Containers: []string{identity.ContainerID}}},
			{HelperSession: helperSession("helper-b", 5), VerifiedAbsent: true,
				SweptInventory:    ocihelper.ResourceInventory{Containers: []string{identity.ContainerID}},
				VerifiedInventory: ocihelper.ResourceInventory{}},
		},
	}
	// The fault lands exactly when the injector runs, and the runtime stays
	// unreachable -- to the old session and to any re-Ensure alike -- until the
	// injector returns it.
	injector := &fakeInjector{
		onInject: func() {
			faulted = true
			session.health = errors.New("OCI helper runtime lost")
		},
		onRestore: func() { faulted = false },
	}
	return barrier, session, injector
}

func TestDriveLossSequenceProducesBothRows(t *testing.T) {
	config := testConfig()
	barrier, _, injector := lossFakes(t, config, false)
	moment := time.Unix(1700000000, 0)
	config.Now = func() time.Time { moment = moment.Add(time.Second); return moment }
	ledger := newCapabilityLedger(config.Now)
	probeCalls := 0
	observation, err := driveLossSequence(context.Background(), lossDependencies{
		barrier: barrier, config: config, ledger: ledger, injector: injector,
		kind: faultHelperLoss, settle: time.Second,
		probe: func(probeContext context.Context) error {
			probeCalls++
			return barrier.fakeProbe(probeContext)
		},
	})
	if err != nil {
		t.Fatalf("loss sequence: %v", err)
	}
	if probeCalls != 2 {
		t.Fatalf("probe was called %d times, want once before the sweep and once after", probeCalls)
	}
	if injector.injected != 1 || injector.restored != 1 {
		t.Fatalf("injector = %+v, want exactly one fault and one restore", injector)
	}
	// The initial acquire happens in the entrypoint, so the sequence itself
	// invalidates twice -- once before the in-window re-Ensure that produces
	// the typed reason, once before the recovery -- and re-acquires exactly
	// once, since the in-window Ensure is refused.
	if barrier.invalidated != 2 || barrier.ensures != 1 {
		t.Fatalf("barrier invalidated=%d ensures=%d, want one re-acquire", barrier.invalidated, barrier.ensures)
	}
	if err := checkLossTransition(observation); err != nil {
		t.Fatalf("recovery assertion: %v", err)
	}
	if err := checkSweepOrdering(observation); err != nil {
		t.Fatalf("ordering assertion: %v", err)
	}
	recovery := lossRow(config, observation, checkLossTransition, "helper_loss", "")
	ordering := lossRow(config, observation, checkSweepOrdering, "sweep_before_recovery", sweepOrderingNote)
	if recovery.Status != "PASS" || ordering.Status != "PASS" {
		t.Fatalf("rows = %+v %+v, want both PASS from one fault execution", recovery, ordering)
	}
}

// The barrier records a capability reason only as an Ensure outcome, so the
// driver has to take one inside the fault window. Without that the withdrawal
// carries "" and cannot explain an OCI restriction at all.
func TestDriveLossSequenceTakesItsTypedReasonFromAnInWindowReEnsure(t *testing.T) {
	config := testConfig()
	barrier, _, injector := lossFakes(t, config, false)
	observation, err := driveLossSequence(context.Background(), lossDependencies{
		barrier: barrier, config: config, ledger: newCapabilityLedger(nil), injector: injector,
		kind: faultHelperLoss, settle: 100 * time.Millisecond, probe: barrier.fakeProbe,
	})
	if err != nil {
		t.Fatalf("loss sequence: %v", err)
	}
	if observation.withdrawalEnsureRefused == "" {
		t.Fatal("the in-window re-Ensure must be recorded, refusal and all")
	}
	if observation.withdrawn.ReasonCode != contract.CapabilityReasonHelperUnitUnavailable {
		t.Fatalf("withdrawal reason = %q, want the barrier's classification of the in-window refusal",
			observation.withdrawn.ReasonCode)
	}
	if !observation.withdrawn.ReasonCode.ValidOCIRestriction() {
		t.Fatalf("withdrawal reason %q must explain an OCI restriction", observation.withdrawn.ReasonCode)
	}
	if err := checkLossTransition(observation); err != nil {
		t.Fatalf("recovery assertion: %v", err)
	}
	// The in-window refusal must not be mistaken for the recovery acquire.
	if barrier.ensures != 1 {
		t.Fatalf("barrier ensures = %d, want exactly one successful re-acquire", barrier.ensures)
	}
	row := lossRow(config, observation, checkLossTransition, "helper_loss", "")
	if row.Status != "PASS" {
		t.Fatalf("row = %+v, want PASS", row)
	}
	for _, phrase := range []string{capabilityReasonNote, string(contract.CapabilityReasonHelperUnitUnavailable)} {
		if !strings.Contains(row.Reason, phrase) {
			t.Fatalf("row reason must say where the typed reason came from, got %q", row.Reason)
		}
	}
}

// A runtime that accepts a re-Ensure mid-fault is not restricted, and the row
// must fail rather than report a withdrawal it cannot explain.
func TestDriveLossSequenceFailsWhenTheInWindowReEnsureIsAccepted(t *testing.T) {
	config := testConfig()
	barrier, _, injector := lossFakes(t, config, false)
	barrier.refuseEnsure = nil
	observation, err := driveLossSequence(context.Background(), lossDependencies{
		barrier: barrier, config: config, ledger: newCapabilityLedger(nil), injector: injector,
		kind: faultHelperLoss, settle: 100 * time.Millisecond, probe: barrier.fakeProbe,
	})
	if err != nil {
		t.Fatalf("loss sequence: %v", err)
	}
	if observation.withdrawalEnsureRefused != "" || observation.withdrawn.ReasonCode != "" {
		t.Fatalf("observation = %+v, want no refusal and no typed reason", observation)
	}
	if err := checkLossTransition(observation); err == nil {
		t.Fatal("a withdrawal with no observed refusal behind it must fail the row")
	}
	if row := lossRow(config, observation, checkLossTransition, "helper_loss", ""); row.Status == "PASS" {
		t.Fatalf("row = %+v, want FAIL", row)
	}
}

func TestDriveLossSequenceFailsWhenTheOldTunnelSurvives(t *testing.T) {
	config := testConfig()
	barrier, _, injector := lossFakes(t, config, true)
	observation, err := driveLossSequence(context.Background(), lossDependencies{
		barrier: barrier, config: config, ledger: newCapabilityLedger(nil), injector: injector,
		kind: faultHelperLoss, settle: 100 * time.Millisecond, probe: barrier.fakeProbe,
	})
	if err != nil {
		t.Fatalf("loss sequence: %v", err)
	}
	if err := checkLossTransition(observation); err == nil {
		t.Fatal("a tunnel that survives the injected loss must fail the row")
	}
}

// ---------------------------------------------------------------------------
// fault injection
// ---------------------------------------------------------------------------

func TestFaultCommandsNameTheRunbookActions(t *testing.T) {
	inject, restore := faultCommands("wefty-oci", faultHelperLoss)
	if !strings.Contains(strings.Join(inject, " "), "systemctl stop dev.wefty.oci-helper.socket") {
		t.Fatalf("helper fault = %v, want the helper socket and service stopped", inject)
	}
	if !strings.Contains(strings.Join(restore, " "), "systemctl start dev.wefty.oci-helper.socket") {
		t.Fatalf("helper restore = %v, want the socket unit started", restore)
	}
	inject, restore = faultCommands("wefty-oci", faultVMLoss)
	if strings.Join(inject, " ") != "limactl stop wefty-oci" || strings.Join(restore, " ") != "limactl start wefty-oci" {
		t.Fatalf("vm fault = %v / %v, want a full instance stop and start", inject, restore)
	}
}

func TestLimactlFaultInjectorReportsTheCommandItRan(t *testing.T) {
	var seen [][]string
	injector := &limactlFaultInjector{instance: "wefty-oci", kind: faultVMLoss,
		run: func(_ context.Context, argv []string) ([]byte, error) { seen = append(seen, argv); return nil, nil }}
	performed, err := injector.inject(context.Background())
	if err != nil || len(performed) != 1 || performed[0] != "limactl stop wefty-oci" {
		t.Fatalf("inject = (%v, %v)", performed, err)
	}
	if _, err := injector.restore(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(seen) != 2 {
		t.Fatalf("ran %d commands, want the fault and the restore", len(seen))
	}
}

func TestManualFaultInjectorPromptsAndWaits(t *testing.T) {
	prompt := &bytes.Buffer{}
	ack := filepath.Join(t.TempDir(), "ack")
	injector := newManualFaultInjector("wefty-oci", faultHelperLoss, prompt, ack)
	injector.wait = func(_ context.Context, path string) error {
		if path != ack {
			t.Fatalf("waited on %q, want the acknowledgement path", path)
		}
		return os.WriteFile(path, nil, 0o600)
	}
	performed, err := injector.inject(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(prompt.String(), "ATTENDED FAULT") || !strings.Contains(prompt.String(), ack) {
		t.Fatalf("prompt must name the action and the acknowledgement file: %q", prompt.String())
	}
	if !strings.Contains(strings.Join(performed, " "), "operator-performed") {
		t.Fatalf("performed = %v, want the command recorded as operator-performed", performed)
	}
}

func TestWaitForAcknowledgementHonoursCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := waitForAcknowledgement(ctx, filepath.Join(t.TempDir(), "never")); err == nil {
		t.Fatal("a cancelled wait must return an error rather than block the window")
	}
}

// ---------------------------------------------------------------------------
// guest_to_host_fallback
// ---------------------------------------------------------------------------

// fallbackSession answers the bridge the way a live attempt would: the live
// capability and authority get a connected guest that makes exactly one
// authenticated run-scoped request, and everything else is refused with the
// typed code -- except the negative named in accept, which is admitted so the
// test can prove an accepted negative fails the row.
func fallbackSession(t *testing.T, accept string) *fakeSession {
	t.Helper()
	const capability = "attended-bridge-capability"
	session := &fakeSession{
		handshake:  ocihelper.AcquireSessionResponse{MaximumAttemptDeadman: time.Minute},
		deleteResp: ocihelper.DeleteResponse{Deleted: true},
		verifyResp: ocihelper.VerifyResponse{Absent: true},
		runFunc: func(request ocihelper.RunRequest) (ocihelper.RunResponse, error) {
			if !request.EnableHostBridgeFallback || !request.ActivateHostBridgeFallback {
				return ocihelper.RunResponse{}, errors.New("fallback row must request an active host bridge")
			}
			response := startedResponse()
			response.HostBridgeReady = true
			response.BridgeCapability = capability
			response.HostBridgeEndpoint = "127.0.0.1:19999"
			return response, nil
		},
		watchFunc: func(ocihelper.AttemptAuthority) []ocihelper.WatchEvent {
			return logEvents("wefty-echo-once-stdout", "wefty-echo-once-stderr", 0)
		},
	}
	refused := &ocihelper.RPCError{Code: ocihelper.CodeUnauthorizedBridge,
		Message: "bridge fallback is not authorized for this live attempt"}
	guestConnected := false
	session.dialBridge = func(request ocihelper.DialHostBridgeRequest) (net.Conn, error) {
		liveAttempt := request.Authority.AttemptID == "attended-guest-to-host-fallback"
		liveCapability := request.BridgeCapability == capability
		if !liveAttempt || !liveCapability {
			// The negative named in accept is admitted instead of refused, so
			// the row has to notice that the helper let it through.
			if accept == "capability" && liveAttempt || accept == "attempt" && liveCapability {
				admitted, _ := net.Pipe()
				return admitted, nil
			}
			return nil, refused
		}
		if guestConnected {
			// The pump reconnects in a loop; one payload makes one request.
			return nil, refused
		}
		guestConnected = true
		guest, helper := net.Pipe()
		go func() {
			defer guest.Close()
			httpRequest, err := http.NewRequest(http.MethodGet,
				"http://bridge.invalid/v1/runs/"+attendedFallbackRunID, nil)
			if err != nil {
				return
			}
			httpRequest.Header.Set("Authorization", "Bearer "+attendedFallbackRunToken)
			if err := httpRequest.Write(guest); err != nil {
				return
			}
			response, err := http.ReadResponse(bufio.NewReader(guest), httpRequest)
			if err != nil {
				return
			}
			_, _ = io.Copy(io.Discard, response.Body)
			_ = response.Body.Close()
		}()
		return helper, nil
	}
	return session
}

// The helper names a handoff volume from the descriptor's OwnerKey, not from
// the attempt digest. This is the only row that requests one, so it is the
// only place the distinction can be caught: a proof written against the
// attempt-derived name would look at a name that can never appear and pass
// over a real leak.
func TestDriveGuestToHostFallbackNamesTheHandoffVolumeFromItsOwnerKey(t *testing.T) {
	ownerName, err := ocihelper.DeterministicHandoffVolumeDirectory(attendedFallbackHandoffOwner)
	if err != nil {
		t.Fatal(err)
	}
	config := testConfig()
	identity, err := ocihelper.DeterministicResourceIdentity(
		config.authority(contract.JobClassOneShot, "guest-to-host-fallback"))
	if err != nil {
		t.Fatal(err)
	}
	if ownerName == identity.HandoffVolumeDirectory {
		t.Fatal("the owner-key and attempt-digest handoff names must differ for this row to mean anything")
	}

	leaked := ocihelper.ResourceInventory{ManagedVolumes: []string{ownerName}}
	session := fallbackSession(t, "")
	session.verifyResp = ocihelper.VerifyResponse{Inventory: leaked, RuntimeResidue: leaked}
	if row := driveGuestToHostFallback(context.Background(), session, config); row.Status == "PASS" {
		t.Fatalf("row = %+v, want FAIL when the attempt's own handoff volume is left as runtime residue", row)
	}

	// Observed but not residue is the helper retaining it for its owner to
	// collect, which is the contract working rather than a leak.
	retained := fallbackSession(t, "")
	retained.verifyResp = ocihelper.VerifyResponse{Inventory: leaked, DurableRetained: leaked}
	if row := driveGuestToHostFallback(context.Background(), retained, config); row.Status != "PASS" {
		t.Fatalf("row = %+v, want PASS when the handoff volume is merely retained", row)
	}

	// The attempt-digest name is not this attempt's handoff volume at all, so
	// a proof pinned to it would have been vacuous.
	digestNamed := ocihelper.ResourceInventory{ManagedVolumes: []string{identity.HandoffVolumeDirectory}}
	vacuous := fallbackSession(t, "")
	vacuous.verifyResp = ocihelper.VerifyResponse{Inventory: digestNamed, RuntimeResidue: digestNamed}
	if row := driveGuestToHostFallback(context.Background(), vacuous, config); row.Status != "PASS" {
		t.Fatalf("row = %+v: the attempt-digest handoff name names nothing this attempt owns", row)
	}
}

func TestDriveGuestToHostFallbackPasses(t *testing.T) {
	row := driveGuestToHostFallback(context.Background(), fallbackSession(t, ""), testConfig())
	if row.Status != "PASS" || row.ExitCode != 0 {
		t.Fatalf("row = %+v, want PASS", row)
	}
	if !row.RoundTrip {
		t.Fatalf("row = %+v, want the one authenticated bridge request recorded", row)
	}
	if !strings.Contains(row.Reason, attendedFallbackRunID) {
		t.Fatalf("row reason must name the run the origin served: %s", row.Reason)
	}
	if !strings.Contains(row.Reason, "discovery failure") {
		t.Fatalf("row reason must state the clause this surface cannot exercise: %s", row.Reason)
	}
}

func TestDriveGuestToHostFallbackFailsWhenANegativeIsAccepted(t *testing.T) {
	for _, accepted := range []string{"capability", "attempt"} {
		row := driveGuestToHostFallback(context.Background(), fallbackSession(t, accepted), testConfig())
		if row.Status == "PASS" || row.ExitCode == 0 {
			t.Fatalf("the %s negative was accepted yet the row = %+v", accepted, row)
		}
		if !strings.Contains(row.Reason, "ACCEPTED-OR-UNTYPED") {
			t.Fatalf("the %s negative reason must record the acceptance: %s", accepted, row.Reason)
		}
	}
}

func TestDriveGuestToHostFallbackFailsWithoutBridgeAuthority(t *testing.T) {
	session := fallbackSession(t, "")
	session.runFunc = func(ocihelper.RunRequest) (ocihelper.RunResponse, error) { return startedResponse(), nil }
	row := driveGuestToHostFallback(context.Background(), session, testConfig())
	if row.Status == "PASS" {
		t.Fatalf("row = %+v, want FAIL when the helper issued no bridge capability", row)
	}
}

func TestDriveGuestToHostFallbackFailsWhenTheOriginIsNeverReached(t *testing.T) {
	session := fallbackSession(t, "")
	session.dialBridge = func(ocihelper.DialHostBridgeRequest) (net.Conn, error) {
		return nil, errors.New("no guest ever connected")
	}
	config := testConfig()
	config.BridgeWait = 250 * time.Millisecond
	row := driveGuestToHostFallback(context.Background(), session, config)
	if row.Status == "PASS" {
		t.Fatalf("row = %+v, want FAIL when no authenticated request reached the host origin", row)
	}
}

func TestHostBridgeOriginRefusesUnauthenticatedAndMisroutedRequests(t *testing.T) {
	served := make(chan string, 4)
	listener, serveErr, err := startHostBridgeOrigin(attendedFallbackRunID, attendedFallbackRunToken, served)
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	client := &http.Client{Timeout: 5 * time.Second}
	base := "http://" + listener.Addr().String()

	response, err := client.Get(base + "/v1/runs/" + attendedFallbackRunID)
	if err != nil {
		t.Fatal(err)
	}
	_ = response.Body.Close()
	if response.StatusCode != http.StatusUnauthorized || serveErr() == nil {
		t.Fatalf("unauthenticated request = HTTP %d, rejection %v; want a refusal", response.StatusCode, serveErr())
	}
}

// ---------------------------------------------------------------------------
// restrictive observations
// ---------------------------------------------------------------------------

func TestRestrictiveFailureExcludesThisDriversOwnContextErrors(t *testing.T) {
	if restrictiveFailure(nil) {
		t.Fatal("a successful call is not a refusal")
	}
	if restrictiveFailure(context.DeadlineExceeded) || restrictiveFailure(context.Canceled) {
		t.Fatal("this driver's own deadline says nothing about the runtime refusing")
	}
	if restrictiveFailure(fmt.Errorf("dial: %w", context.DeadlineExceeded)) {
		t.Fatal("a wrapped context deadline must not count either")
	}
	if !restrictiveFailure(errors.New("OCI helper runtime lost")) {
		t.Fatal("a transport loss is the runtime refusing")
	}
}
