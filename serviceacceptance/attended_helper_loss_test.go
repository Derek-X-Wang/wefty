package serviceacceptance

import (
	"bytes"
	"context"
	"errors"
	"net"
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

type fakeBarrier struct {
	session     attendedSession
	receipts    []ocihelper.VerifiedSweepReceipt
	ensures     int
	invalidated int
	reason      contract.CapabilityReasonCode
	ensureErr   error
}

func (fake *fakeBarrier) Ensure(context.Context) error {
	if fake.ensureErr != nil {
		return fake.ensureErr
	}
	fake.ensures++
	return nil
}

func (fake *fakeBarrier) Invalidate() { fake.invalidated++ }

func (fake *fakeBarrier) SweepReceipt() (ocihelper.VerifiedSweepReceipt, bool) {
	if len(fake.receipts) == 0 {
		return ocihelper.VerifiedSweepReceipt{}, false
	}
	index := min(fake.ensures, len(fake.receipts)-1)
	return fake.receipts[index], true
}

func (fake *fakeBarrier) CapabilityReasonCode() contract.CapabilityReasonCode { return fake.reason }

func (fake *fakeBarrier) Session() (attendedSession, error) { return fake.session, nil }

type fakeInjector struct {
	injected, restored int
	injectErr          error
	onInject           func()
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
		newClaimRefused: "runtime unavailable", preSweepProbeRefused: "no prepared session",
		sweepObservedAt: sweepAt, probeObservedAt: sweepAt.Add(3 * time.Second),
		withdrawn: contract.CapabilityObservation{Revision: 1, ReasonCode: contract.CapabilityReasonHelperUnreachable},
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
	row := lossRow(config, healthyObservation(t), checkLossTransition, "recovery proven")
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
	row := lossRow(testConfig(), observation, checkLossTransition, "recovery proven")
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
	recovery := lossRow(config, observation, checkLossTransition, "recovery")
	ordering := lossRow(config, observation, checkSweepOrdering, "ordering")
	if recovery.Status != "PASS" || ordering.Status != "PASS" {
		t.Fatalf("both rows from one fault execution must pass: %+v %+v", recovery, ordering)
	}
	if len(recovery.HelperGenerations) != len(ordering.HelperGenerations) {
		t.Fatal("both rows must carry the same observed generations")
	}
	if recovery.SessionID != ordering.SessionID || len(ordering.Command) == 0 || ordering.ExitCode != 0 {
		t.Fatalf("each row carries its own session_id, command and exit_code: %+v", ordering)
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
		session: session, reason: contract.CapabilityReasonHelperUnreachable,
		receipts: []ocihelper.VerifiedSweepReceipt{
			{HelperSession: helperSession("helper-a", 4),
				VerifiedInventory: ocihelper.ResourceInventory{Containers: []string{identity.ContainerID}}},
			{HelperSession: helperSession("helper-b", 5), VerifiedAbsent: true,
				SweptInventory:    ocihelper.ResourceInventory{Containers: []string{identity.ContainerID}},
				VerifiedInventory: ocihelper.ResourceInventory{}},
		},
	}
	// The fault lands exactly when the injector runs, and the session stays
	// lost until the barrier re-acquires.
	injector := &fakeInjector{onInject: func() {
		faulted = true
		session.health = errors.New("OCI helper runtime lost")
	}}
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
		probe: func(context.Context) error {
			probeCalls++
			if barrier.ensures == 0 {
				return errors.New("OCI boot barrier is unavailable")
			}
			return nil
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
	if barrier.invalidated != 1 || barrier.ensures != 1 {
		t.Fatalf("barrier invalidated=%d ensures=%d, want one re-acquire", barrier.invalidated, barrier.ensures)
	}
	if err := checkLossTransition(observation); err != nil {
		t.Fatalf("recovery assertion: %v", err)
	}
	if err := checkSweepOrdering(observation); err != nil {
		t.Fatalf("ordering assertion: %v", err)
	}
	recovery := lossRow(config, observation, checkLossTransition, "helper_loss")
	ordering := lossRow(config, observation, checkSweepOrdering, "sweep_before_recovery")
	if recovery.Status != "PASS" || ordering.Status != "PASS" {
		t.Fatalf("rows = %+v %+v, want both PASS from one fault execution", recovery, ordering)
	}
}

func TestDriveLossSequenceFailsWhenTheOldTunnelSurvives(t *testing.T) {
	config := testConfig()
	barrier, _, injector := lossFakes(t, config, true)
	observation, err := driveLossSequence(context.Background(), lossDependencies{
		barrier: barrier, config: config, ledger: newCapabilityLedger(nil), injector: injector,
		kind: faultHelperLoss, settle: 100 * time.Millisecond,
		probe: func(context.Context) error {
			if barrier.ensures == 0 {
				return errors.New("OCI boot barrier is unavailable")
			}
			return nil
		},
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
