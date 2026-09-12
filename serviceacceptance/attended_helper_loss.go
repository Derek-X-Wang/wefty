package serviceacceptance

// Attended exclusive-session helper rows (#410), part two: the host-bridge
// fallback row and the fault-and-recovery rows.
//
// One helper-loss execution produces two rows -- helper_loss (the recovery)
// and sweep_before_recovery (the ordering assertion that the verified sweep
// precedes the functional probe) -- because injecting the same fault twice
// would prove nothing extra and would cost the owner a second window. Each
// row still carries its own session_id, command and exit_code, and each
// row's reason says which fault execution produced it.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/runner/ocihelper"
)

// ---------------------------------------------------------------------------
// Row 6: guest_to_host_fallback
// ---------------------------------------------------------------------------

func (config attendedConfig) bridgeWait() time.Duration {
	if config.BridgeWait > 0 {
		return config.BridgeWait
	}
	return attendedBridgeWait
}

const (
	attendedFallbackRunID    = "attended-fallback-run"
	attendedFallbackRunToken = "attended-fallback-token"
	// attendedBridgeWait bounds how long the row waits for the payload's one
	// authenticated request to reach the host origin after the payload exits.
	attendedBridgeWait = 30 * time.Second
)

// driveGuestToHostFallback proves the helper-issued per-attempt bridge
// capability and its wrong-capability and wrong-attempt refusals. The payload
// is the acceptance image's own one-shot mode, which makes exactly one
// authenticated run-scoped request through WEFTY_L3_ENDPOINT -- the endpoint
// the helper rewrites to the guest side of the host bridge when fallback is
// active.
func driveGuestToHostFallback(ctx context.Context, session attendedSession, config attendedConfig) attendedRow {
	row := newAttendedRow(config.SessionID, config.Command)
	authority := config.authority(contract.JobClassOneShot, "guest-to-host-fallback")
	row.AttemptIDs = []string{authority.AttemptID}

	runID := attendedFallbackRunID
	runToken := attendedFallbackRunToken
	served := make(chan string, 8)
	listener, serveErr, err := startHostBridgeOrigin(runID, runToken, served)
	if err != nil {
		row.fail(fmt.Errorf("start the host-side bridge origin: %w", err))
		return row
	}
	defer listener.Close()

	response, err := session.Run(ctx, ocihelper.RunRequest{
		Authority: authority, InitialDeadman: boundedDeadman(session, config.Deadman),
		EnableHostBridgeFallback: true, ActivateHostBridgeFallback: true,
		Workload: ocihelper.WorkloadInput{
			ImageReference: config.Reference, ImageDigest: config.Digest,
			Argv:        []string{"/usr/local/bin/wefty-echo-service", "--once"},
			L3Endpoint:  "http://l3-origin.invalid",
			RunToken:    runToken,
			Environment: []ocihelper.EnvironmentVariable{{Name: contract.EnvRunID, Value: runID}},
			ManagedVolumes: []ocihelper.ManagedVolumeDescriptor{{
				Kind: ocihelper.ManagedVolumeHandoff, OwnerKey: "attended-fallback-handoff-owner",
			}},
		},
	})
	if err != nil {
		row.fail(fmt.Errorf("run the fallback payload: %w", err))
		return row
	}
	defer reapAttempt(session, authority)
	if !response.Started || response.Image == nil {
		row.fail(fmt.Errorf("fallback run returned no Started evidence: %+v", response))
		return row
	}
	row.TopLevelDigests = []string{response.Image.TopLevelDigest}
	row.PlatformDigests = []string{response.Image.PlatformManifestDigest}
	if !response.HostBridgeReady || response.BridgeCapability == "" || response.HostBridgeEndpoint == "" {
		row.fail(fmt.Errorf(
			"helper omitted host bridge authority: ready=%t capability_present=%t endpoint=%q",
			response.HostBridgeReady, response.BridgeCapability != "", response.HostBridgeEndpoint))
		return row
	}

	pumpContext, stopPump := context.WithCancel(ctx)
	defer stopPump()
	go pumpAttendedHostBridge(pumpContext, session, authority, response.BridgeCapability, listener.Addr().String())

	evidence, err := collectLogEvidence(ctx, session, authority)
	if err != nil {
		row.fail(fmt.Errorf("watch the fallback payload: %w", err))
		return row
	}
	if err := checkLogEvidence(evidence, "wefty-echo-once-stdout", "wefty-echo-once-stderr"); err != nil {
		row.fail(fmt.Errorf("fallback payload did not complete its one authenticated bridge request: %w", err))
		return row
	}
	row.StdoutMarkers = []string{"wefty-echo-once-stdout"}
	row.StderrMarkers = []string{"wefty-echo-once-stderr"}
	row.PayloadExecutions = 1
	select {
	case path := <-served:
		row.RoundTrip = true
		row.appendReason("host bridge origin served " + path)
	case <-time.After(config.bridgeWait()):
		row.fail(errors.New("the host-side bridge origin served no authenticated request"))
		return row
	}
	if failure := serveErr(); failure != nil {
		row.fail(fmt.Errorf("host bridge origin rejected the request: %w", failure))
		return row
	}

	observations, err := runHostBridgeNegatives(ctx, session, config, authority, response.BridgeCapability)
	if err != nil {
		row.appendReason(observations)
		row.fail(err)
		return row
	}
	if err := deleteAndVerify(ctx, session, authority); err != nil {
		row.fail(err)
		return row
	}
	row.appendReason(attemptAbsenceNote)
	row.pass(fmt.Sprintf(
		"host-loopback bridge with a helper-issued per-attempt capability; one authenticated run-scoped request "+
			"completed through DialHostBridge; negatives: %s; not exercised by this surface: gateway discovery "+
			"failure must fail start and must not select fallback, which is agent-side (runner/lima) and outside "+
			"a direct helper-client session", observations))
	return row
}

// startHostBridgeOrigin stands up the host-side origin the guest reaches
// through the bridge. It authenticates exactly the way the run ledger does,
// so an unauthenticated or misrouted request cannot be mistaken for success.
func startHostBridgeOrigin(runID, runToken string, served chan<- string) (net.Listener, func() error, error) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, nil, err
	}
	// The handler runs on the server's own goroutine and serveErr is read from
	// the caller's, so the rejection needs a lock rather than a bare variable.
	var rejectionMu sync.Mutex
	var rejection error
	reject := func(err error) {
		rejectionMu.Lock()
		defer rejectionMu.Unlock()
		if rejection == nil {
			rejection = err
		}
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(writer http.ResponseWriter, request *http.Request) {
		if request.Header.Get("Authorization") != "Bearer "+runToken {
			reject(errors.New("bridge request carried no run-scoped bearer credential"))
			writer.WriteHeader(http.StatusUnauthorized)
			return
		}
		if !strings.HasSuffix(request.URL.Path, "/"+runID) {
			reject(fmt.Errorf("bridge request path %q did not name run %q", request.URL.Path, runID))
			writer.WriteHeader(http.StatusNotFound)
			return
		}
		select {
		case served <- request.URL.Path:
		default:
		}
		writer.Header().Set("Content-Type", "application/json")
		_, _ = writer.Write([]byte(`{"status":"ok"}`))
	})
	server := &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() { _ = server.Serve(listener) }()
	return listener, func() error {
		rejectionMu.Lock()
		defer rejectionMu.Unlock()
		return rejection
	}, nil
}

// pumpAttendedHostBridge is the attended equivalent of the agent's own host
// bridge pump: accept the helper-authorized guest side, relay it to the host
// origin, repeat.
func pumpAttendedHostBridge(ctx context.Context, session attendedSession, authority ocihelper.AttemptAuthority, capability, origin string) {
	for ctx.Err() == nil {
		helper, err := session.DialHostBridge(ctx, ocihelper.DialHostBridgeRequest{
			Authority: authority, BridgeCapability: capability,
		})
		if err != nil {
			return
		}
		host, err := net.Dial("tcp", origin)
		if err != nil {
			_ = helper.Close()
			return
		}
		_ = ocihelper.Relay(ctx, helper, host)
		_ = helper.Close()
		_ = host.Close()
	}
}

func runHostBridgeNegatives(ctx context.Context, session attendedSession, config attendedConfig, live ocihelper.AttemptAuthority, capability string) (string, error) {
	var observations []string
	_, capabilityErr := session.DialHostBridge(ctx, ocihelper.DialHostBridgeRequest{
		Authority: live, BridgeCapability: capability + "-wrong",
	})
	code, err := checkRefusal("wrong bridge capability", capabilityErr, ocihelper.CodeUnauthorizedBridge)
	observations = append(observations, "wrong_bridge_capability="+refusalCode(code, ocihelper.CodeUnauthorizedBridge, err))
	if err != nil {
		return strings.Join(observations, " "), err
	}

	wrongAttempt := config.authority(contract.JobClassOneShot, "guest-to-host-fallback-wrong-attempt")
	_, attemptErr := session.DialHostBridge(ctx, ocihelper.DialHostBridgeRequest{
		Authority: wrongAttempt, BridgeCapability: capability,
	})
	code, err = checkRefusal("wrong attempt tuple", attemptErr, ocihelper.CodeUnauthorizedBridge)
	observations = append(observations, "wrong_attempt_tuple="+refusalCode(code, ocihelper.CodeUnauthorizedBridge, err))
	return strings.Join(observations, " "), err
}

// ---------------------------------------------------------------------------
// Fault injection
// ---------------------------------------------------------------------------

type faultKind string

const (
	faultHelperLoss faultKind = "helper"
	faultVMLoss     faultKind = "vm"
)

// faultInjector takes the helper or the VM away and gives it back. Both
// implementations report the exact commands performed so the row's reason
// records them verbatim.
type faultInjector interface {
	inject(context.Context) ([]string, error)
	restore(context.Context) ([]string, error)
}

func faultCommands(instance string, kind faultKind) (inject, restore []string) {
	if kind == faultVMLoss {
		return []string{"limactl", "stop", instance},
			[]string{"limactl", "start", instance}
	}
	return []string{"limactl", "shell", "--workdir=/", instance, "sudo", "systemctl", "stop",
			"dev.wefty.oci-helper.socket", "dev.wefty.oci-helper.service"},
		[]string{"limactl", "shell", "--workdir=/", instance, "sudo", "systemctl", "start",
			"dev.wefty.oci-helper.socket"}
}

// limactlFaultInjector is the default: the driver takes the fault itself, so
// the window is reproducible and the timing is recorded rather than guessed.
type limactlFaultInjector struct {
	instance string
	kind     faultKind
	run      func(context.Context, []string) ([]byte, error)
}

func newLimactlFaultInjector(instance string, kind faultKind) *limactlFaultInjector {
	return &limactlFaultInjector{instance: instance, kind: kind, run: runFaultCommand}
}

func runFaultCommand(ctx context.Context, argv []string) ([]byte, error) {
	command := exec.CommandContext(ctx, argv[0], argv[1:]...)
	return command.CombinedOutput()
}

func (injector *limactlFaultInjector) inject(ctx context.Context) ([]string, error) {
	argv, _ := faultCommands(injector.instance, injector.kind)
	return injector.execute(ctx, argv)
}

func (injector *limactlFaultInjector) restore(ctx context.Context) ([]string, error) {
	_, argv := faultCommands(injector.instance, injector.kind)
	return injector.execute(ctx, argv)
}

func (injector *limactlFaultInjector) execute(ctx context.Context, argv []string) ([]string, error) {
	output, err := injector.run(ctx, argv)
	performed := []string{strings.Join(argv, " ")}
	if err != nil {
		return performed, fmt.Errorf("%s: %w (%s)", strings.Join(argv, " "), err, strings.TrimSpace(string(output)))
	}
	return performed, nil
}

// manualFaultInjector hands the fault to the operator, for the runbook's
// "manually returned for this attended ticket" phrasing. It prints one clear
// line naming the exact command and then waits for an acknowledgement file.
type manualFaultInjector struct {
	instance string
	kind     faultKind
	prompt   io.Writer
	ackPath  string
	wait     func(context.Context, string) error
}

func newManualFaultInjector(instance string, kind faultKind, prompt io.Writer, ackPath string) *manualFaultInjector {
	return &manualFaultInjector{instance: instance, kind: kind, prompt: prompt, ackPath: ackPath, wait: waitForAcknowledgement}
}

func (injector *manualFaultInjector) inject(ctx context.Context) ([]string, error) {
	argv, _ := faultCommands(injector.instance, injector.kind)
	return injector.ask(ctx, argv, "inject")
}

func (injector *manualFaultInjector) restore(ctx context.Context) ([]string, error) {
	_, argv := faultCommands(injector.instance, injector.kind)
	return injector.ask(ctx, argv, "restore")
}

func (injector *manualFaultInjector) ask(ctx context.Context, argv []string, phase string) ([]string, error) {
	line := strings.Join(argv, " ")
	fmt.Fprintf(injector.prompt,
		"\nATTENDED FAULT (%s %s): run this now in another terminal, then `touch %s` to continue:\n    %s\n",
		injector.kind, phase, injector.ackPath, line)
	if err := injector.wait(ctx, injector.ackPath); err != nil {
		return []string{line}, err
	}
	return []string{line + " (operator-performed)"}, nil
}

func waitForAcknowledgement(ctx context.Context, path string) error {
	deadline := time.Now().Add(15 * time.Minute)
	for {
		if _, err := os.Stat(path); err == nil {
			return os.Remove(path)
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("timed out waiting for the operator acknowledgement file %s", path)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}
}

// ---------------------------------------------------------------------------
// Rows 7-9: helper_loss, sweep_before_recovery, vm_loss
// ---------------------------------------------------------------------------

// lossObservation is one complete fault-and-recovery execution.
type lossObservation struct {
	kind                 faultKind
	commands             []string
	identity             ocihelper.ResourceIdentity
	preGeneration        ocihelper.HelperSession
	postGeneration       ocihelper.HelperSession
	preInventory         ocihelper.ResourceInventory
	postInventory        ocihelper.ResourceInventory
	sweptInventory       ocihelper.ResourceInventory
	verifiedAbsent       bool
	controlStreamFailed  string
	oldTunnelRefused     string
	newClaimRefused      string
	preSweepProbeRefused string
	// preSweepBarrierPrepared records whether the barrier still held a
	// verified sweep receipt at the moment the pre-sweep probe was refused.
	preSweepBarrierPrepared bool
	// withdrawalEnsureRefused is the bounded re-Ensure the driver runs inside
	// the fault window, verbatim. It is what gives the withdrawal below a
	// typed reason at all, so a row whose re-Ensure was not refused has no
	// observation of a restricted runtime and must fail.
	withdrawalEnsureRefused string
	sweepObservedAt         time.Time
	probeObservedAt         time.Time
	withdrawn               contract.CapabilityObservation
	reopened                contract.CapabilityObservation
	bootSessionReused       bool
}

// checkLossTransition is the recovery assertion: a genuinely new helper
// generation swept the pre-fault resources and independently verified the
// namespace absent, having first refused both the old tunnel and any new
// claim. Socket inode identity is explicitly not authority, which is why the
// generation comparison is the load-bearing one.
func checkLossTransition(observation lossObservation) error {
	if observation.controlStreamFailed == "" {
		return errors.New("the old helper control stream did not fail after the injected loss")
	}
	if observation.oldTunnelRefused == "" {
		return errors.New("the pre-fault tunnel was still reachable after the injected loss")
	}
	if observation.newClaimRefused == "" {
		return errors.New("a new OCI claim was admitted while the runtime was unavailable")
	}
	if observation.preGeneration == (ocihelper.HelperSession{}) {
		return errors.New("no pre-fault helper generation was observed")
	}
	if observation.postGeneration == (ocihelper.HelperSession{}) {
		return errors.New("no post-recovery helper generation was observed")
	}
	if observation.preGeneration == observation.postGeneration {
		return fmt.Errorf("helper generation did not advance across the loss: %+v", observation.postGeneration)
	}
	if !observation.verifiedAbsent {
		return errors.New("the recovering helper generation did not independently verify the namespace absent")
	}
	for label, name := range map[string]string{
		"container":   observation.identity.ContainerID,
		"cgroup":      observation.identity.CgroupID,
		"log segment": observation.identity.LogSegmentDirectory,
	} {
		if name == "" {
			return fmt.Errorf("deterministic %s name is empty", label)
		}
	}
	if !slices.Contains(observation.preInventory.Containers, observation.identity.ContainerID) {
		return fmt.Errorf("the pre-fault inventory did not contain the live marker container %s",
			observation.identity.ContainerID)
	}
	// The recovering generation must account for the pre-fault container: it
	// either swept it, or -- when the whole VM went away and came back with
	// the runtime already clean -- its independent verification shows it gone.
	// The second branch is only admissible alongside VerifiedAbsent above,
	// which a sweep that silently skipped the container could not satisfy.
	swept := slices.Contains(observation.sweptInventory.Containers, observation.identity.ContainerID)
	if !swept && slices.Contains(observation.postInventory.Containers, observation.identity.ContainerID) {
		return fmt.Errorf("the recovery neither swept nor verified the absence of the pre-fault container %s",
			observation.identity.ContainerID)
	}
	if observation.probeObservedAt.IsZero() {
		return errors.New("the functional probe never passed after recovery")
	}
	if observation.withdrawn.Revision == 0 || observation.reopened.Revision <= observation.withdrawn.Revision {
		return fmt.Errorf("local capability observations did not advance: withdrawn=%d reopened=%d",
			observation.withdrawn.Revision, observation.reopened.Revision)
	}
	// The barrier records a typed capability reason only as the outcome of an
	// Ensure, so the reason the withdrawal carries is only worth anything if
	// an Ensure was actually refused inside the fault window. A re-Ensure the
	// runtime accepted is the runtime not being restricted at all.
	if observation.withdrawalEnsureRefused == "" {
		return errors.New("the bounded re-Ensure inside the fault window was not refused, so no typed reason describes the restriction")
	}
	if !observation.withdrawn.ReasonCode.ValidOCIRestriction() {
		return fmt.Errorf("withdrawal carried reason code %q, which cannot explain an OCI restriction",
			observation.withdrawn.ReasonCode)
	}
	return nil
}

// unpreparedBarrierRefusals are the boot barrier's own words for "this
// session has no verified sweep behind it". Matching them keeps an unrelated
// failure -- a dial timeout, an image problem -- from satisfying the
// pre-sweep refusal.
var unpreparedBarrierRefusals = []string{
	"OCI boot barrier has not completed",
	"OCI boot barrier is unavailable",
	"OCI helper session is not configured",
}

func isUnpreparedBarrierRefusal(text string) bool {
	for _, phrase := range unpreparedBarrierRefusals {
		if strings.Contains(text, phrase) {
			return true
		}
	}
	return false
}

// sweepOrderingNote is this row's honesty line. The pre-sweep probe cannot
// race the sweep: BootBarrier.Ensure acquires the session and completes the
// verified sweep as one step, so between Invalidate and Ensure there is no
// session to probe with and the refusal is structural. That structure is the
// row's real content -- a probe cannot reach a helper generation whose sweep
// has not completed -- rather than a timing observation, and the assertions
// below check exactly that structure held rather than pretending to have
// observed a race.
const sweepOrderingNote = "sweep_before_recovery is a structural claim, not a timing race: Ensure's acquire and " +
	"verified sweep are one step, so the pre-sweep probe is refused by the unprepared barrier itself and the " +
	"probe can only succeed against a generation whose sweep already completed"

// checkSweepOrdering is sweep_before_recovery: nothing may probe positively,
// and no publication may succeed, before the verified sweep.
func checkSweepOrdering(observation lossObservation) error {
	if observation.preSweepProbeRefused == "" {
		return errors.New("the functional probe passed before the verified sweep")
	}
	if !isUnpreparedBarrierRefusal(observation.preSweepProbeRefused) {
		return fmt.Errorf("the pre-sweep probe failed for an unrelated reason rather than the unprepared barrier: %s",
			observation.preSweepProbeRefused)
	}
	if observation.preSweepBarrierPrepared {
		return errors.New("the barrier still held a verified sweep receipt when the pre-sweep probe was refused")
	}
	if !observation.verifiedAbsent {
		return errors.New("there was no verified sweep for the probe to follow")
	}
	if observation.oldTunnelRefused == "" {
		return errors.New("a pre-sweep publication was still reachable")
	}
	if observation.sweepObservedAt.IsZero() || observation.probeObservedAt.IsZero() {
		return errors.New("sweep and probe were not both observed")
	}
	if !observation.sweepObservedAt.Before(observation.probeObservedAt) {
		return fmt.Errorf("verified sweep at %s did not precede the probe at %s",
			observation.sweepObservedAt.Format(time.RFC3339Nano), observation.probeObservedAt.Format(time.RFC3339Nano))
	}
	return nil
}

type lossDependencies struct {
	barrier   attendedBarrier
	config    attendedConfig
	ledger    *capabilityLedger
	probe     func(context.Context) error
	injector  faultInjector
	kind      faultKind
	reuseBoot bool
	// settle bounds how long the driver waits for the injected loss to reach
	// the client, and for the returned helper to become dialable again.
	settle time.Duration
	// withdrawalEnsure bounds the re-Ensure taken inside the fault window to
	// obtain a typed capability reason. Zero means attendedWithdrawalEnsure.
	withdrawalEnsure time.Duration
}

// attendedWithdrawalEnsure is deliberately much shorter than settle: the
// re-Ensure inside the fault window exists to be refused, and the window is
// the owner's time.
const attendedWithdrawalEnsure = 60 * time.Second

func (dependencies lossDependencies) withdrawalBound() time.Duration {
	if dependencies.withdrawalEnsure > 0 {
		return dependencies.withdrawalEnsure
	}
	return attendedWithdrawalEnsure
}

func driveLossSequence(ctx context.Context, dependencies lossDependencies) (lossObservation, error) {
	observation := lossObservation{kind: dependencies.kind, bootSessionReused: dependencies.reuseBoot}
	config := dependencies.config
	session, err := dependencies.barrier.Session()
	if err != nil {
		return observation, fmt.Errorf("open the pre-fault session: %w", err)
	}
	authority := config.authority(contract.JobClassService, "loss-"+string(dependencies.kind))
	identity, err := ocihelper.DeterministicResourceIdentity(authority)
	if err != nil {
		return observation, err
	}
	observation.identity = identity

	// A live marker workload before the fault, as the runbook requires.
	response, err := session.Run(ctx, ocihelper.RunRequest{
		Authority: authority, InitialDeadman: boundedDeadman(session, config.Deadman),
		AllocateEndpoints: []string{"service"},
		Workload: ocihelper.WorkloadInput{
			ImageReference: config.Reference, ImageDigest: config.Digest,
			ManagedVolumes: []ocihelper.ManagedVolumeDescriptor{{Kind: ocihelper.ManagedVolumeServiceData}},
			Argv: []string{"/bin/sh", "-c",
				"exec /usr/local/bin/wefty-echo-service >/dev/null 2>&1 & printf 'attended-loss-marker\\n'; wait"},
		},
	})
	if err != nil {
		return observation, fmt.Errorf("run the pre-fault marker workload: %w", err)
	}
	if !response.Started || response.Endpoints["service"] == 0 {
		return observation, fmt.Errorf("pre-fault marker workload returned %+v, want a started service endpoint", response)
	}
	if _, err := exchangeAttemptMarkers(ctx, session, authority, "wefty-attended-loss-request"); err != nil {
		return observation, fmt.Errorf("prove the pre-fault workload live: %w", err)
	}
	pre, ok := dependencies.barrier.SweepReceipt()
	if !ok {
		return observation, errors.New("no pre-fault verified sweep receipt was available")
	}
	observation.preGeneration = pre.HelperSession
	// The pre-fault inventory has to be read while the marker workload is
	// live. The acquiring sweep receipt records an empty namespace, which
	// would prove nothing about what the recovery had to clean up.
	live, err := session.Verify(ctx, ocihelper.VerifyRequest{Scope: ocihelper.VerifyNamespaceReadOnly})
	if err != nil {
		return observation, fmt.Errorf("read the pre-fault namespace inventory: %w", err)
	}
	observation.preInventory = live.Inventory

	commands, err := dependencies.injector.inject(ctx)
	observation.commands = append(observation.commands, commands...)
	if err != nil {
		return observation, fmt.Errorf("inject the %s fault: %w", dependencies.kind, err)
	}

	// 1. the old control stream fails, 2. the old tunnel is unreachable and no
	// new claim is admitted.
	if _, dialErr := session.DialAttemptPort(ctx, ocihelper.DialAttemptPortRequest{
		Authority: authority, Name: "service",
	}); restrictiveFailure(dialErr) {
		observation.oldTunnelRefused = dialErr.Error()
	}
	claimAuthority := config.authority(contract.JobClassOneShot, "loss-"+string(dependencies.kind)+"-post-fault-claim")
	if _, claimErr := session.Run(ctx, ocihelper.RunRequest{
		Authority: claimAuthority, InitialDeadman: boundedDeadman(session, config.Deadman),
		Workload: ocihelper.WorkloadInput{
			ImageReference: config.Reference, ImageDigest: config.Digest, Argv: []string{"/bin/true"},
		},
	}); restrictiveFailure(claimErr) {
		observation.newClaimRefused = claimErr.Error()
	} else if claimErr == nil {
		reapAttempt(session, claimAuthority)
	}
	if health := awaitControlStreamFailure(ctx, session, dependencies.settle); health != nil {
		observation.controlStreamFailed = health.Error()
	}
	// BootBarrier.CapabilityReasonCode reflects the last Ensure outcome and
	// nothing else, so a loss observed through Run/Verify transport failures on
	// an already-acquired session leaves it holding the last healthy Ensure's
	// empty reason. The agent never sees that because its readiness timer
	// re-Ensures while the runtime is down and classifies the refusal. Do the
	// same thing here, bounded, and carry that refusal's typed reason into the
	// withdrawal rather than inventing one.
	ensureContext, cancelWithdrawal := context.WithTimeout(ctx, dependencies.withdrawalBound())
	if refusal := dependencies.barrier.Ensure(ensureContext); refusal != nil {
		observation.withdrawalEnsureRefused = refusal.Error()
	}
	cancelWithdrawal()
	observation.withdrawn = dependencies.ledger.withdraw(dependencies.barrier.CapabilityReasonCode())

	// 3. the helper/VM is returned, and a fresh acquire produces a new
	// generation; the probe must not pass until the sweep has.
	restored, err := dependencies.injector.restore(ctx)
	observation.commands = append(observation.commands, restored...)
	if err != nil {
		return observation, fmt.Errorf("restore after the %s fault: %w", dependencies.kind, err)
	}
	dependencies.barrier.Invalidate()
	_, observation.preSweepBarrierPrepared = dependencies.barrier.SweepReceipt()
	if probeErr := dependencies.probe(ctx); probeErr != nil {
		observation.preSweepProbeRefused = probeErr.Error()
	}
	ensureContext, cancelEnsure := context.WithTimeout(ctx, dependencies.settle)
	defer cancelEnsure()
	if err := dependencies.barrier.Ensure(ensureContext); err != nil {
		return observation, fmt.Errorf("re-acquire after the %s fault: %w", dependencies.kind, err)
	}
	post, ok := dependencies.barrier.SweepReceipt()
	if !ok {
		return observation, errors.New("no post-recovery verified sweep receipt was available")
	}
	observation.sweepObservedAt = config.now()
	observation.postGeneration = post.HelperSession
	observation.postInventory = post.VerifiedInventory
	observation.sweptInventory = post.SweptInventory
	observation.verifiedAbsent = post.VerifiedAbsent

	// 5. only then does the real probe pass and capability reopen.
	if err := dependencies.probe(ctx); err != nil {
		return observation, fmt.Errorf("functional probe after recovery: %w", err)
	}
	observation.probeObservedAt = config.now()
	observation.reopened = dependencies.ledger.reopen()
	return observation, nil
}

// restrictiveFailure reports whether err is the runtime refusing, rather than
// this driver's own context running out. A deadline or cancellation says
// nothing about whether the old tunnel was still reachable, so it must not be
// recorded as though it did.
func restrictiveFailure(err error) bool {
	if err == nil {
		return false
	}
	return !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled)
}

const restrictiveObservationNote = "the post-fault tunnel and claim entries are restrictive observations from a " +
	"lost session, not typed helper refusals: once the control stream is gone the helper answers nothing, so what " +
	"is recorded is the transport failure verbatim, with this driver's own context deadline and cancellation " +
	"excluded so they cannot masquerade as the runtime refusing"

func awaitControlStreamFailure(ctx context.Context, session attendedSession, bound time.Duration) error {
	if bound <= 0 {
		bound = 90 * time.Second
	}
	deadline := time.Now().Add(bound)
	for {
		if err := session.HealthError(); err != nil {
			return err
		}
		if time.Now().After(deadline) {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(500 * time.Millisecond):
		}
	}
}

func (config attendedConfig) now() time.Time {
	if config.Now != nil {
		return config.Now()
	}
	return time.Now()
}

// lossRow renders one observation into one receipt row. check is the
// row-specific assertion; narrative says what this row claims.
func lossRow(config attendedConfig, observation lossObservation, check func(lossObservation) error, narrative, notes string) attendedRow {
	row := newAttendedRow(config.SessionID, config.Command)
	row.AttemptIDs = []string{"attended-loss-" + string(observation.kind)}
	generations := []uint64{}
	if observation.preGeneration != (ocihelper.HelperSession{}) {
		generations = append(generations, observation.preGeneration.SessionGeneration)
	}
	if observation.postGeneration != (ocihelper.HelperSession{}) {
		generations = append(generations, observation.postGeneration.SessionGeneration)
	}
	row.HelperGenerations = generations
	revisions := []int64{}
	if observation.withdrawn.Revision != 0 {
		revisions = append(revisions, observation.withdrawn.Revision)
	}
	if observation.reopened.Revision != 0 {
		revisions = append(revisions, observation.reopened.Revision)
	}
	row.CapabilityRevisions = revisions
	inventories := []json.RawMessage{}
	for _, inventory := range []ocihelper.ResourceInventory{observation.preInventory, observation.postInventory} {
		encoded, err := json.Marshal(inventory)
		if err != nil {
			row.fail(fmt.Errorf("encode inventory: %w", err))
			return row
		}
		inventories = append(inventories, encoded)
	}
	row.Inventories = inventories
	row.appendReason(fmt.Sprintf("%s fault execution: %s", observation.kind, strings.Join(observation.commands, " | ")))
	row.appendReason(fmt.Sprintf(
		"pre-fault container swept by the recovering generation: %t; helper generation %s/%d -> %s/%d; "+
			"withdrawal reason %q from the in-window re-Ensure refusal %q; boot session id reused: %t",
		slices.Contains(observation.sweptInventory.Containers, observation.identity.ContainerID),
		observation.preGeneration.HelperInstanceID, observation.preGeneration.SessionGeneration,
		observation.postGeneration.HelperInstanceID, observation.postGeneration.SessionGeneration,
		observation.withdrawn.ReasonCode, observation.withdrawalEnsureRefused, observation.bootSessionReused))
	row.appendReason(capabilityReasonNote)
	row.appendReason(capabilityRevisionNote)
	row.appendReason(restrictiveObservationNote)
	if notes != "" {
		row.appendReason(notes)
	}
	if err := check(observation); err != nil {
		row.fail(err)
		return row
	}
	row.pass(narrative)
	return row
}
