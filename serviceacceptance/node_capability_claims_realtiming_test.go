//go:build service_acceptance_realtiming && (darwin || linux)

package serviceacceptance

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/agent"
	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/l1"
	"github.com/Derek-X-Wang/wefty/runner/lima"
	"github.com/Derek-X-Wang/wefty/runner/ocicontrol"
	"github.com/Derek-X-Wang/wefty/runner/ocihelper"
)

// linuxRootFaultBound is the window the lane gives the root fault supervisor to
// acknowledge one action.
const linuxRootFaultBound = 90 * time.Second

// requestLinuxRootFault writes one action to the lane's root fault FIFO and
// waits for the supervisor's acknowledgement file. The context is the caller's
// on purpose: a restoration that has to survive its own test cannot be bounded
// by t.Context(), which Go cancels before cleanup functions run -- such a
// restoration could never even start its FIFO write, and the helper units would
// stay down for every later test in this sequential lane.
func requestLinuxRootFault(ctx context.Context, fifo, directory, action string) error {
	done := filepath.Join(directory, action+".done")
	failure := filepath.Join(directory, action+".failed")
	_ = os.Remove(done)
	_ = os.Remove(failure)
	command := exec.CommandContext(ctx, "sh", "-c", `printf '%s\n' "$1" > "$2"`, "wefty-fault", action, fifo)
	if output, err := command.CombinedOutput(); err != nil {
		return fmt.Errorf("trigger fault %s: %w\n%s", action, err, output)
	}
	for ctx.Err() == nil {
		if _, err := os.Stat(done); err == nil {
			return nil
		}
		if payload, err := os.ReadFile(failure); err == nil {
			return fmt.Errorf("root assertion %s failed: %s", action, payload)
		}
		time.Sleep(100 * time.Millisecond)
	}
	return fmt.Errorf("fault %s did not complete", action)
}

// claimPairFaultSupervisor is the lane's root fault channel, resolved once in
// the test body so the restoration cleanup never has to look up an environment
// variable (and so never has to report a missing one through a t.Fatalf that
// would abort the harness's own teardown).
type claimPairFaultSupervisor struct{ fifo, directory string }

// run bounds every action independently of the test context, so the same call
// works from the test body and from a cleanup.
func (supervisor claimPairFaultSupervisor) run(action string) error {
	ctx, cancel := context.WithTimeout(context.Background(), linuxRootFaultBound)
	defer cancel()
	return requestLinuxRootFault(ctx, supervisor.fifo, supervisor.directory, action)
}

// ociClaimPairSettleBudget bounds one claim transition. A withdrawal costs the
// heartbeat that reaches barrier recovery, the bounded takeover-and-verify
// window that recovery spends proving the unit is gone, and the heartbeat that
// publishes the restrictive observation. Re-earning costs the same three steps
// with a successful sweep in the middle. Three of each leaves margin on a
// shared runner without inventing a timing constant: the lane keeps its own
// production defaults as the only source of truth.
var ociClaimPairSettleBudget = 3 * (agent.DefaultHeartbeatInterval +
	ocihelper.VerifiedReadyTimeoutForReap(ocihelper.DefaultReapTimeout))

// ociClaimPairPollInterval is the gap between `wefty node doctor` invocations.
// Each poll spawns the real CLI, so a sub-second cadence would measure process
// startup rather than the node's claim.
const ociClaimPairPollInterval = 2 * time.Second

// TestOCINodeCapabilityClaimPairAtProductionTimings proves the capable and
// incapable halves of the kind:oci claim through the operator's own diagnostic
// -- the real `wefty --json node doctor` binary against a real spawned agent --
// rather than through a doctor report a test assembled for itself.
//
// The pair is the point. A node that only ever advertises kind:oci proves
// nothing: the claim has to be withdrawn when the socket-activated helper unit
// is gone, named with the reason an operator can act on
// (helper_unit_unavailable), and then re-earned at a strictly higher capability
// revision so L1 can tell a recovered node from a stale republication of the
// revision it already had. kind:process must survive the whole episode --
// losing the OCI helper is not a node blackout.
//
// This fills the linux.node.capability_claims row of the #157 acceptance
// matrix, which until now carried no live fact at all.
func TestOCINodeCapabilityClaimPairAtProductionTimings(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("the capable/incapable kind:oci claim pair needs the Linux socket-activated helper units")
	}
	assertProductionTimingDefaults(t)
	evidence := newRealTimingEvidence(t)

	helperSocket := requiredClaimPairEnvironment(t, "WEFTY_OCI_HELPER_SOCKET")
	helperChecksum := requiredClaimPairEnvironment(t, "WEFTY_OCI_HELPER_CHECKSUM")
	probeReference := requiredClaimPairEnvironment(t, "WEFTY_OCI_PROBE_REFERENCE")
	probeDigest := requiredClaimPairEnvironment(t, "WEFTY_OCI_PROBE_DIGEST")
	probeArchive := requiredClaimPairEnvironment(t, "WEFTY_OCI_PROBE_ARCHIVE")
	supervisor := claimPairFaultSupervisor{
		fifo:      requiredClaimPairEnvironment(t, "WEFTY_OCI_FAULT_FIFO"),
		directory: requiredClaimPairEnvironment(t, "WEFTY_OCI_FAULT_DIR"),
	}
	importRealtimeProbeImage(t, probeArchive, helperSocket, helperChecksum, probeReference, probeDigest, nil)

	intentPath := filepath.Join(t.TempDir(), "oci-intent.json")
	if _, err := lima.InitializeOCIIntent(intentPath, time.Now()); err != nil {
		t.Fatal(err)
	}
	// The control socket lives in its own short directory: the server stages the
	// socket under a sibling directory before renaming it into place, and a
	// per-test temporary directory named after this test would spend the Unix
	// path budget on the test name.
	socketRoot, err := os.MkdirTemp("", "wefty-claim-pair-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(socketRoot) })
	controlSocket := filepath.Join(socketRoot, "node.sock")
	// The installed node configuration is the only thing that points the CLI at
	// this agent. Writing it with the shipped writer keeps the CLI on its real
	// discovery path instead of a test-only flag.
	nodeConfig := filepath.Join(socketRoot, "node.json")
	if err := ocicontrol.WriteInstalledConfig(nodeConfig, ocicontrol.InstalledConfig{
		Version: ocicontrol.InstalledConfigVersion, ControlSocket: controlSocket,
	}); err != nil {
		t.Fatal(err)
	}

	harness := newAcceptanceHarnessWithOptions(t, acceptanceHarnessOptions{
		leaseDuration:     l1.DefaultLeaseDuration,
		productionTimings: true,
		agentArguments: []string{
			"--oci-control-socket=" + controlSocket,
			"--oci-helper-socket=" + helperSocket,
			"--oci-helper-checksum=" + helperChecksum,
			"--oci-probe-image=" + probeReference,
			"--oci-probe-digest=" + probeDigest,
			"--oci-intent-file=" + intentPath,
		},
	})
	t.Cleanup(func() {
		// L1-side evidence matters as much as the agent's: a claim that never
		// reached the control plane looks identical from the agent log alone.
		evidence.recordProcessOutput("capability-claims-control-plane.log", harness.controlPlane)
		for index, process := range harness.agents {
			evidence.recordProcessOutput(fmt.Sprintf("capability-claims-agent-%02d.log", index+1), process)
		}
	})

	// Arm the restoration before anything can stop the units, and after the
	// harness exists so LIFO cleanup brings the helper back before the agent and
	// control plane are torn down. start-helper-topology is idempotent -- the
	// supervisor starts both units and asserts them active -- so it is a no-op
	// assertion if nothing was stopped and exactly the repair needed if a stop
	// was applied but never acknowledged.
	helperTopologyRestored := false
	t.Cleanup(func() {
		if helperTopologyRestored {
			return
		}
		if err := supervisor.run("start-helper-topology"); err != nil {
			// Errorf, not Fatalf: a Goexit here would skip the harness's own
			// process teardown, and a lane left with both a down helper and a
			// stray agent is strictly worse than a failed test.
			t.Errorf("restore the helper topology: %v", err)
		}
	})

	capable := waitForNodeDoctorClaim(t, nodeConfig, "kind:oci capable with no reason code",
		func(claim nodeDoctorClaim) bool { return claim.capable && claim.reasonCode == "" })
	evidence.write("node-doctor-capable.json", capable.payload)
	assertProcessClaimIntact(t, "capable", capable)

	// The fault class is the lane's existing root-supervised one: an
	// unprivileged test step cannot drive systemctl, so it asks the root fault
	// supervisor to stop both helper units and waits for the acknowledgement.
	if err := supervisor.run("stop-helper-topology"); err != nil {
		t.Fatal(err)
	}

	incapable := waitForNodeDoctorClaim(t, nodeConfig,
		"kind:oci withdrawn with "+string(contract.CapabilityReasonHelperUnitUnavailable),
		func(claim nodeDoctorClaim) bool {
			return !claim.capable && claim.reasonCode == contract.CapabilityReasonHelperUnitUnavailable
		})
	evidence.write("node-doctor-incapable.json", incapable.payload)
	assertProcessClaimIntact(t, "incapable", incapable)
	if !slices.Contains(incapable.missing, "kind:oci") {
		t.Fatalf("withdrawn claim missing_capabilities = %v, want kind:oci named", incapable.missing)
	}
	if incapable.revision <= capable.revision {
		t.Fatalf("withdrawal revision %d did not advance past the capable revision %d",
			incapable.revision, capable.revision)
	}

	if err := supervisor.run("start-helper-topology"); err != nil {
		t.Fatal(err)
	}
	helperTopologyRestored = true

	reearned := waitForNodeDoctorClaim(t, nodeConfig,
		fmt.Sprintf("kind:oci re-earned above capability revision %d", incapable.revision),
		func(claim nodeDoctorClaim) bool {
			return claim.capable && claim.reasonCode == "" && claim.revision > incapable.revision
		})
	evidence.write("node-doctor-reearned.json", reearned.payload)
	assertProcessClaimIntact(t, "re-earned", reearned)
	// Re-earned, not merely republished: an agent that reprinted the revision it
	// already held would leave L1 unable to order the recovery after the loss.
	if reearned.revision <= capable.revision {
		t.Fatalf("re-earned capability revision %d is not above the first capable revision %d",
			reearned.revision, capable.revision)
	}

	evidence.write("oci-node-capability-claims-linux.txt", []byte(fmt.Sprintf(
		"capability_claim_pair_oci_capable=true\ncapability_claim_pair_oci_incapable=true\n"+
			"capability_claim_pair_doctor_source=cli\ncapability_revision_before=%d\ncapability_revision_after=%d\n",
		capable.revision, reearned.revision)))
}

// nodeDoctorClaim is the claim half of one real `wefty node doctor` reading,
// kept alongside the raw JSON so the lane can publish the operator's own bytes
// as evidence rather than a re-encoding of them.
type nodeDoctorClaim struct {
	capable    bool
	process    bool
	reasonCode contract.CapabilityReasonCode
	revision   int64
	pending    int64
	missing    []string
	payload    []byte
}

// settled is the difference between a revision the node holds and a revision L1
// has acknowledged. A non-zero pending publication is the agent's own statement
// that its local observation has not reached the control plane yet, and the
// doctor labels such a report oci_capability_revision_pending. Accepting one
// would let this receipt claim an L1-orderable pair while its own published JSON
// says the revision was still in flight.
func (claim nodeDoctorClaim) settled() bool { return claim.pending == 0 }

func requiredClaimPairEnvironment(t *testing.T, name string) string {
	t.Helper()
	value := os.Getenv(name)
	if value == "" {
		t.Fatalf("the Linux kind:oci claim-pair acceptance requires %s", name)
	}
	return value
}

// readNodeDoctorClaim runs the shipped CLI exactly as an operator would. Errors
// are returned rather than fatal: the agent's control socket appears only once
// the agent is serving, and a doctor run that raced a helper-unit transition is
// a poll to retry, not a verdict.
func readNodeDoctorClaim(ctx context.Context, nodeConfig string) (nodeDoctorClaim, error) {
	command := exec.CommandContext(ctx, weftyBinaryPath, "--json", "--node-config="+nodeConfig, "node", "doctor")
	var stderr bytes.Buffer
	command.Stderr = &stderr
	payload, err := command.Output()
	if err != nil {
		return nodeDoctorClaim{}, fmt.Errorf("run wefty node doctor: %w\n%s", err, stderr.Bytes())
	}
	var report ocicontrol.DoctorResponse
	if err := json.Unmarshal(payload, &report); err != nil {
		return nodeDoctorClaim{}, fmt.Errorf("decode wefty node doctor JSON: %w\n%s", err, payload)
	}
	// Validating here is not ceremony: it is what proves the bytes an operator
	// reads are a complete doctor report and not a partial one this test happened
	// to find the two fields it wanted in.
	if err := report.Validate(); err != nil {
		return nodeDoctorClaim{}, fmt.Errorf("wefty node doctor emitted an invalid report: %w", err)
	}
	return nodeDoctorClaim{
		capable:    report.Probe.Capabilities["kind:oci"],
		process:    report.Probe.Capabilities["kind:process"],
		reasonCode: report.Probe.CapabilityReasonCode,
		revision:   report.Probe.CapabilityRevision,
		pending:    report.Probe.PendingPublicationRevision,
		missing:    report.Probe.MissingCapabilities,
		payload:    payload,
	}, nil
}

func waitForNodeDoctorClaim(t *testing.T, nodeConfig, want string, satisfied func(nodeDoctorClaim) bool) nodeDoctorClaim {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), ociClaimPairSettleBudget)
	defer cancel()
	var last nodeDoctorClaim
	var lastErr error
	for {
		claim, err := readNodeDoctorClaim(ctx, nodeConfig)
		if err == nil {
			last, lastErr = claim, nil
			if claim.settled() && satisfied(claim) {
				return claim
			}
		} else if ctx.Err() == nil {
			lastErr = err
		}
		if ctx.Err() != nil {
			t.Fatalf("wefty node doctor never reported %s within %s: capable=%t kind:process=%t reason=%q revision=%d pending=%d missing=%v last error: %v",
				want, ociClaimPairSettleBudget, last.capable, last.process, last.reasonCode,
				last.revision, last.pending, last.missing, lastErr)
		}
		select {
		case <-ctx.Done():
		case <-time.After(ociClaimPairPollInterval):
		}
	}
}

// assertProcessClaimIntact keeps the claim a pair rather than a switch. Losing
// the OCI helper withdraws kind:oci and nothing else; a node that dropped
// kind:process with it would have stopped being a node.
func assertProcessClaimIntact(t *testing.T, phase string, claim nodeDoctorClaim) {
	t.Helper()
	if !claim.process {
		t.Fatalf("%s claim withdrew kind:process as well: reason=%q missing=%v", phase, claim.reasonCode, claim.missing)
	}
}
