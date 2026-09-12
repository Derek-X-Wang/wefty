//go:build service_acceptance && darwin

package serviceacceptance

// Owner-facing entrypoints for the seven exclusive-session Lima rows (#410).
//
// Both windows run with dev.wefty.agent booted out, exactly as
// docs/acceptance/m3-lima-transport.md instructs, because the helper admits
// one session at a time. Each entrypoint writes its rows to
// WEFTY_ATTENDED_ROWS_OUT in the receipt's row shape; the owner merges the
// fragments into the attended artifact.
//
//	WEFTY_OCI_HELPER_SOCKET=...    WEFTY_OCI_HELPER_CHECKSUM=...
//	WEFTY_OCI_PROBE_REFERENCE=...  WEFTY_OCI_PROBE_DIGEST=...
//	WEFTY_OCI_PROBE_ARCHIVE=...    WEFTY_ATTENDED_SESSION_ID=attended-...
//	WEFTY_ATTENDED_ROWS_OUT=/abs/path.json
//	WEFTY_ATTENDED_MOUNT_ROOT=...  (transport rows only)
//	WEFTY_LIMA_INSTANCE=wefty-oci  (loss rows, default wefty-oci)
//	WEFTY_ATTENDED_MANUAL_FAULTS=1 (loss rows, hand the faults to the operator)
//	WEFTY_ATTENDED_FAULT_ACK=...   (loss rows, manual acknowledgement file)
//
// Without WEFTY_OCI_HELPER_SOCKET both entrypoints skip, so the tagged build
// stays green anywhere but owner hardware.

import (
	"context"
	"os"
	"runtime"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/runner/ocihelper"
)

const attendedProbeDeadman = 60 * time.Second

func attendedEnvironment(t *testing.T, name string) string {
	t.Helper()
	value := os.Getenv(name)
	if value == "" {
		t.Fatalf("the attended exclusive-session rows require %s", name)
	}
	return value
}

func attendedBaseConfig(t *testing.T, entrypoint, bootSuffix string) attendedConfig {
	t.Helper()
	sessionID := attendedEnvironment(t, "WEFTY_ATTENDED_SESSION_ID")
	return attendedConfig{
		SessionID: sessionID,
		Command: []string{
			"go", "test", "-tags=service_acceptance", "-run", entrypoint, "-count=1", "-v", "./serviceacceptance",
		},
		NodeID:        sessionID,
		BootSessionID: sessionID + bootSuffix,
		Reference:     attendedEnvironment(t, "WEFTY_OCI_PROBE_REFERENCE"),
		Digest:        attendedEnvironment(t, "WEFTY_OCI_PROBE_DIGEST"),
		LimaInstance:  limaInstanceName(),
		Deadman:       attendedProbeDeadman,
	}
}

func limaInstanceName() string {
	if instance := os.Getenv("WEFTY_LIMA_INSTANCE"); instance != "" {
		return instance
	}
	return "wefty-oci"
}

// openAttendedBarrier acquires the one exclusive helper session and imports
// the pinned probe archive into it, the same way the realtiming provisioner
// does.
func openAttendedBarrier(t *testing.T, ctx context.Context, config attendedConfig) *ocihelper.BootBarrier {
	t.Helper()
	client := ocihelper.NewUnixClient(
		attendedEnvironment(t, "WEFTY_OCI_HELPER_SOCKET"), attendedEnvironment(t, "WEFTY_OCI_HELPER_CHECKSUM"))
	client.HeartbeatInterval = time.Second
	barrier, err := ocihelper.NewBootBarrier(client, ocihelper.AcquireSessionRequest{
		NodeID: config.NodeID, BootSessionID: config.BootSessionID,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := barrier.Ensure(ctx); err != nil {
		t.Fatalf("acquire the exclusive helper session (is dev.wefty.agent booted out?): %v", err)
	}
	session, err := barrier.Session()
	if err != nil {
		t.Fatal(err)
	}
	archive, err := os.Open(attendedEnvironment(t, "WEFTY_OCI_PROBE_ARCHIVE"))
	if err != nil {
		t.Fatal(err)
	}
	defer archive.Close()
	var imported ocihelper.EnsureImageResponse
	if err := session.ImportImage(ctx, ocihelper.EnsureImageRequest{
		Reference: config.Reference, Digest: config.Digest,
		Platform:         ocihelper.OCIPlatform{OS: "linux", Architecture: runtime.GOARCH},
		OperationTimeout: 2 * time.Minute,
	}, archive, func(event ocihelper.EnsureImageEvent) error {
		if event.Result != nil {
			imported = *event.Result
		}
		return nil
	}); err != nil {
		t.Fatalf("import the pinned probe archive: %v", err)
	}
	if imported.TopLevelDigest != config.Digest || imported.PlatformDigest == "" {
		t.Fatalf("probe import = %+v, want the pinned top-level digest and a platform digest", imported)
	}
	return barrier
}

func emitAttendedRows(t *testing.T, rows map[string]attendedRow) {
	t.Helper()
	path := attendedEnvironment(t, "WEFTY_ATTENDED_ROWS_OUT")
	if err := writeAttendedFragment(path, rows); err != nil {
		t.Fatal(err)
	}
	failed := []string{}
	for name, row := range rows {
		t.Logf("%s: %s (exit %d) -- %s", name, row.Status, row.ExitCode, row.Reason)
		if row.Status != "PASS" {
			failed = append(failed, name)
		}
	}
	t.Logf("wrote %d attended rows to %s", len(rows), path)
	if len(failed) != 0 {
		t.Fatalf("rows did not reach PASS: %v", failed)
	}
}

// TestAttendedHelperTransportRows drives items 2, 3, 4 and the host-bridge
// fallback of the runbook's runtime matrix in one exclusive helper session.
func TestAttendedHelperTransportRows(t *testing.T) {
	if os.Getenv("WEFTY_OCI_HELPER_SOCKET") == "" {
		t.Skip("set WEFTY_OCI_HELPER_SOCKET to drive the attended exclusive-session transport rows")
	}
	config := attendedBaseConfig(t, "TestAttendedHelperTransportRows", "-transport")
	config.MountRoot = attendedEnvironment(t, "WEFTY_ATTENDED_MOUNT_ROOT")
	outsideRoot := os.Getenv("WEFTY_ATTENDED_OUTSIDE_ROOT")
	if outsideRoot == "" {
		outsideRoot = os.TempDir()
	}
	fixtures, err := prepareMountFixtures(config.MountRoot, outsideRoot)
	if err != nil {
		t.Fatal(err)
	}
	defer fixtures.Close()

	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Minute)
	defer cancel()
	barrier := openAttendedBarrier(t, ctx, config)
	defer barrier.Close()
	session, err := barrier.Session()
	if err != nil {
		t.Fatal(err)
	}

	rows := map[string]attendedRow{
		"task_logs_delete":       driveTaskLogsDelete(ctx, session, config),
		"mount_validation":       driveMountValidation(ctx, session, config, fixtures),
		"host_to_guest":          driveHostToGuest(ctx, session, config),
		"guest_to_host_fallback": driveGuestToHostFallback(ctx, session, config),
	}
	emitAttendedRows(t, rows)
}

// TestAttendedHelperLossRows drives the runbook's loss-and-recovery order.
// One helper-loss execution produces helper_loss and sweep_before_recovery;
// one VM-loss execution produces vm_loss. The helper execution reuses the
// textual boot session ID across the fault, as the runbook asks for one
// repetition; the VM execution uses a fresh one.
func TestAttendedHelperLossRows(t *testing.T) {
	if os.Getenv("WEFTY_OCI_HELPER_SOCKET") == "" {
		t.Skip("set WEFTY_OCI_HELPER_SOCKET to drive the attended exclusive-session loss rows")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 45*time.Minute)
	defer cancel()
	ledger := newCapabilityLedger(nil)
	rows := map[string]attendedRow{}

	helperConfig := attendedBaseConfig(t, "TestAttendedHelperLossRows", "-loss")
	helperObservation := runAttendedLoss(t, ctx, helperConfig, ledger, faultHelperLoss, true)
	rows["helper_loss"] = lossRow(helperConfig, helperObservation, checkLossTransition,
		"old control stream failed and the old tunnel and any new claim were refused; the returned helper answered a "+
			"fresh AcquireSession with a new instance/session generation, swept the pre-fault resources and "+
			"independently verified the namespace absent; only then did the real probe pass")
	rows["sweep_before_recovery"] = lossRow(helperConfig, helperObservation, checkSweepOrdering,
		"same helper-loss fault execution as helper_loss: the functional probe was refused before the verified sweep "+
			"and passed only after it, and no publication succeeded pre-sweep")

	vmConfig := attendedBaseConfig(t, "TestAttendedHelperLossRows", "-loss-vm")
	vmObservation := runAttendedLoss(t, ctx, vmConfig, ledger, faultVMLoss, false)
	rows["vm_loss"] = lossRow(vmConfig, vmObservation, checkLossTransition,
		"full VM stop with a fresh textual boot session ID: same ordered recovery, proven against a new helper "+
			"instance rather than a restarted one")

	emitAttendedRows(t, rows)
}

func runAttendedLoss(t *testing.T, ctx context.Context, config attendedConfig, ledger *capabilityLedger, kind faultKind, reuseBoot bool) lossObservation {
	t.Helper()
	barrier := openAttendedBarrier(t, ctx, config)
	defer barrier.Close()
	injector := attendedFaultInjector(config.LimaInstance, kind)
	observation, err := driveLossSequence(ctx, lossDependencies{
		barrier: newLiveBarrier(barrier), config: config, ledger: ledger,
		probe: newLiveProbe(barrier, config), injector: injector, kind: kind,
		reuseBoot: reuseBoot, settle: 5 * time.Minute,
	})
	if err != nil {
		// The window must not be left with the helper or the VM down.
		if _, restoreErr := injector.restore(context.Background()); restoreErr != nil {
			t.Logf("restore after a failed %s loss sequence: %v", kind, restoreErr)
		}
		t.Fatalf("%s loss sequence: %v", kind, err)
	}
	return observation
}

func attendedFaultInjector(instance string, kind faultKind) faultInjector {
	if os.Getenv("WEFTY_ATTENDED_MANUAL_FAULTS") == "" {
		return newLimactlFaultInjector(instance, kind)
	}
	ack := os.Getenv("WEFTY_ATTENDED_FAULT_ACK")
	if ack == "" {
		ack = "/tmp/wefty-attended-fault-ack"
	}
	return newManualFaultInjector(instance, kind, os.Stdout, ack)
}
