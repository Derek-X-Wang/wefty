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
//	WEFTY_LIMA_INSTANCE=wefty-oci  (default wefty-oci)
//	WEFTY_ATTENDED_MANUAL_FAULTS=1 (loss rows, hand the faults to the operator)
//	WEFTY_ATTENDED_FAULT_ACK=...   (loss rows, manual acknowledgement file)
//
// Without WEFTY_OCI_HELPER_SOCKET both entrypoints skip, so the tagged build
// stays green anywhere but owner hardware.

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/runner/ocihelper"
)

const (
	// attendedRowDeadman is generous on purpose: nothing in these rows renews
	// an attempt deadman, and boundedDeadman clamps it to whatever ceiling the
	// helper advertises.
	attendedRowDeadman = 10 * time.Minute
	// attendedProbeDeadman stays modest because ocirunner.Adapter.Probe passes
	// it straight to Run without clamping.
	attendedProbeDeadman = 60 * time.Second
)

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
	instance := limaInstanceName()
	return attendedConfig{
		SessionID: sessionID,
		Command: []string{
			"go", "test", "-tags=service_acceptance", "-run", entrypoint, "-count=1", "-v", "./serviceacceptance",
		},
		NodeID:        sessionID,
		BootSessionID: sessionID + bootSuffix,
		Reference:     attendedEnvironment(t, "WEFTY_OCI_PROBE_REFERENCE"),
		Digest:        attendedEnvironment(t, "WEFTY_OCI_PROBE_DIGEST"),
		LimaInstance:  instance,
		Deadman:       attendedRowDeadman,
		ProbeDeadman:  attendedProbeDeadman,
		GuestPath:     limactlGuestPath(instance),
	}
}

func limaInstanceName() string {
	if instance := os.Getenv("WEFTY_LIMA_INSTANCE"); instance != "" {
		return instance
	}
	return "wefty-oci"
}

// limactlGuestPath reads a file from inside the Lima guest, so the mount row
// can prove the host-to-guest translation from the guest's own side.
func limactlGuestPath(instance string) func(context.Context, string) (string, error) {
	return func(ctx context.Context, guestPath string) (string, error) {
		command := exec.CommandContext(ctx, "limactl", "shell", "--workdir=/", instance, "cat", guestPath)
		output, err := command.Output()
		if err != nil {
			var exit *exec.ExitError
			if errors.As(err, &exit) {
				return "", fmt.Errorf("%w (%s)", err, strings.TrimSpace(string(exit.Stderr)))
			}
			return "", err
		}
		return string(output), nil
	}
}

// requireDaemonBootedOut is the runbook's own `pgrep -fl wefty-agent` check.
// A daemon still holding the session would make every row below fail in a
// confusing way, and injecting a VM loss underneath a live supervisor is
// exactly the race the third attended run hit.
func requireDaemonBootedOut(t *testing.T) {
	t.Helper()
	output, err := exec.Command("pgrep", "-fl", "wefty-agent").Output()
	if err == nil && len(bytes.TrimSpace(output)) != 0 {
		t.Fatalf("dev.wefty.agent is still running; boot it out before this window:\n%s", output)
	}
}

// openAttendedBarrier acquires the one exclusive helper session and imports
// the pinned probe archive into it, the same way the realtiming provisioner
// does. It returns an error rather than failing the test so a caller mid-row
// can still emit the evidence it has.
func openAttendedBarrier(t *testing.T, ctx context.Context, config attendedConfig) (*ocihelper.BootBarrier, error) {
	t.Helper()
	client := ocihelper.NewUnixClient(
		attendedEnvironment(t, "WEFTY_OCI_HELPER_SOCKET"), attendedEnvironment(t, "WEFTY_OCI_HELPER_CHECKSUM"))
	client.HeartbeatInterval = time.Second
	barrier, err := ocihelper.NewBootBarrier(client, ocihelper.AcquireSessionRequest{
		NodeID: config.NodeID, BootSessionID: config.BootSessionID,
	})
	if err != nil {
		return nil, err
	}
	if err := barrier.Ensure(ctx); err != nil {
		return nil, fmt.Errorf("acquire the exclusive helper session (is dev.wefty.agent booted out?): %w", err)
	}
	session, err := barrier.Session()
	if err != nil {
		return barrier, err
	}
	archive, err := os.Open(attendedEnvironment(t, "WEFTY_OCI_PROBE_ARCHIVE"))
	if err != nil {
		return barrier, err
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
		return barrier, fmt.Errorf("import the pinned probe archive: %w", err)
	}
	if imported.TopLevelDigest != config.Digest || imported.PlatformDigest == "" {
		return barrier, fmt.Errorf("probe import = %+v, want the pinned top-level digest and a platform digest", imported)
	}
	return barrier, nil
}

// emitAttendedRows always writes the fragment before deciding the test's own
// outcome. A window that ends red is still evidence, and losing it costs the
// owner the whole window.
func emitAttendedRows(t *testing.T, rows map[string]attendedRow) {
	t.Helper()
	path := attendedEnvironment(t, "WEFTY_ATTENDED_ROWS_OUT")
	writeErr := writeAttendedFragment(path, rows)
	failed := []string{}
	for name, row := range rows {
		t.Logf("%s: %s (exit %d) -- %s", name, row.Status, row.ExitCode, row.Reason)
		if row.Status != "PASS" {
			failed = append(failed, name)
		}
	}
	if writeErr != nil {
		t.Fatalf("write the attended row fragment to %s: %v", path, writeErr)
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
	requireDaemonBootedOut(t)
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

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Minute)
	defer cancel()
	barrier, err := openAttendedBarrier(t, ctx, config)
	if barrier != nil {
		defer barrier.Close()
	}
	if err != nil {
		t.Fatal(err)
	}
	session, err := barrier.Session()
	if err != nil {
		t.Fatal(err)
	}

	emitAttendedRows(t, map[string]attendedRow{
		"task_logs_delete":       driveTaskLogsDelete(ctx, session, config),
		"mount_validation":       driveMountValidation(ctx, session, config, fixtures),
		"host_to_guest":          driveHostToGuest(ctx, session, config),
		"guest_to_host_fallback": driveGuestToHostFallback(ctx, session, config),
	})
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
	requireDaemonBootedOut(t)
	ctx, cancel := context.WithTimeout(t.Context(), 45*time.Minute)
	defer cancel()
	ledger := newCapabilityLedger(nil)
	rows := map[string]attendedRow{}

	helperConfig := attendedBaseConfig(t, "TestAttendedHelperLossRows", "-loss")
	helperObservation, helperErr := runAttendedLoss(t, ctx, helperConfig, ledger, faultHelperLoss, true)
	rows["helper_loss"] = lossRow(helperConfig, helperObservation, checkLossTransition,
		"old control stream failed and the old tunnel and any new claim were refused; the returned helper answered a "+
			"fresh AcquireSession with a new instance/session generation, swept the pre-fault resources and "+
			"independently verified the namespace absent; only then did the real probe pass",
		executionNote(helperErr))
	rows["sweep_before_recovery"] = lossRow(helperConfig, helperObservation, checkSweepOrdering,
		"same helper-loss fault execution as helper_loss: the probe was refused by the unprepared barrier before the "+
			"verified sweep and passed only after it, and no publication succeeded pre-sweep",
		strings.TrimSpace(sweepOrderingNote+"; "+executionNote(helperErr)))

	// The VM execution runs even when the helper one failed: it opens its own
	// barrier under a fresh boot session ID and will fail fast if the node is
	// still unwell, and an honest vm_loss row is worth more than a missing one.
	vmConfig := attendedBaseConfig(t, "TestAttendedHelperLossRows", "-loss-vm")
	vmObservation, vmErr := runAttendedLoss(t, ctx, vmConfig, ledger, faultVMLoss, false)
	rows["vm_loss"] = lossRow(vmConfig, vmObservation, checkLossTransition,
		"full VM stop with a fresh textual boot session ID: same ordered recovery, proven against a new helper "+
			"instance rather than a restarted one",
		executionNote(vmErr))

	emitAttendedRows(t, rows)
}

func executionNote(err error) string {
	if err == nil {
		return ""
	}
	return "the fault execution did not complete: " + err.Error()
}

func runAttendedLoss(t *testing.T, ctx context.Context, config attendedConfig, ledger *capabilityLedger, kind faultKind, reuseBoot bool) (lossObservation, error) {
	t.Helper()
	injector := attendedFaultInjector(config.LimaInstance, kind)
	// The window must never be left with the helper or the VM down, including
	// on a panic, so the restore is registered before the fault can happen.
	t.Cleanup(func() {
		restoreContext, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()
		if performed, err := injector.restore(restoreContext); err != nil {
			t.Logf("best-effort %s restore (%v): %v", kind, performed, err)
		}
	})
	barrier, err := openAttendedBarrier(t, ctx, config)
	if barrier != nil {
		defer barrier.Close()
	}
	if err != nil {
		return lossObservation{kind: kind, bootSessionReused: reuseBoot}, err
	}
	return driveLossSequence(ctx, lossDependencies{
		barrier: newLiveBarrier(barrier), config: config, ledger: ledger,
		probe: newLiveProbe(barrier, config), injector: injector, kind: kind,
		reuseBoot: reuseBoot, settle: 5 * time.Minute,
	})
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
