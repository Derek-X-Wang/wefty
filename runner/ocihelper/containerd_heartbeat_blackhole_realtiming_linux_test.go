//go:build service_acceptance_realtiming && linux

package ocihelper_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	ocirunner "github.com/Derek-X-Wang/wefty/runner/oci"
	"github.com/Derek-X-Wang/wefty/runner/ocihelper"
)

// A heartbeat blackhole is the one crash-recovery failure a closed connection
// cannot stand in for. The agent is still running, its control connection is
// still open, and nothing but the helper's own heartbeat deadline can decide
// the session is gone. Until now that was proven only against the fake engine
// (TestExclusiveSessionEOFAndHeartbeatBlackholeFailClosed and
// TestServiceAcceptanceHelperAuthorityFailsClosed), so nothing showed the real
// socket-activated helper tearing a real containerd attempt down on that
// deadline. This is that proof (#456, the second Linux hole in #402).
//
// The reap is attributed rather than merely observed, because a reap on its own
// would not say what caused it: the container is positively present before the
// blackhole and positively absent after, exactly one helper session close
// appears in the window and its reason is one only a silent client can produce,
// and the elapsed time sits above the helper's published heartbeat deadline and
// far below the attempt deadman.
func TestNativeLinuxHeartbeatBlackholeReapsLiveOCIAttempt(t *testing.T) {
	helperSocket := os.Getenv("WEFTY_OCI_HELPER_SOCKET")
	helperChecksum := os.Getenv("WEFTY_OCI_HELPER_CHECKSUM")
	containerdAddress := os.Getenv("WEFTY_OCI_CONTAINERD_ADDRESS")
	reference := os.Getenv("WEFTY_OCI_ECHO_REFERENCE")
	archivePath := os.Getenv("WEFTY_OCI_ECHO_ARCHIVE")
	if helperSocket == "" || helperChecksum == "" || containerdAddress == "" || reference == "" || archivePath == "" {
		t.Fatal("Linux OCI heartbeat blackhole provisioning is incomplete")
	}
	const helperStderrPath = "/tmp/wefty-oci-helper-realtiming.stderr"
	// The lane's own heartbeat cadence, shared with every other live test here.
	const heartbeatInterval = time.Second
	client := ocihelper.NewUnixClient(helperSocket, helperChecksum)
	client.HeartbeatInterval = heartbeatInterval
	barrier, err := ocihelper.NewBootBarrier(client, ocihelper.AcquireSessionRequest{
		NodeID: "native-blackhole-node", BootSessionID: "native-blackhole-boot",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer barrier.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Minute)
	defer cancel()
	if err := barrier.Ensure(ctx); err != nil {
		t.Fatal(err)
	}
	adapter := ocirunner.NewAdapter(barrier)
	image := loadNativeImageArchive(t, ctx, adapter, reference, archivePath)
	session, err := barrier.Session()
	if err != nil {
		t.Fatal(err)
	}

	// Every bound here is read back from the helper this lane actually runs.
	// The unit at .github/workflows/service-acceptance-realtiming.yml passes no
	// heartbeat flag, so the deadline under test is whatever the helper
	// compiled and published in its handshake, never a test-chosen value.
	handshake := session.Handshake()
	heartbeatTimeout := handshake.HeartbeatTimeout
	if heartbeatTimeout <= heartbeatInterval {
		t.Fatalf("helper published heartbeat timeout = %s, which leaves no window between the lane's %s cadence and the deadline",
			heartbeatTimeout, heartbeatInterval)
	}
	// The deadman is twenty heartbeat timeouts, clamped to the helper's own
	// ceiling. The attempt therefore cannot expire on its own inside the
	// blackhole window, so a reap there can only be the session deadline.
	deadman := 20 * heartbeatTimeout
	if deadman > handshake.MaximumAttemptDeadman {
		deadman = handshake.MaximumAttemptDeadman
	}
	if deadman <= heartbeatTimeout {
		t.Fatalf("attempt deadman %s does not outlive the heartbeat timeout %s", deadman, heartbeatTimeout)
	}
	reapBound := heartbeatTimeout + ocihelper.VerifiedReadyTimeoutForReap(handshake.ReapTimeout)
	if reapBound >= deadman {
		t.Fatalf("reap bound %s does not separate the session deadline from the attempt deadman %s", reapBound, deadman)
	}

	authority := ocihelper.AttemptAuthority{
		NodeID: "native-blackhole-node", BootSessionID: "native-blackhole-boot",
		JobID: "heartbeat-blackhole-job", AttemptID: "heartbeat-blackhole-attempt",
		FencingToken: "heartbeat-blackhole-fence", Class: contract.JobClassService,
		RemovalGeneration: "attempt",
	}
	identity, err := ocihelper.DeterministicResourceIdentity(authority)
	if err != nil {
		t.Fatal(err)
	}
	// The helper arms the deadman when it reserves the attempt, before Run
	// returns, so the clock the deadman runs on starts here and not at the
	// blackhole. Everything between here and the acknowledged suppression eats
	// into it.
	attemptStarted := time.Now()
	if _, err := session.Run(ctx, ocihelper.RunRequest{
		Authority: authority, InitialDeadman: deadman,
		Workload: ocihelper.WorkloadInput{
			ImageReference: reference, ImageDigest: image.TopLevelDigest,
			Argv: []string{"/bin/sh", "-c", "while true; do sleep 1; done"},
		},
	}); err != nil {
		t.Fatal(err)
	}
	present, err := nativeOCIContainerIDs(ctx, containerdAddress)
	if err != nil {
		t.Fatalf("list live OCI containers: %v", err)
	}
	if !slices.Contains(present, identity.ContainerID) {
		t.Fatalf("kind=oci attempt container %s never reached containerd", identity.ContainerID)
	}
	closesBefore := countHelperSessionCloses(t, helperStderrPath)

	// The attempt's own terminal observation. It ends when the helper tears the
	// session down, and runner/oci turns exactly this typed loss into the
	// attempt's runtime_failure (adapter.go classifies *RuntimeLossError).
	watchCtx, cancelWatch := context.WithCancel(context.Background())
	defer cancelWatch()
	watchDone := make(chan error, 1)
	go func() {
		watchDone <- session.Watch(watchCtx, ocihelper.WatchRequest{Authority: authority}, nil)
	}()

	// Go quiet without hanging up. Suppression returns only once the pump has
	// acknowledged it from its own goroutine, so from this instant no heartbeat
	// is in flight and none will start: the control connection stays open, the
	// client keeps believing in its session, and only the helper can end it.
	if err := session.SuppressHeartbeatsForAcceptanceBlackhole(); err != nil {
		t.Fatalf("blackhole the live helper session: %v", err)
	}
	blackholeStarted := time.Now()
	// The attempt must still outlive the whole observation window, measured
	// against the deadman the helper armed at reservation.
	remainingDeadman := deadman - blackholeStarted.Sub(attemptStarted)
	if remainingDeadman <= reapBound {
		t.Fatalf("attempt deadman has %s left at the blackhole, which does not outlast the %s observation window; a deadman expiry would be indistinguishable from the session reap",
			remainingDeadman, reapBound)
	}
	reapElapsed := waitForNativeOCIContainerAbsence(t, containerdAddress, identity.ContainerID, blackholeStarted, reapBound)
	// The helper's deadline is measured from the last heartbeat it accepted,
	// which may be up to one interval before the blackhole began. Anything
	// earlier than that is some other cause, not this one.
	earliest := heartbeatTimeout - heartbeatInterval
	if reapElapsed < earliest {
		t.Fatalf("attempt was reaped %s into the blackhole, before the helper's %s heartbeat deadline could expire even from the last accepted heartbeat; the reap is not the blackhole's",
			reapElapsed, heartbeatTimeout)
	}

	closesAfter := countHelperSessionCloses(t, helperStderrPath)
	if closesAfter.total != closesBefore.total+1 {
		t.Fatalf("helper session closes = %d, want exactly one more than the %d before the blackhole", closesAfter.total, closesBefore.total)
	}
	closeReason := "session control EOF"
	if closesAfter.heartbeatDeadline > closesBefore.heartbeatDeadline {
		closeReason = "session heartbeat deadline expired"
	} else if closesAfter.controlEOF == closesBefore.controlEOF {
		t.Fatal("the helper ended this session for a reason no silent client can cause")
	}

	var watchErr error
	select {
	case watchErr = <-watchDone:
	case <-time.After(reapBound):
		t.Fatal("the attempt watch never ended after the helper reaped its session")
	}
	var watchLoss *ocihelper.RuntimeLossError
	if !errors.As(watchErr, &watchLoss) {
		t.Fatalf("blackholed attempt watch ended with %v, want the typed runtime loss the attempt lands as runtime_failure", watchErr)
	}
	staleCtx, cancelStale := context.WithTimeout(t.Context(), handshake.ReapTimeout)
	_, staleErr := session.Verify(staleCtx, ocihelper.VerifyRequest{Scope: ocihelper.VerifyNamespaceReadOnly})
	cancelStale()
	var staleRPC *ocihelper.RPCError
	if !errors.As(staleErr, &staleRPC) || staleRPC.Code != ocihelper.CodeSessionStale {
		t.Fatalf("post-blackhole request on the dead session = %v, want %s", staleErr, ocihelper.CodeSessionStale)
	}

	// The helper must still be usable. The fresh session's sweep is where the
	// expired session's reap reports itself: ReapSession returns the inventory
	// it removed, the helper holds that in sessionReapSweep, and the next
	// sweep merges it in. So the receipt naming this container is the helper's
	// own account of having reaped it -- and because the container was already
	// positively absent before this takeover began, that account cannot be the
	// takeover's own work.
	barrier.Invalidate()
	if err := barrier.Ensure(ctx); err != nil {
		t.Fatalf("helper did not admit a fresh session after the blackhole reap: %v", err)
	}
	receipt, ok := barrier.SweepReceipt()
	if !ok || !receipt.VerifiedAbsent {
		t.Fatalf("post-blackhole takeover sweep receipt = %+v present=%t", receipt, ok)
	}
	if !slices.Contains(receipt.SweptInventory.Containers, identity.ContainerID) {
		t.Fatalf("fresh session's sweep does not carry container %s from the expired session's reap: %+v",
			identity.ContainerID, receipt.SweptInventory.Containers)
	}
	if !slices.ContainsFunc(receipt.Attempts, func(swept ocihelper.SweptAttemptAuthority) bool {
		return swept.NodeID == authority.NodeID && swept.JobID == authority.JobID &&
			swept.AttemptID == authority.AttemptID && swept.FencingToken == authority.FencingToken &&
			swept.PriorBootSessionID == authority.BootSessionID && swept.Class == authority.Class &&
			swept.RemovalGeneration == authority.RemovalGeneration
	}) {
		t.Fatalf("fresh session's sweep does not attribute the reap to attempt %s: %+v", authority.AttemptID, receipt.Attempts)
	}

	evidenceDirectory := os.Getenv("WEFTY_REALTIME_EVIDENCE_DIR")
	if evidenceDirectory == "" {
		return
	}
	var evidence strings.Builder
	fmt.Fprintf(&evidence, "service_heartbeat_blackhole_live_reaped=true\n")
	fmt.Fprintf(&evidence, "service_heartbeat_blackhole_attempt_class=%s\n", authority.Class)
	fmt.Fprintf(&evidence, "service_heartbeat_blackhole_container=%s\n", identity.ContainerID)
	fmt.Fprintf(&evidence, "service_heartbeat_blackhole_helper_heartbeat_timeout_ns=%d\n", heartbeatTimeout.Nanoseconds())
	fmt.Fprintf(&evidence, "service_heartbeat_blackhole_attempt_deadman_ns=%d\n", deadman.Nanoseconds())
	fmt.Fprintf(&evidence, "service_heartbeat_blackhole_deadman_remaining_at_blackhole_ns=%d\n", remainingDeadman.Nanoseconds())
	fmt.Fprintf(&evidence, "service_heartbeat_blackhole_reap_elapsed_ns=%d\n", reapElapsed.Nanoseconds())
	fmt.Fprintf(&evidence, "service_heartbeat_blackhole_reap_bound_ns=%d\n", reapBound.Nanoseconds())
	fmt.Fprintf(&evidence, "service_heartbeat_blackhole_helper_close_reason=%s\n", closeReason)
	fmt.Fprintf(&evidence, "service_heartbeat_blackhole_terminal_outcome=%s\n", staleRPC.Code)
	fmt.Fprintf(&evidence, "service_heartbeat_blackhole_typed_runtime_loss=true\n")
	fmt.Fprintf(&evidence, "service_heartbeat_blackhole_fresh_session_admitted=true\n")
	if err := os.WriteFile(filepath.Join(evidenceDirectory, "oci-service-heartbeat-blackhole-linux.txt"), []byte(evidence.String()), 0o600); err != nil {
		t.Fatal(err)
	}
}

// nativeOCIContainerIDs requires a successful containerd namespace list, so a
// transport failure can never masquerade as absence. It is the same positive
// check serviceacceptance uses for its own reap proofs; the test process is
// unprivileged and deliberately cannot reach the socket itself. The context
// bounds the command, so a list that outlives the caller's window cannot come
// back and be read as a timely observation.
func nativeOCIContainerIDs(ctx context.Context, containerdAddress string) ([]string, error) {
	list, err := exec.CommandContext(ctx, "sudo", "/usr/local/bin/ctr", "--address", containerdAddress,
		"--namespace", ocihelper.ContainerdNamespace, "containers", "list", "--quiet").CombinedOutput()
	if err != nil {
		return nil, fmt.Errorf("%w\n%s", err, list)
	}
	return strings.Fields(string(list)), nil
}

// waitForNativeOCIContainerAbsence returns how long the blackhole took to reap,
// and only for an absence that was both observed and completed inside the
// bound. A list that finishes late says nothing about a deadline that had
// already passed.
func waitForNativeOCIContainerAbsence(t *testing.T, containerdAddress, containerID string, started time.Time, bound time.Duration) time.Duration {
	t.Helper()
	deadline := started.Add(bound)
	for {
		listCtx, cancelList := context.WithDeadline(context.Background(), deadline)
		ids, err := nativeOCIContainerIDs(listCtx, containerdAddress)
		cancelList()
		observedAt := time.Now()
		if err != nil {
			if observedAt.Before(deadline) {
				t.Fatalf("list live OCI containers to prove the blackhole reap: %v", err)
			}
			t.Fatalf("container %s absence was not observed within %s of the heartbeat blackhole: %v", containerID, bound, err)
		}
		if !observedAt.Before(deadline) {
			t.Fatalf("container %s absence was not observed within %s of the heartbeat blackhole", containerID, bound)
		}
		if !slices.Contains(ids, containerID) {
			return observedAt.Sub(started)
		}
		time.Sleep(250 * time.Millisecond)
	}
}

// helperSessionCloses counts the helper's own session-close log lines by
// reason. The helper arms two deadlines of the same length against a silent
// client: the session heartbeat deadline its watcher owns, and the control
// read deadline. The watcher's is set microseconds earlier and normally wins,
// but a scheduling hiccup on a hosted runner can let the read deadline expire
// first, and a read deadline that expires on a connection nobody closed is
// still the blackhole. So the test requires exactly one new close and one of
// these two reasons, and records which; making the reason itself the assertion
// would buy no proof and invite a #163-class flake.
type helperSessionCloses struct{ total, heartbeatDeadline, controlEOF int }

func countHelperSessionCloses(t *testing.T, path string) helperSessionCloses {
	t.Helper()
	payload, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return helperSessionCloses{}
	}
	if err != nil {
		t.Fatalf("read helper stderr: %v", err)
	}
	text := string(payload)
	return helperSessionCloses{
		total:             strings.Count(text, "closing reason="),
		heartbeatDeadline: strings.Count(text, `closing reason="session heartbeat deadline expired"`),
		controlEOF:        strings.Count(text, `closing reason="session control EOF"`),
	}
}
