//go:build service_acceptance_realtiming && linux

package ocihelper_test

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/runner/ocihelper"
	"golang.org/x/sys/unix"
)

const (
	realtimingHelperServiceUnit = "wefty-oci-helper-realtiming.service"
	realtimingHelperSocketUnit  = "wefty-oci-helper-realtiming.socket"
	// coldHelperHold is how long the stopped service must stay inactive with
	// nothing connecting before the probe dials. It separates "cold" from a
	// unit that some other client is about to re-activate anyway.
	coldHelperHold = 3 * time.Second
)

// The unit properties the proof reads. The *Monotonic timestamps are systemd's
// CLOCK_MONOTONIC microseconds, the same clock the probe samples around its
// dial, so the two can be ordered without trusting wall time.
var helperUnitProperties = []string{
	"ActiveState", "SubState", "MainPID", "NRestarts", "InvocationID", "TriggeredBy",
	"InactiveExitTimestampMonotonic", "ExecMainStartTimestampMonotonic", "ActiveEnterTimestamp",
}

// TestNativeLinuxHelperColdSocketActivation proves the Linux-only claim that
// the root helper is socket-activated (#402): with the .socket listening and
// the .service stopped, the first connect made through the product client is
// what makes systemd start the service, and that one connect is admitted as a
// session. Root ownership is proven by TestNativeLinuxOCIAdapterLifecycle; this
// proves the activation itself.
func TestNativeLinuxHelperColdSocketActivation(t *testing.T) {
	helperSocket := os.Getenv("WEFTY_OCI_HELPER_SOCKET")
	helperChecksum := os.Getenv("WEFTY_OCI_HELPER_CHECKSUM")
	evidenceDirectory := os.Getenv("WEFTY_REALTIME_EVIDENCE_DIR")
	if helperSocket == "" || helperChecksum == "" || evidenceDirectory == "" {
		t.Fatal("Linux OCI helper cold socket activation provisioning is incomplete")
	}
	if os.Geteuid() == 0 {
		t.Fatal("the cold socket activation probe must connect as the unprivileged agent")
	}

	// Only the service is stopped. The socket stays listening, which is the
	// installed resting state: nothing but a connect may start the helper now.
	requestRootFault(t, "stop-helper-service-keep-socket")
	serviceStopped := readHelperUnit(t, realtimingHelperServiceUnit)
	socketStopped := readHelperUnit(t, realtimingHelperSocketUnit)
	assertColdHelperService(t, "after the service-only stop", serviceStopped)
	if socketStopped.properties["ActiveState"] != "active" || socketStopped.properties["SubState"] != "listening" {
		t.Fatalf("helper socket after the service-only stop = %s/%s, want active/listening",
			socketStopped.properties["ActiveState"], socketStopped.properties["SubState"])
	}

	holdStarted := time.Now()
	time.Sleep(coldHelperHold)
	serviceBefore := readHelperUnit(t, realtimingHelperServiceUnit)
	coldHold := time.Since(holdStarted)
	assertColdHelperService(t, "after the cold hold", serviceBefore)
	if serviceBefore.properties["InactiveExitTimestampMonotonic"] != serviceStopped.properties["InactiveExitTimestampMonotonic"] {
		t.Fatalf("helper service left inactive during the %s cold hold with nothing connecting: InactiveExitTimestampMonotonic %s -> %s",
			coldHelperHold, serviceStopped.properties["InactiveExitTimestampMonotonic"], serviceBefore.properties["InactiveExitTimestampMonotonic"])
	}

	// The product client, with its own Unix dialer, wrapped only to count and
	// time the dials it makes.
	client := ocihelper.NewUnixClient(helperSocket, helperChecksum)
	client.HeartbeatInterval = time.Second
	productDial := client.Dial
	var dialMu sync.Mutex
	var dials int
	var firstDial struct {
		started, connected int64
		err                error
	}
	client.Dial = func(ctx context.Context) (net.Conn, error) {
		started := monotonicMicroseconds(t)
		connection, err := productDial(ctx)
		connected := monotonicMicroseconds(t)
		dialMu.Lock()
		dials++
		if dials == 1 {
			firstDial.started, firstDial.connected, firstDial.err = started, connected, err
		}
		dialMu.Unlock()
		return connection, err
	}

	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()
	session, err := client.OpenSession(ctx, ocihelper.AcquireSessionRequest{
		NodeID: "native-cold-activation-node", BootSessionID: "native-cold-activation-boot",
	})
	admitted := monotonicMicroseconds(t)
	dialMu.Lock()
	sessionDials := dials
	firstDialStarted, firstDialConnected, firstDialErr := firstDial.started, firstDial.connected, firstDial.err
	dialMu.Unlock()
	if err != nil {
		t.Fatalf("the first connect to the cold helper socket was not admitted as a session after %d dial(s): %v", sessionDials, err)
	}
	defer session.Close()
	if sessionDials != 1 || firstDialErr != nil {
		t.Fatalf("session admission took %d dial(s), first dial err=%v; want exactly one successful connect", sessionDials, firstDialErr)
	}
	handshake := session.Handshake()
	if handshake.SessionCapability == "" || handshake.HelperInstanceID == "" {
		t.Fatalf("first-connect session carries no admitted capability: %+v", handshake)
	}
	if _, err := session.DoctorStatus(ctx); err != nil {
		t.Fatalf("admitted first-connect session did not serve a request: %v", err)
	}

	serviceAfter := readHelperUnit(t, realtimingHelperServiceUnit)
	after := serviceAfter.properties
	before := serviceBefore.properties
	if serviceAfter.isActive != "active" || after["ActiveState"] != "active" || after["SubState"] != "running" {
		t.Fatalf("helper service after the first connect = is-active %q, %s/%s; want active/running",
			serviceAfter.isActive, after["ActiveState"], after["SubState"])
	}
	mainPID, err := strconv.Atoi(after["MainPID"])
	if err != nil || mainPID <= 0 {
		t.Fatalf("helper service MainPID after the first connect = %q", after["MainPID"])
	}
	mainUID := processRealUID(t, mainPID)
	if mainUID != 0 {
		t.Fatalf("socket-activated helper process %d runs as uid %d, want root", mainPID, mainUID)
	}
	// systemd keeps NRestarts readable after a clean stop and flushes it on the
	// next start that is not a Restart= retry. A retry increments it instead.
	if after["NRestarts"] != "0" && after["NRestarts"] != before["NRestarts"] {
		t.Fatalf("helper service NRestarts %s -> %s: the start was a Restart= retry, not socket activation", before["NRestarts"], after["NRestarts"])
	}
	if after["InvocationID"] == "" || after["InvocationID"] == before["InvocationID"] {
		t.Fatalf("helper service InvocationID %q -> %q: no new invocation was started", before["InvocationID"], after["InvocationID"])
	}
	if !slices.Contains(strings.Fields(after["TriggeredBy"]), realtimingHelperSocketUnit) {
		t.Fatalf("helper service TriggeredBy = %q, want %s", after["TriggeredBy"], realtimingHelperSocketUnit)
	}
	inactiveExit := parseMonotonicProperty(t, "InactiveExitTimestampMonotonic", after["InactiveExitTimestampMonotonic"])
	execStart := parseMonotonicProperty(t, "ExecMainStartTimestampMonotonic", after["ExecMainStartTimestampMonotonic"])
	// The ordering is the proof: systemd began starting the service only after
	// the probe began its connect, and before that connect was admitted.
	if inactiveExit < firstDialStarted || inactiveExit > admitted {
		t.Fatalf("helper service left inactive at monotonic %dus, outside the first connect window [%dus, %dus]",
			inactiveExit, firstDialStarted, admitted)
	}
	if execStart < inactiveExit || execStart > admitted {
		t.Fatalf("helper main process started at monotonic %dus, outside [%dus, %dus]", execStart, inactiveExit, admitted)
	}

	var evidence strings.Builder
	fmt.Fprintf(&evidence, "helper_service_cold_before_first_connect=true\nhelper_service_started_by_first_connect=true\nhelper_session_admitted_on_first_connect=true\n")
	fmt.Fprintf(&evidence, "helper_service_unit=%s\nhelper_socket_unit=%s\n", realtimingHelperServiceUnit, realtimingHelperSocketUnit)
	fmt.Fprintf(&evidence, "helper_service_is_active_before=%s\nhelper_service_state_before=%s/%s\nhelper_service_main_pid_before=%s\nhelper_socket_state_before=%s/%s\nhelper_service_cold_hold_ns=%d\n",
		serviceBefore.isActive, before["ActiveState"], before["SubState"], before["MainPID"],
		socketStopped.properties["ActiveState"], socketStopped.properties["SubState"], coldHold.Nanoseconds())
	fmt.Fprintf(&evidence, "helper_service_is_active_after=%s\nhelper_service_state_after=%s/%s\nhelper_service_main_pid_after=%d\nhelper_service_main_uid_after=%d\nhelper_service_triggered_by=%s\n",
		serviceAfter.isActive, after["ActiveState"], after["SubState"], mainPID, mainUID, after["TriggeredBy"])
	fmt.Fprintf(&evidence, "helper_service_active_enter_timestamp_after=%s\nhelper_service_nrestarts_before=%s\nhelper_service_nrestarts_after=%s\nhelper_service_invocation_before=%s\nhelper_service_invocation_after=%s\n",
		after["ActiveEnterTimestamp"], before["NRestarts"], after["NRestarts"], before["InvocationID"], after["InvocationID"])
	fmt.Fprintf(&evidence, "first_connect_dial_started_monotonic_us=%d\nfirst_connect_dial_connected_monotonic_us=%d\nhelper_service_inactive_exit_monotonic_us=%d\nhelper_service_exec_main_start_monotonic_us=%d\nfirst_connect_session_admitted_monotonic_us=%d\nfirst_connect_session_dials=%d\nfirst_connect_helper_instance=%s\n",
		firstDialStarted, firstDialConnected, inactiveExit, execStart, admitted, sessionDials, handshake.HelperInstanceID)
	if err := os.WriteFile(filepath.Join(evidenceDirectory, "oci-helper-cold-socket-activation-linux.txt"), []byte(evidence.String()), 0o600); err != nil {
		t.Fatal(err)
	}
}

type helperUnitReading struct {
	isActive   string
	properties map[string]string
}

// readHelperUnit reads a unit the way an operator would, with systemctl
// is-active and show. Neither needs root.
func readHelperUnit(t *testing.T, unit string) helperUnitReading {
	t.Helper()
	// is-active exits non-zero for any state but active; its stdout is the
	// state either way.
	isActiveOutput, isActiveErr := exec.Command("systemctl", "is-active", unit).Output()
	isActive := strings.TrimSpace(string(isActiveOutput))
	if isActive == "" {
		t.Fatalf("systemctl is-active %s printed no state: %v", unit, isActiveErr)
	}
	showOutput, err := exec.Command("systemctl", "show", "--property="+strings.Join(helperUnitProperties, ","), "--", unit).Output()
	if err != nil {
		t.Fatalf("systemctl show %s: %v", unit, err)
	}
	properties := map[string]string{}
	scanner := bufio.NewScanner(bytes.NewReader(showOutput))
	for scanner.Scan() {
		name, value, ok := strings.Cut(scanner.Text(), "=")
		if ok {
			properties[name] = value
		}
	}
	if properties["ActiveState"] == "" {
		t.Fatalf("systemctl show %s returned no ActiveState: %q", unit, showOutput)
	}
	return helperUnitReading{isActive: isActive, properties: properties}
}

func assertColdHelperService(t *testing.T, phase string, reading helperUnitReading) {
	t.Helper()
	properties := reading.properties
	if reading.isActive != "inactive" || properties["ActiveState"] != "inactive" || properties["SubState"] != "dead" || properties["MainPID"] != "0" {
		t.Fatalf("helper service %s = is-active %q, %s/%s MainPID=%s; want a cold inactive/dead unit with no process",
			phase, reading.isActive, properties["ActiveState"], properties["SubState"], properties["MainPID"])
	}
}

func monotonicMicroseconds(t *testing.T) int64 {
	var now unix.Timespec
	if err := unix.ClockGettime(unix.CLOCK_MONOTONIC, &now); err != nil {
		// Called from the dial wrapper too; Errorf is goroutine-safe and a
		// zero reading fails the ordering check below.
		t.Errorf("read CLOCK_MONOTONIC: %v", err)
		return 0
	}
	return now.Nano() / int64(time.Microsecond)
}

func parseMonotonicProperty(t *testing.T, name, value string) int64 {
	t.Helper()
	parsed, err := strconv.ParseInt(value, 10, 64)
	if err != nil || parsed <= 0 {
		t.Fatalf("helper service %s = %q, want a positive monotonic timestamp", name, value)
	}
	return parsed
}

func processRealUID(t *testing.T, pid int) int {
	t.Helper()
	status, err := os.ReadFile(fmt.Sprintf("/proc/%d/status", pid))
	if err != nil {
		t.Fatalf("read helper process status: %v", err)
	}
	for _, line := range strings.Split(string(status), "\n") {
		if fields := strings.Fields(line); len(fields) >= 2 && fields[0] == "Uid:" {
			uid, err := strconv.Atoi(fields[1])
			if err != nil {
				t.Fatalf("parse helper process uid %q: %v", line, err)
			}
			return uid
		}
	}
	t.Fatalf("helper process %d status has no Uid line", pid)
	return -1
}
