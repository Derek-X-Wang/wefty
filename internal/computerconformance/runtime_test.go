package computerconformance

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
)

func startReadinessTestListener(t *testing.T, serve func(int, net.Conn)) (int, func()) {
	t.Helper()
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		for accepted := 0; ; accepted++ {
			connection, acceptErr := listener.Accept()
			if acceptErr != nil {
				return
			}
			serve(accepted, connection)
			_ = connection.Close()
		}
	}()
	closeListener := func() {
		_ = listener.Close()
		<-done
	}
	t.Cleanup(func() { _ = listener.Close() })
	return listener.Addr().(*net.TCPAddr).Port, closeListener
}

func serveConformantReadiness(connection net.Conn) {
	_ = connection.SetReadDeadline(time.Now().Add(3 * time.Second))
	reader := bufio.NewReader(connection)
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			return
		}
		if line == "\r\n" {
			break
		}
	}
	_ = connection.SetReadDeadline(time.Time{})
	_, _ = fmt.Fprintf(connection, "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: %s\r\nSec-WebSocket-Protocol: %s\r\n\r\n", expectedWebSocketAccept(), contract.ComputerDisplayWebSocketSubprotocol)
	_, _ = connection.Write(append([]byte{0x82, contract.ComputerRFBVersionBannerBytes}, []byte("RFB 003.008\n")...))
}

func assertStartKeyCauseIsAssertion(t *testing.T, protocol string, frames [][]byte, wantCause string) {
	t.Helper()
	port, closeListener := startReadinessTestListener(t, func(accepted int, connection net.Conn) {
		if accepted > 0 {
			return
		}
		_ = connection.SetReadDeadline(time.Now().Add(3 * time.Second))
		reader := bufio.NewReader(connection)
		for {
			line, err := reader.ReadString('\n')
			if err != nil {
				return
			}
			if line == "\r\n" {
				break
			}
		}
		_, _ = fmt.Fprintf(connection, "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: %s\r\nSec-WebSocket-Protocol: %s\r\n\r\n", expectedWebSocketAccept(), protocol)
		for _, payload := range frames {
			_, _ = connection.Write(append([]byte{0x82, byte(len(payload))}, payload...))
		}
	})
	defer closeListener()
	runner := runtimeRunner{
		controlPort: port,
		config: RuntimeConfig{
			InputOraclePath: "/oracle",
			Now:             time.Now,
			Sleep:           sleepContext,
		},
		recorder: NewRecorder("broken", "docker", "linux/arm64", time.Now()),
		runCommandHook: func(context.Context, ...string) (commandResult, error) {
			return commandResult{stdout: `{"version":1,"generation":0,"key_events":2,"x":0,"y":0,"pointer_history":[]}`}, nil
		},
	}
	if runner.proveViewIsolation(context.Background(), "input.view-isolated", 1, 2, 3, 4) {
		t.Fatal("invalid StartKey exchange proved view isolation")
	}
	for _, check := range runner.recorder.Finish(time.Now()).Checks {
		if check.ID == "input.view-isolated" {
			if check.FailureReason != FailureAssertionFailed || !strings.Contains(check.Detail, wantCause) || check.ReadinessEvent != "" {
				t.Fatalf("StartKey failure evidence = %+v, want cause %q", check, wantCause)
			}
			return
		}
	}
	t.Fatal("input.view-isolated evidence was not emitted")
}

func TestMissingEndpointObservationWindowStaysInsideReadinessBudget(t *testing.T) {
	if missingEndpointObservationWindow != 15*time.Second {
		t.Fatalf("missing endpoint observation window = %s, want 15s", missingEndpointObservationWindow)
	}
	if missingEndpointObservationWindow >= contract.ComputerStartupReadinessTimeout {
		t.Fatal("missing endpoint observation window exhausted the contract readiness budget")
	}
}

func TestTeardownRemoveFailureIsNonFatalWhenInspectProvesContainerAbsent(t *testing.T) {
	root := filepath.Join(t.TempDir(), "checker-root")
	if err := os.Mkdir(root, 0o755); err != nil {
		t.Fatal(err)
	}
	recorder := NewRecorder("image", "docker", "linux/amd64", time.Unix(100, 0))
	var logs []string
	runner := runtimeRunner{
		root:        root,
		containerID: "already-absent",
		recorder:    recorder,
		config:      RuntimeConfig{Runtime: "docker", Image: "image", RepairImage: "reference@sha256:abc"},
		runCommandHook: func(_ context.Context, arguments ...string) (commandResult, error) {
			switch arguments[0] {
			case "exec", "stop":
				return commandResult{}, nil
			case "rm":
				return commandResult{stderr: "Error response from daemon: removal of container already-absent failed"}, errors.New("runtime command failed")
			case "inspect":
				return commandResult{stderr: "Error: No such object: already-absent"}, errors.New("runtime command failed")
			default:
				return commandResult{}, fmt.Errorf("unexpected runtime command %q", arguments[0])
			}
		},
		teardownLogHook: func(message string) { logs = append(logs, message) },
	}
	if err := runner.cleanup(); err != nil {
		t.Fatalf("absent container remained fatal: %v", err)
	}
	if _, err := os.Stat(root); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("temporary root remained after absent container: %v", err)
	}
	receipt := recorder.Finish(time.Unix(101, 0))
	if len(receipt.Teardown.Leftovers) != 0 {
		t.Fatalf("absent container receipt leftovers = %v", receipt.Teardown.Leftovers)
	}
	if len(receipt.Teardown.Observations) != 1 || receipt.Teardown.Observations[0].Reason != string(TeardownContainerDetachFailed) {
		t.Fatalf("remove diagnostic observations = %+v", receipt.Teardown.Observations)
	}
	if joined := strings.Join(logs, "\n"); !strings.Contains(joined, "teardown observation: reason=container_detach_failed") {
		t.Fatalf("remove diagnostic log = %q", joined)
	}
}

func TestRuntimeObjectAbsentDoesNotTreatUnknownInspectFailureAsAbsence(t *testing.T) {
	for name, stderr := range map[string]string{
		"docker":       "Error: No such object: absent",
		"nerdctl":      "FATA[0000] no such container absent",
		"runtime down": "runtime endpoint not found",
	} {
		t.Run(name, func(t *testing.T) {
			got := runtimeObjectAbsent(commandResult{stderr: stderr})
			want := name != "runtime down"
			if got != want {
				t.Fatalf("runtimeObjectAbsent(%q) = %t, want %t", stderr, got, want)
			}
		})
	}
}

func TestMutationFailureStopsAfterOwningCell(t *testing.T) {
	runner := runtimeRunner{
		config:   RuntimeConfig{MutationProfile: "shm-too-small"},
		recorder: NewRecorder("broken", "docker", "linux/arm64", time.Unix(100, 0)),
	}
	if runner.mutationFailed() {
		t.Fatal("mutation stopped before a failed assertion")
	}
	runner.record("harness.shm-size", StatusFail, "/dev/shm ceiling is not 1 GiB")
	if !runner.mutationFailed() {
		t.Fatal("mutation continued after its first failed assertion")
	}
	for _, check := range runner.recorder.Finish(time.Unix(101, 0)).Checks {
		if check.ID == "harness.shm-size" && check.FailureReason != FailureMutationDetected {
			t.Fatalf("owning failure reason = %q, want mutation_detected", check.FailureReason)
		}
	}
}

func TestMutationReadinessTimeoutContinuesUntilOwningCell(t *testing.T) {
	runner := runtimeRunner{
		config:   RuntimeConfig{MutationProfile: "edge-does-not-recover"},
		recorder: NewRecorder("broken", "docker", "linux/arm64", time.Unix(100, 0)),
	}
	runner.recordReadinessTimeout("input.view-isolated-during-tenure", "key observer did not advance", ReadinessEventKeyObserverAdvanced, 18*time.Second, 18*time.Second)
	if runner.mutationFailed() {
		t.Fatal("typed readiness timeout short-circuited the mutation before its owning cell")
	}
	runner.observeReadinessEvent(ReadinessEventKeyObserverAdvanced)
	runner.record("persistence.edge-recovers", StatusFail, "both endpoints did not recover after edge withdrawal")
	if !runner.mutationFailed() {
		t.Fatal("owning mutation cell did not stop the broken-image row")
	}

	receipt := runner.recorder.Finish(time.Unix(101, 0))
	assertion := func(id string) Check {
		t.Helper()
		for _, check := range receipt.Checks {
			if check.ID == id {
				return check
			}
		}
		t.Fatalf("missing receipt check %q", id)
		return Check{}
	}
	if got := assertion("input.view-isolated-during-tenure"); got.FailureReason != FailureReadinessTimeout || got.ReadinessEvent != ReadinessEventKeyObserverAdvanced || !got.ReadinessObservedLater || got.ReadinessObservationWindowSeconds != 18 || got.ReadinessObservationElapsedSeconds != 18 {
		t.Fatalf("readiness timeout evidence = %+v", got)
	}
	if got := assertion("persistence.edge-recovers"); got.FailureReason != FailureMutationDetected || got.ReadinessEvent != "" {
		t.Fatalf("mutation evidence = %+v", got)
	}
}

func TestMutationUnrecoveredInputReadinessTimeoutFailsClosed(t *testing.T) {
	runner := runtimeRunner{
		config:   RuntimeConfig{MutationProfile: "edge-does-not-recover"},
		recorder: NewRecorder("broken", "docker", "linux/arm64", time.Unix(100, 0)),
	}
	runner.recordReadinessTimeout("input.view-isolated-during-tenure", "key observer did not advance", ReadinessEventKeyObserverAdvanced, 18*time.Second, 18*time.Second)
	runner.finalizeInputReadinessTimeouts()
	if !runner.mutationFailed() {
		t.Fatal("unrecovered input readiness timeout did not stop the mutation row")
	}
	for _, check := range runner.recorder.Finish(time.Unix(101, 0)).Checks {
		if check.ID == "input.view-isolated-during-tenure" {
			if check.FailureReason != FailureAssertionFailed || check.Detail != "key observer did not advance" || check.ReadinessEvent != "" {
				t.Fatalf("unrecovered input timeout evidence = %+v", check)
			}
		}
	}
}

func TestMutationUnrelatedAssertionStillStopsFailClosed(t *testing.T) {
	runner := runtimeRunner{
		config:   RuntimeConfig{MutationProfile: "profile-state-lost"},
		recorder: NewRecorder("broken", "docker", "linux/arm64", time.Unix(100, 0)),
	}
	runner.record("harness.rootfs-read-only", StatusFail, "write failed with EACCES, not EROFS")
	if !runner.mutationFailed() {
		t.Fatal("unrelated assertion failure did not stop the mutation row")
	}
	for _, check := range runner.recorder.Finish(time.Unix(101, 0)).Checks {
		if check.ID == "harness.rootfs-read-only" && check.FailureReason != FailureAssertionFailed {
			t.Fatalf("unrelated failure reason = %q", check.FailureReason)
		}
	}
}

func TestInputOracleFailureAfterReadinessFailsMutationClosed(t *testing.T) {
	reads := 0
	runner := runtimeRunner{
		config: RuntimeConfig{
			MutationProfile: "edge-does-not-recover",
			InputOraclePath: "/oracle",
			Now:             time.Now,
			Sleep:           sleepContext,
		},
		recorder: NewRecorder("broken", "docker", "linux/arm64", time.Now()),
		tools:    map[string]bool{"cat": true},
		runCommandHook: func(context.Context, ...string) (commandResult, error) {
			reads++
			if reads == 1 {
				return commandResult{stdout: `{"version":1,"ready":true,"generation":0,"key_events":0,"x":0,"y":0,"pointer_history":[]}`}, nil
			}
			return commandResult{}, errors.New("oracle read failed")
		},
	}
	runner.checkInput(context.Background())
	if !runner.mutationFailed() {
		t.Fatal("input oracle failure after readiness did not fail the mutation row")
	}
	for _, check := range runner.recorder.Finish(time.Now()).Checks {
		if check.ID == "input.view-isolated" && check.Status != StatusNotRun {
			t.Fatalf("input oracle failure evidence = %+v", check)
		}
	}
}

func TestArm64ReadinessEventBudgetTracksObservedStartupWithoutChangingAMD64(t *testing.T) {
	const measuredArm64Startup = 9*time.Second + 293967534*time.Nanosecond
	arm64 := runtimeRunner{
		config:             RuntimeConfig{Platform: "linux/arm64"},
		firstBootReadiness: measuredArm64Startup,
	}
	if got := arm64.readinessEventObservationBudget(); got != 2*measuredArm64Startup {
		t.Fatalf("arm64 readiness-event budget = %s, want %s", got, 2*measuredArm64Startup)
	}
	fastArm64 := runtimeRunner{config: RuntimeConfig{Platform: "linux/arm64"}, firstBootReadiness: 100 * time.Millisecond}
	if got := fastArm64.readinessEventObservationBudget(); got != legacyInputObserverWindow {
		t.Fatalf("fast arm64 readiness-event budget = %s, want legacy floor %s", got, legacyInputObserverWindow)
	}
	amd64 := runtimeRunner{
		config:             RuntimeConfig{Platform: "linux/amd64"},
		firstBootReadiness: measuredArm64Startup,
	}
	if got := amd64.readinessEventObservationBudget(); got != legacyInputObserverWindow {
		t.Fatalf("amd64 readiness-event budget = %s, want unchanged legacy window", got)
	}
}

func TestArm64KeyObserverWaitUsesMeasuredEventBudget(t *testing.T) {
	before := inputObservation{KeyEvents: 2}
	run := func(platform string) (bool, int) {
		t.Helper()
		clock := time.Unix(100, 0)
		reads := 0
		runner := runtimeRunner{
			config: RuntimeConfig{
				Platform:        platform,
				InputOraclePath: "/oracle",
				Now:             func() time.Time { return clock },
				Sleep: func(_ context.Context, duration time.Duration) error {
					clock = clock.Add(duration)
					return nil
				},
			},
			firstBootReadiness: time.Second,
		}
		runner.runCommandHook = func(context.Context, ...string) (commandResult, error) {
			reads++
			if reads == 13 {
				return commandResult{stdout: `{"version":1,"generation":0,"key_events":3,"x":0,"y":0,"pointer_history":[]}`}, nil
			}
			return commandResult{stdout: `{"version":1,"generation":0,"key_events":2,"x":0,"y":0,"pointer_history":[]}`}, nil
		}
		client, server := net.Pipe()
		done := make(chan struct{})
		go func() {
			_, _ = io.Copy(io.Discard, server)
			_ = server.Close()
			close(done)
		}()
		session := &InputSession{connection: &websocketConnection{connection: client}}
		_, observed, _, _, waitErr := runner.waitInputObserverAdvance(context.Background(), before, session)
		session.Close()
		<-done
		if waitErr != nil {
			t.Fatalf("observer wait failed: %v", waitErr)
		}
		return observed, reads
	}

	if observed, reads := run("linux/arm64"); !observed || reads != 13 {
		t.Fatalf("arm64 observed=%t reads=%d, want true after the legacy 12 polls", observed, reads)
	}
	if observed, reads := run("linux/amd64"); observed || reads != 12 {
		t.Fatalf("amd64 observed=%t reads=%d, want unchanged 12-poll behavior", observed, reads)
	}
}

func TestReadinessProbeDoesNotCallFirstFrameDelayPlainTCP(t *testing.T) {
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	done := make(chan struct{})
	go func() {
		defer close(done)
		for accepted := 0; accepted < 2; accepted++ {
			connection, acceptErr := listener.Accept()
			if acceptErr != nil {
				return
			}
			if accepted == 0 {
				_, _ = fmt.Fprintf(connection, "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: %s\r\nSec-WebSocket-Protocol: %s\r\n\r\n", expectedWebSocketAccept(), contract.ComputerDisplayWebSocketSubprotocol)
			}
			_ = connection.Close()
		}
	}()
	port := listener.Addr().(*net.TCPAddr).Port
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	ready, event, err := (&runtimeRunner{}).probeReady(ctx, port, ReadinessEventViewEndpointReady)
	if ready || event != ReadinessEventFirstRFBFrame || err == nil {
		t.Fatalf("first-frame delay probe = ready %t event %q err %v", ready, event, err)
	}
	if terminalProbeProvesPlainTCP(ctx, port, err) {
		t.Fatal("first-frame delay was promoted to plain TCP evidence")
	}
	_ = listener.Close()
	<-done
}

func TestReadinessProbeNamesTheLastMissingEventAfterEndpointDisappears(t *testing.T) {
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	viewDone := make(chan struct{})
	go func() {
		defer close(viewDone)
		connection, acceptErr := listener.Accept()
		if acceptErr != nil {
			return
		}
		defer connection.Close()
		_ = connection.SetReadDeadline(time.Now().Add(time.Second))
		reader := bufio.NewReader(connection)
		for {
			line, readErr := reader.ReadString('\n')
			if readErr != nil {
				return
			}
			if line == "\r\n" {
				break
			}
		}
		_, _ = fmt.Fprintf(connection, "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: %s\r\nSec-WebSocket-Protocol: %s\r\n\r\n", expectedWebSocketAccept(), contract.ComputerDisplayWebSocketSubprotocol)
		_ = listener.Close()
	}()
	controlPort, closeControl := startReadinessTestListener(t, func(_ int, connection net.Conn) {
		serveConformantReadiness(connection)
	})
	defer closeControl()

	startedAt := time.Unix(100, 0)
	now := startedAt
	sleeps := 0
	runner := runtimeRunner{
		viewPort:    listener.Addr().(*net.TCPAddr).Port,
		controlPort: controlPort,
		recorder:    NewRecorder("image", "docker", "linux/amd64", startedAt),
		config: RuntimeConfig{
			Now: func() time.Time { return now },
			Sleep: func(context.Context, time.Duration) error {
				sleeps++
				if sleeps == 1 {
					now = now.Add(driverObservationInterval)
				} else {
					now = startedAt.Add(contract.ComputerStartupReadinessTimeout)
				}
				return nil
			},
		},
	}
	if err := runner.waitReady(context.Background(), startedAt, false); err == nil {
		t.Fatal("disappeared view endpoint passed readiness")
	}
	<-viewDone
	for _, check := range runner.recorder.Finish(now).Checks {
		if check.ID == "transport.view-ready" && check.ReadinessEvent != ReadinessEventViewEndpointReady {
			t.Fatalf("view timeout after endpoint disappeared = %+v", check)
		}
	}
}

func TestReadinessProbeStillCallsNonUpgradeHTTPPlainTCP(t *testing.T) {
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	done := make(chan struct{})
	go func() {
		defer close(done)
		connection, acceptErr := listener.Accept()
		if acceptErr != nil {
			return
		}
		_, _ = fmt.Fprint(connection, "HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\n\r\n")
		_ = connection.Close()
	}()
	port := listener.Addr().(*net.TCPAddr).Port
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	ready, event, err := (&runtimeRunner{}).probeReady(ctx, port, ReadinessEventControlEndpointReady)
	var rejected *rfbUpgradeRejectedError
	if ready || event != ReadinessEventControlEndpointReady || !errors.As(err, &rejected) {
		t.Fatalf("non-upgrade HTTP probe = ready %t event %q err %v", ready, event, err)
	}
	<-done
}

func TestReadinessWindowRecoversAfterTransientAcceptWithoutUpgrade(t *testing.T) {
	viewPort, closeView := startReadinessTestListener(t, func(accepted int, connection net.Conn) {
		if accepted > 0 {
			serveConformantReadiness(connection)
		}
	})
	controlPort, closeControl := startReadinessTestListener(t, func(_ int, connection net.Conn) {
		serveConformantReadiness(connection)
	})
	defer closeView()
	defer closeControl()

	runner := runtimeRunner{
		viewPort:    viewPort,
		controlPort: controlPort,
		recorder:    NewRecorder("image", "docker", "linux/amd64", time.Now()),
		config: RuntimeConfig{
			Now:   time.Now,
			Sleep: sleepContext,
		},
	}
	if err := runner.waitReady(context.Background(), time.Now(), false); err != nil {
		t.Fatalf("transient accept prevented later conformant readiness: %v", err)
	}
}

func TestReadinessWindowAllowsDelayedUpgrade(t *testing.T) {
	viewPort, closeView := startReadinessTestListener(t, func(accepted int, connection net.Conn) {
		if accepted == 0 {
			time.Sleep(100 * time.Millisecond)
		}
		serveConformantReadiness(connection)
	})
	controlPort, closeControl := startReadinessTestListener(t, func(_ int, connection net.Conn) {
		serveConformantReadiness(connection)
	})
	defer closeView()
	defer closeControl()

	runner := runtimeRunner{
		viewPort:    viewPort,
		controlPort: controlPort,
		recorder:    NewRecorder("image", "docker", "linux/amd64", time.Now()),
		config: RuntimeConfig{
			Now:   time.Now,
			Sleep: sleepContext,
		},
	}
	if err := runner.waitReady(context.Background(), time.Now(), false); err != nil {
		t.Fatalf("delayed upgrade inside readiness window failed: %v", err)
	}
}

func TestRawRFBIsRejectedOnlyAfterReadinessWindow(t *testing.T) {
	viewPort, closeView := startReadinessTestListener(t, func(_ int, connection net.Conn) {
		serveConformantReadiness(connection)
	})
	controlPort, closeControl := startReadinessTestListener(t, func(_ int, connection net.Conn) {
		_, _ = io.WriteString(connection, "RFB 003.008\n")
	})
	defer closeView()
	defer closeControl()

	startedAt := time.Unix(100, 0)
	now := startedAt
	sleeps := 0
	runner := runtimeRunner{
		viewPort:    viewPort,
		controlPort: controlPort,
		recorder:    NewRecorder("broken", "docker", "linux/amd64", startedAt),
		config: RuntimeConfig{
			MutationProfile: "plain-rfb-control",
			Now:             func() time.Time { return now },
			Sleep: func(context.Context, time.Duration) error {
				sleeps++
				now = startedAt.Add(contract.ComputerStartupReadinessTimeout)
				return nil
			},
		},
	}
	if err := runner.waitReady(context.Background(), startedAt, false); err == nil {
		t.Fatal("raw RFB endpoint passed readiness")
	}
	if sleeps != 1 {
		t.Fatalf("raw RFB endpoint ended polling after %d sleeps, want the readiness window to expire", sleeps)
	}
	for _, check := range runner.recorder.Finish(now).Checks {
		if check.ID == "transport.plain-tcp-rejected" && (check.Status != StatusFail || check.FailureReason != FailureMutationDetected) {
			t.Fatalf("raw RFB evidence = %+v", check)
		}
	}
}

func TestMissingControlMutationRoutesTimeoutThroughOwningCell(t *testing.T) {
	viewPort, closeView := startReadinessTestListener(t, func(_ int, connection net.Conn) {
		serveConformantReadiness(connection)
	})
	defer closeView()
	controlPort, err := availablePort()
	if err != nil {
		t.Fatal(err)
	}

	startedAt := time.Unix(100, 0)
	now := startedAt
	runner := runtimeRunner{
		viewPort:    viewPort,
		controlPort: controlPort,
		recorder:    NewRecorder("broken", "docker", "linux/amd64", startedAt),
		config: RuntimeConfig{
			MutationProfile: "missing-control-endpoint",
			Now:             func() time.Time { return now },
			Sleep: func(context.Context, time.Duration) error {
				now = startedAt.Add(missingEndpointObservationWindow)
				return nil
			},
		},
	}
	if err := runner.waitReady(context.Background(), startedAt, false); err == nil {
		t.Fatal("missing control endpoint passed readiness")
	}
	for _, check := range runner.recorder.Finish(now).Checks {
		if check.ID == "transport.control-ready" && (check.Status != StatusFail || check.FailureReason != FailureMutationDetected) {
			t.Fatalf("missing control owning-cell evidence = %+v", check)
		}
	}
}

func TestTerminalRawTCPProbeDetectsNonWebSocketListenerShapes(t *testing.T) {
	for name, serve := range map[string]func(net.Conn){
		"raw RFB": func(connection net.Conn) {
			_, _ = io.WriteString(connection, "RFB 003.008\n")
		},
		"silent accept": func(connection net.Conn) {
			_, _ = io.Copy(io.Discard, connection)
		},
		"accept then close": func(net.Conn) {},
	} {
		t.Run(name, func(t *testing.T) {
			listener, err := net.Listen("tcp4", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = listener.Close() })
			done := make(chan struct{})
			go func() {
				defer close(done)
				for accepted := 0; accepted < 2; accepted++ {
					connection, acceptErr := listener.Accept()
					if acceptErr != nil {
						return
					}
					if accepted == 0 {
						serve(connection)
					}
					_ = connection.Close()
				}
			}()
			port := listener.Addr().(*net.TCPAddr).Port
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			ready, event, probeErr := (&runtimeRunner{}).probeReady(ctx, port, ReadinessEventControlEndpointReady)
			if ready || event != ReadinessEventControlEndpointReady || probeErr == nil {
				t.Fatalf("non-WebSocket readiness probe = ready %t event %q err %v", ready, event, probeErr)
			}
			if !terminalProbeProvesPlainTCP(ctx, port, probeErr) {
				t.Fatal("late raw TCP probe did not detect the listener")
			}
			_ = listener.Close()
			<-done
		})
	}
}

func TestViewIsolationClassifiesRFBNegotiationFailureAsAssertion(t *testing.T) {
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	done := make(chan struct{})
	go func() {
		defer close(done)
		connection, acceptErr := listener.Accept()
		if acceptErr != nil {
			return
		}
		_, _ = fmt.Fprint(connection, "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: wrong\r\nSec-WebSocket-Protocol: binary\r\n\r\n")
		_ = connection.Close()
	}()
	runner := runtimeRunner{
		controlPort: listener.Addr().(*net.TCPAddr).Port,
		config: RuntimeConfig{
			InputOraclePath: "/oracle",
			Now:             time.Now,
			Sleep:           sleepContext,
		},
		recorder: NewRecorder("broken", "docker", "linux/arm64", time.Now()),
		runCommandHook: func(context.Context, ...string) (commandResult, error) {
			return commandResult{stdout: `{"version":1,"generation":0,"key_events":2,"x":0,"y":0,"pointer_history":[]}`}, nil
		},
	}
	if runner.proveViewIsolation(context.Background(), "input.view-isolated", 1, 2, 3, 4) {
		t.Fatal("bad WebSocket negotiation proved view isolation")
	}
	for _, check := range runner.recorder.Finish(time.Now()).Checks {
		if check.ID == "input.view-isolated" {
			if check.FailureReason != FailureAssertionFailed || check.ReadinessEvent != "" {
				t.Fatalf("negotiation failure evidence = %+v", check)
			}
		}
	}
	_ = listener.Close()
	<-done
}

func TestViewIsolationSurfacesBadSubprotocolAsAssertion(t *testing.T) {
	assertStartKeyCauseIsAssertion(t, "wrong", nil, "binary subprotocol was not negotiated")
}

func TestViewIsolationSurfacesInvalidRFBGreetingAsAssertion(t *testing.T) {
	assertStartKeyCauseIsAssertion(t, contract.ComputerDisplayWebSocketSubprotocol, [][]byte{[]byte("not RFB")}, "invalid binary RFB greeting")
}

func TestViewIsolationSurfacesUnavailableSecurityTypeAsAssertion(t *testing.T) {
	assertStartKeyCauseIsAssertion(t, contract.ComputerDisplayWebSocketSubprotocol, [][]byte{[]byte("RFB 003.008\n"), {1, 2}}, "RFB None security type unavailable")
}

func TestViewIsolationSurfacesFailedSecurityNegotiationAsAssertion(t *testing.T) {
	assertStartKeyCauseIsAssertion(t, contract.ComputerDisplayWebSocketSubprotocol, [][]byte{[]byte("RFB 003.008\n"), {1, 1}, {0, 0, 0, 1}}, "RFB security negotiation failed")
}

func TestViewIsolationClassifiesOnlyExpiredRFBDeadlineAsReadiness(t *testing.T) {
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	done := make(chan struct{})
	go func() {
		defer close(done)
		connection, acceptErr := listener.Accept()
		if acceptErr != nil {
			return
		}
		defer connection.Close()
		_, _ = io.Copy(io.Discard, connection)
	}()
	runner := runtimeRunner{
		controlPort: listener.Addr().(*net.TCPAddr).Port,
		config: RuntimeConfig{
			InputOraclePath: "/oracle",
			Now:             time.Now,
			Sleep:           sleepContext,
		},
		recorder: NewRecorder("broken", "docker", "linux/arm64", time.Now()),
		runCommandHook: func(context.Context, ...string) (commandResult, error) {
			return commandResult{stdout: `{"version":1,"generation":0,"key_events":2,"x":0,"y":0,"pointer_history":[]}`}, nil
		},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if runner.proveViewIsolation(ctx, "input.view-isolated", 1, 2, 3, 4) {
		t.Fatal("silent RFB endpoint proved view isolation")
	}
	for _, check := range runner.recorder.Finish(time.Now()).Checks {
		if check.ID == "input.view-isolated" {
			if check.FailureReason != FailureReadinessTimeout || check.ReadinessEvent != ReadinessEventKeyObserverAdvanced || check.ReadinessObservationWindowSeconds <= 0 || check.ReadinessObservationElapsedSeconds < check.ReadinessObservationWindowSeconds {
				t.Fatalf("deadline failure evidence = %+v", check)
			}
		}
	}
	_ = listener.Close()
	<-done
}

func TestConformantSubjectNeverUsesMutationShortCircuit(t *testing.T) {
	runner := runtimeRunner{
		recorder: NewRecorder("reference", "docker", "linux/arm64", time.Unix(100, 0)),
	}
	runner.record("input.control-accepted", StatusFail, "control pointer was not observed")
	if runner.mutationFailed() {
		t.Fatal("conformant subject used the broken-image short circuit")
	}
}

func TestUnknownDriverMutationCannotPassWhenObserverAttachesLate(t *testing.T) {
	const (
		falseDocument   = `{"version":1,"human_driving":false}`
		unknownDocument = `{"version":2,"human_driving":true}`
	)

	t.Run("mutation window was overwritten", func(t *testing.T) {
		lateObservation := driverObservation{
			Version:        1,
			HumanDriving:   false,
			Generation:     3,
			Fingerprint:    driverFingerprint(falseDocument),
			Classification: "valid",
		}
		reads := 0
		written := ""
		runner := runtimeRunner{
			controlDir: t.TempDir(),
			config: RuntimeConfig{Sleep: func(context.Context, time.Duration) error {
				return nil
			}},
			readDriverObservationHook: func(context.Context) (driverObservation, error) {
				reads++
				if reads == 1 {
					return driverObservation{
						Version:        1,
						Generation:     1,
						Fingerprint:    driverFingerprint(falseDocument),
						Classification: "valid",
					}, nil
				}
				// The fixture's unknown-version event was emitted before the
				// checker's post-write observer attached. A later snapshot must
				// not be mistaken for observation of that exact mutation.
				return lateObservation, nil
			},
			writeDriverHook: func(document string) error {
				written = document
				return nil
			},
		}
		if err := runner.writeDriver(falseDocument); err != nil {
			t.Fatal(err)
		}
		if result := runner.mutateAndObserveDriver(context.Background(), unknownDocument, "unknown-version", false); result.Failure != driverMutationNotObserved {
			t.Fatal("unknown-driver-version-accepted mutation passed without exact rejection evidence")
		}
		if written != unknownDocument {
			t.Fatalf("written driver document = %q, want %q", written, unknownDocument)
		}
	})
}

func TestDriverMutationWaitsForObserverBeforePublishing(t *testing.T) {
	const (
		falseDocument = `{"version":1,"human_driving":false}`
		trueDocument  = `{"version":1,"human_driving":true}`
	)
	reads := 0
	runner := runtimeRunner{
		controlDir: t.TempDir(),
		config: RuntimeConfig{Sleep: func(context.Context, time.Duration) error {
			return nil
		}},
		readDriverObservationHook: func(context.Context) (driverObservation, error) {
			reads++
			switch reads {
			case 1:
				return driverObservation{Version: 1, Generation: 7, Fingerprint: "stale", Classification: "valid"}, nil
			case 2:
				return driverObservation{Version: 1, Generation: 8, Fingerprint: driverFingerprint(falseDocument), Classification: "valid"}, nil
			default:
				return driverObservation{Version: 1, HumanDriving: true, Generation: 9, Fingerprint: driverFingerprint(trueDocument), Classification: "valid"}, nil
			}
		},
		writeDriverHook: func(string) error {
			if reads < 2 {
				t.Fatal("driver mutation published before the observer attached to the current document")
			}
			return nil
		},
	}
	if err := runner.writeDriver(falseDocument); err != nil {
		t.Fatal(err)
	}
	if !runner.mutateAndObserveDriver(context.Background(), trueDocument, "valid", true).OK() {
		t.Fatal("exact post-mutation observation was not accepted")
	}
}

func TestNegativeDriverFailureDetailsAreDistinct(t *testing.T) {
	for name, fixture := range map[string]struct {
		failure driverMutationFailure
		want    string
	}{
		"accepted":           {driverMutationUnexpectedHumanDriving, "unknown-version generation was accepted"},
		"not observed":       {driverMutationNotObserved, "unknown-version generation was not observed"},
		"never attached":     {driverObserverNeverAttached, "driver observer never attached before unknown-version generation"},
		"generation reset":   {driverObserverGenerationReset, "driver observer generation reset before unknown-version generation was observed"},
		"no-op rewrite":      {driverMutationNoOp, "driver assertion attempted a no-op rewrite before unknown-version generation"},
		"publication failed": {driverMutationWriteFailed, "unknown-version generation could not be published"},
	} {
		t.Run(name, func(t *testing.T) {
			if got := negativeDriverFailureDetail("unknown-version generation", driverMutationResult{Failure: fixture.failure}); got != fixture.want {
				t.Fatalf("detail = %q, want %q", got, fixture.want)
			}
		})
	}
}

func TestDriverMutationRejectsNoOpAndGenerationReset(t *testing.T) {
	const (
		falseDocument = `{"version":1,"human_driving":false}`
		trueDocument  = `{"version":1,"human_driving":true}`
	)
	runner := runtimeRunner{
		controlDir: t.TempDir(),
		config:     RuntimeConfig{Sleep: func(context.Context, time.Duration) error { return nil }},
	}
	if err := runner.writeDriver(falseDocument); err != nil {
		t.Fatal(err)
	}
	if result := runner.mutateAndObserveDriver(context.Background(), falseDocument, "valid", false); result.Failure != driverMutationNoOp {
		t.Fatalf("no-op failure = %q", result.Failure)
	}
	runner.readDriverObservationHook = func(context.Context) (driverObservation, error) {
		payload, err := os.ReadFile(filepath.Join(runner.controlDir, "driver.json"))
		if err != nil {
			return driverObservation{}, err
		}
		if string(payload) == falseDocument {
			return driverObservation{Version: 1, Generation: 8, Fingerprint: driverFingerprint(falseDocument), Classification: "valid"}, nil
		}
		return driverObservation{Version: 1, Generation: 1, Fingerprint: driverFingerprint(trueDocument), Classification: "valid"}, nil
	}
	if result := runner.mutateAndObserveDriver(context.Background(), trueDocument, "valid", true); result.Failure != driverObserverGenerationReset {
		t.Fatalf("generation reset failure = %q", result.Failure)
	}
}

func TestInputObserverAdvanceRequiresKeyAndObserverProgress(t *testing.T) {
	observerLines := func(value uint64) *uint64 { return &value }
	before := inputObservation{KeyEvents: 7, ObserverLines: observerLines(19)}
	for name, after := range map[string]inputObservation{
		"neither":       {KeyEvents: 7, ObserverLines: observerLines(19)},
		"key only":      {KeyEvents: 8, ObserverLines: observerLines(19)},
		"observer only": {KeyEvents: 7, ObserverLines: observerLines(20)},
		"field removed": {KeyEvents: 8},
	} {
		t.Run(name, func(t *testing.T) {
			if inputObserverAdvanced(before, after) {
				t.Fatalf("inputObserverAdvanced(%+v, %+v) = true", before, after)
			}
		})
	}
	if after := (inputObservation{KeyEvents: 8, ObserverLines: observerLines(20)}); !inputObserverAdvanced(before, after) {
		t.Fatalf("inputObserverAdvanced(%+v, %+v) = false", before, after)
	}
	if !inputObserverAdvanced(inputObservation{KeyEvents: 7}, inputObservation{KeyEvents: 8}) {
		t.Fatal("inputObserverAdvanced rejected a key-only legacy oracle")
	}
}

func TestTeardownWaitsForSlowStopBeforeDetachingMounts(t *testing.T) {
	root := filepath.Join(t.TempDir(), "checker-root")
	if err := os.Mkdir(root, 0o755); err != nil {
		t.Fatal(err)
	}
	stopEntered := make(chan struct{})
	allowExit := make(chan struct{})
	removeCalled := make(chan struct{}, 1)
	runner := runtimeRunner{
		root:        root,
		containerID: "slow-container",
		config:      RuntimeConfig{Runtime: "docker", Image: "reference-image", RepairImage: "reference-image@sha256:abc"},
		runCommandHook: func(_ context.Context, arguments ...string) (commandResult, error) {
			switch arguments[0] {
			case "exec":
				return commandResult{}, nil
			case "stop":
				close(stopEntered)
				<-allowExit
				return commandResult{}, nil
			case "rm":
				removeCalled <- struct{}{}
				return commandResult{}, nil
			default:
				return commandResult{}, fmt.Errorf("unexpected runtime command %q", arguments[0])
			}
		},
		teardownLogHook: func(string) {},
	}
	done := make(chan error, 1)
	go func() { done <- runner.cleanup() }()
	select {
	case <-stopEntered:
	case <-time.After(time.Second):
		t.Fatal("stop command did not start")
	}
	select {
	case <-removeCalled:
		t.Fatal("container mounts detached before the slow container exit was observed")
	default:
	}
	if _, err := os.Stat(root); err != nil {
		t.Fatalf("temporary root changed before container exit: %v", err)
	}
	close(allowExit)
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("teardown did not finish after stop returned")
	}
	select {
	case <-removeCalled:
	default:
		t.Fatal("container was not removed after its exit was observed")
	}
	if _, err := os.Stat(root); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("temporary root still exists after teardown: %v", err)
	}
}

func TestTeardownSuccessfulRemovalMakesStopFailureNonFatal(t *testing.T) {
	root := filepath.Join(t.TempDir(), "checker-root")
	if err := os.Mkdir(root, 0o755); err != nil {
		t.Fatal(err)
	}
	recorder := NewRecorder("broken-image", "docker", "linux/amd64", time.Unix(100, 0))
	var logs []string
	runner := runtimeRunner{
		root:        root,
		containerID: "eventually-removed",
		recorder:    recorder,
		config:      RuntimeConfig{Runtime: "docker", Image: "broken-image", RepairImage: "reference@sha256:abc"},
		runCommandHook: func(_ context.Context, arguments ...string) (commandResult, error) {
			switch arguments[0] {
			case "exec":
				return commandResult{}, nil
			case "stop":
				return commandResult{stderr: "Error response from daemon: cannot stop container: eventually-removed: tried to kill container, but did not receive an exit event"}, errors.New("stop command failed")
			case "rm":
				return commandResult{}, nil
			default:
				return commandResult{}, fmt.Errorf("unexpected runtime command %q", arguments[0])
			}
		},
		teardownLogHook: func(message string) { logs = append(logs, message) },
	}
	if err := runner.cleanup(); err != nil {
		t.Fatalf("successful detach and root removal remained fatal: %v", err)
	}
	receipt := recorder.Finish(time.Unix(101, 0))
	if len(receipt.Teardown.Leftovers) != 0 {
		t.Fatalf("successful teardown leftovers = %v", receipt.Teardown.Leftovers)
	}
	if len(receipt.Teardown.Observations) != 1 || receipt.Teardown.Observations[0].Reason != string(TeardownContainerStopFailed) {
		t.Fatalf("stop diagnostic observations = %+v", receipt.Teardown.Observations)
	}
	joined := strings.Join(logs, "\n")
	if !strings.Contains(joined, "teardown observation: reason=container_stop_failed") {
		t.Fatalf("non-fatal teardown log = %q", joined)
	}
	wrapperFatalPattern := regexp.MustCompile(`runtime teardown failed`)
	if wrapperFatalPattern.MatchString(joined) {
		t.Fatalf("wrapper fatal pattern matched non-fatal teardown observation %q", joined)
	}
}

func TestTeardownRepairsDetachedTemporaryRootPermissions(t *testing.T) {
	root := filepath.Join(t.TempDir(), "checker-root")
	if err := os.Mkdir(root, 0o755); err != nil {
		t.Fatal(err)
	}
	removeAttempts := 0
	var repairCommand string
	var logs []string
	var events []string
	runner := runtimeRunner{
		root:        root,
		containerID: "mutation-container",
		config: RuntimeConfig{
			Runtime: "docker", Image: "broken-fixture", RepairImage: "reference-image@sha256:abc", Platform: "linux/amd64",
		},
		removeAllHook: func(path string) error {
			removeAttempts++
			if removeAttempts == 1 {
				return &os.PathError{Op: "openfdat", Path: filepath.Join(path, "service/home/wefty/.config/xfce4/panel/launcher-19"), Err: syscall.EACCES}
			}
			return os.RemoveAll(path)
		},
		runCommandHook: func(_ context.Context, arguments ...string) (commandResult, error) {
			events = append(events, arguments[0])
			if arguments[0] == "run" {
				repairCommand = strings.Join(arguments, " ")
				if len(events) < 4 || events[len(events)-2] != "rm" {
					t.Fatalf("permission repair ran before detach: events=%v", events)
				}
			}
			return commandResult{}, nil
		},
		teardownLogHook: func(message string) { logs = append(logs, message) },
	}
	if err := runner.cleanup(); err != nil {
		t.Fatal(err)
	}
	for _, required := range []string{"run --rm", "--network none", "--user 0:0", "--cap-drop ALL", "--cap-add DAC_OVERRIDE", "--cap-add FOWNER", "type=bind,src=" + root, "--platform linux/amd64", "reference-image@sha256:abc", "chmod -R"} {
		if !strings.Contains(repairCommand, required) {
			t.Fatalf("permission repair command %q is missing %q", repairCommand, required)
		}
	}
	if removeAttempts != 2 {
		t.Fatalf("temporary-root removal attempts = %d, want 2", removeAttempts)
	}
	if joined := strings.Join(logs, "\n"); !strings.Contains(joined, "reason=temporary_root_permission") || !strings.Contains(joined, "permission_repair=true") {
		t.Fatalf("permission repair evidence = %q", joined)
	}
}

func TestPermissionRepairScriptMakesRestrictiveTreeRemovable(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("real-filesystem permission regression requires a non-root test user")
	}
	root := filepath.Join(t.TempDir(), "checker-root")
	locked := filepath.Join(root, "service", "home", "wefty", ".config", "xfce4", "panel", "launcher-17")
	if err := os.MkdirAll(locked, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(locked, "launcher.desktop"), []byte("late write"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(locked, 0); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(locked, 0o700) })
	command := exec.Command("sh", "-c", permissionRepairScript, "wefty-repair", root)
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("permission repair script failed: %v: %s", err, output)
	}
	if err := os.RemoveAll(root); err != nil {
		t.Fatalf("repaired restrictive tree remained non-removable: %v", err)
	}
}

func TestTeardownRetriesTemporaryRootBusyWithinStatedBudget(t *testing.T) {
	root := filepath.Join(t.TempDir(), "checker-root")
	if err := os.Mkdir(root, 0o755); err != nil {
		t.Fatal(err)
	}
	removeAttempts, sleeps := 0, 0
	var logs []string
	runner := runtimeRunner{
		root:     root,
		recorder: NewRecorder("image", "docker", "linux/amd64", time.Unix(100, 0)),
		config: RuntimeConfig{Sleep: func(_ context.Context, duration time.Duration) error {
			if duration != teardownRemoveRetryInterval {
				return fmt.Errorf("busy retry interval = %s", duration)
			}
			sleeps++
			return nil
		}},
		removeAllHook: func(path string) error {
			removeAttempts++
			if removeAttempts <= 2 {
				return &os.PathError{Op: "remove", Path: path, Err: syscall.EBUSY}
			}
			return os.RemoveAll(path)
		},
		teardownLogHook: func(message string) { logs = append(logs, message) },
	}
	if err := runner.cleanup(); err != nil {
		t.Fatal(err)
	}
	if removeAttempts != 3 || sleeps != 2 {
		t.Fatalf("busy teardown attempts=%d sleeps=%d, want 3 and 2", removeAttempts, sleeps)
	}
	joined := strings.Join(logs, "\n")
	if !strings.Contains(joined, "reason=temporary_root_busy") || !strings.Contains(joined, "budget=2s") || !strings.Contains(joined, "removal_retries=2") {
		t.Fatalf("busy retry evidence = %q", joined)
	}
	if evidence := runner.recorder.Finish(time.Unix(101, 0)).Teardown; evidence.RetriesUsed != 2 || len(evidence.Leftovers) != 0 {
		t.Fatalf("busy retry receipt evidence = %+v", evidence)
	}
}

func TestTeardownRetriesTemporaryRootNotEmptyWithinStatedBudget(t *testing.T) {
	root := filepath.Join(t.TempDir(), "checker-root")
	if err := os.Mkdir(root, 0o755); err != nil {
		t.Fatal(err)
	}
	removeAttempts := 0
	runner := runtimeRunner{
		root: root,
		config: RuntimeConfig{Sleep: func(context.Context, time.Duration) error {
			return nil
		}},
		removeAllHook: func(path string) error {
			removeAttempts++
			if removeAttempts == 1 {
				return &os.PathError{Op: "remove", Path: path, Err: syscall.ENOTEMPTY}
			}
			return os.RemoveAll(path)
		},
		teardownLogHook: func(string) {},
	}
	if err := runner.cleanup(); err != nil {
		t.Fatal(err)
	}
	if removeAttempts != 2 {
		t.Fatalf("not-empty removal attempts = %d, want 2", removeAttempts)
	}
}

func TestTeardownBusyExhaustionNamesTypedLeftover(t *testing.T) {
	root := filepath.Join(t.TempDir(), "checker-root")
	if err := os.Mkdir(root, 0o755); err != nil {
		t.Fatal(err)
	}
	removeAttempts := 0
	runner := runtimeRunner{
		root: root,
		config: RuntimeConfig{Sleep: func(context.Context, time.Duration) error {
			return nil
		}},
		removeAllHook: func(path string) error {
			removeAttempts++
			return &os.PathError{Op: "remove", Path: path, Err: syscall.EBUSY}
		},
		teardownLogHook: func(string) {},
	}
	err := runner.cleanup()
	var failure *TeardownFailure
	if !errors.As(err, &failure) {
		t.Fatalf("busy teardown error = %v, want typed failure", err)
	}
	if failure.Reason != TeardownTemporaryRootBusy || failure.Leftover != "temporary-root:"+root {
		t.Fatalf("busy teardown failure = %+v", failure)
	}
	wantAttempts := int(teardownRemoveRetryBudget/teardownRemoveRetryInterval) + 1
	if removeAttempts != wantAttempts {
		t.Fatalf("busy teardown attempts = %d, want %d", removeAttempts, wantAttempts)
	}
}

func TestTeardownDetachFailureRetainsRootAndNamesBothLeftovers(t *testing.T) {
	root := filepath.Join(t.TempDir(), "checker-root")
	if err := os.Mkdir(root, 0o755); err != nil {
		t.Fatal(err)
	}
	recorder := NewRecorder("image", "docker", "linux/amd64", time.Unix(100, 0))
	removeRootCalled := false
	runner := runtimeRunner{
		root:        root,
		containerID: "still-mounted",
		recorder:    recorder,
		config:      RuntimeConfig{Runtime: "docker", Image: "image", RepairImage: "reference@sha256:abc"},
		runCommandHook: func(_ context.Context, arguments ...string) (commandResult, error) {
			if arguments[0] == "rm" {
				return commandResult{stderr: "daemon busy"}, errors.New("remove failed")
			}
			return commandResult{}, nil
		},
		removeAllHook: func(string) error {
			removeRootCalled = true
			return nil
		},
		teardownLogHook: func(string) {},
	}
	err := runner.cleanup()
	var failure *TeardownFailure
	if !errors.As(err, &failure) || failure.Reason != TeardownContainerDetachFailed {
		t.Fatalf("detach failure = %v", err)
	}
	if removeRootCalled {
		t.Fatal("temporary root removal ran without proven bind detachment")
	}
	want := "container:still-mounted,temporary-root:" + root
	if failure.Leftover != want {
		t.Fatalf("typed leftover = %q, want %q", failure.Leftover, want)
	}
	evidence := recorder.Finish(time.Unix(101, 0)).Teardown
	if strings.Join(evidence.Leftovers, ",") != want {
		t.Fatalf("receipt leftovers = %v, want %q", evidence.Leftovers, want)
	}
}
