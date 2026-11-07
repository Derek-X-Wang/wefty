//go:build darwin || linux

package process

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"golang.org/x/sys/unix"
)

// #1468: the agent releases a process service's backend port before the
// payload binds it. These tests take that released port from the test
// process, which is outside the payload's process group, the way any other
// local process could.

func TestServiceListenerInspectionAttributesProcessGroups(t *testing.T) {
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	backend := mustServiceBackend(t, listener.Addr().String())
	group := syscall.Getpgrp()
	if verdict, err := inspectServiceListeners(context.Background(), backend, group); err != nil || verdict != serviceListenerOwned {
		t.Fatalf("own listener verdict = (%d, %v), want owned", verdict, err)
	}
	if verdict, err := inspectServiceListeners(context.Background(), backend, group+1_000_000); err != nil || verdict != serviceListenerForeign {
		t.Fatalf("listener of another group verdict = (%d, %v), want foreign", verdict, err)
	}
	if verdict, err := inspectServiceListeners(context.Background(), mustServiceBackend(t, releasedServiceAddress(t)), group); err != nil || verdict != serviceListenerUnseen {
		t.Fatalf("free port verdict = (%d, %v), want unseen", verdict, err)
	}
}

func TestServiceListenerInspectionIgnoresListenersThatCannotReceiveTheBackend(t *testing.T) {
	address := releasedServiceAddress(t)
	backend := mustServiceBackend(t, address)
	unrelated := listenIPv6Loopback(t, backend.port)
	defer unrelated.Close()
	if verdict, err := inspectServiceListeners(context.Background(), backend, syscall.Getpgrp()+1_000_000); err != nil || verdict != serviceListenerUnseen {
		t.Fatalf("verdict with only [::1]:%d listening = (%d, %v), want unseen", backend.port, verdict, err)
	}
}

func TestServiceBackendReceivingFollowsKernelLookupOrder(t *testing.T) {
	backend := mustServiceBackend(t, "127.0.0.1:8080")
	listener := func(ip string, owner listenerOwner) serviceListener {
		return serviceListener{ip: net.ParseIP(ip), owner: owner}
	}
	for _, test := range []struct {
		name      string
		listeners []serviceListener
		want      serviceListenerVerdict
	}{
		{"exact payload", []serviceListener{listener("127.0.0.1", listenerOfPayload)}, serviceListenerOwned},
		{"v4-mapped exact foreign", []serviceListener{listener("127.0.0.1", listenerOfPayload), listener("::ffff:127.0.0.1", listenerOfOther)}, serviceListenerForeign},
		{"wildcards beside exact payload receive nothing", []serviceListener{listener("127.0.0.1", listenerOfPayload), listener("::", listenerOfOther), listener("0.0.0.0", listenerOfOther)}, serviceListenerOwned},
		{"other addresses never receive", []serviceListener{listener("::1", listenerOfOther), listener("127.0.0.2", listenerOfOther), listener("10.0.0.5", listenerOfOther)}, serviceListenerUnseen},
		{"IPv6 wildcard may be dual-stack beside a wildcard payload", []serviceListener{listener("0.0.0.0", listenerOfPayload), listener("::", listenerOfOther)}, serviceListenerForeign},
		{"wildcard payload alone", []serviceListener{listener("::", listenerOfPayload)}, serviceListenerOwned},
	} {
		if verdict, err := backend.verdict(test.listeners, nil); err != nil || verdict != test.want {
			t.Errorf("%s: verdict = (%d, %v), want %d", test.name, verdict, err, test.want)
		}
	}
	if _, err := backend.verdict([]serviceListener{listener("127.0.0.1", listenerOfUnknown)}, nil); err == nil {
		t.Error("an unattributed receiving listener was given a verdict")
	}
	ipv6 := mustServiceBackend(t, "[::1]:8080")
	if got := ipv6.receiving([]serviceListener{listener("0.0.0.0", listenerOfOther)}); len(got) != 0 {
		t.Error("an IPv6 backend counted the IPv4 wildcard")
	}
}

func TestGuardianPublishesBesideAnIPv6OnlyWildcardListener(t *testing.T) {
	address := releasedServiceAddress(t)
	// Go's tcp6 listener sets IPV6_V6ONLY, so this cannot receive
	// 127.0.0.1 traffic; both Linux and macOS let it share the port.
	wildcard, err := net.Listen("tcp6", net.JoinHostPort("::", strconv.Itoa(mustServiceBackend(t, address).port)))
	if err != nil {
		t.Skipf("IPv6 is unavailable here: %v", err)
	}
	defer wildcard.Close()
	go serveAndCloseForTest(wildcard)
	assertGuardianPublishes(t, helperExecution("listen", address), address)
}

func TestGuardianPublishesBesideAnUnrelatedListenerOnAnotherAddress(t *testing.T) {
	address := releasedServiceAddress(t)
	unrelated := listenIPv6Loopback(t, mustServiceBackend(t, address).port)
	defer unrelated.Close()
	assertGuardianPublishes(t, helperExecution("listen", address), address)
}

func TestGuardianPublishesAListenerOwnedByAChildInThePayloadGroup(t *testing.T) {
	address := releasedServiceAddress(t)
	assertGuardianPublishes(t, helperExecution("listen-child", address), address)
}

// The takeover tests serve the Guardian in this process with the real
// inspector and a short recheck interval; the production interval is 10 s.

func TestGuardianWithdrawsAListenerTakeoverWithoutAFailedProbe(t *testing.T) {
	injectInspector(t)
	address := releasedServiceAddress(t)
	guardian := runInProcessServiceGuardian(t, "listen-reuseport", address, 30*time.Second)
	guardian.expectReadiness(t, true)
	// The payload keeps listening, so every probe keeps succeeding: only the
	// periodic ownership check can see the second, foreign listener.
	foreign := listenReusePort(t, address)
	guardian.expectReadiness(t, false)
	_ = foreign.Close()
	guardian.expectReadiness(t, true)
	guardian.stopWithin(t, 5*time.Second)
}

func TestGuardianStopsAServiceWhoseListenerStaysForeign(t *testing.T) {
	injectInspector(t)
	address := releasedServiceAddress(t)
	guardian := runInProcessServiceGuardian(t, "listen-reuseport", address, time.Second)
	guardian.expectReadiness(t, true)
	foreign := listenReusePort(t, address)
	defer foreign.Close()
	guardian.expectReadiness(t, false)
	guardian.expectPortTaken(t, "outside the payload's process group")
}

// inspectionMode drives the injected inspector of the in-process tests.
type inspectionMode int32

const (
	inspectReal inspectionMode = iota
	inspectFail
	inspectStall
)

var errInjectedInspection = errors.New("injected listener inspection failure")

// injectInspector replaces the Guardian's inspector for one test with one
// that follows mode, and shortens the recheck interval. A stalled
// inspection blocks until its context ends and reports that on cancelled.
func injectInspector(t *testing.T) (mode *atomic.Int32, cancelled <-chan struct{}) {
	t.Helper()
	mode = &atomic.Int32{}
	observed := make(chan struct{}, 16)
	priorInspector, priorInterval := serviceListenerInspector, serviceOwnershipRecheckInterval
	serviceListenerInspector = func(ctx context.Context, backend serviceBackend, group int) (serviceListenerVerdict, error) {
		switch inspectionMode(mode.Load()) {
		case inspectFail:
			return 0, errInjectedInspection
		case inspectStall:
			<-ctx.Done()
			observed <- struct{}{}
			return 0, ctx.Err()
		}
		return inspectServiceListeners(ctx, backend, group)
	}
	serviceOwnershipRecheckInterval = 50 * time.Millisecond
	t.Cleanup(func() { serviceListenerInspector, serviceOwnershipRecheckInterval = priorInspector, priorInterval })
	return mode, observed
}

func TestGuardianInspectionFailureIsNeverPublishedAndEndsTyped(t *testing.T) {
	mode, _ := injectInspector(t)
	t.Run("before startup", func(t *testing.T) {
		mode.Store(int32(inspectFail))
		guardian := runInProcessServiceGuardian(t, "listen", releasedServiceAddress(t), time.Second)
		exited := guardian.next(t)
		if exited.Type != guardianMessageExited || exited.Result.SpawnError == nil ||
			exited.Result.SpawnError.Code != contract.SpawnFailureStartupReadinessTimeout ||
			!strings.Contains(exited.Result.SpawnError.Message, errInjectedInspection.Error()) {
			t.Fatalf("status = %#v, want startup_readiness_timeout naming the inspection failure", exited)
		}
	})
	t.Run("after startup", func(t *testing.T) {
		mode.Store(int32(inspectReal))
		guardian := runInProcessServiceGuardian(t, "listen", releasedServiceAddress(t), time.Second)
		guardian.expectReadiness(t, true)
		mode.Store(int32(inspectFail))
		guardian.expectReadiness(t, false)
		guardian.expectPortTaken(t, errInjectedInspection.Error())
	})
}

func TestGuardianOwnershipDeadlineIgnoresProbeFailures(t *testing.T) {
	mode, _ := injectInspector(t)
	guardian := runInProcessServiceGuardian(t, "listen-toggle", releasedServiceAddress(t), 1500*time.Millisecond)
	guardian.expectReadiness(t, true)
	mode.Store(int32(inspectFail))
	guardian.expectReadiness(t, false)
	// The payload closes its listener: from now on probes fail. An ordinary
	// probe loss never ends the payload, but it must not clear the
	// ownership deadline that the failed inspection started.
	if err := syscall.Kill(guardian.pid, syscall.SIGUSR1); err != nil {
		t.Fatal(err)
	}
	guardian.expectPortTaken(t, errInjectedInspection.Error())
}

func TestGuardianStalledInspectionNeitherChangesReadinessNorDelaysStop(t *testing.T) {
	t.Run("before startup", func(t *testing.T) {
		mode, cancelled := injectInspector(t)
		mode.Store(int32(inspectStall))
		guardian := runInProcessServiceGuardian(t, "listen", releasedServiceAddress(t), 30*time.Second)
		select {
		case status := <-guardian.statuses:
			t.Fatalf("guardian reported %#v while its only inspection stalled", status)
		case <-time.After(500 * time.Millisecond):
		}
		guardian.stopWithin(t, 2*time.Second)
		select {
		case <-cancelled:
		case <-time.After(5 * time.Second):
			t.Fatal("the stop did not cancel the stalled inspection")
		}
	})
	t.Run("while ready", func(t *testing.T) {
		mode, cancelled := injectInspector(t)
		guardian := runInProcessServiceGuardian(t, "listen", releasedServiceAddress(t), 30*time.Second)
		guardian.expectReadiness(t, true)
		mode.Store(int32(inspectStall))
		// Forwarding stays as it is until an inspection answers.
		select {
		case status := <-guardian.statuses:
			t.Fatalf("guardian reported %#v while a recheck stalled", status)
		case <-time.After(500 * time.Millisecond):
		}
		guardian.stopWithin(t, 2*time.Second)
		select {
		case <-cancelled:
		case <-time.After(5 * time.Second):
			t.Fatal("the stop did not cancel the stalled inspection")
		}
	})
}

// A probe failure invalidates an inspection that is still running: the
// listener it saw may be gone, and whatever answers after recovery must be
// inspected anew. The gated inspector holds the first inspection after it
// has seen the payload's own listener; meanwhile the payload closes that
// listener, a probe fails, and another process takes the port.
func TestGuardianDiscardsAnInspectionThatAProbeFailureOutdated(t *testing.T) {
	captured, release, invalidated := make(chan struct{}), make(chan struct{}), make(chan struct{})
	staleReturned := make(chan struct{})
	var calls atomic.Int32
	priorInspector := serviceListenerInspector
	serviceListenerInspector = func(ctx context.Context, backend serviceBackend, group int) (serviceListenerVerdict, error) {
		if calls.Add(1) != 1 {
			// The fresh inspection after recovery answers only after the
			// stale one has delivered its "owned" answer, so that late
			// answer reaches a Guardian that is still supervising.
			<-staleReturned
			time.Sleep(100 * time.Millisecond)
			return inspectServiceListeners(ctx, backend, group)
		}
		verdict, err := inspectServiceListeners(ctx, backend, group)
		if err != nil || verdict != serviceListenerOwned {
			t.Errorf("gated inspection verdict = (%d, %v), want owned", verdict, err)
		}
		close(captured)
		// Cancellation is reported at once, but the stale answer is held
		// until the port answers again.
		go func() {
			select {
			case <-ctx.Done():
				close(invalidated)
			case <-release:
			}
		}()
		<-release
		defer close(staleReturned)
		return verdict, nil
	}
	t.Cleanup(func() { serviceListenerInspector = priorInspector })

	address := releasedServiceAddress(t)
	guardian := runInProcessServiceGuardian(t, "listen-toggle", address, 30*time.Second)
	<-captured
	if err := syscall.Kill(guardian.pid, syscall.SIGUSR1); err != nil {
		t.Fatal(err)
	}
	// The Guardian cancels the held inspection on the failed probe. A
	// Guardian that does not gives no signal, so wait out many probes.
	select {
	case <-invalidated:
	case <-time.After(time.Second):
	}
	foreign, err := net.Listen("tcp4", address)
	if err != nil {
		t.Fatal(err)
	}
	defer foreign.Close()
	taken := time.Now()
	accepted := make(chan struct{}, 64)
	go func() {
		for {
			connection, err := foreign.Accept()
			if err != nil {
				return
			}
			_ = connection.Close()
			accepted <- struct{}{}
		}
	}()
	// Probes are sent one at a time through a one-slot buffer, so a third
	// answered probe proves the Guardian has taken the first: it knows the
	// port answers again. A fresh inspection may end the payload sooner.
	var status *guardianStatusMessage
	for answered := 0; answered < 3 && status == nil; {
		select {
		case <-accepted:
			answered++
		case next, open := <-guardian.statuses:
			if !open {
				t.Fatal("guardian status stream ended")
			}
			status = &next
		case <-time.After(10 * time.Second):
			t.Fatal("the foreign listener never answered a probe")
		}
	}
	close(release)
	if status == nil {
		next := guardian.next(t)
		status = &next
	}
	// The held "owned" answer is about the payload's closed listener. The
	// fresh inspection after recovery sees the foreign one; before startup
	// that ends the payload, and nothing is ever published.
	if status.Type != guardianMessageExited || status.Result.SpawnError == nil ||
		status.Result.SpawnError.Code != contract.SpawnFailureServicePortTaken ||
		!strings.Contains(status.Result.SpawnError.Message, "outside the payload's process group") {
		t.Fatalf("status = %#v, want service_port_taken for the foreign listener", status)
	}
	// The production recheck interval is 10 s: only a fresh inspection on
	// recovery answers this soon.
	if elapsed := time.Since(taken); elapsed > 2*time.Second {
		t.Fatalf("the foreign listener was refused %s after it took the port, want at once", elapsed)
	}
}

// The exposure bound includes slow inspections: forwarding stays as it is
// while one runs. Worst case: the inspection that last confirmed ownership
// saw the payload's listener at once but answered only at its timeout, and
// the recheck after the takeover runs for its whole timeout. Withdrawal must
// still come within the recheck interval, one inspection timeout and one
// probe cycle; the test scales them down from 10 s, 5 s and 450 ms.
func TestGuardianWithdrawsATakeoverWithinTheBoundWhenInspectionsAreSlow(t *testing.T) {
	const (
		interval   = time.Second
		timeout    = time.Second
		probeCycle = 20*time.Millisecond + 100*time.Millisecond
		scheduling = 500 * time.Millisecond
	)
	const (
		inspectNow int32 = iota
		inspectSlowlyOnce
		inspectUntilTimeout
	)
	var phase atomic.Int32
	captured := make(chan struct{})
	priorInspector, priorInterval, priorTimeout := serviceListenerInspector, serviceOwnershipRecheckInterval, serviceListenerInspectTimeout
	serviceListenerInspector = func(ctx context.Context, backend serviceBackend, group int) (serviceListenerVerdict, error) {
		switch {
		case phase.CompareAndSwap(inspectSlowlyOnce, inspectUntilTimeout):
			verdict, err := inspectServiceListeners(ctx, backend, group)
			if err != nil || verdict != serviceListenerOwned {
				t.Errorf("slow inspection verdict = (%d, %v), want owned", verdict, err)
			}
			close(captured)
			<-ctx.Done()
			return verdict, err
		case phase.Load() == inspectUntilTimeout:
			<-ctx.Done()
			return 0, ctx.Err()
		}
		return inspectServiceListeners(ctx, backend, group)
	}
	serviceOwnershipRecheckInterval, serviceListenerInspectTimeout = interval, timeout
	t.Cleanup(func() {
		serviceListenerInspector, serviceOwnershipRecheckInterval, serviceListenerInspectTimeout = priorInspector, priorInterval, priorTimeout
	})

	address := releasedServiceAddress(t)
	guardian := runInProcessServiceGuardian(t, "listen-reuseport", address, 30*time.Second)
	guardian.expectReadiness(t, true)
	phase.Store(inspectSlowlyOnce)
	select {
	case <-captured:
	case <-time.After(10 * time.Second):
		t.Fatal("no recheck inspection started")
	}
	foreign := listenReusePort(t, address)
	defer foreign.Close()
	taken := time.Now()
	guardian.expectReadiness(t, false)
	elapsed := time.Since(taken)
	t.Logf("takeover withdrawn after %s", elapsed)
	if bound := interval + timeout + probeCycle; elapsed > bound+scheduling {
		t.Fatalf("takeover withdrawn after %s, want within %s", elapsed, bound)
	}
	// The slow inspections were on the path: the nominal bound (interval,
	// probe cycle, a quick inspection) would be about 1.1 s here.
	if elapsed < interval+timeout/2 {
		t.Fatalf("takeover withdrawn after %s; the slow inspections were not exercised", elapsed)
	}
	guardian.stopWithin(t, 5*time.Second)
}

// The liveness limit of discarding outdated inspections: while probes flap
// faster than an inspection answers, every inspection is cancelled, so no
// ownership verdict arrives. Routing stays safe: the service is withdrawn on
// the first failed probe and never republished without a completed "owned"
// inspection. When the flapping stops, an inspection completes again (here
// it fails at its timeout) and the ownership deadline ends the payload.
func TestGuardianNeverRepublishesWhileProbesFlapFasterThanInspection(t *testing.T) {
	mode, cancelled := injectInspector(t)
	priorTimeout := serviceListenerInspectTimeout
	serviceListenerInspectTimeout = 2 * time.Second
	t.Cleanup(func() { serviceListenerInspectTimeout = priorTimeout })
	guardian := runInProcessServiceGuardian(t, "listen-toggle", releasedServiceAddress(t), time.Second)
	guardian.expectReadiness(t, true)
	mode.Store(int32(inspectStall))
	cancellations, withdrawn := 0, false
	for range 8 {
		for _, toggle := range []syscall.Signal{syscall.SIGUSR1, syscall.SIGUSR2} {
			if err := syscall.Kill(guardian.pid, toggle); err != nil {
				t.Fatal(err)
			}
			wait := time.After(150 * time.Millisecond)
		waiting:
			for {
				select {
				case <-cancelled:
					cancellations++
				case status := <-guardian.statuses:
					if withdrawn || status.Type != guardianMessageReadiness || status.Ready == nil || *status.Ready {
						t.Fatalf("status while probes flap = %#v, want only the first withdrawal", status)
					}
					withdrawn = true
				case <-wait:
					break waiting
				}
			}
		}
	}
	if !withdrawn || cancellations < 3 {
		t.Fatalf("withdrawn = %t, cancelled inspections = %d; the flapping did not cancel inspections", withdrawn, cancellations)
	}
	// The listener stays open now, so the stalled inspection reaches its
	// timeout and starts the ownership deadline.
	guardian.expectPortTaken(t, context.DeadlineExceeded.Error())
}

type inProcessGuardian struct {
	statuses <-chan guardianStatusMessage
	pid      int
	encoder  *json.Encoder
}

// runInProcessServiceGuardian serves a Guardian in this process for a
// helper payload in mode that owns address, and returns it after Started.
func runInProcessServiceGuardian(t *testing.T, mode, address string, startupDeadline time.Duration) inProcessGuardian {
	t.Helper()
	agentEndpoint, guardianEndpoint, err := newGuardianSocketPair()
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- serveGuardian(guardianEndpoint, os.Stdout, os.Stderr) }()
	start := guardianTestStart(mode)
	start.Start.Args = append(start.Start.Args, address)
	start.Start.ServiceAddress = address
	start.Start.StartupReadinessDeadline = startupDeadline
	start.Start.ReadinessProbeInterval = 20 * time.Millisecond
	start.Start.ReadinessConnectTimeout = 100 * time.Millisecond
	encoder := json.NewEncoder(agentEndpoint)
	if err := encoder.Encode(start); err != nil {
		t.Fatal(err)
	}
	decoder := json.NewDecoder(agentEndpoint)
	started, err := decodeGuardianStatus(decoder)
	if err != nil || started.Type != guardianMessageStarted {
		t.Fatalf("first status = (%#v, %v), want started", started, err)
	}
	statuses := make(chan guardianStatusMessage, 8)
	go func() {
		defer close(statuses)
		for {
			status, err := decodeGuardianStatus(decoder)
			if err != nil {
				return
			}
			statuses <- status
		}
	}()
	t.Cleanup(func() {
		_ = agentEndpoint.Close()
		if err := <-done; err != nil {
			t.Errorf("serveGuardian() error = %v", err)
		}
		_ = guardianEndpoint.Close()
	})
	return inProcessGuardian{statuses: statuses, pid: started.PID, encoder: encoder}
}

func (guardian inProcessGuardian) next(t *testing.T) guardianStatusMessage {
	t.Helper()
	select {
	case status, open := <-guardian.statuses:
		if !open {
			t.Fatal("guardian status stream ended")
		}
		return status
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for a guardian status")
		return guardianStatusMessage{}
	}
}

func (guardian inProcessGuardian) expectReadiness(t *testing.T, want bool) {
	t.Helper()
	if status := guardian.next(t); status.Type != guardianMessageReadiness || status.Ready == nil || *status.Ready != want {
		t.Fatalf("status = %#v, want readiness %t", status, want)
	}
}

func (guardian inProcessGuardian) expectPortTaken(t *testing.T, reason string) {
	t.Helper()
	exited := guardian.next(t)
	if exited.Type != guardianMessageExited || exited.Result.SpawnError == nil ||
		exited.Result.SpawnError.Code != contract.SpawnFailureServicePortTaken ||
		!strings.Contains(exited.Result.SpawnError.Message, reason) {
		t.Fatalf("status = %#v, want service_port_taken naming %q", exited, reason)
	}
}

// stopWithin asks the Guardian to stop and requires its exit report within
// bound. The listen payload honours TERM at once, so bound measures only
// how long the Guardian took to begin termination.
func (guardian inProcessGuardian) stopWithin(t *testing.T, bound time.Duration) {
	t.Helper()
	began := time.Now()
	if err := guardian.encoder.Encode(guardianControlMessage{Type: guardianMessageStop}); err != nil {
		t.Fatal(err)
	}
	exited := guardian.next(t)
	if exited.Type != guardianMessageExited {
		t.Fatalf("status after stop = %#v, want exited", exited)
	}
	if elapsed := time.Since(began); elapsed > bound {
		t.Fatalf("stop took %s, want under %s", elapsed, bound)
	}
}

func assertGuardianPublishes(t *testing.T, execution contract.ExecutionSpec, address string) {
	t.Helper()
	readiness := make(chan bool, 8)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	finished := runServiceAsync(ctx, serviceTestRunner(20*time.Second), Request{
		AttemptID: "published-service", Guarded: true,
		Execution: execution, IdlePolicy: IgnoreIdle, ServiceAddress: address,
		ReadinessChanged: func(_, ready bool) { readiness <- ready },
	})
	select {
	case ready := <-readiness:
		if !ready {
			t.Fatal("first readiness transition was not ready")
		}
	case outcome := <-finished:
		t.Fatalf("service ended before it was published: (%#v, %v)", outcome.result, outcome.err)
	case <-time.After(15 * time.Second):
		t.Fatal("timed out waiting for publication")
	}
	cancel()
	_ = awaitRun(t, finished)
}

func runServiceAsync(ctx context.Context, runner *Runner, request Request) <-chan runOutcome {
	finished := make(chan runOutcome, 1)
	go func() {
		result, err := runner.Run(ctx, request, nil)
		finished <- runOutcome{result: result, err: err}
	}()
	return finished
}

func mustServiceBackend(t *testing.T, address string) serviceBackend {
	t.Helper()
	backend, err := parseServiceBackend(address)
	if err != nil {
		t.Fatal(err)
	}
	return backend
}

// listenIPv6Loopback holds [::1]:port, which cannot receive connections for
// 127.0.0.1:port.
func listenIPv6Loopback(t *testing.T, port int) net.Listener {
	t.Helper()
	listener, err := net.Listen("tcp6", net.JoinHostPort("::1", strconv.Itoa(port)))
	if err != nil {
		t.Skipf("IPv6 loopback is unavailable here: %v", err)
	}
	go serveAndCloseForTest(listener)
	return listener
}

// listenReusePort adds an SO_REUSEPORT listener beside the payload's.
func listenReusePort(t *testing.T, address string) net.Listener {
	t.Helper()
	config := net.ListenConfig{Control: func(_, _ string, connection syscall.RawConn) error {
		var optionErr error
		if err := connection.Control(func(descriptor uintptr) {
			optionErr = unix.SetsockoptInt(int(descriptor), unix.SOL_SOCKET, unix.SO_REUSEPORT, 1)
		}); err != nil {
			return err
		}
		return optionErr
	}}
	listener, err := config.Listen(context.Background(), "tcp4", address)
	if err != nil {
		t.Fatal(err)
	}
	go serveAndCloseForTest(listener)
	return listener
}

func serveAndCloseForTest(listener net.Listener) {
	for {
		connection, err := listener.Accept()
		if err != nil {
			return
		}
		_ = connection.Close()
	}
}

func TestGuardianNeverPublishesAForeignListenerOnTheServicePort(t *testing.T) {
	address := releasedServiceAddress(t)
	foreign, err := net.Listen("tcp4", address)
	if err != nil {
		t.Fatal(err)
	}
	defer foreign.Close()
	accepted := make(chan struct{}, 64)
	go func() {
		for {
			connection, err := foreign.Accept()
			if err != nil {
				return
			}
			accepted <- struct{}{}
			_ = connection.Close()
		}
	}()
	published := make(chan bool, 4)
	began := time.Now()
	// The payload never binds, so only the foreign listener can answer.
	result, err := serviceTestRunner(20*time.Second).Run(context.Background(), Request{
		AttemptID: "foreign-service-listener", Guarded: true,
		Execution: helperExecution("hang"), IdlePolicy: IgnoreIdle, ServiceAddress: address,
		ReadinessChanged: func(_, ready bool) { published <- ready },
	}, nil)
	if err != nil {
		t.Fatalf("foreign listener returned supervision error: %v", err)
	}
	if result.SpawnError == nil || result.SpawnError.Code != contract.SpawnFailureServicePortTaken || result.ExitCode != nil || result.Signal != "" {
		t.Fatalf("foreign listener result = %#v, want sole service_port_taken", result)
	}
	if elapsed := time.Since(began); elapsed > 15*time.Second {
		t.Fatalf("foreign listener was refused after %s, want before the startup deadline", elapsed)
	}
	select {
	case ready := <-published:
		t.Fatalf("guardian reported readiness %t for a listener outside the payload group", ready)
	default:
	}
	select {
	case <-accepted:
	default:
		t.Fatal("the foreign listener never answered a probe; the test did not reach the ownership check")
	}
}

func TestGuardianTypesALostPayloadBindAsServicePortTaken(t *testing.T) {
	address := releasedServiceAddress(t)
	port := mustServiceBackend(t, address).port
	// A bound socket that does not listen: it blocks the payload's bind but
	// answers no probe, so only the payload's exit can report the loss.
	holder, err := syscall.Socket(syscall.AF_INET, syscall.SOCK_STREAM, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer syscall.Close(holder)
	if err := syscall.Bind(holder, &syscall.SockaddrInet4{Port: port, Addr: [4]byte{127, 0, 0, 1}}); err != nil {
		t.Fatal(err)
	}
	result, err := serviceTestRunner(20*time.Second).Run(context.Background(), Request{
		AttemptID: "lost-service-bind", Guarded: true,
		Execution: helperExecution("listen", address), IdlePolicy: IgnoreIdle, ServiceAddress: address,
		ReadinessChanged: func(_, ready bool) { t.Errorf("guardian reported readiness %t without a payload listener", ready) },
	}, nil)
	if err != nil {
		t.Fatalf("lost bind returned supervision error: %v", err)
	}
	if result.SpawnError == nil || result.SpawnError.Code != contract.SpawnFailureServicePortTaken || result.ExitCode != nil {
		t.Fatalf("lost bind result = %#v, want service_port_taken", result)
	}
}

func TestGuardianPublishesOnlyThePayloadsOwnListenerAfterStartup(t *testing.T) {
	injectInspector(t)
	address := releasedServiceAddress(t)
	guardian := runInProcessServiceGuardian(t, "listen-toggle", address, 30*time.Second)
	guardian.expectReadiness(t, true)
	if err := syscall.Kill(guardian.pid, syscall.SIGUSR1); err != nil {
		t.Fatal(err)
	}
	guardian.expectReadiness(t, false)

	// Another process takes the port while the payload is not listening.
	// Probes now succeed, yet nothing is published and the payload lives.
	foreign, err := net.Listen("tcp4", address)
	if err != nil {
		t.Fatal(err)
	}
	go serveAndCloseForTest(foreign)
	select {
	case status := <-guardian.statuses:
		t.Fatalf("guardian reported %#v for a listener outside the payload group", status)
	case <-time.After(time.Second):
	}
	_ = foreign.Close()

	waitServiceAddressBindable(t, address)

	if err := syscall.Kill(guardian.pid, syscall.SIGUSR2); err != nil {
		t.Fatal(err)
	}
	guardian.expectReadiness(t, true)
	guardian.stopWithin(t, 5*time.Second)
}

// waitServiceAddressBindable waits for the foreign listener's final release.
// Match the payload's SO_REUSEADDR bind without listening: a listening check
// could accept readiness probes and delay the next release on macOS again.
// The check socket is close-on-exec, set under ForkLock as net does, so an
// lsof started by the in-process Guardian cannot inherit it and keep the
// port bound after this check closes it.
func waitServiceAddressBindable(t *testing.T, address string) {
	t.Helper()
	syscall.ForkLock.RLock()
	descriptor, err := syscall.Socket(syscall.AF_INET, syscall.SOCK_STREAM, 0)
	if err == nil {
		syscall.CloseOnExec(descriptor)
	}
	syscall.ForkLock.RUnlock()
	if err != nil {
		t.Fatal(err)
	}
	defer syscall.Close(descriptor)
	if err := syscall.SetsockoptInt(descriptor, syscall.SOL_SOCKET, syscall.SO_REUSEADDR, 1); err != nil {
		t.Fatal(err)
	}
	backend := mustServiceBackend(t, address)
	socketAddress := &syscall.SockaddrInet4{Port: backend.port}
	copy(socketAddress.Addr[:], backend.ip.To4())
	deadline := time.Now().Add(5 * time.Second)
	for {
		err := syscall.Bind(descriptor, socketAddress)
		if err == nil {
			return
		}
		if !errors.Is(err, syscall.EADDRINUSE) {
			t.Fatalf("bind %s: %v", address, err)
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting to bind %s: %v", address, err)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func serviceTestRunner(startupDeadline time.Duration) *Runner {
	return New(Config{
		GuardianExecutable: guardianAgentPath, TerminationGraceTime: 100 * time.Millisecond,
		StartupReadinessDeadline: startupDeadline,
		ReadinessProbeInterval:   20 * time.Millisecond, ReadinessConnectTimeout: 100 * time.Millisecond,
	})
}

// releasedServiceAddress picks a backend address the way the agent does:
// bind port 0, read the number, release it.
func releasedServiceAddress(t *testing.T) string {
	t.Helper()
	reservation, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := reservation.Addr().String()
	if err := reservation.Close(); err != nil {
		t.Fatal(err)
	}
	return address
}

func waitGuardianReadiness(t *testing.T, readiness <-chan bool) bool {
	t.Helper()
	select {
	case ready := <-readiness:
		return ready
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for guardian readiness transition")
		return false
	}
}
