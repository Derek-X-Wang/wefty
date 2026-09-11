package lima

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/runner/ocihelper"
)

func TestSupervisorStartsStoppedInstanceOnlyWhileEnabled(t *testing.T) {
	intent := newMutableIntent(false)
	runner := &supervisorRunner{states: []InstanceState{InstanceStopped, InstanceStopped, InstanceRunning}}
	supervisor := newTestSupervisor(t, intent, runner)
	if err := supervisor.Ensure(t.Context()); err == nil {
		t.Fatal("disabled supervisor returned no error")
	}
	if got := runner.commandsSnapshot(); !reflect.DeepEqual(got, [][]string{{"limactl", "list", "--json", DefaultInstanceName}}) {
		t.Fatalf("disabled supervisor ran a lifecycle command: %v", got)
	}
	intent.set(true)
	if err := supervisor.Ensure(t.Context()); err != nil {
		t.Fatal(err)
	}
	want := [][]string{
		{"limactl", "list", "--json", DefaultInstanceName},
		{"limactl", "list", "--json", DefaultInstanceName},
		{"limactl", "start", DefaultInstanceName},
		{"limactl", "list", "--json", DefaultInstanceName},
	}
	if got := runner.commandsSnapshot(); !reflect.DeepEqual(got, want) {
		t.Fatalf("commands = %v, want %v", got, want)
	}
	if facts := supervisor.Facts(); facts.State != InstanceRunning || facts.ReasonCode != "" || facts.Recovering {
		t.Fatalf("running facts = %+v", facts)
	}
}

func TestSupervisorDisableDuringStartWinsAndLeavesLimaStopped(t *testing.T) {
	intent := newMutableIntent(true)
	runner := &supervisorRunner{states: []InstanceState{InstanceStopped}}
	runner.afterCommand = func(arguments []string) {
		if len(arguments) > 0 && arguments[0] == "start" {
			intent.set(false)
		}
	}
	supervisor := newTestSupervisor(t, intent, runner)
	if err := supervisor.Ensure(t.Context()); err == nil || !strings.Contains(err.Error(), "disabled") {
		t.Fatalf("start after disable = %v", err)
	}
	wantTail := [][]string{
		{"limactl", "start", DefaultInstanceName},
		{"limactl", "stop", "--force", DefaultInstanceName},
	}
	commands := runner.commandsSnapshot()
	if !reflect.DeepEqual(commands[len(commands)-2:], wantTail) {
		t.Fatalf("disable did not cancel start: %v", commands)
	}
	if facts := supervisor.Facts(); facts.State != InstanceStopped || facts.Enabled || facts.ReasonCode != contract.CapabilityReasonOCIIntentDisabled {
		t.Fatalf("post-disable facts = %+v", facts)
	}
}

func TestSupervisorDisableDuringBrokenForceStopPreventsRestart(t *testing.T) {
	intent := newMutableIntent(true)
	runner := &supervisorRunner{states: []InstanceState{InstanceBroken}}
	runner.afterCommand = func(arguments []string) {
		if len(arguments) > 0 && arguments[0] == "stop" {
			intent.set(false)
		}
	}
	supervisor := newTestSupervisor(t, intent, runner)
	if err := supervisor.Ensure(t.Context()); err == nil {
		t.Fatal("Broken recovery ignored disable")
	}
	for _, command := range runner.commandsSnapshot() {
		if len(command) > 1 && command[1] == "start" {
			t.Fatalf("disable after stop restarted Lima: %v", runner.commandsSnapshot())
		}
	}
	if facts := supervisor.Facts(); facts.State != InstanceStopped || facts.Enabled {
		t.Fatalf("post-disable facts = %+v", facts)
	}
}

func TestSupervisorBrokenRecoveryBackoffIsCapped(t *testing.T) {
	intent := newMutableIntent(true)
	runner := &supervisorRunner{
		states: []InstanceState{InstanceBroken, InstanceRunning},
		startErrors: []error{
			errors.New("1"), errors.New("2"), errors.New("3"), errors.New("4"),
			errors.New("5"), errors.New("6"), errors.New("7"), nil,
		},
	}
	supervisor := newTestSupervisor(t, intent, runner)
	var waits []time.Duration
	supervisor.config.wait = func(_ context.Context, delay time.Duration) error {
		waits = append(waits, delay)
		return nil
	}
	if err := supervisor.Ensure(t.Context()); err != nil {
		t.Fatal(err)
	}
	want := []time.Duration{time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second, 16 * time.Second, 30 * time.Second, 30 * time.Second}
	if !reflect.DeepEqual(waits, want) {
		t.Fatalf("backoff = %v, want %v", waits, want)
	}
}

func TestSupervisorStartTimeoutUsesInjectedCommandResult(t *testing.T) {
	intent := newMutableIntent(true)
	runner := &supervisorRunner{states: []InstanceState{InstanceStopped}, startErrors: []error{context.DeadlineExceeded}}
	supervisor := newTestSupervisor(t, intent, runner)
	var deadlines []time.Duration
	supervisor.config.withTimeout = func(ctx context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
		deadlines = append(deadlines, timeout)
		return context.WithCancel(ctx)
	}
	if err := supervisor.Ensure(t.Context()); err == nil {
		t.Fatal("timed-out start returned no error")
	}
	if facts := supervisor.Facts(); facts.ReasonCode != contract.CapabilityReasonLimaStartTimeout {
		t.Fatalf("timeout facts = %+v", facts)
	}
	if want := []time.Duration{defaultLimaRecoveryTimeout, defaultLimaCommandTimeout, defaultLimaCommandTimeout}; !reflect.DeepEqual(deadlines, want) {
		t.Fatalf("injected deadlines = %v, want %v", deadlines, want)
	}
}

func TestRunningLimaWithUnreadyHelperIsBoundedAndForceStoppedOnce(t *testing.T) {
	intent := newMutableIntent(true)
	runner := &supervisorRunner{states: []InstanceState{InstanceRunning}}
	supervisor := newTestSupervisor(t, intent, runner)
	var waits []time.Duration
	supervisor.config.wait = func(_ context.Context, delay time.Duration) error {
		waits = append(waits, delay)
		return context.DeadlineExceeded
	}
	client := &ocihelper.Client{
		Version: ocihelper.ProtocolVersion, ExpectedChecksum: "sha256:" + strings.Repeat("a", 64),
		Dial: func(context.Context) (net.Conn, error) { return nil, errors.New("helper unavailable") },
	}
	helperBarrier, err := ocihelper.NewBootBarrier(client, ocihelper.AcquireSessionRequest{NodeID: "node", BootSessionID: "boot"})
	if err != nil {
		t.Fatal(err)
	}
	barrier := &SupervisedBootBarrier{Supervisor: supervisor, Barrier: helperBarrier}
	if err := barrier.Ensure(t.Context()); err == nil {
		t.Fatal("unready helper returned success")
	}
	commands := runner.commandsSnapshot()
	stops := 0
	for _, command := range commands {
		if reflect.DeepEqual(command, []string{"limactl", "stop", "--force", DefaultInstanceName}) {
			stops++
		}
	}
	if stops != 1 || !reflect.DeepEqual(waits, []time.Duration{time.Second}) {
		t.Fatalf("stuck helper recovery commands=%v waits=%v", commands, waits)
	}
	if facts := supervisor.Facts(); facts.State != InstanceStopped || facts.ReasonCode != contract.CapabilityReasonLimaStartTimeout {
		t.Fatalf("stuck helper facts = %+v", facts)
	}
}

type readyLimaHelperEngine struct{ ocihelper.UnavailableEngine }

type residueLimaHelperEngine struct {
	ocihelper.UnavailableEngine
	mu          sync.Mutex
	verifyCalls int
}

type helperServeObservation struct {
	done chan struct{}
	err  error
}

func (observation *helperServeObservation) snapshot() (bool, error) {
	select {
	case <-observation.done:
		return true, observation.err
	default:
		return false, nil
	}
}

func (readyLimaHelperEngine) Sweep(_ context.Context, request ocihelper.SweepRequest) (ocihelper.SweepResponse, error) {
	return ocihelper.SweepResponse{SweepEpoch: request.SweepEpoch}, nil
}

func (readyLimaHelperEngine) Verify(context.Context, ocihelper.VerifyRequest) (ocihelper.VerifyResponse, error) {
	return ocihelper.VerifyResponse{Absent: true}, nil
}

func (engine *residueLimaHelperEngine) Sweep(_ context.Context, request ocihelper.SweepRequest) (ocihelper.SweepResponse, error) {
	return ocihelper.SweepResponse{SweepEpoch: request.SweepEpoch}, nil
}

func (engine *residueLimaHelperEngine) Verify(context.Context, ocihelper.VerifyRequest) (ocihelper.VerifyResponse, error) {
	engine.mu.Lock()
	defer engine.mu.Unlock()
	engine.verifyCalls++
	if engine.verifyCalls == 1 {
		return ocihelper.VerifyResponse{Absent: true}, nil
	}
	const sentinel = "wefty-cgroup-handshake-private-sentinel"
	residue := ocihelper.ResourceInventory{Cgroups: []string{sentinel}}
	return ocihelper.VerifyResponse{Inventory: residue, RuntimeResidue: residue}, nil
}

func startReadyLimaHelper(t *testing.T, checksum string) (string, *helperServeObservation) {
	return startLimaHelper(t, checksum, readyLimaHelperEngine{})
}

func startLimaHelper(t *testing.T, checksum string, engine ocihelper.Engine) (string, *helperServeObservation) {
	t.Helper()
	temporary, err := os.CreateTemp("/tmp", "wefty-lima-helper-*.sock")
	if err != nil {
		t.Fatal(err)
	}
	path := temporary.Name()
	if err := temporary.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	server, err := ocihelper.NewServer(engine, ocihelper.ServerConfig{
		HelperVersion: "test", HelperChecksum: checksum, AllowedUIDs: []uint32{uint32(os.Getuid())},
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	observation := &helperServeObservation{done: make(chan struct{})}
	go func() {
		observation.err = server.Serve(ctx, listener)
		close(observation.done)
	}()
	t.Cleanup(func() {
		cancel()
		_ = listener.Close()
		<-observation.done
		_ = os.Remove(path)
	})
	return path, observation
}

func TestSupervisedBarrierReadinessDiagnosticDoesNotExposeResidueInventory(t *testing.T) {
	const sentinel = "wefty-cgroup-handshake-private-sentinel"
	intent := newMutableIntent(true)
	runner := &supervisorRunner{states: []InstanceState{InstanceRunning}}
	supervisor := newTestSupervisor(t, intent, runner)
	var logMu sync.Mutex
	var logs []string
	supervisor.config.Logf = func(format string, arguments ...any) {
		logMu.Lock()
		defer logMu.Unlock()
		logs = append(logs, fmt.Sprintf(format, arguments...))
	}
	supervisor.config.wait = func(context.Context, time.Duration) error {
		return errors.New("stop after the first readiness diagnostic")
	}
	checksum := "sha256:" + strings.Repeat("a", 64)
	socketPath, _ := startLimaHelper(t, checksum, &residueLimaHelperEngine{})
	client := &ocihelper.Client{
		Version: ocihelper.ProtocolVersion, ExpectedChecksum: checksum,
		Dial: func(ctx context.Context) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "unix", socketPath)
		},
	}
	helperBarrier, err := ocihelper.NewBootBarrierWithConfig(client, ocihelper.AcquireSessionRequest{NodeID: "node", BootSessionID: "boot"}, ocihelper.BootBarrierConfig{
		TakeoverTimeout: 25 * time.Millisecond,
		TakeoverRetry:   time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = helperBarrier.Close() })
	barrier := &SupervisedBootBarrier{Supervisor: supervisor, Barrier: helperBarrier}
	if err := barrier.Ensure(t.Context()); err == nil {
		t.Fatal("residue readiness attempt unexpectedly succeeded")
	}
	logMu.Lock()
	defer logMu.Unlock()
	if len(logs) != 1 {
		t.Fatalf("readiness diagnostic count = %d, want 1: %q", len(logs), logs)
	}
	if strings.Contains(logs[0], sentinel) {
		t.Fatalf("readiness diagnostic exposed runtime inventory: %q", logs[0])
	}
	for _, field := range []string{
		"reason=helper_handshake_failed",
		"inner_reason=boot_sweep_failed",
		"error_type=*ocihelper.NamespaceResidueError",
		"canceled=false",
		"deadline=false",
		"eof=false",
		"timeout=false",
		"connection_refused=false",
	} {
		if !strings.Contains(logs[0], field) {
			t.Errorf("readiness diagnostic missing %q: %q", field, logs[0])
		}
	}
}

func TestSupervisedBarrierRepairsHelperUnitUnavailableAndReearnsOCI(t *testing.T) {
	for _, test := range []struct {
		name   string
		silent bool
	}{
		{name: "unit_unavailable"},
		{name: "unit_unavailable_then_handshake_stalled", silent: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			intent := newMutableIntent(true)
			runner := &supervisorRunner{states: []InstanceState{InstanceRunning}}
			supervisor := newTestSupervisor(t, intent, runner)
			supervisor.config.Logf = t.Logf
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			var recoveryContext context.Context
			var recoveryDeadline time.Time
			recoveryContexts := 0
			withTimeout := supervisor.config.withTimeout
			supervisor.config.withTimeout = func(parent context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
				bounded, stop := withTimeout(parent, timeout)
				if timeout == defaultLimaRecoveryTimeout {
					recoveryContexts++
					recoveryContext = bounded
					recoveryDeadline, _ = bounded.Deadline()
				}
				return bounded, stop
			}
			checksum := "sha256:" + strings.Repeat("a", 64)
			var socketPath string
			t.Cleanup(func() {
				_, err := os.Stat(socketPath)
				absent := errors.Is(err, os.ErrNotExist)
				if !absent {
					t.Errorf("helper socket retained after cleanup: %v", err)
				}
				t.Logf("repair-phase cleanup socket=%s absent=%t", socketPath, absent)
			})
			var helperServe *helperServeObservation
			socketPath, helperServe = startReadyLimaHelper(t, checksum)
			type silentPeer struct {
				connection net.Conn
				done       chan struct{}
				deadline   time.Time
				closedAt   time.Time
				cause      error
			}
			var peers []*silentPeer
			var helperBarrier *ocihelper.BootBarrier
			t.Cleanup(func() {
				serveCompleted, serveErr := helperServe.snapshot()
				cancel()
				if helperBarrier != nil {
					_ = helperBarrier.Close()
				}
				for _, peer := range peers {
					_ = peer.connection.Close()
					<-peer.done
				}
				t.Logf("repair-phase cleanup joined_peers=%d barrier_closed=true helper_serve_completed_before_cleanup=%t helper_serve_error_type=%T helper_serve_error=%v", len(peers), serveCompleted, serveErr, serveErr)
			})
			phase := "unavailable"
			waits, unavailableDials, socketDials := 0, 0, 0
			unavailableTransitions, stalledTransitions := 0, 0
			client := &ocihelper.Client{
				Version: ocihelper.ProtocolVersion, ExpectedChecksum: checksum,
				Dial: func(dialContext context.Context) (net.Conn, error) {
					switch phase {
					case "unavailable":
						unavailableDials++
						return nil, syscall.ECONNREFUSED
					case "silent":
						clientSide, serverSide := net.Pipe()
						peer := &silentPeer{connection: serverSide, done: make(chan struct{})}
						peer.deadline, _ = dialContext.Deadline()
						peers = append(peers, peer)
						go func() {
							<-dialContext.Done()
							peer.closedAt, peer.cause = time.Now(), context.Cause(dialContext)
							_ = serverSide.Close()
							close(peer.done)
						}()
						return clientSide, nil
					default:
						socketDials++
						return (&net.Dialer{}).DialContext(dialContext, "unix", socketPath)
					}
				},
			}
			var err error
			helperBarrier, err = ocihelper.NewBootBarrierWithConfig(client, ocihelper.AcquireSessionRequest{NodeID: "node", BootSessionID: "boot"}, ocihelper.BootBarrierConfig{
				TakeoverTimeout: 25 * time.Millisecond,
				TakeoverRetry:   time.Millisecond,
			})
			if err != nil {
				t.Fatal(err)
			}
			barrier := &SupervisedBootBarrier{Supervisor: supervisor, Barrier: helperBarrier}
			supervisor.config.wait = func(waitContext context.Context, delay time.Duration) error {
				waits++
				reason := helperBarrier.CapabilityReasonCode()
				helperServeCompleted, helperServeErr := helperServe.snapshot()
				deadline, hasDeadline := waitContext.Deadline()
				wantBackoff := []time.Duration{time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second, 16 * time.Second, 30 * time.Second}
				if waitContext != recoveryContext || !hasDeadline || !deadline.Equal(recoveryDeadline) || waitContext.Err() != nil {
					t.Fatalf("repair changed recovery authority: same_context=%t deadline=%s want=%s err=%v", waitContext == recoveryContext, deadline, recoveryDeadline, waitContext.Err())
				}
				if delay != wantBackoff[min(waits-1, len(wantBackoff)-1)] || helperBarrier.Ready() {
					t.Fatalf("repair wait=%d delay=%s ready=%t", waits, delay, helperBarrier.Ready())
				}
				t.Logf("repair-phase at=%s pid=%d pgid=%d phase=%s wait=%d reason=%s backoff=%s deadline=%s unavailable_dials=%d silent_peers=%d socket_dials=%d helper_serve_completed=%t helper_serve_error_type=%T helper_serve_error=%v", time.Now().UTC().Format(time.RFC3339Nano), os.Getpid(), syscall.Getpgrp(), phase, waits, reason, delay, deadline.Format(time.RFC3339Nano), unavailableDials, len(peers), socketDials, helperServeCompleted, helperServeErr, helperServeErr)
				switch phase {
				case "unavailable":
					if reason != contract.CapabilityReasonHelperUnitUnavailable || unavailableDials == 0 || socketDials != 0 || len(peers) != 0 {
						t.Fatalf("invalid initial unavailable phase: reason=%s dials=%d socket_dials=%d peers=%d", reason, unavailableDials, socketDials, len(peers))
					}
					unavailableTransitions++
					phase = "socket"
					if test.silent {
						phase = "silent"
					}
				case "silent":
					if reason != contract.CapabilityReasonHelperHandshakeStalled || len(peers) == 0 || socketDials != 0 || supervisor.Facts().StalledWindows == 0 {
						t.Fatalf("invalid controlled stall: reason=%s peers=%d socket_dials=%d facts=%+v", reason, len(peers), socketDials, supervisor.Facts())
					}
					for _, peer := range peers {
						select {
						case <-peer.done:
						case <-waitContext.Done():
							t.Fatal("silent peer did not join under original recovery authority")
						}
						if peer.deadline.IsZero() || peer.closedAt.Before(peer.deadline) || peer.cause == nil {
							t.Fatalf("silent peer closed before its original deadline: closed=%s deadline=%s cause=%v", peer.closedAt, peer.deadline, peer.cause)
						}
						t.Logf("repair-phase silent_peer_joined deadline=%s closed=%s cause=%v", peer.deadline.Format(time.RFC3339Nano), peer.closedAt.Format(time.RFC3339Nano), peer.cause)
					}
					stalledTransitions++
					phase = "socket"
				case "socket":
					// A real helper may need another bounded takeover window.
					// Only positively classified retryable outcomes belong here.
					if reason != contract.CapabilityReasonHelperUnitUnavailable && reason != contract.CapabilityReasonHelperHandshakeStalled {
						t.Fatalf("unexplained real-socket retry: reason=%s", reason)
					}
				}
				return nil
			}
			err = barrier.Ensure(ctx)
			receipt, receiptOK := barrier.SweepReceipt()
			if err != nil || !barrier.Ready() || barrier.CapabilityReasonCode() != "" || !receiptOK || !receipt.VerifiedAbsent || receipt.SweepEpoch == "" {
				t.Fatalf("invalid recovery: err=%v ready=%t reason=%q receipt_ok=%t receipt=%+v", err, barrier.Ready(), barrier.CapabilityReasonCode(), receiptOK, receipt)
			}
			if recoveryContexts != 1 || unavailableTransitions != 1 || (test.silent && stalledTransitions != 1) || (!test.silent && stalledTransitions != 0) || socketDials == 0 {
				t.Fatalf("invalid repair phases: contexts=%d unavailable=%d stalled=%d socket_dials=%d", recoveryContexts, unavailableTransitions, stalledTransitions, socketDials)
			}
			if supervisor.Facts().StalledWindows != 0 || helperBarrier.HandshakeStalledWindows() != 0 || helperBarrier.CapabilityReasonCode() != "" {
				t.Fatalf("successful repair retained failure facts: supervisor=%+v helper_stalls=%d reason=%s", supervisor.Facts(), helperBarrier.HandshakeStalledWindows(), helperBarrier.CapabilityReasonCode())
			}
			wantCommands := [][]string{{"limactl", "list", "--json", DefaultInstanceName}}
			if got := runner.commandsSnapshot(); !reflect.DeepEqual(got, wantCommands) {
				t.Fatalf("repair mutated running Lima: commands=%v", got)
			}
			t.Logf("repair-phase verified_ready=true unavailable_transitions=%d stalled_transitions=%d waits=%d epoch=%s", unavailableTransitions, stalledTransitions, waits, receipt.SweepEpoch)
		})
	}
}

func TestSupervisedBarrierPersistentStalledHandshakeForceStopsAtRecoveryTimeout(t *testing.T) {
	intent := newMutableIntent(true)
	runner := &supervisorRunner{states: []InstanceState{InstanceRunning, InstanceRunning}}
	supervisor := newTestSupervisor(t, intent, runner)
	supervisor.config.RecoveryTimeout = 100 * time.Millisecond
	supervisor.config.wait = func(ctx context.Context, _ time.Duration) error {
		<-ctx.Done()
		return ctx.Err()
	}
	client := &ocihelper.Client{Version: ocihelper.ProtocolVersion, ExpectedChecksum: "sha256:" + strings.Repeat("a", 64),
		Dial: func(ctx context.Context) (net.Conn, error) {
			clientSide, serverSide := net.Pipe()
			go func() { <-ctx.Done(); _ = serverSide.Close() }()
			return clientSide, nil
		}}
	helperBarrier, err := ocihelper.NewBootBarrierWithConfig(client, ocihelper.AcquireSessionRequest{NodeID: "node", BootSessionID: "boot"}, ocihelper.BootBarrierConfig{
		TakeoverTimeout: 20 * time.Millisecond, TakeoverRetry: time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	barrier := &SupervisedBootBarrier{Supervisor: supervisor, Barrier: helperBarrier}
	err = barrier.Ensure(t.Context())
	var stalled *ocihelper.HelperHandshakeStalledError
	if !errors.As(err, &stalled) || barrier.CapabilityReasonCode() != contract.CapabilityReasonHelperHandshakeStalledPersistent || supervisor.Facts().StalledWindows == 0 {
		t.Fatalf("stalled handshake err=%v reason=%q", err, barrier.CapabilityReasonCode())
	}
	stopped := false
	for _, command := range runner.commandsSnapshot() {
		if slices.Contains(command, "stop") {
			stopped = true
		}
	}
	if !stopped {
		t.Fatalf("persistent stalled handshake did not force-stop Lima: %v", runner.commandsSnapshot())
	}
}

func TestSupervisedBarrierStalledHandshakeRecoversBeforeRecoveryTimeout(t *testing.T) {
	intent := newMutableIntent(true)
	runner := &supervisorRunner{states: []InstanceState{InstanceRunning}}
	supervisor := newTestSupervisor(t, intent, runner)
	ready := false
	checksum := "sha256:" + strings.Repeat("a", 64)
	socketPath, _ := startReadyLimaHelper(t, checksum)
	supervisor.config.wait = func(context.Context, time.Duration) error { ready = true; return nil }
	client := &ocihelper.Client{Version: ocihelper.ProtocolVersion, ExpectedChecksum: checksum, Dial: func(ctx context.Context) (net.Conn, error) {
		if ready {
			return (&net.Dialer{}).DialContext(ctx, "unix", socketPath)
		}
		clientSide, serverSide := net.Pipe()
		go func() { <-ctx.Done(); _ = serverSide.Close() }()
		return clientSide, nil
	}}
	helperBarrier, err := ocihelper.NewBootBarrierWithConfig(client, ocihelper.AcquireSessionRequest{NodeID: "node", BootSessionID: "boot"}, ocihelper.BootBarrierConfig{TakeoverTimeout: 20 * time.Millisecond, TakeoverRetry: time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	barrier := &SupervisedBootBarrier{Supervisor: supervisor, Barrier: helperBarrier}
	if err := barrier.Ensure(t.Context()); err != nil || !barrier.Ready() || supervisor.Facts().StalledWindows != 0 {
		t.Fatalf("stalled recovery err=%v ready=%t facts=%+v", err, barrier.Ready(), supervisor.Facts())
	}
}

func TestSupervisedBarrierHoldsCycleLockAcrossLimaAndHelperBarrier(t *testing.T) {
	intent := newMutableIntent(true)
	runner := &supervisorRunner{states: []InstanceState{InstanceRunning}}
	supervisor := newTestSupervisor(t, intent, runner)
	entered := make(chan struct{})
	var enteredOnce sync.Once
	supervisor.config.wait = func(ctx context.Context, _ time.Duration) error { return ctx.Err() }
	client := &ocihelper.Client{
		Version: ocihelper.ProtocolVersion, ExpectedChecksum: "sha256:" + strings.Repeat("a", 64),
		Dial: func(ctx context.Context) (net.Conn, error) {
			enteredOnce.Do(func() { close(entered) })
			<-ctx.Done()
			return nil, ctx.Err()
		},
	}
	helperBarrier, err := ocihelper.NewBootBarrier(client, ocihelper.AcquireSessionRequest{NodeID: "node", BootSessionID: "boot"})
	if err != nil {
		t.Fatal(err)
	}
	barrier := &SupervisedBootBarrier{Supervisor: supervisor, Barrier: helperBarrier}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- barrier.Ensure(ctx) }()
	<-entered
	if barrier.cycleMu.TryLock() {
		barrier.cycleMu.Unlock()
		t.Fatal("cycle lock was released during helper barrier")
	}
	for _, command := range runner.commandsSnapshot() {
		if len(command) > 1 && command[1] == "stop" {
			t.Fatalf("Lima was force-stopped mid-barrier: %v", runner.commandsSnapshot())
		}
	}
	cancel()
	if err := <-done; err == nil {
		t.Fatal("canceled helper barrier returned success")
	}
}

func TestSupervisedBarrierCycleAcquisitionHonorsContext(t *testing.T) {
	intent := newMutableIntent(true)
	supervisor := newTestSupervisor(t, intent, &supervisorRunner{states: []InstanceState{InstanceRunning}})
	client := &ocihelper.Client{
		Version: ocihelper.ProtocolVersion, ExpectedChecksum: "sha256:" + strings.Repeat("a", 64),
		Dial: func(context.Context) (net.Conn, error) { return nil, errors.New("not reached") },
	}
	helperBarrier, err := ocihelper.NewBootBarrier(client, ocihelper.AcquireSessionRequest{NodeID: "node", BootSessionID: "boot"})
	if err != nil {
		t.Fatal(err)
	}
	barrier := &SupervisedBootBarrier{Supervisor: supervisor, Barrier: helperBarrier}
	barrier.cycleMu.Lock()
	defer barrier.cycleMu.Unlock()
	ctx, cancel := context.WithTimeout(t.Context(), 25*time.Millisecond)
	defer cancel()
	if err := barrier.Ensure(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("contended Ensure error=%v", err)
	}
}

func TestSupervisedBarrierRecreateDeletesAndStartsFromExplicitTemplate(t *testing.T) {
	intent := newMutableIntent(true)
	runner := &supervisorRunner{}
	supervisor := newTestSupervisor(t, intent, runner)
	client := &ocihelper.Client{
		Version: ocihelper.ProtocolVersion, ExpectedChecksum: "sha256:" + strings.Repeat("a", 64),
		Dial: func(context.Context) (net.Conn, error) { return nil, errors.New("not reached") },
	}
	helperBarrier, err := ocihelper.NewBootBarrier(client, ocihelper.AcquireSessionRequest{NodeID: "node", BootSessionID: "boot"})
	if err != nil {
		t.Fatal(err)
	}
	barrier := &SupervisedBootBarrier{Supervisor: supervisor, Barrier: helperBarrier}
	quiesced, recovered := false, false
	err = barrier.Recreate(t.Context(), func(context.Context) error { quiesced = true; return nil }, func(context.Context) error { recovered = true; return nil }, TemplateConfig{
		Sizing: Sizing{Memory: "4GiB", CPUs: 4, Disk: "32GiB"}, HostAllowedMountRoot: "/Users/operator/wefty",
	})
	if err != nil || !quiesced || !recovered {
		t.Fatalf("recreate quiesced=%t recovered=%t err=%v", quiesced, recovered, err)
	}
	commands := runner.commandsSnapshot()
	if len(commands) != 2 || !reflect.DeepEqual(commands[0], []string{"limactl", "delete", "--force", DefaultInstanceName}) ||
		len(commands[1]) != 4 || commands[1][1] != "start" || commands[1][2] != "--name="+DefaultInstanceName || !filepath.IsAbs(commands[1][3]) {
		t.Fatalf("recreate commands=%v", commands)
	}
}

func TestFileIntentSourceFailsClosedAndPreservesDisabledMarker(t *testing.T) {
	path := t.TempDir() + "/intent.json"
	source := FileIntentSource{Path: path}
	if intent, err := source.ReadIntent(t.Context()); err != nil || intent.Enabled {
		t.Fatalf("missing intent = %+v, %v", intent, err)
	}
	created, err := InitializeOCIIntent(path, time.Date(2026, 8, 23, 12, 0, 0, 0, time.UTC))
	if err != nil || !created {
		t.Fatalf("initialize intent = %t, %v", created, err)
	}
	intent, err := source.ReadIntent(t.Context())
	if err != nil || !intent.Enabled || intent.Revision != 1 {
		t.Fatalf("enabled intent = %+v, %v", intent, err)
	}
	disabled := []byte(`{"version":1,"revision":2,"enabled":false,"updated_at":"2026-08-23T12:01:00Z"}` + "\n")
	if err := os.WriteFile(path, disabled, 0o600); err != nil {
		t.Fatal(err)
	}
	created, err = InitializeOCIIntent(path, time.Now())
	if err != nil || created {
		t.Fatalf("disabled marker was replaced: created=%t err=%v", created, err)
	}
	intent, err = source.ReadIntent(t.Context())
	if err != nil || intent.Enabled || intent.Revision != 2 {
		t.Fatalf("preserved disabled intent = %+v, %v", intent, err)
	}
}

func TestSetOCIIntentIsRevisionedIdempotentAndFailClosed(t *testing.T) {
	path := filepath.Join(t.TempDir(), "intent.json")
	firstAt := time.Date(2026, 8, 28, 10, 0, 0, 0, time.UTC)
	if _, err := InitializeOCIIntent(path, firstAt); err != nil {
		t.Fatal(err)
	}
	disabled, err := SetOCIIntent(t.Context(), path, 1, false, firstAt.Add(time.Minute))
	if err != nil || disabled.Enabled || disabled.Revision != 2 {
		t.Fatalf("disable=%+v err=%v", disabled, err)
	}
	replay, err := SetOCIIntent(t.Context(), path, 2, false, firstAt.Add(2*time.Minute))
	if err != nil || replay != disabled {
		t.Fatalf("disabled replay=%+v err=%v", replay, err)
	}
	if _, err := SetOCIIntent(t.Context(), path, 1, true, firstAt.Add(3*time.Minute)); err == nil {
		t.Fatal("stale revision was accepted")
	}
	if err := os.WriteFile(path, []byte(`{"version":1,"revision":2,"enabled":false}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := SetOCIIntent(t.Context(), path, 2, true, firstAt.Add(4*time.Minute)); err == nil {
		t.Fatal("malformed durable intent was overwritten with a confident enable")
	}
}

func TestFileIntentSourceRejectsUnknownAndTrailingFields(t *testing.T) {
	path := filepath.Join(t.TempDir(), "intent.json")
	for _, payload := range []string{
		`{"version":1,"revision":1,"enabled":true,"updated_at":"2026-08-28T10:00:00Z","override":true}`,
		`{"version":1,"revision":1,"enabled":true,"updated_at":"2026-08-28T10:00:00Z"} {}`,
	} {
		if err := os.WriteFile(path, []byte(payload), 0o600); err != nil {
			t.Fatal(err)
		}
		if intent, err := (FileIntentSource{Path: path}).ReadIntent(t.Context()); err == nil || intent.Enabled {
			t.Fatalf("adversarial intent was accepted: intent=%+v err=%v", intent, err)
		}
	}
}

func TestDecodeInstanceStateRejectsAbsentAndUnknownState(t *testing.T) {
	payload := []byte("{\"name\":\"other\",\"status\":\"Running\"}\n{\"name\":\"wefty-oci\",\"status\":\"Broken\"}\n")
	state, err := decodeInstanceState(payload, DefaultInstanceName)
	if err != nil || state != InstanceBroken {
		t.Fatalf("decoded state = %q, %v", state, err)
	}
	if _, err := decodeInstanceState([]byte(`{"name":"other","status":"Running"}`), DefaultInstanceName); err == nil {
		t.Fatal("absent instance was accepted")
	}
}

func TestSupervisedBarrierReasonsStayInStableVocabulary(t *testing.T) {
	tests := []struct {
		err  error
		want contract.CapabilityReasonCode
	}{
		{err: &ocihelper.RPCError{Code: ocihelper.CodeChecksumMismatch}, want: contract.CapabilityReasonHelperVersionMismatch},
		{err: &ocihelper.RPCError{Code: ocihelper.CodeVersionMismatch}, want: contract.CapabilityReasonHelperVersionMismatch},
		{err: &ocihelper.RPCError{Code: ocihelper.CodePeerUnauthenticated}, want: contract.CapabilityReasonLocalPermissionDenied},
		{err: &ocihelper.HelperUnitUnavailableError{DialAttempts: 4, Cause: os.ErrNotExist}, want: contract.CapabilityReasonHelperUnitUnavailable},
		{err: &ocihelper.HelperHandshakeStalledError{DialAttempts: 1, Cause: context.DeadlineExceeded}, want: contract.CapabilityReasonHelperHandshakeStalled},
		{err: errors.New("acquire: dial OCI helper: connection refused at private path"), want: contract.CapabilityReasonHelperUnreachable},
		{err: errors.New("send OCI helper handshake: reset"), want: contract.CapabilityReasonHelperHandshakeFailed},
		{err: errors.New("verify OCI runtime namespace: residue"), want: contract.CapabilityReasonBootSweepFailed},
	}
	for _, test := range tests {
		if got := classifyHelperBarrierError(test.err); got != test.want || !got.Valid() {
			t.Fatalf("classify %v = %q, want %q", test.err, got, test.want)
		}
	}
}

func newTestSupervisor(t *testing.T, intent *mutableIntent, runner *supervisorRunner) *Supervisor {
	t.Helper()
	supervisor, err := NewSupervisor(SupervisorConfig{
		Instance: DefaultInstanceName,
		Intent:   intent,
		run:      runner.run,
		now:      func() time.Time { return time.Date(2026, 8, 23, 12, 0, 0, 0, time.UTC) },
		wait:     func(context.Context, time.Duration) error { return nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	return supervisor
}

type mutableIntent struct {
	mu       sync.Mutex
	revision uint64
	enabled  bool
}

func newMutableIntent(enabled bool) *mutableIntent {
	return &mutableIntent{revision: 1, enabled: enabled}
}

func (intent *mutableIntent) set(enabled bool) {
	intent.mu.Lock()
	intent.revision++
	intent.enabled = enabled
	intent.mu.Unlock()
}

func (intent *mutableIntent) ReadIntent(context.Context) (OCIIntent, error) {
	intent.mu.Lock()
	defer intent.mu.Unlock()
	return OCIIntent{
		Version: OCIIntentVersion, Revision: intent.revision, Enabled: intent.enabled,
		UpdatedAt: time.Date(2026, 8, 23, 12, 0, 0, 0, time.UTC),
	}, nil
}

type supervisorRunner struct {
	mu           sync.Mutex
	commands     [][]string
	states       []InstanceState
	startErrors  []error
	afterCommand func([]string)
}

func (runner *supervisorRunner) run(_ context.Context, name string, arguments ...string) ([]byte, error) {
	runner.mu.Lock()
	command := append([]string{name}, arguments...)
	runner.commands = append(runner.commands, command)
	var output []byte
	var err error
	if len(arguments) > 0 && arguments[0] == "list" {
		state := InstanceUnknown
		if len(runner.states) > 0 {
			state = runner.states[0]
			runner.states = runner.states[1:]
		}
		output = []byte(`{"name":"` + DefaultInstanceName + `","status":"` + string(state) + `"}`)
	}
	if len(arguments) > 0 && arguments[0] == "start" && len(runner.startErrors) > 0 {
		err = runner.startErrors[0]
		runner.startErrors = runner.startErrors[1:]
	}
	after := runner.afterCommand
	runner.mu.Unlock()
	if after != nil {
		after(arguments)
	}
	return output, err
}

func (runner *supervisorRunner) commandsSnapshot() [][]string {
	runner.mu.Lock()
	defer runner.mu.Unlock()
	result := make([][]string, len(runner.commands))
	for index := range runner.commands {
		result[index] = append([]string(nil), runner.commands[index]...)
	}
	return result
}

func TestDecodeInstanceStateDistinguishesAbsenceFromInvalidInspection(t *testing.T) {
	for _, test := range []struct {
		name      string
		payload   string
		wantState InstanceState
		wantError string
		absent    bool
	}{
		{name: "empty", wantState: InstanceUnknown, wantError: `inspect Lima instance: instance "wefty-oci" is absent`, absent: true},
		{name: "other", payload: `{"name":"other","status":"running"}`, wantState: InstanceUnknown, wantError: `inspect Lima instance: instance "wefty-oci" is absent`, absent: true},
		{name: "malformed", payload: `{`, wantState: InstanceUnknown, wantError: "inspect Lima instance: invalid JSON"},
		{name: "unknown_status", payload: `{"name":"wefty-oci","status":"unknown"}`, wantState: InstanceUnknown},
		{name: "running", payload: `{"name":"wefty-oci","status":"Running"}`, wantState: InstanceRunning},
		{name: "stopped", payload: `{"name":"wefty-oci","status":"Stopped"}`, wantState: InstanceStopped},
		{name: "broken", payload: `{"name":"wefty-oci","status":"Broken"}`, wantState: InstanceBroken},
	} {
		t.Run(test.name, func(t *testing.T) {
			state, err := decodeInstanceState([]byte(test.payload), DefaultInstanceName)
			message := ""
			if err != nil {
				message = err.Error()
			}
			var absent *instanceAbsentError
			if state != test.wantState || message != test.wantError || errors.As(err, &absent) != test.absent {
				t.Fatalf("state=%q err=%v absent=%t; want state=%q err=%q absent=%t", state, err, absent != nil, test.wantState, test.wantError, test.absent)
			}
		})
	}
}
