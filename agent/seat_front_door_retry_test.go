package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/fabric/plain"
	"github.com/Derek-X-Wang/wefty/internal/testwait"
	"github.com/Derek-X-Wang/wefty/l1"
	workloadrunner "github.com/Derek-X-Wang/wefty/runner"
)

type retrySeatListenFabric struct {
	fabric.Fabric
	failures      int
	calls         atomic.Int32
	listeners     chan *retrySeatListener
	releaseListen <-chan struct{}
	acceptError   <-chan error
}

func (f *retrySeatListenFabric) Listen(network, address string) (net.Listener, error) {
	if int(f.calls.Add(1)) <= f.failures {
		return nil, errors.New("injected Seat listen failure")
	}
	tracked := &retrySeatListener{closed: make(chan struct{}), accepting: make(chan struct{}), acceptError: f.acceptError}
	if f.listeners != nil {
		f.listeners <- tracked
	}
	if f.releaseListen != nil {
		<-f.releaseListen
	}
	return tracked, nil
}

// Keep session timers on the system clock so only the front-door retry uses
// the manually advanced clock. No workload or host runtime is needed.
func newSeatListenRetryNode(t *testing.T, failures int) (*Agent, *retrySeatListenFabric, *seatLeaseTestClock, <-chan string) {
	t.Helper()
	network := plain.NewNetwork()
	f := &retrySeatListenFabric{Fabric: network.NewFabric(fabric.Identity{NodeID: "retry-fabric", Tags: []string{l1.DefaultAgentPrincipalTag}}), failures: failures}
	client, err := newClient(f, "wefty://control-plane", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(client.Close)
	clock := &seatLeaseTestClock{manualClock: newManualClock(time.Now()), scheduled: make(chan time.Duration, 20)}
	registration := contract.NodeRegistration{NodeID: "retry-node", BootSessionID: "boot", OS: "linux", Architecture: "arm64", AgentVersion: "test", Capabilities: map[string]bool{"kind:process": true}}
	client.httpClient.Transport = captureRoundTrip(func(r *http.Request) (*http.Response, error) {
		w := httptest.NewRecorder()
		node := l1.Node{NodeRegistration: registration, ClaimsEnabled: true, MaxOneshotSlots: 1}
		switch {
		case r.URL.Path == "/v1/agent/nodes/register":
			_ = json.NewEncoder(w).Encode(node)
		case strings.HasSuffix(r.URL.Path, "/heartbeat"):
			_ = json.NewEncoder(w).Encode(l1.HeartbeatResponse{Node: node})
		default:
			w.WriteHeader(http.StatusNoContent)
		}
		return w.Result(), nil
	})
	observer := newLifecycleObserver(systemClock{})
	capabilities := newCapabilityState(nil, nil, systemClock{}, 0, nil)
	session := newAgentSession(client, registration, capabilities, time.Second, time.Second, systemClock{}, observer, nil, 1, 0)
	logs := make(chan string, 20)
	node := &Agent{fabric: f, session: session, registration: registration, clock: clock, observer: observer, capabilities: capabilities, logf: func(format string, args ...any) { logs <- fmt.Sprintf(format, args...) }}
	t.Cleanup(node.Close)
	return node, f, clock, logs
}

func TestSeatFrontDoorListenRetryRecoversWithoutRestart(t *testing.T) {
	node, f, clock, logs := newSeatListenRetryNode(t, 2)
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() { defer close(done); _ = node.Run(ctx) }()
	defer func() { cancel(); awaitSeatTestSignal(t, done) }()
	for _, want := range []time.Duration{time.Second, 2 * time.Second} {
		if got := awaitSeatLeaseSchedule(t, clock, done); got != want {
			t.Fatalf("retry delay=%v, want %v", got, want)
		}
		clock.Advance(want)
	}
	select {
	case line := <-logs:
		if !strings.Contains(line, "injected Seat listen failure") {
			t.Fatalf("failure log=%q", line)
		}
	case <-time.After(testwait.Bound):
		t.Fatal("no failure log")
	}
	select {
	case line := <-logs:
		if !strings.Contains(line, "opened") {
			t.Fatalf("recovery log=%q", line)
		}
	case <-time.After(testwait.Bound):
		t.Fatal("door did not recover")
	}
	select {
	case <-node.Registered():
	case <-time.After(testwait.Bound):
		t.Fatal("node did not register")
	}
	if f.calls.Load() != 3 {
		t.Fatalf("listen calls=%d", f.calls.Load())
	}
	select {
	case line := <-logs:
		t.Fatalf("extra log=%q", line)
	default:
	}
}

func TestSeatFrontDoorClosedAttemptIsRuntimeUnavailable(t *testing.T) {
	c, _, helper := newSeatTestController()
	called := false
	runtime := &seatLifecycleRuntime{seatTestRuntime: helper, run: func(context.Context, workloadrunner.Request) (workloadrunner.Result, error) {
		called = true
		return workloadrunner.Result{}, nil
	}}
	result, err := runComputerSeats(t.Context(), runtime, workloadrunner.Request{}, nil, c, seatComputerServiceConfig{copyRuntime: copyRuntimeFixture{}, copyControl: &copyControlFixture{}, publish: func(context.Context, bool, string) error { return nil }})
	if called || err == nil || result.SpawnError == nil || result.SpawnError.Code != contract.SpawnFailureRuntimeUnavailable || !strings.Contains(err.Error(), "Seat front door not open yet") || !strings.Contains(err.Error(), "retries") {
		t.Fatalf("closed door: runtime called=%v result=%+v err=%v", called, result, err)
	}
}

func TestSeatFrontDoorMissingDependenciesRemainConfigurationErrors(t *testing.T) {
	for _, missing := range []string{"publish", "copyRuntime", "copyControl"} {
		t.Run(missing, func(t *testing.T) {
			config := seatComputerServiceConfig{copyRuntime: copyRuntimeFixture{}, copyControl: &copyControlFixture{}, publish: func(context.Context, bool, string) error { return nil }}
			switch missing {
			case "publish":
				config.publish = nil
			case "copyRuntime":
				config.copyRuntime = nil
			case "copyControl":
				config.copyControl = nil
			}
			// Also leave the router closed: a configuration error must not be hidden
			// by the transient listener failure.
			result, err := runComputerSeats(t.Context(), nil, workloadrunner.Request{}, nil, nil, config)
			if err == nil || err.Error() != "Seat front-door dependencies are incomplete" || result.SpawnError != nil {
				t.Fatalf("missing %s: result=%+v err=%v", missing, result, err)
			}
		})
	}
}

type retrySeatListener struct {
	closed      chan struct{}
	accepting   chan struct{}
	closeOnce   sync.Once
	acceptOnce  sync.Once
	acceptError <-chan error
}

func (l *retrySeatListener) Accept() (net.Conn, error) {
	l.acceptOnce.Do(func() { close(l.accepting) })
	if l.acceptError != nil {
		select {
		case err := <-l.acceptError:
			return nil, err
		case <-l.closed:
			return nil, net.ErrClosed
		}
	}
	<-l.closed
	return nil, net.ErrClosed
}
func (l *retrySeatListener) Close() error {
	l.closeOnce.Do(func() { close(l.closed) })
	return nil
}
func (l *retrySeatListener) Addr() net.Addr {
	return &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 12345}
}

func awaitRetrySeatListener(t *testing.T, listeners <-chan *retrySeatListener) *retrySeatListener {
	t.Helper()
	select {
	case listener := <-listeners:
		return listener
	case <-time.After(testwait.Bound):
		t.Fatal("retry did not listen")
	}
	return nil
}

func TestSeatFrontDoorListenRetryCancellationStopsBackoff(t *testing.T) {
	node, f, clock, logs := newSeatListenRetryNode(t, 100)
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() { defer close(done); _ = node.Run(ctx) }()
	defer func() { cancel(); awaitSeatTestSignal(t, done) }()
	for _, want := range []time.Duration{time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second, 16 * time.Second, 30 * time.Second, 30 * time.Second} {
		if got := awaitSeatLeaseSchedule(t, clock, done); got != want {
			t.Fatalf("retry delay=%v, want %v", got, want)
		}
		if want == 30*time.Second && f.calls.Load() == 7 {
			break
		}
		clock.Advance(want)
	}
	cancel()
	awaitSeatTestSignal(t, done)
	calls := f.calls.Load()
	clock.Advance(time.Hour)
	if calls != 7 || f.calls.Load() != calls {
		t.Fatalf("retry survived cancellation: before=%d after=%d", calls, f.calls.Load())
	}
	clock.mu.Lock()
	defer clock.mu.Unlock()
	for _, timer := range clock.timers {
		if timer.active {
			t.Error("retry timer still active after Run returned")
		}
	}
	if len(logs) != 1 {
		t.Fatalf("failure streak logged %d times", len(logs))
	}
}

func TestSeatFrontDoorListenRetryClosesListenerOpenedDuringShutdown(t *testing.T) {
	node, f, clock, _ := newSeatListenRetryNode(t, 1)
	listeners := make(chan *retrySeatListener, 1)
	release := make(chan struct{})
	var releaseOnce sync.Once
	f.listeners, f.releaseListen = listeners, release
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() { defer close(done); _ = node.Run(ctx) }()
	defer func() { releaseOnce.Do(func() { close(release) }); cancel(); awaitSeatTestSignal(t, done) }()
	if got := awaitSeatLeaseSchedule(t, clock, done); got != time.Second {
		t.Fatalf("first delay=%v", got)
	}
	clock.Advance(time.Second)
	listener := awaitRetrySeatListener(t, listeners)
	cancel()
	select {
	case <-done:
		t.Fatal("Run returned before joining the in-flight listen")
	default:
	}
	releaseOnce.Do(func() { close(release) })
	awaitSeatTestSignal(t, done)
	awaitSeatTestSignal(t, listener.closed)
}

func TestSeatFrontDoorListenRetryServerExitEndsRun(t *testing.T) {
	node, f, clock, _ := newSeatListenRetryNode(t, 1)
	listeners := make(chan *retrySeatListener, 1)
	acceptError := make(chan error, 1)
	f.listeners, f.acceptError = listeners, acceptError
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	var runErr error
	go func() { defer close(done); runErr = node.Run(ctx) }()
	defer func() { cancel(); awaitSeatTestSignal(t, done) }()
	if got := awaitSeatLeaseSchedule(t, clock, done); got != time.Second {
		t.Fatalf("first delay=%v", got)
	}
	clock.Advance(time.Second)
	listener := awaitRetrySeatListener(t, listeners)
	awaitSeatTestSignal(t, listener.accepting)
	want := errors.New("injected late Seat server exit")
	acceptError <- want
	awaitSeatTestSignal(t, done)
	if !errors.Is(runErr, want) {
		t.Fatalf("Run lost server failure: %v", runErr)
	}
	awaitSeatTestSignal(t, listener.closed)
}
