package agent

import (
	"context"
	"errors"
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/l1"
	workloadrunner "github.com/Derek-X-Wang/wefty/runner"
	"github.com/coder/websocket"
)

func TestComputerServicePublishesOnlyFabricFrontDoorAndAdmissionDialsView(t *testing.T) {
	identity := fabric.Identity{FabricID: "fabric-one", UserID: "person-a", DeviceID: "device-a"}
	privateFabric := &recordingComputerServiceFabric{identity: identity}
	now := time.Now().UTC()
	cache := NewComputerPolicyCache(systemClock{}, "node-1", "boot-1")
	defer cache.Close()
	if _, err := cache.Install(policySnapshot(t, now, 1, 1, nil, l1.ComputerGrant{
		FabricID: identity.FabricID, UserID: identity.UserID, Permission: l1.ComputerGrantControl, PolicyRevision: 1,
	})); err != nil {
		t.Fatal(err)
	}
	backend := newComputerBackend(t, computerBackendOptions{})
	defer backend.Close()
	var dialMu sync.Mutex
	dials := map[string]int{}
	dial := func(ctx context.Context, name string) (net.Conn, error) {
		dialMu.Lock()
		dials[name]++
		dialMu.Unlock()
		return backend.dial(ctx)
	}
	type publication struct {
		ready    bool
		endpoint string
	}
	publications := make(chan publication, 4)
	ctx, cancel := context.WithCancel(t.Context())
	runtime := &opaqueEndpointRuntime{release: make(chan struct{})}
	result := make(chan error, 1)
	go func() {
		_, err := runComputerService(ctx, runtime, workloadrunner.Request{}, nil, computerServiceConfig{
			clock: systemClock{}, fabric: privateFabric, authorizer: cache, auditor: &recordingComputerAuditor{},
			computerID: "computer-1", jobID: "job-1", attemptID: "attempt-1", fencingToken: "fence-1", dial: dial,
			publish: func(_ context.Context, ready bool, endpoint string) error {
				publications <- publication{ready: ready, endpoint: endpoint}
				return nil
			},
		})
		result <- err
	}()
	var published publication
	select {
	case published = <-publications:
	case <-time.After(5 * time.Second):
		t.Fatal("Computer front door was not published")
	}
	if !published.ready || published.endpoint == "" || privateFabric.listenNetwork != "tcp" || privateFabric.listenAddress != ":0" {
		t.Fatalf("private publication=%#v listen=%q %q", published, privateFabric.listenNetwork, privateFabric.listenAddress)
	}
	connection, _, err := websocket.Dial(t.Context(), published.endpoint, &websocket.DialOptions{Subprotocols: []string{computerWebSocketSubprotocol}})
	if err != nil {
		t.Fatal(err)
	}
	if kind, banner, err := connection.Read(t.Context()); err != nil || kind != websocket.MessageBinary || string(banner) != "RFB 003.008\n" {
		t.Fatalf("front-door banner=%q kind=%v err=%v", banner, kind, err)
	}
	_ = connection.CloseNow()
	cancel()
	select {
	case <-result:
	case <-time.After(5 * time.Second):
		t.Fatal("Computer service did not stop")
	}
	select {
	case withdrawn := <-publications:
		if withdrawn.ready {
			t.Fatalf("stop republished ready: %#v", withdrawn)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Computer stop did not withdraw publication")
	}
	dialMu.Lock()
	defer dialMu.Unlock()
	if dials[workloadrunner.AttemptEndpointView] != dials[workloadrunner.AttemptEndpointControl]+1 {
		t.Fatalf("endpoint dials=%v, want exactly one admission-only view dial", dials)
	}
}

func TestComputerReadinessIsAtomicAcrossPartialLossAndRecovery(t *testing.T) {
	now := time.Date(2026, 8, 28, 12, 0, 0, 0, time.UTC)
	clock := newManualClock(now)
	view := newComputerBackend(t, computerBackendOptions{})
	defer view.Close()
	control := newComputerBackend(t, computerBackendOptions{})
	defer control.Close()
	var controlReady atomic.Bool
	var viewReady atomic.Bool
	controlReady.Store(true)
	viewReady.Store(true)
	dial := func(ctx context.Context, name string) (net.Conn, error) {
		if name == workloadrunner.AttemptEndpointControl {
			if !controlReady.Load() {
				return nil, errors.New("control endpoint unavailable")
			}
			return control.dial(ctx)
		}
		if !viewReady.Load() {
			return nil, errors.New("view endpoint unavailable")
		}
		return view.dial(ctx)
	}
	started := make(chan time.Time, 1)
	started <- now
	observations := make(chan bool, 8)
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() {
		done <- monitorComputerReadiness(ctx, clock, started, dial, func(ready bool) { observations <- ready })
	}()
	wantComputerReadinessObservation(t, observations, true)

	controlReady.Store(false)
	clock.waitForDeadline(t, clock.Now().Add(DefaultComputerReadinessProbeInterval))
	clock.Advance(DefaultComputerReadinessProbeInterval)
	wantComputerReadinessObservation(t, observations, false)

	clock.waitForDeadline(t, clock.Now().Add(DefaultComputerReadinessProbeInterval))
	clock.Advance(DefaultComputerReadinessProbeInterval)
	wantNoComputerReadinessObservation(t, observations)

	controlReady.Store(true)
	clock.waitForDeadline(t, clock.Now().Add(DefaultComputerReadinessProbeInterval))
	clock.Advance(DefaultComputerReadinessProbeInterval)
	wantComputerReadinessObservation(t, observations, true)

	viewReady.Store(false)
	clock.waitForDeadline(t, clock.Now().Add(DefaultComputerReadinessProbeInterval))
	clock.Advance(DefaultComputerReadinessProbeInterval)
	wantComputerReadinessObservation(t, observations, false)

	viewReady.Store(true)
	clock.waitForDeadline(t, clock.Now().Add(DefaultComputerReadinessProbeInterval))
	clock.Advance(DefaultComputerReadinessProbeInterval)
	wantComputerReadinessObservation(t, observations, true)

	cancel()
	if err := <-done; err != nil {
		t.Fatalf("readiness monitor stop: %v", err)
	}
	wantComputerReadinessObservation(t, observations, false)
}

func TestComputerBackendLossWithdrawsPublicationWithoutKillingPayload(t *testing.T) {
	now := time.Date(2026, 8, 28, 12, 0, 0, 0, time.UTC)
	clock := newManualClock(now)
	identity := fabric.Identity{FabricID: "fabric-one", UserID: "person-a", DeviceID: "device-a"}
	privateFabric := &recordingComputerServiceFabric{identity: identity}
	cache := NewComputerPolicyCache(clock, "node-1", "boot-1")
	defer cache.Close()
	if _, err := cache.Install(policySnapshot(t, now, 1, 1, nil, l1.ComputerGrant{FabricID: identity.FabricID, UserID: identity.UserID, Permission: l1.ComputerGrantView, PolicyRevision: 1})); err != nil {
		t.Fatal(err)
	}
	backend := newComputerBackend(t, computerBackendOptions{})
	defer backend.Close()
	var available atomic.Bool
	available.Store(true)
	dial := func(ctx context.Context, _ string) (net.Conn, error) {
		if !available.Load() {
			return nil, errors.New("backend unavailable")
		}
		return backend.dial(ctx)
	}
	type publication struct{ ready bool }
	publications := make(chan publication, 8)
	ctx, cancel := context.WithCancel(t.Context())
	runtime := &opaqueEndpointRuntime{release: make(chan struct{}), startedAt: now}
	done := make(chan error, 1)
	go func() {
		_, err := runComputerService(ctx, runtime, workloadrunner.Request{}, nil, computerServiceConfig{
			clock: clock, fabric: privateFabric, authorizer: cache, auditor: &recordingComputerAuditor{},
			computerID: "computer-1", jobID: "job-1", attemptID: "attempt-1", fencingToken: "fence-1", dial: dial,
			publish: func(_ context.Context, ready bool, _ string) error {
				publications <- publication{ready: ready}
				return nil
			},
		})
		done <- err
	}()
	if published := <-publications; !published.ready {
		t.Fatal("Computer did not publish after both backends became ready")
	}
	available.Store(false)
	clock.waitForDeadline(t, clock.Now().Add(DefaultComputerReadinessProbeInterval))
	clock.Advance(DefaultComputerReadinessProbeInterval)
	if withdrawn := <-publications; withdrawn.ready {
		t.Fatal("partial backend loss did not withdraw the atomic publication")
	}
	select {
	case err := <-done:
		t.Fatalf("backend loss killed tenant payload: %v", err)
	default:
	}
	available.Store(true)
	clock.waitForDeadline(t, clock.Now().Add(DefaultComputerReadinessProbeInterval))
	clock.Advance(DefaultComputerReadinessProbeInterval)
	clock.waitForDeadline(t, clock.Now().Add(DefaultPublicationRecoveryWindow))
	clock.Advance(DefaultPublicationRecoveryWindow)
	if republished := <-publications; !republished.ready {
		t.Fatal("both recovered backends did not republish atomically")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Computer service did not stop after cancellation")
	}
}

func TestComputerReadinessDeadlineUsesAuthoritativeStartedTimestamp(t *testing.T) {
	now := time.Date(2026, 8, 28, 12, 0, 5, 0, time.UTC)
	startedAt := now.Add(-5 * time.Second)
	clock := newManualClock(now)
	view := newComputerBackend(t, computerBackendOptions{})
	defer view.Close()
	dial := func(ctx context.Context, name string) (net.Conn, error) {
		if name == workloadrunner.AttemptEndpointControl {
			return nil, errors.New("control endpoint unavailable")
		}
		return view.dial(ctx)
	}
	started := make(chan time.Time, 1)
	started <- startedAt
	observations := make(chan bool, 1)
	done := make(chan error, 1)
	go func() {
		done <- monitorComputerReadiness(t.Context(), clock, started, dial, func(ready bool) { observations <- ready })
	}()
	deadline := startedAt.Add(DefaultComputerReadinessDeadline)
	clock.waitForDeadline(t, deadline)
	clock.Advance(deadline.Sub(clock.Now()))
	err := <-done
	var readiness *computerReadinessError
	if !errors.As(err, &readiness) || readiness.Code != contract.SpawnFailureStartupReadinessTimeout {
		t.Fatalf("late readiness error = %#v", err)
	}
	wantNoComputerReadinessObservation(t, observations)
}

func TestComputerSteadyStateConnectTimeoutUsesInjectedClock(t *testing.T) {
	now := time.Date(2026, 8, 28, 12, 0, 0, 0, time.UTC)
	clock := newManualClock(now)
	backend := newComputerBackend(t, computerBackendOptions{})
	defer backend.Close()
	var blocked atomic.Bool
	dial := func(ctx context.Context, _ string) (net.Conn, error) {
		if blocked.Load() {
			<-ctx.Done()
			return nil, context.Cause(ctx)
		}
		return backend.dial(ctx)
	}
	started := make(chan time.Time, 1)
	started <- now
	observations := make(chan bool, 4)
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() {
		done <- monitorComputerReadiness(ctx, clock, started, dial, func(ready bool) { observations <- ready })
	}()
	wantComputerReadinessObservation(t, observations, true)

	blocked.Store(true)
	clock.waitForDeadline(t, clock.Now().Add(DefaultComputerReadinessProbeInterval))
	clock.Advance(DefaultComputerReadinessProbeInterval)
	clock.waitForDeadline(t, clock.Now().Add(DefaultComputerReadinessConnectTimeout))
	clock.Advance(DefaultComputerReadinessConnectTimeout)
	wantComputerReadinessObservation(t, observations, false)
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func wantComputerReadinessObservation(t *testing.T, observations <-chan bool, want bool) {
	t.Helper()
	select {
	case got := <-observations:
		if got != want {
			t.Fatalf("Computer readiness observation = %t, want %t", got, want)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for Computer readiness=%t", want)
	}
}

func wantNoComputerReadinessObservation(t *testing.T, observations <-chan bool) {
	t.Helper()
	select {
	case got := <-observations:
		t.Fatalf("unexpected Computer readiness observation %t", got)
	case <-time.After(25 * time.Millisecond):
	}
}

func TestComputerServiceRestartClearsHeldTenureAndAdmitsFreshHolder(t *testing.T) {
	identity := fabric.Identity{FabricID: "fabric-one", UserID: "person-a", DeviceID: "device-a"}
	privateFabric := &recordingComputerServiceFabric{identity: identity}
	cache := NewComputerPolicyCache(systemClock{}, "node-1", "boot-1")
	defer cache.Close()
	if _, err := cache.Install(policySnapshot(t, time.Now().UTC(), 1, 1, nil, l1.ComputerGrant{
		FabricID: identity.FabricID, UserID: identity.UserID, Permission: l1.ComputerGrantControl, PolicyRevision: 1,
	})); err != nil {
		t.Fatal(err)
	}
	viewBackend := newComputerBackend(t, computerBackendOptions{})
	defer viewBackend.Close()
	controlBackend := newComputerBackend(t, computerBackendOptions{})
	defer controlBackend.Close()
	dial := func(ctx context.Context, name string) (net.Conn, error) {
		if name == workloadrunner.AttemptEndpointControl {
			return controlBackend.dial(ctx)
		}
		return viewBackend.dial(ctx)
	}
	auditor := &recordingComputerAuditor{}
	type runningService struct {
		cancel   context.CancelFunc
		done     chan error
		endpoint string
		runtime  *restartComputerRuntime
	}
	start := func() runningService {
		ctx, cancel := context.WithCancel(t.Context())
		runtime := &restartComputerRuntime{opaqueEndpointRuntime: &opaqueEndpointRuntime{release: make(chan struct{})}}
		published := make(chan string, 2)
		done := make(chan error, 1)
		go func() {
			_, err := runComputerService(ctx, runtime, workloadrunner.Request{}, nil, computerServiceConfig{
				clock: systemClock{}, fabric: privateFabric, authorizer: cache, auditor: auditor,
				computerID: "computer-1", jobID: "job-1", attemptID: "attempt-1", fencingToken: "fence-1", dial: dial,
				publish: func(_ context.Context, ready bool, endpoint string) error {
					if ready {
						published <- endpoint
					}
					return nil
				},
			})
			done <- err
		}()
		select {
		case endpoint := <-published:
			return runningService{cancel: cancel, done: done, endpoint: endpoint, runtime: runtime}
		case <-time.After(5 * time.Second):
			cancel()
			t.Fatal("Computer service was not published")
			return runningService{}
		}
	}

	first := start()
	firstBase := "http" + strings.TrimSuffix(first.endpoint[len("ws"):], computerWebSocketPath)
	oldClient, oldToken := dialComputerFrontDoorWithToken(t, firstBase, nil)
	if _, _, err := oldClient.Read(t.Context()); err != nil {
		t.Fatal(err)
	}
	if status := postComputerControl(t, firstBase, computerControlTakePath, oldToken); status != http.StatusNoContent {
		t.Fatalf("first take status = %d", status)
	}
	if signals := first.runtime.signalSnapshot(); len(signals) != 2 || signals[0] || !signals[1] {
		t.Fatalf("first service startup/take signals = %v", signals)
	}
	first.cancel()
	select {
	case <-first.done:
	case <-time.After(5 * time.Second):
		t.Fatal("old Computer service did not stop")
	}
	readContext, cancelRead := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancelRead()
	if _, _, err := oldClient.Read(readContext); err == nil {
		t.Fatal("agent restart left the old input leg open")
	}
	if signals := first.runtime.signalSnapshot(); len(signals) != 3 || signals[2] {
		t.Fatalf("old service did not clear driver signal: %v", signals)
	}
	var released bool
	for _, event := range auditor.snapshot() {
		released = released || event.Kind == l1.ComputerTakeoverControlReleased
	}
	if !released {
		t.Fatalf("agent restart did not record release: %#v", auditor.snapshot())
	}

	second := start()
	defer second.cancel()
	secondBase := "http" + strings.TrimSuffix(second.endpoint[len("ws"):], computerWebSocketPath)
	freshClient, freshToken := dialComputerFrontDoorWithToken(t, secondBase, nil)
	defer freshClient.CloseNow()
	if _, _, err := freshClient.Read(t.Context()); err != nil {
		t.Fatal(err)
	}
	if status := postComputerControl(t, secondBase, computerControlTakePath, freshToken); status != http.StatusNoContent {
		t.Fatalf("fresh holder take status = %d", status)
	}
	if signals := second.runtime.signalSnapshot(); len(signals) != 2 || signals[0] || !signals[1] {
		t.Fatalf("fresh service startup/take signals = %v", signals)
	}
}

func TestComputerServiceConsumesRetriedFrontDoorAuditFailure(t *testing.T) {
	identity := fabric.Identity{FabricID: "fabric-one", UserID: "person-a", DeviceID: "device-a"}
	privateFabric := &recordingComputerServiceFabric{identity: identity}
	cache := NewComputerPolicyCache(systemClock{}, "node-1", "boot-1")
	defer cache.Close()
	if _, err := cache.Install(policySnapshot(t, time.Now().UTC(), 1, 1, nil, l1.ComputerGrant{
		FabricID: identity.FabricID, UserID: identity.UserID, Permission: l1.ComputerGrantView, PolicyRevision: 1,
	})); err != nil {
		t.Fatal(err)
	}
	backend := newComputerBackend(t, computerBackendOptions{})
	defer backend.Close()
	auditor := &failingComputerAuditor{}
	runtime := &opaqueEndpointRuntime{release: make(chan struct{})}
	published := make(chan string, 1)
	done := make(chan error, 1)
	go func() {
		_, err := runComputerService(t.Context(), runtime, workloadrunner.Request{}, nil, computerServiceConfig{
			clock: systemClock{}, fabric: privateFabric, authorizer: cache, auditor: auditor,
			computerID: "computer-1", jobID: "job-1", attemptID: "attempt-1", fencingToken: "fence-1",
			dial: func(ctx context.Context, _ string) (net.Conn, error) { return backend.dial(ctx) },
			publish: func(_ context.Context, ready bool, endpoint string) error {
				if ready {
					published <- endpoint
				}
				return nil
			},
		})
		done <- err
	}()
	var endpoint string
	select {
	case endpoint = <-published:
	case <-time.After(5 * time.Second):
		t.Fatal("Computer service was not published")
	}
	base := "http" + strings.TrimSuffix(endpoint[len("ws"):], computerWebSocketPath)
	connection, _ := dialComputerFrontDoorWithToken(t, base, nil)
	defer connection.CloseNow()
	select {
	case err := <-done:
		if err == nil || !strings.Contains(err.Error(), "audit unavailable") {
			t.Fatalf("service audit failure = %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Computer service ignored front-door audit failure")
	}
	if calls := auditor.callCount(); calls != computerAuditAttempts {
		t.Fatalf("front-door audit attempts = %d, want %d", calls, computerAuditAttempts)
	}
}

type recordingComputerServiceFabric struct {
	identity                     fabric.Identity
	listenNetwork, listenAddress string
}

func (runtime *opaqueEndpointRuntime) SetComputerControlState(context.Context, workloadrunner.AttemptAuthority, bool) error {
	return nil
}

type restartComputerRuntime struct {
	*opaqueEndpointRuntime
	mu      sync.Mutex
	signals []bool
}

type failingComputerAuditor struct {
	mu    sync.Mutex
	calls int
}

func (auditor *failingComputerAuditor) AppendComputerTakeoverAudit(
	context.Context, string, string, string, l1.ComputerTakeoverAuditRequest,
) (l1.ComputerTakeoverAuditReceipt, error) {
	auditor.mu.Lock()
	defer auditor.mu.Unlock()
	auditor.calls++
	return l1.ComputerTakeoverAuditReceipt{}, errors.New("audit unavailable")
}

func (auditor *failingComputerAuditor) callCount() int {
	auditor.mu.Lock()
	defer auditor.mu.Unlock()
	return auditor.calls
}

func (runtime *restartComputerRuntime) SetComputerControlState(_ context.Context, _ workloadrunner.AttemptAuthority, value bool) error {
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	runtime.signals = append(runtime.signals, value)
	return nil
}

func (runtime *restartComputerRuntime) signalSnapshot() []bool {
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	return append([]bool(nil), runtime.signals...)
}

func (value *recordingComputerServiceFabric) Listen(network, address string) (net.Listener, error) {
	value.listenNetwork, value.listenAddress = network, address
	if network != "tcp" || address != ":0" {
		return nil, errors.New("Computer listener escaped the private Fabric wildcard contract")
	}
	return net.Listen("tcp4", "127.0.0.1:0")
}

func (*recordingComputerServiceFabric) Dial(context.Context, string, string) (net.Conn, error) {
	return nil, errors.New("unused")
}
func (value *recordingComputerServiceFabric) WhoIs(context.Context, string) (fabric.Identity, error) {
	return value.identity, nil
}
func (*recordingComputerServiceFabric) ConnectHost() string { return "127.0.0.1" }

var _ WorkloadRuntime = (*opaqueEndpointRuntime)(nil)
