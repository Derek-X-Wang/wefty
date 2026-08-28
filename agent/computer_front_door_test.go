package agent

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/l1"
	"github.com/coder/websocket"
)

func TestComputerFrontDoorAlwaysAdmitsThroughViewAndDrainsRevocation(t *testing.T) {
	frontDoor, cache, auditor, controlDials, server, identity := computerFrontDoorFixture(t, l1.ComputerGrantControl)
	defer server.Close()
	connection := dialComputerFrontDoor(t, server.URL, nil)
	defer connection.CloseNow()
	messageType, banner, err := connection.Read(t.Context())
	if err != nil || messageType != websocket.MessageBinary || string(banner) != "RFB 003.008\n" {
		t.Fatalf("admitted banner = %q type=%v err=%v", banner, messageType, err)
	}
	if controlDials.Load() != 0 {
		t.Fatalf("control-authorized admission dialed control %d times", controlDials.Load())
	}
	handle := <-frontDoor.sessionHandles
	if !handle.CanTake() {
		t.Fatal("control-authorized session did not retain its narrow take capability")
	}

	now := cache.clock.Now().Add(time.Second)
	revoked := policySnapshot(t, now, 1, 2, nil, l1.ComputerGrant{
		FabricID: identity.FabricID, UserID: identity.UserID, Permission: l1.ComputerGrantNone, PolicyRevision: 2,
	})
	receipt, err := cache.Install(revoked)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-receipt.SessionsClosed:
	case <-time.After(5 * time.Second):
		t.Fatal("revocation acknowledgement did not wait for relay closure")
	}
	if _, _, err := connection.Read(t.Context()); err == nil {
		t.Fatal("revocation left the client relay open")
	}
	events := auditor.snapshot()
	if len(events) != 2 || events[0].Kind != l1.ComputerTakeoverSessionOpen || events[1].Kind != l1.ComputerTakeoverSessionClose ||
		events[0].AuthorizedRole != l1.ComputerGrantControl || events[0].AdmittedMode != "view" ||
		events[1].Reason != string(ComputerPolicyRevoked) {
		t.Fatalf("take-over audit events = %#v", events)
	}
	for _, event := range events {
		if event.UserID != identity.UserID || event.DeviceID != identity.DeviceID || event.AuthorityGeneration != 0 {
			t.Fatalf("audit identity/privacy = %#v", event)
		}
	}
}

func TestComputerFrontDoorViewerCannotTakeControl(t *testing.T) {
	frontDoor, _, _, controlDials, server, _ := computerFrontDoorFixture(t, l1.ComputerGrantView)
	defer server.Close()
	connection := dialComputerFrontDoor(t, server.URL, nil)
	defer connection.CloseNow()
	if _, _, err := connection.Read(t.Context()); err != nil {
		t.Fatal(err)
	}
	handle := <-frontDoor.sessionHandles
	if handle.CanTake() {
		t.Fatal("view-authorized session received a take capability")
	}
	if backend, err := handle.dialControl(t.Context()); err == nil || backend != nil {
		t.Fatalf("viewer take = %#v err=%v", backend, err)
	}
	if controlDials.Load() != 0 {
		t.Fatalf("viewer take refusal dialed control %d times", controlDials.Load())
	}
}

func TestComputerFrontDoorRejectsAdversarialAdmissionBeforeBackendDial(t *testing.T) {
	tests := []struct {
		name       string
		permission l1.ComputerGrantPermission
		identity   func(fabric.Identity) fabric.Identity
		path       string
		protocols  []string
		headers    http.Header
	}{
		{name: "wrong path", permission: l1.ComputerGrantView, path: "/control", protocols: []string{"binary"}},
		{name: "missing subprotocol", permission: l1.ComputerGrantView, path: computerWebSocketPath},
		{name: "wrong subprotocol", permission: l1.ComputerGrantView, path: computerWebSocketPath, protocols: []string{"control"}},
		{name: "multiple subprotocols", permission: l1.ComputerGrantView, path: computerWebSocketPath, protocols: []string{"binary", "control"}},
		{name: "client role header", permission: l1.ComputerGrantControl, path: computerWebSocketPath, protocols: []string{"binary"}, headers: http.Header{"X-Wefty-Role": []string{"control"}}},
		{name: "machine principal", permission: l1.ComputerGrantControl, path: computerWebSocketPath, protocols: []string{"binary"}, identity: func(identity fabric.Identity) fabric.Identity {
			identity.Kind = fabric.IdentityKindMachine
			return identity
		}},
		{name: "unauthorized person", permission: l1.ComputerGrantNone, path: computerWebSocketPath, protocols: []string{"binary"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture, _, auditor, _, server, identity := computerFrontDoorFixture(t, test.permission)
			if test.identity != nil {
				fixture.identity.set(test.identity(identity), nil)
			}
			defer server.Close()
			target := "ws" + server.URL[len("http"):] + test.path
			_, _, err := websocket.Dial(t.Context(), target, &websocket.DialOptions{Subprotocols: test.protocols, HTTPHeader: test.headers})
			if err == nil {
				t.Fatal("adversarial admission succeeded")
			}
			if fixture.viewDials.Load() != 0 || fixture.controlDials.Load() != 0 {
				t.Fatalf("rejected admission dialed view/control = %d/%d", fixture.viewDials.Load(), fixture.controlDials.Load())
			}
			events := auditor.snapshot()
			if len(events) != 1 || events[0].Kind != l1.ComputerTakeoverAdmissionDenied ||
				events[0].UserID != identity.UserID || events[0].DeviceID != identity.DeviceID {
				t.Fatalf("authenticated denial audit = %#v", events)
			}
		})
	}
}

func TestComputerFrontDoorRejectsWhoIsFailureAndStalePolicyBeforeDial(t *testing.T) {
	fixture, cache, auditor, _, server, identity := computerFrontDoorFixture(t, l1.ComputerGrantView)
	defer server.Close()
	fixture.identity.set(identity, errors.New("identity unavailable"))
	if connection, _, err := websocket.Dial(t.Context(), "ws"+server.URL[len("http"):]+computerWebSocketPath,
		&websocket.DialOptions{Subprotocols: []string{"binary"}}); err == nil {
		connection.CloseNow()
		t.Fatal("WhoIs failure admitted")
	}
	if events := auditor.snapshot(); len(events) != 1 || events[0].Kind != l1.ComputerTakeoverAdmissionDenied ||
		events[0].Reason != "identity_unavailable" {
		t.Fatalf("WhoIs denial audit = %#v", events)
	}
	fixture.identity.set(identity, nil)
	cache.Invalidate(ComputerPolicyWatchLost)
	if connection, _, err := websocket.Dial(t.Context(), "ws"+server.URL[len("http"):]+computerWebSocketPath,
		&websocket.DialOptions{Subprotocols: []string{"binary"}}); err == nil {
		connection.CloseNow()
		t.Fatal("stale policy admitted")
	}
	if fixture.viewDials.Load() != 0 || fixture.controlDials.Load() != 0 {
		t.Fatalf("failed admission dialed view/control = %d/%d", fixture.viewDials.Load(), fixture.controlDials.Load())
	}

	fresh := policySnapshot(t, cache.clock.Now().Add(time.Second), 1, 2, nil, l1.ComputerGrant{
		FabricID: identity.FabricID, UserID: identity.UserID, Permission: l1.ComputerGrantView, PolicyRevision: 2,
	})
	if _, err := cache.Install(fresh); err != nil {
		t.Fatal(err)
	}
	lostAuthority, cancelAuthority := context.WithCancel(t.Context())
	cancelAuthority()
	config := fixture.frontDoor.config
	config.authorityContext = lostAuthority
	frontDoor, err := newComputerFrontDoor(config)
	if err != nil {
		t.Fatal(err)
	}
	lostServer := httptest.NewServer(frontDoor)
	defer lostServer.Close()
	if connection, _, err := websocket.Dial(t.Context(), "ws"+lostServer.URL[len("http"):]+computerWebSocketPath,
		&websocket.DialOptions{Subprotocols: []string{"binary"}}); err == nil {
		connection.CloseNow()
		t.Fatal("lost attempt authority admitted")
	}
	if fixture.viewDials.Load() != 0 || fixture.controlDials.Load() != 0 {
		t.Fatalf("authority-lost admission dialed view/control = %d/%d", fixture.viewDials.Load(), fixture.controlDials.Load())
	}
}

func TestComputerFrontDoorSessionCapAndTextFramesCloseBothLegs(t *testing.T) {
	t.Run("session cap", func(t *testing.T) {
		fixture, _, auditor, _, server, _ := computerFrontDoorFixture(t, l1.ComputerGrantView)
		defer server.Close()
		fixture.frontDoor.config.sessionCap = 5 * time.Minute
		connection := dialComputerFrontDoor(t, server.URL, nil)
		defer connection.CloseNow()
		if _, _, err := connection.Read(t.Context()); err != nil {
			t.Fatal(err)
		}
		fixture.clock.waitForDeadline(t, fixture.clock.Now().Add(5*time.Minute))
		fixture.clock.Advance(5 * time.Minute)
		waitComputerAuditKind(t, auditor, l1.ComputerTakeoverSessionClose)
		events := auditor.snapshot()
		if events[len(events)-1].Reason != "session_cap_expired" {
			t.Fatalf("session cap close reason = %q", events[len(events)-1].Reason)
		}
		callsBeforeReconnect := fixture.identity.callCount()
		reconnected := dialComputerFrontDoor(t, server.URL, nil)
		defer reconnected.CloseNow()
		if _, _, err := reconnected.Read(t.Context()); err != nil {
			t.Fatal(err)
		}
		if calls := fixture.identity.callCount(); calls <= callsBeforeReconnect {
			t.Fatalf("session-cap reconnect reused identity: calls before/after = %d/%d", callsBeforeReconnect, calls)
		}
	})

	t.Run("periodic identity revalidation", func(t *testing.T) {
		fixture, _, auditor, _, server, identity := computerFrontDoorFixture(t, l1.ComputerGrantView)
		defer server.Close()
		fixture.frontDoor.config.revalidationInterval = 5 * time.Minute
		connection := dialComputerFrontDoor(t, server.URL, nil)
		defer connection.CloseNow()
		if _, _, err := connection.Read(t.Context()); err != nil {
			t.Fatal(err)
		}
		identity.DeviceID = "different-device"
		fixture.identity.set(identity, nil)
		fixture.clock.waitForDeadline(t, fixture.clock.Now().Add(5*time.Minute))
		fixture.clock.Advance(5 * time.Minute)
		waitComputerAuditKind(t, auditor, l1.ComputerTakeoverSessionClose)
		events := auditor.snapshot()
		if events[len(events)-1].Reason != "identity_changed" {
			t.Fatalf("identity-change close reason = %q", events[len(events)-1].Reason)
		}
	})

	t.Run("text frame", func(t *testing.T) {
		_, _, auditor, _, server, _ := computerFrontDoorFixture(t, l1.ComputerGrantView)
		defer server.Close()
		connection := dialComputerFrontDoor(t, server.URL, nil)
		defer connection.CloseNow()
		if _, _, err := connection.Read(t.Context()); err != nil {
			t.Fatal(err)
		}
		if err := connection.Write(t.Context(), websocket.MessageText, []byte("RFB input must be binary")); err != nil {
			t.Fatal(err)
		}
		readContext, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		defer cancel()
		if _, _, err := connection.Read(readContext); err == nil || websocket.CloseStatus(err) != websocket.StatusUnsupportedData {
			t.Fatalf("text frame close = %v status=%v", err, websocket.CloseStatus(err))
		}
		waitComputerAuditKind(t, auditor, l1.ComputerTakeoverSessionClose)
	})
}

func TestComputerBackendReadinessEnforcesWireContractAndDeadline(t *testing.T) {
	good := newComputerBackend(t, computerBackendOptions{})
	defer good.Close()
	if err := probeComputerBackends(t.Context(), systemClock{}, time.Now(), good.dial, good.dial); err != nil {
		t.Fatalf("valid readiness: %v", err)
	}

	fixtures := []struct {
		name    string
		options computerBackendOptions
	}{
		{name: "wrong path", options: computerBackendOptions{requiredPath: "/wrong"}},
		{name: "wrong subprotocol", options: computerBackendOptions{subprotocol: "other"}},
		{name: "text banner", options: computerBackendOptions{textBanner: true}},
	}
	for _, fixture := range fixtures {
		t.Run(fixture.name, func(t *testing.T) {
			backend := newComputerBackend(t, fixture.options)
			defer backend.Close()
			err := probeComputerBackends(t.Context(), systemClock{}, time.Now(), backend.dial, good.dial)
			var readiness *computerReadinessError
			if !errors.As(err, &readiness) || readiness.Code != "runtime_unavailable" {
				t.Fatalf("readiness error = %#v", err)
			}
		})
	}

	t.Run("60 second deadline", func(t *testing.T) {
		now := time.Date(2026, 8, 28, 12, 0, 0, 0, time.UTC)
		clock := newManualClock(now)
		blocked := func(ctx context.Context) (net.Conn, error) {
			<-ctx.Done()
			return nil, ctx.Err()
		}
		result := make(chan error, 1)
		go func() { result <- probeComputerBackends(t.Context(), clock, now, blocked, blocked) }()
		clock.waitForDeadline(t, now.Add(DefaultComputerReadinessDeadline))
		clock.Advance(DefaultComputerReadinessDeadline)
		err := <-result
		var readiness *computerReadinessError
		if !errors.As(err, &readiness) || readiness.Code != "startup_readiness_timeout" {
			t.Fatalf("deadline error = %#v", err)
		}
	})
}

func TestComputerFrontDoorRequiresDenyByDefaultConstruction(t *testing.T) {
	_, err := newComputerFrontDoor(computerFrontDoorConfig{})
	if err == nil {
		t.Fatal("Computer front door constructed without mandatory authority")
	}
	fixture, _, _, _, _, _ := computerFrontDoorFixture(t, l1.ComputerGrantView)
	config := fixture.frontDoor.config
	config.authorizer = nil
	if _, err := newComputerFrontDoor(config); err == nil {
		t.Fatal("Computer front door constructed without authorizer")
	}
	config = fixture.frontDoor.config
	config.sessionCap = time.Hour + time.Nanosecond
	if _, err := newComputerFrontDoor(config); err == nil {
		t.Fatal("Computer front door accepted a session cap over one hour")
	}
}

type computerFrontDoorTestFixture struct {
	frontDoor      *computerFrontDoor
	sessionHandles chan *computerSessionHandle
	identity       *mutableWhoIsFabric
	clock          *manualClock
	viewDials      atomic.Int64
	controlDials   atomic.Int64
}

func computerFrontDoorFixture(t *testing.T, permission l1.ComputerGrantPermission) (
	*computerFrontDoorTestFixture, *ComputerPolicyCache, *recordingComputerAuditor, *atomic.Int64, *httptest.Server, fabric.Identity,
) {
	t.Helper()
	now := time.Date(2026, 8, 28, 12, 0, 0, 0, time.UTC)
	clock := newManualClock(now)
	identity := fabric.Identity{FabricID: "fabric-one", UserID: "person-a", DeviceID: "device-a"}
	identityFabric := &mutableWhoIsFabric{}
	identityFabric.set(identity, nil)
	cache := NewComputerPolicyCache(clock, "node-1", "boot-1")
	t.Cleanup(cache.Close)
	if permission != l1.ComputerGrantNone {
		if _, err := cache.Install(policySnapshot(t, now, 1, 1, nil, l1.ComputerGrant{
			FabricID: identity.FabricID, UserID: identity.UserID, Permission: permission, PolicyRevision: 1,
		})); err != nil {
			t.Fatal(err)
		}
	} else {
		if _, err := cache.Install(policySnapshot(t, now, 1, 1, nil)); err != nil {
			t.Fatal(err)
		}
	}
	backend := newComputerBackend(t, computerBackendOptions{})
	t.Cleanup(backend.Close)
	auditor := &recordingComputerAuditor{}
	fixture := &computerFrontDoorTestFixture{identity: identityFabric, clock: clock, sessionHandles: make(chan *computerSessionHandle, 1)}
	viewDial := func(ctx context.Context) (net.Conn, error) {
		fixture.viewDials.Add(1)
		return backend.dial(ctx)
	}
	controlDial := func(context.Context) (net.Conn, error) {
		fixture.controlDials.Add(1)
		return nil, errors.New("control must not be dialed by admission")
	}
	frontDoor, err := newComputerFrontDoor(computerFrontDoorConfig{
		authorityContext: t.Context(), fabric: identityFabric, authorizer: cache, auditor: auditor, clock: clock,
		computerID: "computer-1", jobID: "job-1", attemptID: "attempt-1", fencingToken: "fence-1",
		viewDial: viewDial, controlDial: controlDial, onSession: func(handle *computerSessionHandle) { fixture.sessionHandles <- handle },
	})
	if err != nil {
		t.Fatal(err)
	}
	fixture.frontDoor = frontDoor
	server := httptest.NewServer(frontDoor)
	return fixture, cache, auditor, &fixture.controlDials, server, identity
}

func dialComputerFrontDoor(t *testing.T, serverURL string, headers http.Header) *websocket.Conn {
	t.Helper()
	connection, _, err := websocket.Dial(t.Context(), "ws"+serverURL[len("http"):]+computerWebSocketPath,
		&websocket.DialOptions{Subprotocols: []string{computerWebSocketSubprotocol}, HTTPHeader: headers})
	if err != nil {
		t.Fatal(err)
	}
	if connection.Subprotocol() != computerWebSocketSubprotocol {
		t.Fatalf("front door subprotocol = %q", connection.Subprotocol())
	}
	return connection
}

type mutableWhoIsFabric struct {
	mu       sync.Mutex
	identity fabric.Identity
	err      error
	calls    int
}

func (value *mutableWhoIsFabric) set(identity fabric.Identity, err error) {
	value.mu.Lock()
	defer value.mu.Unlock()
	value.identity, value.err = identity, err
}

func (*mutableWhoIsFabric) Listen(string, string) (net.Listener, error) {
	return nil, errors.New("unused")
}
func (*mutableWhoIsFabric) Dial(context.Context, string, string) (net.Conn, error) {
	return nil, errors.New("unused")
}
func (value *mutableWhoIsFabric) WhoIs(context.Context, string) (fabric.Identity, error) {
	value.mu.Lock()
	defer value.mu.Unlock()
	value.calls++
	return value.identity, value.err
}
func (*mutableWhoIsFabric) ConnectHost() string { return "computer.test" }

func (value *mutableWhoIsFabric) callCount() int {
	value.mu.Lock()
	defer value.mu.Unlock()
	return value.calls
}

type recordingComputerAuditor struct {
	mu     sync.Mutex
	events []l1.ComputerTakeoverAuditEvent
}

func (auditor *recordingComputerAuditor) AppendComputerTakeoverAudit(
	_ context.Context, _, _, _ string, request l1.ComputerTakeoverAuditRequest,
) (l1.ComputerTakeoverAuditReceipt, error) {
	auditor.mu.Lock()
	defer auditor.mu.Unlock()
	auditor.events = append(auditor.events, request.Event)
	return l1.ComputerTakeoverAuditReceipt{Event: request.Event}, nil
}

func (auditor *recordingComputerAuditor) snapshot() []l1.ComputerTakeoverAuditEvent {
	auditor.mu.Lock()
	defer auditor.mu.Unlock()
	return append([]l1.ComputerTakeoverAuditEvent(nil), auditor.events...)
}

func waitComputerAuditKind(t *testing.T, auditor *recordingComputerAuditor, kind l1.ComputerTakeoverAuditEventKind) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		for _, event := range auditor.snapshot() {
			if event.Kind == kind {
				return
			}
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s audit: %#v", kind, auditor.snapshot())
}

type computerBackendOptions struct {
	requiredPath string
	subprotocol  string
	textBanner   bool
}

type computerBackendServer struct {
	server  *httptest.Server
	address string
}

func newComputerBackend(t *testing.T, options computerBackendOptions) *computerBackendServer {
	t.Helper()
	requiredPath := options.requiredPath
	if requiredPath == "" {
		requiredPath = computerWebSocketPath
	}
	subprotocol := options.subprotocol
	if subprotocol == "" {
		subprotocol = computerWebSocketSubprotocol
	}
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path != requiredPath {
			http.NotFound(writer, request)
			return
		}
		connection, err := websocket.Accept(writer, request, &websocket.AcceptOptions{Subprotocols: []string{subprotocol}})
		if err != nil {
			return
		}
		defer connection.CloseNow()
		messageType := websocket.MessageBinary
		if options.textBanner {
			messageType = websocket.MessageText
		}
		if err := connection.Write(request.Context(), messageType, []byte("RFB 003.008\n")); err != nil {
			return
		}
		for {
			kind, payload, err := connection.Read(request.Context())
			if err != nil {
				return
			}
			if err := connection.Write(request.Context(), kind, payload); err != nil {
				return
			}
		}
	}))
	parsed, err := url.Parse(server.URL)
	if err != nil {
		server.Close()
		t.Fatal(err)
	}
	return &computerBackendServer{server: server, address: parsed.Host}
}

func (backend *computerBackendServer) dial(ctx context.Context) (net.Conn, error) {
	return (&net.Dialer{}).DialContext(ctx, "tcp", backend.address)
}

func (backend *computerBackendServer) Close() { backend.server.Close() }
