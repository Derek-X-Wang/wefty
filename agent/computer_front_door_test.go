package agent

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/internal/takeover"
	"github.com/Derek-X-Wang/wefty/l1"
	workloadrunner "github.com/Derek-X-Wang/wefty/runner"
	"github.com/coder/websocket"
)

func TestComputerFrontDoorReturnsTypedStalePolicyRefusal(t *testing.T) {
	_, _, _, _, server, _ := computerFrontDoorFixture(t, l1.ComputerGrantNone)
	defer server.Close()
	header := http.Header{}
	header.Set(contract.ComputerPolicyRevisionHeader, "2")
	connection, response, err := websocket.Dial(t.Context(), "ws"+server.URL[len("http"):]+computerWebSocketPath,
		&websocket.DialOptions{Subprotocols: []string{computerWebSocketSubprotocol}, HTTPHeader: header})
	if connection != nil {
		connection.CloseNow()
		t.Fatal("stale policy refusal upgraded the connection")
	}
	if err == nil || response == nil || response.StatusCode != http.StatusForbidden {
		t.Fatalf("stale policy refusal = status=%v err=%v", response, err)
	}
	defer response.Body.Close()
	body, readErr := io.ReadAll(response.Body)
	var failure contract.ComputerControlErrorResponse
	if readErr != nil || json.Unmarshal(body, &failure) != nil || failure.Error.Code != contract.ErrorStalePolicyRevision || !failure.Error.Retryable {
		t.Fatalf("stale policy refusal body = %s readErr=%v decoded=%+v", body, readErr, failure)
	}
}

func TestComputerFrontDoorKeepsPermanentDenialNonRetryable(t *testing.T) {
	_, _, _, _, server, _ := computerFrontDoorFixture(t, l1.ComputerGrantNone)
	defer server.Close()
	header := http.Header{}
	header.Set(contract.ComputerPolicyRevisionHeader, "1")
	_, response, err := websocket.Dial(t.Context(), "ws"+server.URL[len("http"):]+computerWebSocketPath,
		&websocket.DialOptions{Subprotocols: []string{computerWebSocketSubprotocol}, HTTPHeader: header})
	if err == nil || response == nil || response.StatusCode != http.StatusForbidden {
		t.Fatalf("permanent policy refusal = status=%v err=%v", response, err)
	}
	defer response.Body.Close()
	body, _ := io.ReadAll(response.Body)
	var failure contract.ComputerControlErrorResponse
	if json.Unmarshal(body, &failure) != nil || failure.Error.Code != contract.ErrorControlNotAuthorized || failure.Error.Retryable {
		t.Fatalf("permanent policy refusal body = %s decoded=%+v", body, failure)
	}
}

func TestComputerFrontDoorAlwaysAdmitsThroughViewAndDrainsRevocation(t *testing.T) {
	_, cache, auditor, controlDials, server, identity := computerFrontDoorFixture(t, l1.ComputerGrantControl)
	defer server.Close()
	connection, token := dialComputerFrontDoorWithToken(t, server.URL, nil)
	defer connection.CloseNow()
	messageType, banner, err := connection.Read(t.Context())
	if err != nil || messageType != websocket.MessageBinary || string(banner) != "RFB 003.008\n" {
		t.Fatalf("admitted banner = %q type=%v err=%v", banner, messageType, err)
	}
	if controlDials.Load() != 0 {
		t.Fatalf("control-authorized admission dialed control %d times", controlDials.Load())
	}
	if status := postComputerControl(t, server.URL, computerControlTakePath, token); status != http.StatusServiceUnavailable {
		t.Fatalf("default control tenure status = %d", status)
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
	waitComputerAuditKind(t, auditor, l1.ComputerTakeoverSessionClose)
	events := auditor.snapshot()
	if len(events) != 2 || events[0].Kind != l1.ComputerTakeoverSessionOpen || events[1].Kind != l1.ComputerTakeoverSessionClose ||
		events[0].AuthorizedRole != l1.ComputerGrantControl || events[0].AdmittedMode != "view" ||
		events[1].Reason != l1.ComputerTakeoverRevoked {
		t.Fatalf("take-over audit events = %#v", events)
	}
	for _, event := range events {
		if event.UserID != identity.UserID || event.DeviceID != identity.DeviceID || event.AuthorityGeneration != 0 {
			t.Fatalf("audit identity/privacy = %#v", event)
		}
	}
}

func TestComputerFrontDoorIgnoresClientAuthorityHeaders(t *testing.T) {
	fixture, _, _, _, server, _ := computerFrontDoorFixture(t, l1.ComputerGrantView)
	defer server.Close()
	connection, token := dialComputerFrontDoorWithToken(t, server.URL, http.Header{"X-Wefty-Take": []string{"control"}, "X-Wefty-Role": []string{"control"}})
	defer connection.CloseNow()
	if _, _, err := connection.Read(t.Context()); err != nil {
		t.Fatal(err)
	}
	if status := postComputerControl(t, server.URL, computerControlTakePath, token); status != http.StatusForbidden || fixture.controlDials.Load() != 0 {
		t.Fatalf("client headers changed authority: status=%d controlDials=%d", status, fixture.controlDials.Load())
	}
}

func TestComputerFrontDoorViewerCannotTakeControl(t *testing.T) {
	_, _, _, controlDials, server, _ := computerFrontDoorFixture(t, l1.ComputerGrantView)
	defer server.Close()
	connection, token := dialComputerFrontDoorWithToken(t, server.URL, nil)
	defer connection.CloseNow()
	if _, _, err := connection.Read(t.Context()); err != nil {
		t.Fatal(err)
	}
	_, err := takeover.Perform(t.Context(), directTakeoverFabric{},
		"ws"+server.URL[len("http"):]+computerWebSocketPath, token, "take")
	var actionErr *takeover.ActionError
	if !errors.As(err, &actionErr) || actionErr.APIError.Code != contract.ErrorControlNotAuthorized {
		t.Fatalf("CLI viewer take error = %#v", err)
	}
	if controlDials.Load() != 0 {
		t.Fatalf("viewer take refusal dialed control %d times", controlDials.Load())
	}
}

type directTakeoverFabric struct{}

func (directTakeoverFabric) Listen(string, string) (net.Listener, error) {
	return nil, errors.New("unused")
}
func (directTakeoverFabric) Dial(ctx context.Context, network, address string) (net.Conn, error) {
	return (&net.Dialer{}).DialContext(ctx, network, address)
}
func (directTakeoverFabric) WhoIs(context.Context, string) (fabric.Identity, error) {
	return fabric.Identity{}, errors.New("unused")
}
func (directTakeoverFabric) ConnectHost() string { return "" }

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
			fixture.frontDoor.denials.flush(t.Context())
			events := auditor.snapshot()
			if len(events) != 1 || events[0].Kind != l1.ComputerTakeoverAdmissionDenied {
				t.Fatalf("authenticated denial audit = %#v", events)
			}
			if test.name == "machine principal" {
				if events[0].UserID != "" || events[0].DeviceID != "" {
					t.Fatalf("machine denial populated person columns: %#v", events[0])
				}
			} else if events[0].UserID != identity.UserID || events[0].DeviceID != identity.DeviceID {
				t.Fatalf("person denial identity = %#v", events[0])
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
	fixture.frontDoor.denials.flush(t.Context())
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
		server.Close()
		config := fixture.frontDoor.config
		config.sessionCap = 5 * time.Minute
		frontDoor, err := newComputerFrontDoor(config)
		if err != nil {
			t.Fatal(err)
		}
		frontDoor.SetReady(true)
		server = httptest.NewServer(frontDoor)
		defer server.Close()
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
		server.Close()
		config := fixture.frontDoor.config
		config.revalidationInterval = 5 * time.Minute
		frontDoor, err := newComputerFrontDoor(config)
		if err == nil {
			t.Fatal("front door accepted revalidation less frequent than once per minute")
		}
		config.revalidationInterval = 30 * time.Second
		frontDoor, err = newComputerFrontDoor(config)
		if err != nil {
			t.Fatal(err)
		}
		frontDoor.SetReady(true)
		server = httptest.NewServer(frontDoor)
		defer server.Close()
		connection := dialComputerFrontDoor(t, server.URL, nil)
		defer connection.CloseNow()
		if _, _, err := connection.Read(t.Context()); err != nil {
			t.Fatal(err)
		}
		identity.DeviceID = "different-device"
		fixture.identity.set(identity, nil)
		fixture.clock.waitForDeadline(t, fixture.clock.Now().Add(30*time.Second))
		fixture.clock.Advance(30 * time.Second)
		waitComputerAuditKind(t, auditor, l1.ComputerTakeoverSessionClose)
		events := auditor.snapshot()
		if events[len(events)-1].Reason != l1.ComputerTakeoverRevalidationFailed {
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

func TestComputerFrontDoorClosesRelayBeforeAuditFinalization(t *testing.T) {
	fixture, _, auditor, _, server, _ := computerFrontDoorFixture(t, l1.ComputerGrantView)
	server.Close()
	var viewClosed atomic.Bool
	originalDial := fixture.frontDoor.config.dial
	config := fixture.frontDoor.config
	config.sessionCap = 5 * time.Minute
	config.dial = func(ctx context.Context, name string) (net.Conn, error) {
		connection, err := originalDial(ctx, name)
		if err != nil || name != workloadrunner.AttemptEndpointView {
			return connection, err
		}
		return &closeObservedConn{Conn: connection, closed: &viewClosed}, nil
	}
	releaseObserved := make(chan bool, 1)
	config.controlTenure = &releaseOrderingTenure{viewClosed: &viewClosed, releaseObserved: releaseObserved}
	frontDoor, err := newComputerFrontDoor(config)
	if err != nil {
		t.Fatal(err)
	}
	frontDoor.SetReady(true)
	server = httptest.NewServer(frontDoor)
	defer server.Close()
	connection := dialComputerFrontDoor(t, server.URL, nil)
	defer connection.CloseNow()
	if _, _, err := connection.Read(t.Context()); err != nil {
		t.Fatal(err)
	}
	fixture.clock.waitForDeadline(t, fixture.clock.Now().Add(5*time.Minute))
	fixture.clock.Advance(5 * time.Minute)
	if closed := <-releaseObserved; !closed {
		t.Fatal("audit finalization began before the relay socket closed")
	}
	waitComputerAuditKind(t, auditor, l1.ComputerTakeoverSessionClose)
}

func TestComputerPolicyDrainDoesNotWaitForAuditFinalization(t *testing.T) {
	fixture, cache, auditor, _, server, identity := computerFrontDoorFixture(t, l1.ComputerGrantView)
	defer server.Close()
	connection := dialComputerFrontDoor(t, server.URL, nil)
	defer connection.CloseNow()
	if _, _, err := connection.Read(t.Context()); err != nil {
		t.Fatal(err)
	}
	auditStarted := make(chan struct{}, 1)
	releaseAudit := make(chan struct{})
	var releaseOnce sync.Once
	unblockAudit := func() { releaseOnce.Do(func() { close(releaseAudit) }) }
	t.Cleanup(unblockAudit)
	auditor.mu.Lock()
	auditor.blockKind = l1.ComputerTakeoverSessionClose
	auditor.blockStarted = auditStarted
	auditor.blockRelease = releaseAudit
	auditor.mu.Unlock()
	receipt, err := cache.Install(policySnapshot(t, fixture.clock.Now().Add(time.Second), 1, 2, nil, l1.ComputerGrant{
		FabricID: identity.FabricID, UserID: identity.UserID, Permission: l1.ComputerGrantNone, PolicyRevision: 2,
	}))
	if err != nil {
		t.Fatal(err)
	}
	<-auditStarted
	select {
	case <-receipt.SessionsClosed:
	default:
		t.Fatal("policy drain waited for session audit after the relay socket closed")
	}
	if _, _, err := connection.Read(t.Context()); err == nil {
		t.Fatal("policy drain completed before closing the client socket")
	}
	unblockAudit()
	waitComputerAuditKind(t, auditor, l1.ComputerTakeoverSessionClose)
}

func TestComputerBackendReadinessEnforcesWireContractAndDeadline(t *testing.T) {
	good := newComputerBackend(t, computerBackendOptions{})
	defer good.Close()
	dialGood := func(ctx context.Context, _ string) (net.Conn, error) { return good.dial(ctx) }
	if err := probeComputerBackends(t.Context(), systemClock{}, time.Now(), dialGood); err != nil {
		t.Fatalf("valid readiness: %v", err)
	}

	fixtures := []struct {
		name    string
		options computerBackendOptions
	}{
		{name: "wrong path", options: computerBackendOptions{requiredPath: "/wrong"}},
		{name: "missing subprotocol", options: computerBackendOptions{noSubprotocol: true}},
		{name: "wrong subprotocol", options: computerBackendOptions{subprotocol: "other"}},
		{name: "text banner", options: computerBackendOptions{textBanner: true}},
		{name: "HTTP response without upgrade", options: computerBackendOptions{httpOnly: true}},
	}
	for _, fixture := range fixtures {
		t.Run(fixture.name, func(t *testing.T) {
			backend := newComputerBackend(t, fixture.options)
			defer backend.Close()
			dial := func(ctx context.Context, name string) (net.Conn, error) {
				if name == workloadrunner.AttemptEndpointView {
					return backend.dial(ctx)
				}
				return good.dial(ctx)
			}
			assertComputerBackendNeverReady(t, dial)
		})
	}

	t.Run("plain TCP", func(t *testing.T) {
		listener, err := net.Listen("tcp4", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		defer listener.Close()
		go func() {
			for {
				connection, acceptErr := listener.Accept()
				if acceptErr != nil {
					return
				}
				_, _ = connection.Write([]byte("RFB 003.008\n"))
				_ = connection.Close()
			}
		}()
		dial := func(ctx context.Context, name string) (net.Conn, error) {
			if name == workloadrunner.AttemptEndpointView {
				return (&net.Dialer{}).DialContext(ctx, "tcp4", listener.Addr().String())
			}
			return good.dial(ctx)
		}
		assertComputerBackendNeverReady(t, dial)
	})

	t.Run("60 second deadline", func(t *testing.T) {
		now := time.Date(2026, 8, 28, 12, 0, 0, 0, time.UTC)
		clock := newManualClock(now)
		blocked := func(ctx context.Context, _ string) (net.Conn, error) {
			<-ctx.Done()
			return nil, ctx.Err()
		}
		result := make(chan error, 1)
		go func() { result <- probeComputerBackends(t.Context(), clock, now, blocked) }()
		clock.waitForDeadline(t, now.Add(DefaultComputerReadinessDeadline))
		clock.Advance(DefaultComputerReadinessDeadline)
		err := <-result
		var readiness *computerReadinessError
		if !errors.As(err, &readiness) || readiness.Code != "startup_readiness_timeout" {
			t.Fatalf("deadline error = %#v", err)
		}
	})

	t.Run("success at deadline is timeout", func(t *testing.T) {
		for range 20 {
			now := time.Date(2026, 8, 28, 12, 0, 0, 0, time.UTC)
			clock := newManualClock(now)
			advanced := false
			dial := func(ctx context.Context, name string) (net.Conn, error) {
				if name == workloadrunner.AttemptEndpointControl && !advanced {
					advanced = true
					clock.Advance(DefaultComputerReadinessDeadline)
				}
				return good.dial(ctx)
			}
			err := probeComputerBackends(t.Context(), clock, now, dial)
			var readiness *computerReadinessError
			if !errors.As(err, &readiness) || readiness.Code != contract.SpawnFailureStartupReadinessTimeout {
				t.Fatalf("at-deadline successful probe = %#v", err)
			}
		}
	})
}

func TestComputerSessionAuthorityLossOwnsConcurrentRelayClosure(t *testing.T) {
	frontDoor := &computerFrontDoor{config: computerFrontDoorConfig{
		clock: systemClock{}, sessionCap: time.Hour, revalidationInterval: time.Hour,
	}}
	authorization := &ComputerGrantAuthorization{revocations: make(chan ComputerPolicyRevocation)}
	for range 64 {
		ctx, cancel := context.WithCancelCause(t.Context())
		cancel(&computerSessionEnd{reason: l1.ComputerTakeoverAttemptAuthorityLost})
		relay := &computerSessionRelay{reason: make(chan l1.ComputerTakeoverReason, 1)}
		relay.reason <- l1.ComputerTakeoverControlBackendClosed
		if got := frontDoor.waitForSessionEnd(ctx, "127.0.0.1:1", fabric.Identity{}, authorization, relay, time.Now()); got != l1.ComputerTakeoverAttemptAuthorityLost {
			t.Fatalf("concurrent authority loss reason = %q, want %q", got, l1.ComputerTakeoverAttemptAuthorityLost)
		}
	}
}

func TestComputerSessionContextCanonicalizesParentAuthorityLoss(t *testing.T) {
	authority, cancelAuthority := context.WithCancel(t.Context())
	session, cancelSession, stop := newComputerSessionContext(authority)
	defer stop()
	defer cancelSession(nil)
	cancelAuthority()
	<-session.Done()
	if got := computerSessionEndReason(session, l1.ComputerTakeoverControlBackendClosed); got != l1.ComputerTakeoverAttemptAuthorityLost {
		t.Fatalf("parent authority loss reason = %q, want %q", got, l1.ComputerTakeoverAttemptAuthorityLost)
	}
}

func assertComputerBackendNeverReady(t *testing.T, dial computerEndpointDial) {
	t.Helper()
	if err := probeComputerBackendPairOnce(t.Context(), dial); err == nil {
		t.Fatal("negative readiness fixture completed a conformant probe")
	}
	now := time.Date(2026, 8, 28, 12, 0, 0, 0, time.UTC)
	clock := newManualClock(now)
	result := make(chan error, 1)
	go func() { result <- probeComputerBackends(t.Context(), clock, now, dial) }()
	clock.waitForDeadline(t, now.Add(DefaultComputerReadinessDeadline))
	clock.Advance(DefaultComputerReadinessDeadline)
	err := <-result
	var readiness *computerReadinessError
	if !errors.As(err, &readiness) || readiness.Code != contract.SpawnFailureStartupReadinessTimeout {
		t.Fatalf("readiness error = %#v", err)
	}
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

func TestComputerFrontDoorRetriesAndRejectsUnassertedAuditReceipts(t *testing.T) {
	fixture, _, auditor, _, server, _ := computerFrontDoorFixture(t, l1.ComputerGrantView)
	defer server.Close()
	auditor.mu.Lock()
	auditor.mismatchReceipt = true
	auditor.mu.Unlock()
	base := l1.ComputerTakeoverAuditEvent{
		ComputerID: "computer-1", JobID: "job-1", AttemptID: "attempt-1", FabricID: "fabric-1",
		UserID: "person-1", DeviceID: "device-1", AuthorizedRole: l1.ComputerGrantView,
		AdmittedMode: l1.ComputerAdmittedView, PolicyRevision: 1,
		OccurredAt: time.Date(2026, 8, 28, 5, 0, 0, 123, time.FixedZone("audit-local", -7*60*60)),
	}
	for _, kind := range []l1.ComputerTakeoverAuditEventKind{
		l1.ComputerTakeoverAdmissionDenied, l1.ComputerTakeoverSessionOpen, l1.ComputerTakeoverSessionClose,
	} {
		before := len(auditor.snapshot())
		event := base
		event.EventID = "unasserted:" + string(kind)
		event.Kind = kind
		if kind == l1.ComputerTakeoverAdmissionDenied {
			event.SessionID = ""
			event.AdmittedMode = ""
			event.Reason = l1.ComputerTakeoverUnauthorizedIdentity
			event.EventCount = 1
		} else {
			event.SessionID = "session-1"
			if kind == l1.ComputerTakeoverSessionClose {
				event.Reason = l1.ComputerTakeoverClientClosed
			}
		}
		if err := fixture.frontDoor.record(t.Context(), event); err == nil {
			t.Fatalf("%s accepted an unasserted receipt", kind)
		}
		if attempts := len(auditor.snapshot()) - before; attempts != computerAuditAttempts {
			t.Fatalf("%s attempts = %d, want %d", kind, attempts, computerAuditAttempts)
		}
	}
}

type computerFrontDoorTestFixture struct {
	frontDoor    *computerFrontDoor
	identity     *mutableWhoIsFabric
	clock        *manualClock
	viewDials    atomic.Int64
	controlDials atomic.Int64
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
	fixture := &computerFrontDoorTestFixture{identity: identityFabric, clock: clock}
	dial := func(ctx context.Context, name string) (net.Conn, error) {
		switch name {
		case workloadrunner.AttemptEndpointView:
			fixture.viewDials.Add(1)
			return backend.dial(ctx)
		case workloadrunner.AttemptEndpointControl:
			fixture.controlDials.Add(1)
			return nil, errors.New("control must not be dialed by admission")
		default:
			return nil, errors.New("unknown Computer endpoint")
		}
	}
	frontDoor, err := newComputerFrontDoor(computerFrontDoorConfig{
		authorityContext: t.Context(), fabric: identityFabric, authorizer: cache, auditor: auditor, clock: clock,
		computerID: "computer-1", jobID: "job-1", attemptID: "attempt-1", fencingToken: "fence-1",
		dial: dial,
	})
	if err != nil {
		t.Fatal(err)
	}
	fixture.frontDoor = frontDoor
	frontDoor.SetReady(true)
	server := httptest.NewServer(frontDoor)
	return fixture, cache, auditor, &fixture.controlDials, server, identity
}

func dialComputerFrontDoor(t *testing.T, serverURL string, headers http.Header) *websocket.Conn {
	t.Helper()
	connection, _ := dialComputerFrontDoorWithToken(t, serverURL, headers)
	return connection
}

func dialComputerFrontDoorWithToken(t *testing.T, serverURL string, headers http.Header) (*websocket.Conn, string) {
	t.Helper()
	connection, response, err := websocket.Dial(t.Context(), "ws"+serverURL[len("http"):]+computerWebSocketPath,
		&websocket.DialOptions{Subprotocols: []string{computerWebSocketSubprotocol}, HTTPHeader: headers})
	if err != nil {
		t.Fatal(err)
	}
	if connection.Subprotocol() != computerWebSocketSubprotocol {
		t.Fatalf("front door subprotocol = %q", connection.Subprotocol())
	}
	token := response.Header.Get(computerControlTokenHeader)
	if token == "" {
		connection.CloseNow()
		t.Fatal("front door omitted the session-bound control token")
	}
	return connection, token
}

func postComputerControl(t *testing.T, serverURL, path, token string) int {
	t.Helper()
	request, err := http.NewRequestWithContext(t.Context(), http.MethodPost, serverURL+path, nil)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set(computerControlTokenHeader, token)
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	return response.StatusCode
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
	mu              sync.Mutex
	events          []l1.ComputerTakeoverAuditEvent
	mismatchReceipt bool
	changed         chan struct{}
	blockKind       l1.ComputerTakeoverAuditEventKind
	blockStarted    chan<- struct{}
	blockRelease    <-chan struct{}
}

type closeObservedConn struct {
	net.Conn
	closed *atomic.Bool
}

func (connection *closeObservedConn) Close() error {
	connection.closed.Store(true)
	return connection.Conn.Close()
}

type releaseOrderingTenure struct {
	viewClosed      *atomic.Bool
	releaseObserved chan<- bool
	once            sync.Once
}

func (*releaseOrderingTenure) Register(controlTenureSession) error { return nil }
func (*releaseOrderingTenure) Take(context.Context, string) (net.Conn, error) {
	return nil, &ComputerTenureError{Code: ComputerTenureUnavailable}
}
func (tenure *releaseOrderingTenure) TakeReceipt(ctx context.Context, sessionID string) (contract.ComputerControlReceipt, error) {
	_, err := tenure.Take(ctx, sessionID)
	return contract.ComputerControlReceipt{}, err
}
func (tenure *releaseOrderingTenure) Release(context.Context, string, l1.ComputerTakeoverReason) error {
	tenure.once.Do(func() { tenure.releaseObserved <- tenure.viewClosed.Load() })
	return nil
}
func (tenure *releaseOrderingTenure) ReleaseReceipt(ctx context.Context, sessionID string, reason l1.ComputerTakeoverReason) (contract.ComputerControlReceipt, error) {
	err := tenure.Release(ctx, sessionID, reason)
	return contract.ComputerControlReceipt{}, err
}
func (*releaseOrderingTenure) Unregister(string) {}
func (*releaseOrderingTenure) controlTenure()    {}

func (auditor *recordingComputerAuditor) AppendComputerTakeoverAudit(
	ctx context.Context, _, _, _ string, request l1.ComputerTakeoverAuditRequest,
) (l1.ComputerTakeoverAuditReceipt, error) {
	auditor.mu.Lock()
	block := request.Event.Kind == auditor.blockKind
	started, release := auditor.blockStarted, auditor.blockRelease
	auditor.mu.Unlock()
	if block && release != nil {
		select {
		case started <- struct{}{}:
		default:
		}
		select {
		case <-release:
		case <-ctx.Done():
			return l1.ComputerTakeoverAuditReceipt{}, context.Cause(ctx)
		}
	}
	auditor.mu.Lock()
	defer auditor.mu.Unlock()
	auditor.events = append(auditor.events, request.Event)
	if auditor.changed != nil {
		close(auditor.changed)
	}
	auditor.changed = make(chan struct{})
	receiptEvent := request.Event
	receiptEvent.AuthorityGeneration = 1
	if auditor.mismatchReceipt {
		receiptEvent.EventID += ":different"
	}
	return l1.ComputerTakeoverAuditReceipt{Event: receiptEvent}, nil
}

func (auditor *recordingComputerAuditor) snapshot() []l1.ComputerTakeoverAuditEvent {
	auditor.mu.Lock()
	defer auditor.mu.Unlock()
	return append([]l1.ComputerTakeoverAuditEvent(nil), auditor.events...)
}

func waitComputerAuditKind(t *testing.T, auditor *recordingComputerAuditor, kind l1.ComputerTakeoverAuditEventKind) {
	t.Helper()
	for {
		auditor.mu.Lock()
		for _, event := range auditor.events {
			if event.Kind == kind {
				auditor.mu.Unlock()
				return
			}
		}
		if auditor.changed == nil {
			auditor.changed = make(chan struct{})
		}
		changed := auditor.changed
		auditor.mu.Unlock()
		select {
		case <-changed:
		case <-t.Context().Done():
			t.Fatalf("waiting for %s audit: %v; events: %#v", kind, context.Cause(t.Context()), auditor.snapshot())
		}
	}
}

type computerBackendOptions struct {
	requiredPath         string
	subprotocol          string
	noSubprotocol        bool
	textBanner           bool
	httpOnly             bool
	ignoreCloseHandshake bool
	echoPrefix           string
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
		if options.httpOnly {
			writer.WriteHeader(http.StatusOK)
			return
		}
		subprotocols := []string{subprotocol}
		if options.noSubprotocol {
			subprotocols = nil
		}
		connection, err := websocket.Accept(writer, request, &websocket.AcceptOptions{Subprotocols: subprotocols})
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
		if options.ignoreCloseHandshake {
			select {
			case <-request.Context().Done():
			case <-time.After(time.Second):
			}
			return
		}
		for {
			kind, payload, err := connection.Read(request.Context())
			if err != nil {
				return
			}
			if err := connection.Write(request.Context(), kind, append([]byte(options.echoPrefix), payload...)); err != nil {
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
