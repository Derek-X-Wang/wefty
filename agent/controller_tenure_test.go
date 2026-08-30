package agent

import (
	"bytes"
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/l1"
	workloadrunner "github.com/Derek-X-Wang/wefty/runner"
	"github.com/coder/websocket"
)

func TestControllerTenureFirstDriverRetainsWheelAndOperationsAreSessionBound(t *testing.T) {
	fixture := newControllerTenureFixture(t)
	viewer := fixture.register(t, "viewer", false, false)
	first := fixture.register(t, "first", true, false)
	second := fixture.register(t, "second", true, false)

	if _, err := fixture.tenure.Take(t.Context(), viewer.id); tenureCode(err) != ComputerTenureUnauthorized {
		t.Fatalf("view-only take error = %v", err)
	}
	if _, err := fixture.tenure.Take(t.Context(), "not-admitted"); tenureCode(err) != ComputerTenureSessionEnded {
		t.Fatalf("different-session take error = %v", err)
	}

	start := make(chan struct{})
	results := make(chan error, 2)
	connections := make(chan net.Conn, 2)
	for _, session := range []controlTenureSession{first, second} {
		session := session
		go func() {
			<-start
			connection, err := fixture.tenure.Take(t.Context(), session.id)
			if connection != nil {
				connections <- connection
			}
			results <- err
		}()
	}
	close(start)
	var succeeded, busy int
	for range 2 {
		if err := <-results; err == nil {
			succeeded++
		} else if tenureCode(err) == ComputerTenureBusy {
			busy++
		} else {
			t.Fatalf("concurrent take error = %v", err)
		}
	}
	if succeeded != 1 || busy != 1 || fixture.controlDials != 1 {
		t.Fatalf("concurrent takes succeeded/busy/dials = %d/%d/%d", succeeded, busy, fixture.controlDials)
	}
	winner := <-connections
	if winner == nil || fixture.signalSnapshot()[0] != true {
		t.Fatalf("winning take did not set truthful signal: signals=%v conn=%v", fixture.signalSnapshot(), winner)
	}
	heldSession := first.id
	if fixture.tenure.held.session.id == second.id {
		heldSession = second.id
	}
	loser := second.id
	if heldSession == second.id {
		loser = first.id
	}
	nonholderReceipt, err := fixture.tenure.ReleaseReceipt(t.Context(), loser, l1.ComputerTakeoverExplicitRelease)
	if err != nil {
		t.Fatal(err)
	}
	if nonholderReceipt.TenureState != contract.ComputerControlTenureHeld || nonholderReceipt.HolderSessionID != heldSession || !nonholderReceipt.HumanDriving {
		t.Fatalf("non-holder release receipt = %#v", nonholderReceipt)
	}
	if len(fixture.signalSnapshot()) != 1 {
		t.Fatalf("non-holder release changed signal: %v", fixture.signalSnapshot())
	}
	if err := fixture.tenure.Release(t.Context(), heldSession, l1.ComputerTakeoverExplicitRelease); err != nil {
		t.Fatal(err)
	}
	if got := fixture.signalSnapshot(); len(got) != 2 || got[0] != true || got[1] != false {
		t.Fatalf("holder release signal writes = %v", got)
	}
}

func TestControllerTenureAlreadyHeldAndConnectionCloseRelease(t *testing.T) {
	fixture := newControllerTenureFixture(t)
	session := fixture.register(t, "driver", true, false)
	connection, err := fixture.tenure.Take(t.Context(), session.id)
	if err != nil {
		t.Fatal(err)
	}
	if another, err := fixture.tenure.Take(t.Context(), session.id); another != nil || tenureCode(err) != ComputerTenureAlreadyHeld {
		t.Fatalf("second take = conn=%v err=%v", another, err)
	}
	if err := connection.Close(); err != nil {
		t.Fatal(err)
	}
	waitForControllerFree(t, fixture.tenure)
	events := fixture.eventSnapshot()
	if events[len(events)-1].Kind != l1.ComputerTakeoverControlReleased || events[len(events)-1].Reason != l1.ComputerTakeoverExplicitRelease {
		t.Fatalf("connection close evidence = %#v", events)
	}
}

func TestControllerTenureSerializesAdminOverridesWithoutBlockingOldRelease(t *testing.T) {
	fixture := newControllerTenureFixture(t)
	first := fixture.register(t, "first", true, false)
	adminOne := fixture.register(t, "admin-one", true, true)
	adminTwo := fixture.register(t, "admin-two", true, true)
	if _, err := fixture.tenure.Take(t.Context(), first.id); err != nil {
		t.Fatal(err)
	}
	fixture.dialGate = make(chan struct{})
	fixture.dialStarted = make(chan struct{}, 1)
	result := make(chan error, 1)
	go func() {
		_, err := fixture.tenure.Take(t.Context(), adminOne.id)
		result <- err
	}()
	select {
	case <-fixture.dialStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("admin override did not reach the bounded control dial")
	}
	if _, err := fixture.tenure.Take(t.Context(), adminTwo.id); tenureCode(err) != ComputerTenureBusy {
		t.Fatalf("concurrent admin override error = %v", err)
	}
	released := make(chan error, 1)
	go func() { released <- fixture.tenure.Release(t.Context(), first.id, l1.ComputerTakeoverRevoked) }()
	select {
	case err := <-released:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("old session release blocked behind replacement dial")
	}
	close(fixture.dialGate)
	if err := <-result; err != nil {
		t.Fatalf("serialized admin override failed: %v", err)
	}
}

func TestControllerTenureSignalLeadsControlAndFailedBackendReturnsFree(t *testing.T) {
	fixture := newControllerTenureFixture(t)
	session := fixture.register(t, "driver", true, false)
	fixture.failDial = true
	if _, err := fixture.tenure.Take(t.Context(), session.id); err == nil {
		t.Fatal("control backend failure acquired tenure")
	}
	if fixture.tenure.held != nil {
		t.Fatalf("failed backend retained tenure: %#v", fixture.tenure.held)
	}
	if got := fixture.actionSnapshot(); len(got) < 3 || got[0] != "signal:true" || got[1] != "dial:control" || got[2] != "signal:false" {
		t.Fatalf("failed take ordering = %v", got)
	}
	fixture.failDial = false
	connection, err := fixture.tenure.Take(t.Context(), session.id)
	if err != nil || connection == nil {
		t.Fatalf("take after failed backend = conn=%v err=%v", connection, err)
	}
	actions := fixture.actionSnapshot()
	if actions[len(actions)-3] != "signal:true" || actions[len(actions)-2] != "dial:control" || actions[len(actions)-1] != "audit:control_acquired" {
		t.Fatalf("successful signal/dial/audit ordering = %v", actions)
	}
	if err := fixture.tenure.Release(t.Context(), session.id, l1.ComputerTakeoverExplicitRelease); err != nil {
		t.Fatal(err)
	}
}

func TestControllerTenureSignalAndEvidenceFailuresDenyControl(t *testing.T) {
	fixture := newControllerTenureFixture(t)
	session := fixture.register(t, "driver", true, false)
	fixture.failSet = true
	if _, err := fixture.tenure.Take(t.Context(), session.id); err == nil {
		t.Fatal("signal-set failure acquired control")
	}
	if fixture.controlDials != 0 || fixture.tenure.held != nil {
		t.Fatalf("signal-set failure dialed/held control = %d/%#v", fixture.controlDials, fixture.tenure.held)
	}

	fixture.failSet = false
	fixture.mismatchReceipt = true
	if _, err := fixture.tenure.Take(t.Context(), session.id); err == nil {
		t.Fatal("non-asserting evidence receipt acquired control")
	}
	if fixture.tenure.held != nil {
		t.Fatalf("non-asserting evidence retained tenure: %#v", fixture.tenure.held)
	}
	if got := fixture.signalSnapshot(); len(got) != 3 || !got[0] || !got[1] || got[2] {
		t.Fatalf("signal/evidence failure writes = %v", got)
	}
}

func TestControllerTenureAcceptsSemanticReceiptFromRealL1Store(t *testing.T) {
	store, err := l1.OpenStore(filepath.Join(t.TempDir(), "l1.sqlite"), l1.StoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	nodeID := "computer-node"
	fabricNodeID := "fabric-computer-node"
	node, err := store.RegisterNode(t.Context(), fabric.Identity{NodeID: fabricNodeID}, contract.NodeRegistration{
		NodeID: nodeID, BootSessionID: "boot-computer-node", RootInstanceID: "root-computer-node",
		OS: "linux", Architecture: "amd64", AgentVersion: "test",
		Capabilities:       map[string]bool{"kind:oci": true, "cgroup_v2": true, "computer": true},
		CapabilityRevision: 1, CapabilityObservedAt: time.Now(), MissingCapabilities: []string{},
	}, l1.NodePolicy{Tags: []string{contract.StableNodeTagPrefix + nodeID}, MaxOneshotSlots: 1, MaxServiceSlots: 1}, true)
	if err != nil {
		t.Fatal(err)
	}
	digest := "sha256:" + strings.Repeat("a", 64)
	memoryBytes := int64(64 << 20)
	computer, _, err := store.CreateComputer(t.Context(), l1.CreateComputerRequest{
		Name: "receipt-computer", Actor: "operator",
		Spec: contract.JobSpec{
			SchemaVersion: contract.SchemaVersionV1, DispatchKey: "computer:receipt", Kind: contract.JobKindOCI,
			Class: contract.JobClassService, Restart: contract.RestartAlways,
			RoutingTags: []string{contract.StableNodeTagPrefix + nodeID},
			Execution: contract.ExecutionSpec{OCI: &contract.OCIExecutionSpec{
				Image:  contract.OCIImageSpec{Reference: "ghcr.io/example/computer:v1", Digest: &digest},
				Limits: &contract.OCILimits{MemoryBytes: &memoryBytes},
				Computer: &contract.OCIComputerSpec{
					Display:   contract.OCIComputerDisplaySpec{Protocol: contract.ComputerDisplayProtocolRFBWebSocketV1},
					DiskBytes: 1 << 30,
				},
			}},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	claim, err := store.ClaimJob(t.Context(), fabricNodeID, node.NodeID, node.BootSessionID, contract.JobClassService)
	if err != nil || claim == nil {
		t.Fatalf("claim = %#v err=%v", claim, err)
	}

	backend := newComputerBackend(t, computerBackendOptions{})
	defer backend.Close()
	localClock := newManualClock(time.Date(2026, 8, 28, 5, 0, 0, 123, time.FixedZone("receipt-local", -7*60*60)))
	tenure, err := newControllerTenure(controllerTenureConfig{
		authorityContext: t.Context(), clock: localClock,
		dial:            func(ctx context.Context, _ string) (net.Conn, error) { return backend.dial(ctx) },
		setControlState: func(context.Context, bool) error { return nil },
		record: func(ctx context.Context, event l1.ComputerTakeoverAuditEvent) (l1.ComputerTakeoverAuditReceipt, error) {
			return store.AppendComputerTakeoverAudit(ctx, fabricNodeID, computer.ComputerID, claim.Job.JobID, claim.Lease.AttemptID,
				l1.ComputerTakeoverAuditRequest{FencingToken: claim.Lease.FencingToken, Event: event})
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	session := controlTenureSession{
		id: "real-l1-session", context: t.Context(), canTake: true,
		event: l1.ComputerTakeoverAuditEvent{
			ComputerID: computer.ComputerID, JobID: claim.Job.JobID, AttemptID: claim.Lease.AttemptID,
			SessionID: "real-l1-session", FabricID: "fabric-person", UserID: "person", DeviceID: "device",
			AuthorizedRole: l1.ComputerGrantControl, AdmittedMode: l1.ComputerAdmittedView, PolicyRevision: 1,
		},
	}
	if err := tenure.Register(session); err != nil {
		t.Fatal(err)
	}
	connection, err := tenure.Take(t.Context(), session.id)
	if err != nil || connection == nil {
		t.Fatalf("real L1 receipt take = conn=%v err=%v", connection, err)
	}
	if err := connection.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestControllerTenureControlLegOutlivesTakeActionButNotSession(t *testing.T) {
	fixture := newControllerTenureFixture(t)
	sessionContext, endSession := context.WithCancelCause(t.Context())
	session := fixture.session("driver", true, false, sessionContext)
	if err := fixture.tenure.Register(session); err != nil {
		t.Fatal(err)
	}
	actionContext, finishAction := context.WithCancel(t.Context())
	connection, err := fixture.tenure.Take(actionContext, session.id)
	if err != nil {
		t.Fatal(err)
	}
	finishAction()
	if _, err := connection.Write([]byte("still session bound")); err != nil {
		t.Fatalf("completed take action closed the control leg: %v", err)
	}
	endSession(&computerSessionEnd{reason: l1.ComputerTakeoverClientClosed})
	waitForControllerFree(t, fixture.tenure)
	if _, err := connection.Write([]byte("expired")); !errors.Is(err, net.ErrClosed) {
		t.Fatalf("ended session retained control leg: %v", err)
	}
}

func TestControllerTenureEndedSessionOwnsConcurrentBackendFailureReason(t *testing.T) {
	fixture := newControllerTenureFixture(t)
	sessionContext, endSession := context.WithCancelCause(t.Context())
	session := fixture.session("driver", true, false, sessionContext)
	backend, peer := net.Pipe()
	defer peer.Close()
	managed := newControllerConn(backend, nil, fixture.tenure, session.id)
	fixture.tenure.held = &controllerHolder{session: session, serial: 1, conn: managed}
	endSession(&computerSessionEnd{reason: l1.ComputerTakeoverAttemptAuthorityLost})

	if err := fixture.tenure.Release(t.Context(), session.id, l1.ComputerTakeoverControlBackendClosed); err != nil {
		t.Fatal(err)
	}
	events := fixture.eventSnapshot()
	if len(events) != 1 || events[0].Kind != l1.ComputerTakeoverControlReleased || events[0].Reason != l1.ComputerTakeoverAttemptAuthorityLost {
		t.Fatalf("ended-session backend release audit = %#v", events)
	}
}

func TestControllerTenureAdminOverrideObservesOldLegAndPreservesTrueSignal(t *testing.T) {
	fixture := newControllerTenureFixture(t)
	first := fixture.register(t, "first", true, false)
	admin := fixture.register(t, "admin", true, true)
	oldConnection, err := fixture.tenure.Take(t.Context(), first.id)
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := fixture.tenure.TakeReceipt(t.Context(), admin.id)
	if err != nil {
		t.Fatalf("admin override: %v", err)
	}
	if receipt.OverrideDisplacedSessionID != first.id || receipt.HolderSessionID != admin.id || !receipt.SignalStayedTrue || !receipt.HumanDriving {
		t.Fatalf("admin override receipt = %#v", receipt)
	}
	if _, err := oldConnection.Write([]byte("input")); !errors.Is(err, net.ErrClosed) {
		t.Fatalf("old input leg remained usable after override: %v", err)
	}
	if got := fixture.signalSnapshot(); len(got) != 1 || !got[0] {
		t.Fatalf("successful override toggled true signal: %v", got)
	}
	events := fixture.eventSnapshot()
	wantKinds := []l1.ComputerTakeoverAuditEventKind{
		l1.ComputerTakeoverControlAcquired,
		l1.ComputerTakeoverControlReleased,
		l1.ComputerTakeoverAdminOverrode,
		l1.ComputerTakeoverControlAcquired,
	}
	if len(events) != len(wantKinds) {
		t.Fatalf("override events = %#v", events)
	}
	for index, kind := range wantKinds {
		if events[index].Kind != kind {
			t.Fatalf("override event %d = %s, want %s", index, events[index].Kind, kind)
		}
	}
	if events[1].Reason != l1.ComputerTakeoverControllerOverridden || events[2].SessionID != admin.id {
		t.Fatalf("override evidence = %#v", events)
	}
	if err := fixture.tenure.Release(t.Context(), admin.id, l1.ComputerTakeoverExplicitRelease); err != nil {
		t.Fatal(err)
	}
}

func TestControllerTenureFailedOverrideClearsSignalAndReturnsFree(t *testing.T) {
	fixture := newControllerTenureFixture(t)
	first := fixture.register(t, "first", true, false)
	admin := fixture.register(t, "admin", true, true)
	if _, err := fixture.tenure.Take(t.Context(), first.id); err != nil {
		t.Fatal(err)
	}
	fixture.failDial = true
	receipt, err := fixture.tenure.TakeReceipt(t.Context(), admin.id)
	if err == nil {
		t.Fatal("failed replacement backend preserved tenure")
	}
	if receipt.TenureState != contract.ComputerControlTenureFree || receipt.HumanDriving || receipt.SignalStayedTrue ||
		receipt.OverrideDisplacedSessionID != first.id {
		t.Fatalf("failed replacement receipt = %#v", receipt)
	}
	if fixture.tenure.held != nil {
		t.Fatalf("failed override retained holder: %#v", fixture.tenure.held)
	}
	if got := fixture.signalSnapshot(); len(got) != 2 || !got[0] || got[1] {
		t.Fatalf("failed override signals = %v", got)
	}
	events := fixture.eventSnapshot()
	if events[len(events)-1].Kind != l1.ComputerTakeoverAdminOverrode || events[len(events)-1].Reason != l1.ComputerTakeoverControlBackendFailed {
		t.Fatalf("failed override audit = %#v", events)
	}
}

func TestControllerTenureSessionEndReleasesAndUnconfirmedClearReaps(t *testing.T) {
	fixture := newControllerTenureFixture(t)
	sessionContext, cancel := context.WithCancelCause(t.Context())
	session := fixture.session("expiring", true, false, sessionContext)
	if err := fixture.tenure.Register(session); err != nil {
		t.Fatal(err)
	}
	connection, err := fixture.tenure.Take(t.Context(), session.id)
	if err != nil {
		t.Fatal(err)
	}
	cancel(&computerSessionEnd{reason: l1.ComputerTakeoverSessionCapExpired})
	waitForControllerFree(t, fixture.tenure)
	if _, err := connection.Write([]byte("input")); !errors.Is(err, net.ErrClosed) {
		t.Fatalf("expired control leg remained open: %v", err)
	}
	events := fixture.eventSnapshot()
	if events[len(events)-1].Kind != l1.ComputerTakeoverControlReleased || events[len(events)-1].Reason != l1.ComputerTakeoverSessionCapExpired {
		t.Fatalf("expiry audit = %#v", events)
	}

	second := fixture.register(t, "revoked", true, false)
	if _, err := fixture.tenure.Take(t.Context(), second.id); err != nil {
		t.Fatal(err)
	}
	fixture.failClear = true
	if err := fixture.tenure.Release(t.Context(), second.id, l1.ComputerTakeoverRevoked); err == nil {
		t.Fatal("unconfirmed clear reported success")
	}
	select {
	case <-fixture.reaped:
	case <-time.After(5 * time.Second):
		t.Fatal("unconfirmed clear did not request attempt reap")
	}
}

func TestControllerTenureRetriesAndReportsBackgroundReleaseAuditFailure(t *testing.T) {
	fixture := newControllerTenureFixture(t)
	sessionContext, cancelSession := context.WithCancelCause(t.Context())
	session := fixture.session("driver", true, false, sessionContext)
	if err := fixture.tenure.Register(session); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.tenure.Take(t.Context(), session.id); err != nil {
		t.Fatal(err)
	}
	fixture.mu.Lock()
	fixture.failRecordKind = l1.ComputerTakeoverControlReleased
	fixture.mu.Unlock()
	cancelSession(&computerSessionEnd{reason: l1.ComputerTakeoverRevoked})
	select {
	case err := <-fixture.reported:
		if !strings.Contains(err.Error(), "control_released") {
			t.Fatalf("reported audit error = %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("background release audit failure was not surfaced")
	}
	events := fixture.eventSnapshot()
	attempts := 0
	for _, event := range events {
		if event.Kind == l1.ComputerTakeoverControlReleased {
			attempts++
		}
	}
	if attempts != computerAuditAttempts {
		t.Fatalf("release audit attempts = %d, want %d", attempts, computerAuditAttempts)
	}
}

func TestControllerTenureRestartDoesNotRestoreHeldState(t *testing.T) {
	old := newControllerTenureFixture(t)
	oldSession := old.register(t, "old", true, false)
	if _, err := old.tenure.Take(t.Context(), oldSession.id); err != nil {
		t.Fatal(err)
	}
	fresh := newControllerTenureFixture(t)
	if fresh.tenure.held != nil || len(fresh.tenure.live) != 0 {
		t.Fatalf("fresh agent restored tenure: held=%#v live=%v", fresh.tenure.held, fresh.tenure.live)
	}
	freshSession := fresh.register(t, "fresh", true, false)
	if _, err := fresh.tenure.Take(t.Context(), freshSession.id); err != nil {
		t.Fatalf("fresh process could not acquire Free tenure: %v", err)
	}
}

func TestComputerFrontDoorSidebandTakeAndReleaseReplaceRelayLeg(t *testing.T) {
	fixture, _, auditor, _, originalServer, _ := computerFrontDoorFixture(t, l1.ComputerGrantControl)
	originalServer.Close()
	viewBackend := newComputerBackend(t, computerBackendOptions{echoPrefix: "view:"})
	defer viewBackend.Close()
	controlBackend := newComputerBackend(t, computerBackendOptions{echoPrefix: "control:", rfbHandshake: true})
	defer controlBackend.Close()

	config := fixture.frontDoor.config
	config.dial = func(ctx context.Context, name string) (net.Conn, error) {
		switch name {
		case workloadrunner.AttemptEndpointView:
			return viewBackend.dial(ctx)
		case workloadrunner.AttemptEndpointControl:
			return controlBackend.dial(ctx)
		default:
			return nil, errors.New("unexpected endpoint")
		}
	}
	var signalMu sync.Mutex
	var signals []bool
	tenure, err := newControllerTenure(controllerTenureConfig{
		authorityContext: config.authorityContext,
		clock:            config.clock,
		dial:             config.dial,
		setControlState: func(_ context.Context, value bool) error {
			signalMu.Lock()
			signals = append(signals, value)
			signalMu.Unlock()
			return nil
		},
		record: func(ctx context.Context, event l1.ComputerTakeoverAuditEvent) (l1.ComputerTakeoverAuditReceipt, error) {
			return auditor.AppendComputerTakeoverAudit(ctx, config.computerID, config.jobID, config.attemptID,
				l1.ComputerTakeoverAuditRequest{FencingToken: config.fencingToken, Event: event})
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	config.controlTenure = tenure
	frontDoor, err := newComputerFrontDoor(config)
	if err != nil {
		t.Fatal(err)
	}
	tenure.config.report = frontDoor.report
	frontDoor.SetReady(true)
	server := httptest.NewServer(frontDoor)
	defer server.Close()

	client, token := dialComputerFrontDoorWithToken(t, server.URL, nil)
	defer client.CloseNow()
	if _, banner, err := client.Read(t.Context()); err != nil || string(banner) != "RFB 003.008\n" {
		t.Fatalf("banner = %q err=%v", banner, err)
	}
	assertRelayRoundTrip(t, client, "before", "view:before")
	if status := postComputerControl(t, server.URL, computerControlTakePath, token); status != http.StatusOK {
		t.Fatalf("take status = %d", status)
	}
	completeControlRFBHandshake(t, client)
	assertRelayRoundTrip(t, client, "driving", "control:driving")
	if status := postComputerControl(t, server.URL, computerControlReleasePath, token); status != http.StatusOK {
		t.Fatalf("release status = %d", status)
	}
	assertRelayRoundTrip(t, client, "after", "view:after")

	signalMu.Lock()
	gotSignals := append([]bool(nil), signals...)
	signalMu.Unlock()
	if len(gotSignals) != 2 || !gotSignals[0] || gotSignals[1] {
		t.Fatalf("sideband signal writes = %v", gotSignals)
	}
	events := auditor.snapshot()
	var acquired, released bool
	for _, event := range events {
		acquired = acquired || event.Kind == l1.ComputerTakeoverControlAcquired
		released = released || event.Kind == l1.ComputerTakeoverControlReleased && event.Reason == l1.ComputerTakeoverExplicitRelease
	}
	if !acquired || !released {
		t.Fatalf("sideband evidence = %#v", events)
	}
}

func completeControlRFBHandshake(t *testing.T, connection *websocket.Conn) {
	t.Helper()
	if err := connection.Write(t.Context(), websocket.MessageBinary, []byte("RFB 003.008\n")); err != nil {
		t.Fatal(err)
	}
	if kind, securityTypes, err := connection.Read(t.Context()); err != nil || kind != websocket.MessageBinary || !bytes.Equal(securityTypes, []byte{1, 1}) {
		t.Fatalf("control RFB security types = %x kind=%v err=%v", securityTypes, kind, err)
	}
	if err := connection.Write(t.Context(), websocket.MessageBinary, []byte{1}); err != nil {
		t.Fatal(err)
	}
	if kind, result, err := connection.Read(t.Context()); err != nil || kind != websocket.MessageBinary || !bytes.Equal(result, []byte{0, 0, 0, 0}) {
		t.Fatalf("control RFB security result = %x kind=%v err=%v", result, kind, err)
	}
	if err := connection.Write(t.Context(), websocket.MessageBinary, []byte{1}); err != nil {
		t.Fatal(err)
	}
	if kind, serverInit, err := connection.Read(t.Context()); err != nil || kind != websocket.MessageBinary || len(serverInit) != 24 {
		t.Fatalf("control RFB server init = %x kind=%v err=%v", serverInit, kind, err)
	}
}

func assertRelayRoundTrip(t *testing.T, connection *websocket.Conn, input, want string) {
	t.Helper()
	if err := connection.Write(t.Context(), websocket.MessageBinary, []byte(input)); err != nil {
		t.Fatal(err)
	}
	kind, payload, err := connection.Read(t.Context())
	if err != nil || kind != websocket.MessageBinary || string(payload) != want {
		t.Fatalf("relay round trip = %q kind=%v err=%v, want %q", payload, kind, err, want)
	}
}

func TestComputerFrontDoorRevocationWhileDrivingClosesControlBeforeSession(t *testing.T) {
	fixture, cache, auditor, _, originalServer, identity := computerFrontDoorFixture(t, l1.ComputerGrantControl)
	originalServer.Close()
	controlBackend := newComputerBackend(t, computerBackendOptions{})
	defer controlBackend.Close()
	config := fixture.frontDoor.config
	originalDial := config.dial
	config.dial = func(ctx context.Context, name string) (net.Conn, error) {
		if name == workloadrunner.AttemptEndpointControl {
			return controlBackend.dial(ctx)
		}
		return originalDial(ctx, name)
	}
	var signalMu sync.Mutex
	var signals []bool
	tenure, err := newControllerTenure(controllerTenureConfig{
		authorityContext: config.authorityContext, clock: config.clock, dial: config.dial,
		setControlState: func(_ context.Context, value bool) error {
			signalMu.Lock()
			signals = append(signals, value)
			signalMu.Unlock()
			return nil
		},
		record: func(ctx context.Context, event l1.ComputerTakeoverAuditEvent) (l1.ComputerTakeoverAuditReceipt, error) {
			return auditor.AppendComputerTakeoverAudit(ctx, config.computerID, config.jobID, config.attemptID,
				l1.ComputerTakeoverAuditRequest{FencingToken: config.fencingToken, Event: event})
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	config.controlTenure = tenure
	frontDoor, err := newComputerFrontDoor(config)
	if err != nil {
		t.Fatal(err)
	}
	frontDoor.SetReady(true)
	server := httptest.NewServer(frontDoor)
	defer server.Close()
	connection, token := dialComputerFrontDoorWithToken(t, server.URL, nil)
	defer connection.CloseNow()
	if kind, banner, err := connection.Read(t.Context()); err != nil || kind != websocket.MessageBinary || string(banner) != "RFB 003.008\n" {
		t.Fatalf("view banner = %q kind=%v err=%v", banner, kind, err)
	}
	if status := postComputerControl(t, server.URL, computerControlTakePath, token); status != http.StatusOK {
		t.Fatalf("take status = %d", status)
	}
	tenure.mu.Lock()
	control := net.Conn(tenure.held.conn)
	tenure.mu.Unlock()
	if _, err := cache.Install(policySnapshot(t, fixture.clock.Now().Add(time.Second), 1, 2, nil, l1.ComputerGrant{
		FabricID: identity.FabricID, UserID: identity.UserID, Permission: l1.ComputerGrantView, PolicyRevision: 2,
	})); err != nil {
		t.Fatal(err)
	}
	waitForControllerFree(t, tenure)
	if _, err := control.Write([]byte("input")); !errors.Is(err, net.ErrClosed) {
		t.Fatalf("revoked control leg remained open: %v", err)
	}
	waitComputerAuditKind(t, auditor, l1.ComputerTakeoverSessionClose)
	events := auditor.snapshot()
	var released, closed bool
	for _, event := range events {
		switch event.Kind {
		case l1.ComputerTakeoverControlReleased:
			released = event.Reason == l1.ComputerTakeoverRevoked
		case l1.ComputerTakeoverSessionClose:
			closed = event.Reason == l1.ComputerTakeoverRevoked
		}
	}
	if !released || !closed {
		t.Fatalf("revocation evidence = %#v", events)
	}
	signalMu.Lock()
	defer signalMu.Unlock()
	if len(signals) != 2 || !signals[0] || signals[1] {
		t.Fatalf("revocation signal writes = %v", signals)
	}
}

func TestComputerFrontDoorRegistersControlTokenBeforePublishingBanner(t *testing.T) {
	fixture, _, auditor, _, originalServer, _ := computerFrontDoorFixture(t, l1.ComputerGrantControl)
	originalServer.Close()
	controlBackend := newComputerBackend(t, computerBackendOptions{})
	defer controlBackend.Close()
	config := fixture.frontDoor.config
	originalDial := config.dial
	config.dial = func(ctx context.Context, name string) (net.Conn, error) {
		if name == workloadrunner.AttemptEndpointControl {
			return controlBackend.dial(ctx)
		}
		return originalDial(ctx, name)
	}
	tenure, err := newControllerTenure(controllerTenureConfig{
		authorityContext: config.authorityContext, clock: config.clock, dial: config.dial,
		setControlState: func(context.Context, bool) error { return nil },
		record: func(ctx context.Context, event l1.ComputerTakeoverAuditEvent) (l1.ComputerTakeoverAuditReceipt, error) {
			return auditor.AppendComputerTakeoverAudit(ctx, config.computerID, config.jobID, config.attemptID,
				l1.ComputerTakeoverAuditRequest{FencingToken: config.fencingToken, Event: event})
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	config.controlTenure = tenure
	registrationEntered := make(chan struct{})
	releaseRegistration := make(chan struct{})
	config.beforeControlSessionRegistration = func() {
		close(registrationEntered)
		<-releaseRegistration
	}
	frontDoor, err := newComputerFrontDoor(config)
	if err != nil {
		t.Fatal(err)
	}
	frontDoor.SetReady(true)
	server := httptest.NewServer(frontDoor)
	defer server.Close()
	connection, token := dialComputerFrontDoorWithToken(t, server.URL, nil)
	defer connection.CloseNow()
	type bannerResult struct {
		kind    websocket.MessageType
		payload []byte
		err     error
	}
	banner := make(chan bannerResult, 1)
	go func() {
		kind, payload, err := connection.Read(t.Context())
		banner <- bannerResult{kind: kind, payload: payload, err: err}
	}()
	<-registrationEntered
	var publishedBeforeRegistration bool
	select {
	case <-banner:
		publishedBeforeRegistration = true
	case <-time.After(100 * time.Millisecond):
	}
	close(releaseRegistration)
	if publishedBeforeRegistration {
		t.Fatal("Computer banner became actionable before control-token registration")
	}
	result := <-banner
	if result.err != nil || result.kind != websocket.MessageBinary || string(result.payload) != "RFB 003.008\n" {
		t.Fatalf("view banner = %q kind=%v err=%v", result.payload, result.kind, result.err)
	}
	if status := postComputerControl(t, server.URL, computerControlTakePath, token); status != http.StatusOK {
		t.Fatalf("immediate take status = %d", status)
	}
}

type controllerTenureFixture struct {
	testing         *testing.T
	tenure          *controllerTenure
	backend         *computerBackendServer
	clock           *manualClock
	reaped          chan error
	reported        chan error
	mu              sync.Mutex
	signals         []bool
	actions         []string
	events          []l1.ComputerTakeoverAuditEvent
	controlDials    int
	failDial        bool
	failSet         bool
	failClear       bool
	mismatchReceipt bool
	failRecordKind  l1.ComputerTakeoverAuditEventKind
	dialGate        chan struct{}
	dialStarted     chan struct{}
}

func newControllerTenureFixture(t *testing.T) *controllerTenureFixture {
	t.Helper()
	fixture := &controllerTenureFixture{testing: t, backend: newComputerBackend(t, computerBackendOptions{}),
		clock: newManualClock(time.Date(2026, 8, 28, 12, 0, 0, 0, time.UTC)), reaped: make(chan error, 1), reported: make(chan error, 4)}
	t.Cleanup(fixture.backend.Close)
	tenure, err := newControllerTenure(controllerTenureConfig{
		authorityContext: t.Context(), clock: fixture.clock,
		dial: func(ctx context.Context, name string) (net.Conn, error) {
			fixture.mu.Lock()
			fixture.controlDials++
			fixture.actions = append(fixture.actions, "dial:"+name)
			fail := fixture.failDial
			gate := fixture.dialGate
			started := fixture.dialStarted
			fixture.mu.Unlock()
			if name != workloadrunner.AttemptEndpointControl {
				return nil, errors.New("tenure dialed a non-control endpoint")
			}
			if fail {
				return nil, errors.New("injected control backend failure")
			}
			if gate != nil {
				select {
				case started <- struct{}{}:
				default:
				}
				select {
				case <-gate:
				case <-ctx.Done():
					return nil, context.Cause(ctx)
				}
			}
			return fixture.backend.dial(ctx)
		},
		setControlState: func(_ context.Context, value bool) error {
			fixture.mu.Lock()
			defer fixture.mu.Unlock()
			fixture.signals = append(fixture.signals, value)
			if value {
				fixture.actions = append(fixture.actions, "signal:true")
			} else {
				fixture.actions = append(fixture.actions, "signal:false")
			}
			if value && fixture.failSet {
				return errors.New("injected signal-set failure")
			}
			if !value && fixture.failClear {
				return errors.New("injected clear failure")
			}
			return nil
		},
		record: func(_ context.Context, event l1.ComputerTakeoverAuditEvent) (l1.ComputerTakeoverAuditReceipt, error) {
			fixture.mu.Lock()
			defer fixture.mu.Unlock()
			fixture.events = append(fixture.events, event)
			fixture.actions = append(fixture.actions, "audit:"+string(event.Kind))
			if event.Kind == fixture.failRecordKind {
				return l1.ComputerTakeoverAuditReceipt{}, errors.New("injected audit failure")
			}
			receiptEvent := event
			receiptEvent.AuthorityGeneration = 1
			receipt := l1.ComputerTakeoverAuditReceipt{Event: receiptEvent}
			if fixture.mismatchReceipt {
				receipt.Event.EventID += ":different"
			}
			return receipt, nil
		},
		report:             func(err error) { fixture.reported <- err },
		onUnconfirmedClear: func(err error) { fixture.reaped <- err },
	})
	if err != nil {
		t.Fatal(err)
	}
	fixture.tenure = tenure
	return fixture
}

func (fixture *controllerTenureFixture) session(id string, canTake, administrator bool, sessionContext context.Context) controlTenureSession {
	return controlTenureSession{id: id, context: sessionContext, canTake: canTake, administrator: administrator,
		event: l1.ComputerTakeoverAuditEvent{ComputerID: "computer-1", JobID: "job-1", AttemptID: "attempt-1", SessionID: id,
			FabricID: "fabric-1", UserID: "person-" + id, DeviceID: "device-" + id, AuthorizedRole: l1.ComputerGrantControl,
			AdmittedMode: l1.ComputerAdmittedView, PolicyRevision: 1, OccurredAt: fixture.clock.Now()}}
}

func (fixture *controllerTenureFixture) register(t *testing.T, id string, canTake, administrator bool) controlTenureSession {
	t.Helper()
	session := fixture.session(id, canTake, administrator, t.Context())
	if !canTake {
		session.event.AuthorizedRole = l1.ComputerGrantView
	}
	if err := fixture.tenure.Register(session); err != nil {
		t.Fatal(err)
	}
	return session
}

func (fixture *controllerTenureFixture) signalSnapshot() []bool {
	fixture.mu.Lock()
	defer fixture.mu.Unlock()
	return append([]bool(nil), fixture.signals...)
}

func (fixture *controllerTenureFixture) actionSnapshot() []string {
	fixture.mu.Lock()
	defer fixture.mu.Unlock()
	return append([]string(nil), fixture.actions...)
}

func (fixture *controllerTenureFixture) eventSnapshot() []l1.ComputerTakeoverAuditEvent {
	fixture.mu.Lock()
	defer fixture.mu.Unlock()
	return append([]l1.ComputerTakeoverAuditEvent(nil), fixture.events...)
}

func tenureCode(err error) ComputerTenureErrorCode {
	var failure *ComputerTenureError
	if errors.As(err, &failure) {
		return failure.Code
	}
	return ""
}

func waitForControllerFree(t *testing.T, tenure *controllerTenure) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		tenure.mu.Lock()
		free := tenure.held == nil && tenure.op == nil
		tenure.mu.Unlock()
		if free {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("timed out waiting for Controller tenure to return Free")
}
