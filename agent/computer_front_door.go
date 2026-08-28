package agent

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/l1"
	"github.com/coder/websocket"
)

const (
	computerWebSocketPath                 = "/websockify"
	computerWebSocketSubprotocol          = "binary"
	computerRFBVersionBannerBytes         = 12
	DefaultComputerSessionCap             = time.Hour
	DefaultComputerIdentityRevalidation   = time.Minute
	DefaultComputerReadinessDeadline      = 60 * time.Second
	defaultComputerAuditFinalizationLimit = 30 * time.Second
)

type computerTakeoverAuditor interface {
	AppendComputerTakeoverAudit(context.Context, string, string, string, l1.ComputerTakeoverAuditRequest) (l1.ComputerTakeoverAuditReceipt, error)
}

type computerBackendDial func(context.Context) (net.Conn, error)

type computerFrontDoorConfig struct {
	authorityContext     context.Context
	fabric               fabric.Fabric
	authorizer           *ComputerPolicyCache
	auditor              computerTakeoverAuditor
	clock                Clock
	computerID           string
	jobID                string
	attemptID            string
	fencingToken         string
	viewDial             computerBackendDial
	controlDial          computerBackendDial
	sessionCap           time.Duration
	revalidationInterval time.Duration
	newSessionID         func() (string, error)
	onSession            func(*computerSessionHandle)
}

// computerSessionHandle is the narrow seam intentionally left for #179. A
// handle names one live admission, and only a control-authorized handle owns a
// session-bound capability that can dial control. Admission never invokes it.
type computerSessionHandle struct {
	id   string
	take *computerTakeCapability
}

func (handle *computerSessionHandle) ID() string {
	if handle == nil {
		return ""
	}
	return handle.id
}

func (handle *computerSessionHandle) CanTake() bool { return handle != nil && handle.take != nil }

func (handle *computerSessionHandle) dialControl(ctx context.Context) (net.Conn, error) {
	if !handle.CanTake() {
		return nil, errors.New("agent: Computer session is not authorized to take control")
	}
	return handle.take.dialControl(ctx)
}

type computerTakeCapability struct {
	session context.Context
	dial    computerBackendDial
}

func (capability *computerTakeCapability) dialControl(ctx context.Context) (net.Conn, error) {
	if capability == nil || capability.dial == nil {
		return nil, errors.New("agent: Computer session has no take capability")
	}
	select {
	case <-capability.session.Done():
		return nil, errors.New("agent: Computer session is no longer live")
	default:
	}
	return capability.dial(ctx)
}

type computerFrontDoor struct {
	config computerFrontDoorConfig
	errors chan error
}

// newComputerFrontDoor deliberately requires the deny-by-default policy cache.
// There is no constructor that can serve a Computer without authorization,
// per-connection Fabric identity, attempt cancellation, and durable audit.
func newComputerFrontDoor(config computerFrontDoorConfig) (*computerFrontDoor, error) {
	if config.authorityContext == nil {
		return nil, errors.New("agent: Computer front door requires attempt authority cancellation")
	}
	if config.fabric == nil {
		return nil, errors.New("agent: Computer front door requires Fabric identity")
	}
	if config.authorizer == nil {
		return nil, errors.New("agent: Computer front door requires the deny-by-default authorizer")
	}
	if config.auditor == nil {
		return nil, errors.New("agent: Computer front door requires durable take-over audit")
	}
	if config.computerID == "" || config.jobID == "" || config.attemptID == "" || config.fencingToken == "" {
		return nil, errors.New("agent: Computer front door requires complete attempt identity")
	}
	if config.viewDial == nil || config.controlDial == nil {
		return nil, errors.New("agent: Computer front door requires distinct view and control backend dialers")
	}
	if config.clock == nil {
		config.clock = systemClock{}
	}
	config.sessionCap = durationOrDefault(config.sessionCap, DefaultComputerSessionCap)
	if config.sessionCap > DefaultComputerSessionCap {
		return nil, errors.New("agent: Computer session cap cannot exceed one hour")
	}
	config.revalidationInterval = durationOrDefault(config.revalidationInterval, DefaultComputerIdentityRevalidation)
	if config.newSessionID == nil {
		config.newSessionID = newComputerSessionID
	}
	return &computerFrontDoor{config: config, errors: make(chan error, 16)}, nil
}

func (frontDoor *computerFrontDoor) Errors() <-chan error { return frontDoor.errors }

func (frontDoor *computerFrontDoor) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	identity, err := frontDoor.config.fabric.WhoIs(request.Context(), request.RemoteAddr)
	if err != nil {
		frontDoor.recordDenial(request.Context(), fabric.Identity{}, "identity_unavailable")
		http.Error(writer, "Fabric identity could not be authenticated", http.StatusUnauthorized)
		return
	}
	if request.URL.Path != computerWebSocketPath || request.URL.RawQuery != "" || request.Method != http.MethodGet {
		frontDoor.recordDenial(request.Context(), identity, "invalid_request_path")
		http.Error(writer, "Computer display path is fixed", http.StatusNotFound)
		return
	}
	if hasComputerAuthorityOverride(request) {
		frontDoor.recordDenial(request.Context(), identity, "client_authority_override")
		http.Error(writer, "client-supplied Computer authority is forbidden", http.StatusBadRequest)
		return
	}
	if !exactComputerSubprotocol(request.Header.Values("Sec-WebSocket-Protocol")) {
		frontDoor.recordDenial(request.Context(), identity, "invalid_subprotocol")
		http.Error(writer, "Computer display requires the binary WebSocket subprotocol", http.StatusBadRequest)
		return
	}
	authorization, err := frontDoor.config.authorizer.AcquireGrant(frontDoor.config.computerID, identity)
	if err != nil || authorization == nil {
		frontDoor.recordDenial(request.Context(), identity, "unauthorized_identity")
		http.Error(writer, "Computer access is not authorized", http.StatusForbidden)
		return
	}
	frontDoor.serveAuthorized(writer, request, identity, authorization)
}

func (frontDoor *computerFrontDoor) serveAuthorized(
	writer http.ResponseWriter,
	request *http.Request,
	identity fabric.Identity,
	authorization *ComputerGrantAuthorization,
) {
	defer authorization.Release()
	sessionID, err := frontDoor.config.newSessionID()
	if err != nil {
		frontDoor.report(fmt.Errorf("generate Computer session identity: %w", err))
		http.Error(writer, "Computer session unavailable", http.StatusServiceUnavailable)
		return
	}
	select {
	case <-frontDoor.config.authorityContext.Done():
		frontDoor.recordDenial(request.Context(), identity, "attempt_authority_lost")
		http.Error(writer, "Computer attempt authority is unavailable", http.StatusServiceUnavailable)
		return
	case revocation := <-authorization.Revocations():
		frontDoor.recordDenial(request.Context(), identity, string(revocation.Reason))
		http.Error(writer, "Computer access was revoked", http.StatusForbidden)
		return
	default:
	}

	sessionContext, cancelSession := context.WithCancelCause(frontDoor.config.authorityContext)
	defer cancelSession(nil)
	backend, backendWebSocket, banner, err := dialComputerBackend(sessionContext, frontDoor.config.viewDial)
	if err != nil {
		frontDoor.recordDenial(request.Context(), identity, "view_backend_unavailable")
		frontDoor.report(fmt.Errorf("dial Computer view backend: %w", err))
		http.Error(writer, "Computer display unavailable", http.StatusServiceUnavailable)
		return
	}
	defer backendWebSocket.CloseNow()

	clientWebSocket, err := websocket.Accept(writer, request, &websocket.AcceptOptions{
		Subprotocols: []string{computerWebSocketSubprotocol},
	})
	if err != nil {
		_ = backend.Close()
		frontDoor.recordDenial(request.Context(), identity, "client_upgrade_failed")
		frontDoor.report(fmt.Errorf("upgrade Computer display client: %w", err))
		return
	}
	defer clientWebSocket.CloseNow()
	client := websocket.NetConn(sessionContext, clientWebSocket, websocket.MessageBinary)
	defer client.Close()
	openedAt := wallNow(frontDoor.config.clock)
	baseEvent := frontDoor.sessionEvent(identity, authorization, sessionID, openedAt)
	open := baseEvent
	open.EventID = sessionID + ":open"
	open.Kind = l1.ComputerTakeoverSessionOpen
	if err := frontDoor.record(request.Context(), open); err != nil {
		frontDoor.report(fmt.Errorf("record Computer session open: %w", err))
		_ = clientWebSocket.CloseNow()
		_ = backendWebSocket.CloseNow()
		return
	}
	if _, err := client.Write(banner); err != nil {
		_ = backend.Close()
		frontDoor.finishSession(request.Context(), baseEvent, "client_closed")
		return
	}

	handle := &computerSessionHandle{id: sessionID}
	if authorization.CanTake() {
		handle.take = &computerTakeCapability{session: sessionContext, dial: frontDoor.config.controlDial}
	}
	if frontDoor.config.onSession != nil {
		frontDoor.config.onSession(handle)
	}

	reason := frontDoor.relay(sessionContext, cancelSession, request.RemoteAddr, identity, authorization, client, backend)
	_ = clientWebSocket.CloseNow()
	_ = backendWebSocket.CloseNow()
	frontDoor.finishSession(request.Context(), baseEvent, reason)
}

func (frontDoor *computerFrontDoor) relay(
	ctx context.Context,
	cancel context.CancelCauseFunc,
	remoteAddress string,
	identity fabric.Identity,
	authorization *ComputerGrantAuthorization,
	client, backend net.Conn,
) string {
	closed := make(chan string, 2)
	var copies sync.WaitGroup
	copies.Add(2)
	go copyComputerRelay(&copies, backend, client, "client_closed", closed)
	go copyComputerRelay(&copies, client, backend, "view_backend_closed", closed)
	revalidation := make(chan string, 1)
	go frontDoor.revalidateIdentity(ctx, remoteAddress, identity, revalidation)
	capTimer := frontDoor.config.clock.NewTimer(frontDoor.config.sessionCap)
	defer stopTimer(capTimer)
	reason := "session_closed"
	select {
	case <-ctx.Done():
		reason = "attempt_authority_lost"
	case revocation := <-authorization.Revocations():
		reason = string(revocation.Reason)
	case reason = <-revalidation:
	case reason = <-closed:
	case <-capTimer.C():
		reason = "session_cap_expired"
	}
	cancel(errors.New(reason))
	_ = client.Close()
	_ = backend.Close()
	copies.Wait()
	return reason
}

func (frontDoor *computerFrontDoor) revalidateIdentity(ctx context.Context, remoteAddress string, original fabric.Identity, failed chan<- string) {
	for {
		timer := frontDoor.config.clock.NewTimer(frontDoor.config.revalidationInterval)
		select {
		case <-ctx.Done():
			stopTimer(timer)
			return
		case <-timer.C():
		}
		identity, err := frontDoor.config.fabric.WhoIs(ctx, remoteAddress)
		if err != nil {
			select {
			case failed <- "identity_revalidation_failed":
			case <-ctx.Done():
			}
			return
		}
		if identity.Kind != original.Kind || identity.FabricID != original.FabricID ||
			identity.UserID != original.UserID || identity.DeviceID != original.DeviceID {
			select {
			case failed <- "identity_changed":
			case <-ctx.Done():
			}
			return
		}
	}
}

func copyComputerRelay(wait *sync.WaitGroup, destination, source net.Conn, reason string, closed chan<- string) {
	defer wait.Done()
	_, _ = io.Copy(destination, source)
	select {
	case closed <- reason:
	default:
	}
}

func (frontDoor *computerFrontDoor) sessionEvent(identity fabric.Identity, authorization *ComputerGrantAuthorization, sessionID string, occurred time.Time) l1.ComputerTakeoverAuditEvent {
	return l1.ComputerTakeoverAuditEvent{
		ComputerID: frontDoor.config.computerID, JobID: frontDoor.config.jobID,
		AttemptID: frontDoor.config.attemptID, SessionID: sessionID,
		FabricID: identity.FabricID, UserID: identity.UserID, DeviceID: identity.DeviceID,
		AuthorizedRole: authorization.decision.Permission, AdmittedMode: "view",
		PolicyRevision: authorization.decision.PolicyRevision, OccurredAt: occurred,
	}
}

func (frontDoor *computerFrontDoor) finishSession(ctx context.Context, base l1.ComputerTakeoverAuditEvent, reason string) {
	closeEvent := base
	closeEvent.EventID = base.SessionID + ":close"
	closeEvent.Kind = l1.ComputerTakeoverSessionClose
	closeEvent.OccurredAt = wallNow(frontDoor.config.clock)
	closeEvent.Reason = reason
	auditContext, cancel := context.WithTimeout(context.WithoutCancel(ctx), defaultComputerAuditFinalizationLimit)
	defer cancel()
	if err := frontDoor.record(auditContext, closeEvent); err != nil {
		frontDoor.report(fmt.Errorf("record Computer session close: %w", err))
	}
}

func (frontDoor *computerFrontDoor) recordDenial(ctx context.Context, identity fabric.Identity, reason string) {
	eventID, err := frontDoor.config.newSessionID()
	if err != nil {
		frontDoor.report(fmt.Errorf("generate Computer denial identity: %w", err))
		return
	}
	event := l1.ComputerTakeoverAuditEvent{
		EventID: eventID + ":denied", Kind: l1.ComputerTakeoverAdmissionDenied,
		ComputerID: frontDoor.config.computerID, JobID: frontDoor.config.jobID, AttemptID: frontDoor.config.attemptID,
		FabricID: identity.FabricID, UserID: identity.UserID, DeviceID: identity.DeviceID,
		PolicyRevision: frontDoor.config.authorizer.Revision(), OccurredAt: wallNow(frontDoor.config.clock), Reason: reason,
	}
	if err := frontDoor.record(ctx, event); err != nil {
		frontDoor.report(fmt.Errorf("record Computer admission denial: %w", err))
	}
}

func (frontDoor *computerFrontDoor) record(ctx context.Context, event l1.ComputerTakeoverAuditEvent) error {
	_, err := frontDoor.config.auditor.AppendComputerTakeoverAudit(
		ctx, frontDoor.config.computerID, frontDoor.config.jobID, frontDoor.config.attemptID,
		l1.ComputerTakeoverAuditRequest{FencingToken: frontDoor.config.fencingToken, Event: event},
	)
	return err
}

func (frontDoor *computerFrontDoor) report(err error) {
	select {
	case frontDoor.errors <- err:
	default:
	}
}

func hasComputerAuthorityOverride(request *http.Request) bool {
	for name := range request.Header {
		lower := strings.ToLower(name)
		if strings.HasPrefix(lower, "x-wefty-") && (strings.Contains(lower, "role") ||
			strings.Contains(lower, "mode") || strings.Contains(lower, "backend") || strings.Contains(lower, "control")) {
			return true
		}
	}
	return false
}

func exactComputerSubprotocol(values []string) bool {
	var protocols []string
	for _, value := range values {
		for _, protocol := range strings.Split(value, ",") {
			protocols = append(protocols, strings.TrimSpace(protocol))
		}
	}
	return len(protocols) == 1 && protocols[0] == computerWebSocketSubprotocol
}

func newComputerSessionID() (string, error) {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "", err
	}
	return "takeover_" + hex.EncodeToString(value[:]), nil
}

func dialComputerBackend(ctx context.Context, dial computerBackendDial) (net.Conn, *websocket.Conn, []byte, error) {
	var mu sync.Mutex
	used := false
	transport := &http.Transport{DialContext: func(dialContext context.Context, _, _ string) (net.Conn, error) {
		mu.Lock()
		defer mu.Unlock()
		if used {
			return nil, errors.New("Computer backend handshake attempted more than one dial")
		}
		used = true
		return dial(dialContext)
	}}
	defer transport.CloseIdleConnections()
	connection, _, err := websocket.Dial(ctx, "ws://computer-backend.invalid"+computerWebSocketPath, &websocket.DialOptions{
		HTTPClient: &http.Client{Transport: transport}, Subprotocols: []string{computerWebSocketSubprotocol},
	})
	if err != nil {
		return nil, nil, nil, err
	}
	if connection.Subprotocol() != computerWebSocketSubprotocol {
		_ = connection.CloseNow()
		return nil, nil, nil, errors.New("Computer backend did not negotiate the binary subprotocol")
	}
	network := websocket.NetConn(ctx, connection, websocket.MessageBinary)
	banner := make([]byte, computerRFBVersionBannerBytes)
	if _, err := io.ReadFull(network, banner); err != nil {
		_ = connection.CloseNow()
		return nil, nil, nil, fmt.Errorf("read Computer RFB version banner: %w", err)
	}
	if !validRFBVersionBanner(banner) {
		_ = connection.CloseNow()
		return nil, nil, nil, fmt.Errorf("invalid Computer RFB version banner %q", banner)
	}
	return network, connection, banner, nil
}

func validRFBVersionBanner(banner []byte) bool {
	if len(banner) != computerRFBVersionBannerBytes || string(banner[:4]) != "RFB " || banner[7] != '.' || banner[11] != '\n' {
		return false
	}
	for _, index := range []int{4, 5, 6, 8, 9, 10} {
		if banner[index] < '0' || banner[index] > '9' {
			return false
		}
	}
	return true
}

type computerReadinessError struct {
	Code contract.SpawnFailureCode
	Err  error
}

func (failure *computerReadinessError) Error() string { return failure.Err.Error() }
func (failure *computerReadinessError) Unwrap() error { return failure.Err }

// probeComputerBackends proves the exact wire contract on both helper tunnels.
// Both probes must succeed before a caller may publish either one.
func probeComputerBackends(ctx context.Context, clock Clock, startedAt time.Time, viewDial, controlDial computerBackendDial) error {
	if clock == nil {
		clock = systemClock{}
	}
	remaining := DefaultComputerReadinessDeadline - clock.Now().Sub(startedAt)
	if remaining <= 0 {
		return &computerReadinessError{Code: contract.SpawnFailureStartupReadinessTimeout,
			Err: errors.New("Computer display startup readiness exceeded 60 seconds from Started")}
	}
	probeContext, cancel := context.WithCancel(ctx)
	defer cancel()
	results := make(chan error, 2)
	for _, dial := range []computerBackendDial{viewDial, controlDial} {
		go func(dial computerBackendDial) {
			connection, websocketConnection, _, err := dialComputerBackend(probeContext, dial)
			if connection != nil {
				_ = connection.Close()
			}
			if websocketConnection != nil {
				_ = websocketConnection.CloseNow()
			}
			results <- err
		}(dial)
	}
	timer := clock.NewTimer(remaining)
	defer stopTimer(timer)
	for range 2 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C():
			cancel()
			return &computerReadinessError{Code: contract.SpawnFailureStartupReadinessTimeout,
				Err: errors.New("Computer display startup readiness exceeded 60 seconds from Started")}
		case err := <-results:
			if err != nil {
				cancel()
				return &computerReadinessError{Code: contract.SpawnFailureRuntimeUnavailable,
					Err: fmt.Errorf("Computer display readiness: %w", err)}
			}
		}
	}
	return nil
}
