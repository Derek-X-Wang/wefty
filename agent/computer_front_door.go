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
	workloadrunner "github.com/Derek-X-Wang/wefty/runner"
	"github.com/coder/websocket"
)

const (
	computerWebSocketPath                  = "/websockify"
	computerWebSocketSubprotocol           = "binary"
	computerRFBVersionBannerBytes          = 12
	DefaultComputerSessionCap              = time.Hour
	DefaultComputerIdentityRevalidation    = time.Minute
	DefaultComputerReadinessDeadline       = 60 * time.Second
	DefaultComputerReadinessProbeInterval  = 100 * time.Millisecond
	DefaultComputerReadinessConnectTimeout = 2 * time.Second
	DefaultComputerDenialFlushInterval     = time.Minute
	defaultComputerAuditFinalizationLimit  = 30 * time.Second
	maximumComputerDenialGroups            = 128
)

type computerTakeoverAuditor interface {
	AppendComputerTakeoverAudit(context.Context, string, string, string, l1.ComputerTakeoverAuditRequest) (l1.ComputerTakeoverAuditReceipt, error)
}

type computerEndpointDial func(context.Context, string) (net.Conn, error)

type ComputerTenureErrorCode string

const ComputerTenureUnavailable ComputerTenureErrorCode = "tenure_unavailable"

type ComputerTenureError struct {
	Code ComputerTenureErrorCode
}

func (failure *ComputerTenureError) Error() string {
	return "agent: Computer control tenure is unavailable"
}

// ControlTenure is sealed to package agent so only the #179 implementation
// can gain a control dial. Admission receives no control endpoint capability.
type ControlTenure interface {
	Take(context.Context, string) (net.Conn, error)
	controlTenure()
}

type unavailableControlTenure struct{}

func (unavailableControlTenure) Take(context.Context, string) (net.Conn, error) {
	return nil, &ComputerTenureError{Code: ComputerTenureUnavailable}
}
func (unavailableControlTenure) controlTenure() {}

type computerSessionEnd struct{ reason l1.ComputerTakeoverReason }

func (end *computerSessionEnd) Error() string { return string(end.reason) }

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
	dial                 computerEndpointDial
	controlTenure        ControlTenure
	sessionCap           time.Duration
	revalidationInterval time.Duration
	denialFlushInterval  time.Duration
	newSessionID         func() (string, error)
	onSession            func(*computerSessionHandle)
}

// computerSessionHandle is the narrow seam intentionally left for #179. A
// handle names one live admission, and only a control-authorized handle owns a
// session-bound capability that can dial control. Admission never invokes it.
type computerSessionHandle struct {
	id      string
	canTake bool
	tenure  ControlTenure
	session context.Context
}

func (handle *computerSessionHandle) ID() string {
	if handle == nil {
		return ""
	}
	return handle.id
}

func (handle *computerSessionHandle) CanTake() bool { return handle != nil && handle.canTake }

func (handle *computerSessionHandle) dialControl(ctx context.Context) (net.Conn, error) {
	if !handle.CanTake() {
		return nil, errors.New("agent: Computer session is not authorized to take control")
	}
	select {
	case <-handle.session.Done():
		return nil, errors.New("agent: Computer session is no longer live")
	default:
	}
	return handle.tenure.Take(ctx, handle.id)
}

type computerFrontDoor struct {
	config  computerFrontDoorConfig
	errors  chan error
	denials *computerDenialCoalescer
	mu      sync.Mutex
	ready   bool
	active  map[string]context.CancelCauseFunc
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
	if config.dial == nil {
		return nil, errors.New("agent: Computer front door requires an exact-authority endpoint dialer")
	}
	if config.clock == nil {
		config.clock = systemClock{}
	}
	config.sessionCap = durationOrDefault(config.sessionCap, DefaultComputerSessionCap)
	if config.sessionCap > DefaultComputerSessionCap {
		return nil, errors.New("agent: Computer session cap cannot exceed one hour")
	}
	config.revalidationInterval = durationOrDefault(config.revalidationInterval, DefaultComputerIdentityRevalidation)
	if config.revalidationInterval > DefaultComputerIdentityRevalidation {
		return nil, errors.New("agent: Computer identity revalidation cannot be less frequent than once per minute")
	}
	config.denialFlushInterval = durationOrDefault(config.denialFlushInterval, DefaultComputerDenialFlushInterval)
	if config.controlTenure == nil {
		config.controlTenure = unavailableControlTenure{}
	}
	if config.newSessionID == nil {
		config.newSessionID = newComputerSessionID
	}
	frontDoor := &computerFrontDoor{config: config, errors: make(chan error, 16), active: make(map[string]context.CancelCauseFunc)}
	frontDoor.denials = newComputerDenialCoalescer(frontDoor)
	go frontDoor.denials.run(config.authorityContext)
	return frontDoor, nil
}

func (frontDoor *computerFrontDoor) Errors() <-chan error { return frontDoor.errors }

func (frontDoor *computerFrontDoor) SetReady(ready bool) {
	frontDoor.mu.Lock()
	frontDoor.ready = ready
	if !ready {
		for _, cancel := range frontDoor.active {
			cancel(&computerSessionEnd{reason: l1.ComputerTakeoverViewBackendClosed})
		}
	}
	frontDoor.mu.Unlock()
}

func (frontDoor *computerFrontDoor) isReady() bool {
	frontDoor.mu.Lock()
	defer frontDoor.mu.Unlock()
	return frontDoor.ready
}

func (frontDoor *computerFrontDoor) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	admittedAt := frontDoor.config.clock.Now()
	if !frontDoor.isReady() {
		http.Error(writer, "Computer display is not ready", http.StatusServiceUnavailable)
		return
	}
	identity, err := frontDoor.config.fabric.WhoIs(request.Context(), request.RemoteAddr)
	if err != nil {
		frontDoor.recordPreAuthorizationDenial(fabric.Identity{}, l1.ComputerTakeoverIdentityUnavailable)
		http.Error(writer, "Fabric identity could not be authenticated", http.StatusUnauthorized)
		return
	}
	if request.URL.Path != computerWebSocketPath || request.URL.RawQuery != "" || request.Method != http.MethodGet {
		frontDoor.recordPreAuthorizationDenial(identity, l1.ComputerTakeoverInvalidRequestPath)
		http.Error(writer, "Computer display path is fixed", http.StatusNotFound)
		return
	}
	if !exactComputerSubprotocol(request.Header.Values("Sec-WebSocket-Protocol")) {
		frontDoor.recordPreAuthorizationDenial(identity, l1.ComputerTakeoverInvalidSubprotocol)
		http.Error(writer, "Computer display requires the binary WebSocket subprotocol", http.StatusBadRequest)
		return
	}
	authorization, err := frontDoor.config.authorizer.AcquireGrant(frontDoor.config.computerID, identity)
	if err != nil || authorization == nil {
		frontDoor.recordPreAuthorizationDenial(identity, l1.ComputerTakeoverUnauthorizedIdentity)
		http.Error(writer, "Computer access is not authorized", http.StatusForbidden)
		return
	}
	frontDoor.serveAuthorized(writer, request, identity, authorization, admittedAt)
}

func (frontDoor *computerFrontDoor) serveAuthorized(
	writer http.ResponseWriter,
	request *http.Request,
	identity fabric.Identity,
	authorization *ComputerGrantAuthorization,
	admittedAt time.Time,
) {
	released := false
	release := func() {
		if !released {
			authorization.Release()
			released = true
		}
	}
	defer release()
	sessionID, err := frontDoor.config.newSessionID()
	if err != nil {
		frontDoor.report(fmt.Errorf("generate Computer session identity: %w", err))
		http.Error(writer, "Computer session unavailable", http.StatusServiceUnavailable)
		return
	}
	select {
	case <-frontDoor.config.authorityContext.Done():
		frontDoor.recordDenial(request.Context(), identity, l1.ComputerTakeoverAttemptAuthorityLost)
		http.Error(writer, "Computer attempt authority is unavailable", http.StatusServiceUnavailable)
		return
	case <-authorization.Revocations():
		frontDoor.recordDenial(request.Context(), identity, l1.ComputerTakeoverRevoked)
		http.Error(writer, "Computer access was revoked", http.StatusForbidden)
		return
	default:
	}

	sessionContext, cancelSession := context.WithCancelCause(frontDoor.config.authorityContext)
	defer cancelSession(nil)
	frontDoor.mu.Lock()
	frontDoor.active[sessionID] = cancelSession
	frontDoor.mu.Unlock()
	defer func() {
		frontDoor.mu.Lock()
		delete(frontDoor.active, sessionID)
		frontDoor.mu.Unlock()
	}()
	backend, backendWebSocket, banner, err := dialComputerBackend(sessionContext, frontDoor.config.dial, workloadrunner.AttemptEndpointView)
	if err != nil {
		frontDoor.recordDenial(request.Context(), identity, l1.ComputerTakeoverViewBackendUnavailable)
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
		frontDoor.recordDenial(request.Context(), identity, l1.ComputerTakeoverClientUpgradeFailed)
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
		release()
		frontDoor.finishSession(request.Context(), baseEvent, l1.ComputerTakeoverClientClosed)
		return
	}

	handle := &computerSessionHandle{id: sessionID, canTake: authorization.CanTake(), tenure: frontDoor.config.controlTenure, session: sessionContext}
	if frontDoor.config.onSession != nil {
		frontDoor.config.onSession(handle)
	}

	reason := frontDoor.relay(sessionContext, cancelSession, request.RemoteAddr, identity, authorization, client, backend, admittedAt)
	_ = clientWebSocket.CloseNow()
	_ = backendWebSocket.CloseNow()
	release()
	frontDoor.finishSession(request.Context(), baseEvent, reason)
}

func (frontDoor *computerFrontDoor) relay(
	ctx context.Context,
	cancel context.CancelCauseFunc,
	remoteAddress string,
	identity fabric.Identity,
	authorization *ComputerGrantAuthorization,
	client, backend net.Conn,
	admittedAt time.Time,
) l1.ComputerTakeoverReason {
	closed := make(chan l1.ComputerTakeoverReason, 2)
	var copies sync.WaitGroup
	copies.Add(2)
	go copyComputerRelay(&copies, backend, client, "client_closed", closed)
	go copyComputerRelay(&copies, client, backend, "view_backend_closed", closed)
	revalidation := make(chan l1.ComputerTakeoverReason, 1)
	go frontDoor.revalidateIdentity(ctx, remoteAddress, identity, revalidation)
	remaining := frontDoor.config.sessionCap - frontDoor.config.clock.Now().Sub(admittedAt)
	if remaining < 0 {
		remaining = 0
	}
	capTimer := frontDoor.config.clock.NewTimer(remaining)
	defer stopTimer(capTimer)
	reason := l1.ComputerTakeoverClientClosed
	select {
	case <-ctx.Done():
		var ended *computerSessionEnd
		if errors.As(context.Cause(ctx), &ended) {
			reason = ended.reason
		} else {
			reason = l1.ComputerTakeoverAttemptAuthorityLost
		}
	case <-authorization.Revocations():
		reason = l1.ComputerTakeoverRevoked
	case reason = <-revalidation:
	case reason = <-closed:
	case <-capTimer.C():
		reason = l1.ComputerTakeoverSessionCapExpired
	}
	cancel(errors.New(string(reason)))
	_ = client.Close()
	_ = backend.Close()
	copies.Wait()
	return reason
}

func (frontDoor *computerFrontDoor) revalidateIdentity(ctx context.Context, remoteAddress string, original fabric.Identity, failed chan<- l1.ComputerTakeoverReason) {
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
			case failed <- l1.ComputerTakeoverRevalidationFailed:
			case <-ctx.Done():
			}
			return
		}
		if identity.Kind != original.Kind || identity.FabricID != original.FabricID ||
			identity.UserID != original.UserID || identity.DeviceID != original.DeviceID {
			select {
			case failed <- l1.ComputerTakeoverRevalidationFailed:
			case <-ctx.Done():
			}
			return
		}
	}
}

func copyComputerRelay(wait *sync.WaitGroup, destination, source net.Conn, reason l1.ComputerTakeoverReason, closed chan<- l1.ComputerTakeoverReason) {
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
		AuthorizedRole: authorization.AuthorizedRole(), AdmittedMode: l1.ComputerAdmittedMode(authorization.AdmissionRole()),
		PolicyRevision: authorization.PolicyRevision(), OccurredAt: occurred,
	}
}

func (frontDoor *computerFrontDoor) finishSession(ctx context.Context, base l1.ComputerTakeoverAuditEvent, reason l1.ComputerTakeoverReason) {
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

func (frontDoor *computerFrontDoor) recordDenial(ctx context.Context, identity fabric.Identity, reason l1.ComputerTakeoverReason) {
	eventID, err := frontDoor.config.newSessionID()
	if err != nil {
		frontDoor.report(fmt.Errorf("generate Computer denial identity: %w", err))
		return
	}
	event := l1.ComputerTakeoverAuditEvent{
		EventID: eventID + ":denied", Kind: l1.ComputerTakeoverAdmissionDenied,
		ComputerID: frontDoor.config.computerID, JobID: frontDoor.config.jobID, AttemptID: frontDoor.config.attemptID,
		FabricID: identity.FabricID, UserID: identity.UserID, DeviceID: identity.DeviceID,
		PolicyRevision: frontDoor.config.authorizer.Revision(), OccurredAt: wallNow(frontDoor.config.clock), Reason: reason, EventCount: 1,
	}
	if err := frontDoor.record(ctx, event); err != nil {
		frontDoor.report(fmt.Errorf("record Computer admission denial: %w", err))
	}
}

func (frontDoor *computerFrontDoor) recordPreAuthorizationDenial(identity fabric.Identity, reason l1.ComputerTakeoverReason) {
	if identity.Kind == fabric.IdentityKindMachine {
		identity = fabric.Identity{}
	}
	frontDoor.denials.add(identity, reason)
}

type computerDenialKey struct {
	reason                     l1.ComputerTakeoverReason
	fabricID, userID, deviceID string
}

type computerDenialSummary struct {
	key   computerDenialKey
	count int64
}

// computerDenialCoalescer keeps untrusted pre-authorization traffic off the
// synchronous agent-to-L1 path. Its bounded local map is periodically flushed
// as counted evidence, and attempt shutdown performs one best-effort flush.
type computerDenialCoalescer struct {
	frontDoor *computerFrontDoor
	mu        sync.Mutex
	counts    map[computerDenialKey]int64
}

func newComputerDenialCoalescer(frontDoor *computerFrontDoor) *computerDenialCoalescer {
	return &computerDenialCoalescer{frontDoor: frontDoor, counts: make(map[computerDenialKey]int64)}
}

func (coalescer *computerDenialCoalescer) add(identity fabric.Identity, reason l1.ComputerTakeoverReason) {
	key := computerDenialKey{reason: reason, fabricID: identity.FabricID, userID: identity.UserID, deviceID: identity.DeviceID}
	coalescer.mu.Lock()
	if _, exists := coalescer.counts[key]; !exists && len(coalescer.counts) >= maximumComputerDenialGroups {
		key = computerDenialKey{reason: reason}
	}
	coalescer.counts[key]++
	coalescer.mu.Unlock()
}

func (coalescer *computerDenialCoalescer) run(ctx context.Context) {
	for {
		timer := coalescer.frontDoor.config.clock.NewTimer(coalescer.frontDoor.config.denialFlushInterval)
		select {
		case <-ctx.Done():
			stopTimer(timer)
			flushContext, cancel := context.WithTimeout(context.WithoutCancel(ctx), defaultComputerAuditFinalizationLimit)
			coalescer.flush(flushContext)
			cancel()
			return
		case <-timer.C():
			coalescer.flush(ctx)
		}
	}
}

func (coalescer *computerDenialCoalescer) flush(ctx context.Context) {
	coalescer.mu.Lock()
	summaries := make([]computerDenialSummary, 0, len(coalescer.counts))
	for key, count := range coalescer.counts {
		summaries = append(summaries, computerDenialSummary{key: key, count: count})
	}
	clear(coalescer.counts)
	coalescer.mu.Unlock()
	for _, summary := range summaries {
		eventID, err := coalescer.frontDoor.config.newSessionID()
		if err != nil {
			coalescer.frontDoor.report(fmt.Errorf("generate Computer denial identity: %w", err))
			continue
		}
		event := l1.ComputerTakeoverAuditEvent{
			EventID: eventID + ":denied", Kind: l1.ComputerTakeoverAdmissionDenied,
			ComputerID: coalescer.frontDoor.config.computerID, JobID: coalescer.frontDoor.config.jobID,
			AttemptID: coalescer.frontDoor.config.attemptID, FabricID: summary.key.fabricID,
			UserID: summary.key.userID, DeviceID: summary.key.deviceID,
			PolicyRevision: coalescer.frontDoor.config.authorizer.Revision(), OccurredAt: wallNow(coalescer.frontDoor.config.clock),
			Reason: summary.key.reason, EventCount: summary.count,
		}
		if err := coalescer.frontDoor.record(ctx, event); err != nil {
			coalescer.frontDoor.report(fmt.Errorf("record Computer admission denial summary: %w", err))
		}
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

func dialComputerBackend(ctx context.Context, dial computerEndpointDial, endpointName string) (net.Conn, *websocket.Conn, []byte, error) {
	var mu sync.Mutex
	used := false
	transport := &http.Transport{DialContext: func(dialContext context.Context, _, _ string) (net.Conn, error) {
		mu.Lock()
		defer mu.Unlock()
		if used {
			return nil, errors.New("Computer backend handshake attempted more than one dial")
		}
		used = true
		return dial(dialContext, endpointName)
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

// probeComputerBackends polls the exact wire contract on both helper tunnels.
// Connection refusal while the guest starts is ordinary; only the deadline
// produces startup_readiness_timeout.
func probeComputerBackends(ctx context.Context, clock Clock, startedAt time.Time, dial computerEndpointDial) error {
	if clock == nil {
		clock = systemClock{}
	}
	remaining := DefaultComputerReadinessDeadline - clock.Now().Sub(startedAt)
	if remaining <= 0 {
		return &computerReadinessError{Code: contract.SpawnFailureStartupReadinessTimeout,
			Err: errors.New("Computer display startup readiness exceeded 60 seconds from Started")}
	}
	deadline := clock.NewTimer(remaining)
	defer stopTimer(deadline)
	timeout := func() error {
		return &computerReadinessError{Code: contract.SpawnFailureStartupReadinessTimeout,
			Err: errors.New("Computer display startup readiness exceeded 60 seconds from Started")}
	}
	for {
		probeContext, cancelProbe := context.WithCancel(ctx)
		probeResult := make(chan error, 1)
		go func() { probeResult <- probeComputerBackendPairOnce(probeContext, dial) }()
		connectTimeout := clock.NewTimer(DefaultComputerReadinessConnectTimeout)
		var probeErr error
		select {
		case <-ctx.Done():
			cancelProbe()
			stopTimer(connectTimeout)
			return &computerReadinessError{Code: contract.SpawnFailureRuntimeUnavailable,
				Err: fmt.Errorf("Computer display readiness canceled: %w", context.Cause(ctx))}
		case <-deadline.C():
			cancelProbe()
			stopTimer(connectTimeout)
			return timeout()
		case <-connectTimeout.C():
			cancelProbe()
			probeErr = errors.New("Computer display readiness probe timed out")
		case probeErr = <-probeResult:
			cancelProbe()
			stopTimer(connectTimeout)
		}
		if probeErr == nil {
			return nil
		}
		interval := clock.NewTimer(DefaultComputerReadinessProbeInterval)
		select {
		case <-ctx.Done():
			stopTimer(interval)
			return &computerReadinessError{Code: contract.SpawnFailureRuntimeUnavailable,
				Err: fmt.Errorf("Computer display readiness canceled: %w", context.Cause(ctx))}
		case <-deadline.C():
			stopTimer(interval)
			return timeout()
		case <-interval.C():
		}
	}
}
