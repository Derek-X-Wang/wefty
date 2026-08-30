package agent

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
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
	computerWebSocketPath                  = contract.ComputerDisplayWebSocketPath
	computerWebSocketSubprotocol           = contract.ComputerDisplayWebSocketSubprotocol
	computerControlTakePath                = contract.ComputerControlTakePath
	computerControlReleasePath             = contract.ComputerControlReleasePath
	computerControlTokenHeader             = contract.ComputerControlTokenHeader
	computerRFBVersionBannerBytes          = contract.ComputerRFBVersionBannerBytes
	DefaultComputerSessionCap              = time.Hour
	DefaultComputerIdentityRevalidation    = time.Minute
	DefaultComputerReadinessDeadline       = contract.ComputerStartupReadinessTimeout
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

type ComputerTenureErrorCode = contract.ErrorCode

const ComputerTenureUnavailable = contract.ErrorTenureUnavailable

const (
	ComputerTenureUnauthorized = contract.ErrorControlNotAuthorized
	ComputerTenureBusy         = contract.ErrorControllerBusy
	ComputerTenureAlreadyHeld  = contract.ErrorControllerAlreadyHeld
	ComputerTenureSessionEnded = contract.ErrorTakeoverSessionEnded
)

type ComputerTenureError struct {
	Code ComputerTenureErrorCode
	Err  error
}

func (failure *ComputerTenureError) Error() string {
	if failure == nil {
		return "agent: Computer control tenure failed"
	}
	if failure.Err != nil {
		return fmt.Sprintf("agent: Computer control tenure %s: %v", failure.Code, failure.Err)
	}
	return "agent: Computer control tenure " + string(failure.Code)
}

func (failure *ComputerTenureError) Unwrap() error { return failure.Err }

type controlTenureSession struct {
	id            string
	context       context.Context
	canTake       bool
	administrator bool
	event         l1.ComputerTakeoverAuditEvent
	relay         *computerSessionRelay
}

// ControlTenure is sealed to package agent so only the #179 implementation
// can gain a control dial. Admission receives no control endpoint capability.
type ControlTenure interface {
	Register(controlTenureSession) error
	Take(context.Context, string) (net.Conn, error)
	TakeReceipt(context.Context, string) (contract.ComputerControlReceipt, error)
	Release(context.Context, string, l1.ComputerTakeoverReason) error
	ReleaseReceipt(context.Context, string, l1.ComputerTakeoverReason) (contract.ComputerControlReceipt, error)
	Unregister(string)
	controlTenure()
}

type unavailableControlTenure struct{}

func (unavailableControlTenure) Register(controlTenureSession) error { return nil }
func (unavailableControlTenure) Take(context.Context, string) (net.Conn, error) {
	return nil, &ComputerTenureError{Code: ComputerTenureUnavailable}
}
func (unavailableControlTenure) TakeReceipt(context.Context, string) (contract.ComputerControlReceipt, error) {
	return contract.ComputerControlReceipt{AdmittedMode: string(l1.ComputerAdmittedView),
		TenureState: contract.ComputerControlTenureFree}, &ComputerTenureError{Code: ComputerTenureUnavailable}
}
func (unavailableControlTenure) Release(context.Context, string, l1.ComputerTakeoverReason) error {
	return nil
}
func (unavailableControlTenure) ReleaseReceipt(context.Context, string, l1.ComputerTakeoverReason) (contract.ComputerControlReceipt, error) {
	return contract.ComputerControlReceipt{AdmittedMode: string(l1.ComputerAdmittedView),
		TenureState: contract.ComputerControlTenureFree}, nil
}
func (unavailableControlTenure) Unregister(string) {}
func (unavailableControlTenure) controlTenure()    {}

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
	storageID            string
	storageGeneration    int64
	fencingToken         string
	dial                 computerEndpointDial
	controlTenure        ControlTenure
	sessionCap           time.Duration
	revalidationInterval time.Duration
	denialFlushInterval  time.Duration
	newSessionID         func() (string, error)
	newControlToken      func() (string, error)
	controlTokens        *computerControlTokenCodec
	// Tests use this hook to prove the client banner cannot overtake registration.
	beforeControlSessionRegistration func()
}

// computerSessionHandle is the private sideband capability for one live
// admission. A MAC-authenticated bearer resolves to this handle only while the
// exact session is live; later doors recognize its Node lineage only to return
// terminal evidence. Neither identity nor policy role can name another
// session's take or release operation.
type computerSessionHandle struct {
	id               string
	canTake          bool
	admin            bool
	policyRevision   int64
	tenure           ControlTenure
	session          context.Context
	terminalMu       sync.Mutex
	terminalReason   l1.ComputerTakeoverReason
	terminalRevision int64
}

type activeComputerSession struct {
	handle   *computerSessionHandle
	identity fabric.Identity
	cancel   context.CancelCauseFunc
}

func (session *activeComputerSession) end(reason l1.ComputerTakeoverReason, policyRevision int64) {
	session.handle.markTerminal(reason, policyRevision)
	session.cancel(&computerSessionEnd{reason: reason})
}

func (handle *computerSessionHandle) CanTake() bool { return handle != nil && handle.canTake }

func (handle *computerSessionHandle) Take(ctx context.Context) (contract.ComputerControlReceipt, error) {
	if handle == nil {
		return contract.ComputerControlReceipt{}, &ComputerTenureError{Code: ComputerTenureUnauthorized}
	}
	if !handle.CanTake() {
		return contract.ComputerControlReceipt{}, &ComputerTenureError{Code: ComputerTenureUnauthorized}
	}
	select {
	case <-handle.session.Done():
		return handle.terminalReceipt(computerSessionEndReason(handle.session, l1.ComputerTakeoverClientClosed)),
			&ComputerTenureError{Code: ComputerTenureSessionEnded}
	default:
	}
	return handle.tenure.TakeReceipt(ctx, handle.id)
}

func (handle *computerSessionHandle) Release(ctx context.Context) (contract.ComputerControlReceipt, error) {
	if handle == nil || handle.tenure == nil {
		return contract.ComputerControlReceipt{AdmittedMode: string(l1.ComputerAdmittedView),
			TenureState: contract.ComputerControlTenureFree}, nil
	}
	select {
	case <-handle.session.Done():
		return handle.terminalReceipt(computerSessionEndReason(handle.session, l1.ComputerTakeoverClientClosed)),
			&ComputerTenureError{Code: ComputerTenureSessionEnded}
	default:
	}
	return handle.tenure.ReleaseReceipt(ctx, handle.id, l1.ComputerTakeoverExplicitRelease)
}

func (handle *computerSessionHandle) terminalReceipt(reason l1.ComputerTakeoverReason) contract.ComputerControlReceipt {
	handle.terminalMu.Lock()
	if handle.terminalReason != "" {
		reason = handle.terminalReason
	}
	policyRevision := handle.terminalRevision
	handle.terminalMu.Unlock()
	if policyRevision == 0 {
		policyRevision = handle.policyRevision
	}
	return contract.ComputerControlReceipt{
		AdmittedMode:     string(l1.ComputerAdmittedView),
		TenureState:      contract.ComputerControlTenureFree,
		PolicyRevision:   policyRevision,
		SessionEndReason: string(reason),
	}
}

func (handle *computerSessionHandle) markTerminal(reason l1.ComputerTakeoverReason, policyRevision int64) {
	if handle == nil {
		return
	}
	handle.terminalMu.Lock()
	handle.terminalReason = reason
	if policyRevision > 0 {
		handle.terminalRevision = policyRevision
	}
	handle.terminalMu.Unlock()
}

type closedComputerSession struct {
	identity fabric.Identity
	receipt  contract.ComputerControlReceipt
	canTake  bool
}

const maximumClosedComputerSessionTokens = 256

type computerFrontDoor struct {
	config        computerFrontDoorConfig
	errors        chan error
	errorMu       sync.Mutex
	pendingErrors []error
	denials       *computerDenialCoalescer
	mu            sync.Mutex
	ready         bool
	active        map[string]*activeComputerSession
	tokens        map[string]*activeComputerSession
	closedTokens  map[string]closedComputerSession
	closedOrder   []string
	sessionCond   *sync.Cond
	sessionCount  int
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
	if config.computerID == "" || config.jobID == "" || config.attemptID == "" || config.storageID == "" ||
		config.storageGeneration <= 0 || config.fencingToken == "" {
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
	if config.newControlToken == nil {
		config.newControlToken = newComputerControlToken
	}
	if config.controlTokens == nil {
		key := make([]byte, computerControlTokenKeySize)
		if _, err := rand.Read(key); err != nil {
			return nil, fmt.Errorf("agent: generate Computer control token key: %w", err)
		}
		var err error
		config.controlTokens, err = newComputerControlTokenCodec(key)
		if err != nil {
			return nil, err
		}
	}
	frontDoor := &computerFrontDoor{
		config: config, errors: make(chan error, 16), active: make(map[string]*activeComputerSession),
		tokens: make(map[string]*activeComputerSession), closedTokens: make(map[string]closedComputerSession),
	}
	frontDoor.sessionCond = sync.NewCond(&frontDoor.mu)
	frontDoor.denials = newComputerDenialCoalescer(frontDoor)
	go frontDoor.denials.run(config.authorityContext)
	return frontDoor, nil
}

func (frontDoor *computerFrontDoor) Errors() <-chan error { return frontDoor.errors }

func (frontDoor *computerFrontDoor) takeErrors() error {
	frontDoor.errorMu.Lock()
	defer frontDoor.errorMu.Unlock()
	joined := errors.Join(frontDoor.pendingErrors...)
	frontDoor.pendingErrors = nil
	return joined
}

func (frontDoor *computerFrontDoor) SetReady(ready bool) {
	frontDoor.mu.Lock()
	frontDoor.ready = ready
	if !ready {
		for _, session := range frontDoor.active {
			session.end(l1.ComputerTakeoverViewBackendClosed, 0)
		}
	}
	frontDoor.mu.Unlock()
}

func (frontDoor *computerFrontDoor) EndSessions(reason l1.ComputerTakeoverReason) {
	frontDoor.mu.Lock()
	defer frontDoor.mu.Unlock()
	for _, session := range frontDoor.active {
		session.end(reason, 0)
	}
}

func (frontDoor *computerFrontDoor) isReady() bool {
	frontDoor.mu.Lock()
	defer frontDoor.mu.Unlock()
	return frontDoor.ready
}

func (frontDoor *computerFrontDoor) WaitForSessions() {
	frontDoor.mu.Lock()
	defer frontDoor.mu.Unlock()
	for frontDoor.sessionCount != 0 {
		frontDoor.sessionCond.Wait()
	}
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
	if request.URL.Path == computerControlTakePath || request.URL.Path == computerControlReleasePath {
		frontDoor.serveControlAction(writer, request, identity)
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
		requestedRevision, revisionErr := strconv.ParseInt(request.Header.Get(contract.ComputerPolicyRevisionHeader), 10, 64)
		installedRevision := frontDoor.config.authorizer.Revision()
		if revisionErr == nil && requestedRevision > installedRevision {
			writeComputerControlError(writer, http.StatusForbidden, contract.APIError{
				Code: contract.ErrorStalePolicyRevision, Message: "Computer policy revision is not installed at the hosting agent", Retryable: true,
				Details: map[string]any{"requested_policy_revision": requestedRevision, "installed_policy_revision": installedRevision},
			}, nil)
			return
		}
		writeComputerControlError(writer, http.StatusForbidden, contract.APIError{Code: contract.ErrorControlNotAuthorized,
			Message: "Computer access is not authorized", Retryable: false}, nil)
		return
	}
	frontDoor.serveAuthorized(writer, request, identity, authorization, admittedAt)
}

func (frontDoor *computerFrontDoor) serveControlAction(writer http.ResponseWriter, request *http.Request, identity fabric.Identity) {
	if request.Method != http.MethodPost || request.URL.RawQuery != "" {
		http.Error(writer, "Computer control action is a fixed POST", http.StatusNotFound)
		return
	}
	values := request.Header.Values(computerControlTokenHeader)
	if len(values) != 1 || values[0] == "" || values[0] != strings.TrimSpace(values[0]) {
		writeComputerControlError(writer, http.StatusUnauthorized, contract.APIError{Code: contract.ErrorUnauthorized,
			Message: "Computer control session token is required"}, nil)
		return
	}
	token := values[0]
	frontDoor.mu.Lock()
	session := frontDoor.tokens[token]
	closed, wasClosed := frontDoor.closedTokens[token]
	frontDoor.mu.Unlock()
	if session == nil && wasClosed && sameComputerSessionIdentity(closed.identity, identity) {
		action := "release"
		if request.URL.Path == computerControlTakePath {
			action = "take"
			if !closed.canTake {
				writeComputerControlError(writer, http.StatusForbidden, contract.APIError{Code: contract.ErrorControlNotAuthorized,
					Message: "Computer access is not authorized for control"}, nil)
				return
			}
		}
		receipt := closed.receipt
		receipt.ComputerID = frontDoor.config.computerID
		receipt.Action = action
		writeComputerControlError(writer, http.StatusGone, contract.APIError{Code: contract.ErrorTakeoverSessionEnded,
			Message: "Computer " + action + " was refused because the session ended"}, &receipt)
		return
	}
	if session == nil {
		claims, issued := frontDoor.config.controlTokens.authenticate(token, frontDoor.config.computerID, frontDoor.config.storageID, identity)
		if issued {
			action := "release"
			if request.URL.Path == computerControlTakePath {
				action = "take"
				if !claims.CanTake {
					writeComputerControlError(writer, http.StatusForbidden, contract.APIError{Code: contract.ErrorControlNotAuthorized,
						Message: "Computer access is not authorized for control"}, nil)
					return
				}
			}
			receipt := contract.ComputerControlReceipt{ComputerID: frontDoor.config.computerID, Action: action,
				AdmittedMode: string(l1.ComputerAdmittedView), TenureState: contract.ComputerControlTenureFree,
				PolicyRevision: claims.PolicyRevision, SessionEndReason: string(l1.ComputerTakeoverAttemptAuthorityLost)}
			writeComputerControlError(writer, http.StatusGone, contract.APIError{Code: contract.ErrorTakeoverSessionEnded,
				Message: "Computer " + action + " was refused because the session ended"}, &receipt)
			return
		}
	}
	if session == nil || !sameComputerSessionIdentity(session.identity, identity) {
		writeComputerControlError(writer, http.StatusUnauthorized, contract.APIError{Code: contract.ErrorUnauthorized,
			Message: "Computer control session is not active for this identity"}, nil)
		return
	}

	action := "release"
	var receipt contract.ComputerControlReceipt
	var err error
	if request.URL.Path == computerControlTakePath {
		action = "take"
		receipt, err = session.handle.Take(request.Context())
	} else {
		receipt, err = session.handle.Release(request.Context())
	}
	receipt.ComputerID = frontDoor.config.computerID
	receipt.Action = action
	if err == nil {
		writeComputerControlJSON(writer, http.StatusOK, receipt)
		return
	}
	var tenureErr *ComputerTenureError
	if errors.As(err, &tenureErr) {
		status := http.StatusServiceUnavailable
		switch tenureErr.Code {
		case ComputerTenureUnauthorized:
			status = http.StatusForbidden
		case ComputerTenureBusy, ComputerTenureAlreadyHeld:
			status = http.StatusConflict
		case ComputerTenureSessionEnded:
			status = http.StatusGone
		}
		writeComputerControlError(writer, status, contract.APIError{Code: tenureErr.Code,
			Message:   "Computer " + action + " was refused by Controller tenure",
			Retryable: tenureErr.Code == contract.ErrorControllerBusy}, &receipt)
		if tenureErr.Err != nil {
			frontDoor.report(fmt.Errorf("perform Computer control action: %w", err))
		}
		return
	}
	frontDoor.report(fmt.Errorf("perform Computer control action: %w", err))
	writeComputerControlError(writer, http.StatusServiceUnavailable, contract.APIError{Code: contract.ErrorTenureUnavailable,
		Message: "Computer Controller tenure is unavailable"}, &receipt)
}

func sameComputerSessionIdentity(left, right fabric.Identity) bool {
	return left.Kind == right.Kind && left.FabricID == right.FabricID && left.UserID == right.UserID && left.DeviceID == right.DeviceID
}

func writeComputerControlError(writer http.ResponseWriter, status int, apiError contract.APIError, receipt *contract.ComputerControlReceipt) {
	writeComputerControlJSON(writer, status, contract.ComputerControlErrorResponse{Error: apiError, Receipt: receipt})
}

func writeComputerControlJSON(writer http.ResponseWriter, status int, value any) {
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(status)
	if err := json.NewEncoder(writer).Encode(value); err != nil {
		return
	}
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
	controlNonce, err := frontDoor.config.newControlToken()
	if err != nil {
		frontDoor.report(fmt.Errorf("generate Computer control token: %w", err))
		http.Error(writer, "Computer session unavailable", http.StatusServiceUnavailable)
		return
	}
	controlToken, err := frontDoor.config.controlTokens.issue(computerControlTokenClaims{
		ComputerID: frontDoor.config.computerID, StorageID: frontDoor.config.storageID,
		StorageGeneration: frontDoor.config.storageGeneration, AttemptID: frontDoor.config.attemptID,
		FabricKind: string(identity.Kind), FabricID: identity.FabricID, UserID: identity.UserID, DeviceID: identity.DeviceID,
		CanTake: authorization.CanTake(), PolicyRevision: authorization.PolicyRevision(), Nonce: controlNonce,
	})
	if err != nil {
		frontDoor.report(fmt.Errorf("issue Computer control token: %w", err))
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

	sessionContext, cancelSession, stopAuthorityPropagation := newComputerSessionContext(frontDoor.config.authorityContext)
	defer stopAuthorityPropagation()
	defer cancelSession(nil)
	backend, backendWebSocket, banner, err := dialComputerBackend(sessionContext, frontDoor.config.dial, workloadrunner.AttemptEndpointView)
	if err != nil {
		frontDoor.recordDenial(request.Context(), identity, l1.ComputerTakeoverViewBackendUnavailable)
		frontDoor.report(fmt.Errorf("dial Computer view backend: %w", err))
		http.Error(writer, "Computer display unavailable", http.StatusServiceUnavailable)
		return
	}
	writer.Header().Set(computerControlTokenHeader, controlToken)
	clientWebSocket, err := websocket.Accept(writer, request, &websocket.AcceptOptions{
		Subprotocols: []string{computerWebSocketSubprotocol},
	})
	if err != nil {
		_ = backend.Close()
		frontDoor.recordDenial(request.Context(), identity, l1.ComputerTakeoverClientUpgradeFailed)
		frontDoor.report(fmt.Errorf("upgrade Computer display client: %w", err))
		return
	}
	client := websocket.NetConn(sessionContext, clientWebSocket, websocket.MessageBinary)
	openedAt := wallNow(frontDoor.config.clock).UTC().Round(0)
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
	relay := newComputerSessionRelay(client, backend, clientWebSocket, backendWebSocket)
	handle := &computerSessionHandle{id: sessionID, canTake: authorization.CanTake(), admin: authorization.IsAdministrator(),
		policyRevision: authorization.PolicyRevision(), tenure: frontDoor.config.controlTenure, session: sessionContext}
	if frontDoor.config.beforeControlSessionRegistration != nil {
		frontDoor.config.beforeControlSessionRegistration()
	}
	if err := handle.tenure.Register(controlTenureSession{id: sessionID, context: sessionContext, canTake: handle.canTake,
		administrator: handle.admin, event: baseEvent, relay: relay}); err != nil {
		frontDoor.report(fmt.Errorf("register Computer control session: %w", err))
		_ = clientWebSocket.CloseNow()
		_ = backendWebSocket.CloseNow()
		release()
		frontDoor.finishSession(request.Context(), baseEvent, computerSessionEndReason(sessionContext, l1.ComputerTakeoverClientClosed))
		return
	}
	defer handle.tenure.Unregister(sessionID)
	active := &activeComputerSession{handle: handle, identity: identity, cancel: cancelSession}
	frontDoor.mu.Lock()
	if !frontDoor.ready {
		frontDoor.mu.Unlock()
		reason := l1.ComputerTakeoverAttemptAuthorityLost
		cancelSession(&computerSessionEnd{reason: reason})
		relay.Close()
		release()
		frontDoor.finishSession(request.Context(), baseEvent, reason)
		return
	}
	frontDoor.active[sessionID] = active
	frontDoor.tokens[controlToken] = active
	frontDoor.sessionCount++
	frontDoor.mu.Unlock()
	endReason := l1.ComputerTakeoverClientClosed
	defer func() {
		frontDoor.mu.Lock()
		delete(frontDoor.active, sessionID)
		delete(frontDoor.tokens, controlToken)
		frontDoor.rememberClosedTokenLocked(controlToken, closedComputerSession{identity: identity,
			receipt: handle.terminalReceipt(endReason), canTake: handle.canTake})
		frontDoor.sessionCount--
		frontDoor.sessionCond.Broadcast()
		frontDoor.mu.Unlock()
	}()
	if _, err := client.Write(banner); err != nil {
		_ = backend.Close()
		release()
		frontDoor.finishSession(request.Context(), baseEvent, l1.ComputerTakeoverClientClosed)
		return
	}
	relay.Start()

	end := frontDoor.waitForSessionEnd(sessionContext, request.RemoteAddr, identity, authorization, relay, admittedAt)
	endReason = end.reason
	handle.markTerminal(end.reason, end.policyRevision)
	cancelSession(&computerSessionEnd{reason: end.reason})
	relay.Close()
	// A policy drain acknowledges the observable socket boundary. Durable
	// control/session audit is finalized afterward and must not delay revocation.
	release()
	if err := handle.tenure.Release(context.WithoutCancel(request.Context()), sessionID, end.reason); err != nil {
		frontDoor.report(fmt.Errorf("release Computer control tenure: %w", err))
	}
	frontDoor.finishSession(request.Context(), baseEvent, end.reason)
}

func (frontDoor *computerFrontDoor) rememberClosedTokenLocked(token string, session closedComputerSession) {
	frontDoor.closedTokens[token] = session
	frontDoor.closedOrder = append(frontDoor.closedOrder, token)
	if len(frontDoor.closedOrder) <= maximumClosedComputerSessionTokens {
		return
	}
	oldest := frontDoor.closedOrder[0]
	frontDoor.closedOrder = frontDoor.closedOrder[1:]
	delete(frontDoor.closedTokens, oldest)
}

// newComputerSessionContext translates generic parent cancellation into the
// one attempt-authority reason before any relay lifetime observes it. Directly
// parenting WithCancelCause would retain context.DeadlineExceeded and let a
// concurrent backend EOF invent a different finalization reason.
func newComputerSessionContext(authority context.Context) (context.Context, context.CancelCauseFunc, func() bool) {
	ctx, cancel := context.WithCancelCause(context.WithoutCancel(authority))
	stop := context.AfterFunc(authority, func() {
		cancel(&computerSessionEnd{reason: l1.ComputerTakeoverAttemptAuthorityLost})
	})
	return ctx, cancel, stop
}

type computerSessionEndEvidence struct {
	reason         l1.ComputerTakeoverReason
	policyRevision int64
}

func (frontDoor *computerFrontDoor) waitForSessionEnd(
	ctx context.Context,
	remoteAddress string,
	identity fabric.Identity,
	authorization *ComputerGrantAuthorization,
	relay *computerSessionRelay,
	admittedAt time.Time,
) computerSessionEndEvidence {
	revalidation := make(chan l1.ComputerTakeoverReason, 1)
	go frontDoor.revalidateIdentity(ctx, remoteAddress, identity, revalidation)
	remaining := frontDoor.config.sessionCap - frontDoor.config.clock.Now().Sub(admittedAt)
	if remaining < 0 {
		remaining = 0
	}
	capTimer := frontDoor.config.clock.NewTimer(remaining)
	defer stopTimer(capTimer)
	reason := l1.ComputerTakeoverClientClosed
	policyRevision := authorization.PolicyRevision()
	select {
	case <-ctx.Done():
		reason = computerSessionEndReason(ctx, l1.ComputerTakeoverAttemptAuthorityLost)
	case revocation := <-authorization.Revocations():
		reason = l1.ComputerTakeoverRevoked
		if revocation.PolicyRevision > 0 {
			policyRevision = revocation.PolicyRevision
		}
	case reason = <-revalidation:
	case reason = <-relay.Reasons():
	case <-capTimer.C():
		reason = l1.ComputerTakeoverSessionCapExpired
	}
	// Session cancellation carries the authoritative attempt/session reason.
	// Prefer it when a lower-layer relay closure became observable at the same
	// time so scheduling cannot rewrite authority loss as a backend failure.
	select {
	case <-ctx.Done():
		reason = computerSessionEndReason(ctx, l1.ComputerTakeoverAttemptAuthorityLost)
	default:
	}
	return computerSessionEndEvidence{reason: reason, policyRevision: policyRevision}
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

func (frontDoor *computerFrontDoor) sessionEvent(identity fabric.Identity, authorization *ComputerGrantAuthorization, sessionID string, occurred time.Time) l1.ComputerTakeoverAuditEvent {
	return l1.ComputerTakeoverAuditEvent{
		ComputerID: frontDoor.config.computerID, JobID: frontDoor.config.jobID,
		AttemptID: frontDoor.config.attemptID, SessionID: sessionID,
		FabricID: identity.FabricID, UserID: identity.UserID, DeviceID: identity.DeviceID,
		AuthorizedRole: authorization.AuthorizedRole(), AdmittedMode: l1.ComputerAdmittedMode(authorization.AdmissionRole()),
		PolicyRevision: authorization.PolicyRevision(), OccurredAt: occurred.UTC().Round(0),
	}
}

func (frontDoor *computerFrontDoor) finishSession(ctx context.Context, base l1.ComputerTakeoverAuditEvent, reason l1.ComputerTakeoverReason) {
	closeEvent := base
	closeEvent.EventID = base.SessionID + ":close"
	closeEvent.Kind = l1.ComputerTakeoverSessionClose
	closeEvent.OccurredAt = wallNow(frontDoor.config.clock).UTC().Round(0)
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
		PolicyRevision: frontDoor.config.authorizer.Revision(), OccurredAt: wallNow(frontDoor.config.clock).UTC().Round(0), Reason: reason, EventCount: 1,
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
			PolicyRevision: coalescer.frontDoor.config.authorizer.Revision(), OccurredAt: wallNow(coalescer.frontDoor.config.clock).UTC().Round(0),
			Reason: summary.key.reason, EventCount: summary.count,
		}
		if err := coalescer.frontDoor.record(ctx, event); err != nil {
			coalescer.frontDoor.report(fmt.Errorf("record Computer admission denial summary: %w", err))
		}
	}
}

func (frontDoor *computerFrontDoor) record(ctx context.Context, event l1.ComputerTakeoverAuditEvent) error {
	event = canonicalComputerAuditEvent(event)
	auditContext, cancel := context.WithTimeout(context.WithoutCancel(ctx), defaultComputerAuditFinalizationLimit)
	defer cancel()
	return appendAssertedComputerAudit(auditContext, func(appendContext context.Context, requested l1.ComputerTakeoverAuditEvent) (l1.ComputerTakeoverAuditReceipt, error) {
		return frontDoor.config.auditor.AppendComputerTakeoverAudit(
			appendContext, frontDoor.config.computerID, frontDoor.config.jobID, frontDoor.config.attemptID,
			l1.ComputerTakeoverAuditRequest{FencingToken: frontDoor.config.fencingToken, Event: requested},
		)
	}, event)
}

func (frontDoor *computerFrontDoor) report(err error) {
	if err == nil {
		return
	}
	frontDoor.errorMu.Lock()
	frontDoor.pendingErrors = append(frontDoor.pendingErrors, err)
	notify := len(frontDoor.pendingErrors) == 1
	frontDoor.errorMu.Unlock()
	if notify {
		select {
		case frontDoor.errors <- err:
		default:
		}
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

func newComputerControlToken() (string, error) {
	var value [32]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "", err
	}
	return "control_" + hex.EncodeToString(value[:]), nil
}

func dialComputerBackend(ctx context.Context, dial computerEndpointDial, endpointName string) (net.Conn, *websocket.Conn, []byte, error) {
	return dialComputerBackendWithLifetime(ctx, ctx, dial, endpointName)
}

func dialComputerBackendWithLifetime(dialContext, lifetimeContext context.Context, dial computerEndpointDial, endpointName string) (net.Conn, *websocket.Conn, []byte, error) {
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
	connection, _, err := websocket.Dial(dialContext, "ws://computer-backend.invalid"+computerWebSocketPath, &websocket.DialOptions{
		HTTPClient: &http.Client{Transport: transport}, Subprotocols: []string{computerWebSocketSubprotocol},
	})
	if err != nil {
		return nil, nil, nil, err
	}
	if connection.Subprotocol() != computerWebSocketSubprotocol {
		_ = connection.CloseNow()
		return nil, nil, nil, errors.New("Computer backend did not negotiate the binary subprotocol")
	}
	network := websocket.NetConn(lifetimeContext, connection, websocket.MessageBinary)
	banner := make([]byte, computerRFBVersionBannerBytes)
	bannerRead := make(chan error, 1)
	go func() {
		_, readErr := io.ReadFull(network, banner)
		bannerRead <- readErr
	}()
	select {
	case err = <-bannerRead:
	case <-dialContext.Done():
		_ = connection.CloseNow()
		<-bannerRead
		err = context.Cause(dialContext)
	}
	if err != nil {
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
	return contract.ValidComputerRFBVersionBanner(banner)
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
			if !clock.Now().Before(startedAt.Add(DefaultComputerReadinessDeadline)) {
				return timeout()
			}
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
