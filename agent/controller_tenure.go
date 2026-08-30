package agent

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/l1"
	workloadrunner "github.com/Derek-X-Wang/wefty/runner"
	"github.com/coder/websocket"
)

const (
	controllerTenureFinalizationLimit = 30 * time.Second
	controllerControlDialLimit        = 5 * time.Second
	replacementRFBStringLimit         = 1 << 20
)

type controllerTenureConfig struct {
	authorityContext   context.Context
	clock              Clock
	dial               computerEndpointDial
	setControlState    func(context.Context, bool) error
	record             func(context.Context, l1.ComputerTakeoverAuditEvent) (l1.ComputerTakeoverAuditReceipt, error)
	report             func(error)
	onUnconfirmedClear func(error)
}

// controllerTenure is process-local and attempt-scoped. Its mutex protects
// only state transitions; helper, backend, relay, and L1 calls always happen
// after the transition has been claimed and the mutex released.
type controllerTenure struct {
	config controllerTenureConfig
	mu     sync.Mutex
	next   uint64
	live   map[string]controlTenureSession
	held   *controllerHolder
	op     *controllerOperation
}

type controllerHolder struct {
	session controlTenureSession
	serial  uint64
	conn    *controllerConn
}

type controllerOperation struct {
	sessionID string
	context   context.Context
	cancel    context.CancelFunc
	done      chan struct{}
	take      bool
}

func newControllerTenure(config controllerTenureConfig) (*controllerTenure, error) {
	if config.authorityContext == nil || config.dial == nil || config.setControlState == nil || config.record == nil {
		return nil, errors.New("agent: Controller tenure requires complete attempt authority, signal, dial, and audit seams")
	}
	if config.clock == nil {
		config.clock = systemClock{}
	}
	return &controllerTenure{config: config, live: make(map[string]controlTenureSession)}, nil
}

func (*controllerTenure) controlTenure() {}

func (tenure *controllerTenure) Register(session controlTenureSession) error {
	if session.id == "" || session.context == nil || session.event.SessionID != session.id || session.event.ComputerID == "" ||
		session.event.JobID == "" || session.event.AttemptID == "" {
		return errors.New("agent: Controller tenure registration requires a live session-bound audit identity")
	}
	select {
	case <-session.context.Done():
		return &ComputerTenureError{Code: ComputerTenureSessionEnded}
	default:
	}
	tenure.mu.Lock()
	defer tenure.mu.Unlock()
	if _, exists := tenure.live[session.id]; exists {
		return errors.New("agent: Controller tenure session is already registered")
	}
	tenure.live[session.id] = session
	go tenure.releaseWhenSessionEnds(session)
	return nil
}

func (tenure *controllerTenure) releaseWhenSessionEnds(session controlTenureSession) {
	<-session.context.Done()
	ctx, cancel := context.WithTimeout(context.Background(), controllerTenureFinalizationLimit)
	defer cancel()
	if err := tenure.Release(ctx, session.id, computerSessionEndReason(session.context, l1.ComputerTakeoverAttemptAuthorityLost)); err != nil {
		tenure.report(fmt.Errorf("release ended Computer control session: %w", err))
	}
}

func (tenure *controllerTenure) Unregister(sessionID string) {
	ctx, cancel := context.WithTimeout(context.Background(), controllerTenureFinalizationLimit)
	defer cancel()
	if err := tenure.Release(ctx, sessionID, l1.ComputerTakeoverClientClosed); err != nil {
		tenure.report(fmt.Errorf("release unregistered Computer control session: %w", err))
	}
	tenure.mu.Lock()
	delete(tenure.live, sessionID)
	tenure.mu.Unlock()
}

func (tenure *controllerTenure) Take(ctx context.Context, sessionID string) (net.Conn, error) {
	connection, _, err := tenure.take(ctx, sessionID)
	return connection, err
}

func (tenure *controllerTenure) TakeReceipt(ctx context.Context, sessionID string) (contract.ComputerControlReceipt, error) {
	_, displacedSessionID, err := tenure.take(ctx, sessionID)
	receipt := tenure.receipt(sessionID)
	receipt.OverrideDisplacedSessionID = displacedSessionID
	receipt.SignalStayedTrue = err == nil && displacedSessionID != ""
	return receipt, err
}

func (tenure *controllerTenure) take(ctx context.Context, sessionID string) (net.Conn, string, error) {
	session, prior, serial, operation, operationContext, err := tenure.beginTake(ctx, sessionID)
	if err != nil {
		return nil, "", err
	}
	defer operation.cancel()

	override := prior != nil
	displacedSessionID := ""
	if prior != nil {
		displacedSessionID = prior.session.id
	}
	signalTrue := override
	if override {
		tenure.detachControl(prior)
		if err := tenure.record(prior.session, prior.serial, l1.ComputerTakeoverControlReleased, l1.ComputerTakeoverControllerOverridden); err != nil {
			return nil, displacedSessionID, tenure.failTake(operation, session, serial, override, signalTrue, false, err)
		}
	} else {
		if err := tenure.config.setControlState(operationContext, true); err != nil {
			tenure.finishOperation(operation, nil)
			return nil, "", &ComputerTenureError{Code: ComputerTenureUnavailable, Err: fmt.Errorf("set driver signal true: %w", err)}
		}
		signalTrue = true
	}

	dialContext, cancelDial := context.WithTimeout(operationContext, controllerControlDialLimit)
	connection, websocketConnection, banner, dialErr := dialComputerBackendWithLifetime(
		dialContext, session.context, tenure.config.dial, workloadrunner.AttemptEndpointControl,
	)
	if dialErr != nil {
		cancelDial()
		return nil, displacedSessionID, tenure.failTake(operation, session, serial, override, signalTrue, true,
			fmt.Errorf("dial Computer control backend: %w", dialErr))
	}
	if err := negotiateReplacementComputerControl(dialContext, connection, banner); err != nil {
		_ = websocketConnection.CloseNow()
		_ = connection.Close()
		cancelDial()
		return nil, displacedSessionID, tenure.failTake(operation, session, serial, override, signalTrue, true,
			fmt.Errorf("negotiate replacement Computer control backend: %w", err))
	}
	cancelDial()
	managed := newControllerConn(connection, websocketConnection, tenure, sessionID)

	if override {
		if err := tenure.record(session, serial, l1.ComputerTakeoverAdminOverrode, ""); err != nil {
			managed.closeAndWait()
			return nil, displacedSessionID, tenure.failTake(operation, session, serial, true, signalTrue, false, err)
		}
	}
	if err := tenure.record(session, serial, l1.ComputerTakeoverControlAcquired, ""); err != nil {
		managed.closeAndWait()
		return nil, displacedSessionID, tenure.failTake(operation, session, serial, override, signalTrue, false, err)
	}
	if session.relay != nil {
		if err := session.relay.activateControl(managed); err != nil {
			managed.closeAndWait()
			return nil, displacedSessionID, tenure.failTake(operation, session, serial, override, signalTrue, false, err)
		}
	}
	if !tenure.finishOperation(operation, &controllerHolder{session: session, serial: serial, conn: managed}) {
		tenure.detachControl(&controllerHolder{session: session, serial: serial, conn: managed})
		return nil, displacedSessionID, tenure.failTake(operation, session, serial, override, signalTrue, false,
			&ComputerTenureError{Code: ComputerTenureSessionEnded})
	}
	return managed, displacedSessionID, nil
}

// negotiateReplacementComputerControl owns the fresh control backend's RFB
// handshake before the relay leg swap. The already-negotiated client sees one
// continuous RFB session and never receives a second banner or ServerInit.
func negotiateReplacementComputerControl(ctx context.Context, connection net.Conn, banner []byte) error {
	if deadline, ok := ctx.Deadline(); ok {
		if err := connection.SetDeadline(deadline); err != nil {
			return fmt.Errorf("bound replacement RFB handshake: %w", err)
		}
		defer connection.SetDeadline(time.Time{})
	}
	clientVersion, negotiatedMinor, err := replacementComputerRFBVersion(banner)
	if err != nil {
		return err
	}
	if _, err := connection.Write(clientVersion); err != nil {
		return fmt.Errorf("write replacement RFB version: %w", err)
	}
	if negotiatedMinor == 3 {
		var securityType [4]byte
		if _, err := io.ReadFull(connection, securityType[:]); err != nil {
			return fmt.Errorf("read replacement RFB 3.3 security type: %w", err)
		}
		selected := binary.BigEndian.Uint32(securityType[:])
		if selected == 0 {
			reason, err := readBoundedRFBString(connection, replacementRFBStringLimit)
			if err != nil {
				return fmt.Errorf("read replacement RFB 3.3 security failure reason: %w", err)
			}
			return fmt.Errorf("replacement RFB 3.3 security failed: %s", reason)
		}
		if selected != 1 {
			return errors.New("replacement RFB 3.3 backend does not select None security")
		}
	} else {
		var count [1]byte
		if _, err := io.ReadFull(connection, count[:]); err != nil {
			return fmt.Errorf("read replacement RFB security count: %w", err)
		}
		if count[0] == 0 {
			reason, err := readBoundedRFBString(connection, replacementRFBStringLimit)
			if err != nil {
				return fmt.Errorf("read replacement RFB security failure reason: %w", err)
			}
			return fmt.Errorf("replacement RFB security failed: %s", reason)
		}
		securityTypes := make([]byte, int(count[0]))
		if _, err := io.ReadFull(connection, securityTypes); err != nil {
			return fmt.Errorf("read replacement RFB security types: %w", err)
		}
		if !bytes.Contains(securityTypes, []byte{1}) {
			return errors.New("replacement RFB backend does not offer None security")
		}
		if _, err := connection.Write([]byte{1}); err != nil {
			return fmt.Errorf("select replacement RFB security: %w", err)
		}
		if negotiatedMinor >= 8 {
			var securityResult [4]byte
			if _, err := io.ReadFull(connection, securityResult[:]); err != nil {
				return fmt.Errorf("read replacement RFB security result: %w", err)
			}
			if securityResult != [4]byte{} {
				return fmt.Errorf("replacement RFB security failed: %x", securityResult)
			}
		}
	}
	if _, err := connection.Write([]byte{1}); err != nil {
		return fmt.Errorf("write replacement RFB ClientInit: %w", err)
	}
	var serverInit [24]byte
	if _, err := io.ReadFull(connection, serverInit[:]); err != nil {
		return fmt.Errorf("read replacement RFB ServerInit: %w", err)
	}
	nameBytes := binary.BigEndian.Uint32(serverInit[20:24])
	if nameBytes > replacementRFBStringLimit {
		return fmt.Errorf("replacement RFB desktop name is %d bytes, limit %d", nameBytes, replacementRFBStringLimit)
	}
	if _, err := io.CopyN(io.Discard, connection, int64(nameBytes)); err != nil {
		return fmt.Errorf("read replacement RFB desktop name: %w", err)
	}
	return nil
}

func replacementComputerRFBVersion(banner []byte) ([]byte, int, error) {
	if !contract.ValidComputerRFBVersionBanner(banner) {
		return nil, 0, errors.New("replacement RFB backend sent an invalid version banner")
	}
	major := 100*int(banner[4]-'0') + 10*int(banner[5]-'0') + int(banner[6]-'0')
	minor := 100*int(banner[8]-'0') + 10*int(banner[9]-'0') + int(banner[10]-'0')
	if major < 3 || (major == 3 && minor < 3) {
		return nil, 0, fmt.Errorf("replacement RFB backend version %03d.%03d predates supported version 003.003", major, minor)
	}
	negotiatedMinor := 8
	if major == 3 && minor < 8 {
		negotiatedMinor = 7
	}
	if major == 3 && minor < 7 {
		negotiatedMinor = 3
	}
	return []byte(fmt.Sprintf("RFB 003.%03d\n", negotiatedMinor)), negotiatedMinor, nil
}

func readBoundedRFBString(connection io.Reader, limit uint32) (string, error) {
	var encodedLength [4]byte
	if _, err := io.ReadFull(connection, encodedLength[:]); err != nil {
		return "", err
	}
	length := binary.BigEndian.Uint32(encodedLength[:])
	if length > limit {
		return "", fmt.Errorf("RFB string is %d bytes, limit %d", length, limit)
	}
	value := make([]byte, int(length))
	if _, err := io.ReadFull(connection, value); err != nil {
		return "", err
	}
	return string(value), nil
}

func (tenure *controllerTenure) beginTake(
	ctx context.Context,
	sessionID string,
) (controlTenureSession, *controllerHolder, uint64, *controllerOperation, context.Context, error) {
	tenure.mu.Lock()
	defer tenure.mu.Unlock()
	session, ok := tenure.live[sessionID]
	if !ok {
		return controlTenureSession{}, nil, 0, nil, nil, &ComputerTenureError{Code: ComputerTenureSessionEnded}
	}
	if !session.canTake {
		return controlTenureSession{}, nil, 0, nil, nil, &ComputerTenureError{Code: ComputerTenureUnauthorized}
	}
	select {
	case <-session.context.Done():
		return controlTenureSession{}, nil, 0, nil, nil, &ComputerTenureError{Code: ComputerTenureSessionEnded}
	case <-tenure.config.authorityContext.Done():
		return controlTenureSession{}, nil, 0, nil, nil, &ComputerTenureError{Code: ComputerTenureSessionEnded}
	default:
	}
	if tenure.op != nil {
		return controlTenureSession{}, nil, 0, nil, nil, &ComputerTenureError{Code: ComputerTenureBusy}
	}
	if tenure.held != nil && tenure.held.session.id == sessionID {
		return controlTenureSession{}, nil, 0, nil, nil, &ComputerTenureError{Code: ComputerTenureAlreadyHeld}
	}
	prior := tenure.held
	if prior != nil && !session.administrator {
		return controlTenureSession{}, nil, 0, nil, nil, &ComputerTenureError{Code: ComputerTenureBusy}
	}

	operationContext, cancel := context.WithTimeout(session.context, controllerTenureFinalizationLimit)
	stopCaller := context.AfterFunc(ctx, cancel)
	operation := &controllerOperation{
		sessionID: sessionID,
		context:   operationContext,
		cancel: func() {
			stopCaller()
			cancel()
		},
		done: make(chan struct{}),
		take: true,
	}
	tenure.next++
	serial := tenure.next
	tenure.op = operation
	if prior != nil {
		// The override operation now owns the old input leg and truthful true
		// signal. Releases for the old session must not wait behind its dial.
		tenure.held = nil
	}
	return session, prior, serial, operation, operationContext, nil
}

func (tenure *controllerTenure) Release(ctx context.Context, sessionID string, reason l1.ComputerTakeoverReason) error {
	for {
		tenure.mu.Lock()
		if tenure.op != nil {
			operation := tenure.op
			if operation.sessionID != sessionID {
				tenure.mu.Unlock()
				return nil
			}
			if operation.take {
				operation.cancel()
			}
			done := operation.done
			tenure.mu.Unlock()
			select {
			case <-done:
				continue
			case <-ctx.Done():
				return context.Cause(ctx)
			}
		}
		if tenure.held == nil || tenure.held.session.id != sessionID {
			tenure.mu.Unlock()
			return nil
		}
		holder := tenure.held
		select {
		case <-holder.session.context.Done():
			// The session authority owns the release reason once it has ended,
			// even when its lifetime closure is first observed as backend EOF.
			reason = computerSessionEndReason(holder.session.context, reason)
		default:
		}
		operationContext, cancel := context.WithTimeout(context.WithoutCancel(ctx), controllerTenureFinalizationLimit)
		operation := &controllerOperation{sessionID: sessionID, context: operationContext, cancel: cancel, done: make(chan struct{})}
		tenure.held = nil
		tenure.op = operation
		tenure.mu.Unlock()

		tenure.detachControl(holder)
		auditErr := tenure.record(holder.session, holder.serial, l1.ComputerTakeoverControlReleased, reason)
		clearErr := tenure.clear(operationContext)
		tenure.finishOperation(operation, nil)
		cancel()
		return errors.Join(auditErr, clearErr)
	}
}

func (tenure *controllerTenure) ReleaseReceipt(
	ctx context.Context,
	sessionID string,
	reason l1.ComputerTakeoverReason,
) (contract.ComputerControlReceipt, error) {
	err := tenure.Release(ctx, sessionID, reason)
	return tenure.receipt(sessionID), err
}

func (tenure *controllerTenure) receipt(sessionID string) contract.ComputerControlReceipt {
	tenure.mu.Lock()
	defer tenure.mu.Unlock()
	receipt := contract.ComputerControlReceipt{
		AdmittedMode: string(l1.ComputerAdmittedView),
		TenureState:  contract.ComputerControlTenureFree,
	}
	if session, ok := tenure.live[sessionID]; ok {
		receipt.PolicyRevision = session.event.PolicyRevision
	}
	if tenure.held == nil {
		return receipt
	}
	receipt.HolderSessionID = tenure.held.session.id
	receipt.TenureState = contract.ComputerControlTenureHeld
	receipt.HumanDriving = true
	if tenure.held.session.id == sessionID {
		receipt.AdmittedMode = string(l1.ComputerAdmittedController)
	}
	return receipt
}

func (tenure *controllerTenure) releaseAfterBackendFailure(sessionID string) {
	ctx, cancel := context.WithTimeout(context.Background(), controllerTenureFinalizationLimit)
	defer cancel()
	if err := tenure.Release(ctx, sessionID, l1.ComputerTakeoverControlBackendClosed); err != nil {
		tenure.report(fmt.Errorf("release failed Computer control backend: %w", err))
	}
}

func (tenure *controllerTenure) failTake(
	operation *controllerOperation,
	session controlTenureSession,
	serial uint64,
	override bool,
	signalTrue bool,
	backendFailed bool,
	failure error,
) error {
	if override && backendFailed {
		if recordErr := tenure.record(session, serial, l1.ComputerTakeoverAdminOverrode, l1.ComputerTakeoverControlBackendFailed); recordErr != nil {
			failure = errors.Join(failure, recordErr)
		}
	}
	if signalTrue {
		failure = errors.Join(failure, tenure.clear(context.Background()))
	}
	tenure.finishOperation(operation, nil)
	return &ComputerTenureError{Code: ComputerTenureUnavailable, Err: failure}
}

func (tenure *controllerTenure) finishOperation(operation *controllerOperation, holder *controllerHolder) bool {
	tenure.mu.Lock()
	defer tenure.mu.Unlock()
	if tenure.op != operation {
		return false
	}
	if holder != nil {
		select {
		case <-operation.context.Done():
			return false
		case <-holder.session.context.Done():
			return false
		case <-tenure.config.authorityContext.Done():
			return false
		default:
		}
		tenure.held = holder
	}
	tenure.op = nil
	close(operation.done)
	return true
}

func (tenure *controllerTenure) detachControl(holder *controllerHolder) {
	if holder.session.relay != nil {
		holder.session.relay.deactivateControl(holder.conn)
		return
	}
	holder.conn.closeAndWait()
}

func (tenure *controllerTenure) clear(parent context.Context) error {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(parent), controllerTenureFinalizationLimit)
	defer cancel()
	if err := tenure.config.setControlState(ctx, false); err != nil {
		failure := fmt.Errorf("clear driver signal: %w", err)
		if tenure.config.onUnconfirmedClear != nil {
			tenure.config.onUnconfirmedClear(failure)
		}
		return failure
	}
	return nil
}

func (tenure *controllerTenure) record(
	session controlTenureSession,
	serial uint64,
	kind l1.ComputerTakeoverAuditEventKind,
	reason l1.ComputerTakeoverReason,
) error {
	event := session.event
	event.EventID = fmt.Sprintf("%s:control:%d:%s", session.id, serial, kind)
	event.Kind = kind
	event.AdmittedMode = l1.ComputerAdmittedController
	event.OccurredAt = wallNow(tenure.config.clock).UTC().Round(0)
	event.Reason = reason
	ctx, cancel := context.WithTimeout(context.WithoutCancel(session.context), controllerTenureFinalizationLimit)
	defer cancel()
	if err := appendAssertedComputerAudit(ctx, tenure.config.record, event); err != nil {
		return fmt.Errorf("record %s: %w", kind, err)
	}
	return nil
}

func (tenure *controllerTenure) report(err error) {
	if err != nil && tenure.config.report != nil {
		tenure.config.report(err)
	}
}

// controllerConn observes every in-flight backend operation before an
// override or release may install a replacement input path.
type controllerConn struct {
	net.Conn
	websocket immediateWebSocketCloser
	owner     *controllerTenure
	sessionID string

	mu          sync.Mutex
	cond        *sync.Cond
	active      int
	closing     bool
	failureOnce sync.Once
}

func newControllerConn(connection net.Conn, websocketConnection *websocket.Conn, owner *controllerTenure, sessionID string) *controllerConn {
	managed := &controllerConn{Conn: connection, owner: owner, sessionID: sessionID}
	if websocketConnection != nil {
		managed.websocket = websocketConnection
	}
	managed.cond = sync.NewCond(&managed.mu)
	return managed
}

func (connection *controllerConn) begin() bool {
	connection.mu.Lock()
	defer connection.mu.Unlock()
	if connection.closing {
		return false
	}
	connection.active++
	return true
}

func (connection *controllerConn) end(failed bool) {
	connection.mu.Lock()
	connection.active--
	if connection.active == 0 {
		connection.cond.Broadcast()
	}
	closing := connection.closing
	connection.mu.Unlock()
	if failed && !closing {
		connection.failureOnce.Do(func() { go connection.owner.releaseAfterBackendFailure(connection.sessionID) })
	}
}

func (connection *controllerConn) Read(buffer []byte) (int, error) {
	if !connection.begin() {
		return 0, net.ErrClosed
	}
	count, err := connection.Conn.Read(buffer)
	connection.end(err != nil)
	return count, err
}

func (connection *controllerConn) Write(buffer []byte) (int, error) {
	if !connection.begin() {
		return 0, net.ErrClosed
	}
	count, err := connection.Conn.Write(buffer)
	connection.end(err != nil)
	return count, err
}

func (connection *controllerConn) Close() error {
	ctx, cancel := context.WithTimeout(context.Background(), controllerTenureFinalizationLimit)
	defer cancel()
	return connection.owner.Release(ctx, connection.sessionID, l1.ComputerTakeoverExplicitRelease)
}

func (connection *controllerConn) closeAndWait() {
	connection.mu.Lock()
	if !connection.closing {
		connection.closing = true
		// websocket.NetConn.Close waits up to five seconds for a peer close
		// frame. Authority loss must close the input socket without depending on
		// peer cooperation, then wait only for already-active operations.
		if connection.websocket != nil {
			_ = connection.websocket.CloseNow()
		}
		_ = connection.Conn.Close()
	}
	for connection.active != 0 {
		connection.cond.Wait()
	}
	connection.mu.Unlock()
}

var _ ControlTenure = (*controllerTenure)(nil)
var _ net.Conn = (*controllerConn)(nil)
