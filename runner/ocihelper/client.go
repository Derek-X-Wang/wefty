package ocihelper

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"
)

type DialFunc func(context.Context) (net.Conn, error)

type Client struct {
	Dial                 DialFunc
	Version              int
	ExpectedChecksum     string
	HeartbeatInterval    time.Duration
	disableHeartbeatPump bool
}

func NewUnixClient(socketPath, expectedChecksum string) *Client {
	dialer := &net.Dialer{}
	return &Client{
		Version: ProtocolVersion, ExpectedChecksum: expectedChecksum,
		Dial: func(ctx context.Context) (net.Conn, error) {
			return dialer.DialContext(ctx, "unix", socketPath)
		},
	}
}

type Session struct {
	client      *Client
	capability  string
	control     net.Conn
	controlWire *framedConn
	response    AcquireSessionResponse

	controlMu   sync.Mutex
	queueMu     sync.Mutex
	pending     map[string]pendingRenewal
	queueToken  uint64
	sequence    uint64
	queued      chan struct{}
	pumpCtx     context.Context
	pumpCancel  context.CancelFunc
	pumpDone    chan struct{}
	pumpErr     error
	lossHandler func(error)
	closed      bool
	closeOnce   sync.Once
	lossOnce    sync.Once
}

func (client *Client) protocolVersion() int {
	if client.Version == 0 {
		return ProtocolVersion
	}
	return client.Version
}

func (client *Client) OpenSession(ctx context.Context, request AcquireSessionRequest) (*Session, error) {
	if client == nil || client.ExpectedChecksum == "" {
		return nil, errors.New("OCI helper checksum verification is required")
	}
	if client == nil || client.Dial == nil {
		return nil, errors.New("OCI helper client dialer is required")
	}
	if request.ExpectedHelperChecksum == "" {
		request.ExpectedHelperChecksum = client.ExpectedChecksum
	}
	connection, err := client.Dial(ctx)
	if err != nil {
		return nil, fmt.Errorf("dial OCI helper: %w", err)
	}
	stopCancellation := context.AfterFunc(ctx, func() { _ = connection.Close() })
	defer stopCancellation()
	if err := applyContextDeadline(ctx, connection); err != nil {
		_ = connection.Close()
		return nil, err
	}
	wire := newFramedConn(connection)
	body, err := marshalBody(request)
	if err != nil {
		_ = connection.Close()
		return nil, err
	}
	if err := wire.write(frame{Version: client.protocolVersion(), Method: MethodAcquireSession, Body: body}); err != nil {
		_ = connection.Close()
		return nil, fmt.Errorf("send OCI helper handshake: %w", err)
	}
	var response AcquireSessionResponse
	if err := decodeResponse(wire, &response); err != nil {
		_ = connection.Close()
		return nil, err
	}
	_ = connection.SetDeadline(time.Time{})
	if response.ProtocolVersion != client.protocolVersion() || response.SessionCapability == "" ||
		response.HelperInstanceID == "" || response.SessionGeneration == 0 || response.ReapTimeout <= 0 {
		_ = connection.Close()
		return nil, errors.New("OCI helper returned an invalid handshake")
	}
	if client.ExpectedChecksum != "" && response.HelperChecksum != client.ExpectedChecksum {
		_ = connection.Close()
		return nil, &RPCError{Code: CodeChecksumMismatch, Message: "helper checksum does not match local expectation"}
	}
	pumpCtx, pumpCancel := context.WithCancel(context.Background())
	session := &Session{
		client: client, capability: response.SessionCapability, control: connection, controlWire: wire, response: response,
		pending: make(map[string]pendingRenewal), queued: make(chan struct{}, 1),
		pumpCtx: pumpCtx, pumpCancel: pumpCancel, pumpDone: make(chan struct{}),
	}
	if client.disableHeartbeatPump {
		close(session.pumpDone)
	} else {
		go session.heartbeatPump()
	}
	return session, nil
}

func (session *Session) Handshake() AcquireSessionResponse {
	if session == nil {
		return AcquireSessionResponse{}
	}
	response := session.response
	response.SessionCapability = ""
	return response
}

func (session *Session) Close() error {
	if session == nil {
		return nil
	}
	var err error
	session.closeOnce.Do(func() {
		session.queueMu.Lock()
		session.closed = true
		session.queueMu.Unlock()
		session.pumpCancel()
		err = session.control.Close()
		<-session.pumpDone
	})
	return err
}

// HealthError reports whether the exclusive helper session can still be used.
// A disabled heartbeat pump is a test-only mode and remains healthy until
// Close; production sessions fail here as soon as their pump loses authority.
func (session *Session) HealthError() error {
	if session == nil {
		return errors.New("OCI helper session is unavailable")
	}
	session.queueMu.Lock()
	defer session.queueMu.Unlock()
	if session.closed {
		return errors.New("OCI helper session is closed")
	}
	return session.pumpErr
}

// SetLossHandler installs the callback run synchronously by the heartbeat
// pump after helper authority is known lost and before the pump exits.
func (session *Session) SetLossHandler(handler func(error)) {
	if session == nil {
		return
	}
	session.queueMu.Lock()
	session.lossHandler = handler
	lossErr := session.pumpErr
	closed := session.closed
	session.queueMu.Unlock()
	if handler != nil && lossErr != nil && !closed {
		handler(lossErr)
	}
}

// QueueAttemptRenewal records successful L1 lease evidence for the heartbeat
// pump. It is the only API that can renew an attempt deadman.
func (session *Session) QueueAttemptRenewal(authority AttemptAuthority, ttl time.Duration) error {
	if session == nil || session.control == nil {
		return errors.New("OCI helper session is closed")
	}
	if err := authority.validate(); err != nil {
		return err
	}
	if ttl <= 0 || ttl > session.response.MaximumAttemptDeadman {
		return errors.New("attempt deadman TTL is outside helper bounds")
	}
	if !session.client.disableHeartbeatPump {
		select {
		case <-session.pumpDone:
			return errors.New("OCI helper heartbeat pump has stopped")
		default:
		}
	}
	session.queueMu.Lock()
	session.queueToken++
	session.pending[authority.key()] = pendingRenewal{
		renewal: DeadmanRenewal{Authority: authority, TTL: ttl}, token: session.queueToken,
	}
	session.queueMu.Unlock()
	notify(session.queued)
	return nil
}

func (session *Session) heartbeatPump() {
	defer close(session.pumpDone)
	interval := session.client.HeartbeatInterval
	if interval <= 0 || interval >= session.response.HeartbeatTimeout {
		interval = session.response.HeartbeatTimeout / 3
	}
	if interval <= 0 {
		interval = time.Millisecond
	}
	timer := time.NewTimer(interval)
	defer timer.Stop()
	for {
		select {
		case <-session.pumpCtx.Done():
			return
		case <-session.queued:
		case <-timer.C:
		}
		ctx, cancel := context.WithTimeout(session.pumpCtx, interval)
		err := session.flushHeartbeat(ctx)
		cancel()
		if err != nil {
			session.markLost(runtimeLossError(err))
			_ = session.control.Close()
			return
		}
		timer.Reset(interval)
	}
}

func (session *Session) flushHeartbeat(ctx context.Context) error {
	session.controlMu.Lock()
	defer session.controlMu.Unlock()
	if err := applyContextDeadline(ctx, session.control); err != nil {
		return err
	}
	session.queueMu.Lock()
	renewals := make([]DeadmanRenewal, 0, len(session.pending))
	snapshot := make(map[string]uint64, len(session.pending))
	for key, pending := range session.pending {
		snapshot[key] = pending.token
		renewals = append(renewals, pending.renewal)
	}
	session.sequence++
	sequence := session.sequence
	session.queueMu.Unlock()
	body, err := marshalBody(HeartbeatRequest{Sequence: sequence, RenewedAttempts: renewals})
	if err != nil {
		return err
	}
	if err := session.controlWire.write(frame{
		Version: session.client.protocolVersion(), Method: MethodHeartbeat,
		SessionCapability: session.capability, Body: body,
	}); err != nil {
		return fmt.Errorf("send OCI helper heartbeat: %w", err)
	}
	if err := decodeResponse(session.controlWire, &struct{}{}); err != nil {
		return err
	}
	session.queueMu.Lock()
	for key, token := range snapshot {
		if session.pending[key].token == token {
			delete(session.pending, key)
		}
	}
	session.queueMu.Unlock()
	return session.control.SetDeadline(time.Time{})
}

type pendingRenewal struct {
	renewal DeadmanRenewal
	token   uint64
}

func (session *Session) EnsureImage(ctx context.Context, request EnsureImageRequest, receive func(EnsureImageEvent) error) error {
	if request.Source == "" {
		request.Source = ImageSourceRegistry
	}
	return session.stream(ctx, MethodEnsureImage, request, false, func(wire *framedConn, raw frame) error {
		var event EnsureImageEvent
		if err := decodeBody(raw.Body, &event); err != nil {
			return err
		}
		if err := validateEnsureImageEvent(event); err != nil {
			return err
		}
		if receive != nil {
			return receive(event)
		}
		return nil
	})
}

func (session *Session) ReconcileImagePins(ctx context.Context, request ReconcileImagePinsRequest) (ReconcileImagePinsResponse, error) {
	var response ReconcileImagePinsResponse
	err := session.call(ctx, MethodReconcileImagePins, request, &response)
	return response, err
}

func (session *Session) ReleaseImagePin(ctx context.Context, jobID string) error {
	return session.call(ctx, MethodReleaseImagePin, ReleaseImagePinRequest{JobID: jobID}, &struct{}{})
}

func (session *Session) ReleaseAttemptImagePin(ctx context.Context, authority AttemptAuthority) error {
	return session.call(ctx, MethodReleaseAttemptPin, ReleaseAttemptImagePinRequest{Authority: authority}, &struct{}{})
}

func (session *Session) ImageCacheStatus(ctx context.Context) (ImageCacheStatus, error) {
	var response ImageCacheStatus
	err := session.call(ctx, MethodImageCacheStatus, struct{}{}, &response)
	return response, err
}

// ImportImage streams one verified OCI image-layout archive into the helper.
// The raw bytes never enter a JSON frame or an argv/path supplied to root.
func (session *Session) ImportImage(ctx context.Context, request EnsureImageRequest, archive io.Reader, receive func(EnsureImageEvent) error) error {
	if archive == nil {
		return errors.New("OCI image archive reader is required")
	}
	request.Source = ImageSourceArchive
	connection, wire, err := session.dialRequest(ctx, MethodEnsureImage, request)
	if err != nil {
		return err
	}
	defer connection.Close()
	if err := decodeResponse(wire, &struct{}{}); err != nil {
		return session.markOperationFailure(ctx, err)
	}
	copyDone := make(chan error, 1)
	var closeSource sync.Once
	closeArchive := func() {
		closeSource.Do(func() {
			if closer, ok := archive.(io.Closer); ok {
				_ = closer.Close()
			}
		})
	}
	stopSourceCancellation := context.AfterFunc(ctx, closeArchive)
	defer stopSourceCancellation()
	go func() {
		_, copyErr := io.Copy(connection, archive)
		if closeErr := closeWrite(connection); copyErr == nil {
			copyErr = closeErr
		}
		copyDone <- copyErr
	}()
	streamErr := session.receiveImageEvents(ctx, wire, receive)
	if streamErr != nil {
		_ = connection.Close()
		closeArchive()
	}
	copyErr := <-copyDone
	if streamErr != nil {
		return streamErr
	}
	if copyErr != nil {
		return fmt.Errorf("stream OCI image archive: %w", copyErr)
	}
	return nil
}

func (session *Session) receiveImageEvents(ctx context.Context, wire *framedConn, receive func(EnsureImageEvent) error) error {
	for {
		var response frame
		if err := wire.read(&response); err != nil {
			err = fmt.Errorf("receive OCI helper image stream: %w", err)
			return session.markOperationFailure(ctx, err)
		}
		if response.Version != session.client.protocolVersion() {
			return &RPCError{Code: CodeVersionMismatch, Message: "helper response used an unsupported version"}
		}
		if response.Error != nil {
			return session.markOperationFailure(ctx, response.Error)
		}
		if !response.OK {
			return errors.New("OCI helper returned neither success nor error")
		}
		if len(response.Body) == 0 {
			return nil
		}
		var event EnsureImageEvent
		if err := decodeBody(response.Body, &event); err != nil {
			return err
		}
		if err := validateEnsureImageEvent(event); err != nil {
			return err
		}
		if receive != nil {
			if err := receive(event); err != nil {
				return err
			}
		}
	}
}

func (session *Session) Run(ctx context.Context, request RunRequest) (RunResponse, error) {
	var response RunResponse
	err := session.call(ctx, MethodRun, request, &response)
	return response, err
}

func (session *Session) Signal(ctx context.Context, request SignalRequest) error {
	return session.call(ctx, MethodSignal, request, &struct{}{})
}

func (session *Session) SetComputerControlState(ctx context.Context, request SetComputerControlStateRequest) error {
	return session.call(ctx, MethodSetComputerControl, request, &struct{}{})
}

func (session *Session) Watch(ctx context.Context, request WatchRequest, receive func(WatchEvent) error) error {
	return session.stream(ctx, MethodWatch, request, true, func(wire *framedConn, raw frame) error {
		var event WatchEvent
		if err := decodeBody(raw.Body, &event); err != nil {
			return err
		}
		if err := validateWatchEvent(event); err != nil {
			return err
		}
		if receive != nil {
			return receive(event)
		}
		return nil
	})
}

func (session *Session) Delete(ctx context.Context, request DeleteRequest) (DeleteResponse, error) {
	var response DeleteResponse
	err := session.call(ctx, MethodDelete, request, &response)
	return response, err
}

func (session *Session) DeleteManagedVolume(ctx context.Context, request DeleteManagedVolumeRequest) (DeleteManagedVolumeResponse, error) {
	var response DeleteManagedVolumeResponse
	err := session.call(ctx, MethodDeleteVolume, request, &response)
	return response, err
}

func (session *Session) InventoryRemoval(ctx context.Context, request InventoryRemovalRequest) (InventoryRemovalResponse, error) {
	var response InventoryRemovalResponse
	err := session.call(ctx, MethodInventoryRemoval, request, &response)
	return response, err
}

func (session *Session) AttestRemoval(ctx context.Context, request AttestRemovalRequest) (AttestRemovalResponse, error) {
	var response AttestRemovalResponse
	err := session.call(ctx, MethodAttestRemoval, request, &response)
	return response, err
}

func (session *Session) ResetComputerStorage(ctx context.Context, request ResetComputerStorageRequest) (ResetComputerStorageResponse, error) {
	var response ResetComputerStorageResponse
	err := session.call(ctx, MethodResetStorage, request, &response)
	return response, err
}

func (session *Session) Verify(ctx context.Context, request VerifyRequest) (VerifyResponse, error) {
	var response VerifyResponse
	err := session.call(ctx, MethodVerify, request, &response)
	return response, err
}

func (session *Session) Sweep(ctx context.Context, request SweepRequest) (SweepResponse, error) {
	var response SweepResponse
	err := session.call(ctx, MethodSweep, request, &response)
	return response, err
}

func (session *Session) DialAttemptPort(ctx context.Context, request DialAttemptPortRequest) (net.Conn, error) {
	return session.openStream(ctx, MethodDialAttemptPort, request, attemptPortBackendReady, true)
}

func (session *Session) DialHostBridge(ctx context.Context, request DialHostBridgeRequest) (net.Conn, error) {
	return session.openStream(ctx, MethodDialHostBridge, request, 0, false)
}

func (session *Session) call(ctx context.Context, method Method, request, response any) error {
	connection, wire, err := session.dialRequest(ctx, method, request)
	if err != nil {
		return err
	}
	defer connection.Close()
	err = decodeResponse(wire, response)
	return session.markOperationFailure(ctx, err)
}

func (session *Session) stream(ctx context.Context, method Method, request any, acknowledge bool, receive func(*framedConn, frame) error) error {
	connection, wire, err := session.dialRequest(ctx, method, request)
	if err != nil {
		return err
	}
	defer connection.Close()
	for {
		var response frame
		if err := wire.read(&response); err != nil {
			err = fmt.Errorf("receive OCI helper stream: %w", err)
			return session.markOperationFailure(ctx, err)
		}
		if response.Version != session.client.protocolVersion() {
			return &RPCError{Code: CodeVersionMismatch, Message: "helper response used an unsupported version"}
		}
		if response.Error != nil {
			return session.markOperationFailure(ctx, response.Error)
		}
		if !response.OK {
			return errors.New("OCI helper returned neither success nor error")
		}
		if len(response.Body) == 0 {
			return nil
		}
		if err := receive(wire, response); err != nil {
			return err
		}
		if acknowledge {
			if err := writeAll(connection, []byte{1}); err != nil {
				err = fmt.Errorf("acknowledge OCI helper stream event: %w", err)
				return session.markOperationFailure(ctx, err)
			}
		}
	}
}

func (session *Session) openStream(ctx context.Context, method Method, request any, readyMarker byte, detachSuccessful bool) (net.Conn, error) {
	connection, wire, err := session.dialRequest(ctx, method, request)
	if err != nil {
		return nil, err
	}
	if err := decodeResponse(wire, &struct{}{}); err != nil {
		err = session.markOperationFailure(ctx, err)
		_ = connection.Close()
		return nil, err
	}
	if _, err := connection.Write([]byte{1}); err != nil {
		_ = connection.Close()
		err = fmt.Errorf("acknowledge OCI helper stream: %w", err)
		err = session.markOperationFailure(ctx, err)
		return nil, err
	}
	if readyMarker != 0 {
		var marker [1]byte
		if _, err := io.ReadFull(connection, marker[:]); err != nil {
			_ = connection.Close()
			err = fmt.Errorf("await OCI helper stream backend: %w", err)
			err = session.markOperationFailure(ctx, err)
			return nil, err
		}
		if marker[0] != readyMarker {
			_ = connection.Close()
			return nil, errors.New("OCI helper stream returned an invalid backend-ready marker")
		}
	}
	if err := connection.SetDeadline(time.Time{}); err != nil {
		_ = connection.Close()
		return nil, err
	}
	if detachSuccessful {
		operation, ok := connection.(*clientOperationConn)
		if !ok {
			_ = connection.Close()
			return nil, errors.New("OCI helper stream operation wrapper is missing")
		}
		if err := operation.detachSetupContext(ctx); err != nil {
			return nil, err
		}
	}
	return connection, nil
}

// StreamSetupCancelledError means cancellation won the race with detaching a
// successfully authorized attempt-port stream from its setup context.
type StreamSetupCancelledError struct{ Cause error }

func (err *StreamSetupCancelledError) Error() string {
	return "OCI helper stream setup was cancelled: " + err.Cause.Error()
}
func (err *StreamSetupCancelledError) Unwrap() error { return err.Cause }

// markOperationFailure turns transport replacement/error into a synchronous
// loss of session authority. Caller cancellation and ordinary helper RPC
// refusals do not invalidate an otherwise healthy helper session.
func (session *Session) markOperationFailure(ctx context.Context, err error) error {
	if err == nil {
		return nil
	}
	if ctx.Err() != nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	var rpcErr *RPCError
	if errors.As(err, &rpcErr) && !rpcErrorProvesRuntimeLoss(rpcErr) {
		return err
	}
	loss := runtimeLossError(err)
	session.markLost(loss)
	return loss
}

func rpcErrorProvesRuntimeLoss(err *RPCError) bool {
	if err == nil {
		return false
	}
	if err.Code == CodeSessionStale || err.Code == CodeEngineFailure {
		return true
	}
	return err.ImageFailure != nil && err.ImageFailure.Kind == ImageFailureEngineLoss
}

func runtimeLossError(err error) error {
	var loss *RuntimeLossError
	if errors.As(err, &loss) {
		return err
	}
	return &RuntimeLossError{Cause: err}
}

func (session *Session) markLost(err error) {
	if session == nil || err == nil {
		return
	}
	session.lossOnce.Do(func() {
		session.queueMu.Lock()
		if session.closed {
			session.queueMu.Unlock()
			return
		}
		session.pumpErr = err
		handler := session.lossHandler
		session.queueMu.Unlock()
		if handler != nil {
			handler(err)
		}
		_ = session.control.Close()
	})
}

func (session *Session) dialRequest(ctx context.Context, method Method, request any) (net.Conn, *framedConn, error) {
	if session == nil || session.client == nil || session.capability == "" {
		return nil, nil, errors.New("OCI helper session is closed")
	}
	connection, err := session.client.Dial(ctx)
	if err != nil {
		err = fmt.Errorf("dial OCI helper RPC: %w", err)
		return nil, nil, session.markOperationFailure(ctx, err)
	}
	if err := applyContextDeadline(ctx, connection); err != nil {
		_ = connection.Close()
		return nil, nil, err
	}
	rawConnection := connection
	stop := context.AfterFunc(ctx, func() { _ = rawConnection.Close() })
	connection = &clientOperationConn{Conn: rawConnection, stop: stop}
	body, err := marshalBody(request)
	if err != nil {
		_ = connection.Close()
		return nil, nil, err
	}
	wire := newFramedConn(connection)
	if err := wire.write(frame{
		Version: session.client.protocolVersion(), Method: method,
		SessionCapability: session.capability, Body: body,
	}); err != nil {
		_ = connection.Close()
		err = fmt.Errorf("send OCI helper %s: %w", method, err)
		return nil, nil, session.markOperationFailure(ctx, err)
	}
	return connection, wire, nil
}

type clientOperationConn struct {
	net.Conn
	stop func() bool
}

func (connection *clientOperationConn) detachSetupContext(ctx context.Context) error {
	if connection.stop() {
		return nil
	}
	_ = connection.Close()
	cause := ctx.Err()
	if cause == nil {
		cause = context.Canceled
	}
	return &StreamSetupCancelledError{Cause: cause}
}

func closeWrite(connection net.Conn) error {
	if half, ok := connection.(interface{ CloseWrite() error }); ok {
		return half.CloseWrite()
	}
	return errors.New("OCI helper transport does not support a write half-close")
}

func (connection *clientOperationConn) CloseWrite() error {
	return closeWrite(connection.Conn)
}

func (connection *clientOperationConn) Close() error {
	connection.stop()
	return connection.Conn.Close()
}

func decodeResponse(wire *framedConn, target any) error {
	var response frame
	if err := wire.read(&response); err != nil {
		return fmt.Errorf("receive OCI helper response: %w", err)
	}
	if response.Version != ProtocolVersion {
		return &RPCError{Code: CodeVersionMismatch, Message: "helper response used an unsupported version"}
	}
	if response.Error != nil {
		return response.Error
	}
	if !response.OK {
		return errors.New("OCI helper returned neither success nor error")
	}
	if target == nil || len(response.Body) == 0 {
		return nil
	}
	if err := decodeBody(response.Body, target); err != nil {
		return fmt.Errorf("decode OCI helper response: %w", err)
	}
	return nil
}

func applyContextDeadline(ctx context.Context, connection net.Conn) error {
	if deadline, ok := ctx.Deadline(); ok {
		return connection.SetDeadline(deadline)
	}
	return connection.SetDeadline(time.Time{})
}
