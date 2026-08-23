package ocihelper

import (
	"context"
	"errors"
	"fmt"
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

	controlMu  sync.Mutex
	queueMu    sync.Mutex
	pending    map[string]pendingRenewal
	queueToken uint64
	sequence   uint64
	queued     chan struct{}
	pumpCtx    context.Context
	pumpCancel context.CancelFunc
	pumpDone   chan struct{}
	pumpErr    error
	closeOnce  sync.Once
}

func (client *Client) protocolVersion() int {
	if client.Version == 0 {
		return ProtocolVersion
	}
	return client.Version
}

func (client *Client) OpenSession(ctx context.Context, request AcquireSessionRequest) (*Session, error) {
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
	if response.ProtocolVersion != client.protocolVersion() || response.SessionCapability == "" {
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
		session.pumpCancel()
		err = session.control.Close()
		<-session.pumpDone
	})
	return err
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
			session.queueMu.Lock()
			session.pumpErr = err
			session.queueMu.Unlock()
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
	return session.stream(ctx, MethodEnsureImage, request, func(wire *framedConn, raw frame) error {
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

func (session *Session) Run(ctx context.Context, request RunRequest) (RunResponse, error) {
	var response RunResponse
	err := session.call(ctx, MethodRun, request, &response)
	return response, err
}

func (session *Session) Signal(ctx context.Context, request SignalRequest) error {
	return session.call(ctx, MethodSignal, request, &struct{}{})
}

func (session *Session) Watch(ctx context.Context, request WatchRequest, receive func(WatchEvent) error) error {
	return session.stream(ctx, MethodWatch, request, func(wire *framedConn, raw frame) error {
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
	return session.openStream(ctx, MethodDialAttemptPort, request)
}

func (session *Session) DialHostBridge(ctx context.Context, request DialHostBridgeRequest) (net.Conn, error) {
	return session.openStream(ctx, MethodDialHostBridge, request)
}

func (session *Session) call(ctx context.Context, method Method, request, response any) error {
	connection, wire, err := session.dialRequest(ctx, method, request)
	if err != nil {
		return err
	}
	defer connection.Close()
	return decodeResponse(wire, response)
}

func (session *Session) stream(ctx context.Context, method Method, request any, receive func(*framedConn, frame) error) error {
	connection, wire, err := session.dialRequest(ctx, method, request)
	if err != nil {
		return err
	}
	defer connection.Close()
	for {
		var response frame
		if err := wire.read(&response); err != nil {
			return fmt.Errorf("receive OCI helper stream: %w", err)
		}
		if response.Version != session.client.protocolVersion() {
			return &RPCError{Code: CodeVersionMismatch, Message: "helper response used an unsupported version"}
		}
		if response.Error != nil {
			return response.Error
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
	}
}

func (session *Session) openStream(ctx context.Context, method Method, request any) (net.Conn, error) {
	connection, wire, err := session.dialRequest(ctx, method, request)
	if err != nil {
		return nil, err
	}
	if err := decodeResponse(wire, &struct{}{}); err != nil {
		_ = connection.Close()
		return nil, err
	}
	if _, err := connection.Write([]byte{1}); err != nil {
		_ = connection.Close()
		return nil, fmt.Errorf("acknowledge OCI helper stream: %w", err)
	}
	if err := connection.SetDeadline(time.Time{}); err != nil {
		_ = connection.Close()
		return nil, err
	}
	return connection, nil
}

func (session *Session) dialRequest(ctx context.Context, method Method, request any) (net.Conn, *framedConn, error) {
	if session == nil || session.client == nil || session.capability == "" {
		return nil, nil, errors.New("OCI helper session is closed")
	}
	connection, err := session.client.Dial(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("dial OCI helper RPC: %w", err)
	}
	if err := applyContextDeadline(ctx, connection); err != nil {
		_ = connection.Close()
		return nil, nil, err
	}
	stop := context.AfterFunc(ctx, func() { _ = connection.Close() })
	connection = &clientOperationConn{Conn: connection, stop: stop}
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
		return nil, nil, fmt.Errorf("send OCI helper %s: %w", method, err)
	}
	return connection, wire, nil
}

type clientOperationConn struct {
	net.Conn
	stop func() bool
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
