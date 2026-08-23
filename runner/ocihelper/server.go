package ocihelper

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"slices"
	"sync"
	"time"
)

const (
	defaultHeartbeatTimeout = 3 * time.Second
	defaultMaximumDeadman   = 2 * time.Minute
	defaultReapTimeout      = 10 * time.Second
	defaultConnectionLimit  = 64
)

type Peer struct {
	UID uint32
	GID uint32
}

func (peer Peer) authorityKey() string { return fmt.Sprintf("%d:%d", peer.UID, peer.GID) }

type ServerConfig struct {
	HelperVersion         string
	HelperChecksum        string
	HeartbeatTimeout      time.Duration
	MaximumAttemptDeadman time.Duration
	ReapTimeout           time.Duration
	RequestTimeout        time.Duration
	ConnectionLimit       int
	AllowedUIDs           []uint32
	AllowedMountRoots     []string
	Clock                 Clock
}

type Server struct {
	engine      Engine
	config      ServerConfig
	createSweep sync.RWMutex
	connections chan struct{}

	sessionMu sync.Mutex
	active    *serverSession
	swept     bool
	listener  net.Listener
	serveCtx  context.Context
	fatalErr  error
	fatalOnce sync.Once
}

type attemptState string

const (
	attemptStarting   attemptState = "starting"
	attemptLive       attemptState = "live"
	attemptReaping    attemptState = "reaping"
	attemptTombstoned attemptState = "tombstoned"
)

type serverAttempt struct {
	authority        AttemptAuthority
	state            attemptState
	port             uint16
	bridgeCapability string
	deadline         time.Time
	deadlineChanged  chan struct{}
	watchDone        chan struct{}
	reaped           chan struct{}
	runCancel        context.CancelFunc
	closeWatch       sync.Once
	closeReaped      sync.Once
}

func (attempt *serverAttempt) stopWatcher() {
	attempt.closeWatch.Do(func() { close(attempt.watchDone) })
}

type sessionOperation struct {
	session *serverSession
	conn    net.Conn
	ctx     context.Context
	cancel  context.CancelFunc
	done    chan struct{}
	once    sync.Once
}

func (operation *sessionOperation) finish() {
	operation.once.Do(func() {
		operation.cancel()
		operation.session.mu.Lock()
		delete(operation.session.operations, operation)
		operation.session.mu.Unlock()
		close(operation.done)
	})
}

func (operation *sessionOperation) monitorEOF() {
	go func() {
		var byteBuffer [1]byte
		_, _ = operation.conn.Read(byteBuffer[:])
		operation.cancel()
	}()
}

type serverSession struct {
	server     *Server
	identity   SessionIdentity
	peerKey    string
	capability string
	control    net.Conn

	mu                sync.Mutex
	closed            bool
	sequence          uint64
	heartbeatDeadline time.Time
	heartbeatChanged  chan struct{}
	done              chan struct{}
	attempts          map[string]*serverAttempt
	operations        map[*sessionOperation]struct{}
	internalReaps     sync.WaitGroup
	invalidateOnce    sync.Once
}

func NewServer(engine Engine, config ServerConfig) (*Server, error) {
	if engine == nil {
		return nil, errors.New("OCI helper engine is required")
	}
	if len(config.AllowedUIDs) == 0 {
		return nil, errors.New("at least one allowed OCI helper peer UID is required")
	}
	if config.HeartbeatTimeout <= 0 {
		config.HeartbeatTimeout = defaultHeartbeatTimeout
	}
	if config.MaximumAttemptDeadman <= 0 {
		config.MaximumAttemptDeadman = defaultMaximumDeadman
	}
	if config.ReapTimeout <= 0 {
		config.ReapTimeout = defaultReapTimeout
	}
	if config.RequestTimeout <= 0 {
		config.RequestTimeout = defaultRequestTimeout
	}
	if config.ConnectionLimit <= 0 {
		config.ConnectionLimit = defaultConnectionLimit
	}
	if config.Clock == nil {
		config.Clock = systemClock{}
	}
	config.AllowedUIDs = slices.Clone(config.AllowedUIDs)
	config.AllowedMountRoots = slices.Clone(config.AllowedMountRoots)
	return &Server{
		engine: engine, config: config,
		connections: make(chan struct{}, config.ConnectionLimit),
	}, nil
}

func (server *Server) Serve(ctx context.Context, listener net.Listener) error {
	if listener == nil {
		return errors.New("OCI helper listener is required")
	}
	server.sessionMu.Lock()
	server.listener = listener
	server.serveCtx = ctx
	server.sessionMu.Unlock()
	go func() {
		<-ctx.Done()
		server.sessionMu.Lock()
		active := server.active
		server.sessionMu.Unlock()
		if active != nil {
			active.invalidate("helper shutdown")
		}
		_ = listener.Close()
	}()
	for {
		connection, err := listener.Accept()
		if err != nil {
			server.sessionMu.Lock()
			fatalErr := server.fatalErr
			server.sessionMu.Unlock()
			if fatalErr != nil {
				return fatalErr
			}
			if ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
				return nil
			}
			return fmt.Errorf("accept OCI helper connection: %w", err)
		}
		select {
		case server.connections <- struct{}{}:
			go func() {
				defer func() { <-server.connections }()
				server.handleConnection(ctx, connection)
			}()
		default:
			_ = connection.Close()
		}
	}
}

func (server *Server) fail(err error) {
	server.fatalOnce.Do(func() {
		server.sessionMu.Lock()
		server.fatalErr = err
		listener := server.listener
		server.sessionMu.Unlock()
		if listener != nil {
			_ = listener.Close()
		}
	})
}

func (server *Server) handleConnection(ctx context.Context, connection net.Conn) {
	defer connection.Close()
	if err := connection.SetReadDeadline(time.Now().Add(server.config.RequestTimeout)); err != nil {
		return
	}
	peer, peerErr := authenticateUnixPeer(connection)
	wire := newFramedConn(connection)
	var request frame
	if err := wire.read(&request); err != nil {
		return
	}
	// Capture kernel credentials before reading, but consume exactly one
	// bounded initial frame so a well-formed unauthorized peer receives a
	// deterministic typed refusal instead of an OS-dependent EOF or EPIPE.
	if peerErr != nil || !slices.Contains(server.config.AllowedUIDs, peer.UID) {
		_ = writeFailure(wire, CodePeerUnauthenticated, "peer authentication failed")
		return
	}
	_ = connection.SetReadDeadline(time.Time{})
	if request.Version != ProtocolVersion {
		_ = writeFailure(wire, CodeVersionMismatch, "unsupported OCI helper protocol version")
		return
	}
	if request.Method == MethodAcquireSession {
		server.acquireSession(ctx, connection, wire, peer, request)
		return
	}
	session, rpcErr := server.authorizeSession(peer, request.SessionCapability)
	if rpcErr != nil {
		_ = writeRPCError(wire, rpcErr)
		return
	}
	operation, rpcErr := session.beginOperation(ctx, connection)
	if rpcErr != nil {
		_ = writeRPCError(wire, rpcErr)
		return
	}
	defer operation.finish()
	server.dispatch(operation, wire, request)
}

func (server *Server) acquireSession(ctx context.Context, connection net.Conn, wire *framedConn, peer Peer, request frame) {
	var body AcquireSessionRequest
	if err := decodeBody(request.Body, &body); err != nil {
		_ = writeFailure(wire, CodeInvalidRequest, err.Error())
		return
	}
	if body.NodeID == "" || body.BootSessionID == "" {
		_ = writeFailure(wire, CodeInvalidRequest, "node and boot session are required")
		return
	}
	if body.ExpectedHelperChecksum != "" && body.ExpectedHelperChecksum != server.config.HelperChecksum {
		_ = writeFailure(wire, CodeChecksumMismatch, "helper checksum does not match agent expectation")
		return
	}
	capability, err := randomCapability()
	if err != nil {
		_ = writeFailure(wire, CodeEngineFailure, "session capability generation failed")
		return
	}
	now := server.config.Clock.Now()
	session := &serverSession{
		server: server, identity: SessionIdentity{NodeID: body.NodeID, BootSessionID: body.BootSessionID},
		peerKey: peer.authorityKey(), capability: capability, control: connection,
		heartbeatDeadline: now.Add(server.config.HeartbeatTimeout), heartbeatChanged: make(chan struct{}, 1),
		done: make(chan struct{}), attempts: make(map[string]*serverAttempt), operations: make(map[*sessionOperation]struct{}),
	}
	server.sessionMu.Lock()
	if server.active != nil {
		server.sessionMu.Unlock()
		_ = writeFailure(wire, CodeSessionBusy, "an OCI helper session already owns this helper")
		return
	}
	server.active = session
	server.sessionMu.Unlock()
	response := AcquireSessionResponse{
		ProtocolVersion: ProtocolVersion, HelperVersion: server.config.HelperVersion,
		HelperChecksum: server.config.HelperChecksum, SessionCapability: capability,
		HeartbeatTimeout: server.config.HeartbeatTimeout, MaximumAttemptDeadman: server.config.MaximumAttemptDeadman,
	}
	if err := writeSuccess(wire, response); err != nil {
		session.invalidate("session handshake response failed")
		return
	}
	go session.watchHeartbeat()
	for {
		_ = connection.SetReadDeadline(time.Now().Add(server.config.HeartbeatTimeout))
		var heartbeat frame
		if err := wire.read(&heartbeat); err != nil {
			session.invalidate("session control EOF")
			return
		}
		if heartbeat.Version != ProtocolVersion || heartbeat.Method != MethodHeartbeat || heartbeat.SessionCapability != capability {
			_ = writeFailure(wire, CodeSessionStale, "invalid session heartbeat")
			session.invalidate("invalid session heartbeat")
			return
		}
		if rpcErr := session.applyHeartbeat(heartbeat.Body); rpcErr != nil {
			_ = writeRPCError(wire, rpcErr)
			session.invalidate("invalid session heartbeat body")
			return
		}
		if err := writeSuccess(wire, struct{}{}); err != nil {
			session.invalidate("session heartbeat response failed")
			return
		}
		select {
		case <-ctx.Done():
			session.invalidate("helper context canceled")
			return
		default:
		}
	}
}

func (server *Server) authorizeSession(peer Peer, capability string) (*serverSession, *RPCError) {
	server.sessionMu.Lock()
	active := server.active
	server.sessionMu.Unlock()
	if active == nil || capability == "" {
		return nil, &RPCError{Code: CodeSessionStale, Message: "OCI helper session is not active"}
	}
	active.mu.Lock()
	defer active.mu.Unlock()
	if active.closed || active.capability != capability || active.peerKey != peer.authorityKey() {
		return nil, &RPCError{Code: CodeSessionStale, Message: "OCI helper session capability is stale"}
	}
	return active, nil
}

func (session *serverSession) beginOperation(parent context.Context, connection net.Conn) (*sessionOperation, *RPCError) {
	ctx, cancel := context.WithCancel(parent)
	operation := &sessionOperation{session: session, conn: connection, ctx: ctx, cancel: cancel, done: make(chan struct{})}
	session.mu.Lock()
	if session.closed {
		session.mu.Unlock()
		cancel()
		return nil, &RPCError{Code: CodeSessionStale, Message: "OCI helper session has ended"}
	}
	session.operations[operation] = struct{}{}
	session.mu.Unlock()
	return operation, nil
}

func (session *serverSession) applyHeartbeat(raw json.RawMessage) *RPCError {
	var heartbeat HeartbeatRequest
	if err := decodeBody(raw, &heartbeat); err != nil {
		return &RPCError{Code: CodeInvalidRequest, Message: err.Error()}
	}
	session.mu.Lock()
	defer session.mu.Unlock()
	now := session.server.config.Clock.Now()
	if session.closed || !now.Before(session.heartbeatDeadline) || heartbeat.Sequence <= session.sequence {
		return &RPCError{Code: CodeSessionStale, Message: "heartbeat sequence is stale"}
	}
	for _, renewal := range heartbeat.RenewedAttempts {
		if err := renewal.Authority.validate(); err != nil {
			return &RPCError{Code: CodeInvalidRequest, Message: err.Error()}
		}
		if renewal.TTL <= 0 || renewal.TTL > session.server.config.MaximumAttemptDeadman {
			return &RPCError{Code: CodeInvalidRequest, Message: "attempt deadman TTL is outside helper bounds"}
		}
		attempt := session.attempts[renewal.Authority.key()]
		if attempt == nil || (attempt.state != attemptStarting && attempt.state != attemptLive) || !now.Before(attempt.deadline) || attempt.authority != renewal.Authority {
			return &RPCError{Code: CodeUnauthorizedAttempt, Message: "attempt renewal is not owned by this session"}
		}
		attempt.deadline = now.Add(renewal.TTL)
		notify(attempt.deadlineChanged)
	}
	session.sequence = heartbeat.Sequence
	session.heartbeatDeadline = now.Add(session.server.config.HeartbeatTimeout)
	notify(session.heartbeatChanged)
	return nil
}

func notify(channel chan struct{}) {
	select {
	case channel <- struct{}{}:
	default:
	}
}

func (session *serverSession) watchHeartbeat() {
	session.mu.Lock()
	deadline := session.heartbeatDeadline
	session.mu.Unlock()
	timer := session.server.config.Clock.NewTimerAt(deadline)
	defer timer.Stop()
	for {
		select {
		case <-session.done:
			return
		case <-session.heartbeatChanged:
			session.mu.Lock()
			deadline = session.heartbeatDeadline
			session.mu.Unlock()
			resetTimerAt(timer, deadline)
		case <-timer.C():
			session.mu.Lock()
			deadline = session.heartbeatDeadline
			expired := !session.server.config.Clock.Now().Before(deadline)
			session.mu.Unlock()
			if !expired {
				resetTimerAt(timer, deadline)
				continue
			}
			session.invalidate("session heartbeat deadline expired")
			return
		}
	}
}

func resetTimerAt(timer Timer, deadline time.Time) {
	if !timer.Stop() {
		select {
		case <-timer.C():
		default:
		}
	}
	timer.ResetAt(deadline)
}

func (session *serverSession) invalidate(reason string) {
	session.invalidateOnce.Do(func() {
		session.mu.Lock()
		session.closed = true
		close(session.done)
		_ = session.control.Close()
		operations := make([]*sessionOperation, 0, len(session.operations))
		for operation := range session.operations {
			operations = append(operations, operation)
			operation.cancel()
			_ = operation.conn.Close()
		}
		for _, attempt := range session.attempts {
			if attempt.runCancel != nil {
				attempt.runCancel()
			}
			attempt.stopWatcher()
		}
		session.mu.Unlock()
		for _, operation := range operations {
			<-operation.done
		}
		session.internalReaps.Wait()
		session.server.createSweep.Lock()
		ctx, cancel := session.server.reapContext()
		reapErr := session.server.engine.ReapSession(ctx, session.identity)
		cancel()
		session.server.createSweep.Unlock()
		if reapErr != nil {
			session.server.fail(fmt.Errorf("reap OCI helper session after %s: %w", reason, reapErr))
			return
		}
		session.server.sessionMu.Lock()
		if session.server.active == session {
			session.server.active = nil
		}
		session.server.sessionMu.Unlock()
	})
}

func (server *Server) reapContext() (context.Context, context.CancelFunc) {
	server.sessionMu.Lock()
	parent := server.serveCtx
	server.sessionMu.Unlock()
	if parent == nil {
		parent = context.TODO()
	}
	return context.WithTimeout(context.WithoutCancel(parent), server.config.ReapTimeout)
}

func (session *serverSession) reserveAttempt(request RunRequest, runCancel context.CancelFunc) (*serverAttempt, *RPCError) {
	if request.InitialDeadman <= 0 || request.InitialDeadman > session.server.config.MaximumAttemptDeadman {
		return nil, &RPCError{Code: CodeInvalidRequest, Message: "initial attempt deadman is outside helper bounds"}
	}
	if err := validateWorkloadWire(request.Workload); err != nil {
		return nil, &RPCError{Code: CodeInvalidRequest, Message: err.Error()}
	}
	attempt := &serverAttempt{
		authority: request.Authority, state: attemptStarting,
		deadline:        session.server.config.Clock.Now().Add(request.InitialDeadman),
		deadlineChanged: make(chan struct{}, 1), watchDone: make(chan struct{}), reaped: make(chan struct{}),
		runCancel: runCancel,
	}
	session.mu.Lock()
	defer session.mu.Unlock()
	if session.closed {
		return nil, &RPCError{Code: CodeSessionStale, Message: "session ended while reserving attempt"}
	}
	key := request.Authority.key()
	if _, exists := session.attempts[key]; exists {
		return nil, &RPCError{Code: CodeUnauthorizedAttempt, Message: "attempt authority has already been used in this session"}
	}
	session.attempts[key] = attempt
	go session.watchAttempt(attempt)
	return attempt, nil
}

func (session *serverSession) completeAttempt(request RunRequest, attempt *serverAttempt, response *RunResponse) *RPCError {
	if !response.Started {
		return &RPCError{Code: CodeEngineFailure, Message: "engine Run returned without authoritative Started evidence"}
	}
	if response.HostBridgeReady && !request.EnableHostBridgeFallback {
		return &RPCError{Code: CodeEngineFailure, Message: "engine enabled an unrequested host bridge fallback"}
	}
	bridgeCapability := ""
	var err error
	if response.HostBridgeReady {
		bridgeCapability, err = randomCapability()
		if err != nil {
			return &RPCError{Code: CodeEngineFailure, Message: "bridge capability generation failed"}
		}
	}
	session.mu.Lock()
	defer session.mu.Unlock()
	if session.closed || session.attempts[request.Authority.key()] != attempt || attempt.state != attemptStarting {
		return &RPCError{Code: CodeSessionStale, Message: "attempt authority expired while starting"}
	}
	attempt.port = response.AttemptPort
	attempt.bridgeCapability = bridgeCapability
	attempt.state = attemptLive
	response.BridgeCapability = bridgeCapability
	return nil
}

func (session *serverSession) watchAttempt(attempt *serverAttempt) {
	session.mu.Lock()
	deadline := attempt.deadline
	session.mu.Unlock()
	timer := session.server.config.Clock.NewTimerAt(deadline)
	defer timer.Stop()
	for {
		select {
		case <-attempt.watchDone:
			return
		case <-attempt.deadlineChanged:
			session.mu.Lock()
			deadline = attempt.deadline
			session.mu.Unlock()
			resetTimerAt(timer, deadline)
		case <-timer.C():
			session.mu.Lock()
			deadline = attempt.deadline
			expired := !session.closed && (attempt.state == attemptStarting || attempt.state == attemptLive) && !session.server.config.Clock.Now().Before(deadline)
			session.mu.Unlock()
			if !expired {
				resetTimerAt(timer, deadline)
				continue
			}
			if err := session.reapAttempt(attempt, false); err != nil {
				go session.invalidate("attempt deadman reap failed")
			}
			return
		}
	}
}

func (session *serverSession) reapAttempt(attempt *serverAttempt, createGateHeld bool) error {
	session.mu.Lock()
	if session.closed {
		session.mu.Unlock()
		return nil
	}
	switch attempt.state {
	case attemptTombstoned:
		session.mu.Unlock()
		return nil
	case attemptReaping:
		reaped := attempt.reaped
		session.mu.Unlock()
		<-reaped
		return nil
	case attemptStarting, attemptLive:
		attempt.state = attemptReaping
		session.internalReaps.Add(1)
		if attempt.runCancel != nil {
			attempt.runCancel()
		}
		attempt.stopWatcher()
	}
	session.mu.Unlock()
	if !createGateHeld {
		session.server.createSweep.RLock()
		defer session.server.createSweep.RUnlock()
	}
	ctx, cancel := session.server.reapContext()
	err := session.server.engine.ReapAttempt(ctx, attempt.authority)
	cancel()
	session.mu.Lock()
	attempt.state = attemptTombstoned
	attempt.closeReaped.Do(func() { close(attempt.reaped) })
	session.mu.Unlock()
	session.internalReaps.Done()
	return err
}

func (session *serverSession) authorizeAttempt(authority AttemptAuthority) (*serverAttempt, *RPCError) {
	if err := authority.validate(); err != nil {
		return nil, &RPCError{Code: CodeInvalidRequest, Message: err.Error()}
	}
	session.mu.Lock()
	defer session.mu.Unlock()
	if session.closed || authority.NodeID != session.identity.NodeID || authority.BootSessionID != session.identity.BootSessionID {
		return nil, &RPCError{Code: CodeUnauthorizedAttempt, Message: "attempt is outside this helper session"}
	}
	attempt := session.attempts[authority.key()]
	if attempt == nil || attempt.state != attemptLive || attempt.authority != authority {
		return nil, &RPCError{Code: CodeUnauthorizedAttempt, Message: "attempt authority does not match a live attempt"}
	}
	return attempt, nil
}

func (server *Server) sweepRequired(method Method) bool {
	switch method {
	case MethodAcquireSession, MethodSweep, MethodVerify:
		return false
	case MethodEnsureImage, MethodRun, MethodSignal, MethodWatch, MethodDelete, MethodDialAttemptPort, MethodDialHostBridge:
		server.sessionMu.Lock()
		defer server.sessionMu.Unlock()
		return !server.swept
	default:
		return false
	}
}

func (server *Server) dispatch(operation *sessionOperation, wire *framedConn, request frame) {
	session := operation.session
	if server.sweepRequired(request.Method) {
		_ = writeFailure(wire, CodeSweepRequired, "a boot sweep must succeed before OCI operations")
		return
	}
	switch request.Method {
	case MethodEnsureImage:
		var body EnsureImageRequest
		if !decodeRequest(wire, request.Body, &body) {
			return
		}
		operation.monitorEOF()
		err := server.engine.EnsureImage(operation.ctx, body, func(event EnsureImageEvent) error {
			if err := validateEnsureImageEvent(event); err != nil {
				return err
			}
			return writeSuccess(wire, event)
		})
		writeStreamResult(wire, err)
	case MethodRun:
		var body RunRequest
		if !decodeRequest(wire, request.Body, &body) {
			return
		}
		if err := body.Authority.validate(); err != nil || body.Authority.NodeID != session.identity.NodeID || body.Authority.BootSessionID != session.identity.BootSessionID {
			_ = writeFailure(wire, CodeUnauthorizedAttempt, "Run authority is outside this helper session")
			return
		}
		if _, err := DeterministicResourceIdentity(body.Authority); err != nil {
			_ = writeFailure(wire, CodeInvalidRequest, err.Error())
			return
		}
		runContext, runCancel := context.WithCancel(operation.ctx)
		attempt, rpcErr := session.reserveAttempt(body, runCancel)
		if rpcErr != nil {
			runCancel()
			_ = writeRPCError(wire, rpcErr)
			return
		}
		operation.monitorEOF()
		server.createSweep.RLock()
		response, err := server.engine.Run(runContext, body)
		runCancel()
		if err == nil {
			if rpcErr := session.completeAttempt(body, attempt, &response); rpcErr != nil {
				err = rpcErr
			}
		}
		if err != nil {
			reapErr := session.reapAttempt(attempt, true)
			server.createSweep.RUnlock()
			if reapErr != nil {
				go session.invalidate("ambiguous Run reap failed")
			}
			var rpcErr *RPCError
			var specRejection *RuntimeSpecRejectionError
			if errors.As(err, &rpcErr) {
				_ = writeRPCError(wire, rpcErr)
			} else if errors.As(err, &specRejection) {
				_ = writeFailure(wire, CodeOCISpecRejected, "OCI runtime spec was rejected")
			} else {
				_ = writeFailure(wire, CodeEngineFailure, "OCI engine operation failed")
			}
			return
		}
		writeErr := writeSuccess(wire, response)
		if writeErr != nil {
			reapErr := session.reapAttempt(attempt, true)
			if reapErr != nil {
				go session.invalidate("undeliverable Run evidence reap failed")
			}
		}
		server.createSweep.RUnlock()
	case MethodSignal:
		var body SignalRequest
		if !decodeRequest(wire, request.Body, &body) || !authorizeRequest(wire, session, body.Authority) {
			return
		}
		if body.Signal != SignalTERM && body.Signal != SignalKILL {
			_ = writeFailure(wire, CodeInvalidRequest, "signal must be TERM or KILL")
			return
		}
		operation.monitorEOF()
		_ = writeEngineResponse(wire, struct{}{}, server.engine.Signal(operation.ctx, body))
	case MethodWatch:
		var body WatchRequest
		if !decodeRequest(wire, request.Body, &body) || !authorizeRequest(wire, session, body.Authority) {
			return
		}
		operation.monitorEOF()
		err := server.engine.Watch(operation.ctx, body, func(event WatchEvent) error {
			if err := validateWatchEvent(event); err != nil {
				return err
			}
			return writeSuccess(wire, event)
		})
		writeStreamResult(wire, err)
	case MethodDelete:
		var body DeleteRequest
		if !decodeRequest(wire, request.Body, &body) {
			return
		}
		attempt, rpcErr := session.authorizeAttempt(body.Authority)
		if rpcErr != nil {
			_ = writeRPCError(wire, rpcErr)
			return
		}
		operation.monitorEOF()
		response, err := server.engine.Delete(operation.ctx, body)
		if err == nil && response.Deleted {
			err = session.reapAttempt(attempt, false)
		}
		_ = writeEngineResponse(wire, response, err)
	case MethodVerify:
		var body VerifyRequest
		if !decodeRequest(wire, request.Body, &body) {
			return
		}
		if body.Scope == VerifyAttempt {
			if body.Authority == nil || !authorizeRequest(wire, session, *body.Authority) {
				return
			}
		} else if body.Scope != VerifyNamespace || body.Authority != nil {
			_ = writeFailure(wire, CodeInvalidRequest, "Verify requires exactly one valid scope")
			return
		}
		operation.monitorEOF()
		response, err := server.engine.Verify(operation.ctx, body)
		_ = writeEngineResponse(wire, response, err)
	case MethodSweep:
		var body SweepRequest
		if !decodeRequest(wire, request.Body, &body) {
			return
		}
		operation.monitorEOF()
		server.createSweep.Lock()
		response, err := server.engine.Sweep(operation.ctx, body)
		if err == nil {
			server.sessionMu.Lock()
			server.swept = true
			server.sessionMu.Unlock()
		}
		server.createSweep.Unlock()
		_ = writeEngineResponse(wire, response, err)
	case MethodDialAttemptPort:
		var body DialAttemptPortRequest
		if !decodeRequest(wire, request.Body, &body) {
			return
		}
		attempt, rpcErr := session.authorizeAttempt(body.Authority)
		if rpcErr != nil || body.Port == 0 || body.Port != attempt.port {
			_ = writeFailure(wire, CodeUnauthorizedPort, "port is not allocated to this live attempt")
			return
		}
		if err := writeSuccess(wire, struct{}{}); err != nil || !readStreamAcknowledgement(operation.conn) {
			return
		}
		_ = server.engine.DialAttemptPort(operation.ctx, body, &operationStream{Conn: operation.conn, cancel: operation.cancel})
	case MethodDialHostBridge:
		var body DialHostBridgeRequest
		if !decodeRequest(wire, request.Body, &body) {
			return
		}
		attempt, rpcErr := session.authorizeAttempt(body.Authority)
		if rpcErr != nil || attempt.bridgeCapability == "" || body.BridgeCapability != attempt.bridgeCapability {
			_ = writeFailure(wire, CodeUnauthorizedBridge, "bridge fallback is not authorized for this live attempt")
			return
		}
		if err := writeSuccess(wire, struct{}{}); err != nil || !readStreamAcknowledgement(operation.conn) {
			return
		}
		_ = server.engine.DialHostBridge(operation.ctx, body, &operationStream{Conn: operation.conn, cancel: operation.cancel})
	default:
		_ = writeFailure(wire, CodeUnsupportedOperation, "unknown OCI helper method")
	}
}

type operationStream struct {
	net.Conn
	cancel context.CancelFunc
}

func (stream *operationStream) Read(buffer []byte) (int, error) {
	count, err := stream.Conn.Read(buffer)
	if err != nil {
		stream.cancel()
	}
	return count, err
}

func (stream *operationStream) Write(buffer []byte) (int, error) {
	count, err := stream.Conn.Write(buffer)
	if err != nil {
		stream.cancel()
	}
	return count, err
}

func authorizeRequest(connection *framedConn, session *serverSession, authority AttemptAuthority) bool {
	_, rpcErr := session.authorizeAttempt(authority)
	if rpcErr != nil {
		_ = writeRPCError(connection, rpcErr)
		return false
	}
	return true
}

func decodeRequest(connection *framedConn, raw json.RawMessage, target any) bool {
	if err := decodeBody(raw, target); err != nil {
		_ = writeFailure(connection, CodeInvalidRequest, err.Error())
		return false
	}
	return true
}

func writeEngineResponse(connection *framedConn, response any, err error) error {
	if err != nil {
		return writeFailure(connection, CodeEngineFailure, "OCI engine operation failed")
	}
	return writeSuccess(connection, response)
}

func writeStreamResult(connection *framedConn, err error) {
	if err != nil {
		_ = writeFailure(connection, CodeEngineFailure, "OCI engine operation failed")
		return
	}
	_ = connection.write(frame{Version: ProtocolVersion, OK: true})
}

func readStreamAcknowledgement(connection io.Reader) bool {
	var acknowledgement [1]byte
	_, err := io.ReadFull(connection, acknowledgement[:])
	return err == nil && acknowledgement[0] == 1
}

func writeSuccess(connection *framedConn, body any) error {
	raw, err := marshalBody(body)
	if err != nil {
		return err
	}
	return connection.write(frame{Version: ProtocolVersion, OK: true, Body: raw})
}

func writeFailure(connection *framedConn, code ErrorCode, message string) error {
	return writeRPCError(connection, &RPCError{Code: code, Message: message})
}

func writeRPCError(connection *framedConn, rpcErr *RPCError) error {
	return connection.write(frame{Version: ProtocolVersion, Error: rpcErr})
}
