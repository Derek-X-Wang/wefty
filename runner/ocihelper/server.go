package ocihelper

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"net"
	"os"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
)

const (
	defaultHeartbeatTimeout = 3 * time.Second
	defaultMaximumDeadman   = 2 * time.Minute
	defaultReapTimeout      = 10 * time.Second
	defaultConnectionLimit  = 64
	maximumReapedBoots      = 256
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
	beforeRunCreateLock   func()
}

type Server struct {
	engine      Engine
	config      ServerConfig
	instanceID  string
	createSweep sync.RWMutex
	connections chan struct{}

	sessionMu             sync.Mutex
	active                *serverSession
	listener              net.Listener
	serveCtx              context.Context
	fatalErr              error
	fatalOnce             sync.Once
	nextSessionGeneration uint64
	startupSweep          *SweepResponse
	sessionReapSweep      *SweepResponse
	reapedBootSessions    map[SessionIdentity]uint64
	reapedBootSequence    uint64
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
	computerStorage  *ComputerStorageReference
	state            attemptState
	endpoints        map[string]uint16
	cgroupID         string
	bridgeCapability string
	computer         bool
	deadline         time.Time
	deadlineChanged  chan struct{}
	watchDone        chan struct{}
	reaped           chan struct{}
	runCancel        context.CancelFunc
	closeWatch       sync.Once
	closeReaped      sync.Once
	guardianReaping  bool
	guardianReaped   bool
	guardianDelete   bool
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

func (operation *sessionOperation) monitorAcknowledgements() <-chan error {
	acknowledged := make(chan error)
	go func() {
		defer close(acknowledged)
		var value [1]byte
		for {
			_, err := io.ReadFull(operation.conn, value[:])
			if err == nil && value[0] != 1 {
				err = errors.New("invalid OCI watch acknowledgement")
			}
			select {
			case acknowledged <- err:
			case <-operation.ctx.Done():
				return
			}
			if err != nil {
				operation.cancel()
				return
			}
		}
	}()
	return acknowledged
}

func (operation *sessionOperation) monitorAcknowledgement() <-chan error {
	acknowledged := make(chan error, 1)
	go func() {
		var value [1]byte
		_, err := io.ReadFull(operation.conn, value[:])
		if err == nil && value[0] != 1 {
			err = errors.New("invalid OCI stream acknowledgement")
		}
		if err != nil {
			operation.cancel()
		}
		acknowledged <- err
	}()
	return acknowledged
}

type serverSession struct {
	server     *Server
	identity   SessionIdentity
	helper     HelperSession
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
	sweepPending      bool
	sweepVerified     bool
	sweepResponse     SweepResponse
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
	instanceID, err := randomCapability()
	if err != nil {
		return nil, errors.New("generate OCI helper instance ID")
	}
	return &Server{
		engine: engine, config: config,
		instanceID:         instanceID,
		connections:        make(chan struct{}, config.ConnectionLimit),
		reapedBootSessions: make(map[SessionIdentity]uint64),
	}, nil
}

func (server *Server) Serve(ctx context.Context, listener net.Listener) error {
	if listener == nil {
		return errors.New("OCI helper listener is required")
	}
	// A restarted helper has no trustworthy in-memory session history. Sweep
	// and verify before accepting even the first session; each acquired session
	// must then repeat the same barrier before OCI operations are admitted.
	if err := server.sweepAndVerifyStartup(ctx); err != nil {
		_ = listener.Close()
		return err
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

func (server *Server) sweepAndVerifyStartup(ctx context.Context) error {
	server.createSweep.Lock()
	defer server.createSweep.Unlock()
	sweepContext, cancel := context.WithTimeout(ctx, server.config.ReapTimeout)
	defer cancel()
	sweepEpoch, err := randomCapability()
	if err != nil {
		return errors.New("startup sweep OCI runtime namespace: generate sweep epoch")
	}
	sweep, err := server.engine.Sweep(sweepContext, SweepRequest{SweepEpoch: sweepEpoch})
	if err != nil {
		return fmt.Errorf("startup sweep OCI runtime namespace: %w", err)
	}
	sweep.SweepEpoch = sweepEpoch
	verification, err := server.engine.Verify(sweepContext, VerifyRequest{Scope: VerifyNamespace})
	if err != nil {
		return fmt.Errorf("startup verify OCI runtime namespace: %w", err)
	}
	if err := validateNamespaceVerification("startup verify OCI runtime namespace", verification); err != nil {
		return err
	}
	if !verification.Absent {
		return namespaceResidueError("startup verify OCI runtime namespace", verification)
	}
	server.sessionMu.Lock()
	server.startupSweep = &sweep
	server.sessionMu.Unlock()
	return nil
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
		_ = writeRPCError(wire, engineFailureRPC(MethodAcquireSession, "session capability generation failed", EngineFailureOperationFailed))
		return
	}
	now := server.config.Clock.Now()
	server.sessionMu.Lock()
	if server.active != nil {
		server.sessionMu.Unlock()
		_ = writeFailure(wire, CodeSessionBusy, "an OCI helper session already owns this helper")
		return
	}
	server.nextSessionGeneration++
	generation := server.nextSessionGeneration
	session := &serverSession{
		server: server, identity: SessionIdentity{NodeID: body.NodeID, BootSessionID: body.BootSessionID},
		helper:  HelperSession{HelperInstanceID: server.instanceID, SessionGeneration: generation},
		peerKey: peer.authorityKey(), capability: capability, control: connection,
		heartbeatDeadline: now.Add(server.config.HeartbeatTimeout), heartbeatChanged: make(chan struct{}, 1),
		done: make(chan struct{}), attempts: make(map[string]*serverAttempt), operations: make(map[*sessionOperation]struct{}),
	}
	server.active = session
	server.sessionMu.Unlock()
	response := AcquireSessionResponse{
		ProtocolVersion: ProtocolVersion, HelperVersion: server.config.HelperVersion,
		HelperChecksum: server.config.HelperChecksum, SessionCapability: capability,
		HelperInstanceID: server.instanceID, SessionGeneration: generation,
		HeartbeatTimeout: server.config.HeartbeatTimeout, MaximumAttemptDeadman: server.config.MaximumAttemptDeadman,
		ReapTimeout: server.config.ReapTimeout,
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
		reapSweep, reapErr := session.server.engine.ReapSession(ctx, session.identity)
		cancel()
		session.server.createSweep.Unlock()
		if reapErr != nil {
			session.server.fail(fmt.Errorf("reap OCI helper session after %s: %w", reason, reapErr))
			return
		}
		session.server.sessionMu.Lock()
		session.server.rememberReapedBootsLocked(append(reapSweep.PriorBootSessionsSeen, session.identity)...)
		session.server.sessionReapSweep = mergeSweepResponsePointer(session.server.sessionReapSweep, reapSweep)
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
	if err := validateEndpointNames(request.AllocateEndpoints); err != nil {
		return nil, &RPCError{Code: CodeInvalidRequest, Message: err.Error()}
	}
	if request.ActivateHostBridgeFallback && !request.EnableHostBridgeFallback {
		return nil, &RPCError{Code: CodeInvalidRequest, Message: "active host bridge fallback was not prepared"}
	}
	computer := request.Workload.Computer
	if computer && request.Authority.Class != contract.JobClassService {
		return nil, &RPCError{Code: CodeInvalidRequest, Message: "Computer mechanics require service attempt authority"}
	}
	if err := validateRunEndpointContract(computer, request.AllocateEndpoints); err != nil {
		return nil, &RPCError{Code: CodeInvalidRequest, Message: err.Error()}
	}
	attempt := &serverAttempt{
		authority: request.Authority, state: attemptStarting,
		computer:        computer,
		deadline:        session.server.config.Clock.Now().Add(request.InitialDeadman),
		deadlineChanged: make(chan struct{}, 1), watchDone: make(chan struct{}), reaped: make(chan struct{}),
		runCancel: runCancel,
	}
	for _, volume := range request.Workload.ManagedVolumes {
		if volume.Kind == ManagedVolumeComputerDisk && volume.ComputerStorage != nil {
			storage := *volume.ComputerStorage
			attempt.computerStorage = &storage
			break
		}
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

// hasLiveRemovalAttemptLocked is called while session.mu excludes Run's
// reserveAttempt edge. Tombstones are already positively reaped and do not
// fence read-only inventory.
func (session *serverSession) hasLiveRemovalAttemptLocked(jobID string, storage *ComputerStorageReference) bool {
	for _, attempt := range session.attempts {
		if attempt.state == attemptTombstoned || attempt.authority.JobID != jobID {
			continue
		}
		if storage == nil || attempt.computerStorage == nil || sameComputerStorageIdentity(*attempt.computerStorage, *storage) {
			return true
		}
	}
	return false
}

const maximumAttemptEndpoints = 8

func validateEndpointNames(names []string) error {
	if len(names) > maximumAttemptEndpoints {
		return fmt.Errorf("at most %d attempt endpoints may be allocated", maximumAttemptEndpoints)
	}
	seen := make(map[string]struct{}, len(names))
	for _, name := range names {
		if len(name) == 0 || len(name) > 32 || name[0] < 'a' || name[0] > 'z' {
			return errors.New("attempt endpoint names must start with a lowercase letter and contain at most 32 lowercase letters, digits, underscores, or hyphens")
		}
		for _, value := range name[1:] {
			if (value < 'a' || value > 'z') && (value < '0' || value > '9') && value != '_' && value != '-' {
				return errors.New("attempt endpoint names must start with a lowercase letter and contain at most 32 lowercase letters, digits, underscores, or hyphens")
			}
		}
		if _, exists := seen[name]; exists {
			return errors.New("attempt endpoint names must be unique")
		}
		seen[name] = struct{}{}
	}
	return nil
}

func validateRunEndpointContract(computer bool, names []string) error {
	seen := make(map[string]struct{}, len(names))
	for _, name := range names {
		seen[name] = struct{}{}
	}
	if computer {
		if len(names) != 2 {
			return errors.New("Computer attempts require exactly the view and control endpoints")
		}
		if _, ok := seen[contract.ComputerDisplayEndpointView]; !ok {
			return errors.New("Computer attempts require exactly the view and control endpoints")
		}
		if _, ok := seen[contract.ComputerDisplayEndpointControl]; !ok {
			return errors.New("Computer attempts require exactly the view and control endpoints")
		}
		return nil
	}
	if len(names) == 0 || (len(names) == 1 && names[0] == "service") {
		return nil
	}
	return errors.New("ordinary attempts may allocate only the service endpoint")
}

func endpointAllocationMatches(requested []string, allocated map[string]uint16) bool {
	if len(requested) != len(allocated) {
		return false
	}
	ports := make(map[uint16]struct{}, len(allocated))
	for _, name := range requested {
		port := allocated[name]
		if port == 0 {
			return false
		}
		if _, duplicate := ports[port]; duplicate {
			return false
		}
		ports[port] = struct{}{}
	}
	return true
}

func (session *serverSession) completeAttempt(request RunRequest, attempt *serverAttempt, response *RunResponse) *RPCError {
	if !response.Started {
		return engineFailureRPC(MethodRun, "engine Run returned without authoritative Started evidence", EngineFailureOperationFailed)
	}
	if response.StartedAt.IsZero() {
		return engineFailureRPC(MethodRun, "engine Run returned without the authoritative Started timestamp", EngineFailureOperationFailed)
	}
	response.StartedAt = response.StartedAt.UTC().Round(0)
	if response.HostBridgeReady && !request.EnableHostBridgeFallback {
		return engineFailureRPC(MethodRun, "engine enabled an unrequested host bridge fallback", EngineFailureOperationFailed)
	}
	if response.HostBridgeReady != (response.HostBridgeEndpoint != "") {
		return engineFailureRPC(MethodRun, "engine host bridge endpoint evidence is inconsistent", EngineFailureOperationFailed)
	}
	if !endpointAllocationMatches(request.AllocateEndpoints, response.Endpoints) {
		return engineFailureRPC(MethodRun, "engine endpoint allocation did not match the request", EngineFailureOperationFailed)
	}
	bridgeCapability := ""
	var err error
	if response.HostBridgeReady {
		bridgeCapability, err = randomCapability()
		if err != nil {
			return engineFailureRPC(MethodRun, "bridge capability generation failed", EngineFailureOperationFailed)
		}
	}
	session.mu.Lock()
	defer session.mu.Unlock()
	if session.closed || session.attempts[request.Authority.key()] != attempt || attempt.state != attemptStarting {
		return &RPCError{Code: CodeUnauthorizedAttempt, Message: "attempt authority expired while starting"}
	}
	attempt.endpoints = maps.Clone(response.Endpoints)
	attempt.cgroupID = request.Resources.CgroupID
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
			if err := session.reapAttempt(attempt, false, true); err != nil {
				go session.invalidate("attempt deadman reap failed")
			}
			return
		}
	}
}

func (session *serverSession) reapAttempt(attempt *serverAttempt, createGateHeld, guardian bool) error {
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
		attempt.guardianReaping = guardian
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
	var err error
	if guardian {
		if reaper, ok := session.server.engine.(GuardianReaper); ok {
			err = reaper.ReapAttemptAsGuardian(ctx, attempt.authority)
		} else {
			err = session.server.engine.ReapAttempt(ctx, attempt.authority)
		}
	} else {
		err = session.server.engine.ReapAttempt(ctx, attempt.authority)
	}
	cancel()
	outcome := "verified_absent"
	if err != nil {
		outcome = "failed"
	}
	if guardian {
		payload, _ := json.Marshal(map[string]string{
			"event": "guardian_deadman_reap", "attempt_id": attempt.authority.AttemptID,
			"fencing_token": attempt.authority.FencingToken, "outcome": outcome,
		})
		_, _ = fmt.Fprintln(os.Stderr, string(payload))
	}
	session.mu.Lock()
	attempt.guardianReaped = guardian && err == nil
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
		return nil, &RPCError{Code: CodeAttemptOutsideSession, Message: "attempt is outside this helper session"}
	}
	attempt := session.attempts[authority.key()]
	if attempt == nil {
		return nil, &RPCError{Code: CodeAttemptOutsideSession, Message: "attempt is outside this helper session"}
	}
	if attempt.state != attemptLive || attempt.authority != authority {
		return nil, &RPCError{Code: CodeUnauthorizedAttempt, Message: "attempt authority does not match a live attempt"}
	}
	return attempt, nil
}

// authorizeDelete accepts a completed guardian reap as exact positive absence
// evidence. Ordinary repeated Delete remains a non-live authority refusal: the
// exception exists only for the race where the helper's own deadman completed
// the same fenced attempt before the agent could request final deletion.
func (session *serverSession) authorizeDelete(ctx context.Context, authority AttemptAuthority) (*serverAttempt, bool, *RPCError) {
	if err := authority.validate(); err != nil {
		return nil, false, &RPCError{Code: CodeInvalidRequest, Message: err.Error()}
	}
	for {
		session.mu.Lock()
		if session.closed || authority.NodeID != session.identity.NodeID || authority.BootSessionID != session.identity.BootSessionID {
			session.mu.Unlock()
			return nil, false, &RPCError{Code: CodeAttemptOutsideSession, Message: "attempt is outside this helper session"}
		}
		attempt := session.attempts[authority.key()]
		if attempt == nil {
			session.mu.Unlock()
			return nil, false, &RPCError{Code: CodeAttemptOutsideSession, Message: "attempt is outside this helper session"}
		}
		if attempt.authority != authority {
			session.mu.Unlock()
			return nil, false, &RPCError{Code: CodeUnauthorizedAttempt, Message: "attempt authority does not match a live attempt"}
		}
		if attempt.state == attemptReaping && attempt.guardianReaping {
			reaped := attempt.reaped
			session.mu.Unlock()
			select {
			case <-reaped:
				continue
			case <-ctx.Done():
				return nil, false, engineFailureRPC(MethodDelete, "guardian attempt reap was not confirmed", engineFailureReason(ctx.Err()))
			}
		}
		if attempt.state == attemptTombstoned && attempt.guardianReaped && !attempt.guardianDelete {
			attempt.guardianDelete = true
			session.mu.Unlock()
			return attempt, true, nil
		}
		if attempt.state != attemptLive {
			session.mu.Unlock()
			return nil, false, &RPCError{Code: CodeUnauthorizedAttempt, Message: "attempt authority does not match a live attempt"}
		}
		session.mu.Unlock()
		return attempt, false, nil
	}
}

func (session *serverSession) consumeGuardianDelete(attempt *serverAttempt) {
	session.mu.Lock()
	attempt.guardianReaped = false
	session.mu.Unlock()
}

func (session *serverSession) sweepRequired(method Method) bool {
	switch method {
	case MethodAcquireSession, MethodSweep, MethodVerify:
		return false
	case MethodEnsureImage, MethodReconcileImagePins, MethodReleaseImagePin, MethodReleaseAttemptPin, MethodImageCacheStatus,
		MethodRun, MethodSignal, MethodWatch, MethodDelete, MethodDeleteVolume, MethodInventoryRemoval, MethodAttestRemoval,
		MethodDialAttemptPort, MethodDialHostBridge, MethodSetComputerControl, MethodSetComputerToken:
		session.mu.Lock()
		defer session.mu.Unlock()
		return !session.sweepVerified
	default:
		return false
	}
}

func (server *Server) dispatch(operation *sessionOperation, wire *framedConn, request frame) {
	session := operation.session
	if session.sweepRequired(request.Method) {
		_ = writeFailure(wire, CodeSweepRequired, "boot sweep and namespace verification must succeed before OCI operations")
		return
	}
	switch request.Method {
	case MethodEnsureImage:
		var body EnsureImageRequest
		if !decodeRequest(wire, request.Body, &body) {
			return
		}
		if err := validateEnsureImageRequest(body); err != nil {
			_ = writeFailure(wire, CodeInvalidRequest, err.Error())
			return
		}
		if body.Pin != nil && (body.Pin.Authority.NodeID != session.identity.NodeID || body.Pin.Authority.BootSessionID != session.identity.BootSessionID) {
			_ = writeFailure(wire, CodeUnauthorizedAttempt, "image pin authority is outside this helper session")
			return
		}
		var archive io.Reader
		if body.Source == ImageSourceArchive {
			timeout := body.OperationTimeout
			if timeout <= 0 {
				timeout = 10 * time.Minute
			}
			if err := operation.conn.SetReadDeadline(time.Now().Add(timeout)); err != nil {
				_ = writeRPCError(wire, engineFailureRPC(MethodEnsureImage, "bound OCI archive upload deadline", EngineFailureOperationFailed))
				return
			}
			if err := writeSuccess(wire, struct{}{}); err != nil {
				return
			}
			archive = operation.conn
		} else {
			operation.monitorEOF()
		}
		err := server.engine.EnsureImage(operation.ctx, body, archive, func(event EnsureImageEvent) error {
			if err := validateEnsureImageEvent(event); err != nil {
				return err
			}
			return writeSuccess(wire, event)
		})
		writeImageStreamResult(wire, err)
	case MethodReconcileImagePins:
		var body ReconcileImagePinsRequest
		if !decodeRequest(wire, request.Body, &body) {
			return
		}
		if body.CacheMaxBytes <= 0 {
			_ = writeFailure(wire, CodeInvalidRequest, "image cache maximum bytes must be positive")
			return
		}
		cacheEngine, ok := server.engine.(ImageCacheEngine)
		if !ok {
			_ = writeFailure(wire, CodeUnsupportedOperation, "image cache policy is unavailable")
			return
		}
		response, err := cacheEngine.ReconcileImagePins(operation.ctx, body)
		_ = writeEngineResponseWithMethod(wire, request.Method, response, err)
	case MethodReleaseImagePin:
		var body ReleaseImagePinRequest
		if !decodeRequest(wire, request.Body, &body) {
			return
		}
		if strings.TrimSpace(body.JobID) == "" {
			_ = writeFailure(wire, CodeInvalidRequest, "binding image pin job ID is required")
			return
		}
		cacheEngine, ok := server.engine.(ImageCacheEngine)
		if !ok {
			_ = writeFailure(wire, CodeUnsupportedOperation, "image cache policy is unavailable")
			return
		}
		err := cacheEngine.ReleaseImagePin(operation.ctx, body)
		_ = writeEngineResponseWithMethod(wire, request.Method, struct{}{}, err)
	case MethodReleaseAttemptPin:
		var body ReleaseAttemptImagePinRequest
		if !decodeRequest(wire, request.Body, &body) {
			return
		}
		if err := body.Authority.validate(); err != nil || body.Authority.NodeID != session.identity.NodeID || body.Authority.BootSessionID != session.identity.BootSessionID {
			_ = writeFailure(wire, CodeUnauthorizedAttempt, "attempt image pin authority is outside this helper session")
			return
		}
		cacheEngine, ok := server.engine.(ImageCacheEngine)
		if !ok {
			_ = writeFailure(wire, CodeUnsupportedOperation, "image cache policy is unavailable")
			return
		}
		err := cacheEngine.ReleaseAttemptImagePin(operation.ctx, body)
		_ = writeEngineResponseWithMethod(wire, request.Method, struct{}{}, err)
	case MethodImageCacheStatus:
		var body struct{}
		if !decodeRequest(wire, request.Body, &body) {
			return
		}
		cacheEngine, ok := server.engine.(ImageCacheEngine)
		if !ok {
			_ = writeFailure(wire, CodeUnsupportedOperation, "image cache policy is unavailable")
			return
		}
		response, err := cacheEngine.ImageCacheStatus(operation.ctx)
		_ = writeEngineResponseWithMethod(wire, request.Method, response, err)
	case MethodDoctorStatus:
		var body struct{}
		if !decodeRequest(wire, request.Body, &body) {
			return
		}
		doctorEngine, ok := server.engine.(DoctorEngine)
		if !ok {
			_ = writeFailure(wire, CodeUnsupportedOperation, "OCI doctor facts are unavailable")
			return
		}
		response, err := doctorEngine.DoctorStatus(operation.ctx)
		_ = writeDiagnosticResponse(wire, response, err)
	case MethodRun:
		var body RunRequest
		if !decodeRequest(wire, request.Body, &body) {
			return
		}
		if err := body.Authority.validate(); err != nil || body.Authority.NodeID != session.identity.NodeID || body.Authority.BootSessionID != session.identity.BootSessionID {
			_ = writeFailure(wire, CodeAttemptOutsideSession, "Run authority is outside this helper session")
			return
		}
		resources, err := DeterministicResourceIdentity(body.Authority)
		if err != nil {
			_ = writeFailure(wire, CodeInvalidRequest, err.Error())
			return
		}
		for _, volume := range body.Workload.ManagedVolumes {
			if volume.Kind != ManagedVolumeHandoff {
				continue
			}
			resources.HandoffVolumeDirectory, err = DeterministicHandoffVolumeDirectory(volume.OwnerKey)
			if err != nil {
				_ = writeFailure(wire, CodeInvalidRequest, err.Error())
				return
			}
		}
		body.Resources = resources
		if server.config.beforeRunCreateLock != nil {
			server.config.beforeRunCreateLock()
		}
		server.createSweep.RLock()
		if session.sweepRequired(MethodRun) {
			server.createSweep.RUnlock()
			_ = writeFailure(wire, CodeSweepRequired, "boot sweep and namespace verification must succeed before OCI operations")
			return
		}
		runContext, runCancel := context.WithCancel(operation.ctx)
		attempt, rpcErr := session.reserveAttempt(body, runCancel)
		if rpcErr != nil {
			runCancel()
			server.createSweep.RUnlock()
			_ = writeRPCError(wire, rpcErr)
			return
		}
		operation.monitorEOF()
		response, err := server.engine.Run(runContext, body)
		runCancel()
		if err == nil {
			if rpcErr := session.completeAttempt(body, attempt, &response); rpcErr != nil {
				err = rpcErr
			}
		}
		if err != nil {
			reapErr := session.reapAttempt(attempt, true, false)
			server.createSweep.RUnlock()
			if reapErr != nil {
				go session.invalidate("ambiguous Run reap failed")
			}
			var rpcErr *RPCError
			var specRejection *RuntimeSpecRejectionError
			var serviceDataRejection *ServiceDataRejectionError
			var insufficientMemory *insufficientMemoryError
			var insufficientDisk *insufficientDiskError
			var imageUnavailable *ImageUnavailableError
			var storageRetired *computerStorageRetiredError
			if errors.As(err, &rpcErr) {
				_ = writeRPCError(wire, rpcErr)
			} else if errors.Is(err, errComputerStorageAttachmentOwned) {
				if reapErr == nil {
					// A live owner refusing a second Computer attachment is an
					// attempt-scoped admission result only after the helper has
					// positively reaped the losing attempt's runtime resources.
					_ = writeFailure(wire, CodeComputerStorageBusy, errComputerStorageAttachmentOwned.Error())
				} else {
					_ = writeFailure(wire, CodeSessionStale, "Computer Storage conflict cleanup could not verify runtime absence")
				}
			} else if errors.As(err, &storageRetired) {
				if reapErr == nil {
					_ = writeFailure(wire, CodeComputerStorageRetired, storageRetired.Error())
				} else {
					_ = writeFailure(wire, CodeSessionStale, "retired Computer Storage refusal could not verify runtime absence")
				}
			} else if errors.As(err, &specRejection) {
				_ = writeFailure(wire, CodeOCISpecRejected, "OCI runtime spec was rejected")
			} else if errors.As(err, &serviceDataRejection) {
				_ = writeFailure(wire, CodeOCISpecRejected, serviceDataRejection.Error())
			} else if errors.As(err, &insufficientMemory) {
				_ = writeRPCError(wire, &RPCError{Code: CodeInsufficientMemory, Message: insufficientMemory.Error(), MemoryFailure: &MemoryFailureFact{RequestedBytes: insufficientMemory.RequestedBytes, ObservedAvailableBytes: insufficientMemory.ObservedAvailableBytes}})
			} else if errors.As(err, &insufficientDisk) {
				_ = writeRPCError(wire, &RPCError{Code: CodeInsufficientDisk, Message: insufficientDisk.Error(), DiskFailure: &DiskFailureFact{RequestedBytes: insufficientDisk.RequestedBytes, ObservedAvailableBytes: insufficientDisk.ObservedAvailableBytes}})
			} else if errors.As(err, &imageUnavailable) {
				_ = writeFailure(wire, CodeImageUnavailable, "pinned local OCI image is unavailable")
			} else {
				_ = writeRPCError(wire, engineFailureRPC(MethodRun, "OCI engine operation failed", engineFailureReason(err), err))
			}
			return
		}
		writeErr := writeSuccess(wire, response)
		if writeErr != nil {
			reapErr := session.reapAttempt(attempt, true, false)
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
		signalErr := server.engine.Signal(operation.ctx, body)
		response := SignalResponse{}
		if errors.Is(signalErr, ErrTaskAlreadyTerminated) {
			// The task crossed its terminal edge before this signal reached
			// containerd. Watch remains the authority for how it terminated;
			// this race is neither a failed mechanics operation nor runtime loss.
			response.AlreadyTerminated = true
			signalErr = nil
		}
		_ = writeEngineResponseWithMethod(wire, request.Method, response, signalErr)
	case MethodSetComputerControl:
		var body SetComputerControlStateRequest
		if !decodeRequest(wire, request.Body, &body) {
			return
		}
		attempt, rpcErr := session.authorizeAttempt(body.Authority)
		if rpcErr != nil {
			_ = writeRPCError(wire, rpcErr)
			return
		}
		if !attempt.computer {
			_ = writeFailure(wire, CodeUnauthorizedAttempt, "control state is not authorized for this live attempt")
			return
		}
		engine, ok := server.engine.(ComputerControlEngine)
		if !ok {
			_ = writeFailure(wire, CodeUnsupportedOperation, "Computer control state is unavailable")
			return
		}
		operation.monitorEOF()
		_ = writeEngineResponseWithMethod(wire, request.Method, struct{}{}, engine.SetComputerControlState(operation.ctx, body))
	case MethodSetComputerToken:
		var body SetComputerTokenRequest
		if !decodeRequest(wire, request.Body, &body) {
			return
		}
		attempt, rpcErr := session.authorizeAttempt(body.Authority)
		if rpcErr != nil {
			_ = writeRPCError(wire, rpcErr)
			return
		}
		if !attempt.computer {
			_ = writeFailure(wire, CodeUnauthorizedAttempt, "Computer token delivery is not authorized for this live attempt")
			return
		}
		if (body.Token == "") != (body.L3Endpoint == "") {
			_ = writeFailure(wire, CodeInvalidRequest, "Computer token and L3 endpoint must be published or removed together")
			return
		}
		engine, ok := server.engine.(ComputerControlEngine)
		if !ok {
			_ = writeFailure(wire, CodeUnsupportedOperation, "Computer token delivery is unavailable")
			return
		}
		operation.monitorEOF()
		_ = writeEngineResponseWithMethod(wire, request.Method, struct{}{}, engine.SetComputerToken(operation.ctx, body))
	case MethodWatch:
		var body WatchRequest
		if !decodeRequest(wire, request.Body, &body) || !authorizeRequest(wire, session, body.Authority) {
			return
		}
		acknowledged := operation.monitorAcknowledgements()
		err := server.engine.Watch(operation.ctx, body, func(event WatchEvent) error {
			if err := validateWatchEvent(event); err != nil {
				return err
			}
			if err := writeSuccess(wire, event); err != nil {
				return err
			}
			select {
			case err := <-acknowledged:
				return err
			case <-operation.ctx.Done():
				return operation.ctx.Err()
			}
		})
		writeStreamResult(wire, request.Method, err)
	case MethodDelete:
		var body DeleteRequest
		if !decodeRequest(wire, request.Body, &body) {
			return
		}
		attempt, alreadyReaped, rpcErr := session.authorizeDelete(operation.ctx, body.Authority)
		if rpcErr != nil {
			_ = writeRPCError(wire, rpcErr)
			return
		}
		operation.monitorEOF()
		response, err := server.engine.Delete(operation.ctx, body)
		if alreadyReaped {
			session.consumeGuardianDelete(attempt)
		} else if err == nil && response.Deleted {
			err = session.reapAttempt(attempt, false, false)
		}
		_ = writeEngineResponseWithMethod(wire, request.Method, response, err)
	case MethodDeleteVolume:
		var body DeleteManagedVolumeRequest
		if !decodeRequest(wire, request.Body, &body) {
			return
		}
		var identityErr error
		switch body.Kind {
		case ManagedVolumeHandoff:
			_, identityErr = DeterministicHandoffVolumeDirectory(body.OwnerKey)
		case ManagedVolumeServiceData:
			_, identityErr = DeterministicServiceVolumeDirectory(body.OwnerKey)
			if identityErr == nil && (body.Removal == nil || body.Removal.JobID != body.OwnerKey || !validCurrentRemovalAuthority(*body.Removal, session.identity)) {
				identityErr = errors.New("complete current-session service-data removal authority is required")
			}
		case ManagedVolumeComputerDisk:
			if body.ComputerStorage == nil || body.Removal == nil || body.Removal.PriorJobID == "" || !validCurrentRemovalAuthority(*body.Removal, session.identity) {
				identityErr = errors.New("complete current-session Computer removal authority is required")
			} else if body.QuarantineOnFailure && body.FailureAttempts < 1 {
				identityErr = errors.New("Computer cleanup quarantine requires a positive bounded attempt count")
			}
		default:
			identityErr = fmt.Errorf("managed volume kind %q cannot be deleted", body.Kind)
		}
		if body.Kind != ManagedVolumeComputerDisk && (body.QuarantineOnFailure || body.FailureAttempts != 0) {
			identityErr = errors.New("cleanup quarantine evidence is closed to Computer disks")
		}
		if identityErr != nil {
			_ = writeFailure(wire, CodeInvalidRequest, identityErr.Error())
			return
		}
		engine, ok := server.engine.(ManagedVolumeEngine)
		if !ok {
			_ = writeFailure(wire, CodeUnsupportedOperation, "managed-volume deletion is unavailable")
			return
		}
		operation.monitorEOF()
		response, err := engine.DeleteManagedVolume(operation.ctx, body)
		_ = writeEngineResponseWithMethod(wire, request.Method, response, err)
	case MethodInventoryRemoval:
		var body InventoryRemovalRequest
		if !decodeRequest(wire, request.Body, &body) {
			return
		}
		if !validCurrentRemovalAuthority(body.Removal, session.identity) {
			_ = writeFailure(wire, CodeInvalidRequest, "complete current-session removal inventory authority is required")
			return
		}
		engine, ok := server.engine.(RemovalInventoryEngine)
		if !ok {
			_ = writeFailure(wire, CodeUnsupportedOperation, "removal inventory is unavailable")
			return
		}
		operation.monitorEOF()
		// Snapshot the helper-owned attempt registry before inventory, then check
		// it again after the engine scan. Run reserves under the same mutex before
		// it can create runtime or attach Storage resources, so either edge fences
		// Storage-only evidence without holding the session-wide heartbeat mutex
		// across filesystem and containerd I/O.
		session.mu.Lock()
		busy := session.hasLiveRemovalAttemptLocked(body.Removal.JobID, body.ComputerStorage)
		session.mu.Unlock()
		if busy {
			_ = writeFailure(wire, CodeComputerStorageBusy, "removal inventory is fenced by a live attempt authority")
			return
		}
		response, err := engine.InventoryRemoval(operation.ctx, body)
		if err == nil {
			session.mu.Lock()
			busy = session.hasLiveRemovalAttemptLocked(body.Removal.JobID, body.ComputerStorage)
			closed := session.closed
			helper := session.helper
			session.mu.Unlock()
			if closed {
				_ = writeFailure(wire, CodeSessionStale, "OCI helper session ended during removal inventory")
				return
			}
			if busy {
				_ = writeFailure(wire, CodeComputerStorageBusy, "removal inventory raced a live attempt authority")
				return
			}
			response.JobID = body.Removal.JobID
			response.RemovalGeneration = body.Removal.RemovalGeneration
			response.HelperSession = helper
		}
		_ = writeEngineResponseWithMethod(wire, request.Method, response, err)
	case MethodAttestRemoval:
		var body AttestRemovalRequest
		if !decodeRequest(wire, request.Body, &body) {
			return
		}
		if err := validateAttestRemovalRequest(body, session.identity.NodeID); err != nil {
			_ = writeFailure(wire, CodeInvalidRequest, err.Error())
			return
		}
		engine, ok := server.engine.(RemovalProofEngine)
		if !ok {
			_ = writeFailure(wire, CodeUnsupportedOperation, "removal attestation is unavailable")
			return
		}
		operation.monitorEOF()
		response, err := engine.AttestRemoval(operation.ctx, body)
		if err == nil {
			response.JobID = body.JobID
			response.RemovalGeneration = body.RemovalGeneration
			response.HelperSession = session.helper
		}
		_ = writeEngineResponseWithMethod(wire, request.Method, response, err)
	case MethodResetStorage:
		var body ResetComputerStorageRequest
		if !decodeRequest(wire, request.Body, &body) {
			return
		}
		if body.Authority.NodeID != session.identity.NodeID || body.Authority.BootSessionID != session.identity.BootSessionID || body.Authority.NodeID == "" ||
			body.Authority.HelperGeneration != session.helper.SessionGeneration || body.Authority.HelperGeneration == 0 ||
			body.Authority.RootInstanceID == "" || body.Authority.JobID == "" || body.Authority.PriorJobID == "" || body.Authority.IntentRevision < 1 || body.Authority.CleanupFence == "" ||
			body.Storage.ComputerID == "" || body.Storage.StorageID == "" || body.Storage.StorageGeneration < 1 ||
			body.Storage.IntentRevision != body.Authority.IntentRevision || body.NewGeneration != body.Storage.StorageGeneration+1 {
			_ = writeFailure(wire, CodeInvalidRequest, "complete current-session Computer Storage reset authority is required")
			return
		}
		engine, ok := server.engine.(ComputerStorageResetEngine)
		if !ok {
			_ = writeFailure(wire, CodeUnsupportedOperation, "Computer Storage reset is unavailable")
			return
		}
		operation.monitorEOF()
		response, err := engine.ResetComputerStorage(operation.ctx, body)
		_ = writeEngineResponseWithMethod(wire, request.Method, response, err)
	case MethodGrowStorage:
		var body GrowComputerStorageRequest
		if !decodeRequest(wire, request.Body, &body) {
			return
		}
		if body.Authority.NodeID != session.identity.NodeID || body.Authority.BootSessionID != session.identity.BootSessionID ||
			body.Authority.NodeID == "" || body.Authority.HelperGeneration != session.helper.SessionGeneration ||
			body.Authority.HelperGeneration == 0 || body.Authority.RootInstanceID == "" || body.Authority.JobID == "" ||
			body.Authority.OperationRevision < 1 || body.Authority.OperationFence == "" || body.Storage.ComputerID == "" ||
			body.Storage.StorageID == "" || body.Storage.StorageGeneration < 1 || body.Storage.DiskBytes <= 0 ||
			body.Storage.IntentRevision != body.Authority.OperationRevision || body.NewDiskBytes <= body.Storage.DiskBytes {
			_ = writeFailure(wire, CodeInvalidRequest, "complete current-session grow-only Computer Storage authority is required")
			return
		}
		engine, ok := server.engine.(ComputerStorageGrowEngine)
		if !ok {
			_ = writeFailure(wire, CodeUnsupportedOperation, "Computer Storage grow is unavailable")
			return
		}
		operation.monitorEOF()
		response, err := engine.GrowComputerStorage(operation.ctx, body)
		var uncertain *ComputerStorageGrowUncertainError
		if errors.As(err, &uncertain) {
			_ = writeFailure(wire, CodeComputerStorageGrowUncertain, uncertain.Error())
		} else {
			_ = writeEngineResponseWithMethod(wire, request.Method, response, err)
		}
	case MethodPreflightReimage:
		var body PreflightComputerReimageRequest
		if !decodeRequest(wire, request.Body, &body) {
			return
		}
		if body.Authority.NodeID != session.identity.NodeID || body.Authority.BootSessionID != session.identity.BootSessionID ||
			body.Authority.NodeID == "" || body.Authority.HelperGeneration != session.helper.SessionGeneration ||
			body.Authority.HelperGeneration == 0 || body.Authority.RootInstanceID == "" || body.Authority.OldJobID == "" ||
			body.Authority.StagingJobID == "" || body.Authority.OperationRevision < 1 || body.Authority.OperationFence == "" ||
			body.Storage.ComputerID == "" || body.Storage.StorageID == "" || body.Storage.StorageGeneration < 1 ||
			body.Storage.IntentRevision != body.Authority.OperationRevision || body.TargetImage.Reference == "" ||
			body.TargetImage.Digest == "" || body.TargetImage.Platform.OS == "" || body.TargetImage.Platform.Architecture == "" {
			_ = writeFailure(wire, CodeInvalidRequest, "complete current-session Computer reimage preflight authority is required")
			return
		}
		engine, ok := server.engine.(ComputerReimagePreflightEngine)
		if !ok {
			_ = writeFailure(wire, CodeUnsupportedOperation, "Computer reimage preflight is unavailable")
			return
		}
		operation.monitorEOF()
		response, err := engine.PreflightComputerReimage(operation.ctx, body)
		_ = writeEngineResponseWithMethod(wire, request.Method, response, err)
	case MethodCreateBackup:
		var body CreateComputerBackupRequest
		if !decodeRequest(wire, request.Body, &body) {
			return
		}
		if !validComputerBackupRequest(body.BackupID, body.CopyID, body.Storage, body.Authority, session, true) {
			_ = writeFailure(wire, CodeInvalidRequest, "complete current-session Computer Backup authority is required")
			return
		}
		engine, ok := server.engine.(ComputerBackupEngine)
		if !ok {
			_ = writeFailure(wire, CodeUnsupportedOperation, "Computer Backup is unavailable")
			return
		}
		operation.monitorEOF()
		response, err := engine.CreateComputerBackup(operation.ctx, body)
		_ = writeEngineResponseWithMethod(wire, request.Method, response, err)
	case MethodDeleteBackup:
		var body DeleteComputerBackupCopyRequest
		if !decodeRequest(wire, request.Body, &body) {
			return
		}
		if !validComputerBackupRequest(body.BackupID, body.CopyID, body.Storage, body.Authority, session, false) {
			_ = writeFailure(wire, CodeInvalidRequest, "complete current-session Backup copy removal authority is required")
			return
		}
		engine, ok := server.engine.(ComputerBackupEngine)
		if !ok {
			_ = writeFailure(wire, CodeUnsupportedOperation, "Computer Backup removal is unavailable")
			return
		}
		operation.monitorEOF()
		response, err := engine.DeleteComputerBackupCopy(operation.ctx, body)
		_ = writeEngineResponseWithMethod(wire, request.Method, response, err)
	case MethodCopyStorage:
		var body CopyComputerStorageRequest
		if !decodeRequest(wire, request.Body, &body) {
			return
		}
		managedSource := body.Operation == "restore" || body.Operation == "clone"
		importSource := body.Operation == "import"
		if (!managedSource && !importSource) || body.BackupID == "" || body.CopyID == "" ||
			(importSource && (body.ExportID == "" || body.ExternalPath == "" || body.ManifestDigest == "")) ||
			body.SourceComputerID == "" || body.SourceStorageID == "" || body.SourceGeneration < 1 ||
			body.SourceSize < 1 || body.SourceDigest == "" || body.Destination.ComputerID == "" ||
			body.Destination.StorageID == "" || body.Destination.StorageGeneration < 1 || body.Destination.DiskBytes < body.SourceSize ||
			body.Destination.IntentRevision != body.Authority.OperationRevision || body.Authority.NodeID != session.identity.NodeID ||
			body.Authority.BootSessionID != session.identity.BootSessionID || body.Authority.HelperGeneration != session.helper.SessionGeneration ||
			body.Authority.HelperGeneration == 0 || body.Authority.RootInstanceID == "" || body.Authority.JobID == "" ||
			body.Authority.OperationRevision < 1 || body.Authority.CleanupFence == "" {
			_ = writeFailure(wire, CodeInvalidRequest, "complete current-session Computer Storage copy authority is required")
			return
		}
		engine, ok := server.engine.(ComputerStorageCopyEngine)
		if !ok {
			_ = writeFailure(wire, CodeUnsupportedOperation, "Computer Storage copy is unavailable")
			return
		}
		operation.monitorEOF()
		response, err := engine.CopyComputerStorage(operation.ctx, body)
		_ = writeEngineResponseWithMethod(wire, request.Method, response, err)
	case MethodExportCustody:
		var body ExportComputerCustodyRequest
		if !decodeRequest(wire, request.Body, &body) {
			return
		}
		if body.ExportID == "" || body.BackupID == "" || body.CopyID == "" ||
			body.Storage.ComputerID == "" || body.Storage.StorageID == "" || body.Storage.StorageGeneration < 1 ||
			body.SourceSize < 1 || body.SourceDigest == "" || body.ExternalPath == "" ||
			body.Authority.NodeID != session.identity.NodeID || body.Authority.BootSessionID != session.identity.BootSessionID ||
			body.Authority.HelperGeneration != session.helper.SessionGeneration || body.Authority.HelperGeneration == 0 ||
			body.Authority.RootInstanceID == "" || body.Authority.OperationRevision < 1 || body.Authority.CustodyFence == "" {
			_ = writeFailure(wire, CodeInvalidRequest, "complete current-session Custody export authority is required")
			return
		}
		engine, ok := server.engine.(ComputerCustodyExportEngine)
		if !ok {
			_ = writeFailure(wire, CodeUnsupportedOperation, "Custody export is unavailable")
			return
		}
		operation.monitorEOF()
		response, err := engine.ExportComputerCustody(operation.ctx, body)
		_ = writeEngineResponseWithMethod(wire, request.Method, response, err)
	case MethodVerify:
		var body VerifyRequest
		if !decodeRequest(wire, request.Body, &body) {
			return
		}
		if body.Scope == VerifyAttempt {
			if body.Authority == nil || !authorizeRequest(wire, session, *body.Authority) {
				return
			}
		} else if (body.Scope != VerifyNamespace && body.Scope != VerifyNamespaceReadOnly) || body.Authority != nil {
			_ = writeFailure(wire, CodeInvalidRequest, "Verify requires exactly one valid scope")
			return
		}
		operation.monitorEOF()
		if body.Scope == VerifyNamespace {
			server.createSweep.Lock()
		}
		response, err := server.engine.Verify(operation.ctx, body)
		if err == nil && body.Scope == VerifyNamespace && response.Absent {
			session.mu.Lock()
			if session.sweepPending {
				session.sweepPending = false
				session.sweepVerified = true
				session.sweepResponse = SweepResponse{}
			}
			session.mu.Unlock()
		}
		if body.Scope == VerifyNamespace {
			server.createSweep.Unlock()
		}
		_ = writeEngineResponseWithMethod(wire, request.Method, response, err)
	case MethodSweep:
		var body SweepRequest
		if !decodeRequest(wire, request.Body, &body) {
			return
		}
		operation.monitorEOF()
		server.createSweep.Lock()
		session.mu.Lock()
		carriedSweep := session.sweepResponse
		session.sweepPending = false
		session.sweepVerified = false
		session.sweepResponse = SweepResponse{}
		session.mu.Unlock()
		sweepEpoch, err := randomCapability()
		body.SweepEpoch = sweepEpoch
		var response SweepResponse
		if err == nil {
			response, err = server.engine.Sweep(operation.ctx, body)
			response.SweepEpoch = body.SweepEpoch
		}
		if err == nil {
			if carriedSweep.SweepEpoch != "" {
				response.Removed += carriedSweep.Removed
				response.PriorBootSessionsSeen = append(response.PriorBootSessionsSeen, carriedSweep.PriorBootSessionsSeen...)
				response.Inventory = mergeResourceInventory(response.Inventory, carriedSweep.Inventory)
				response.Attempts = mergeSweepAttempts(response.Attempts, carriedSweep.Attempts)
			}
			server.sessionMu.Lock()
			startup := server.startupSweep
			server.startupSweep = nil
			sessionReap := server.sessionReapSweep
			server.sessionReapSweep = nil
			if startup != nil {
				server.rememberReapedBootsLocked(startup.PriorBootSessionsSeen...)
			}
			for identity := range server.reapedBootSessions {
				response.PriorBootSessionsSeen = append(response.PriorBootSessionsSeen, identity)
			}
			server.sessionMu.Unlock()
			if startup != nil {
				response.Removed += startup.Removed
				response.PriorBootSessionsSeen = append(response.PriorBootSessionsSeen, startup.PriorBootSessionsSeen...)
				response.Inventory = mergeResourceInventory(response.Inventory, startup.Inventory)
				response.Attempts = mergeSweepAttempts(response.Attempts, startup.Attempts)
			}
			if sessionReap != nil {
				response.Removed += sessionReap.Removed
				response.PriorBootSessionsSeen = append(response.PriorBootSessionsSeen, sessionReap.PriorBootSessionsSeen...)
				response.Inventory = mergeResourceInventory(response.Inventory, sessionReap.Inventory)
				response.Attempts = mergeSweepAttempts(response.Attempts, sessionReap.Attempts)
			}
			slices.SortFunc(response.PriorBootSessionsSeen, compareSessionIdentity)
			response.PriorBootSessionsSeen = slices.Compact(response.PriorBootSessionsSeen)
			session.mu.Lock()
			session.sweepPending = true
			session.sweepResponse = response
			session.mu.Unlock()
		}
		server.createSweep.Unlock()
		_ = writeEngineResponseWithMethod(wire, request.Method, response, err)
	case MethodDialAttemptPort:
		var body DialAttemptPortRequest
		if !decodeRequest(wire, request.Body, &body) {
			return
		}
		attempt, rpcErr := session.authorizeAttempt(body.Authority)
		if rpcErr != nil {
			_ = writeRPCError(wire, rpcErr)
			return
		}
		port := attempt.endpoints[body.Name]
		if body.Name == "" || port == 0 {
			_ = writeFailure(wire, CodeUnauthorizedPort, "endpoint is not allocated to this live attempt")
			return
		}
		body.Port = port
		body.CgroupID = attempt.cgroupID
		stream := &attemptPortOperationStream{
			operationStream: &operationStream{Conn: operation.conn, cancel: operation.cancel},
			wire:            wire,
			acknowledged:    operation.monitorAcknowledgement(),
			ctx:             operation.ctx,
			writeSetup:      func() error { return writeSuccess(wire, struct{}{}) },
		}
		err := server.engine.DialAttemptPort(operation.ctx, body, stream)
		if err != nil && !stream.ready {
			_ = writeEngineResponseWithMethod(wire, request.Method, struct{}{}, err)
		}
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

// attemptPortOperationStream withholds the RPC success frame until the engine
// has connected the attempt backend and emits its ready marker. A refused
// payload listener therefore remains a typed, attempt-scoped engine error
// instead of looking like helper-stream EOF and invalidating the whole session.
type attemptPortOperationStream struct {
	*operationStream
	wire         *framedConn
	acknowledged <-chan error
	ctx          context.Context
	setupOnce    sync.Once
	setupErr     error
	writeSetup   func() error
	ready        bool
}

func (stream *attemptPortOperationStream) Write(payload []byte) (int, error) {
	if !stream.ready {
		if len(payload) == 0 || payload[0] != attemptPortBackendReady {
			return 0, errors.New("attempt-port stream omitted the backend-ready marker")
		}
		stream.setupOnce.Do(func() {
			if err := stream.writeSetup(); err != nil {
				stream.setupErr = err
				return
			}
			select {
			case err := <-stream.acknowledged:
				if err != nil {
					stream.setupErr = fmt.Errorf("attempt-port client did not acknowledge stream setup: %w", err)
				}
			case <-stream.ctx.Done():
				stream.setupErr = fmt.Errorf("attempt-port client acknowledgement: %w", context.Cause(stream.ctx))
			}
		})
		if stream.setupErr != nil {
			return 0, stream.setupErr
		}
		stream.ready = true
	}
	return stream.Conn.Write(payload)
}

func validComputerBackupRequest(backupID, copyID string, storage ComputerStorageReference, authority ComputerBackupAuthority, session *serverSession, requireJob bool) bool {
	return strings.TrimSpace(backupID) != "" && strings.TrimSpace(copyID) != "" &&
		storage.ComputerID != "" && storage.StorageID != "" && storage.StorageGeneration > 0 &&
		storage.DiskBytes > 0 && storage.IntentRevision == authority.OperationRevision &&
		authority.NodeID == session.identity.NodeID && authority.BootSessionID == session.identity.BootSessionID &&
		authority.HelperGeneration == session.helper.SessionGeneration && authority.HelperGeneration > 0 &&
		authority.RootInstanceID != "" && (!requireJob || authority.JobID != "") && (!requireJob || authority.PriorJobID != "") && authority.OperationRevision > 0 &&
		authority.CleanupFence != ""
}

func validCurrentRemovalAuthority(removal ManagedVolumeRemovalAuthority, identity SessionIdentity) bool {
	return removal.NodeID == identity.NodeID && removal.BootSessionID == identity.BootSessionID &&
		strings.TrimSpace(removal.JobID) != "" && removal.RemovalGeneration > 0 && strings.TrimSpace(removal.CleanupFence) != ""
}

func compareSessionIdentity(left, right SessionIdentity) int {
	if order := strings.Compare(left.NodeID, right.NodeID); order != 0 {
		return order
	}
	return strings.Compare(left.BootSessionID, right.BootSessionID)
}

func (server *Server) rememberReapedBootsLocked(identities ...SessionIdentity) {
	for _, identity := range identities {
		if identity.NodeID == "" || identity.BootSessionID == "" {
			continue
		}
		server.reapedBootSequence++
		server.reapedBootSessions[identity] = server.reapedBootSequence
	}
	for len(server.reapedBootSessions) > maximumReapedBoots {
		var oldest SessionIdentity
		var oldestSequence uint64
		for identity, sequence := range server.reapedBootSessions {
			if oldestSequence == 0 || sequence < oldestSequence {
				oldest, oldestSequence = identity, sequence
			}
		}
		delete(server.reapedBootSessions, oldest)
	}
}

func mergeSweepResponsePointer(target *SweepResponse, addition SweepResponse) *SweepResponse {
	if target == nil {
		copy := addition
		return &copy
	}
	target.Removed += addition.Removed
	target.PriorBootSessionsSeen = append(target.PriorBootSessionsSeen, addition.PriorBootSessionsSeen...)
	target.Inventory = mergeResourceInventory(target.Inventory, addition.Inventory)
	target.Attempts = mergeSweepAttempts(target.Attempts, addition.Attempts)
	return target
}

func mergeSweepAttempts(left, right []SweptAttemptAuthority) []SweptAttemptAuthority {
	merged := append(slices.Clone(left), right...)
	slices.SortFunc(merged, func(left, right SweptAttemptAuthority) int {
		for _, values := range [][2]string{
			{left.NodeID, right.NodeID}, {left.JobID, right.JobID}, {left.RemovalGeneration, right.RemovalGeneration},
			{left.AttemptID, right.AttemptID}, {left.FencingToken, right.FencingToken},
			{left.PriorBootSessionID, right.PriorBootSessionID}, {left.Class, right.Class},
		} {
			if comparison := strings.Compare(values[0], values[1]); comparison != 0 {
				return comparison
			}
		}
		return 0
	})
	return slices.Compact(merged)
}

func mergeResourceInventory(left, right ResourceInventory) ResourceInventory {
	left.Leases = mergeInventoryClass(left.Leases, right.Leases)
	left.Snapshots = mergeInventoryClass(left.Snapshots, right.Snapshots)
	left.Containers = mergeInventoryClass(left.Containers, right.Containers)
	left.Tasks = mergeInventoryClass(left.Tasks, right.Tasks)
	left.Shims = mergeInventoryClass(left.Shims, right.Shims)
	left.Cgroups = mergeInventoryClass(left.Cgroups, right.Cgroups)
	left.LogSegments = mergeInventoryClass(left.LogSegments, right.LogSegments)
	left.ImageSpools = mergeInventoryClass(left.ImageSpools, right.ImageSpools)
	left.ManagedVolumes = mergeInventoryClass(left.ManagedVolumes, right.ManagedVolumes)
	left.ManagedVolumeRecords = mergeInventoryClass(left.ManagedVolumeRecords, right.ManagedVolumeRecords)
	left.ComputerDiskImages = mergeInventoryClass(left.ComputerDiskImages, right.ComputerDiskImages)
	left.ComputerDiskAllocations = mergeInventoryClass(left.ComputerDiskAllocations, right.ComputerDiskAllocations)
	left.ComputerDiskQuotas = mergeInventoryClass(left.ComputerDiskQuotas, right.ComputerDiskQuotas)
	left.ComputerDiskManifests = mergeInventoryClass(left.ComputerDiskManifests, right.ComputerDiskManifests)
	left.ComputerDiskMounts = mergeInventoryClass(left.ComputerDiskMounts, right.ComputerDiskMounts)
	left.ComputerDiskLoops = mergeInventoryClass(left.ComputerDiskLoops, right.ComputerDiskLoops)
	left.ComputerAttachments = mergeInventoryClass(left.ComputerAttachments, right.ComputerAttachments)
	left.ComputerResetManifests = mergeInventoryClass(left.ComputerResetManifests, right.ComputerResetManifests)
	left.ComputerQuarantines = mergeInventoryClass(left.ComputerQuarantines, right.ComputerQuarantines)
	left.ComputerDiskAnomalies = mergeInventoryClass(left.ComputerDiskAnomalies, right.ComputerDiskAnomalies)
	return left
}

func mergeInventoryClass(left, right []string) []string {
	merged := append(slices.Clone(left), right...)
	slices.Sort(merged)
	return slices.Compact(merged)
}

type operationStream struct {
	net.Conn
	cancel context.CancelFunc
}

func (stream *operationStream) Read(buffer []byte) (int, error) {
	count, err := stream.Conn.Read(buffer)
	if err != nil && !errors.Is(err, io.EOF) {
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

func (stream *operationStream) CloseWrite() error {
	if half, ok := stream.Conn.(interface{ CloseWrite() error }); ok {
		return half.CloseWrite()
	}
	return errors.New("OCI helper stream transport does not support a write half-close")
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

func writeEngineResponseWithMethod(connection *framedConn, method Method, response any, err error) error {
	if err != nil {
		var serviceDataRejection *ServiceDataRejectionError
		if errors.As(err, &serviceDataRejection) {
			return writeFailure(connection, CodeOCISpecRejected, serviceDataRejection.Error())
		}
		var preflightStage *computerReimagePreflightStageError
		if method == MethodPreflightReimage && (errors.Is(err, errComputerReimageDetachmentRequired) || errors.As(err, &preflightStage)) {
			return writeRPCError(connection, engineFailureRPC(method, "OCI engine operation failed", engineFailureReason(err), err))
		}
		return writeRPCError(connection, engineFailureRPC(method, "OCI engine operation failed", engineFailureReason(err)))
	}
	return writeSuccess(connection, response)
}

func writeDiagnosticResponse(connection *framedConn, response any, err error) error {
	if err != nil {
		return writeFailure(connection, CodeDiagnosticFailure, "OCI diagnostic read failed")
	}
	return writeSuccess(connection, response)
}

func writeStreamResult(connection *framedConn, method Method, err error) {
	if err != nil {
		_ = writeRPCError(connection, engineFailureRPC(method, "OCI engine operation failed", engineFailureReason(err)))
		return
	}
	_ = connection.write(frame{Version: ProtocolVersion, OK: true})
}

func engineFailureRPC(method Method, message string, reason EngineFailureReason, cause ...error) *RPCError {
	const messageLimit = 1024
	if len(cause) > 0 && cause[0] != nil {
		detail := strings.TrimSpace(strings.NewReplacer("\r", " ", "\n", " ").Replace(cause[0].Error()))
		if len(detail) > messageLimit {
			detail = detail[:messageLimit]
		}
		if detail != "" {
			message += ": " + detail
		}
	}
	return &RPCError{Code: CodeEngineFailure, Message: message, EngineFailure: &EngineFailureFact{Operation: method, Reason: reason}}
}

func engineFailureReason(err error) EngineFailureReason {
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		return EngineFailureDeadlineExceeded
	case errors.Is(err, context.Canceled):
		return EngineFailureCanceled
	case errors.Is(err, os.ErrPermission):
		return EngineFailurePermissionDenied
	default:
		return EngineFailureOperationFailed
	}
}

func writeImageStreamResult(connection *framedConn, err error) {
	if err != nil {
		var mechanics *ImageMechanicsError
		var imageUnavailable *ImageUnavailableError
		if errors.As(err, &mechanics) {
			fact := mechanics.Fact
			if fact.RetryAfter < 0 {
				fact.RetryAfter = 0
			}
			_ = writeRPCError(connection, &RPCError{Code: CodeImageUnavailable, Message: "OCI image delivery failed", ImageFailure: &fact})
		} else if errors.As(err, &imageUnavailable) {
			fact := ImageFailureFact{Kind: ImageFailureUnavailable}
			_ = writeRPCError(connection, &RPCError{Code: CodeImageUnavailable, Message: "pinned local OCI image is unavailable", ImageFailure: &fact})
		} else {
			fact := ImageFailureFact{Kind: ImageFailureUnavailable}
			_ = writeRPCError(connection, &RPCError{Code: CodeImageUnavailable, Message: "OCI image delivery failed", ImageFailure: &fact})
		}
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
