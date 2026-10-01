package ocihelper

import (
	"context"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// holdingDialEngine keeps every attempt-port stream open until the client
// closes it, like a proxied keep-alive client of a published service.
type holdingDialEngine struct {
	*fakeEngine
}

func (engine *holdingDialEngine) DialAttemptPort(_ context.Context, _ DialAttemptPortRequest, stream io.ReadWriteCloser) error {
	if _, err := stream.Write([]byte{attemptPortBackendReady}); err != nil {
		return err
	}
	_, _ = io.Copy(io.Discard, stream)
	return nil
}

// budgetServer is a helper whose own slot bookkeeping a test can wait on.
// Slots are freed asynchronously, after the handler that held one returns, so
// a test that needs a slot free waits for that release, never for time.
type budgetServer struct {
	server  *Server
	path    string
	changed chan struct{}
}

func startBudgetServer(t *testing.T, engine Engine, config ServerConfig) *budgetServer {
	t.Helper()
	directory, err := os.MkdirTemp("", "wefty-oci-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(directory) })
	path := filepath.Join(directory, "helper.sock")
	listener, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	if config.HelperChecksum == "" {
		config.HelperChecksum = "checksum-test"
	}
	if len(config.AllowedUIDs) == 0 {
		config.AllowedUIDs = []uint32{uint32(os.Getuid())}
	}
	budget := &budgetServer{path: path, changed: make(chan struct{}, 1)}
	// A coalescing wakeup is enough: the waiter re-reads the counts after
	// every wake, so a release between its read and its wait is never lost.
	config.connectionReleased = func() {
		select {
		case budget.changed <- struct{}{}:
		default:
		}
	}
	server, err := NewServer(engine, config)
	if err != nil {
		t.Fatal(err)
	}
	budget.server = server
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- server.Serve(ctx, listener) }()
	t.Cleanup(func() {
		cancel()
		_ = listener.Close()
		<-done
	})
	return budget
}

// awaitHeld waits until exactly slots connection slots, of which dataStreams
// are data streams, are held.
func (budget *budgetServer) awaitHeld(t *testing.T, slots, dataStreams int) {
	t.Helper()
	for len(budget.server.connections) != slots || len(budget.server.dataStreams) != dataStreams {
		select {
		case <-budget.changed:
		case <-t.Context().Done():
			t.Fatalf("helper never settled at %d slots and %d data streams", slots, dataStreams)
		}
	}
}

func startHoldingSession(t *testing.T, config ServerConfig) (*holdingDialEngine, *Session, AttemptAuthority, *budgetServer) {
	t.Helper()
	base := newFakeEngine()
	base.setRunResponse(RunResponse{Started: true, StartedAt: testStartedAt(), Endpoints: map[string]uint16{"service": 42001}})
	engine := &holdingDialEngine{fakeEngine: base}
	config.HeartbeatTimeout = 5 * time.Second
	budget := startBudgetServer(t, engine, config)
	client := NewUnixClient(budget.path, "checksum-test")
	client.disableHeartbeatPump = true
	session, err := client.OpenSession(t.Context(), testSessionRequest())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = session.Close() })
	requireSweep(t, session)
	authority := testAuthority()
	request := testRunRequest(authority, 5*time.Second)
	request.AllocateEndpoints = []string{"service"}
	if _, err := session.Run(t.Context(), request); err != nil {
		t.Fatal(err)
	}
	// Sweep and Run each held a slot until their handlers returned. Only
	// the control connection may remain before a test counts slots.
	budget.awaitHeld(t, 1, 0)
	return engine, session, authority, budget
}

func holdStreams(t *testing.T, session *Session, authority AttemptAuthority, count int) []net.Conn {
	t.Helper()
	streams := make([]net.Conn, 0, count)
	t.Cleanup(func() {
		for _, stream := range streams {
			_ = stream.Close()
		}
	})
	for index := range count {
		stream, err := session.DialAttemptPort(t.Context(), DialAttemptPortRequest{Authority: authority, Name: "service"})
		if err != nil {
			t.Fatalf("stream %d: %v", index, err)
		}
		streams = append(streams, stream)
	}
	return streams
}

func requireSessionSurvived(t *testing.T, engine *holdingDialEngine, session *Session, generation uint64) {
	t.Helper()
	if err := session.HealthError(); err != nil {
		t.Fatalf("a connection_limit refusal marked the helper session lost: %v", err)
	}
	if err := session.flushHeartbeat(t.Context()); err != nil {
		t.Fatalf("a connection_limit refusal ended the helper session: %v", err)
	}
	if got := session.Handshake().SessionGeneration; got != generation {
		t.Fatalf("session generation = %d after refusal, want %d", got, generation)
	}
	engine.mu.Lock()
	sessionReaps, attemptReaps := len(engine.sessionReaps), len(engine.attemptReaps)
	engine.mu.Unlock()
	if sessionReaps != 0 || attemptReaps != 0 {
		t.Fatalf("a connection_limit refusal reaped the session %d times and attempts %d times", sessionReaps, attemptReaps)
	}
}

// One proxied client over the helper's stream budget used to be accepted and
// closed without a frame. The client read EOF, marked the session lost, and
// the helper reaped every OCI workload on the Node (#597). Data streams now
// stop at the connection limit minus the control reserve, the next one is
// refused with a typed connection_limit frame, and control RPCs still find a
// slot.
func TestDataStreamOverBudgetIsRefusedAndTheSessionSurvives(t *testing.T) {
	// A limit of 6 clamps the default reserve of 8 to 3, leaving 3 slots to
	// data streams and 3 to the control connection and control RPCs.
	engine, session, authority, budget := startHoldingSession(t, ServerConfig{ConnectionLimit: 6})
	generation := session.Handshake().SessionGeneration
	streams := holdStreams(t, session, authority, 3)

	_, err := session.DialAttemptPort(t.Context(), DialAttemptPortRequest{Authority: authority, Name: "service"})
	if !IsConnectionLimitRefusal(err) {
		t.Fatalf("data stream over its budget = %v, want a typed connection_limit refusal", err)
	}
	requireSessionSurvived(t, engine, session, generation)
	// The refused stream was admitted to a slot before its data budget
	// refused it; wait for that slot back, so the reserve below is exactly
	// what the control connection and three streams leave.
	budget.awaitHeld(t, 4, 3)

	// Control RPCs are not data streams: the reserve keeps them a slot.
	if err := session.Signal(t.Context(), SignalRequest{Authority: authority, Signal: SignalTERM}); err != nil {
		t.Fatalf("Signal while data streams are at budget: %v", err)
	}
	budget.awaitHeld(t, 4, 3)
	var completed bool
	if err := session.Watch(t.Context(), WatchRequest{Authority: authority}, func(event WatchEvent) error {
		completed = completed || event.Result != nil
		return nil
	}); err != nil || !completed {
		t.Fatalf("Watch while data streams are at budget: err=%v completed=%v", err, completed)
	}
	requireSessionSurvived(t, engine, session, generation)

	// The budget is a live count, not a latch: closing one stream admits the
	// next. Once the closed stream's handler, and the Watch's, have given
	// their slots back, the control connection and two streams remain.
	_ = streams[0].Close()
	budget.awaitHeld(t, 3, 2)
	next, err := session.DialAttemptPort(t.Context(), DialAttemptPortRequest{Authority: authority, Name: "service"})
	if err != nil {
		t.Fatalf("data stream after one closed = %v", err)
	}
	_ = next.Close()
	requireSessionSurvived(t, engine, session, generation)
}

// When every connection slot is taken, the helper still answers the next
// connection with a typed refusal rather than closing it, so a control RPC
// caught in the overflow fails alone and the session survives.
func TestConnectionOverTheWholeLimitIsATypedRefusal(t *testing.T) {
	// A reserve of 1 lets data streams take 5 of 6 slots; with the control
	// connection that fills the helper.
	engine, session, authority, budget := startHoldingSession(t, ServerConfig{ConnectionLimit: 6, ControlConnectionReserve: 1})
	generation := session.Handshake().SessionGeneration
	streams := holdStreams(t, session, authority, 5)

	if _, err := session.DialAttemptPort(t.Context(), DialAttemptPortRequest{Authority: authority, Name: "service"}); !IsConnectionLimitRefusal(err) {
		t.Fatalf("stream over the whole limit = %v, want a typed connection_limit refusal", err)
	}
	if err := session.Signal(t.Context(), SignalRequest{Authority: authority, Signal: SignalTERM}); !IsConnectionLimitRefusal(err) {
		t.Fatalf("Signal over the whole limit = %v, want a typed connection_limit refusal", err)
	}
	requireSessionSurvived(t, engine, session, generation)

	// The refused requests never held a slot, so once the closed stream's
	// handler gives its slot back exactly one is free.
	_ = streams[0].Close()
	budget.awaitHeld(t, 5, 4)
	if err := session.Signal(t.Context(), SignalRequest{Authority: authority, Signal: SignalTERM}); err != nil {
		t.Fatalf("Signal after one stream closed = %v", err)
	}
	requireSessionSurvived(t, engine, session, generation)
}

// startRawHelper starts a helper for raw-socket tests. Admitted silent peers
// and refusal workers hold for an hour unless a test says otherwise, so what a
// test observes never depends on a deadline expiring under it.
func startRawHelper(t *testing.T, config ServerConfig) string {
	t.Helper()
	config.RequestTimeout = time.Hour
	if config.limitRefusalTimeout == 0 {
		config.limitRefusalTimeout = time.Hour
	}
	return startBudgetServer(t, newFakeEngine(), config).path
}

func dialRaw(t *testing.T, path string) net.Conn {
	t.Helper()
	connection, err := net.Dial("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = connection.Close() })
	return connection
}

// requestRaw sends one request frame and reads the one response frame.
func requestRaw(t *testing.T, connection net.Conn) (frame, error) {
	t.Helper()
	// A hang guard only: every answer here is immediate or waits on an event
	// the test itself causes.
	_ = connection.SetDeadline(time.Now().Add(30 * time.Second))
	wire := newFramedConn(connection)
	if err := wire.write(frame{Version: ProtocolVersion, Method: MethodSignal, SessionCapability: "capability"}); err != nil {
		return frame{}, err
	}
	var response frame
	err := wire.read(&response)
	return response, err
}

// The overflow answer is bounded on both sides. A peer outside the allowlist
// is closed on the accept loop and never holds a refusal worker; an allowed
// peer is never bare-closed, and a silent one holding a worker does not hold
// up the next answer (#597 review). Silent peers here hold their slot or
// worker for an hour, so none of this depends on a deadline expiring.
func TestOverLimitRefusalIsBoundedAndAuthenticated(t *testing.T) {
	t.Run("allowed peer gets connection_limit, even behind a silent one", func(t *testing.T) {
		path := startRawHelper(t, ServerConfig{ConnectionLimit: 1})
		_ = dialRaw(t, path) // holds the only slot, never writes
		_ = dialRaw(t, path) // holds a refusal worker, never writes
		response, err := requestRaw(t, dialRaw(t, path))
		if err != nil || response.Error == nil || response.Error.Code != CodeConnectionLimit {
			t.Fatalf("overflow response = %+v err=%v, want connection_limit", response, err)
		}
	})
	t.Run("allowed peer past a full refusal budget waits for a typed answer", func(t *testing.T) {
		path := startRawHelper(t, ServerConfig{ConnectionLimit: 1})
		_ = dialRaw(t, path)
		silent := make([]net.Conn, 0, connectionLimitRefusalBudget)
		for range connectionLimitRefusalBudget {
			silent = append(silent, dialRaw(t, path)) // each holds a refusal worker
		}
		type answer struct {
			response frame
			err      error
		}
		answered := make(chan answer, 1)
		requester := dialRaw(t, path)
		go func() {
			response, err := requestRaw(t, requester)
			answered <- answer{response, err}
		}()
		// Freeing one worker is what lets the accept loop answer the waiting
		// peer; nothing else would within the hour the workers hold.
		_ = silent[0].Close()
		got := <-answered
		if got.err != nil || got.response.Error == nil || got.response.Error.Code != CodeConnectionLimit {
			t.Fatalf("allowed peer past the refusal budget = %+v err=%v, want connection_limit, never a bare close", got.response, got.err)
		}
	})
	t.Run("peers outside the allowlist are closed at once and hold nothing", func(t *testing.T) {
		path := startRawHelper(t, ServerConfig{ConnectionLimit: 1, AllowedUIDs: []uint32{uint32(os.Getuid()) + 1}})
		_ = dialRaw(t, path)
		// Each must be closed outright: one that took a refusal worker would
		// sit there for the worker's hour and trip the read guard.
		for index := range 3 * connectionLimitRefusalBudget {
			silent := dialRaw(t, path)
			_ = silent.SetReadDeadline(time.Now().Add(30 * time.Second))
			var buffer [1]byte
			if _, err := silent.Read(buffer[:]); err == nil || isTimeout(err) {
				t.Fatalf("foreign overflow peer %d was not closed: %v", index, err)
			}
		}
	})
}

// With every slot taken and every refusal worker held by silent local peers,
// the agent's next RPC still gets a typed answer and the session survives:
// the old bare close at that point was read as EOF and reaped everything.
func TestAgentRequestBehindSilentOverflowPeersGetsATypedAnswer(t *testing.T) {
	engine, session, authority, budget := startHoldingSession(t, ServerConfig{ConnectionLimit: 6, ControlConnectionReserve: 1})
	generation := session.Handshake().SessionGeneration
	_ = holdStreams(t, session, authority, 5)
	for range connectionLimitRefusalBudget + 2 {
		silent, err := net.Dial("unix", budget.path)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = silent.Close() })
	}
	err := session.Signal(t.Context(), SignalRequest{Authority: authority, Signal: SignalTERM})
	if !IsConnectionLimitRefusal(err) {
		t.Fatalf("Signal behind silent overflow peers = %v, want a typed connection_limit refusal", err)
	}
	requireSessionSurvived(t, engine, session, generation)
}

func isTimeout(err error) bool {
	var netErr net.Error
	return errors.As(err, &netErr) && netErr.Timeout()
}

func TestConnectionLimitLogReportsOncePerBurst(t *testing.T) {
	var burst ConnectionLimitLog
	start := time.Unix(1000, 0)
	if report, suppressed := burst.Note(start); !report || suppressed != 0 {
		t.Fatalf("first refusal: report=%v suppressed=%d", report, suppressed)
	}
	for offset := range 10 {
		if report, _ := burst.Note(start.Add(time.Duration(offset+1) * time.Second)); report {
			t.Fatalf("refusal %d inside the burst was reported", offset)
		}
	}
	// A burst that lasts is reported again, once per interval, with its count.
	now := start.Add(10*time.Second + connectionLimitLogInterval)
	for at := start.Add(11 * time.Second); at.Before(now); at = at.Add(20 * time.Second) {
		burst.Note(at)
	}
	if report, suppressed := burst.Note(now); !report || suppressed == 0 {
		t.Fatalf("lasting burst: report=%v suppressed=%d, want a report with the count", report, suppressed)
	}
	// After a quiet gap a new burst is reported at once.
	if report, _ := burst.Note(now.Add(connectionLimitBurstQuiet)); !report {
		t.Fatal("a new burst after a quiet gap was not reported")
	}
}

// Honest overload -- many real agent requests while every slot is taken --
// is answered at once: each request's frame is already on the wire, so a
// refusal worker is busy for microseconds and the accept loop's wait for one
// is just as short. Only a silent peer, which can only be an allowlisted and
// therefore trusted one, can make that wait approach the refusal deadline.
// A burst far wider than the refusal budget must therefore get every typed
// answer well inside a Signal's one-second delivery bound.
func TestHonestOverloadIsAnsweredWellInsideTheSignalDeadline(t *testing.T) {
	engine, session, authority, _ := startHoldingSession(t, ServerConfig{ConnectionLimit: 6, ControlConnectionReserve: 1})
	generation := session.Handshake().SessionGeneration
	_ = holdStreams(t, session, authority, 5)

	const requests = 8 * connectionLimitRefusalBudget
	const bound = 250 * time.Millisecond // a quarter of the 1 s signal deadline
	latencies := make(chan time.Duration, requests)
	failures := make(chan error, requests)
	var start sync.WaitGroup
	start.Add(1)
	var done sync.WaitGroup
	for range requests {
		done.Add(1)
		go func() {
			defer done.Done()
			start.Wait()
			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			defer cancel()
			began := time.Now()
			err := session.Signal(ctx, SignalRequest{Authority: authority, Signal: SignalTERM})
			latencies <- time.Since(began)
			if !IsConnectionLimitRefusal(err) {
				failures <- err
			}
		}()
	}
	start.Done()
	done.Wait()
	close(latencies)
	close(failures)
	for err := range failures {
		t.Fatalf("a request under honest overload got %v, want a typed connection_limit refusal", err)
	}
	var slowest time.Duration
	for latency := range latencies {
		slowest = max(slowest, latency)
	}
	if slowest >= bound {
		t.Fatalf("the slowest of %d refusals took %s, want under %s", requests, slowest, bound)
	}
	t.Logf("slowest of %d refusals under honest overload: %s", requests, slowest)
	requireSessionSurvived(t, engine, session, generation)
}
