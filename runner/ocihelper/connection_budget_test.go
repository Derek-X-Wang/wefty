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

func startHoldingSession(t *testing.T, config ServerConfig) (*holdingDialEngine, *Session, AttemptAuthority) {
	engine, session, authority, _ := startHoldingSessionWithClient(t, config)
	return engine, session, authority
}

func startHoldingSessionWithClient(t *testing.T, config ServerConfig) (*holdingDialEngine, *Session, AttemptAuthority, *Client) {
	t.Helper()
	base := newFakeEngine()
	base.setRunResponse(RunResponse{Started: true, StartedAt: testStartedAt(), Endpoints: map[string]uint16{"service": 42001}})
	engine := &holdingDialEngine{fakeEngine: base}
	config.HeartbeatTimeout = 5 * time.Second
	client, stop := startTestServer(t, engine, config)
	t.Cleanup(stop)
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
	return engine, session, authority, client
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
	engine, session, authority := startHoldingSession(t, ServerConfig{ConnectionLimit: 6})
	generation := session.Handshake().SessionGeneration
	streams := holdStreams(t, session, authority, 3)

	_, err := session.DialAttemptPort(t.Context(), DialAttemptPortRequest{Authority: authority, Name: "service"})
	if !IsConnectionLimitRefusal(err) {
		t.Fatalf("data stream over its budget = %v, want a typed connection_limit refusal", err)
	}
	requireSessionSurvived(t, engine, session, generation)

	// Control RPCs are not data streams: the reserve keeps them a slot.
	if err := session.Signal(t.Context(), SignalRequest{Authority: authority, Signal: SignalTERM}); err != nil {
		t.Fatalf("Signal while data streams are at budget: %v", err)
	}
	var completed bool
	if err := session.Watch(t.Context(), WatchRequest{Authority: authority}, func(event WatchEvent) error {
		completed = completed || event.Result != nil
		return nil
	}); err != nil || !completed {
		t.Fatalf("Watch while data streams are at budget: err=%v completed=%v", err, completed)
	}
	requireSessionSurvived(t, engine, session, generation)

	// The budget is a live count, not a latch: closing one stream admits the
	// next.
	_ = streams[0].Close()
	deadline := time.Now().Add(2 * time.Second)
	for {
		next, err := session.DialAttemptPort(t.Context(), DialAttemptPortRequest{Authority: authority, Name: "service"})
		if err == nil {
			_ = next.Close()
			break
		}
		if !IsConnectionLimitRefusal(err) || time.Now().After(deadline) {
			t.Fatalf("data stream after one closed = %v", err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	requireSessionSurvived(t, engine, session, generation)
}

// When every connection slot is taken, the helper still answers the next
// connection with a typed refusal rather than closing it, so a control RPC
// caught in the overflow fails alone and the session survives.
func TestConnectionOverTheWholeLimitIsATypedRefusal(t *testing.T) {
	// A reserve of 1 lets data streams take 5 of 6 slots; with the control
	// connection that fills the helper.
	engine, session, authority := startHoldingSession(t, ServerConfig{ConnectionLimit: 6, ControlConnectionReserve: 1})
	generation := session.Handshake().SessionGeneration
	streams := holdStreams(t, session, authority, 5)

	if _, err := session.DialAttemptPort(t.Context(), DialAttemptPortRequest{Authority: authority, Name: "service"}); !IsConnectionLimitRefusal(err) {
		t.Fatalf("stream over the whole limit = %v, want a typed connection_limit refusal", err)
	}
	if err := session.Signal(t.Context(), SignalRequest{Authority: authority, Signal: SignalTERM}); !IsConnectionLimitRefusal(err) {
		t.Fatalf("Signal over the whole limit = %v, want a typed connection_limit refusal", err)
	}
	requireSessionSurvived(t, engine, session, generation)

	_ = streams[0].Close()
	waitFor(t, 2*time.Second, func() bool {
		return session.Signal(t.Context(), SignalRequest{Authority: authority, Signal: SignalTERM}) == nil
	}, "Signal after one stream closed")
	requireSessionSurvived(t, engine, session, generation)
}

func startRawHelper(t *testing.T, config ServerConfig) string {
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
	server, err := NewServer(newFakeEngine(), config)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- server.Serve(ctx, listener) }()
	t.Cleanup(func() {
		cancel()
		_ = listener.Close()
		<-done
	})
	return path
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
	_ = connection.SetDeadline(time.Now().Add(3 * time.Second))
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
// peer is never bare-closed, and one that never sends its frame delays the
// next answer by at most the refusal deadline (#597 review).
func TestOverLimitRefusalIsBoundedAndAuthenticated(t *testing.T) {
	t.Run("allowed peer gets connection_limit, even behind a silent one", func(t *testing.T) {
		path := startRawHelper(t, ServerConfig{ConnectionLimit: 1, AllowedUIDs: []uint32{uint32(os.Getuid())}})
		_ = dialRaw(t, path) // holds the only slot, never writes
		_ = dialRaw(t, path) // overflow peer that never writes
		answered := dialRaw(t, path)
		started := time.Now()
		response, err := requestRaw(t, answered)
		if err != nil || response.Error == nil || response.Error.Code != CodeConnectionLimit {
			t.Fatalf("overflow response = %+v err=%v, want connection_limit", response, err)
		}
		if elapsed := time.Since(started); elapsed >= connectionLimitRefusalTimeout {
			t.Fatalf("a silent overflow peer delayed the next refusal by %s", elapsed)
		}
	})
	t.Run("allowed peer past a full refusal budget waits for a typed answer", func(t *testing.T) {
		path := startRawHelper(t, ServerConfig{ConnectionLimit: 1, AllowedUIDs: []uint32{uint32(os.Getuid())}})
		_ = dialRaw(t, path)
		for range connectionLimitRefusalBudget {
			_ = dialRaw(t, path) // each holds a refusal worker until its deadline
		}
		response, err := requestRaw(t, dialRaw(t, path))
		if err != nil || response.Error == nil || response.Error.Code != CodeConnectionLimit {
			t.Fatalf("allowed peer past the refusal budget = %+v err=%v, want connection_limit, never a bare close", response, err)
		}
	})
	t.Run("peers outside the allowlist are closed at once and hold nothing", func(t *testing.T) {
		path := startRawHelper(t, ServerConfig{ConnectionLimit: 1, AllowedUIDs: []uint32{uint32(os.Getuid()) + 1}})
		_ = dialRaw(t, path)
		started := time.Now()
		for index := range 3 * connectionLimitRefusalBudget {
			silent := dialRaw(t, path)
			_ = silent.SetReadDeadline(time.Now().Add(3 * time.Second))
			var buffer [1]byte
			if _, err := silent.Read(buffer[:]); err == nil || isTimeout(err) {
				t.Fatalf("foreign overflow peer %d was not closed: %v", index, err)
			}
		}
		// Had any of them held a refusal worker until its deadline, the
		// ninth would have waited at least that long.
		if elapsed := time.Since(started); elapsed >= connectionLimitRefusalTimeout {
			t.Fatalf("foreign overflow peers took %s to close; they held refusal workers", elapsed)
		}
	})
}

// With every slot taken and every refusal worker held by silent local peers,
// the agent's next RPC still gets a typed answer and the session survives:
// the old bare close at that point was read as EOF and reaped everything.
func TestAgentRequestBehindSilentOverflowPeersGetsATypedAnswer(t *testing.T) {
	engine, session, authority, client := startHoldingSessionWithClient(t, ServerConfig{ConnectionLimit: 6, ControlConnectionReserve: 1})
	generation := session.Handshake().SessionGeneration
	_ = holdStreams(t, session, authority, 5)
	for range connectionLimitRefusalBudget + 2 {
		silent, err := client.Dial(t.Context())
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
	engine, session, authority := startHoldingSession(t, ServerConfig{ConnectionLimit: 6, ControlConnectionReserve: 1})
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
