package oci

import (
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	workloadrunner "github.com/Derek-X-Wang/wefty/runner"
	"github.com/Derek-X-Wang/wefty/runner/ocihelper"
)

// heldControlConn holds each frame the client writes on the helper control
// connection, which carries only heartbeats, before it leaves: the helper
// applies a heartbeat late, as a slow helper or a congested socket would. A
// hold shorter than the heartbeat interval keeps every heartbeat answered in
// time, so the session stays healthy, but a renewal queued meanwhile waits
// behind the heartbeat in flight.
type heldControlConn struct {
	net.Conn
	hold    *atomic.Int64
	writing chan struct{}
}

func (conn *heldControlConn) Write(payload []byte) (int, error) {
	// A frame is written as its 4-byte length, then its body; holding the
	// length holds the frame.
	if hold := time.Duration(conn.hold.Load()); hold > 0 && len(payload) == 4 {
		select {
		case conn.writing <- struct{}{}:
		default:
		}
		time.Sleep(hold)
	}
	return conn.Conn.Write(payload)
}

// heldPumpEngine keeps the attempt's payload running and records which
// attempts the helper reaped.
type heldPumpEngine struct {
	*adapterTestEngine
	release chan struct{}
	mu      sync.Mutex
	reaped  []string
}

func (engine *heldPumpEngine) Watch(ctx context.Context, _ ocihelper.WatchRequest, emit func(ocihelper.WatchEvent) error) error {
	select {
	case <-engine.release:
	case <-ctx.Done():
		return ctx.Err()
	}
	exitCode := 0
	return emit(ocihelper.WatchEvent{Kind: ocihelper.WatchComplete, Result: &ocihelper.WatchResponse{ExitCode: &exitCode}})
}

func (engine *heldPumpEngine) ReapAttempt(ctx context.Context, authority ocihelper.AttemptAuthority) error {
	engine.mu.Lock()
	engine.reaped = append(engine.reaped, authority.AttemptID)
	engine.mu.Unlock()
	return engine.adapterTestEngine.ReapAttempt(ctx, authority)
}

func (engine *heldPumpEngine) reapedAttempts() []string {
	engine.mu.Lock()
	defer engine.mu.Unlock()
	return append([]string(nil), engine.reaped...)
}

// A short deadman, an admission at the very end of the adapter's admission
// budget, and a heartbeat already in flight that the helper takes most of
// its timeout to apply: the worst case for the renewal forwarded at
// admission. It must still reach the helper before the attempt's deadman, so
// the attempt outlives its initial deadman and the session, with its
// neighbour, is never invalidated.
func TestAdmittedRenewalReachesHelperBeforeDeadmanBehindHeldHeartbeat(t *testing.T) {
	const (
		heartbeatTimeout = 3 * time.Second         // a one-second heartbeat interval
		initialDeadman   = 2500 * time.Millisecond // short against that interval
		hold             = 800 * time.Millisecond  // each heartbeat answered inside its interval
		admitBeforeEnd   = 50 * time.Millisecond
	)
	engine := &heldPumpEngine{adapterTestEngine: &adapterTestEngine{}, release: make(chan struct{})}
	directory, err := os.MkdirTemp("", "woci-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(directory) })
	socketPath := filepath.Join(directory, "helper.sock")
	listener, err := net.Listen("unix", socketPath)
	if err != nil {
		t.Fatal(err)
	}
	server, err := ocihelper.NewServer(engine, ocihelper.ServerConfig{
		HelperChecksum: "held-pump", AllowedUIDs: []uint32{uint32(os.Getuid())},
		HeartbeatTimeout: heartbeatTimeout, MaximumAttemptDeadman: 10 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	serveContext, cancelServe := context.WithCancel(context.Background())
	served := make(chan error, 1)
	go func() { served <- server.Serve(serveContext, listener) }()
	defer func() {
		cancelServe()
		_ = listener.Close()
		<-served
	}()

	client := ocihelper.NewUnixClient(socketPath, "held-pump")
	var holdNanos atomic.Int64
	heldWrite := make(chan struct{}, 1)
	dial := client.Dial
	var dials atomic.Int32
	client.Dial = func(ctx context.Context) (net.Conn, error) {
		conn, err := dial(ctx)
		if err != nil || dials.Add(1) != 1 {
			return conn, err
		}
		// The session's first connection is its control connection.
		return &heldControlConn{Conn: conn, hold: &holdNanos, writing: heldWrite}, nil
	}
	barrier, err := ocihelper.NewBootBarrier(client, ocihelper.AcquireSessionRequest{NodeID: "node", BootSessionID: "boot"})
	if err != nil {
		t.Fatal(err)
	}
	defer barrier.Close()
	if err := barrier.Ensure(t.Context()); err != nil {
		t.Fatal(err)
	}
	session, err := barrier.Session()
	if err != nil {
		t.Fatal(err)
	}
	generation := session.Handshake().SessionGeneration
	adapter := NewAdapterWithPolicy(&adapterSnapshotSource{barrier: barrier}, ImagePolicy{})
	adapter.probePlatforms[helperSession(session)] = ocihelper.OCIPlatform{OS: "linux", Architecture: "amd64"}

	// A neighbour attempt shares the session, with a long deadman of its own.
	neighbour := ocihelper.AttemptAuthority{NodeID: "node", BootSessionID: "boot", JobID: "neighbour-job",
		AttemptID: "neighbour-attempt", FencingToken: "neighbour-fence", Class: "one-shot", RemovalGeneration: "attempt"}
	if _, err := session.Run(t.Context(), ocihelper.RunRequest{Authority: neighbour, InitialDeadman: 10 * time.Second,
		Workload: ocihelper.WorkloadInput{ImageDigest: adapterTestDigest, Argv: []string{"/bin/true"}}}); err != nil {
		t.Fatal(err)
	}

	request := adapterTestRequest()
	request.InitialDeadman = initialDeadman
	var timesMu sync.Mutex
	var runRequested, admittedAt time.Time
	var budget time.Duration
	times := func() (time.Time, time.Duration, time.Time) {
		timesMu.Lock()
		defer timesMu.Unlock()
		return runRequested, budget, admittedAt
	}
	request.OCIAdmissionBudget = func(reported time.Duration) {
		timesMu.Lock()
		runRequested, budget = time.Now(), reported
		timesMu.Unlock()
	}
	request.OCIStarted = func(context.Context, workloadrunner.OCIImageObservation) error {
		// The agent admits no later than the end of the budget; this one
		// admits right at it.
		requested, budget, _ := times()
		admitAt := requested.Add(budget - admitBeforeEnd)
		if budget <= admitBeforeEnd {
			return errors.New("no admission budget")
		}
		time.Sleep(time.Until(admitAt))
		// Put a heartbeat in flight that the helper applies only after
		// hold, by renewing the neighbour, and admit while it is in flight.
		holdNanos.Store(int64(hold))
		if err := session.QueueAttemptRenewalUntil(neighbour, time.Now().Add(9*time.Second)); err != nil {
			return err
		}
		select {
		case <-heldWrite:
		case <-time.After(2 * time.Second):
			return errors.New("no heartbeat went in flight")
		}
		return nil
	}
	request.OCIHelperAdmitted = func(workloadrunner.RuntimeGeneration) error {
		// What the agent forwards at admission: the L1 renewal it held.
		timesMu.Lock()
		admittedAt = time.Now()
		timesMu.Unlock()
		return session.QueueAttemptRenewalUntil(HelperAuthority(request.Authority), time.Now().Add(9*time.Second))
	}
	ran := make(chan error, 1)
	go func() {
		_, err := adapter.Run(context.Background(), request, nil)
		ran <- err
	}()
	defer func() {
		holdNanos.Store(0)
		close(engine.release)
		select {
		case <-ran:
		case <-time.After(10 * time.Second):
			t.Error("the attempt did not finish")
		}
	}()

	// Wait out the attempt's initial deadman, with room for its reap.
	deadline := time.Now().Add(10 * time.Second)
	requested, reported, _ := times()
	for requested.IsZero() && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
		requested, reported, _ = times()
	}
	if requested.IsZero() {
		t.Fatal("the adapter never reported an admission budget")
	}
	time.Sleep(time.Until(requested.Add(initialDeadman + time.Second)))
	_, _, admitted := times()
	if admitted.IsZero() {
		t.Fatalf("the attempt was not admitted inside its %s budget", reported)
	}
	if err := session.HealthError(); err != nil {
		t.Fatalf("helper session after the forwarded renewal: %v", err)
	}
	current, err := barrier.Session()
	if err != nil || current.Handshake().SessionGeneration != generation {
		t.Fatalf("helper session was replaced: %v", err)
	}
	doctor, err := current.DoctorStatus(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if evidence := doctor.LastSessionInvalidation; evidence != nil {
		t.Fatalf("helper session invalidated: attempt=%s code=%s", evidence.AttemptID, evidence.RejectionCode)
	}
	if reaped := engine.reapedAttempts(); len(reaped) != 0 {
		t.Fatalf("attempts reaped by %s after Run, admitted at %s: %v; the forwarded renewal missed the deadman",
			time.Since(requested).Round(time.Millisecond), admitted.Sub(requested).Round(time.Millisecond), reaped)
	}
}
