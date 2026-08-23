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

func TestDeterministicResourceIdentityCarriesCompleteAuthority(t *testing.T) {
	authority := testAuthority()
	first, err := DeterministicResourceIdentity(authority)
	if err != nil {
		t.Fatal(err)
	}
	second, err := DeterministicResourceIdentity(authority)
	if err != nil {
		t.Fatal(err)
	}
	if first.LeaseID != second.LeaseID || first.ContainerID != second.ContainerID || first.TaskID != second.TaskID || first.SnapshotID != second.SnapshotID {
		t.Fatalf("deterministic identity changed: first=%+v second=%+v", first, second)
	}
	if len(first.Labels) != 7 || first.Labels["io.wefty/fencing_token"] != authority.FencingToken || first.Labels["io.wefty/removal_generation"] != authority.RemovalGeneration {
		t.Fatalf("resource labels do not carry complete authority: %#v", first.Labels)
	}
	changed := authority
	changed.FencingToken = "fence-2"
	different, err := DeterministicResourceIdentity(changed)
	if err != nil {
		t.Fatal(err)
	}
	if different.LeaseID == first.LeaseID {
		t.Fatal("a new fence reused the prior deterministic identity")
	}
}

func TestWrongVersionAndPeerFailBeforeSessionAuthority(t *testing.T) {
	engine := newFakeEngine()
	client, stop := startTestServer(t, engine, ServerConfig{})
	client.Version = ProtocolVersion + 1
	_, err := client.OpenSession(t.Context(), testSessionRequest())
	assertRPCCode(t, err, CodeVersionMismatch)
	stop()
	if engine.sessionReapCount() != 0 {
		t.Fatal("a rejected protocol version minted session authority")
	}

	client, stop = startTestServer(t, engine, ServerConfig{AllowedUIDs: []uint32{uint32(os.Getuid() + 1)}})
	defer stop()
	_, err = client.OpenSession(t.Context(), testSessionRequest())
	assertRPCCode(t, err, CodePeerUnauthenticated)
	if engine.sessionReapCount() != 0 {
		t.Fatal("an unauthenticated peer minted session authority")
	}
}

func TestClientVerifiesReturnedChecksumLocally(t *testing.T) {
	client, stop := startTestServer(t, newFakeEngine(), ServerConfig{})
	defer stop()
	client.ExpectedChecksum = "different-installed-checksum"
	_, err := client.OpenSession(t.Context(), AcquireSessionRequest{
		NodeID: "node-1", BootSessionID: "boot-1", ExpectedHelperChecksum: "checksum-test",
	})
	assertRPCCode(t, err, CodeChecksumMismatch)
}

func TestExclusiveSessionEOFAndHeartbeatBlackholeFailClosed(t *testing.T) {
	engine := newFakeEngine()
	clock := newManualClock(time.Unix(10_000, 0))
	client, stop := startTestServer(t, engine, ServerConfig{HeartbeatTimeout: time.Second, Clock: clock})
	defer stop()
	first, err := client.OpenSession(t.Context(), testSessionRequest())
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.OpenSession(t.Context(), AcquireSessionRequest{NodeID: "node-2", BootSessionID: "boot-2"})
	assertRPCCode(t, err, CodeSessionBusy)
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	waitFor(t, time.Second, func() bool { return engine.sessionReapCount() == 1 }, "EOF session reap")

	second, err := client.OpenSession(t.Context(), AcquireSessionRequest{NodeID: "node-2", BootSessionID: "boot-2"})
	if err != nil {
		t.Fatal(err)
	}
	clock.Advance(time.Second)
	waitFor(t, time.Second, func() bool { return engine.sessionReapCount() == 2 }, "heartbeat blackhole reap")
	err = second.EnsureImage(t.Context(), EnsureImageRequest{Reference: "example.invalid/probe", Digest: "sha256:test"}, nil)
	assertRPCCode(t, err, CodeSessionStale)
}

func TestFailedSessionReapStopsHelperForFreshProcessRecovery(t *testing.T) {
	engine := newFakeEngine()
	engine.sessionReapErr = errors.New("residue remains")
	client, stop := startTestServer(t, engine, ServerConfig{HeartbeatTimeout: time.Second})
	defer stop()
	session, err := client.OpenSession(t.Context(), testSessionRequest())
	if err != nil {
		t.Fatal(err)
	}
	_ = session.Close()
	waitFor(t, time.Second, func() bool { return engine.sessionReapCount() == 1 }, "failed session reap")
	_, err = client.OpenSession(t.Context(), AcquireSessionRequest{NodeID: "replacement", BootSessionID: "replacement-boot"})
	if err == nil {
		t.Fatal("failed reap left the helper accepting connections")
	}
}

func TestHeartbeatRefreshesOnlyExactLiveAttemptDeadman(t *testing.T) {
	engine := newFakeEngine()
	clock := newManualClock(time.Unix(20_000, 0))
	client, stop := startTestServer(t, engine, ServerConfig{
		HeartbeatTimeout: 5 * time.Minute, MaximumAttemptDeadman: 5 * time.Minute, Clock: clock,
	})
	defer stop()
	session, err := client.OpenSession(t.Context(), testSessionRequest())
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	requireSweep(t, session)
	authority := testAuthority()
	if _, err := session.Run(t.Context(), testRunRequest(authority, time.Minute)); err != nil {
		t.Fatal(err)
	}
	clock.Advance(30 * time.Second)
	if err := session.QueueAttemptRenewal(authority, time.Minute); err != nil {
		t.Fatal(err)
	}
	if err := session.flushHeartbeat(t.Context()); err != nil {
		t.Fatal(err)
	}
	clock.Advance(31 * time.Second)
	if engine.attemptReapCount() != 0 {
		t.Fatal("attempt was reaped at the superseded deadman")
	}
	clock.Advance(29 * time.Second)
	waitFor(t, time.Second, func() bool { return engine.attemptReapCount() == 1 }, "renewed attempt deadman reap")

	stale := authority
	stale.FencingToken = "stale"
	err = session.QueueAttemptRenewal(stale, time.Second)
	if err == nil {
		err = session.flushHeartbeat(t.Context())
	}
	assertRPCCode(t, err, CodeUnauthorizedAttempt)
}

func TestAttemptPortAndMacBridgeRequireExactAttemptCapabilities(t *testing.T) {
	engine := newFakeEngine()
	engine.setRunResponse(RunResponse{Started: true, AttemptPort: 42001, HostBridgeReady: true})
	client, stop := startTestServer(t, engine, ServerConfig{HeartbeatTimeout: 2 * time.Second})
	defer stop()
	session, err := client.OpenSession(t.Context(), testSessionRequest())
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	requireSweep(t, session)
	authority := testAuthority()
	request := testRunRequest(authority, time.Second)
	request.EnableHostBridgeFallback = true
	run, err := session.Run(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	if run.BridgeCapability == "" {
		t.Fatal("authorized Mac fallback did not receive an opaque bridge capability")
	}
	_, err = session.DialAttemptPort(t.Context(), DialAttemptPortRequest{Authority: authority, Port: 42002})
	assertRPCCode(t, err, CodeUnauthorizedPort)
	port, err := session.DialAttemptPort(t.Context(), DialAttemptPortRequest{Authority: authority, Port: 42001})
	if err != nil {
		t.Fatal(err)
	}
	assertStreamPayload(t, port, "attempt-port")

	_, err = session.DialHostBridge(t.Context(), DialHostBridgeRequest{Authority: authority, BridgeCapability: "wrong"})
	assertRPCCode(t, err, CodeUnauthorizedBridge)
	bridge, err := session.DialHostBridge(t.Context(), DialHostBridgeRequest{Authority: authority, BridgeCapability: run.BridgeCapability})
	if err != nil {
		t.Fatal(err)
	}
	assertStreamPayload(t, bridge, "host-bridge")
}

func TestSweepSerializesAgainstAttemptCreation(t *testing.T) {
	engine := newFakeEngine()
	engine.runEntered = make(chan struct{})
	engine.releaseRun = make(chan struct{})
	client, stop := startTestServer(t, engine, ServerConfig{HeartbeatTimeout: 2 * time.Second})
	defer stop()
	session, err := client.OpenSession(t.Context(), testSessionRequest())
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	requireSweep(t, session)
	engine.sweepEntered = make(chan struct{})
	runDone := make(chan error, 1)
	go func() {
		_, err := session.Run(context.Background(), testRunRequest(testAuthority(), time.Second))
		runDone <- err
	}()
	<-engine.runEntered
	sweepDone := make(chan error, 1)
	go func() {
		_, err := session.Sweep(context.Background(), SweepRequest{})
		sweepDone <- err
	}()
	select {
	case <-engine.sweepEntered:
		t.Fatal("Sweep entered the engine while Run was creating resources")
	case <-time.After(75 * time.Millisecond):
	}
	close(engine.releaseRun)
	if err := <-runDone; err != nil {
		t.Fatal(err)
	}
	select {
	case <-engine.sweepEntered:
	case <-time.After(time.Second):
		t.Fatal("Sweep did not enter after Run released the creation gate")
	}
	if err := <-sweepDone; err != nil {
		t.Fatal(err)
	}
}

func TestAllNarrowRPCsReachFakeEngineWithoutContainerdTypes(t *testing.T) {
	engine := newFakeEngine()
	client, stop := startTestServer(t, engine, ServerConfig{HeartbeatTimeout: 2 * time.Second})
	defer stop()
	session, err := client.OpenSession(t.Context(), testSessionRequest())
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	requireSweep(t, session)
	if err := session.EnsureImage(t.Context(), EnsureImageRequest{Reference: "registry.invalid/app", Digest: "sha256:digest"}, nil); err != nil {
		t.Fatal(err)
	}
	authority := testAuthority()
	if _, err := session.Run(t.Context(), testRunRequest(authority, time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := session.Signal(t.Context(), SignalRequest{Authority: authority, Signal: "TERM"}); err != nil {
		t.Fatal(err)
	}
	if err := session.Watch(t.Context(), WatchRequest{Authority: authority}, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := session.Verify(t.Context(), VerifyRequest{Scope: VerifyAttempt, Authority: &authority}); err != nil {
		t.Fatal(err)
	}
	if _, err := session.Delete(t.Context(), DeleteRequest{Authority: authority}); err != nil {
		t.Fatal(err)
	}
	if _, err := session.Sweep(t.Context(), SweepRequest{}); err != nil {
		t.Fatal(err)
	}
	if got := engine.methods(); !equalStrings(got, []string{"Sweep", "EnsureImage", "Run", "Signal", "Watch", "Verify", "Delete", "Sweep"}) {
		t.Fatalf("fake engine methods = %v", got)
	}
}

func TestSweepGateAndClosedWorkloadValidation(t *testing.T) {
	engine := newFakeEngine()
	client, stop := startTestServer(t, engine, ServerConfig{MaximumAttemptDeadman: time.Minute})
	defer stop()
	session, err := client.OpenSession(t.Context(), testSessionRequest())
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	err = session.EnsureImage(t.Context(), EnsureImageRequest{Reference: "registry.invalid/app"}, nil)
	assertRPCCode(t, err, CodeSweepRequired)
	requireSweep(t, session)
	request := testRunRequest(testAuthority(), time.Second)
	request.Workload.OperatorMounts = []OperatorMount{{NodePath: "/operator/data", ContainerPath: "/data"}}
	_, err = session.Run(t.Context(), request)
	assertRPCCode(t, err, CodeInvalidRequest)
	request = testRunRequest(testAuthority(), 2*time.Minute)
	_, err = session.Run(t.Context(), request)
	assertRPCCode(t, err, CodeInvalidRequest)
}

func TestOperatorMountValidatorResolvesAllowedRootsAndSymlinks(t *testing.T) {
	root := t.TempDir()
	allowedDirectory := filepath.Join(root, "allowed")
	if err := os.Mkdir(allowedDirectory, 0o700); err != nil {
		t.Fatal(err)
	}
	if !withinAllowedRoot(allowedDirectory, []string{root}) {
		t.Fatal("existing path under configured root was rejected")
	}
	outside := t.TempDir()
	link := filepath.Join(root, "escape")
	if err := os.Symlink(outside, link); err != nil {
		t.Fatal(err)
	}
	if withinAllowedRoot(link, []string{root}) {
		t.Fatal("symlink escaped configured operator mount root")
	}
	if withinAllowedRoot(allowedDirectory, nil) {
		t.Fatal("operator mount was accepted without configured roots")
	}
}

func TestSweepBlocksSubsequentRunUntilItCompletes(t *testing.T) {
	engine := newFakeEngine()
	client, stop := startTestServer(t, engine, ServerConfig{})
	defer stop()
	session, err := client.OpenSession(t.Context(), testSessionRequest())
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	requireSweep(t, session)
	engine.mu.Lock()
	engine.sweepEntered = make(chan struct{})
	engine.releaseSweep = make(chan struct{})
	sweepEntered := engine.sweepEntered
	releaseSweep := engine.releaseSweep
	engine.mu.Unlock()
	sweepDone := make(chan error, 1)
	go func() {
		_, sweepErr := session.Sweep(t.Context(), SweepRequest{})
		sweepDone <- sweepErr
	}()
	<-sweepEntered
	runDone := make(chan error, 1)
	go func() {
		_, runErr := session.Run(t.Context(), testRunRequest(testAuthority(), time.Second))
		runDone <- runErr
	}()
	select {
	case err := <-runDone:
		t.Fatalf("Run crossed active Sweep: %v", err)
	case <-time.After(75 * time.Millisecond):
	}
	close(releaseSweep)
	if err := <-sweepDone; err != nil {
		t.Fatal(err)
	}
	if err := <-runDone; err != nil {
		t.Fatal(err)
	}
}

func TestVerifyScopeAndUnknownMethod(t *testing.T) {
	engine := newFakeEngine()
	client, stop := startTestServer(t, engine, ServerConfig{})
	defer stop()
	session, err := client.OpenSession(t.Context(), testSessionRequest())
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	if _, err := session.Verify(t.Context(), VerifyRequest{Scope: VerifyNamespace}); err != nil {
		t.Fatal(err)
	}
	_, err = session.Verify(t.Context(), VerifyRequest{Scope: VerifyNamespace, Authority: pointerTo(testAuthority())})
	assertRPCCode(t, err, CodeInvalidRequest)
	err = session.call(t.Context(), Method("FutureRootOperation"), struct{}{}, &struct{}{})
	assertRPCCode(t, err, CodeUnsupportedOperation)
}

func TestAttemptDeadmanCancelsStartingRunAndTombstonesFence(t *testing.T) {
	engine := newFakeEngine()
	engine.runEntered = make(chan struct{})
	engine.releaseRun = make(chan struct{})
	clock := newManualClock(time.Unix(30_000, 0))
	client, stop := startTestServer(t, engine, ServerConfig{
		Clock: clock, HeartbeatTimeout: time.Hour, MaximumAttemptDeadman: time.Minute,
	})
	defer stop()
	session, err := client.OpenSession(t.Context(), testSessionRequest())
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	requireSweep(t, session)
	runDone := make(chan error, 1)
	request := testRunRequest(testAuthority(), time.Second)
	go func() {
		_, runErr := session.Run(t.Context(), request)
		runDone <- runErr
	}()
	<-engine.runEntered
	clock.Advance(time.Second)
	waitFor(t, time.Second, func() bool { return engine.attemptReapCount() == 1 }, "starting attempt deadman reap")
	close(engine.releaseRun)
	if err := <-runDone; err == nil {
		t.Fatal("expired starting Run returned Started evidence")
	}
	_, err = session.Run(t.Context(), request)
	assertRPCCode(t, err, CodeUnauthorizedAttempt)
}

func TestUndeliverableStartedEvidenceReapsAndTombstonesAttempt(t *testing.T) {
	engine := newFakeEngine()
	engine.runEntered = make(chan struct{})
	engine.releaseRun = make(chan struct{})
	client, stop := startTestServer(t, engine, ServerConfig{})
	defer stop()
	session, err := client.OpenSession(t.Context(), testSessionRequest())
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	requireSweep(t, session)
	requestContext, cancel := context.WithCancel(t.Context())
	runDone := make(chan error, 1)
	request := testRunRequest(testAuthority(), time.Minute)
	go func() {
		_, runErr := session.Run(requestContext, request)
		runDone <- runErr
	}()
	<-engine.runEntered
	cancel()
	// Let the server observe EOF before the fake engine produces Started.
	time.Sleep(25 * time.Millisecond)
	close(engine.releaseRun)
	if err := <-runDone; err == nil {
		t.Fatal("closed response connection unexpectedly delivered Started evidence")
	}
	waitFor(t, time.Second, func() bool { return engine.attemptReapCount() == 1 }, "undeliverable Started reap")
	_, err = session.Run(t.Context(), request)
	assertRPCCode(t, err, CodeUnauthorizedAttempt)
}

func TestStaleCapabilityRejectedAfterHelperRestart(t *testing.T) {
	firstEngine := newFakeEngine()
	firstClient, stopFirst := startTestServer(t, firstEngine, ServerConfig{})
	stale, err := firstClient.OpenSession(t.Context(), testSessionRequest())
	if err != nil {
		t.Fatal(err)
	}
	stopFirst()
	secondClient, stopSecond := startTestServer(t, newFakeEngine(), ServerConfig{})
	defer stopSecond()
	stale.client.Dial = secondClient.Dial
	_, err = stale.Verify(t.Context(), VerifyRequest{Scope: VerifyNamespace})
	assertRPCCode(t, err, CodeSessionStale)
	_ = stale.Close()
}

func TestSessionReapJoinsEveryOperationBeforeExclusiveReap(t *testing.T) {
	engine := newFakeEngine()
	engine.signalEntered = make(chan struct{})
	engine.releaseSignal = make(chan struct{})
	client, stop := startTestServer(t, engine, ServerConfig{})
	defer stop()
	session, err := client.OpenSession(t.Context(), testSessionRequest())
	if err != nil {
		t.Fatal(err)
	}
	requireSweep(t, session)
	if _, err := session.Run(t.Context(), testRunRequest(testAuthority(), time.Minute)); err != nil {
		t.Fatal(err)
	}
	signalDone := make(chan error, 1)
	go func() {
		signalDone <- session.Signal(t.Context(), SignalRequest{Authority: testAuthority(), Signal: SignalTERM})
	}()
	<-engine.signalEntered
	if err := session.Close(); err != nil {
		t.Fatal(err)
	}
	time.Sleep(25 * time.Millisecond)
	if engine.sessionReapCount() != 0 {
		t.Fatal("session reap raced an in-flight Signal operation")
	}
	close(engine.releaseSignal)
	<-signalDone
	waitFor(t, time.Second, func() bool { return engine.sessionReapCount() == 1 }, "joined session reap")
}

func pointerTo[T any](value T) *T { return &value }

func testAuthority() AttemptAuthority {
	return AttemptAuthority{
		NodeID: "node-1", JobID: "job-1", AttemptID: "attempt-1", FencingToken: "fence-1",
		BootSessionID: "boot-1", Class: "one-shot", RemovalGeneration: "removal-1",
	}
}

func testRunRequest(authority AttemptAuthority, deadman time.Duration) RunRequest {
	return RunRequest{
		Authority: authority, InitialDeadman: deadman,
		Workload: WorkloadInput{
			ImageDigest: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			Argv:        []string{"/bin/probe"},
		},
	}
}

func requireSweep(t *testing.T, session *Session) {
	t.Helper()
	if _, err := session.Sweep(t.Context(), SweepRequest{}); err != nil {
		t.Fatal(err)
	}
}

func testSessionRequest() AcquireSessionRequest {
	return AcquireSessionRequest{NodeID: "node-1", BootSessionID: "boot-1", ExpectedHelperChecksum: "checksum-test"}
}

func startTestServer(t *testing.T, engine Engine, config ServerConfig) (*Client, func()) {
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
	server, err := NewServer(engine, config)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- server.Serve(ctx, listener) }()
	stop := func() {
		cancel()
		_ = listener.Close()
		select {
		case err := <-done:
			server.sessionMu.Lock()
			fatalErr := server.fatalErr
			server.sessionMu.Unlock()
			if err != nil && fatalErr == nil {
				t.Errorf("helper server: %v", err)
			}
		case <-time.After(time.Second):
			t.Error("helper server did not stop")
		}
	}
	client := NewUnixClient(path, "checksum-test")
	client.disableHeartbeatPump = true
	return client, stop
}

func assertRPCCode(t *testing.T, err error, code ErrorCode) {
	t.Helper()
	var rpcErr *RPCError
	if !errors.As(err, &rpcErr) || rpcErr.Code != code {
		t.Fatalf("error = %v, want RPC code %s", err, code)
	}
}

func waitFor(t *testing.T, timeout time.Duration, condition func() bool, description string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !condition() && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if !condition() {
		t.Fatalf("timed out waiting for %s", description)
	}
}

func assertStreamPayload(t *testing.T, connection net.Conn, expected string) {
	t.Helper()
	defer connection.Close()
	if err := connection.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	bytes, err := io.ReadAll(connection)
	if err != nil {
		t.Fatal(err)
	}
	if string(bytes) != expected {
		t.Fatalf("stream payload = %q, want %q", bytes, expected)
	}
}

type fakeEngine struct {
	mu             sync.Mutex
	calls          []string
	sessionReaps   []SessionIdentity
	attemptReaps   []AttemptAuthority
	runResponse    RunResponse
	runEntered     chan struct{}
	releaseRun     chan struct{}
	sweepEntered   chan struct{}
	releaseSweep   chan struct{}
	signalEntered  chan struct{}
	releaseSignal  chan struct{}
	sessionReapErr error
}

func newFakeEngine() *fakeEngine { return &fakeEngine{runResponse: RunResponse{Started: true}} }

func (engine *fakeEngine) setRunResponse(response RunResponse) {
	engine.mu.Lock()
	engine.runResponse = response
	engine.mu.Unlock()
}

func (engine *fakeEngine) record(method string) {
	engine.mu.Lock()
	engine.calls = append(engine.calls, method)
	engine.mu.Unlock()
}

func (engine *fakeEngine) EnsureImage(_ context.Context, _ EnsureImageRequest, emit func(EnsureImageEvent) error) error {
	engine.record("EnsureImage")
	return emit(EnsureImageEvent{Kind: ImageComplete, Result: &EnsureImageResponse{TopLevelDigest: "sha256:top", PlatformDigest: "sha256:platform"}})
}
func (engine *fakeEngine) Run(context.Context, RunRequest) (RunResponse, error) {
	engine.record("Run")
	engine.mu.Lock()
	runEntered := engine.runEntered
	releaseRun := engine.releaseRun
	response := engine.runResponse
	engine.mu.Unlock()
	if runEntered != nil {
		close(runEntered)
		<-releaseRun
	}
	return response, nil
}
func (engine *fakeEngine) Signal(context.Context, SignalRequest) error {
	engine.record("Signal")
	engine.mu.Lock()
	signalEntered := engine.signalEntered
	releaseSignal := engine.releaseSignal
	engine.mu.Unlock()
	if signalEntered != nil {
		close(signalEntered)
		<-releaseSignal
	}
	return nil
}
func (engine *fakeEngine) Watch(_ context.Context, _ WatchRequest, emit func(WatchEvent) error) error {
	engine.record("Watch")
	exitCode := 0
	return emit(WatchEvent{Kind: WatchComplete, Result: &WatchResponse{ExitCode: &exitCode}})
}
func (engine *fakeEngine) Delete(context.Context, DeleteRequest) (DeleteResponse, error) {
	engine.record("Delete")
	return DeleteResponse{Deleted: true}, nil
}
func (engine *fakeEngine) Verify(context.Context, VerifyRequest) (VerifyResponse, error) {
	engine.record("Verify")
	return VerifyResponse{Absent: true}, nil
}
func (engine *fakeEngine) Sweep(context.Context, SweepRequest) (SweepResponse, error) {
	engine.mu.Lock()
	sweepEntered := engine.sweepEntered
	releaseSweep := engine.releaseSweep
	engine.mu.Unlock()
	if sweepEntered != nil {
		close(sweepEntered)
	}
	if releaseSweep != nil {
		<-releaseSweep
	}
	engine.record("Sweep")
	return SweepResponse{Removed: 1}, nil
}
func (engine *fakeEngine) DialAttemptPort(_ context.Context, _ DialAttemptPortRequest, stream io.ReadWriteCloser) error {
	engine.record("DialAttemptPort")
	_, err := io.WriteString(stream, "attempt-port")
	return err
}
func (engine *fakeEngine) DialHostBridge(_ context.Context, _ DialHostBridgeRequest, stream io.ReadWriteCloser) error {
	engine.record("DialHostBridge")
	_, err := io.WriteString(stream, "host-bridge")
	return err
}
func (engine *fakeEngine) ReapAttempt(_ context.Context, authority AttemptAuthority) error {
	engine.mu.Lock()
	engine.attemptReaps = append(engine.attemptReaps, authority)
	engine.mu.Unlock()
	return nil
}
func (engine *fakeEngine) ReapSession(_ context.Context, identity SessionIdentity) error {
	engine.mu.Lock()
	engine.sessionReaps = append(engine.sessionReaps, identity)
	engine.mu.Unlock()
	return engine.sessionReapErr
}
func (engine *fakeEngine) methods() []string {
	engine.mu.Lock()
	defer engine.mu.Unlock()
	return append([]string(nil), engine.calls...)
}
func (engine *fakeEngine) sessionReapCount() int {
	engine.mu.Lock()
	defer engine.mu.Unlock()
	return len(engine.sessionReaps)
}
func (engine *fakeEngine) attemptReapCount() int {
	engine.mu.Lock()
	defer engine.mu.Unlock()
	return len(engine.attemptReaps)
}

func equalStrings(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

type manualClock struct {
	mu     sync.Mutex
	now    time.Time
	timers map[*manualTimer]struct{}
}

type manualTimer struct {
	clock    *manualClock
	channel  chan time.Time
	deadline time.Time
	active   bool
}

func newManualClock(now time.Time) *manualClock {
	return &manualClock{now: now, timers: make(map[*manualTimer]struct{})}
}

func (clock *manualClock) Now() time.Time {
	clock.mu.Lock()
	defer clock.mu.Unlock()
	return clock.now
}

func (clock *manualClock) NewTimerAt(deadline time.Time) Timer {
	clock.mu.Lock()
	defer clock.mu.Unlock()
	timer := &manualTimer{
		clock: clock, channel: make(chan time.Time, 1), deadline: deadline, active: true,
	}
	clock.timers[timer] = struct{}{}
	clock.fireLocked(timer)
	return timer
}

func (clock *manualClock) Advance(duration time.Duration) {
	clock.mu.Lock()
	clock.now = clock.now.Add(duration)
	for timer := range clock.timers {
		clock.fireLocked(timer)
	}
	clock.mu.Unlock()
}

func (clock *manualClock) fireLocked(timer *manualTimer) {
	if !timer.active || timer.deadline.After(clock.now) {
		return
	}
	timer.active = false
	select {
	case timer.channel <- clock.now:
	default:
	}
}

func (timer *manualTimer) C() <-chan time.Time { return timer.channel }

func (timer *manualTimer) Stop() bool {
	timer.clock.mu.Lock()
	defer timer.clock.mu.Unlock()
	wasActive := timer.active
	timer.active = false
	return wasActive
}

func (timer *manualTimer) ResetAt(deadline time.Time) bool {
	timer.clock.mu.Lock()
	defer timer.clock.mu.Unlock()
	wasActive := timer.active
	timer.deadline = deadline
	timer.active = true
	timer.clock.fireLocked(timer)
	return wasActive
}
