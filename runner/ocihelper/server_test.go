package ocihelper

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
)

var testImagePlatform = OCIPlatform{OS: "linux", Architecture: "amd64"}

func TestDeterministicResourceIdentityCarriesCompleteAuthority(t *testing.T) {
	authority := testAuthority()
	authority.Class = contract.JobClassService
	first, err := DeterministicResourceIdentity(authority)
	if err != nil {
		t.Fatal(err)
	}
	second, err := DeterministicResourceIdentity(authority)
	if err != nil {
		t.Fatal(err)
	}
	if first.LeaseID != second.LeaseID || first.ContainerID != second.ContainerID || first.TaskID != second.TaskID ||
		first.SnapshotID != second.SnapshotID || first.ShimID != second.ShimID || first.CgroupID != second.CgroupID ||
		first.LogSegmentDirectory != second.LogSegmentDirectory || first.HandoffVolumeDirectory != second.HandoffVolumeDirectory ||
		first.ServiceVolumeDirectory != second.ServiceVolumeDirectory || first.ServiceVolumeOwnerRecord != second.ServiceVolumeOwnerRecord {
		t.Fatalf("deterministic identity changed: first=%+v second=%+v", first, second)
	}
	if len(first.Labels) != 7 || first.Labels["io.wefty/fencing_token"] != authority.FencingToken || first.Labels["io.wefty/removal_generation"] != authority.RemovalGeneration {
		t.Fatalf("resource labels do not carry complete authority: %#v", first.Labels)
	}
	if first.TaskID != first.ContainerID || first.ShimID != first.ContainerID {
		t.Fatalf("containerd task and shim identities must use container ID: %+v", first)
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
	if different.ServiceVolumeDirectory != first.ServiceVolumeDirectory || different.ServiceVolumeOwnerRecord != first.ServiceVolumeOwnerRecord {
		t.Fatal("a new attempt changed the stable job-owned service volume identity")
	}
	changed.JobID = "job-2"
	differentJob, err := DeterministicResourceIdentity(changed)
	if err != nil {
		t.Fatal(err)
	}
	if differentJob.ServiceVolumeDirectory == first.ServiceVolumeDirectory || strings.Contains(first.ServiceVolumeDirectory, authority.JobID) {
		t.Fatal("service volume identity is not stable, opaque, and job-scoped")
	}
}

func TestProcessAttemptIdentityDoesNotRequireServiceVolumeJobID(t *testing.T) {
	authority := testAuthority()
	authority.JobID = strings.Repeat("x", 256)
	identity, err := DeterministicResourceIdentity(authority)
	if err != nil {
		t.Fatalf("process attempt identity inherited service volume validation: %v", err)
	}
	if identity.ServiceVolumeDirectory != "" || identity.ServiceVolumeOwnerRecord != "" {
		t.Fatalf("process attempt received service identities: %+v", identity)
	}
	authority.Class = contract.JobClassService
	if _, err := DeterministicResourceIdentity(authority); err == nil {
		t.Fatal("service identity accepted an overlong stable job ID")
	}
}

func TestHandoffVolumeIdentityUsesStableOpaqueOwner(t *testing.T) {
	first, err := DeterministicHandoffVolumeDirectory("run-source")
	if err != nil {
		t.Fatal(err)
	}
	second, err := DeterministicHandoffVolumeDirectory("run-source")
	if err != nil {
		t.Fatal(err)
	}
	different, err := DeterministicHandoffVolumeDirectory("run-other")
	if err != nil {
		t.Fatal(err)
	}
	if first != second || first == different || strings.Contains(first, "run-source") {
		t.Fatalf("stable opaque handoff identities = first %q second %q different %q", first, second, different)
	}
}

func TestEngineReceivesHelperDerivedResourcePlanBeforeRun(t *testing.T) {
	engine := newFakeEngine()
	client, stop := startTestServer(t, engine, ServerConfig{})
	defer stop()
	session, err := client.OpenSession(t.Context(), testSessionRequest())
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	requireSweep(t, session)
	authority := testAuthority()
	if _, err := session.Run(t.Context(), testRunRequest(authority, time.Second)); err != nil {
		t.Fatal(err)
	}
	engine.mu.Lock()
	request := engine.lastRunRequest
	engine.mu.Unlock()
	want, err := DeterministicResourceIdentity(authority)
	if err != nil {
		t.Fatal(err)
	}
	if request.Resources.LeaseID != want.LeaseID || request.Resources.SnapshotID != want.SnapshotID ||
		request.Resources.ContainerID != want.ContainerID || request.Resources.TaskID != want.TaskID ||
		request.Resources.ShimID != want.ShimID || request.Resources.CgroupID != want.CgroupID ||
		request.Resources.LogSegmentDirectory != want.LogSegmentDirectory ||
		request.Resources.HandoffVolumeDirectory != want.HandoffVolumeDirectory ||
		request.Resources.ServiceVolumeDirectory != want.ServiceVolumeDirectory ||
		len(request.Resources.Labels) != len(want.Labels) {
		t.Fatalf("engine resource plan = %#v, want %#v", request.Resources, want)
	}
	for label, value := range want.Labels {
		if request.Resources.Labels[label] != value {
			t.Fatalf("engine resource label %q = %q, want %q", label, request.Resources.Labels[label], value)
		}
	}
}

func TestEngineReceivesOwnerKeyedHandoffAndFinalizesItSeparately(t *testing.T) {
	engine := newFakeEngine()
	client, stop := startTestServer(t, engine, ServerConfig{})
	defer stop()
	session, err := client.OpenSession(t.Context(), testSessionRequest())
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	requireSweep(t, session)
	request := testRunRequest(testAuthority(), time.Second)
	request.Workload.ManagedVolumes = []ManagedVolumeDescriptor{{Kind: ManagedVolumeHandoff, OwnerKey: "run-source"}}
	if _, err := session.Run(t.Context(), request); err != nil {
		t.Fatal(err)
	}
	engine.mu.Lock()
	got := engine.lastRunRequest.Resources.HandoffVolumeDirectory
	engine.mu.Unlock()
	want, err := DeterministicHandoffVolumeDirectory("run-source")
	if err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("engine handoff identity = %q, want stable owner identity %q", got, want)
	}
	deleted, err := session.DeleteManagedVolume(t.Context(), DeleteManagedVolumeRequest{Kind: ManagedVolumeHandoff, OwnerKey: "run-source"})
	if err != nil || !deleted.Deleted {
		t.Fatalf("managed handoff deletion = %+v err=%v", deleted, err)
	}
}

func TestAttemptOutsideSessionIsDistinctFromNonLiveAttempt(t *testing.T) {
	engine := newFakeEngine()
	client, stop := startTestServer(t, engine, ServerConfig{})
	defer stop()
	session, err := client.OpenSession(t.Context(), testSessionRequest())
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	requireSweep(t, session)
	outside := testAuthority()
	outside.BootSessionID = "different-boot"
	_, err = session.Run(t.Context(), testRunRequest(outside, time.Second))
	assertRPCCode(t, err, CodeAttemptOutsideSession)

	authority := testAuthority()
	if _, err := session.Run(t.Context(), testRunRequest(authority, time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := session.Delete(t.Context(), DeleteRequest{Authority: authority}); err != nil {
		t.Fatal(err)
	}
	_, err = session.Delete(t.Context(), DeleteRequest{Authority: authority})
	assertRPCCode(t, err, CodeUnauthorizedAttempt)
}

func TestBootBarrierSweepsVerifiesAndRetriesExclusiveTakeover(t *testing.T) {
	engine := newFakeEngine()
	client, stop := startTestServer(t, engine, ServerConfig{HeartbeatTimeout: time.Second, ReapTimeout: 2 * time.Second})
	defer stop()
	incumbent, err := client.OpenSession(t.Context(), testSessionRequest())
	if err != nil {
		t.Fatal(err)
	}
	if got := incumbent.Handshake().ReapTimeout; got != 2*time.Second {
		t.Fatalf("advertised reap timeout = %s, want 2s", got)
	}
	incumbentGeneration := incumbent.Handshake().SessionGeneration
	requireSweep(t, incumbent)

	clock := &observedClock{manualClock: newManualClock(time.Unix(1_000, 0)), timerCreated: make(chan struct{}, 8)}
	barrier, err := NewBootBarrierWithConfig(client, testSessionRequest(), BootBarrierConfig{Clock: clock})
	if err != nil {
		t.Fatal(err)
	}
	defer barrier.Close()
	if barrier.Ready() {
		t.Fatal("unprepared barrier reported ready")
	}
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- barrier.Ensure(ctx) }()
	<-clock.timerCreated
	select {
	case err := <-done:
		t.Fatalf("exclusive takeover crossed live incumbent: %v", err)
	default:
	}
	readyRead := make(chan bool, 1)
	go func() { readyRead <- barrier.Ready() }()
	select {
	case ready := <-readyRead:
		if ready {
			t.Fatal("barrier reported ready during takeover")
		}
	case <-time.After(75 * time.Millisecond):
		t.Fatal("Ready blocked behind takeover retry")
	}
	if err := incumbent.Close(); err != nil {
		t.Fatal(err)
	}
	clock.Advance(defaultTakeoverRetryInterval)
	for {
		select {
		case err := <-done:
			if err != nil {
				t.Fatal(err)
			}
			goto takeoverComplete
		case <-clock.timerCreated:
			clock.Advance(defaultTakeoverRetryInterval)
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
takeoverComplete:
	if !barrier.Ready() {
		t.Fatal("successful sweep and verification did not make barrier ready")
	}
	receipt, ok := barrier.SweepReceipt()
	if !ok || receipt.SweepEpoch == "" || receipt.HelperSession.HelperInstanceID == "" || receipt.HelperSession.SessionGeneration == 0 {
		t.Fatalf("verified sweep receipt = %#v, ok=%t", receipt, ok)
	}
	if receipt.HelperSession.SessionGeneration <= incumbentGeneration {
		t.Fatalf("takeover generation = %d, want newer than %d", receipt.HelperSession.SessionGeneration, incumbentGeneration)
	}
	receipt.PriorBootSessionsSeen = append(receipt.PriorBootSessionsSeen, "mutated")
	retained, ok := barrier.SweepReceipt()
	if !ok || len(retained.PriorBootSessionsSeen) != 0 {
		t.Fatalf("barrier receipt was mutable through caller copy: %#v", retained)
	}
	before := engine.methods()
	if err := barrier.Ensure(ctx); err != nil {
		t.Fatal(err)
	}
	if got := engine.methods(); !equalStrings(got, before) {
		t.Fatalf("idempotent barrier repeated engine operations: before=%v after=%v", before, got)
	}
}

func TestBootBarrierRefusesResidueAndNeverExposesSession(t *testing.T) {
	engine := newFakeEngine()
	engine.verifyResponses = []VerifyResponse{{Absent: true}, {Absent: false}}
	client, stop := startTestServer(t, engine, ServerConfig{})
	defer stop()
	barrier, err := NewBootBarrier(client, testSessionRequest())
	if err != nil {
		t.Fatal(err)
	}
	if err := barrier.Ensure(t.Context()); err == nil {
		t.Fatal("barrier accepted residue after sweep")
	}
	if barrier.Ready() {
		t.Fatal("failed verification made barrier ready")
	}
	if _, err := barrier.Session(); err == nil {
		t.Fatal("failed verification exposed a helper session")
	}
}

func TestBootBarrierInspectsIndependentNamespaceInventory(t *testing.T) {
	engine := newFakeEngine()
	engine.verifyResponses = []VerifyResponse{{Absent: true}, {
		Absent: true, Inventory: ResourceInventory{Leases: []string{"wefty-lease-unlabelled"}},
	}}
	client, stop := startTestServer(t, engine, ServerConfig{})
	defer stop()
	barrier, err := NewBootBarrier(client, testSessionRequest())
	if err != nil {
		t.Fatal(err)
	}
	if err := barrier.Ensure(t.Context()); err == nil {
		t.Fatal("barrier trusted absent=true despite independently inventoried residue")
	}
}

func TestHelperRestartSweepsAndVerifiesBeforeAcceptingSession(t *testing.T) {
	engine := newFakeEngine()
	engine.sweepEntered = make(chan struct{})
	engine.releaseSweep = make(chan struct{})
	client, stop := startTestServer(t, engine, ServerConfig{})
	defer stop()
	opened := make(chan error, 1)
	go func() {
		session, err := client.OpenSession(t.Context(), testSessionRequest())
		if session != nil {
			_ = session.Close()
		}
		opened <- err
	}()
	<-engine.sweepEntered
	select {
	case err := <-opened:
		t.Fatalf("helper accepted a session before startup sweep completed: %v", err)
	case <-time.After(75 * time.Millisecond):
	}
	close(engine.releaseSweep)
	if err := <-opened; err != nil {
		t.Fatal(err)
	}
	if got := engine.methods(); !equalStrings(got, []string{"Sweep", "Verify"}) {
		t.Fatalf("helper startup barrier operations = %v", got)
	}
}

func TestHelperStartupResidueFailsServeBeforeSessionAuthority(t *testing.T) {
	engine := newFakeEngine()
	engine.verifyResponses = []VerifyResponse{{Absent: false}}
	server, err := NewServer(engine, ServerConfig{
		HelperChecksum: "checksum-test", AllowedUIDs: []uint32{uint32(os.Getuid())},
	})
	if err != nil {
		t.Fatal(err)
	}
	directory, err := os.MkdirTemp("", "wefty-oci-startup-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(directory) })
	listener, err := net.Listen("unix", filepath.Join(directory, "helper.sock"))
	if err != nil {
		t.Fatal(err)
	}
	err = server.Serve(t.Context(), listener)
	if err == nil || err.Error() != "startup verify OCI runtime namespace: residue remains after sweep" {
		t.Fatalf("helper startup error = %v", err)
	}
	if got := engine.methods(); !equalStrings(got, []string{"Sweep", "Verify"}) {
		t.Fatalf("failed helper startup operations = %v", got)
	}
}

func TestCrashAtEveryCreateBoundaryIsSweptBeforeReusedBootSession(t *testing.T) {
	for boundary := 1; boundary <= 10; boundary++ {
		t.Run(fmt.Sprintf("boundary-%d", boundary), func(t *testing.T) {
			engine := newCrashBoundaryEngine(boundary)
			client, stop := startTestServer(t, engine, ServerConfig{})
			barrier, err := NewBootBarrier(client, testSessionRequest())
			if err != nil {
				t.Fatal(err)
			}
			if err := barrier.Ensure(t.Context()); err != nil {
				t.Fatal(err)
			}
			session, err := barrier.Session()
			if err != nil {
				t.Fatal(err)
			}
			_, err = session.Run(t.Context(), testRunRequest(testAuthority(), time.Second))
			if err == nil {
				t.Fatal("simulated helper crash returned Started evidence")
			}
			var rpcErr *RPCError
			if errors.As(err, &rpcErr) && rpcErr.Code != CodeEngineFailure {
				t.Fatalf("crashed helper Run error = %v", err)
			}
			if engine.residueCount() != boundary {
				t.Fatalf("crash residue count = %d, want %d", engine.residueCount(), boundary)
			}
			_ = barrier.Close()
			stop()

			engine = engine.restartAfterCrash()
			restartedClient, stopRestarted := startTestServer(t, engine, ServerConfig{})
			defer stopRestarted()
			restarted, err := NewBootBarrier(restartedClient, testSessionRequest())
			if err != nil {
				t.Fatal(err)
			}
			defer restarted.Close()
			if err := restarted.Ensure(t.Context()); err != nil {
				t.Fatal(err)
			}
			receipt, ok := restarted.SweepReceipt()
			if !ok || len(receipt.PriorBootSessionsSeen) != 1 || len(receipt.Attempts) != 1 ||
				receipt.Attempts[0].AttemptID != testAuthority().AttemptID || InventoryEmpty(receipt.SweptInventory) {
				t.Fatalf("restarted barrier receipt = %#v, ok=%t", receipt, ok)
			}
			if engine.residueCount() != 0 {
				t.Fatalf("reused boot session retained %d stale resources", engine.residueCount())
			}
			restartedSession, err := restarted.Session()
			if err != nil {
				t.Fatal(err)
			}
			if _, err := restartedSession.Run(t.Context(), testRunRequest(testAuthority(), time.Second)); err != nil {
				t.Fatal(err)
			}
			if engine.duplicateStarts() != 0 {
				t.Fatalf("reused boot adopted or overlapped %d survivor executions", engine.duplicateStarts())
			}
		})
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

func TestRuntimeSpecRejectionSurvivesHelperAndAgentMapping(t *testing.T) {
	engine := newFakeEngine()
	engine.runErr = &RuntimeSpecRejectionError{err: errors.New("invalid translated mount")}
	client, stop := startTestServer(t, engine, ServerConfig{})
	defer stop()
	session, err := client.OpenSession(t.Context(), testSessionRequest())
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	requireSweep(t, session)
	_, err = session.Run(t.Context(), testRunRequest(testAuthority(), time.Second))
	assertRPCCode(t, err, CodeOCISpecRejected)
	failure := SpawnFailureForRunError(err)
	if failure == nil || failure.Code != contract.SpawnFailureOCISpecRejected {
		t.Fatalf("agent spawn failure = %#v", failure)
	}
}

func TestServiceDataOwnerRejectionSurvivesHelperAndAgentMapping(t *testing.T) {
	engine := newFakeEngine()
	engine.runErr = &ServiceDataRejectionError{Reason: "directory owner mismatch", ActualUID: 12001, ActualGID: 12002, WantedUID: 13001, WantedGID: 13002}
	client, stop := startTestServer(t, engine, ServerConfig{})
	defer stop()
	session, err := client.OpenSession(t.Context(), testSessionRequest())
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	requireSweep(t, session)
	_, err = session.Run(t.Context(), testRunRequest(testAuthority(), time.Second))
	assertRPCCode(t, err, CodeOCISpecRejected)
	if !strings.Contains(err.Error(), "actual owner 12001:12002, wanted 13001:13002") {
		t.Fatalf("service data rejection lost owner diagnostic: %v", err)
	}
	failure := SpawnFailureForRunError(err)
	if failure == nil || failure.Code != contract.SpawnFailureOCISpecRejected || !strings.Contains(failure.Message, "wanted 13001:13002") {
		t.Fatalf("agent spawn failure = %#v", failure)
	}
}

func TestInsufficientDiskSurvivesHelperAndAgentMapping(t *testing.T) {
	engine := newFakeEngine()
	engine.runErr = &insufficientDiskError{RequestedBytes: 8 << 30, ObservedAvailableBytes: 2 << 30, err: syscall.ENOSPC}
	client, stop := startTestServer(t, engine, ServerConfig{})
	defer stop()
	session, err := client.OpenSession(t.Context(), testSessionRequest())
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	requireSweep(t, session)
	_, err = session.Run(t.Context(), testRunRequest(testAuthority(), time.Second))
	assertRPCCode(t, err, CodeInsufficientDisk)
	failure := SpawnFailureForRunError(err)
	if failure == nil || failure.Code != contract.SpawnFailureInsufficientDisk || failure.RequestedBytes != 8<<30 || failure.ObservedAvailableBytes != 2<<30 {
		t.Fatalf("insufficient disk spawn failure = %#v", failure)
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

func TestClientRequiresChecksumBeforeDial(t *testing.T) {
	dialed := false
	client := &Client{Dial: func(context.Context) (net.Conn, error) {
		dialed = true
		return nil, errors.New("unexpected dial")
	}}
	_, err := client.OpenSession(t.Context(), testSessionRequest())
	if err == nil || !strings.Contains(err.Error(), "checksum verification is required") {
		t.Fatalf("OpenSession error = %v, want checksum refusal", err)
	}
	if dialed {
		t.Fatal("client dialed helper before requiring a checksum")
	}
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
	err = second.EnsureImage(t.Context(), EnsureImageRequest{Reference: "example.invalid/probe", Digest: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", Platform: testImagePlatform}, nil)
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

func TestAttemptDeadmanUsesGuardianReaper(t *testing.T) {
	engine := &guardianRecordingEngine{fakeEngine: newFakeEngine()}
	clock := newManualClock(time.Unix(25_000, 0))
	client, stop := startTestServer(t, engine, ServerConfig{Clock: clock, HeartbeatTimeout: time.Hour, MaximumAttemptDeadman: time.Minute})
	defer stop()
	session, err := client.OpenSession(t.Context(), testSessionRequest())
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	requireSweep(t, session)
	if _, err := session.Run(t.Context(), testRunRequest(testAuthority(), time.Second)); err != nil {
		t.Fatal(err)
	}
	clock.Advance(time.Second)
	waitFor(t, time.Second, func() bool { return engine.guardianCount() == 1 }, "guardian deadman reap")
}

func TestAttemptPortAndMacBridgeRequireExactAttemptCapabilities(t *testing.T) {
	engine := newFakeEngine()
	engine.setRunResponse(RunResponse{Started: true, Endpoints: map[string]uint16{"service": 42001}, HostBridgeReady: true})
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
	request.AllocateEndpoints = []string{"service"}
	request.EnableHostBridgeFallback = true
	run, err := session.Run(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	if run.BridgeCapability == "" {
		t.Fatal("authorized Mac fallback did not receive an opaque bridge capability")
	}
	_, err = session.DialAttemptPort(t.Context(), DialAttemptPortRequest{Authority: authority, Name: "missing"})
	assertRPCCode(t, err, CodeUnauthorizedPort)
	dialContext, cancelDial := context.WithCancel(t.Context())
	port, err := session.DialAttemptPort(dialContext, DialAttemptPortRequest{Authority: authority, Name: "service"})
	if err != nil {
		t.Fatal(err)
	}
	cancelDial()
	assertStreamPayload(t, port, "attempt-port")

	_, err = session.DialHostBridge(t.Context(), DialHostBridgeRequest{Authority: authority, BridgeCapability: "wrong"})
	assertRPCCode(t, err, CodeUnauthorizedBridge)
	bridge, err := session.DialHostBridge(t.Context(), DialHostBridgeRequest{Authority: authority, BridgeCapability: run.BridgeCapability})
	if err != nil {
		t.Fatal(err)
	}
	assertStreamPayload(t, bridge, "host-bridge")
}

func TestNamedEndpointAuthorizationResolvesOnlyTheRequestedName(t *testing.T) {
	engine := newFakeEngine()
	engine.runResponse = RunResponse{Started: true, Endpoints: map[string]uint16{"view": 42011, "control": 42012}}
	client, stop := startTestServer(t, engine, ServerConfig{HeartbeatTimeout: 2 * time.Second})
	defer stop()
	session, err := client.OpenSession(t.Context(), testSessionRequest())
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	requireSweep(t, session)
	request := testRunRequest(testAuthority(), time.Second)
	request.AllocateEndpoints = []string{"view", "control"}
	if _, err := session.Run(t.Context(), request); err != nil {
		t.Fatal(err)
	}
	stream, err := session.DialAttemptPort(t.Context(), DialAttemptPortRequest{Authority: request.Authority, Name: "control"})
	if err != nil {
		t.Fatal(err)
	}
	assertStreamPayload(t, stream, "attempt-port")
	engine.mu.Lock()
	dial := engine.lastDialAttemptRequest
	engine.mu.Unlock()
	if dial.Name != "control" || dial.Port != 42012 || dial.CgroupID == "" {
		t.Fatalf("engine dial request = %+v", dial)
	}
}

func TestAttemptPortMarkerFailureInvalidatesSession(t *testing.T) {
	engine := newFakeEngine()
	engine.runResponse = RunResponse{Started: true, Endpoints: map[string]uint16{"service": 42001}}
	engine.dialAttemptWithoutMarker = true
	client, stop := startTestServer(t, engine, ServerConfig{HeartbeatTimeout: 2 * time.Second})
	defer stop()
	session, err := client.OpenSession(t.Context(), testSessionRequest())
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	requireSweep(t, session)
	request := testRunRequest(testAuthority(), time.Second)
	request.AllocateEndpoints = []string{"service"}
	if _, err := session.Run(t.Context(), request); err != nil {
		t.Fatal(err)
	}
	if _, err := session.DialAttemptPort(t.Context(), DialAttemptPortRequest{Authority: request.Authority, Name: "service"}); err == nil {
		t.Fatal("attempt-port marker EOF returned nil error")
	}
	if err := session.HealthError(); err == nil {
		t.Fatal("attempt-port marker transport failure did not invalidate the session")
	}
}

func TestHostBridgeStreamRemainsCoupledToDialContext(t *testing.T) {
	engine := newFakeEngine()
	engine.runResponse = RunResponse{Started: true, HostBridgeReady: true}
	engine.dialHostBridgeRead = true
	engine.dialHostBridgeDone = make(chan struct{})
	client, stop := startTestServer(t, engine, ServerConfig{HeartbeatTimeout: 2 * time.Second})
	defer stop()
	session, err := client.OpenSession(t.Context(), testSessionRequest())
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	requireSweep(t, session)
	request := testRunRequest(testAuthority(), time.Second)
	request.EnableHostBridgeFallback = true
	run, err := session.Run(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	dialContext, cancel := context.WithCancel(t.Context())
	stream, err := session.DialHostBridge(dialContext, DialHostBridgeRequest{Authority: request.Authority, BridgeCapability: run.BridgeCapability})
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	defer stream.Close()
	select {
	case <-engine.dialHostBridgeDone:
	case <-time.After(time.Second):
		t.Fatal("DialHostBridge cancellation did not clean up the engine stream")
	}
}

func TestAttemptPortSetupCancellationReturnsTypedError(t *testing.T) {
	client, server := net.Pipe()
	ctx, cancel := context.WithCancel(t.Context())
	operation := &clientOperationConn{Conn: client}
	operation.stop = context.AfterFunc(ctx, func() { _ = client.Close() })
	cancel()
	_ = server.SetReadDeadline(time.Now().Add(time.Second))
	var buffer [1]byte
	_, _ = server.Read(buffer[:])
	defer server.Close()
	err := operation.detachSetupContext(ctx)
	var cancelled *StreamSetupCancelledError
	if !errors.As(err, &cancelled) || !errors.Is(err, context.Canceled) {
		t.Fatalf("setup cancellation error = %T %v", err, err)
	}
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
	if err := session.EnsureImage(t.Context(), EnsureImageRequest{Reference: "registry.invalid/app", Digest: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", Platform: testImagePlatform}, nil); err != nil {
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
	if got := engine.methods(); !equalStrings(got, []string{
		"Sweep", "Verify", // helper restart barrier
		"Sweep", "Verify", // exclusive agent-session barrier
		"EnsureImage", "Run", "Signal", "Watch", "Verify", "Delete", "Sweep",
	}) {
		t.Fatalf("fake engine methods = %v", got)
	}
}

func TestImportImageStreamsArchiveAfterAuthorization(t *testing.T) {
	engine := &archiveCaptureEngine{fakeEngine: newFakeEngine()}
	client, stop := startTestServer(t, engine, ServerConfig{})
	defer stop()
	session, err := client.OpenSession(t.Context(), testSessionRequest())
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	requireSweep(t, session)
	payload := []byte("opaque-oci-archive")
	var result EnsureImageResponse
	err = session.ImportImage(t.Context(), EnsureImageRequest{
		Reference: "registry.invalid/app", Platform: testImagePlatform, Source: ImageSourceArchive, OperationTimeout: time.Minute,
	}, bytes.NewReader(payload), func(event EnsureImageEvent) error {
		if event.Result != nil {
			result = *event.Result
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(engine.archive, payload) || result.TopLevelDigest == "" || result.PlatformDigest == "" {
		t.Fatalf("archive=%q result=%+v", engine.archive, result)
	}
}

func TestImportImageClosesBlockedUploadSourceWhenHelperRejects(t *testing.T) {
	reader := newBlockedArchiveReader()
	engine := &rejectBlockedArchiveEngine{fakeEngine: newFakeEngine(), entered: reader.entered}
	client, stop := startTestServer(t, engine, ServerConfig{})
	defer stop()
	session, err := client.OpenSession(t.Context(), testSessionRequest())
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	requireSweep(t, session)
	err = session.ImportImage(t.Context(), EnsureImageRequest{Reference: "registry.invalid/app", Platform: testImagePlatform, OperationTimeout: time.Minute}, reader, nil)
	if err == nil {
		t.Fatal("helper rejection unexpectedly succeeded")
	}
	select {
	case <-reader.closed:
	case <-time.After(time.Second):
		t.Fatal("helper rejection did not close and unblock the archive source")
	}
}

func TestImportImageOperationTimeoutBoundsBlockedSpoolRead(t *testing.T) {
	reader := newBlockedArchiveReader()
	engine := &archiveCaptureEngine{fakeEngine: newFakeEngine()}
	client, stop := startTestServer(t, engine, ServerConfig{})
	defer stop()
	session, err := client.OpenSession(t.Context(), testSessionRequest())
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	requireSweep(t, session)
	started := time.Now()
	err = session.ImportImage(t.Context(), EnsureImageRequest{Reference: "registry.invalid/app", Platform: testImagePlatform, OperationTimeout: 20 * time.Millisecond}, reader, nil)
	if err == nil || time.Since(started) > time.Second {
		t.Fatalf("blocked spool deadline = (%v, %s)", err, time.Since(started))
	}
	select {
	case <-reader.closed:
	case <-time.After(time.Second):
		t.Fatal("spool deadline did not close the blocked upload source")
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
	err = session.EnsureImage(t.Context(), EnsureImageRequest{Reference: "registry.invalid/app", Platform: testImagePlatform}, nil)
	assertRPCCode(t, err, CodeSweepRequired)
	requireSweep(t, session)
	request := testRunRequest(testAuthority(), time.Second)
	request.Workload.OperatorMounts = []OperatorMount{{NodePath: "relative", ContainerPath: "/data"}}
	_, err = session.Run(t.Context(), request)
	assertRPCCode(t, err, CodeInvalidRequest)
	request = testRunRequest(testAuthority(), 2*time.Minute)
	_, err = session.Run(t.Context(), request)
	assertRPCCode(t, err, CodeInvalidRequest)
}

func TestEnsureImageRejectsEmbeddedDigestReference(t *testing.T) {
	engine := newFakeEngine()
	client, stop := startTestServer(t, engine, ServerConfig{})
	defer stop()
	session, err := client.OpenSession(t.Context(), testSessionRequest())
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	requireSweep(t, session)
	err = session.EnsureImage(t.Context(), EnsureImageRequest{
		Reference: "registry.invalid/app@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		Digest:    "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		Platform:  testImagePlatform,
	}, nil)
	assertRPCCode(t, err, CodeInvalidRequest)
}

func TestOperatorMountValidatorRejectsSymlinksAndNonFilesystemTrees(t *testing.T) {
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	allowedDirectory := filepath.Join(root, "allowed")
	if err := os.Mkdir(allowedDirectory, 0o700); err != nil {
		t.Fatal(err)
	}
	if !withinAllowedRoot(allowedDirectory, []string{root}) {
		t.Fatal("existing path under configured root was rejected")
	}
	outside, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
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
	insideLink := filepath.Join(root, "inside-link")
	if err := os.Symlink(allowedDirectory, insideLink); err != nil {
		t.Fatal(err)
	}
	if withinAllowedRoot(insideLink, []string{root}) {
		t.Fatal("symlink within the configured root was accepted")
	}
	if withinAllowedRoot(root, []string{root}) {
		t.Fatal("the configured root itself was accepted as a delegated subtree")
	}
	if withinAllowedRoot("/dev/null", []string{"/dev"}) {
		t.Fatal("device node was accepted as an operator mount source")
	}
}

func TestRunDefersOperatorMountFilesystemValidationUntilGuestTranslation(t *testing.T) {
	engine := newFakeEngine()
	client, stop := startTestServer(t, engine, ServerConfig{AllowedMountRoots: []string{t.TempDir()}})
	defer stop()
	session, err := client.OpenSession(t.Context(), testSessionRequest())
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	requireSweep(t, session)
	request := testRunRequest(testAuthority(), time.Second)
	request.Workload.OperatorMounts = []OperatorMount{{
		NodePath: "/host/path-not-visible-in-the-linux-guest", ContainerPath: "/workspace/input",
	}}
	if _, err := session.Run(t.Context(), request); err != nil {
		t.Fatalf("wire validation touched the untranslated host path: %v", err)
	}
}

func TestSweepEmbargoesRunUntilNamespaceVerificationCompletes(t *testing.T) {
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
		assertRPCCode(t, err, CodeSweepRequired)
	case <-time.After(75 * time.Millisecond):
		t.Fatal("Run remained admitted while a replacement sweep was active")
	}
	close(releaseSweep)
	if err := <-sweepDone; err != nil {
		t.Fatal(err)
	}
	verification, err := session.Verify(t.Context(), VerifyRequest{Scope: VerifyNamespace})
	if err != nil || !verification.Absent {
		t.Fatalf("post-sweep verification = %#v, err=%v", verification, err)
	}
	if _, err := session.Run(t.Context(), testRunRequest(testAuthority(), time.Second)); err != nil {
		t.Fatal(err)
	}
}

func TestPreadmittedRunRechecksSweepBarrierUnderCreationLock(t *testing.T) {
	engine := newFakeEngine()
	runPoised := make(chan struct{})
	releaseRun := make(chan struct{})
	client, stop := startTestServer(t, engine, ServerConfig{beforeRunCreateLock: func() {
		close(runPoised)
		<-releaseRun
	}})
	defer stop()
	session, err := client.OpenSession(t.Context(), testSessionRequest())
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	requireSweep(t, session)

	runDone := make(chan error, 1)
	go func() {
		_, runErr := session.Run(t.Context(), testRunRequest(testAuthority(), time.Second))
		runDone <- runErr
	}()
	<-runPoised
	if _, err := session.Sweep(t.Context(), SweepRequest{}); err != nil {
		t.Fatal(err)
	}
	close(releaseRun)
	assertRPCCode(t, <-runDone, CodeSweepRequired)
	if engine.runCount() != 0 {
		t.Fatal("pre-admitted Run crossed a newly unverified sweep boundary")
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
	lost := make(chan error, 1)
	stale.SetLossHandler(func(err error) { lost <- err })
	stopFirst()
	secondClient, stopSecond := startTestServer(t, newFakeEngine(), ServerConfig{})
	defer stopSecond()
	stale.client.Dial = secondClient.Dial
	_, err = stale.Verify(t.Context(), VerifyRequest{Scope: VerifyNamespace})
	assertRPCCode(t, err, CodeSessionStale)
	select {
	case <-lost:
	default:
		t.Fatal("replacement response returned before withdrawing old session authority")
	}
	if stale.HealthError() == nil {
		t.Fatal("replacement did not invalidate the old session")
	}
	_ = stale.Close()
}

func TestOperationTransportFailureSynchronouslyInvalidatesSession(t *testing.T) {
	client, stop := startTestServer(t, newFakeEngine(), ServerConfig{})
	defer stop()
	client.disableHeartbeatPump = true
	session, err := client.OpenSession(t.Context(), testSessionRequest())
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	lost := make(chan error, 1)
	session.SetLossHandler(func(err error) { lost <- err })
	client.Dial = func(context.Context) (net.Conn, error) { return nil, errors.New("forward replaced") }
	err = session.Signal(t.Context(), SignalRequest{Authority: testAuthority(), Signal: SignalTERM})
	if err == nil {
		t.Fatal("transport failure was accepted")
	}
	var runtimeLoss *RuntimeLossError
	if !errors.As(err, &runtimeLoss) {
		t.Fatalf("transport failure = %T %v, want typed runtime loss", err, err)
	}
	select {
	case <-lost:
	default:
		t.Fatal("transport failure returned before session authority was withdrawn")
	}
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

func TestControlLossCancelsBlockedWatchBeforeSessionReap(t *testing.T) {
	engine := &blockingWatchEngine{fakeEngine: newFakeEngine(), entered: make(chan struct{})}
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
	watchDone := make(chan error, 1)
	go func() {
		watchDone <- session.Watch(context.Background(), WatchRequest{Authority: testAuthority()}, nil)
	}()
	<-engine.entered
	if err := session.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-watchDone:
		if err == nil {
			t.Fatal("control-loss Watch returned success")
		}
	case <-time.After(time.Second):
		t.Fatal("control loss deadlocked blocked Watch")
	}
	waitFor(t, time.Second, func() bool { return engine.sessionReapCount() == 1 }, "session reap after Watch cancellation")
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
	verification, err := session.Verify(t.Context(), VerifyRequest{Scope: VerifyNamespace})
	if err != nil {
		t.Fatal(err)
	}
	if !verification.Absent {
		t.Fatal("namespace residue remained after required sweep")
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
	mu                       sync.Mutex
	calls                    []string
	sessionReaps             []SessionIdentity
	attemptReaps             []AttemptAuthority
	runResponse              RunResponse
	lastRunRequest           RunRequest
	runErr                   error
	runEntered               chan struct{}
	releaseRun               chan struct{}
	sweepEntered             chan struct{}
	releaseSweep             chan struct{}
	signalEntered            chan struct{}
	releaseSignal            chan struct{}
	sessionReapErr           error
	verifyResponses          []VerifyResponse
	dialAttemptWithoutMarker bool
	dialHostBridgeRead       bool
	dialHostBridgeDone       chan struct{}
	lastDialAttemptRequest   DialAttemptPortRequest
}

type blockingWatchEngine struct {
	*fakeEngine
	entered chan struct{}
}

type guardianRecordingEngine struct {
	*fakeEngine
	guardianMu sync.Mutex
	guardians  int
}

func (engine *guardianRecordingEngine) ReapAttemptAsGuardian(ctx context.Context, authority AttemptAuthority) error {
	engine.guardianMu.Lock()
	engine.guardians++
	engine.guardianMu.Unlock()
	return engine.fakeEngine.ReapAttempt(ctx, authority)
}

func (engine *guardianRecordingEngine) guardianCount() int {
	engine.guardianMu.Lock()
	defer engine.guardianMu.Unlock()
	return engine.guardians
}

func (engine *blockingWatchEngine) Watch(ctx context.Context, _ WatchRequest, _ func(WatchEvent) error) error {
	close(engine.entered)
	<-ctx.Done()
	return ctx.Err()
}

type crashBoundaryEngine struct {
	*fakeEngine
	stateMu       sync.Mutex
	crashAfter    int
	residues      map[string]struct{}
	live          map[string]struct{}
	duplicateRuns int
	lastAuthority AttemptAuthority
}

func newCrashBoundaryEngine(crashAfter int) *crashBoundaryEngine {
	return &crashBoundaryEngine{
		fakeEngine: newFakeEngine(), crashAfter: crashAfter,
		residues: make(map[string]struct{}), live: make(map[string]struct{}),
	}
}

func (engine *crashBoundaryEngine) Run(_ context.Context, request RunRequest) (RunResponse, error) {
	engine.record("Run")
	resources := request.Resources
	steps := []string{
		"lease:" + resources.LeaseID, // intentionally discoverable before labels are applied
		"snapshot:" + resources.SnapshotID, "container:" + resources.ContainerID, "task:" + resources.TaskID,
		"shim:" + resources.ShimID, "cgroup:" + resources.CgroupID, "log:" + resources.LogSegmentDirectory,
		"volume:" + resources.HandoffVolumeDirectory, "volume:" + resources.ServiceVolumeDirectory,
		"started:" + resources.TaskID,
	}
	if resources.LeaseID == "" || len(resources.Labels) != 7 {
		return RunResponse{}, errors.New("helper-derived lease and labels were unavailable before create")
	}
	engine.stateMu.Lock()
	defer engine.stateMu.Unlock()
	engine.lastAuthority = request.Authority
	for index, resource := range steps {
		engine.residues[resource] = struct{}{}
		if index == len(steps)-1 {
			if _, exists := engine.live[resources.TaskID]; exists {
				engine.duplicateRuns++
			}
			engine.live[resources.TaskID] = struct{}{}
		}
		if engine.crashAfter == index+1 {
			return RunResponse{}, errors.New("simulated helper crash after create boundary")
		}
	}
	return RunResponse{Started: true}, nil
}

func (engine *crashBoundaryEngine) Sweep(context.Context, SweepRequest) (SweepResponse, error) {
	engine.record("Sweep")
	engine.stateMu.Lock()
	removed := len(engine.residues)
	inventory := engine.inventoryLocked()
	authority := engine.lastAuthority
	clear(engine.residues)
	clear(engine.live)
	engine.stateMu.Unlock()
	response := SweepResponse{SweepEpoch: "fake-sweep", Removed: removed, Inventory: inventory}
	if removed != 0 && authority.AttemptID != "" {
		response.PriorBootSessionsSeen = []string{authority.BootSessionID}
		response.Attempts = []SweptAttemptAuthority{{
			RemovalGeneration: authority.RemovalGeneration, AttemptID: authority.AttemptID,
			FencingToken: authority.FencingToken, PriorBootSessionID: authority.BootSessionID,
		}}
	}
	return response, nil
}

func (engine *crashBoundaryEngine) Verify(context.Context, VerifyRequest) (VerifyResponse, error) {
	engine.record("Verify")
	engine.stateMu.Lock()
	inventory := engine.inventoryLocked()
	absent := InventoryEmpty(inventory)
	engine.stateMu.Unlock()
	return VerifyResponse{Absent: absent, Inventory: inventory}, nil
}

func (engine *crashBoundaryEngine) inventoryLocked() ResourceInventory {
	var inventory ResourceInventory
	for residue := range engine.residues {
		kind, name, found := strings.Cut(residue, ":")
		if !found {
			continue
		}
		switch kind {
		case "lease":
			inventory.Leases = append(inventory.Leases, name)
		case "snapshot":
			inventory.Snapshots = append(inventory.Snapshots, name)
		case "container":
			inventory.Containers = append(inventory.Containers, name)
		case "task":
			inventory.Tasks = append(inventory.Tasks, name)
		case "shim":
			inventory.Shims = append(inventory.Shims, name)
		case "cgroup":
			inventory.Cgroups = append(inventory.Cgroups, name)
		case "log":
			inventory.LogSegments = append(inventory.LogSegments, name)
		case "volume":
			inventory.ManagedVolumes = append(inventory.ManagedVolumes, name)
		}
	}
	return inventory
}

func (engine *crashBoundaryEngine) ReapAttempt(context.Context, AttemptAuthority) error {
	return errors.New("simulated helper crash prevented attempt reap")
}

func (engine *crashBoundaryEngine) ReapSession(context.Context, SessionIdentity) error {
	engine.stateMu.Lock()
	defer engine.stateMu.Unlock()
	if engine.crashAfter == 0 {
		clear(engine.residues)
		clear(engine.live)
		return nil
	}
	if len(engine.residues) != 0 {
		return errors.New("simulated crashed session retained residue")
	}
	return nil
}

func (engine *crashBoundaryEngine) restartAfterCrash() *crashBoundaryEngine {
	engine.stateMu.Lock()
	defer engine.stateMu.Unlock()
	restarted := newCrashBoundaryEngine(0)
	restarted.duplicateRuns = engine.duplicateRuns
	restarted.lastAuthority = engine.lastAuthority
	for residue := range engine.residues {
		restarted.residues[residue] = struct{}{}
	}
	for task := range engine.live {
		restarted.live[task] = struct{}{}
	}
	return restarted
}

func (engine *crashBoundaryEngine) residueCount() int {
	engine.stateMu.Lock()
	defer engine.stateMu.Unlock()
	return len(engine.residues)
}

func (engine *crashBoundaryEngine) duplicateStarts() int {
	engine.stateMu.Lock()
	defer engine.stateMu.Unlock()
	return engine.duplicateRuns
}

func newFakeEngine() *fakeEngine { return &fakeEngine{runResponse: RunResponse{Started: true}} }

type archiveCaptureEngine struct {
	*fakeEngine
	archive []byte
}

type rejectBlockedArchiveEngine struct {
	*fakeEngine
	entered <-chan struct{}
}

func (engine *rejectBlockedArchiveEngine) EnsureImage(context.Context, EnsureImageRequest, io.Reader, func(EnsureImageEvent) error) error {
	<-engine.entered
	return NewImageMechanicsError(ImageFailureFact{Kind: ImageFailureManifestRejected}, errors.New("archive rejected"))
}

type blockedArchiveReader struct {
	entered chan struct{}
	closed  chan struct{}
	once    sync.Once
}

func newBlockedArchiveReader() *blockedArchiveReader {
	return &blockedArchiveReader{entered: make(chan struct{}), closed: make(chan struct{})}
}

func (reader *blockedArchiveReader) Read([]byte) (int, error) {
	reader.once.Do(func() { close(reader.entered) })
	<-reader.closed
	return 0, errors.New("archive source closed")
}

func (reader *blockedArchiveReader) Close() error {
	select {
	case <-reader.closed:
	default:
		close(reader.closed)
	}
	return nil
}

func (engine *archiveCaptureEngine) EnsureImage(_ context.Context, request EnsureImageRequest, archive io.Reader, emit func(EnsureImageEvent) error) error {
	if request.Source != ImageSourceArchive || archive == nil {
		return errors.New("archive import was not delivered as an archive stream")
	}
	payload, err := io.ReadAll(archive)
	if err != nil {
		return err
	}
	engine.archive = payload
	return emit(EnsureImageEvent{Kind: ImageComplete, Result: &EnsureImageResponse{
		TopLevelDigest: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		PlatformDigest: "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
		Evidence:       fakeEnsureImageEvidence(),
	}})
}

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

func (engine *fakeEngine) EnsureImage(_ context.Context, _ EnsureImageRequest, _ io.Reader, emit func(EnsureImageEvent) error) error {
	engine.record("EnsureImage")
	return emit(EnsureImageEvent{Kind: ImageComplete, Result: &EnsureImageResponse{
		TopLevelDigest: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		PlatformDigest: "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
		Evidence:       fakeEnsureImageEvidence(),
	}})
}

func fakeEnsureImageEvidence() ImageEvidence {
	return ImageEvidence{
		SubmittedReference:     "registry.invalid/app",
		TopLevelDigest:         "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		TopLevelMediaType:      "application/vnd.oci.image.index.v1+json",
		PlatformManifestDigest: "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
		Platform:               OCIPlatform{OS: "linux", Architecture: "amd64"},
		RuntimeHandler:         DefaultRuntimeHandler, Snapshotter: DefaultSnapshotter,
	}
}
func (engine *fakeEngine) Run(_ context.Context, request RunRequest) (RunResponse, error) {
	engine.record("Run")
	engine.mu.Lock()
	engine.lastRunRequest = request
	runEntered := engine.runEntered
	releaseRun := engine.releaseRun
	response := engine.runResponse
	runErr := engine.runErr
	engine.mu.Unlock()
	if runEntered != nil {
		close(runEntered)
		<-releaseRun
	}
	return response, runErr
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
func (*fakeEngine) DeleteManagedVolume(context.Context, DeleteManagedVolumeRequest) (DeleteManagedVolumeResponse, error) {
	return DeleteManagedVolumeResponse{Deleted: true}, nil
}
func (engine *fakeEngine) Verify(context.Context, VerifyRequest) (VerifyResponse, error) {
	engine.record("Verify")
	engine.mu.Lock()
	defer engine.mu.Unlock()
	if len(engine.verifyResponses) > 0 {
		response := engine.verifyResponses[0]
		engine.verifyResponses = engine.verifyResponses[1:]
		return response, nil
	}
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
func (engine *fakeEngine) DialAttemptPort(_ context.Context, request DialAttemptPortRequest, stream io.ReadWriteCloser) error {
	engine.record("DialAttemptPort")
	engine.mu.Lock()
	engine.lastDialAttemptRequest = request
	engine.mu.Unlock()
	if engine.dialAttemptWithoutMarker {
		return nil
	}
	_, err := stream.Write(append([]byte{attemptPortBackendReady}, []byte("attempt-port")...))
	return err
}
func (engine *fakeEngine) DialHostBridge(_ context.Context, _ DialHostBridgeRequest, stream io.ReadWriteCloser) error {
	engine.record("DialHostBridge")
	if engine.dialHostBridgeRead {
		_, err := io.Copy(io.Discard, stream)
		close(engine.dialHostBridgeDone)
		return err
	}
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

func (engine *fakeEngine) runCount() int {
	engine.mu.Lock()
	defer engine.mu.Unlock()
	count := 0
	for _, call := range engine.calls {
		if call == "Run" {
			count++
		}
	}
	return count
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

type observedClock struct {
	*manualClock
	timerCreated chan struct{}
}

func (clock *observedClock) NewTimerAt(deadline time.Time) Timer {
	timer := clock.manualClock.NewTimerAt(deadline)
	clock.timerCreated <- struct{}{}
	return timer
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
