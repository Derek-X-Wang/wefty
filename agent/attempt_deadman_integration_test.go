//go:build darwin || linux

package agent

import (
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/fabric/plain"
	"github.com/Derek-X-Wang/wefty/l1"
	workloadrunner "github.com/Derek-X-Wang/wefty/runner"
	ocirunner "github.com/Derek-X-Wang/wefty/runner/oci"
	"github.com/Derek-X-Wang/wefty/runner/ocihelper"
)

const (
	preAdmissionImageReference = "example.invalid/pre-admission:v1"
	preAdmissionImageDigest    = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
)

func TestPreAdmissionRenewalDoesNotInvalidateHelperSession(t *testing.T) {
	network := plain.NewNetwork()
	store, stopL1 := startFailureServerWithPoliciesAndLease(t, network, nil, map[string]l1.NodePolicy{
		"pre-admission-node": {Tags: []string{"pre-admission"}, MaxOneshotSlots: 1, MaxServiceSlots: 1},
	}, 2*time.Second)
	defer stopL1()

	engine := newPreAdmissionRenewalEngine()
	clock := newManualClock(time.Unix(10_000, 0))
	barrier, stopHelper := startPreAdmissionHelper(t, engine, clock.Now)
	defer stopHelper()
	helperRenewals := make(chan struct{}, 8)
	adapter := ocirunner.NewAdapter(barrier)
	agentFabric := network.NewFabric(fabric.Identity{NodeID: "pre-admission-agent", Tags: []string{l1.DefaultAgentPrincipalTag}})
	managedRoot, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	nodeAgent, err := New(Config{
		Fabric: agentFabric, ControlPlaneAddress: "wefty://control-plane",
		NodeID: "pre-admission-node", BootSessionID: "pre-admission-boot", Version: "test",
		OS: "linux", Architecture: "amd64",
		Capabilities: map[string]bool{"kind:process": true},
		CapabilityProbe: capabilityProbeFunc(func(ctx context.Context) (CapabilityProbeResult, error) {
			if err := adapter.Probe(ctx, "pre-admission-node", "pre-admission-boot", preAdmissionImageReference, preAdmissionImageDigest, 2*time.Second); err != nil {
				return CapabilityProbeResult{}, err
			}
			return CapabilityProbeResult{Capabilities: map[string]bool{
				"kind:oci": true, "runtime_handler:" + ocihelper.DefaultRuntimeHandler: true,
			}}, nil
		}),
		OCIIntent: enabledTestOCIIntent, OCIBootBarrier: barrier,
		WorkloadRuntimes: map[string]WorkloadRuntime{contract.JobKindOCI: adapter},
		AttemptDeadman: preAdmissionDeadman{
			barrier: barrier, nodeID: "pre-admission-node", bootSessionID: "pre-admission-boot",
			observe: func() { helperRenewals <- struct{}{} },
		},
		ManagedRootDirectory: managedRoot, LogSpoolDirectory: t.TempDir(), MaxServiceSlots: 1,
		RenewalInterval: 200 * time.Millisecond, Clock: clock, Logf: t.Logf,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer nodeAgent.Close()
	if _, err := nodeAgent.Register(t.Context()); err != nil {
		t.Fatal(err)
	}

	job, _, err := store.CreateJob(t.Context(), contract.JobSpec{
		SchemaVersion: contract.SchemaVersionV1, DispatchKey: "pre-admission-renewal",
		Kind: contract.JobKindOCI, Class: contract.JobClassService, Restart: contract.RestartAlways,
		RoutingTags: []string{"pre-admission"}, RuntimeHandler: ocihelper.DefaultRuntimeHandler,
		Execution: contract.ExecutionSpec{OCI: &contract.OCIExecutionSpec{
			Image: contract.OCIImageSpec{Reference: preAdmissionImageReference, Digest: preAdmissionString(preAdmissionImageDigest)},
			Argv:  []string{"/payload"},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	claim, err := nodeAgent.session.client.Claim(t.Context(), "pre-admission-node", "pre-admission-boot", contract.JobClassService)
	if err != nil || claim == nil || claim.Job.JobID != job.JobID {
		nodes, _ := store.ListNodes(t.Context())
		current, _ := store.GetJob(t.Context(), job.JobID)
		t.Fatalf("service claim = %+v err=%v job=%+v nodes=%+v", claim, err, current, nodes)
	}

	executionDone := make(chan error, 1)
	go func() {
		_, executeErr := nodeAgent.executeClaim(t.Context(), *claim, time.Now())
		executionDone <- executeErr
	}()
	select {
	case authority := <-engine.imageEntered:
		if authority.AttemptID != claim.Lease.AttemptID {
			t.Fatalf("image delivery attempt=%q, want %q", authority.AttemptID, claim.Lease.AttemptID)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("attempt did not reach held pre-admission image delivery")
	}

	clock.Advance(200 * time.Millisecond)
	renewalDeadline := time.Now().Add(3 * time.Second)
	for {
		attempts, listErr := store.ListJobAttempts(t.Context(), job.JobID)
		if listErr != nil {
			t.Fatal(listErr)
		}
		if len(attempts) == 1 && attempts[0].LeaseExpiresAt.After(claim.Lease.LeaseExpires) {
			break
		}
		if time.Now().After(renewalDeadline) {
			t.Fatalf("first L1 renewal did not land at 200ms: attempts=%+v", attempts)
		}
		time.Sleep(5 * time.Millisecond)
	}

	// Give the independently scheduled helper heartbeat ample time to expose
	// an unauthorized pre-admission tuple. The helper session must remain the
	// same authority until Run reserves this attempt.
	continuityDeadline := time.Now().Add(300 * time.Millisecond)
	for time.Now().Before(continuityDeadline) {
		if _, err := barrier.Session(); err != nil {
			t.Fatalf("pre-admission L1 renewal invalidated the helper session: %v", err)
		}
		time.Sleep(5 * time.Millisecond)
	}
	session, err := barrier.Session()
	if err != nil {
		t.Fatal(err)
	}
	doctor, err := session.DoctorStatus(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if evidence := doctor.LastSessionInvalidation; evidence != nil && evidence.AttemptID == claim.Lease.AttemptID {
		t.Fatalf("pre-admission renewal invalidated helper session: attempt=%s code=%s generation=%d",
			evidence.AttemptID, evidence.RejectionCode, evidence.SessionGeneration)
	}
	close(engine.releaseImage)

	select {
	case authority := <-engine.runEntered:
		if authority.AttemptID != claim.Lease.AttemptID {
			t.Fatalf("helper admitted attempt=%q, want original %q", authority.AttemptID, claim.Lease.AttemptID)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("original attempt did not reach helper Run admission")
	}
	select {
	case <-engine.watchEntered:
	case <-time.After(5 * time.Second):
		t.Fatal("original attempt did not continue through Started into Watch")
	}
	select {
	case <-helperRenewals:
	case <-time.After(time.Second):
		t.Fatal("admission did not flush the retained helper renewal")
	}

	// A later admitted renewal must move the helper deadline. Waiting before
	// this renewal gives a stuck-closed gate enough separation for the original
	// InitialDeadman edge to be observed independently.
	time.Sleep(time.Second)
	beforeRenewal, err := store.ListJobAttempts(t.Context(), job.JobID)
	if err != nil || len(beforeRenewal) != 1 {
		t.Fatalf("attempt before admitted renewal = %+v err=%v", beforeRenewal, err)
	}
	clock.Advance(200 * time.Millisecond)
	admittedRenewalDeadline := time.Now().Add(time.Second)
	for {
		afterRenewal, listErr := store.ListJobAttempts(t.Context(), job.JobID)
		if listErr != nil {
			t.Fatal(listErr)
		}
		if len(afterRenewal) == 1 && afterRenewal[0].LeaseExpiresAt.After(beforeRenewal[0].LeaseExpiresAt) {
			break
		}
		if time.Now().After(admittedRenewalDeadline) {
			t.Fatalf("post-admission L1 renewal did not land: before=%+v after=%+v", beforeRenewal, afterRenewal)
		}
		time.Sleep(5 * time.Millisecond)
	}
	select {
	case <-helperRenewals:
	case <-time.After(time.Second):
		t.Fatal("post-admission renewal did not reach the helper session")
	}
	time.Sleep(1200 * time.Millisecond)
	if reaps := engine.attemptReapCount(); reaps != 0 {
		t.Fatalf("helper attempt did not outlive its original InitialDeadman: reaps=%d", reaps)
	}
	close(engine.releaseWatch)
	select {
	case <-engine.deleteEntered:
	case <-time.After(5 * time.Second):
		t.Fatal("terminal attempt did not enter helper reap")
	}
	beforeTerminalRenewal, err := store.ListJobAttempts(t.Context(), job.JobID)
	if err != nil || len(beforeTerminalRenewal) != 1 {
		t.Fatalf("attempt before terminal renewal = %+v err=%v", beforeTerminalRenewal, err)
	}
	clock.Advance(200 * time.Millisecond)
	terminalRenewalDeadline := time.Now().Add(time.Second)
	for {
		afterTerminalRenewal, listErr := store.ListJobAttempts(t.Context(), job.JobID)
		if listErr != nil {
			t.Fatal(listErr)
		}
		if len(afterTerminalRenewal) == 1 && afterTerminalRenewal[0].LeaseExpiresAt.After(beforeTerminalRenewal[0].LeaseExpiresAt) {
			break
		}
		if time.Now().After(terminalRenewalDeadline) {
			t.Fatalf("terminal L1 renewal did not land: before=%+v after=%+v", beforeTerminalRenewal, afterTerminalRenewal)
		}
		time.Sleep(5 * time.Millisecond)
	}
	select {
	case <-helperRenewals:
		t.Fatal("terminal L1 renewal reached the helper while reap was in flight")
	case <-time.After(100 * time.Millisecond):
	}
	session, err = barrier.Session()
	if err != nil {
		t.Fatal(err)
	}
	doctor, err = session.DoctorStatus(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if evidence := doctor.LastSessionInvalidation; evidence != nil && evidence.AttemptID == claim.Lease.AttemptID {
		t.Fatalf("terminal renewal invalidated helper session during reap: attempt=%s code=%s generation=%d",
			evidence.AttemptID, evidence.RejectionCode, evidence.SessionGeneration)
	}
	close(engine.releaseDelete)

	select {
	case err := <-executionDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("original attempt did not finish")
	}
	attempts, err := store.ListJobAttempts(t.Context(), job.JobID)
	if err != nil || len(attempts) != 1 || attempts[0].AttemptID != claim.Lease.AttemptID {
		t.Fatalf("pre-admission ordering produced replacement attempts=%+v err=%v", attempts, err)
	}
}

func TestSessionGenerationMismatchDropsRenewalAfterHelperTakeover(t *testing.T) {
	engine := newPreAdmissionRenewalEngine()
	barrier, stopHelper := startPreAdmissionHelper(t, engine, time.Now)
	defer stopHelper()
	if err := barrier.Ensure(t.Context()); err != nil {
		t.Fatal(err)
	}
	staleSession, err := barrier.Session()
	if err != nil {
		t.Fatal(err)
	}
	staleHandshake := staleSession.Handshake()
	staleGeneration := workloadrunner.RuntimeGeneration{
		InstanceID: staleHandshake.HelperInstanceID,
		Generation: staleHandshake.SessionGeneration,
	}

	// Reject a genuinely unknown tuple to withdraw the incumbent session, then
	// let the real boot barrier complete a fresh exclusive-session takeover.
	trigger := ocihelper.AttemptAuthority{
		NodeID: "pre-admission-node", BootSessionID: "pre-admission-boot",
		JobID: "takeover-trigger", AttemptID: "takeover-trigger-attempt", FencingToken: "takeover-trigger-fence",
		Class: contract.JobClassService, RemovalGeneration: fmt.Sprint(l1.InitialServiceRemovalGeneration),
	}
	if err := staleSession.QueueAttemptRenewal(trigger, time.Second); err != nil {
		t.Fatal(err)
	}
	lossDeadline := time.Now().Add(time.Second)
	for staleSession.HealthError() == nil && time.Now().Before(lossDeadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if err := staleSession.HealthError(); err == nil {
		t.Fatal("unknown renewal did not withdraw the incumbent helper session")
	}
	if err := barrier.Ensure(t.Context()); err != nil {
		t.Fatalf("replacement helper session takeover: %v", err)
	}
	replacement, err := barrier.Session()
	if err != nil {
		t.Fatal(err)
	}
	replacementHandshake := replacement.Handshake()
	replacementGeneration := workloadrunner.RuntimeGeneration{
		InstanceID: replacementHandshake.HelperInstanceID,
		Generation: replacementHandshake.SessionGeneration,
	}
	if replacementGeneration == staleGeneration {
		t.Fatalf("helper takeover retained stale generation: %+v", replacementGeneration)
	}

	queued := make(chan struct{}, 1)
	var logs []string
	claim := l1.Claim{
		Job:   l1.Job{JobID: "stale-generation-job", Spec: contract.JobSpec{Kind: contract.JobKindOCI, Class: contract.JobClassService}},
		Lease: l1.AttemptLease{AttemptID: "stale-generation-attempt", FencingToken: "stale-generation-fence", LeaseTTL: time.Second},
	}
	admission := newAttemptDeadmanAdmission(preAdmissionDeadman{
		barrier: barrier, nodeID: "pre-admission-node", bootSessionID: "pre-admission-boot",
		observe: func() { queued <- struct{}{} },
	}, claim, systemClock{}, func(format string, args ...any) {
		logs = append(logs, fmt.Sprintf(format, args...))
	})
	if err := admission.admit(staleGeneration); err != nil {
		t.Fatal(err)
	}
	if err := admission.queue(claim.Lease, time.Now().Add(time.Second)); err != nil {
		t.Fatalf("stale-generation renewal was not dropped: %v", err)
	}
	if !strings.Contains(strings.Join(logs, "\n"), "reason=session_generation_mismatch") {
		t.Fatalf("stale-generation renewal lacked mismatch evidence: %q", logs)
	}
	if err := admission.queue(claim.Lease, time.Now().Add(time.Second)); err != nil {
		t.Fatalf("terminal mismatch gate returned a later renewal error: %v", err)
	}
	select {
	case <-queued:
		t.Fatal("stale-generation renewal reached the replacement helper session")
	case <-time.After(100 * time.Millisecond):
	}
	if err := replacement.HealthError(); err != nil {
		t.Fatalf("stale-generation renewal invalidated replacement helper session: %v", err)
	}
}

func preAdmissionString(value string) *string { return &value }

type preAdmissionDeadman struct {
	barrier               *ocihelper.BootBarrier
	nodeID, bootSessionID string
	observe               func()
}

func (renewer preAdmissionDeadman) QueueSuccessfulRenewal(claim l1.Claim, expiresAt time.Time, expected workloadrunner.RuntimeGeneration) error {
	session, err := renewer.barrier.Session()
	if err != nil {
		return err
	}
	if err := ValidateAttemptDeadmanSessionGeneration(session, expected); err != nil {
		return err
	}
	err = session.QueueAttemptRenewalUntil(ocihelper.AttemptAuthority{
		NodeID: renewer.nodeID, BootSessionID: renewer.bootSessionID,
		JobID: claim.Job.JobID, AttemptID: claim.Lease.AttemptID, FencingToken: claim.Lease.FencingToken,
		Class: claim.Job.Spec.Class, RemovalGeneration: fmt.Sprint(l1.InitialServiceRemovalGeneration),
	}, expiresAt)
	if err == nil && renewer.observe != nil {
		renewer.observe()
	}
	return err
}

type preAdmissionRenewalEngine struct {
	ocihelper.UnavailableEngine
	imageEntered  chan ocihelper.AttemptAuthority
	releaseImage  chan struct{}
	runEntered    chan ocihelper.AttemptAuthority
	watchEntered  chan struct{}
	releaseWatch  chan struct{}
	deleteEntered chan struct{}
	releaseDelete chan struct{}
	imageOnce     sync.Once
	watchOnce     sync.Once
	deleteOnce    sync.Once
	mu            sync.Mutex
	attemptReaps  int
}

func newPreAdmissionRenewalEngine() *preAdmissionRenewalEngine {
	return &preAdmissionRenewalEngine{
		imageEntered: make(chan ocihelper.AttemptAuthority, 1),
		releaseImage: make(chan struct{}), runEntered: make(chan ocihelper.AttemptAuthority, 1),
		watchEntered: make(chan struct{}), releaseWatch: make(chan struct{}),
		deleteEntered: make(chan struct{}), releaseDelete: make(chan struct{}),
	}
}

func (engine *preAdmissionRenewalEngine) EnsureImage(ctx context.Context, request ocihelper.EnsureImageRequest, _ io.Reader, emit func(ocihelper.EnsureImageEvent) error) error {
	if request.Pin != nil {
		engine.imageOnce.Do(func() { engine.imageEntered <- request.Pin.Authority })
		select {
		case <-engine.releaseImage:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	response := preAdmissionImageResponse()
	return emit(ocihelper.EnsureImageEvent{Kind: ocihelper.ImageComplete, Result: &response})
}

func (engine *preAdmissionRenewalEngine) Run(_ context.Context, request ocihelper.RunRequest) (ocihelper.RunResponse, error) {
	if request.Authority.JobID[:min(len(request.Authority.JobID), len("probe-"))] != "probe-" {
		engine.runEntered <- request.Authority
	}
	evidence := preAdmissionImageResponse().Evidence
	return ocihelper.RunResponse{Started: true, StartedAt: time.Now().UTC(), Image: &evidence}, nil
}

func (engine *preAdmissionRenewalEngine) Watch(ctx context.Context, request ocihelper.WatchRequest, emit func(ocihelper.WatchEvent) error) error {
	if request.Authority.JobID[:min(len(request.Authority.JobID), len("probe-"))] != "probe-" {
		engine.watchOnce.Do(func() { close(engine.watchEntered) })
		select {
		case <-engine.releaseWatch:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	exitCode := 0
	return emit(ocihelper.WatchEvent{Kind: ocihelper.WatchComplete, Result: &ocihelper.WatchResponse{ExitCode: &exitCode}})
}

func (engine *preAdmissionRenewalEngine) ReapAttempt(_ context.Context, authority ocihelper.AttemptAuthority) error {
	if authority.JobID[:min(len(authority.JobID), len("probe-"))] == "probe-" {
		return nil
	}
	engine.mu.Lock()
	engine.attemptReaps++
	engine.mu.Unlock()
	return nil
}

func (engine *preAdmissionRenewalEngine) attemptReapCount() int {
	engine.mu.Lock()
	defer engine.mu.Unlock()
	return engine.attemptReaps
}

func (engine *preAdmissionRenewalEngine) Delete(ctx context.Context, request ocihelper.DeleteRequest) (ocihelper.DeleteResponse, error) {
	if request.Authority.JobID[:min(len(request.Authority.JobID), len("probe-"))] != "probe-" {
		engine.deleteOnce.Do(func() { close(engine.deleteEntered) })
		select {
		case <-engine.releaseDelete:
		case <-ctx.Done():
			return ocihelper.DeleteResponse{}, ctx.Err()
		}
	}
	return ocihelper.DeleteResponse{Deleted: true}, nil
}

func (*preAdmissionRenewalEngine) Verify(context.Context, ocihelper.VerifyRequest) (ocihelper.VerifyResponse, error) {
	return ocihelper.VerifyResponse{Absent: true}, nil
}

func (*preAdmissionRenewalEngine) Sweep(context.Context, ocihelper.SweepRequest) (ocihelper.SweepResponse, error) {
	return ocihelper.SweepResponse{SweepEpoch: "pre-admission-sweep"}, nil
}

func (*preAdmissionRenewalEngine) ReapSession(context.Context, ocihelper.SessionIdentity) (ocihelper.SweepResponse, error) {
	return ocihelper.SweepResponse{SweepEpoch: "pre-admission-session-reap"}, nil
}

func (*preAdmissionRenewalEngine) ReconcileImagePins(context.Context, ocihelper.ReconcileImagePinsRequest) (ocihelper.ReconcileImagePinsResponse, error) {
	return ocihelper.ReconcileImagePinsResponse{}, nil
}

func (*preAdmissionRenewalEngine) ReleaseImagePin(context.Context, ocihelper.ReleaseImagePinRequest) error {
	return nil
}

func (*preAdmissionRenewalEngine) ReleaseAttemptImagePin(context.Context, ocihelper.ReleaseAttemptImagePinRequest) error {
	return nil
}

func (*preAdmissionRenewalEngine) ImageCacheStatus(context.Context) (ocihelper.ImageCacheStatus, error) {
	return ocihelper.ImageCacheStatus{}, nil
}

func (*preAdmissionRenewalEngine) DoctorStatus(context.Context) (ocihelper.DoctorStatus, error) {
	return ocihelper.DoctorStatus{}, nil
}

func preAdmissionImageResponse() ocihelper.EnsureImageResponse {
	evidence := ocihelper.ImageEvidence{
		SubmittedReference: preAdmissionImageReference, TopLevelDigest: preAdmissionImageDigest,
		TopLevelMediaType: "application/vnd.oci.image.manifest.v1+json", PlatformManifestDigest: preAdmissionImageDigest,
		Platform:       ocihelper.OCIPlatform{OS: "linux", Architecture: "amd64"},
		RuntimeHandler: ocihelper.DefaultRuntimeHandler, Snapshotter: ocihelper.DefaultSnapshotter,
	}
	return ocihelper.EnsureImageResponse{TopLevelDigest: preAdmissionImageDigest, PlatformDigest: preAdmissionImageDigest, Evidence: evidence}
}

func startPreAdmissionHelper(t *testing.T, engine ocihelper.Engine, now func() time.Time) (*ocihelper.BootBarrier, func()) {
	t.Helper()
	directory, err := os.MkdirTemp("", "wefty-332-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(directory) })
	path := filepath.Join(directory, "helper.sock")
	listener, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	server, err := ocihelper.NewServer(engine, ocihelper.ServerConfig{
		HelperChecksum: "checksum-test", AllowedUIDs: []uint32{uint32(os.Getuid())},
		HeartbeatTimeout: time.Second, MaximumAttemptDeadman: 5 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	serveContext, cancelServe := context.WithCancel(context.Background())
	serveDone := make(chan error, 1)
	go func() { serveDone <- server.Serve(serveContext, listener) }()
	client := ocihelper.NewUnixClient(path, "checksum-test")
	client.HeartbeatInterval = 20 * time.Millisecond
	client.Now = now
	barrier, err := ocihelper.NewBootBarrier(client, ocihelper.AcquireSessionRequest{
		NodeID: "pre-admission-node", BootSessionID: "pre-admission-boot",
	})
	if err != nil {
		cancelServe()
		_ = listener.Close()
		t.Fatal(err)
	}
	return barrier, func() {
		_ = barrier.Close()
		cancelServe()
		_ = listener.Close()
		select {
		case err := <-serveDone:
			if err != nil {
				t.Errorf("serve helper: %v", err)
			}
		case <-time.After(time.Second):
			t.Error("helper server did not stop")
		}
	}
}
