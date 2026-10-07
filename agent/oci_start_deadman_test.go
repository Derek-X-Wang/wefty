//go:build darwin || linux

package agent

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
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
	ociDeadmanNodeID        = "pre-admission-node"
	ociDeadmanBootSessionID = "pre-admission-boot"
)

// deadmanTestEngine is a helper engine whose payloads run until the test
// ends. A non-probe Run starts runDelay after the helper reserves the
// attempt, as a slow container start does; the helper arms the attempt's
// initial deadman at that reservation. The engine records which attempts the
// helper reaped, and closes expired when the helper's own deadman reaps one.
type deadmanTestEngine struct {
	*preAdmissionRenewalEngine
	runDelay    time.Duration
	runEntered  chan time.Time
	expired     chan struct{}
	expiredOnce sync.Once
	reapedMu    sync.Mutex
	reaped      []string
}

func newDeadmanTestEngine(runDelay time.Duration) *deadmanTestEngine {
	base := newPreAdmissionRenewalEngine()
	close(base.releaseImage)
	close(base.releaseWatch)
	close(base.releaseDelete)
	return &deadmanTestEngine{preAdmissionRenewalEngine: base, runDelay: runDelay,
		runEntered: make(chan time.Time, 1), expired: make(chan struct{})}
}

func probeAttempt(authority ocihelper.AttemptAuthority) bool {
	return strings.HasPrefix(authority.JobID, "probe-")
}

func (engine *deadmanTestEngine) EnsureImage(_ context.Context, _ ocihelper.EnsureImageRequest, _ io.Reader, emit func(ocihelper.EnsureImageEvent) error) error {
	response := preAdmissionImageResponse()
	return emit(ocihelper.EnsureImageEvent{Kind: ocihelper.ImageComplete, Result: &response})
}

func (engine *deadmanTestEngine) Run(ctx context.Context, request ocihelper.RunRequest) (ocihelper.RunResponse, error) {
	if !probeAttempt(request.Authority) {
		select {
		case engine.runEntered <- time.Now():
		default:
		}
		select {
		case <-time.After(engine.runDelay):
		case <-ctx.Done():
			return ocihelper.RunResponse{}, ctx.Err()
		}
	}
	evidence := preAdmissionImageResponse().Evidence
	return ocihelper.RunResponse{Started: true, StartedAt: time.Now().UTC(), Image: &evidence}, nil
}

func (engine *deadmanTestEngine) Watch(ctx context.Context, request ocihelper.WatchRequest, emit func(ocihelper.WatchEvent) error) error {
	if !probeAttempt(request.Authority) {
		<-ctx.Done()
		return ctx.Err()
	}
	exitCode := 0
	return emit(ocihelper.WatchEvent{Kind: ocihelper.WatchComplete, Result: &ocihelper.WatchResponse{ExitCode: &exitCode}})
}

// Signal delivers to the running payload, as a live engine does.
func (engine *deadmanTestEngine) Signal(context.Context, ocihelper.SignalRequest) error { return nil }

func (engine *deadmanTestEngine) Delete(context.Context, ocihelper.DeleteRequest) (ocihelper.DeleteResponse, error) {
	return ocihelper.DeleteResponse{Deleted: true}, nil
}

func (engine *deadmanTestEngine) ReapAttempt(ctx context.Context, authority ocihelper.AttemptAuthority) error {
	if !probeAttempt(authority) {
		engine.reapedMu.Lock()
		engine.reaped = append(engine.reaped, authority.AttemptID)
		engine.reapedMu.Unlock()
	}
	return engine.preAdmissionRenewalEngine.ReapAttempt(ctx, authority)
}

// ReapAttemptAsGuardian is the helper's own deadman expiry, distinct from a
// reap the agent asked for.
func (engine *deadmanTestEngine) ReapAttemptAsGuardian(ctx context.Context, authority ocihelper.AttemptAuthority) error {
	if !probeAttempt(authority) {
		engine.expiredOnce.Do(func() { close(engine.expired) })
	}
	return engine.ReapAttempt(ctx, authority)
}

func (engine *deadmanTestEngine) reapedAttempts() []string {
	engine.reapedMu.Lock()
	defer engine.reapedMu.Unlock()
	return append([]string(nil), engine.reaped...)
}

// newOCIDeadmanAgent registers an OCI node agent on the helper behind barrier;
// the caller closes it before stopping the helper. probe, when set, gates the
// OCI capability as the adapter's functional probe does; renewalQueued, when
// set, sees every L1 renewal forwarded to the helper.
func newOCIDeadmanAgent(t *testing.T, network *plain.Network, barrier *ocihelper.BootBarrier, runtime WorkloadRuntime,
	probe func(context.Context) error, renewalQueued func()) *Agent {
	t.Helper()
	managedRoot, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	nodeAgent, err := New(Config{
		Fabric:              network.NewFabric(fabric.Identity{NodeID: ociDeadmanNodeID + "-fabric", Tags: []string{l1.DefaultAgentPrincipalTag}}),
		ControlPlaneAddress: "wefty://control-plane", NodeID: ociDeadmanNodeID, BootSessionID: ociDeadmanBootSessionID, Version: "test",
		OS: "linux", Architecture: "amd64",
		Capabilities: map[string]bool{"kind:process": true},
		CapabilityProbe: capabilityProbeFunc(func(ctx context.Context) (CapabilityProbeResult, error) {
			if probe != nil {
				if err := probe(ctx); err != nil {
					return CapabilityProbeResult{}, err
				}
			}
			return CapabilityProbeResult{Capabilities: map[string]bool{
				"kind:oci": true, "runtime_handler:" + ocihelper.DefaultRuntimeHandler: true,
			}}, nil
		}),
		OCIIntent: enabledTestOCIIntent, OCIBootBarrier: barrier,
		WorkloadRuntimes: map[string]WorkloadRuntime{contract.JobKindOCI: runtime},
		AttemptDeadman: preAdmissionDeadman{
			barrier: barrier, nodeID: ociDeadmanNodeID, bootSessionID: ociDeadmanBootSessionID, observe: renewalQueued,
		},
		ManagedRootDirectory: managedRoot, LogSpoolDirectory: t.TempDir(), HandoffRoot: t.TempDir(), MaxServiceSlots: 1,
		RenewalInterval: 100 * time.Millisecond, LogRetryInterval: 10 * time.Millisecond, OperationTimeout: 2 * time.Second, Logf: t.Logf,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := nodeAgent.Register(t.Context()); err != nil {
		nodeAgent.Close()
		t.Fatal(err)
	}
	return nodeAgent
}

// claimOCIDeadmanService creates a never-restarting OCI service and claims its
// attempt for nodeAgent under an L1 lease of leaseTTL, which is also the
// helper's initial deadman for it.
func claimOCIDeadmanService(t *testing.T, store *l1.Store, nodeAgent *Agent, dispatchKey string, leaseTTL time.Duration) (l1.Job, l1.Claim) {
	t.Helper()
	job, _, err := store.CreateJob(t.Context(), contract.JobSpec{
		SchemaVersion: contract.SchemaVersionV1, DispatchKey: dispatchKey,
		Kind: contract.JobKindOCI, Class: contract.JobClassService, Restart: contract.RestartNever,
		RoutingTags: []string{ociStartTag}, RuntimeHandler: ocihelper.DefaultRuntimeHandler,
		Execution: contract.ExecutionSpec{OCI: &contract.OCIExecutionSpec{
			Image: contract.OCIImageSpec{Reference: preAdmissionImageReference, Digest: preAdmissionString(preAdmissionImageDigest)},
			Argv:  []string{"/payload"},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	claim, err := nodeAgent.session.client.Claim(t.Context(), ociDeadmanNodeID, ociDeadmanBootSessionID, contract.JobClassService)
	if err != nil || claim == nil || claim.Job.JobID != job.JobID || claim.Lease.LeaseTTL != leaseTTL {
		t.Fatalf("service claim = %+v err=%v", claim, err)
	}
	return job, *claim
}

// requireHelperSessionIntact fails unless the helper session is the healthy,
// never invalidated generation the test started with.
func requireHelperSessionIntact(t *testing.T, barrier *ocihelper.BootBarrier, generation uint64) {
	t.Helper()
	current, err := barrier.Session()
	if err != nil {
		t.Fatalf("helper session: %v", err)
	}
	if err := current.HealthError(); err != nil || current.Handshake().SessionGeneration != generation {
		t.Fatalf("helper session = %v (generation %d, want %d)", err, current.Handshake().SessionGeneration, generation)
	}
	doctor, err := current.DoctorStatus(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if evidence := doctor.LastSessionInvalidation; evidence != nil {
		t.Fatalf("helper session invalidated: attempt=%s code=%s", evidence.AttemptID, evidence.RejectionCode)
	}
}

// L1 commits an OCI Started whose answers are then lost, and the helper's
// container start is slow, so the helper's initial deadman -- armed when Run
// reserved the attempt -- runs out before a lease window counted from Run's
// return would. Once the helper has expired the attempt, L1 answers a Started
// replay, but that acceptance could only lose the attempt. So the retry ends
// at the helper's deadman: the attempt is never admitted, no renewal reaches
// the helper, and the session stays intact.
func TestOCIStartRetryEndsAtHelperDeadman(t *testing.T) {
	const (
		initialDeadman = 4 * time.Second // the L1 lease, and so the helper's initial deadman
		runDelay       = 1500 * time.Millisecond
	)
	engine := newDeadmanTestEngine(runDelay)
	barrier, stopHelper := startPreAdmissionHelper(t, engine, time.Now)
	defer stopHelper()

	var startCalls, lateReplays atomic.Int32
	network := plain.NewNetwork()
	store := startInterceptedL1(t, network, ociDeadmanNodeID, initialDeadman, func(next http.Handler, w http.ResponseWriter, r *http.Request) {
		if !strings.HasSuffix(r.URL.Path, "/started") {
			next.ServeHTTP(w, r)
			return
		}
		select {
		case <-engine.expired:
			// The helper has expired the attempt; L1 answers the replay.
			lateReplays.Add(1)
			next.ServeHTTP(w, r)
			return
		default:
		}
		if startCalls.Add(1) == 1 {
			commitAndLoseAnswer(t, next, w, r)
			return
		}
		loseAnswer(t, w)
	})

	adapter := ocirunner.NewAdapter(barrier)
	var renewalsQueued atomic.Int32
	nodeAgent := newOCIDeadmanAgent(t, network, barrier, adapter, func(ctx context.Context) error {
		return adapter.Probe(ctx, ociDeadmanNodeID, ociDeadmanBootSessionID, preAdmissionImageReference, preAdmissionImageDigest, 2*time.Second)
	}, func() { renewalsQueued.Add(1) })
	defer nodeAgent.Close()
	job, claim := claimOCIDeadmanService(t, store, nodeAgent, "oci-start-deadman", initialDeadman)
	session, err := barrier.Session()
	if err != nil {
		t.Fatal(err)
	}
	generation := session.Handshake().SessionGeneration

	runContext, cancelRun := context.WithCancel(t.Context())
	executed := make(chan struct{})
	go func() {
		defer close(executed)
		_, executeErr := nodeAgent.executeClaim(runContext, claim, time.Now())
		t.Logf("attempt ended: %v", executeErr)
	}()
	defer func() {
		cancelRun()
		<-executed
	}()
	var runAt time.Time
	select {
	case runAt = <-engine.runEntered:
	case <-time.After(10 * time.Second):
		t.Fatal("the attempt never reached helper Run")
	}
	select {
	case <-executed:
	case <-time.After(15 * time.Second):
		t.Fatal("the attempt never ended")
	}
	// Outlast the helper's deadline for this attempt, so any renewal for it
	// would have reached the helper by now.
	if wait := time.Until(runAt.Add(initialDeadman + 500*time.Millisecond)); wait > 0 {
		time.Sleep(wait)
	}

	if calls := startCalls.Load(); calls < 2 {
		t.Fatalf("Started calls = %d, want the committed one and retries after its lost answer", calls)
	}
	// A request sent just inside the deadman can reach L1 after it, but its
	// answer lands after the agent stopped waiting.
	t.Logf("Started replays answered after the helper expired the attempt: %d", lateReplays.Load())
	if queued := renewalsQueued.Load(); queued != 0 {
		t.Fatalf("%d helper deadman renewals queued: the Started retry outlived the helper's deadman and admitted the attempt", queued)
	}
	requireHelperSessionIntact(t, barrier, generation)
	attempts, err := store.ListJobAttempts(t.Context(), job.JobID)
	if err != nil || len(attempts) != 1 || attempts[0].Image == nil || attempts[0].Image.StartedAt == nil {
		t.Fatalf("attempt = %+v %v, want the committed start whose answers were lost", attempts, err)
	}
}

// lateAdmissionRuntime runs an OCI attempt on the real helper session as the
// adapter does around admission: it registers for the attempt's loss and
// reports the Run request before helper Run, acknowledges Started, admits the
// attempt, and ends with the helper's attempt-scoped refusal. It admits only
// once the helper's deadman has expired the attempt, as when an acceptance
// lands at that deadman or the adapter is slow to admit. It opens no Watch.
// The helper also refuses a Watch for an attempt it no longer holds, and in
// the adapter that refusal races this one.
type lateAdmissionRuntime struct {
	captureRuntime
	barrier *ocihelper.BootBarrier
	engine  *deadmanTestEngine
}

func (runtime *lateAdmissionRuntime) Run(ctx context.Context, request workloadrunner.Request, _ workloadrunner.OutputSink) (workloadrunner.Result, error) {
	fail := func(code contract.SpawnFailureCode, err error) (workloadrunner.Result, error) {
		return workloadrunner.Result{Outcome: spawnFailure(code, err)}, err
	}
	session, err := runtime.barrier.Session()
	if err != nil {
		return fail(contract.SpawnFailureRuntimeUnavailable, err)
	}
	authority := ocirunner.HelperAuthority(request.Authority)
	lost, release := session.ObserveAttemptLoss(authority)
	defer release()
	image := request.Execution.OCI.Image
	observation := workloadrunner.OCIImageObservation{
		SubmittedReference: image.Reference, TopLevelDigest: *image.Digest,
		TopLevelMediaType: "application/vnd.oci.image.manifest.v1+json", PlatformManifestDigest: *image.Digest,
		PlatformOS: "linux", PlatformArchitecture: "amd64",
		RuntimeHandler: ocihelper.DefaultRuntimeHandler, Snapshotter: ocihelper.DefaultSnapshotter,
	}
	if err := request.OCIImageResolved(ctx, observation); err != nil {
		return fail(contract.SpawnFailureRuntimeUnavailable, err)
	}
	request.OCIRunRequested()
	if _, err := session.Run(ctx, ocihelper.RunRequest{Authority: authority, InitialDeadman: request.InitialDeadman,
		Workload: ocihelper.WorkloadInput{ImageReference: image.Reference, ImageDigest: *image.Digest, Argv: request.Execution.OCI.Argv}}); err != nil {
		return fail(contract.SpawnFailureRuntimeUnavailable, err)
	}
	if err := request.OCIStarted(ctx, observation); err != nil {
		return fail(contract.SpawnFailureProcessRequest, err)
	}
	select {
	case <-runtime.engine.expired:
	case <-ctx.Done():
		return fail(contract.SpawnFailureRuntimeUnavailable, ctx.Err())
	}
	helper := session.Handshake()
	if err := request.OCIHelperAdmitted(workloadrunner.RuntimeGeneration{InstanceID: helper.HelperInstanceID, Generation: helper.SessionGeneration}); err != nil {
		return fail(contract.SpawnFailureRuntimeUnavailable, err)
	}
	select {
	case refusal := <-lost:
		return workloadrunner.Result{Outcome: contract.ProcessResult{RuntimeFailure: &contract.RuntimeFailure{
			Code: contract.RuntimeFailureUnavailable, Message: refusal.Error(),
		}}}, refusal
	case <-ctx.Done():
		return workloadrunner.Result{Outcome: contract.ProcessResult{Signal: "terminated", TerminationCause: contract.TerminationCauseAgent}}, ctx.Err()
	}
}

func (*lateAdmissionRuntime) RemovalResourceManifest(request workloadrunner.Request) (workloadrunner.RuntimeResourceManifest, error) {
	manifest := testRuntimeResourceManifest(request.Authority.JobID, request.Authority.AttemptID)
	manifest.NodeID, manifest.BootSessionID = request.Authority.NodeID, request.Authority.BootSessionID
	manifest.FencingToken, manifest.WorkloadClass = request.Authority.FencingToken, request.Authority.WorkloadClass
	manifest.RemovalGeneration = request.Authority.RemovalGeneration
	return manifest, nil
}

// L1 accepts an OCI start, but the attempt is admitted only after the
// helper's initial deadman has expired it. Admission forwards the L1 renewal
// the agent held, and the helper refuses it as attempt_expired. That ends this
// attempt alone: it is settled lost, with no completion, while a long-lived
// neighbour on the same helper session keeps running and the session keeps
// its generation. L1 keeps the committed start and settles the attempt when
// its lease runs out.
func TestOCILateAdmissionAfterHelperDeadmanLosesOnlyThatAttempt(t *testing.T) {
	const lease = 1500 * time.Millisecond // also the helper's initial deadman
	engine := newDeadmanTestEngine(0)
	barrier, stopHelper := startPreAdmissionHelper(t, engine, time.Now)
	defer stopHelper()
	var completions atomic.Int32
	network := plain.NewNetwork()
	store := startInterceptedL1(t, network, ociDeadmanNodeID, lease, func(next http.Handler, w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/complete") {
			completions.Add(1)
		}
		next.ServeHTTP(w, r)
	})
	var renewalsQueued atomic.Int32
	runtime := &lateAdmissionRuntime{barrier: barrier, engine: engine}
	nodeAgent := newOCIDeadmanAgent(t, network, barrier, runtime, nil, func() { renewalsQueued.Add(1) })
	defer nodeAgent.Close()
	job, claim := claimOCIDeadmanService(t, store, nodeAgent, "oci-late-admission", lease)
	session, err := barrier.Session()
	if err != nil {
		t.Fatal(err)
	}
	generation := session.Handshake().SessionGeneration
	neighbour := ocihelper.AttemptAuthority{NodeID: ociDeadmanNodeID, BootSessionID: ociDeadmanBootSessionID,
		JobID: "neighbour-job", AttemptID: "neighbour-attempt", FencingToken: "neighbour-fence",
		Class: contract.JobClassService, RemovalGeneration: fmt.Sprint(l1.InitialServiceRemovalGeneration)}
	if _, err := session.Run(t.Context(), ocihelper.RunRequest{Authority: neighbour, InitialDeadman: 5 * time.Second,
		Workload: ocihelper.WorkloadInput{ImageReference: preAdmissionImageReference, ImageDigest: preAdmissionImageDigest, Argv: []string{"/payload"}}}); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	destination, err := nodeAgent.executeClaim(ctx, claim, time.Now())
	var lost *ocihelper.AttemptLostError
	if !errors.As(err, &lost) || lost.Authority.AttemptID != claim.Lease.AttemptID || destination != errorDestinationAttemptAuthority {
		t.Fatalf("late admission = destination %v err %v, want the attempt lost to attempt_expired", destination, err)
	}
	if queued := renewalsQueued.Load(); queued == 0 {
		t.Fatal("admission forwarded no renewal to the helper")
	}
	if calls := completions.Load(); calls != 0 {
		t.Fatalf("the lost attempt published %d completions", calls)
	}
	requireHelperSessionIntact(t, barrier, generation)
	if reaped := engine.reapedAttempts(); len(reaped) != 1 || reaped[0] != claim.Lease.AttemptID {
		t.Fatalf("helper reaped %v, want only the expired attempt %s", reaped, claim.Lease.AttemptID)
	}
	if err := session.Signal(t.Context(), ocihelper.SignalRequest{Authority: neighbour, Signal: ocihelper.SignalTERM}); err != nil {
		t.Fatalf("the neighbour attempt is no longer live: %v", err)
	}

	// Renewal ended with the loss, so L1 settles the attempt by lease expiry.
	deadline := time.Now().Add(3 * lease)
	for {
		if _, err := store.Reconcile(t.Context()); err != nil {
			t.Fatal(err)
		}
		attempts, err := store.ListJobAttempts(t.Context(), job.JobID)
		if err != nil || len(attempts) != 1 || attempts[0].Image == nil || attempts[0].Image.StartedAt == nil || attempts[0].Result != nil {
			t.Fatalf("attempts = %+v %v, want the committed start and no completion", attempts, err)
		}
		if attempts[0].State == contract.AttemptLost {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("attempt after the late admission = %+v, want lost by lease expiry", attempts[0])
		}
		time.Sleep(20 * time.Millisecond)
	}
}
