//go:build darwin || linux

package agent

import (
	"context"
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
	ocirunner "github.com/Derek-X-Wang/wefty/runner/oci"
	"github.com/Derek-X-Wang/wefty/runner/ocihelper"
)

// slowStartEngine is a helper engine whose payload starts runDelay after the
// helper reserves the attempt, as a slow container start does. The helper arms
// the attempt's initial deadman at that reservation, so it runs out runDelay
// before a lease window counted from Run's return. Its deadman expiry is
// reported on expired.
type slowStartEngine struct {
	*preAdmissionRenewalEngine
	runDelay    time.Duration
	runEntered  chan time.Time
	expired     chan struct{}
	expiredOnce sync.Once
}

func probeAttempt(authority ocihelper.AttemptAuthority) bool {
	return strings.HasPrefix(authority.JobID, "probe-")
}

func (engine *slowStartEngine) EnsureImage(_ context.Context, _ ocihelper.EnsureImageRequest, _ io.Reader, emit func(ocihelper.EnsureImageEvent) error) error {
	response := preAdmissionImageResponse()
	return emit(ocihelper.EnsureImageEvent{Kind: ocihelper.ImageComplete, Result: &response})
}

func (engine *slowStartEngine) Run(ctx context.Context, request ocihelper.RunRequest) (ocihelper.RunResponse, error) {
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

func (engine *slowStartEngine) Watch(ctx context.Context, request ocihelper.WatchRequest, emit func(ocihelper.WatchEvent) error) error {
	if !probeAttempt(request.Authority) {
		<-ctx.Done()
		return ctx.Err()
	}
	exitCode := 0
	return emit(ocihelper.WatchEvent{Kind: ocihelper.WatchComplete, Result: &ocihelper.WatchResponse{ExitCode: &exitCode}})
}

// Signal delivers to the running payload, as a live engine does.
func (engine *slowStartEngine) Signal(context.Context, ocihelper.SignalRequest) error { return nil }

func (engine *slowStartEngine) Delete(context.Context, ocihelper.DeleteRequest) (ocihelper.DeleteResponse, error) {
	return ocihelper.DeleteResponse{Deleted: true}, nil
}

// ReapAttemptAsGuardian is the helper's own deadman expiry, distinct from a
// reap the agent asked for.
func (engine *slowStartEngine) ReapAttemptAsGuardian(ctx context.Context, authority ocihelper.AttemptAuthority) error {
	if !probeAttempt(authority) {
		engine.expiredOnce.Do(func() { close(engine.expired) })
	}
	return engine.ReapAttempt(ctx, authority)
}

// L1 commits an OCI Started whose answers are then lost, and the helper's
// container start is slow, so the helper's initial deadman -- armed when Run
// reserved the attempt -- runs out before a lease window counted from Run's
// return would. Once the helper has expired the attempt, L1 answers a Started
// replay. An agent still retrying would get that late success, admit the
// attempt, and forward an L1 renewal for an attempt the helper already
// expired; the helper rejects it and invalidates the whole session, reaping
// every neighbour. Instead the retry ends inside the helper's admission
// window: no Started request reaches L1 after it, the attempt is never
// admitted, no renewal reaches the helper, and the session stays intact.
func TestOCIStartRetryEndsBeforeHelperDeadman(t *testing.T) {
	const (
		nodeID         = "pre-admission-node"
		bootSessionID  = "pre-admission-boot"
		initialDeadman = 4 * time.Second // the L1 lease, and so the helper's initial deadman
		runDelay       = 1500 * time.Millisecond
	)
	base := newPreAdmissionRenewalEngine()
	close(base.releaseImage)
	close(base.releaseWatch)
	close(base.releaseDelete)
	engine := &slowStartEngine{preAdmissionRenewalEngine: base, runDelay: runDelay,
		runEntered: make(chan time.Time, 1), expired: make(chan struct{})}
	barrier, stopHelper := startPreAdmissionHelper(t, engine, time.Now)
	defer stopHelper()

	var startCalls, lateReplays atomic.Int32
	var lastStart atomic.Int64
	network := plain.NewNetwork()
	store := startInterceptedL1(t, network, nodeID, initialDeadman, func(next http.Handler, w http.ResponseWriter, r *http.Request) {
		if !strings.HasSuffix(r.URL.Path, "/started") {
			next.ServeHTTP(w, r)
			return
		}
		lastStart.Store(time.Now().UnixNano())
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
	managedRoot, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	nodeAgent, err := New(Config{
		Fabric:              network.NewFabric(fabric.Identity{NodeID: nodeID + "-fabric", Tags: []string{l1.DefaultAgentPrincipalTag}}),
		ControlPlaneAddress: "wefty://control-plane", NodeID: nodeID, BootSessionID: bootSessionID, Version: "test",
		OS: "linux", Architecture: "amd64",
		Capabilities: map[string]bool{"kind:process": true},
		CapabilityProbe: capabilityProbeFunc(func(ctx context.Context) (CapabilityProbeResult, error) {
			if err := adapter.Probe(ctx, nodeID, bootSessionID, preAdmissionImageReference, preAdmissionImageDigest, 2*time.Second); err != nil {
				return CapabilityProbeResult{}, err
			}
			return CapabilityProbeResult{Capabilities: map[string]bool{
				"kind:oci": true, "runtime_handler:" + ocihelper.DefaultRuntimeHandler: true,
			}}, nil
		}),
		OCIIntent: enabledTestOCIIntent, OCIBootBarrier: barrier,
		WorkloadRuntimes: map[string]WorkloadRuntime{contract.JobKindOCI: adapter},
		AttemptDeadman: preAdmissionDeadman{
			barrier: barrier, nodeID: nodeID, bootSessionID: bootSessionID,
			observe: func() { renewalsQueued.Add(1) },
		},
		ManagedRootDirectory: managedRoot, LogSpoolDirectory: t.TempDir(), HandoffRoot: t.TempDir(), MaxServiceSlots: 1,
		RenewalInterval: 200 * time.Millisecond, LogRetryInterval: 10 * time.Millisecond, OperationTimeout: 2 * time.Second, Logf: t.Logf,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer nodeAgent.Close()
	if _, err := nodeAgent.Register(t.Context()); err != nil {
		t.Fatal(err)
	}
	job, _, err := store.CreateJob(t.Context(), contract.JobSpec{
		SchemaVersion: contract.SchemaVersionV1, DispatchKey: "oci-admission-window",
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
	claim, err := nodeAgent.session.client.Claim(t.Context(), nodeID, bootSessionID, contract.JobClassService)
	if err != nil || claim == nil || claim.Job.JobID != job.JobID || claim.Lease.LeaseTTL != initialDeadman {
		t.Fatalf("service claim = %+v err=%v", claim, err)
	}
	session, err := barrier.Session()
	if err != nil {
		t.Fatal(err)
	}
	generation := session.Handshake().SessionGeneration

	runContext, cancelRun := context.WithCancel(t.Context())
	executed := make(chan struct{})
	go func() {
		defer close(executed)
		_, executeErr := nodeAgent.executeClaim(runContext, *claim, time.Now())
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
	// Outlast the helper's deadline for this attempt, so a renewal sent for
	// it at any point would have been rejected by now.
	if wait := time.Until(runAt.Add(initialDeadman + 500*time.Millisecond)); wait > 0 {
		time.Sleep(wait)
	}

	if calls := startCalls.Load(); calls < 2 {
		t.Fatalf("Started calls = %d, want the committed one and retries after its lost answer", calls)
	}
	if late := lateReplays.Load(); late != 0 {
		t.Fatalf("%d Started requests reached L1 after the helper expired the attempt", late)
	}
	if last := time.Unix(0, lastStart.Load()); !last.Before(runAt.Add(initialDeadman - 500*time.Millisecond)) {
		t.Fatalf("last Started request %s after helper Run, want it to end inside the admission window", last.Sub(runAt))
	}
	if queued := renewalsQueued.Load(); queued != 0 {
		t.Fatalf("%d helper deadman renewals queued for an attempt that was never admitted", queued)
	}
	current, err := barrier.Session()
	if err != nil {
		t.Fatalf("helper session after the start retry: %v", err)
	}
	if err := current.HealthError(); err != nil || current.Handshake().SessionGeneration != generation {
		t.Fatalf("helper session after the start retry = %v (generation %d, want %d)", err, current.Handshake().SessionGeneration, generation)
	}
	doctor, err := current.DoctorStatus(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if evidence := doctor.LastSessionInvalidation; evidence != nil {
		t.Fatalf("helper session invalidated: attempt=%s code=%s", evidence.AttemptID, evidence.RejectionCode)
	}
	attempts, err := store.ListJobAttempts(t.Context(), job.JobID)
	if err != nil || len(attempts) != 1 || attempts[0].Image == nil || attempts[0].Image.StartedAt == nil {
		t.Fatalf("attempt = %+v %v, want the committed start whose answers were lost", attempts, err)
	}
}
