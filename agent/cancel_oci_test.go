//go:build darwin || linux

package agent

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"net/http"
	"path/filepath"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/fabric/plain"
	"github.com/Derek-X-Wang/wefty/l1"
	workloadrunner "github.com/Derek-X-Wang/wefty/runner"
)

func TestCancelOCIHandoffResultThroughDirectApp(t *testing.T) {
	for _, phase := range []string{"image_preparation", "observation", "helper_admission", "started"} {
		t.Run(phase, func(t *testing.T) {
			network := plain.NewNetwork()
			_, stopServer := startFailureServer(t, network, nil, map[string][]string{"node-1": {"oci-result"}})
			defer stopServer()
			submitter, err := NewClient(network.NewFabric(fabric.Identity{NodeID: "ordinary-app", Tags: []string{l1.DefaultClientPrincipalTag}}), "wefty://control-plane")
			if err != nil {
				t.Fatal(err)
			}
			defer submitter.Close()
			digest := "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
			spec := contract.JobSpec{SchemaVersion: contract.SchemaVersionV1, DispatchKey: "handoff-no-l3", Kind: contract.JobKindOCI, Class: contract.JobClassOneShot,
				RoutingTags: []string{"oci-result"}, RuntimeHandler: "io.containerd.runc.v2",
				Execution: contract.ExecutionSpec{OCI: &contract.OCIExecutionSpec{Image: contract.OCIImageSpec{Reference: "example.invalid/result:v1", Digest: &digest}, Argv: []string{"/payload"}}},
			}
			var job l1.Job
			if err := submitter.post(t.Context(), "/v1/jobs", spec, &job); err != nil {
				t.Fatal(err)
			}
			root, err := filepath.EvalSymlinks(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			runtime := &cancelResultOCIRuntime{phase: phase, ready: make(chan struct{}), release: make(chan struct{}), publicationOCIRuntime: publicationOCIRuntime{fakeRunMailboxRuntime: newFakeRunMailboxRuntime()}}
			if phase == "image_preparation" {
				runtime.readErr = errors.New("helper attempt not admitted during image preparation")
			}
			heartbeatInterval := 10 * time.Millisecond
			if phase == "observation" {
				// Pull finishes before either directive channel can deliver;
				// only the committing image checkpoint can prevent helper Run.
				heartbeatInterval = time.Hour
			}
			nodeAgent, err := New(Config{
				Fabric: network.NewFabric(fabric.Identity{NodeID: "agent", Tags: []string{l1.DefaultAgentPrincipalTag}}), ControlPlaneAddress: "wefty://control-plane",
				NodeID: "node-1", BootSessionID: "boot-no-l3", Version: "test", OS: "linux", Architecture: "amd64",
				Capabilities: map[string]bool{"kind:oci": true, "runtime_handler:io.containerd.runc.v2": true},
				CapabilityProbe: capabilityProbeFunc(func(context.Context) (CapabilityProbeResult, error) {
					return CapabilityProbeResult{Capabilities: map[string]bool{"kind:oci": true, "runtime_handler:io.containerd.runc.v2": true}}, nil
				}),
				AttemptDeadman: newRecordingDeadmanRenewer(), OCIBootBarrier: readyOCIBootBarrier{},
				OCIIntent: func(context.Context) (OCIIntentObservation, error) {
					return OCIIntentObservation{Enabled: true, Revision: 1}, nil
				},
				WorkloadRuntimes: map[string]WorkloadRuntime{contract.JobKindOCI: runtime}, ManagedRootDirectory: root, LogSpoolDirectory: t.TempDir(), HandoffRoot: t.TempDir(),
				HeartbeatInterval: heartbeatInterval, ClaimInterval: 5 * time.Millisecond, RenewalInterval: time.Hour, Logf: t.Logf,
			})
			if err != nil {
				t.Fatal(err)
			}
			defer nodeAgent.Close()
			ctx, cancel := context.WithCancel(t.Context())
			done := make(chan error, 1)
			go func() { done <- nodeAgent.Run(ctx) }()
			defer func() {
				cancel()
				if err := <-done; err != nil {
					t.Errorf("agent Run: %v", err)
				}
			}()
			// The observation payload holds until release. A failure before the
			// cancel commits must still release it, or the shutdown above waits
			// on the held attempt until the package timeout.
			var releaseOnce sync.Once
			release := func() { releaseOnce.Do(func() { close(runtime.release) }) }
			defer release()
			select {
			case <-runtime.ready:
			case <-time.After(5 * time.Second * raceTimeoutScale):
				t.Fatal("OCI never reached cancellation edge")
			}
			var pending l1.Job
			if err := submitter.post(t.Context(), "/v1/jobs/"+job.JobID+"/cancel", nil, &pending); err != nil {
				t.Fatal(err)
			}
			release()
			if pending.Outcome != "canceled" {
				t.Fatalf("pending=%+v", pending)
			}
			deadline := time.Now().Add(5 * time.Second * raceTimeoutScale)
			var result l1.JobResult
			for {
				err = submitter.request(t.Context(), http.MethodGet, "/v1/jobs/"+job.JobID+"/result", nil, &result)
				if err == nil {
					break
				}
				if time.Now().After(deadline) {
					t.Fatalf("L1 result unavailable: %v; agent=%+v", err, nodeAgent.Status())
				}
				time.Sleep(5 * time.Millisecond)
			}
			if phase == "image_preparation" {
				if result.SkipReason != contract.ResultUploadSkipUnreadable || len(result.Document) != 0 {
					t.Fatalf("unadmitted handoff=%+v", result)
				}
			} else if phase == "observation" {
				if runtime.payloadRuns.Load() != 0 {
					t.Fatalf("canceled observation called helper Run %d times", runtime.payloadRuns.Load())
				}
				if result.SkipReason != contract.ResultUploadSkipAbsent || len(result.Document) != 0 {
					t.Fatalf("never-started payload result=%+v", result)
				}
			} else {
				if string(result.Document) != `{"ok":true}` || result.SkipReason != "" || result.JobID != job.JobID || result.AttemptID == "" {
					t.Fatalf("L1 result=%+v", result)
				}
				hash := sha256.Sum256(result.Document)
				if result.SHA256 != fmt.Sprintf("%x", hash) {
					t.Fatalf("result digest=%q", result.SHA256)
				}
			}
			var finished l1.Job
			if err := submitter.request(t.Context(), http.MethodGet, "/v1/jobs/"+job.JobID, nil, &finished); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(finished.Spec.Labels, spec.Labels) {
				t.Fatalf("submitted labels changed: %+v", finished.Spec.Labels)
			}
			if finished.State != contract.JobFailed || finished.Outcome != "canceled" || !runtime.reaped.Load() || runtime.resultRead.Load() != (phase != "observation") {
				t.Fatalf("job=%+v; reaped=%t read=%t", finished, runtime.reaped.Load(), runtime.resultRead.Load())
			}
			for {
				record, found, err := nodeAgent.handoffs.readOCIRecord(job.JobID)
				if err == nil && found && !record.live() && record.evidenceReachedLedger() == (phase != "image_preparation") {
					break
				}
				if time.Now().After(deadline) {
					t.Fatalf("cancel retention=%+v found=%t err=%v", record, found, err)
				}
				time.Sleep(5 * time.Millisecond)
			}

		})
	}
}

type cancelResultOCIRuntime struct {
	publicationOCIRuntime
	phase       string
	ready       chan struct{}
	release     chan struct{}
	payloadRuns atomic.Int32
	readyOnce   sync.Once
}

func (r *cancelResultOCIRuntime) Run(ctx context.Context, request workloadrunner.Request, sink workloadrunner.OutputSink) (workloadrunner.Result, error) {
	r.request = request
	if request.RunMailbox != nil {
		return workloadrunner.Result{}, errors.New("direct cancellation unexpectedly requires L3")
	}
	if r.phase == "observation" {
		r.readyOnce.Do(func() { close(r.ready) })
		<-r.release // Cancel is committed before image preparation finishes.
	} else if r.phase != "started" {
		r.readyOnce.Do(func() { close(r.ready) })
		<-ctx.Done()
		if r.phase == "image_preparation" {
			return workloadrunner.Result{Outcome: contract.ProcessResult{SpawnError: &contract.SpawnFailure{Code: contract.SpawnFailureRuntimeUnavailable, Message: "canceled image preparation"}}}, ctx.Err()
		}
	}
	digest := *request.Execution.OCI.Image.Digest
	observation := workloadrunner.OCIImageObservation{SubmittedReference: request.Execution.OCI.Image.Reference,
		TopLevelDigest: digest, TopLevelMediaType: "application/vnd.oci.image.manifest.v1+json", PlatformManifestDigest: digest,
		PlatformOS: "linux", PlatformArchitecture: "amd64", RuntimeHandler: request.RuntimeHandler, Snapshotter: "overlayfs"}
	if r.phase == "observation" {
		if err := request.OCIImageResolved(context.WithoutCancel(ctx), observation); err != nil {
			return workloadrunner.Result{Outcome: contract.ProcessResult{SpawnError: &contract.SpawnFailure{Code: contract.SpawnFailureProcessRequest, Message: err.Error()}}}, err
		}
	}
	r.payloadRuns.Add(1) // Fake helper Run: only reachable after image observation.
	if err := request.OCIStarted(context.WithoutCancel(ctx), observation); err != nil {
		return workloadrunner.Result{Outcome: contract.ProcessResult{SpawnError: &contract.SpawnFailure{Code: contract.SpawnFailureProcessRequest, Message: err.Error()}}}, err
	}
	if err := admitReadyOCIHelper(request); err != nil {
		return workloadrunner.Result{}, err
	}
	r.readyOnce.Do(func() { close(r.ready) })
	<-ctx.Done()
	zero := 0
	return workloadrunner.Result{Outcome: contract.ProcessResult{ExitCode: &zero, TerminationInitiator: contract.TerminationCauseAgent}}, nil
}

func TestCancelDirectivesOnlyStopExactOCIOneShot(t *testing.T) {
	for _, mismatch := range []string{"", "job", "attempt", "fence", "service"} {
		t.Run(mismatch, func(t *testing.T) {
			ctx, cancel := context.WithCancelCause(t.Context())
			defer cancel(nil)
			session := &agentSession{resident: map[string]*residentAttempt{"job": {class: contract.JobClassOneShot, kind: contract.JobKindOCI, attemptID: "attempt", fencingToken: "fence", cancel: cancel}}}
			directive := l1.OneShotCancelDirective{JobID: "job", AttemptID: "attempt", FencingToken: "fence"}
			switch mismatch {
			case "job":
				directive.JobID = "successor"
			case "attempt":
				directive.AttemptID = "successor"
			case "fence":
				directive.FencingToken = "successor"
			case "service":
				session.resident["job"].class = contract.JobClassService
			}
			session.processCancelDirectives([]l1.OneShotCancelDirective{directive})
			if (context.Cause(ctx) == errAttemptDirectiveCancel) != (mismatch == "") {
				t.Fatalf("exact resident cancellation cause=%v", context.Cause(ctx))
			}
		})
	}
}

func TestCancelFailedSignalDeliveryIsNotConfirmedTermination(t *testing.T) {
	result := contract.ProcessResult{RuntimeFailure: &contract.RuntimeFailure{Code: contract.RuntimeFailureUnavailable, Message: "TERM and KILL could not be delivered"}}
	got := agentTerminatedResult(result)
	if got.RuntimeFailure == nil || got.Signal != "" || got.TerminationCause != "" || got.TerminationInitiator != "" {
		t.Fatalf("failed delivery became confirmed termination=%+v", got)
	}
}

func TestCancelOCIFailedUploadRetainsUnpublishedHandoff(t *testing.T) {
	h := newRetentionHarness(t, time.Hour)
	claim := ociHandoffClaim("unused", "cancel-upload-attempt")
	claim.Job.JobID = "job-cancel-upload"
	claim.Job.Spec.Labels = nil
	claim.Job.Spec.Execution.Env = nil
	claim.Job.Spec.Execution.SensitiveEnv = nil
	claim.SubmittedByRunLedger = false
	recorder := &resultUploadRecorder{status: http.StatusConflict}
	lifecycle := uploadingLifecycle(t, h.manager, recorder, successfulRetentionRun)
	ctx, cancel := context.WithCancelCause(t.Context())
	defer cancel(nil)
	runtime := &cancelUploadOCIRuntime{publicationOCIRuntime: publicationOCIRuntime{fakeRunMailboxRuntime: newFakeRunMailboxRuntime()}, cancel: cancel}
	lifecycle.dependencies.runtimes = workloadRuntimeSet{contract.JobKindOCI: runtime}
	if _, err := lifecycle.execute(ctx, claim, time.Now()); err != nil {
		t.Fatal(err)
	}
	if !runtime.canceled.Load() {
		t.Fatal("OCI heartbeat did not cancel the resident attempt")
	}
	record, found, err := h.manager.readOCIRecord(claim.Job.JobID)
	if err != nil || !found || record.live() || record.evidenceReachedLedger() {
		t.Fatalf("failed upload retention=%+v found=%t err=%v", record, found, err)
	}
	if !runtime.resultRead.Load() || !runtime.reaped.Load() {
		t.Fatal("cancellation skipped result capture before reap")
	}
	upload := requireUploadRecord(t, h.manager, claim.Job.JobID)
	if upload.publishes() || upload.Reason != contract.ResultUploadSkipTransport {
		t.Fatalf("unconfirmed upload=%+v", upload)
	}
	requests, _ := recorder.observed()
	if len(requests) != 1 || requests[0].FencingToken != claim.Lease.FencingToken || string(requests[0].Document) != `{"ok":true}` {
		t.Fatalf("fenced upload=%+v", requests)
	}
}

type cancelUploadOCIRuntime struct {
	publicationOCIRuntime
	cancel   context.CancelCauseFunc
	canceled atomic.Bool
}

func (r *cancelUploadOCIRuntime) Run(ctx context.Context, request workloadrunner.Request, _ workloadrunner.OutputSink) (workloadrunner.Result, error) {
	r.request = request
	session := &agentSession{resident: map[string]*residentAttempt{request.Authority.JobID: {
		class: contract.JobClassOneShot, kind: contract.JobKindOCI,
		attemptID: request.Authority.AttemptID, fencingToken: request.Authority.FencingToken, cancel: r.cancel,
	}}}
	session.processCancelDirectives([]l1.OneShotCancelDirective{{JobID: request.Authority.JobID, AttemptID: request.Authority.AttemptID, FencingToken: request.Authority.FencingToken}})
	// The mailbox fence forwards resident cancellation to execution asynchronously.
	select {
	case <-ctx.Done():
		r.canceled.Store(true)
	case <-time.After(time.Second):
		return workloadrunner.Result{}, errors.New("OCI heartbeat did not cancel the resident attempt")
	}
	return workloadrunner.Result{Outcome: contract.ProcessResult{Signal: "terminated", TerminationCause: contract.TerminationCauseAgent}}, nil
}
