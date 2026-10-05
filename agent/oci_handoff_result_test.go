//go:build darwin || linux

package agent

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"net/http"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/fabric/plain"
	"github.com/Derek-X-Wang/wefty/l1"
	workloadrunner "github.com/Derek-X-Wang/wefty/runner"
)

// Real L1 HTTP admission, node lifecycle, and result retrieval with an OCI
// runtime double. No L3 server, submitted L3 endpoint, run token, or mailbox
// seed exists; the node's default bridge is never used by the workload.
func TestOCIHandoffResultWithoutL3ThroughL1(t *testing.T) {
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
	runtime := &l1ResultOCIRuntime{publicationOCIRuntime: publicationOCIRuntime{fakeRunMailboxRuntime: newFakeRunMailboxRuntime()}}
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
		HeartbeatInterval: 50 * time.Millisecond, ClaimInterval: 5 * time.Millisecond, RenewalInterval: 50 * time.Millisecond, Logf: t.Logf,
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
	deadline := time.Now().Add(5 * time.Second)
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
	if string(result.Document) != `{"ok":true}` || result.SkipReason != "" || result.JobID != job.JobID || result.AttemptID == "" {
		t.Fatalf("L1 result = %+v", result)
	}
	hash := sha256.Sum256(result.Document)
	if result.SHA256 != fmt.Sprintf("%x", hash) {
		t.Fatalf("result digest = %q", result.SHA256)
	}
	var finished l1.Job
	if err := submitter.request(t.Context(), http.MethodGet, "/v1/jobs/"+job.JobID, nil, &finished); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(finished.Spec.Labels, spec.Labels) {
		t.Fatalf("submitted labels changed: %+v", finished.Spec.Labels)
	}
	if finished.State != contract.JobSucceeded || !runtime.reaped.Load() || !runtime.resultRead.Load() {
		t.Fatalf("job=%+v; reaped=%t read=%t", finished, runtime.reaped.Load(), runtime.resultRead.Load())
	}
}

type l1ResultOCIRuntime struct{ publicationOCIRuntime }

func (r *l1ResultOCIRuntime) Run(ctx context.Context, request workloadrunner.Request, sink workloadrunner.OutputSink) (workloadrunner.Result, error) {
	if request.RunMailbox != nil || request.Execution.Env[contract.EnvRunDir] != "" || request.Execution.SensitiveEnv[contract.EnvRunToken] != "" {
		return workloadrunner.Result{}, fmt.Errorf("OCI result fixture unexpectedly carries L3 context")
	}
	digest := *request.Execution.OCI.Image.Digest
	observation := workloadrunner.OCIImageObservation{SubmittedReference: request.Execution.OCI.Image.Reference,
		TopLevelDigest: digest, TopLevelMediaType: "application/vnd.oci.image.manifest.v1+json", PlatformManifestDigest: digest,
		PlatformOS: "linux", PlatformArchitecture: "amd64", RuntimeHandler: request.RuntimeHandler, Snapshotter: "overlayfs",
	}
	if err := request.OCIStarted(ctx, observation); err != nil {
		return workloadrunner.Result{}, err
	}
	if err := admitReadyOCIHelper(request); err != nil {
		return workloadrunner.Result{}, err
	}
	return r.captureRuntime.Run(ctx, request, sink)
}

// One owner must govern admission, locking, reading, publication and eviction.
// The runtime double rejects a reader whose owner differs from its volume.
func TestDirectOCIHandoffOwnerPublicationAndEviction(t *testing.T) {
	for _, tc := range []struct {
		name      string
		status    int
		published bool
	}{
		{"uploaded", 0, true}, {"upload_failed", http.StatusConflict, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newRetentionHarness(t, time.Hour)
			claim := ociHandoffClaim("unused", "attempt-direct")
			claim.Job.JobID = "job_direct"
			claim.Job.Spec.Labels = nil
			claim.Job.Spec.Execution.Env = nil
			claim.Job.Spec.Execution.SensitiveEnv = nil
			claim.SubmittedByRunLedger = false
			before, _ := json.Marshal(claim.Job.Spec)
			lifecycle := uploadingLifecycle(t, h.manager, &resultUploadRecorder{status: tc.status}, successfulRetentionRun)
			runtime := &publicationOCIRuntime{fakeRunMailboxRuntime: newFakeRunMailboxRuntime()}
			lifecycle.dependencies.runtimes = workloadRuntimeSet{contract.JobKindOCI: runtime}
			if _, err := lifecycle.execute(t.Context(), claim, time.Now()); err != nil {
				t.Fatal(err)
			}
			after, _ := json.Marshal(claim.Job.Spec)
			if string(before) != string(after) {
				t.Fatal("submitted spec changed")
			}
			if got := handoffOwnerRunIDFromRequest(runtime.request); got != claim.Job.JobID {
				t.Fatalf("volume owner=%q", got)
			}
			if runtime.request.RunMailbox != nil || !runtime.resultRead.Load() || !runtime.reaped.Load() {
				t.Fatal("result not read before reap without mailbox")
			}
			record, found, err := h.manager.readOCIRecord(claim.Job.JobID)
			if err != nil || !found || record.OwnerKey != claim.Job.JobID || record.AttemptID != claim.Lease.AttemptID || record.live() || record.evidenceReachedLedger() != tc.published {
				t.Fatalf("admission/retention=%+v found=%t err=%v", record, found, err)
			}
			upload := requireUploadRecord(t, h.manager, claim.Job.JobID)
			if upload.publishes() != tc.published || !upload.MailboxDrained || upload.AttemptID != claim.Lease.AttemptID {
				t.Fatalf("upload=%+v", upload)
			}
			// Hold the producing owner's lease again: eviction must use that same key.
			helper := &fakeHelperHandoffRoot{volumes: []workloadrunner.RetainedHandoffVolume{ociVolume(t, claim.Job.JobID, 4096, 2, h.now)}}
			h.manager.ociHandoffs, h.manager.ociEvictor = helper, helper
			published, unpublished := h.manager.evictionCandidates(h.account())
			if (len(published) == 1) != tc.published || (len(unpublished) == 1) == tc.published {
				t.Fatalf("published=%v unpublished=%v", published, unpublished)
			}
			lease, err := h.manager.lockOCIHandoff(t.Context(), claim.Job.JobID)
			if err != nil {
				t.Fatal(err)
			}
			h.budget(1)
			if len(helper.evicted) != 0 {
				t.Fatal("locked owner evicted")
			}
			lease.release()
			h.budget(1)
			if len(helper.evicted) != 1 || helper.evicted[0] != claim.Job.JobID {
				t.Fatalf("evicted=%v", helper.evicted)
			}
		})
	}
}
