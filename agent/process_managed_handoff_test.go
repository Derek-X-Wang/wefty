//go:build darwin || linux

package agent

import (
	"context"
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/fabric/plain"
	"github.com/Derek-X-Wang/wefty/l1"
	processrunner "github.com/Derek-X-Wang/wefty/runner/process"
)

func TestProcessManagedHandoffThroughL1WithoutL3(t *testing.T) {
	network := plain.NewNetwork()
	_, stop := startFailureServer(t, network, nil, map[string][]string{"node-1": {"process-result"}})
	defer stop()
	submitter, err := NewClient(network.NewFabric(fabric.Identity{NodeID: "ordinary-app", Tags: []string{l1.DefaultClientPrincipalTag}}), "wefty://control-plane")
	if err != nil {
		t.Fatal(err)
	}
	defer submitter.Close()
	spec := contract.JobSpec{SchemaVersion: 1, DispatchKey: "managed-process-http", Kind: contract.JobKindProcess, Class: contract.JobClassOneShot,
		RoutingTags: []string{"process-result"}, Labels: map[string]string{"app": "test"}, Execution: contract.ExecutionSpec{
			Executable: contract.ExecutableSpec{Path: "/bin/sh"}, Argv: []string{"sh", "-c", `test -z "$WEFTY_RUN_DIR" && test -z "$WEFTY_RUN_ID" && printf '{"dir":"%s"}' "$WEFTY_HANDOFF_DIR" > "$WEFTY_HANDOFF_DIR/result.json"`}, WorkingDirectory: t.TempDir(),
			Env: map[string]string{contract.EnvHandoffDir: "forged-public"}, SensitiveEnv: map[string]string{contract.EnvHandoffDir: "forged-secret"},
		}}
	var job l1.Job
	if err := submitter.post(t.Context(), "/v1/jobs", spec, &job); err != nil {
		t.Fatal(err)
	}
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	node, err := New(Config{
		Fabric:              network.NewFabric(fabric.Identity{NodeID: "agent", Tags: []string{l1.DefaultAgentPrincipalTag}}),
		ControlPlaneAddress: "wefty://control-plane", NodeID: "node-1", BootSessionID: "managed-process", Version: "test",
		OS: "darwin", Architecture: "arm64", Capabilities: map[string]bool{"kind:process": true},
		ManagedRootDirectory: root, HandoffRoot: filepath.Join(root, "handoffs"), LogSpoolDirectory: t.TempDir(),
		HeartbeatInterval: 50 * time.Millisecond, ClaimInterval: 5 * time.Millisecond, RenewalInterval: 50 * time.Millisecond, Logf: t.Logf,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer node.Close()
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- node.Run(ctx) }()
	defer func() {
		cancel()
		if err := <-done; err != nil {
			t.Errorf("Run: %v", err)
		}
	}()
	var result l1.JobResult
	deadline := time.Now().Add(10 * time.Second)
	for {
		err = submitter.request(t.Context(), http.MethodGet, "/v1/jobs/"+job.JobID+"/result", nil, &result)
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("result unavailable: %v status=%+v", err, node.Status())
		}
		time.Sleep(5 * time.Millisecond)
	}
	path := filepath.Join(root, "handoffs", job.JobID)
	var document struct {
		Dir string `json:"dir"`
	}
	if err := json.Unmarshal(result.Document, &document); err != nil {
		t.Fatal(err)
	}
	if document.Dir != path || result.JobID != job.JobID || result.AttemptID == "" || result.SkipReason != "" {
		t.Fatalf("result=%+v dir=%q want=%q", result, document.Dir, path)
	}
	var stored l1.Job
	if err := submitter.request(t.Context(), http.MethodGet, "/v1/jobs/"+job.JobID, nil, &stored); err != nil {
		t.Fatal(err)
	}
	if stored.State != contract.JobSucceeded || stored.Spec.Execution.HandoffDirectory != "" || !reflect.DeepEqual(stored.Spec.Labels, spec.Labels) {
		t.Fatalf("stored job=%+v", stored)
	}
	var replay l1.Job
	if err := submitter.post(t.Context(), "/v1/jobs", spec, &replay); err != nil || replay.JobID != job.JobID {
		t.Fatalf("identical replay=%+v err=%v", replay, err)
	}
	// L1 accepts the upload before the agent commits its local retention fact.
	for {
		records := node.handoffs.loadRecords()
		if len(records) == 1 && !records[0].RetainUntil.IsZero() {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("local retention did not finalize")
		}
		time.Sleep(5 * time.Millisecond)
	}
	record := requireRetentionRecord(t, node.handoffs, job.JobID)
	if record.Directory != path || record.HandoffOwnerKey != job.JobID || !record.evidenceReachedLedger() {
		t.Fatalf("retention=%+v", record)
	}
	upload := requireUploadRecord(t, node.handoffs, job.JobID)
	if !upload.publishes() || !upload.MailboxDrained {
		t.Fatalf("upload=%+v", upload)
	}
}

func TestProcessManagedHandoffPublicationAndCleanup(t *testing.T) {
	for _, tc := range []struct {
		name      string
		write     bool
		status    int
		published bool
		labels    map[string]string
		owner     string
	}{
		{"document", true, 0, true, nil, "job_managed"}, {"absent", false, 0, true, nil, "job_managed"}, {"upload_refused", true, http.StatusConflict, false, nil, "job_managed"},
		{"run_path_omitted", true, 0, true, map[string]string{contract.LabelRunID: "run_managed"}, "run_managed"},
		{"rerun_path_omitted", true, 0, true, map[string]string{contract.LabelRunID: "run_rerun", contract.LabelHandoffOwnerRunID: "run_source"}, "run_source"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newRetentionHarness(t, time.Hour)
			claim := retentionClaim(t, "", "")
			claim.Job.JobID = "job_managed"
			claim.Job.Spec.Labels = tc.labels
			if tc.labels == nil {
				claim.Job.Spec.Labels = map[string]string{"app": "test"}
			}
			before, _ := json.Marshal(claim.Job.Spec)
			path := filepath.Join(h.root, tc.owner)
			run := completionDirectiveRunFunc(func(ctx context.Context, request processrunner.Request, sink processrunner.OutputSink) (contract.ProcessResult, error) {
				if request.Execution.HandoffDirectory != path || request.Execution.Env[contract.EnvHandoffDir] != path {
					t.Fatalf("execution=%+v", request.Execution)
				}
				if tc.write {
					if err := os.WriteFile(filepath.Join(path, "result.json"), []byte(`{"ok":true}`), 0o600); err != nil {
						return contract.ProcessResult{}, err
					}
				}
				return successfulRetentionRun(ctx, request, sink)
			})
			lifecycle := uploadingLifecycle(t, h.manager, &resultUploadRecorder{status: tc.status}, run)
			if _, err := lifecycle.execute(t.Context(), claim, time.Now()); err != nil {
				t.Fatal(err)
			}
			after, _ := json.Marshal(claim.Job.Spec)
			if string(before) != string(after) {
				t.Fatal("submitted spec changed")
			}
			record := requireRetentionRecord(t, h.manager, tc.owner)
			if record.Directory != path || record.HandoffOwnerKey != tc.owner || record.evidenceReachedLedger() != tc.published {
				t.Fatalf("retention=%+v", record)
			}
			upload := requireUploadRecord(t, h.manager, tc.owner)
			if upload.publishes() != tc.published || !upload.MailboxDrained {
				t.Fatalf("upload=%+v", upload)
			}
			h.now = h.now.Add(time.Hour)
			if err := h.manager.collect(); err != nil {
				t.Fatal(err)
			}
			if _, err := os.Stat(path); !os.IsNotExist(err) {
				t.Fatalf("managed directory survived expiry: %v", err)
			}
		})
	}
}
