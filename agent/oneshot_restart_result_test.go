//go:build darwin || linux

package agent

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/fabric/plain"
	"github.com/Derek-X-Wang/wefty/l1"
	"github.com/Derek-X-Wang/wefty/l3"
	processrunner "github.com/Derek-X-Wang/wefty/runner/process"
)

type restartResultExecutor struct{ run completionDirectiveRunFunc }

func (restartResultExecutor) GuardianAvailable() bool { return true }
func (e restartResultExecutor) Run(ctx context.Context, req processrunner.Request, sink processrunner.OutputSink) (contract.ProcessResult, error) {
	return e.run(ctx, req, sink)
}

func TestRestartablePredecessorCannotOverwriteUploadedResult(t *testing.T) {
	for _, phase := range []string{"after preparation", "after result publication"} {
		t.Run(phase, func(t *testing.T) {
			harness := newRetentionHarness(t, time.Hour)
			recorder := &resultUploadRecorder{}
			path := filepath.Join(harness.root, "run_isolated")
			claim := retentionClaim(t, "run_isolated", path)
			claim.Job.Spec.Restartable = true
			claim.Lease.AttemptID = "successor"
			var previous string
			var lifecycle *attemptLifecycle
			run := func(ctx context.Context, req processrunner.Request, sink processrunner.OutputSink) (contract.ProcessResult, error) {
				previous = filepath.Join(path, restartableResultPath("previous"), handoffResultName)
				if err := os.MkdirAll(filepath.Dir(previous), 0700); err != nil {
					return contract.ProcessResult{}, err
				}
				successor := filepath.Join(req.Execution.Env[contract.EnvHandoffDir], handoffResultName)
				if successor != filepath.Join(path, restartableResultPath("successor"), handoffResultName) {
					return contract.ProcessResult{}, fmt.Errorf("shared result path: %s", successor)
				}
				if phase == "after preparation" {
					if err := os.WriteFile(previous, []byte(`{"exit_code":0}`), 0600); err != nil {
						return contract.ProcessResult{}, err
					}
					result, _ := lifecycle.attemptResult(claim)
					if result.skip != contract.ResultUploadSkipAbsent {
						return contract.ProcessResult{}, fmt.Errorf("predecessor inherited: %+v", result)
					}
				}
				if err := os.WriteFile(successor, []byte(`{"exit_code":7}`), 0600); err != nil {
					return contract.ProcessResult{}, err
				}
				if err := os.WriteFile(previous, []byte(`{"exit_code":0}`), 0600); err != nil {
					return contract.ProcessResult{}, err
				}
				return successfulRetentionRun(ctx, req, sink)
			}
			lifecycle = uploadingLifecycle(t, harness.manager, recorder, nil)
			lifecycle.dependencies.runtimes = testRuntimeSet(restartResultExecutor{run})
			if _, err := lifecycle.execute(t.Context(), claim, time.Now()); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(previous, []byte(`{"exit_code":99}`), 0600); err != nil {
				t.Fatal(err)
			}
			// A late predecessor write after upload cannot mutate the ledger's copy.
			lifecycle.uploadResult(t.Context(), claim)
			requests, paths := recorder.observed()
			if len(requests) != 1 || string(requests[0].Document) != `{"exit_code":7}` || requests[0].FencingToken != claim.Lease.FencingToken || !strings.HasSuffix(paths[0], "/attempts/successor/result") {
				t.Fatalf("uploads=%+v paths=%v", requests, paths)
			}
		})
	}
}

// Exercise the public L3 result API and real L1 transitions. Checkout/capture
// fetch is covered by acceptance; retain the shipped command/wait/EXIT path
// here and block only the cleanup helper after the first command exits 7.
func TestRestartableCleanupInterruptionUsesSuccessorVerdict(t *testing.T) {
	dir := t.TempDir()
	once, blocked, ready, release := filepath.Join(dir, "once"), filepath.Join(dir, "cleanup-blocked"), filepath.Join(dir, "successor-ready"), filepath.Join(dir, "release")
	raw, err := os.ReadFile("../cmd/wefty/exec_wrapper.sh")
	if err != nil {
		t.Fatal(err)
	}
	script := string(raw)
	input := fmt.Sprintf(`storage="$WEFTY_HANDOFF_DIR/work-test"
mkdir -m 700 -- "$storage" || exit 1
folder=.
tmp_link=%q
command=(/bin/sh -c 'if [ ! -f "$1" ]; then touch "$1"; exit 7; fi; touch "$2"; while [ ! -f "$3" ]; do sleep 0.01; done; exit 0' sh %q %q %q)
`, dir, once, ready, release)
	script = strings.Replace(script, "__WEFTY_EXEC_INPUT__", input, 1)
	script = strings.Replace(script, "__WEFTY_EXEC_CLEANUP__", fmt.Sprintf(`retire_exec_storage() { if [ "$code" -eq 7 ]; then : > %q; while :; do sleep 0.01; done; fi; }`, blocked), 1)
	begin, end := strings.Index(script, `[ -d "$storage/snapshot.git" ]`), strings.Index(script, `: > "$started"`)
	if begin < 0 || end < begin {
		t.Fatal("wrapper preparation seam changed")
	}
	script = script[:begin] + script[end:]
	script = strings.Replace(script, "#!/bin/bash\n", "#!/bin/bash\n"+l3.ExecCaptureScriptMarker+"\n", 1)
	clock := newManualClock(mailboxTestClockOrigin)
	store, err := l1.OpenStore(filepath.Join(dir, "l1.sqlite"), l1.StoreOptions{Clock: clock})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	network := plain.NewNetwork()
	control := network.NewFabric(fabric.Identity{NodeID: "control-plane"})
	ledger := network.NewFabric(fabric.Identity{NodeID: "run-ledger", Tags: []string{l1.DefaultClientPrincipalTag}})
	node := network.NewFabric(fabric.Identity{NodeID: "agent", Tags: []string{l1.DefaultAgentPrincipalTag}})
	caller := network.NewFabric(fabric.Identity{NodeID: "caller", Tags: []string{l3.DefaultCallerPrincipalTag}})
	one, err := l1.NewServer(control, store, l1.ServerConfig{})
	if err != nil {
		t.Fatal(err)
	}
	oneListener, err := control.Listen("tcp", l3.DefaultL1Address)
	if err != nil {
		t.Fatal(err)
	}
	upstream, err := l3.NewL1Client(ledger, l3.DefaultL1Address)
	if err != nil {
		t.Fatal(err)
	}
	defer upstream.CloseIdleConnections()
	runs, err := l3.OpenStore(filepath.Join(dir, "l3.sqlite"), l3.StoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer runs.Close()
	reconciler, err := l3.NewReconciler(runs, upstream, l3.ReconcilerConfig{})
	if err != nil {
		t.Fatal(err)
	}
	three, err := l3.NewServer(ledger, runs, l3.ServerConfig{Jobs: upstream, Results: upstream})
	if err != nil {
		t.Fatal(err)
	}
	threeListener, err := ledger.Listen("tcp", l3.DefaultL3Address)
	if err != nil {
		t.Fatal(err)
	}
	serveContext, stopServe := context.WithCancel(t.Context())
	served := make(chan error, 2)
	go func() { served <- one.Serve(serveContext, oneListener) }()
	go func() { served <- three.Serve(serveContext, threeListener) }()
	defer func() {
		stopServe()
		for range 2 {
			if err := <-served; err != nil {
				t.Error(err)
			}
		}
	}()
	httpClient := newHTTPClient(caller, l3.DefaultL3Address)
	defer httpClient.CloseIdleConnections()
	exchange := func(method, path string, body []byte, answer any) int {
		t.Helper()
		request, err := http.NewRequestWithContext(t.Context(), method, "http://wefty.invalid"+path, bytes.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set("Idempotency-Key", "cleanup-restart")
		response, err := httpClient.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		defer response.Body.Close()
		raw, err := io.ReadAll(response.Body)
		if err != nil {
			t.Fatal(err)
		}
		if response.StatusCode >= 400 && response.StatusCode != http.StatusNotFound {
			t.Logf("%s %s: %d %s", method, path, response.StatusCode, raw)
		}
		if response.StatusCode >= 200 && response.StatusCode < 300 && answer != nil {
			if err := json.Unmarshal(raw, answer); err != nil {
				t.Fatalf("decode %s: %v %s", path, err, raw)
			}
		}
		return response.StatusCode
	}
	payload := []byte("frozen capture (fetch is outside this test)")
	digest := sha256.Sum256(payload)
	hash := hex.EncodeToString(digest[:])
	if status := exchange(http.MethodPost, "/v1/exec-captures/"+hash, payload, nil); status != http.StatusCreated {
		t.Fatalf("capture upload=%d", status)
	}
	params, _ := json.Marshal(map[string]any{"exec_version": 2, "restartable": true, "capture_sha256": hash, "capture_size_bytes": len(payload), "argv": []string{"frozen command"}, "base_commit": strings.Repeat("0", 40), "snapshot_tree": strings.Repeat("1", 40)})
	mode := uint32(0700)
	body, _ := json.Marshal(l3.CreateRunRequest{Restartable: true, Tags: []string{contract.StableNodeTagPrefix + "node"}, Params: params, InlineScript: &l3.InlineScriptInput{Content: script, SHA256: fmt.Sprintf("%x", sha256.Sum256([]byte(script))), Interpreter: []string{"/bin/bash"}, Mode: &mode}})
	var accepted l3.RunAccepted
	if status := exchange(http.MethodPost, "/v1/runs", body, &accepted); status != http.StatusCreated {
		t.Fatalf("create run=%d", status)
	}
	registration := contract.NodeRegistration{NodeID: "node", BootSessionID: "boot", OS: "linux", Architecture: "amd64", AgentVersion: "test", Capabilities: map[string]bool{"kind:process": true, l1.CapabilityOneshotRestartable: true, contract.CapabilityExecCaptureBundle: true}}
	if _, err := store.RegisterNode(t.Context(), fabric.Identity{NodeID: "agent"}, registration, l1.DefaultNodePolicy(contract.StableNodeTagPrefix+"node"), true); err != nil {
		t.Fatal(err)
	}
	if err := reconciler.ReconcileOnce(t.Context()); err != nil {
		t.Fatal(err)
	}
	claimNext := func() l1.Claim {
		t.Helper()
		claim, err := store.ClaimJob(t.Context(), "agent", "node", "boot", contract.JobClassOneShot)
		if err != nil || claim == nil {
			t.Fatalf("successor claim unavailable: %v", err)
		}
		// The script is already frozen by L3. Bypass checkout preparation only.
		claim.Job.Spec.Execution.Executable = contract.ExecutableSpec{Path: "/bin/bash"}
		claim.Job.Spec.Execution.Argv = []string{"bash", "-c", script}
		claim.Job.Spec.Execution.WorkingDirectory = dir
		return *claim
	}
	client, err := NewClient(node, l3.DefaultL1Address)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	manager := newHandoffManager(filepath.Join(dir, "handoffs"), t.TempDir(), "node", time.Hour, nil)
	runtime := processrunner.New(processrunner.Config{GuardianExecutable: agentBinaryPath, TerminationGraceTime: 50 * time.Millisecond})
	sink := &recordingOutputSink{}
	newLifecycle := func() *attemptLifecycle {
		return newAttemptLifecycle(attemptLifecycleDependencies{client: client, handoffs: manager, runtimes: testRuntimeSet(runtime), nodeID: "node", bootSessionID: "boot", clock: systemClock{}, renewalInterval: time.Hour, observer: newLifecycleObserver(systemClock{}), logf: t.Logf, outputSinkFactory: func(l1.Claim) processrunner.OutputSink { return sink }})
	}
	first := claimNext()
	firstLifecycle := newLifecycle()
	firstContext, interrupt := context.WithCancelCause(t.Context())
	defer interrupt(nil)
	firstDone := make(chan error, 1)
	go func() { _, err := firstLifecycle.execute(firstContext, first, time.Now()); firstDone <- err }()
	waitRestartRecoveryPath(t, blocked)
	predecessorResult := filepath.Join(manager.root, accepted.RunID, restartableResultPath(first.Lease.AttemptID), handoffResultName)
	if raw, err := os.ReadFile(predecessorResult); err != nil || !bytes.Contains(raw, []byte(`"exit_code":7`)) {
		t.Fatalf("command did not exit 7 before cleanup: %s %v", raw, err)
	}
	if err := reconciler.ReconcileOnce(t.Context()); err != nil {
		t.Fatal(err)
	}
	interrupt(ErrAgentShutdown)
	select {
	case err := <-firstDone:
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("interruption did not finish")
	}
	job, err := store.GetJob(t.Context(), first.Job.JobID)
	if err != nil || job.State != contract.JobQueued {
		t.Fatalf("predecessor Job state=%s err=%v", job.State, err)
	}
	attempts, err := store.ListJobAttempts(t.Context(), first.Job.JobID)
	if err != nil || len(attempts) != 1 || attempts[0].State != contract.AttemptInterrupted {
		t.Fatalf("predecessor Attempts=%+v err=%v", attempts, err)
	}
	if err := reconciler.ReconcileOnce(t.Context()); err != nil {
		t.Fatal(err)
	}
	run, err := runs.GetRun(t.Context(), accepted.RunID)
	if err != nil || run.Status != contract.RunRunning {
		t.Fatalf("interrupted Run status=%s err=%v", run.Status, err)
	}
	successor := claimNext()
	next := newLifecycle()
	successorContext, cancelSuccessor := context.WithCancel(t.Context())
	defer cancelSuccessor()
	nextDone := make(chan error, 1)
	go func() { _, err := next.execute(successorContext, successor, time.Now()); nextDone <- err }()
	readyDeadline := time.After(15 * time.Second)
	for {
		if _, err := os.Stat(ready); err == nil {
			break
		}
		select {
		case err := <-nextDone:
			attempts, _ := store.ListJobAttempts(t.Context(), first.Job.JobID)
			t.Fatalf("successor finished before ready: err=%v attempts=%+v output=%+v", err, attempts, sink.events)
		case <-readyDeadline:
			t.Fatal("successor never became ready")
		case <-time.After(10 * time.Millisecond):
		}
	}
	// Both a late predecessor write and its previous exit-7 upload must be
	// excluded after the successor claims, before and after its publication.
	if err := os.WriteFile(predecessorResult, []byte(`{"exit_code":99}`), 0600); err != nil {
		t.Fatal(err)
	}
	resultPath := "/v1/runs/" + accepted.RunID + "/result"
	if status := exchange(http.MethodGet, resultPath, nil, nil); status != http.StatusNotFound {
		t.Fatalf("predecessor result leaked while successor running: %d", status)
	}
	if err := os.WriteFile(release, nil, 0600); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-nextDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("successor did not finish")
	}
	if err := os.WriteFile(predecessorResult, []byte(`{"exit_code":101}`), 0600); err != nil {
		t.Fatal(err)
	}
	if err := reconciler.ReconcileOnce(t.Context()); err != nil {
		t.Fatal(err)
	}
	run, err = runs.GetRun(t.Context(), accepted.RunID)
	if err != nil || run.Status != contract.RunSucceeded {
		t.Fatalf("successor Run status=%s err=%v", run.Status, err)
	}
	var result l3.RunResult
	if status := exchange(http.MethodGet, resultPath, nil, &result); status != http.StatusOK || result.AttemptID != successor.Lease.AttemptID || !bytes.Contains(result.Document, []byte(`"exit_code":0`)) {
		t.Fatalf("latest result=%+v status=%d", result, status)
	}
}
