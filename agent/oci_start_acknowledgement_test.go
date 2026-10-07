//go:build darwin || linux

package agent

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/fabric/plain"
	"github.com/Derek-X-Wang/wefty/l1"
	workloadrunner "github.com/Derek-X-Wang/wefty/runner"
)

const ociStartTag = "oci-start"

// startInterceptedL1 serves a real L1 store, letting intercept see every
// request first. It does not run L1's reconcile loop; a test settles leases
// itself with Reconcile.
func startInterceptedL1(t *testing.T, network *plain.Network, nodeID string, leaseDuration time.Duration,
	intercept func(next http.Handler, w http.ResponseWriter, r *http.Request)) *l1.Store {
	t.Helper()
	store, err := l1.OpenStore(filepath.Join(t.TempDir(), "oci-start.sqlite"), l1.StoreOptions{LeaseDuration: leaseDuration})
	if err != nil {
		t.Fatal(err)
	}
	serverFabric := network.NewFabric(fabric.Identity{NodeID: "control-plane"})
	server, err := l1.NewServer(serverFabric, store, l1.ServerConfig{NodePolicies: map[string]l1.NodePolicy{
		nodeID: {Tags: []string{ociStartTag}, MaxOneshotSlots: 1, MaxServiceSlots: 1},
	}})
	if err != nil {
		store.Close()
		t.Fatal(err)
	}
	listener, err := serverFabric.Listen("tcp", "wefty://control-plane")
	if err != nil {
		store.Close()
		t.Fatal(err)
	}
	next := server.Handler()
	httpServer := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { intercept(next, w, r) })}
	served := make(chan error, 1)
	go func() { served <- httpServer.Serve(listener) }()
	t.Cleanup(func() {
		_ = httpServer.Close()
		if err := <-served; err != nil && !errors.Is(err, http.ErrServerClosed) {
			t.Errorf("serve L1: %v", err)
		}
		if err := store.Close(); err != nil {
			t.Errorf("close L1: %v", err)
		}
	})
	return store
}

// commitAndLoseAnswer lets L1 commit the request, then drops the connection,
// so the answer never reaches the agent. It returns the status L1 wrote.
func commitAndLoseAnswer(t *testing.T, next http.Handler, w http.ResponseWriter, r *http.Request) int {
	recorder := httptest.NewRecorder()
	next.ServeHTTP(recorder, r)
	loseAnswer(t, w)
	return recorder.Code
}

func loseAnswer(t *testing.T, w http.ResponseWriter) {
	connection, _, err := w.(http.Hijacker).Hijack()
	if err != nil {
		t.Error(err)
		return
	}
	_ = connection.Close()
}

// ociStartRuntime drives the agent's OCI start hooks as the adapter does for a
// one-shot: the pre-Run image observation, then Started once the helper has
// started the payload. A Started acknowledgement that fails stops the payload
// and becomes a spawn failure; otherwise the helper admits the attempt and the
// payload exits zero.
type ociStartRuntime struct {
	*intentStopRuntime
	startErr chan error
}

func newOCIStartRuntime() *ociStartRuntime {
	return &ociStartRuntime{intentStopRuntime: newIntentStopRuntime(), startErr: make(chan error, 1)}
}

func (runtime *ociStartRuntime) Run(ctx context.Context, request workloadrunner.Request, _ workloadrunner.OutputSink) (workloadrunner.Result, error) {
	digest := *request.Execution.OCI.Image.Digest
	observation := workloadrunner.OCIImageObservation{
		SubmittedReference: request.Execution.OCI.Image.Reference,
		TopLevelDigest:     digest, TopLevelMediaType: "application/vnd.oci.image.manifest.v1+json",
		PlatformManifestDigest: digest, PlatformOS: "linux", PlatformArchitecture: "amd64",
		RuntimeHandler: "io.containerd.runc.v2", Snapshotter: "overlayfs",
	}
	if err := request.OCIImageResolved(ctx, observation); err != nil {
		return workloadrunner.Result{Outcome: spawnFailure(contract.SpawnFailureRuntimeUnavailable, err)}, err
	}
	startErr := request.OCIStarted(ctx, observation)
	select {
	case runtime.startErr <- startErr:
	default:
	}
	if startErr != nil {
		return workloadrunner.Result{Outcome: spawnFailure(contract.SpawnFailureProcessRequest, startErr)}, startErr
	}
	if err := admitReadyOCIHelper(request); err != nil {
		return workloadrunner.Result{Outcome: spawnFailure(contract.SpawnFailureRuntimeUnavailable, err)}, err
	}
	exitCode := 0
	return workloadrunner.Result{Outcome: contract.ProcessResult{ExitCode: &exitCode}}, nil
}

func (runtime *ociStartRuntime) waitStart(t *testing.T, timeout time.Duration) error {
	t.Helper()
	select {
	case err := <-runtime.startErr:
		return err
	case <-time.After(timeout):
		t.Fatal("the OCI Started acknowledgement reached no outcome")
		return nil
	}
}

// startOCIStartAgent runs an OCI node agent and returns it with the channel
// its Run reports on. The agent is shut down when the test ends.
func startOCIStartAgent(t *testing.T, network *plain.Network, nodeID string, runtime WorkloadRuntime) (*Agent, <-chan error) {
	t.Helper()
	managedRoot, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	capabilities := map[string]bool{"kind:oci": true, "runtime_handler:io.containerd.runc.v2": true}
	nodeAgent, err := New(Config{
		Fabric:              network.NewFabric(fabric.Identity{NodeID: nodeID + "-fabric", Tags: []string{l1.DefaultAgentPrincipalTag}}),
		ControlPlaneAddress: "wefty://control-plane", NodeID: nodeID, BootSessionID: nodeID + "-boot", Version: "test",
		Capabilities: capabilities,
		CapabilityProbe: capabilityProbeFunc(func(context.Context) (CapabilityProbeResult, error) {
			return CapabilityProbeResult{Capabilities: capabilities}, nil
		}),
		OCIIntent: func(context.Context) (OCIIntentObservation, error) {
			return OCIIntentObservation{Enabled: true, Revision: 1}, nil
		},
		OCIBootBarrier: readyOCIBootBarrier{}, WorkloadRuntimes: map[string]WorkloadRuntime{contract.JobKindOCI: runtime},
		ManagedRootDirectory: managedRoot, LogSpoolDirectory: t.TempDir(), HandoffRoot: t.TempDir(), MaxOneshotSlots: 1,
		HeartbeatInterval: 50 * time.Millisecond, ClaimInterval: 5 * time.Millisecond, RenewalInterval: 50 * time.Millisecond,
		OperationTimeout: 2 * time.Second, LogRetryInterval: 10 * time.Millisecond,
		AttemptDeadman: newRecordingDeadmanRenewer(), Logf: t.Logf,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- nodeAgent.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("agent shutdown: %v", err)
			}
		case <-time.After(10 * time.Second):
			t.Error("agent did not shut down")
		}
		nodeAgent.Close()
	})
	return nodeAgent, done
}

func createOCIStartJob(t *testing.T, store *l1.Store, dispatchKey string) l1.Job {
	t.Helper()
	digest := "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	job, _, err := store.CreateJobAs(t.Context(), contract.JobSpec{
		SchemaVersion: contract.SchemaVersionV1, DispatchKey: dispatchKey,
		Kind: contract.JobKindOCI, Class: contract.JobClassOneShot, Labels: map[string]string{contract.LabelRunID: "run-" + dispatchKey},
		RoutingTags: []string{ociStartTag}, RuntimeHandler: "io.containerd.runc.v2",
		Execution: contract.ExecutionSpec{OCI: &contract.OCIExecutionSpec{
			Image: contract.OCIImageSpec{Reference: "example.invalid/start:v1", Digest: &digest}, Argv: []string{"/payload"},
		}},
	}, runLedgerOrigin)
	if err != nil {
		t.Fatal(err)
	}
	return job
}

// L1 commits an OCI Started, but its answer never reaches the agent. The agent
// retries the idempotent acknowledgement, the retry gets L1's replay of the
// committed start, and the attempt proceeds as started: the payload runs to
// its own exit and the job succeeds, with no spawn failure.
func TestOCIStartedSurvivesLostAcknowledgement(t *testing.T) {
	const nodeID = "oci-lost-start-node"
	var startCalls atomic.Int32
	committed := make(chan int, 1)
	network := plain.NewNetwork()
	store := startInterceptedL1(t, network, nodeID, 5*time.Second, func(next http.Handler, w http.ResponseWriter, r *http.Request) {
		if !strings.HasSuffix(r.URL.Path, "/started") || startCalls.Add(1) > 1 {
			next.ServeHTTP(w, r)
			return
		}
		committed <- commitAndLoseAnswer(t, next, w, r)
	})
	runtime := newOCIStartRuntime()
	startOCIStartAgent(t, network, nodeID, runtime)
	job := createOCIStartJob(t, store, "oci-lost-start")
	select {
	case code := <-committed:
		if code != http.StatusOK {
			t.Fatalf("first Started = HTTP %d, want a committed start", code)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Started never reached L1")
	}
	if err := runtime.waitStart(t, 10*time.Second); err != nil {
		t.Fatalf("a lost answer to a committed Started failed the start: %v", err)
	}
	if calls := startCalls.Load(); calls < 2 {
		t.Fatalf("Started calls = %d, want a retry after the lost answer", calls)
	}
	completed, err := waitForFailureJobState(store, job.JobID, contract.JobSucceeded, 10*time.Second)
	attempts, attemptsErr := store.ListJobAttempts(t.Context(), job.JobID)
	if err != nil || attemptsErr != nil || len(attempts) != 1 {
		t.Fatalf("job after a lost Started answer = %+v %v; attempts = %+v %v", completed, err, attempts, attemptsErr)
	}
	attempt := attempts[0]
	if attempt.Image == nil || attempt.Image.StartedAt == nil || attempt.Result == nil || attempt.Result.SpawnError != nil ||
		attempt.Result.ExitCode == nil || *attempt.Result.ExitCode != 0 {
		t.Fatalf("attempt after a lost Started answer = %+v", attempt)
	}
}

// L1 commits an OCI Started, and no answer to it or to any retry reaches the
// agent within the lease window. The adapter then stops the payload and
// reports a spawn_error, which L1 can never accept for an attempt it records
// as started: it answers 409 conflict, a permanent rejection. The completion
// is not retried, the node session keeps running, evidence recovery seals the
// refused completion, and L1 settles the attempt once its lease runs out.
func TestOCISpawnErrorAfterStartedIsNotRetriedForever(t *testing.T) {
	const nodeID = "oci-refused-completion-node"
	const leaseDuration = 2 * time.Second
	var startCalls, completeCalls atomic.Int32
	committed := make(chan int, 1)
	completeStatus := make(chan int, 64)
	network := plain.NewNetwork()
	store := startInterceptedL1(t, network, nodeID, leaseDuration, func(next http.Handler, w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "/started"):
			if startCalls.Add(1) == 1 {
				committed <- commitAndLoseAnswer(t, next, w, r)
				return
			}
			loseAnswer(t, w)
		case strings.HasSuffix(r.URL.Path, "/complete"):
			completeCalls.Add(1)
			recorder := httptest.NewRecorder()
			next.ServeHTTP(recorder, r)
			select {
			case completeStatus <- recorder.Code:
			default:
			}
			for name, values := range recorder.Header() {
				w.Header()[name] = values
			}
			w.WriteHeader(recorder.Code)
			_, _ = w.Write(recorder.Body.Bytes())
		default:
			next.ServeHTTP(w, r)
		}
	})
	runtime := newOCIStartRuntime()
	nodeAgent, done := startOCIStartAgent(t, network, nodeID, runtime)
	job := createOCIStartJob(t, store, "oci-refused-completion")
	select {
	case code := <-committed:
		if code != http.StatusOK {
			t.Fatalf("first Started = HTTP %d, want a committed start", code)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Started never reached L1")
	}
	if err := runtime.waitStart(t, 10*time.Second); err == nil {
		t.Fatal("Started with every answer lost reported success")
	}
	select {
	case code := <-completeStatus:
		if code != http.StatusConflict {
			t.Fatalf("spawn_error completion after Started = HTTP %d, want 409", code)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the spawn_error completion never reached L1")
	}
	running, err := store.GetJob(t.Context(), job.JobID)
	if err != nil || running.CurrentAttemptID == "" {
		t.Fatalf("job = %+v %v", running, err)
	}
	attemptID := running.CurrentAttemptID
	waitCompletionReceiptState(t, nodeAgent.outbox, attemptID, "sealed_incomplete", 5*time.Second)
	receipt := nodeAgent.outbox.spool.inspectCompletion(t.Context(), attemptID)
	if receipt.Incomplete.ErrorCode != contract.ErrorConflict || receipt.Reason != "completion was permanently rejected" {
		t.Fatalf("sealed completion = %+v", receipt)
	}
	calls := completeCalls.Load()
	time.Sleep(250 * time.Millisecond) // 25 completion retry intervals
	if again := completeCalls.Load(); again != calls || calls > 2 {
		t.Fatalf("completion calls = %d, then %d: a refused completion was retried", calls, again)
	}
	select {
	case err := <-done:
		t.Fatalf("a refused completion ended the node session: %v", err)
	default:
	}
	// Renewal ended with the refusal, so the lease runs out and L1 settles the
	// attempt by its lease-loss rules.
	deadline := time.Now().Add(3 * leaseDuration)
	for {
		if _, err := store.Reconcile(t.Context()); err != nil {
			t.Fatal(err)
		}
		settled, err := store.GetJob(t.Context(), job.JobID)
		if err != nil {
			t.Fatal(err)
		}
		if settled.State == contract.JobFailed {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("job after a refused completion = %+v, want failed by lease loss", settled)
		}
		time.Sleep(20 * time.Millisecond)
	}
	attempts, err := store.ListJobAttempts(t.Context(), job.JobID)
	if err != nil || len(attempts) != 1 || attempts[0].State != contract.AttemptLost || attempts[0].Image == nil || attempts[0].Image.StartedAt == nil {
		t.Fatalf("attempts after settlement = %+v %v", attempts, err)
	}
}

// A completion L1 permanently rejects is sent once. The live attempt stops on
// that answer and leaves the durable completion, neither delivered nor sealed,
// to evidence recovery. It is the attempt's own refusal, so the node session
// absorbs it.
func TestCompletionStopsOnPermanentRejection(t *testing.T) {
	for _, tc := range []struct {
		status int
		code   contract.ErrorCode
	}{
		{http.StatusConflict, contract.ErrorConflict},
		{http.StatusBadRequest, contract.ErrorInvalidRequest},
		{http.StatusConflict, contract.ErrorIdempotencyConflict},
		{http.StatusNotFound, contract.ErrorNotFound},
		{http.StatusUnprocessableEntity, contract.ErrorUnsupportedKind},
		{http.StatusNotImplemented, contract.ErrorNotImplemented},
	} {
		t.Run(string(tc.code), func(t *testing.T) {
			var calls atomic.Int32
			client, stop := startEvidenceReplayServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(tc.status)
				_ = json.NewEncoder(w).Encode(contract.ErrorResponse{Error: contract.APIError{Code: tc.code, Message: "permanently rejected"}})
			}), time.Second)
			defer stop()
			defer client.Close()
			outbox, err := newEvidenceOutbox(t.TempDir(), "refusal-node", 1024, systemClock{}, 8, time.Hour, time.Millisecond)
			if err != nil {
				t.Fatal(err)
			}
			defer outbox.Close()
			attemptID := "refused-" + string(tc.code)
			storeRecoveryCompletion(t, outbox, attemptID)
			lifecycle := newAttemptLifecycle(attemptLifecycleDependencies{client: client, outbox: outbox, clock: systemClock{}, completionRetry: time.Millisecond})
			claim := spoolTestClaim(attemptID)
			exitCode := 0
			ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
			defer cancel()
			failure := lifecycle.completeWithRetry(ctx, claim, l1.CompletionRequest{
				FencingToken: claim.Lease.FencingToken, IdempotencyKey: "completion:" + attemptID,
				Result: l1.ProcessResult{ExitCode: &exitCode},
			})
			if failure.destination != errorDestinationAttemptAuthority || protocolErrorCode(failure.err) != tc.code || calls.Load() != 1 {
				t.Fatalf("refused completion = destination %v err %v after %d calls, want one call and an attempt-scoped refusal",
					failure.destination, failure.err, calls.Load())
			}
			if receipt := outbox.spool.inspectCompletion(t.Context(), attemptID); receipt.State != "durable_completion" {
				t.Fatalf("durable completion after the live refusal = %+v, want it left to recovery", receipt)
			}
		})
	}
}
