//go:build darwin || linux

package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/agent/managedroot"
	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/fabric/plain"
	"github.com/Derek-X-Wang/wefty/l1"
	workloadrunner "github.com/Derek-X-Wang/wefty/runner"
	processrunner "github.com/Derek-X-Wang/wefty/runner/process"
)

type upgradeFailureFixture struct {
	a        *Agent
	store    *l1.Store
	done     <-chan error
	service  l1.Job
	request  UpgradeServiceQuiescence
	jobs     []l1.Job
	attempts []string
	pids     []int
	releases []string
	marker   string
}

func newUpgradeFailureFixture(t *testing.T, hook func(*Agent)) *upgradeFailureFixture {
	t.Helper()
	return newUpgradeFailureFixtureWithLease(t, hook, l1.DefaultLeaseDuration)
}

func newUpgradeFailureFixtureWithLease(t *testing.T, hook func(*Agent), lease time.Duration) *upgradeFailureFixture {
	t.Helper()
	return newUpgradeFailureFixtureWithServices(t, hook, lease, 1)
}

func newUpgradeFailureFixtureWithServices(t *testing.T, hook func(*Agent), lease time.Duration, services int) *upgradeFailureFixture {
	t.Helper()
	network := plain.NewNetwork()
	store, stop := startFailureServerWithPoliciesAndLease(t, network, nil, map[string]l1.NodePolicy{"node": {Tags: []string{"linux"}, MaxOneshotSlots: 3, MaxServiceSlots: services}}, lease)
	t.Cleanup(stop)
	root := temporaryManagedRoot(t)
	zero := 0.0
	a, err := New(Config{Fabric: network.NewFabric(fabric.Identity{NodeID: "upgrade-failure-agent", Tags: []string{l1.DefaultAgentPrincipalTag}}), ControlPlaneAddress: "wefty://control-plane", NodeID: "node", AgentGenerationID: "generation", BootSessionID: "boot", OS: "darwin", Version: "test", Capabilities: map[string]bool{"kind:process": true, "upgrade:retirement:v1": true, contract.CapabilityExecCaptureBundle: true}, GuardianExecutable: agentBinaryPath, ManagedRootDirectory: root, LogSpoolDirectory: t.TempDir(), HandoffRoot: t.TempDir(), MaxOneshotSlots: 3, MaxServiceSlots: services, OneshotLoadLimit: &zero, HeartbeatInterval: 100 * time.Millisecond, ClaimInterval: 10 * time.Millisecond, RenewalInterval: 200 * time.Millisecond, OperationTimeout: time.Second, Logf: t.Logf})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(a.Close)
	resource := a.managedResource.(*processManagedResource)
	resource.root, err = managedroot.Open(managedroot.Config{Root: root, NodeID: "node", BootSessionID: "boot", HostBootIdentity: rootHostIdentity})
	if err != nil {
		t.Fatal(err)
	}
	if hook != nil {
		hook(a)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- a.Run(ctx); close(done) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Error("agent did not join")
		}
	})
	select {
	case <-a.Registered():
	case <-time.After(10 * time.Second):
		t.Fatal("registration timeout")
	}
	f := &upgradeFailureFixture{a: a, store: store, done: done}
	for _, restartable := range []bool{false, true} {
		dir := t.TempDir()
		ready, release := filepath.Join(dir, "ready"), filepath.Join(dir, "release")
		job, _, err := store.CreateJob(t.Context(), contract.JobSpec{SchemaVersion: contract.SchemaVersionV1, DispatchKey: fmt.Sprintf("kept-%t", restartable), Class: contract.JobClassOneShot, Kind: contract.JobKindProcess, Restartable: restartable, RoutingTags: []string{"linux"}, Execution: contract.ExecutionSpec{Executable: contract.ExecutableSpec{Path: "/bin/sh"}, Argv: []string{"sh", "-c", `echo $$ > "$1"; while [ ! -e "$2" ]; do sleep 0.05; done; exit 17`, "sh", ready, release}, WorkingDirectory: dir, HandoffDirectory: dir}})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = os.WriteFile(release, nil, 0600) })
		retirementWait(t, "one-shot ready", func() bool {
			bytes, err := os.ReadFile(ready)
			return err == nil && strings.TrimSpace(string(bytes)) != ""
		})
		bytes, err := os.ReadFile(ready)
		if err != nil {
			t.Fatal(err)
		}
		pid, err := strconv.Atoi(strings.TrimSpace(string(bytes)))
		if err != nil {
			t.Fatal(err)
		}
		// A payload can write its ready file before the concurrent /started
		// request commits. Establish both runtime and L1 readiness before the
		// barrier so the preservation assertion cannot mistake claimed for loss.
		retirementWait(t, "one-shot running acknowledgement", func() bool {
			current, err := store.GetJob(t.Context(), job.JobID)
			return err == nil && current.State == contract.JobRunning && current.CurrentAttemptID != ""
		})
		current, err := store.GetJob(t.Context(), job.JobID)
		if err != nil {
			t.Fatal(err)
		}
		f.jobs = append(f.jobs, job)
		f.attempts = append(f.attempts, current.CurrentAttemptID)
		f.pids = append(f.pids, pid)
		f.releases = append(f.releases, release)
	}
	f.request = UpgradeServiceQuiescence{OperationID: "upgrade", NodeID: "node", AgentGenerationID: "generation", BootSessionID: "boot"}
	for i := range services {
		dir := t.TempDir()
		service, _, err := store.CreateJob(t.Context(), contract.JobSpec{SchemaVersion: contract.SchemaVersionV1, DispatchKey: fmt.Sprintf("service-%d", i), Class: contract.JobClassService, Kind: contract.JobKindProcess, Restart: contract.RestartNever, RoutingTags: []string{"linux"}, Execution: contract.ExecutionSpec{Executable: contract.ExecutableSpec{Path: "/bin/sh"}, Argv: []string{"sh", "-c", `trap 'exit 0' TERM; while :; do sleep 0.05; done`}, WorkingDirectory: dir}})
		if err != nil {
			t.Fatal(err)
		}
		retirementWait(t, "service running", func() bool {
			j, err := store.GetJob(t.Context(), service.JobID)
			return err == nil && j.State == contract.JobRunning
		})
		current, err := store.GetJob(t.Context(), service.JobID)
		if err != nil {
			t.Fatal(err)
		}
		if i == 0 {
			f.service = service
		}
		f.request.Services = append(f.request.Services, l1.NodeActiveAttempt{JobID: service.JobID, AttemptID: current.CurrentAttemptID, BootSessionID: "boot", Kind: contract.JobKindProcess, Class: contract.JobClassService})
	}
	node, err := store.GetNode(t.Context(), "node")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = store.SetNodeClaimsByOperator(t.Context(), "node", "operator", l1.NodeIntentRequest{ClaimsEnabled: false, IntentRevision: node.IntentRevision, Reason: "upgrade"}); err != nil {
		t.Fatal(err)
	}
	f.marker = filepath.Join(root, "agent", "nodes", managedroot.EncodeID("node"), "services", managedroot.EncodeID(f.service.JobID), "data", "keep")
	if err := os.WriteFile(f.marker, []byte("service data"), 0600); err != nil {
		t.Fatal(err)
	}
	return f
}

func (f *upgradeFailureFixture) assertKept(t *testing.T) {
	t.Helper()
	for i, job := range f.jobs {
		current, err := f.store.GetJob(t.Context(), job.JobID)
		if err != nil || current.State != contract.JobRunning || current.CurrentAttemptID != f.attempts[i] {
			t.Fatalf("upgrade canceled/replaced one-shot: state=%s attempt=%s expected=%s error=%v", current.State, current.CurrentAttemptID, f.attempts[i], err)
		}
		if err := syscall.Kill(f.pids[i], 0); err != nil {
			t.Fatalf("one-shot process %d ended: %v", f.pids[i], err)
		}
	}
	if bytes, err := os.ReadFile(f.marker); err != nil || string(bytes) != "service data" {
		t.Fatalf("service data changed: %q %v", bytes, err)
	}
	if claim, sent, err := f.a.session.claim(t.Context(), f.a.session.gates[workloadClassService], contract.JobClassService, false); err != nil || sent || claim != nil {
		t.Fatalf("upgrade replay reopened service admission: %+v %v %v", claim, sent, err)
	}
	select {
	case err := <-f.done:
		t.Fatalf("service failure ended agent: %v", err)
	default:
	}
}

func TestUpgradeServicesCompletionTimeoutKeepsSiblingProcesses(t *testing.T) {
	var blocked atomic.Bool
	blocked.Store(true)
	f := newUpgradeFailureFixture(t, func(a *Agent) {
		base := a.session.client.httpClient.Transport
		a.session.client.httpClient.Transport = captureRoundTrip(func(r *http.Request) (*http.Response, error) {
			if strings.HasSuffix(r.URL.Path, "/complete") && blocked.Load() {
				<-r.Context().Done()
				return nil, r.Context().Err()
			}
			return base.RoundTrip(r)
		})
	})
	// Only the service can complete: both one-shots are waiting for their release files.
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	_, err := f.a.QuiesceUpgradeServices(ctx, f.request)
	cancel()
	var failure *UpgradeServiceQuiescenceError
	if !errors.As(err, &failure) || failure.JobID != f.service.JobID || failure.AttemptID != f.request.Services[0].AttemptID || failure.Phase != "completion" || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("unactionable completion timeout: %v", err)
	}
	if receipt := f.a.logSpool.inspectCompletion(t.Context(), failure.AttemptID); receipt.State != "durable_completion" {
		t.Fatalf("pending completion bytes were discarded: %+v", receipt)
	}
	f.assertKept(t)
	blocked.Store(false)
	ctx, cancel = context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	if _, err := f.a.QuiesceUpgradeServices(ctx, f.request); err != nil {
		t.Fatal("completion replay failed:", err)
	}
	f.assertKept(t)
	for i, release := range f.releases {
		if err := os.WriteFile(release, nil, 0600); err != nil {
			t.Fatal(err)
		}
		retirementWait(t, "kept one-shot finished", func() bool {
			job, err := f.store.GetJob(t.Context(), f.jobs[i].JobID)
			return err == nil && job.State == contract.JobFailed
		})
		attempts, err := f.store.ListJobAttempts(t.Context(), f.jobs[i].JobID)
		if err != nil || len(attempts) != 1 || attempts[0].AttemptID != f.attempts[i] || attempts[0].Result == nil || attempts[0].Result.ExitCode == nil || *attempts[0].Result.ExitCode != 17 {
			t.Fatalf("kept one-shot result changed: %+v %v", attempts, err)
		}
	}
}

func TestUpgradeServicesJoinsAdmittedServiceBeforeResidency(t *testing.T) {
	network := plain.NewNetwork()
	store, stop := startFailureServerWithPoliciesAndLease(t, network, nil, map[string]l1.NodePolicy{"node": {MaxServiceSlots: 1}}, l1.DefaultLeaseDuration)
	t.Cleanup(stop)
	root := temporaryManagedRoot(t)
	a, err := New(Config{Fabric: network.NewFabric(fabric.Identity{NodeID: "prestart-agent", Tags: []string{l1.DefaultAgentPrincipalTag}}), ControlPlaneAddress: "wefty://control-plane", NodeID: "node", AgentGenerationID: "generation", BootSessionID: "boot", OS: "darwin", Version: "test", Capabilities: map[string]bool{"kind:process": true, "upgrade:retirement:v1": true}, GuardianExecutable: agentBinaryPath, ManagedRootDirectory: root, LogSpoolDirectory: t.TempDir(), HandoffRoot: t.TempDir(), MaxServiceSlots: 1, OperationTimeout: time.Second, ClaimInterval: time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(a.Close)
	resource := a.managedResource.(*processManagedResource)
	resource.root, err = managedroot.Open(managedroot.Config{Root: root, NodeID: "node", BootSessionID: "boot", HostBootIdentity: rootHostIdentity})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = a.Register(t.Context()); err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	marker := filepath.Join(dir, "must-not-run")
	job, _, err := store.CreateJob(t.Context(), contract.JobSpec{SchemaVersion: contract.SchemaVersionV1, DispatchKey: "admitted-service", Class: contract.JobClassService, Kind: contract.JobKindProcess, Restart: contract.RestartNever, Execution: contract.ExecutionSpec{Executable: contract.ExecutableSpec{Path: "/bin/sh"}, Argv: []string{"sh", "-c", `touch "$1"`, "sh", marker}, WorkingDirectory: dir}})
	if err != nil {
		t.Fatal(err)
	}
	claim, _, err := a.session.claim(t.Context(), a.session.gates[workloadClassService], contract.JobClassService, false)
	if err != nil || claim == nil {
		t.Fatalf("claim=%+v %v", claim, err)
	}
	node, err := store.GetNode(t.Context(), "node")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = store.SetNodeClaimsByOperator(t.Context(), "node", "operator", l1.NodeIntentRequest{ClaimsEnabled: false, IntentRevision: node.IntentRevision, Reason: "upgrade"}); err != nil {
		t.Fatal(err)
	}
	request := UpgradeServiceQuiescence{NodeID: "node", AgentGenerationID: "generation", BootSessionID: "boot", OperationID: "activation", Services: []l1.NodeActiveAttempt{{JobID: job.JobID, AttemptID: claim.Lease.AttemptID, BootSessionID: "boot", Class: contract.JobClassService, Kind: contract.JobKindProcess}}}
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	barrier := make(chan error, 1)
	go func() { _, err := a.QuiesceUpgradeServices(ctx, request); barrier <- err }()
	retirementWait(t, "service gate closed", func() bool {
		a.session.claimMu.Lock()
		defer a.session.claimMu.Unlock()
		return a.session.upgradeServiceOperation != ""
	})
	a.session.attempts.Add(1)
	if _, err := a.session.executeResident(t.Context(), workloadClassService, *claim, a.clock.Now(), a.executeClaim); err != nil {
		t.Fatal(err)
	}
	if err := <-barrier; err != nil {
		t.Fatal("real pre-start lifecycle stranded barrier:", err)
	}
	attempts, err := store.ListJobAttempts(t.Context(), job.JobID)
	if err != nil || len(attempts) != 1 || attempts[0].Result == nil || attempts[0].Result.TerminationCause != contract.TerminationCauseAgent {
		t.Fatalf("unstarted completion not accepted: %+v %v", attempts, err)
	}
	if receipt := a.logSpool.inspectCompletion(t.Context(), claim.Lease.AttemptID); receipt.State != "delivered" {
		t.Fatalf("unstarted evidence not delivered: %+v", receipt)
	}
	a.session.claimMu.Lock()
	proof := a.session.serviceReaps[job.JobID]
	record := a.session.retirementAttempts[claim.Lease.AttemptID]
	retained := record.recoverEvidence != nil || record.recheckRuntime != nil
	a.session.claimMu.Unlock()
	if retained {
		t.Fatal("settled service retained lifecycle recovery callbacks")
	}
	if proof.err != nil || !proof.receipt.RuntimeQuiesced || proof.receipt.Evidence != workloadrunner.ReapEvidenceNoRuntime {
		t.Fatalf("unstarted reap=%+v", proof)
	}
	if _, err := os.Stat(marker); !os.IsNotExist(err) {
		t.Fatalf("unstarted service executed: %v", err)
	}
	if _, err := a.QuiesceUpgradeServices(t.Context(), request); err != nil {
		t.Fatal("pre-start replay:", err)
	}
}

type upgradeReapFault struct{ workloadrunner.WorkloadRuntime }

func (r *upgradeReapFault) ReapAndVerify(ctx context.Context, request workloadrunner.ReapRequest) (workloadrunner.ReapReceipt, error) {
	receipt, err := r.WorkloadRuntime.ReapAndVerify(ctx, request)
	if request.Authority.WorkloadClass == contract.JobClassService {
		return workloadrunner.ReapReceipt{}, processrunner.ErrProcessReapTimeout
	}
	return receipt, err
}

func TestUpgradeServicesReapFailureReportsAndReverifies(t *testing.T) {
	f := newUpgradeFailureFixture(t, func(a *Agent) {
		a.runtimes[contract.JobKindProcess] = &upgradeReapFault{a.runtimes[contract.JobKindProcess]}
	})
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	_, err := f.a.QuiesceUpgradeServices(ctx, f.request)
	cancel()
	var failure *UpgradeServiceQuiescenceError
	if !errors.As(err, &failure) || failure.JobID != f.service.JobID || failure.AttemptID != f.request.Services[0].AttemptID || failure.Phase != "reap" || !errors.Is(err, processrunner.ErrProcessReapTimeout) {
		t.Fatalf("unactionable reap failure: %v", err)
	}
	f.assertKept(t)
	ctx, cancel = context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	if _, err := f.a.QuiesceUpgradeServices(ctx, f.request); err != nil {
		t.Fatal("exact runtime re-verification failed:", err)
	}
	f.assertKept(t)
}

func TestUpgradeServicesRejectedEvidenceRetainsBytesAndReplays(t *testing.T) {
	var reject atomic.Bool
	reject.Store(true)
	f := newUpgradeFailureFixture(t, func(a *Agent) {
		base := a.session.client.httpClient.Transport
		a.session.client.httpClient.Transport = captureRoundTrip(func(r *http.Request) (*http.Response, error) {
			if strings.HasSuffix(r.URL.Path, "/complete") && reject.Load() {
				return &http.Response{StatusCode: http.StatusBadRequest, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(`{"error":{"code":"invalid_request","message":"injected completion rejection","retryable":false}}`)), Request: r}, nil
			}
			return base.RoundTrip(r)
		})
	})
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	_, err := f.a.QuiesceUpgradeServices(ctx, f.request)
	cancel()
	var failure *UpgradeServiceQuiescenceError
	if !errors.As(err, &failure) || failure.JobID != f.service.JobID || failure.AttemptID != f.request.Services[0].AttemptID || failure.Phase != "completion" || protocolErrorCode(err) != contract.ErrorInvalidRequest {
		t.Fatalf("unactionable evidence refusal: %v", err)
	}
	if receipt := f.a.logSpool.inspectCompletion(t.Context(), failure.AttemptID); receipt.State != "durable_completion" {
		t.Fatalf("rejected completion bytes were discarded: %+v", receipt)
	}
	f.assertKept(t)
	reject.Store(false)
	ctx, cancel = context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	if _, err := f.a.QuiesceUpgradeServices(ctx, f.request); err != nil {
		t.Fatal("exact evidence replay failed:", err)
	}
	f.assertKept(t)
}

func TestUpgradeServicesAuthorityRefusalRetainsEvidence(t *testing.T) {
	for _, code := range []contract.ErrorCode{contract.ErrorStaleFence, contract.ErrorLeaseExpired} {
		t.Run(string(code), func(t *testing.T) {
			var reject atomic.Bool
			reject.Store(true)
			client, stop := startEvidenceReplayServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				if reject.Load() {
					w.WriteHeader(http.StatusConflict)
					fmt.Fprintf(w, `{"error":{"code":%q,"message":"injected authority refusal"}}`, code)
					return
				}
				io.WriteString(w, `{}`)
			}), time.Second)
			defer stop()
			defer client.Close()
			outbox, err := newEvidenceOutbox(t.TempDir(), "node", 1024, systemClock{}, 8, time.Hour, time.Millisecond)
			if err != nil {
				t.Fatal(err)
			}
			defer outbox.Close()
			claim := spoolTestClaim("authority-refusal")
			claim.Job.Spec.Class, claim.Job.Spec.Kind = contract.JobClassService, contract.JobKindProcess
			if err := outbox.ensureAttempt(t.Context(), claim); err != nil {
				t.Fatal(err)
			}
			request := completionRequest(claim, agentTerminatedResult(contract.ProcessResult{}), workloadrunner.ReapEvidenceAttempt)
			if err := outbox.storeDurableCompletion(t.Context(), claim.Lease.AttemptID, durableCompletionOf(request), time.Now()); err != nil {
				t.Fatal(err)
			}
			lifecycle := newAttemptLifecycle(attemptLifecycleDependencies{client: client, outbox: outbox, clock: systemClock{}, completionRetry: time.Millisecond})
			lifecycle.completion.Store(&request)
			err = lifecycle.recoverUpgradeServiceEvidence(t.Context(), claim)
			var failure *UpgradeServiceQuiescenceError
			if !errors.As(err, &failure) || failure.Phase != "completion" || protocolErrorCode(err) != code {
				t.Fatalf("authority refusal supplied upgrade proof: %v", err)
			}
			if receipt := outbox.spool.inspectCompletion(t.Context(), claim.Lease.AttemptID); receipt.State != "durable_completion" {
				t.Fatalf("authority refusal discarded exact evidence: %+v", receipt)
			}
			reject.Store(false)
			if err := lifecycle.recoverUpgradeServiceEvidence(t.Context(), claim); err != nil {
				t.Fatal("exact evidence replay:", err)
			}
			if receipt := outbox.spool.inspectCompletion(t.Context(), claim.Lease.AttemptID); receipt.State != "delivered" {
				t.Fatalf("accepted evidence was not delivered: %+v", receipt)
			}
		})
	}
}

func TestUpgradeServicesInFlightCompletionRetainsEvidence(t *testing.T) {
	client, stop := startEvidenceReplayServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusConflict)
		io.WriteString(w, `{"error":{"code":"stale_fence","message":"injected authority refusal"}}`)
	}), time.Second)
	defer stop()
	defer client.Close()
	outbox, err := newEvidenceOutbox(t.TempDir(), "node", 1024, systemClock{}, 8, time.Hour, time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	defer outbox.Close()
	claim := spoolTestClaim("in-flight-service")
	claim.Job.Spec.Class, claim.Job.Spec.Kind = contract.JobClassService, contract.JobKindProcess
	if err := outbox.ensureAttempt(t.Context(), claim); err != nil {
		t.Fatal(err)
	}
	request := completionRequest(claim, agentTerminatedResult(contract.ProcessResult{}), workloadrunner.ReapEvidenceAttempt)
	if err := outbox.storeDurableCompletion(t.Context(), claim.Lease.AttemptID, durableCompletionOf(request), time.Now()); err != nil {
		t.Fatal(err)
	}
	base := client.httpClient.Transport
	client.httpClient.Transport = captureRoundTrip(func(r *http.Request) (*http.Response, error) {
		outbox.holdUpgradeAttempt(claim.Lease.AttemptID)
		return base.RoundTrip(r)
	})
	lifecycle := newAttemptLifecycle(attemptLifecycleDependencies{client: client, outbox: outbox, clock: systemClock{}, completionRetry: time.Millisecond})
	if failure := lifecycle.completeWithRetry(t.Context(), claim, request); protocolErrorCode(failure.err) != contract.ErrorStaleFence {
		t.Fatalf("lost refusal: %+v", failure)
	}
	if receipt := outbox.spool.inspectCompletion(t.Context(), claim.Lease.AttemptID); receipt.State != "durable_completion" {
		t.Fatalf("in-flight completion discarded upgrade evidence: %+v", receipt)
	}
}

func TestUpgradeServicesSealedEvidenceReportsFailure(t *testing.T) {
	outbox, err := newEvidenceOutbox(t.TempDir(), "node", 1024, systemClock{}, 8, time.Hour, time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	defer outbox.Close()
	storeRecoveryCompletion(t, outbox, "sealed")
	if err := outbox.sealAttemptEvidence(t.Context(), "sealed", "injected evidence loss", contract.ErrorInvalidRequest); err != nil {
		t.Fatal(err)
	}
	clock := systemClock{}
	s := newAgentSession(nil, contract.NodeRegistration{NodeID: "node", AgentGenerationID: "generation", BootSessionID: "boot"}, nil, time.Second, time.Millisecond, clock, newLifecycleObserver(clock), nil, 1, 1)
	s.retirementAttempts["sealed"] = &retirementAttempt{jobID: "job", class: contract.JobClassService, kind: contract.JobKindProcess, reaped: true, finished: true, checkEvidence: func(ctx context.Context) (bool, error) { return outbox.attemptEvidenceDelivered(ctx, "sealed", false) }}
	a := &Agent{session: s, outbox: outbox}
	request := UpgradeServiceQuiescence{OperationID: "upgrade", NodeID: "node", AgentGenerationID: "generation", BootSessionID: "boot"}
	for range 2 {
		_, err := a.QuiesceUpgradeServices(t.Context(), request)
		var failure *UpgradeServiceQuiescenceError
		if !errors.As(err, &failure) || failure.JobID != "job" || failure.AttemptID != "sealed" || failure.Phase != "evidence" || !strings.Contains(err.Error(), "injected evidence loss") {
			t.Fatalf("unactionable sealed evidence: %v", err)
		}
		if s.upgradeServiceOperation != "upgrade" || s.upgradeServicesSettled {
			t.Fatal("lost evidence reopened admission or certified proof")
		}
	}
}

// Completion authority must outlive payload cancellation and bounded barrier
// calls. Refusals below are real L1 verdicts, not a fake expiry that can flip back.
func TestUpgradeServicesRenewUntilDelayedFinalizationSettles(t *testing.T) {
	for _, phase := range []string{"completion", "reap"} {
		t.Run(phase, func(t *testing.T) {
			const lease = 2 * time.Second
			var blocked atomic.Bool
			blocked.Store(true)
			f := newUpgradeFailureFixtureWithLease(t, func(a *Agent) {
				if phase == "reap" {
					a.runtimes[contract.JobKindProcess] = &upgradeReapFault{a.runtimes[contract.JobKindProcess]}
					return
				}
				base := a.session.client.httpClient.Transport
				a.session.client.httpClient.Transport = captureRoundTrip(func(r *http.Request) (*http.Response, error) {
					if strings.HasSuffix(r.URL.Path, "/complete") && blocked.Load() {
						<-r.Context().Done()
						return nil, r.Context().Err()
					}
					return base.RoundTrip(r)
				})
			}, lease)
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			_, err := f.a.QuiesceUpgradeServices(ctx, f.request)
			cancel()
			var failure *UpgradeServiceQuiescenceError
			if !errors.As(err, &failure) || failure.Phase != phase {
				t.Fatalf("expected bounded %s failure: %v", phase, err)
			}
			// The lifecycle worker has already joined. Keep the operation stuck
			// longer than a whole lease without a barrier call driving renewal.
			timer := time.NewTimer(lease + time.Second)
			defer timer.Stop()
			select {
			case <-timer.C:
			case <-t.Context().Done():
				t.Fatal(t.Context().Err())
			}
			f.assertKept(t)
			blocked.Store(false)
			ctx, cancel = context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			if _, err := f.a.QuiesceUpgradeServices(ctx, f.request); err != nil {
				t.Fatal("real L1 lost completion authority while finalization was pending:", err)
			}
			attempts, err := f.store.ListJobAttempts(t.Context(), f.service.JobID)
			if err != nil || len(attempts) != 1 || attempts[0].Result == nil || attempts[0].State == contract.AttemptLost {
				t.Fatalf("delayed finalization was not accepted: %+v %v", attempts, err)
			}
			f.assertKept(t)
			if err := f.a.ResumeUpgradeServiceClaims(t.Context(), f.request.OperationID); err != nil {
				t.Fatal("settled service could not resume admission:", err)
			}
		})
	}
}

func TestUpgradeServicesFenceInFlightLogRefusal(t *testing.T) {
	for _, code := range []contract.ErrorCode{contract.ErrorStaleFence, contract.ErrorInvalidRequest} {
		for _, released := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/released=%t", code, released), func(t *testing.T) {
				entered, allow := make(chan struct{}), make(chan struct{})
				var enterOnce, allowOnce sync.Once
				release := func() { allowOnce.Do(func() { close(allow) }) }
				defer release()
				var reject atomic.Bool
				reject.Store(true)
				client, stop := startEvidenceReplayServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					w.Header().Set("Content-Type", "application/json")
					if strings.HasSuffix(r.URL.Path, "/logs") {
						if reject.Load() {
							enterOnce.Do(func() { close(entered) })
							select {
							case <-allow:
							case <-r.Context().Done():
								return
							}
							w.WriteHeader(http.StatusConflict)
							fmt.Fprintf(w, `{"error":{"code":%q,"message":"injected in-flight log refusal"}}`, code)
							return
						}
						json.NewEncoder(w).Encode(l1.AppendLogsResponse{Acknowledged: map[contract.LogStream]uint64{contract.LogStdout: 0}, AttemptState: contract.AttemptRunning})
						return
					}
					io.WriteString(w, `{}`)
				}), 5*time.Second)
				defer stop()
				defer client.Close()
				outbox, err := newEvidenceOutbox(t.TempDir(), "node", 1024, systemClock{}, 8, time.Hour, time.Millisecond)
				if err != nil {
					t.Fatal(err)
				}
				defer outbox.Close()
				claim := spoolTestClaim("in-flight-log-service")
				claim.Job.Spec.Class, claim.Job.Spec.Kind = contract.JobClassService, contract.JobKindProcess
				if err := outbox.ensureAttempt(t.Context(), claim); err != nil {
					t.Fatal(err)
				}
				if err := outbox.spool.append(t.Context(), spoolTestEvent(claim.Lease.AttemptID, contract.LogStdout, 0, "retained service bytes")); err != nil {
					t.Fatal(err)
				}
				request := completionRequest(claim, agentTerminatedResult(contract.ProcessResult{}), workloadrunner.ReapEvidenceAttempt)
				if err := outbox.storeDurableCompletion(t.Context(), claim.Lease.AttemptID, durableCompletionOf(request), time.Now()); err != nil {
					t.Fatal(err)
				}
				attempt := logSpoolAttempt{jobID: claim.Job.JobID, attemptID: claim.Lease.AttemptID, fencingToken: claim.Lease.FencingToken, class: contract.JobClassService, kind: contract.JobKindProcess}
				ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
				defer cancel()
				done := make(chan error, 1)
				go func() { done <- outbox.recoverLogs(ctx, client, attempt) }()
				select {
				case <-entered:
				case <-ctx.Done():
					t.Fatal("log replay did not enter transport")
				}
				outbox.holdUpgradeAttempt(claim.Lease.AttemptID)
				if released {
					outbox.releaseUpgradeAttempt(claim.Lease.AttemptID)
				}
				release()
				select {
				case err := <-done:
					if protocolErrorCode(err) != code {
						t.Fatalf("lost original refusal: %v", err)
					}
				case <-ctx.Done():
					t.Fatal("in-flight log replay did not join")
				}
				if receipt := outbox.spool.inspectCompletion(t.Context(), claim.Lease.AttemptID); receipt.State != "durable_completion" {
					t.Fatalf("in-flight replay sealed upgrade evidence: %+v", receipt)
				}
				batch, err := outbox.spool.pendingBatch(t.Context(), claim.Lease.AttemptID, 8)
				if err != nil || len(batch) != 1 || batch[0].event.Gap != nil || string(batch[0].event.Bytes) != "retained service bytes" {
					t.Fatalf("in-flight replay replaced original bytes: %+v %v", batch, err)
				}
				reject.Store(false)
				lifecycle := newAttemptLifecycle(attemptLifecycleDependencies{client: client, outbox: outbox, clock: systemClock{}, completionRetry: time.Millisecond})
				lifecycle.completion.Store(&request)
				if err := lifecycle.recoverUpgradeServiceEvidence(ctx, claim); err != nil {
					t.Fatal("retained evidence could not replay:", err)
				}
				if receipt := outbox.spool.inspectCompletion(t.Context(), claim.Lease.AttemptID); receipt.State != "delivered" || receipt.EventCount != 0 {
					t.Fatalf("replay did not settle exact evidence: %+v", receipt)
				}
			})
		}
	}
}
