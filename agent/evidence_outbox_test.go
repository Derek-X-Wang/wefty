package agent

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/fabric/plain"
	"github.com/Derek-X-Wang/wefty/l1"
	processrunner "github.com/Derek-X-Wang/wefty/runner/process"
)

func TestAttemptPersistsCompletionBeforeDelivery(t *testing.T) {
	assertAttemptPersistsCompletionBeforeDelivery(t)
}

func TestBoundedFinalizationDeadlineThroughDurableLogSinkPreservesPayload(t *testing.T) {
	uploadStarted := make(chan struct{}, 1)
	handler := http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if !strings.HasSuffix(request.URL.Path, "/logs") {
			http.NotFound(w, request)
			return
		}
		select {
		case uploadStarted <- struct{}{}:
		default:
		}
		<-request.Context().Done()
	})
	client, stopServer := startEvidenceReplayServer(t, handler, time.Second)
	defer stopServer()
	defer client.Close()
	outbox, err := newEvidenceOutbox(t.TempDir(), "bounded-log-node", 1024*1024, systemClock{}, 8, time.Hour, time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	defer outbox.Close()
	claim := l1.Claim{
		Job: l1.Job{JobID: "bounded-log-job", Spec: contract.JobSpec{
			Kind: contract.JobKindProcess, Class: contract.JobClassOneShot,
		}},
		Lease: l1.AttemptLease{AttemptID: "bounded-log-attempt", FencingToken: "fence"},
	}
	lifecycle := newAttemptLifecycle(attemptLifecycleDependencies{
		client: client, outbox: outbox,
		runtimes: workloadRuntimeSet{contract.JobKindProcess: &restartableCrashRuntime{}},
		clock:    systemClock{}, finalizationTimeout: 25 * time.Millisecond,
	})
	result, runErr := lifecycle.runWorkload(t.Context(), claim)
	if runErr != nil || result.Signal != "killed" || result.OutputError != "" || !result.LogEvidenceIncomplete {
		t.Fatalf("bounded production log finalization = %#v err=%v", result, runErr)
	}
	select {
	case <-uploadStarted:
	default:
		t.Fatal("durable log sink never attempted AppendLogs")
	}
	pending, err := outbox.spool.pending(t.Context(), claim.Lease.AttemptID, 8)
	if err != nil || len(pending) != 1 || string(pending[0].Bytes) != "before crash" {
		t.Fatalf("recoverable spooled log events = %#v err=%v", pending, err)
	}
}

func TestAttemptHandsAbandonedCompletionToRecovery(t *testing.T) {
	firstCompletionStarted := make(chan struct{}, 1)
	replayed := make(chan l1.CompletionRequest, 1)
	var mu sync.Mutex
	completionCalls := 0
	handler := http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if !strings.HasSuffix(request.URL.Path, "/complete") {
			http.NotFound(w, request)
			return
		}
		mu.Lock()
		completionCalls++
		call := completionCalls
		mu.Unlock()
		if call == 1 {
			firstCompletionStarted <- struct{}{}
			<-request.Context().Done()
			return
		}
		var completion l1.CompletionRequest
		if err := json.NewDecoder(request.Body).Decode(&completion); err != nil {
			t.Error(err)
			return
		}
		replayed <- completion
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusConflict)
		_ = json.NewEncoder(w).Encode(contract.ErrorResponse{Error: contract.APIError{
			Code: contract.ErrorLeaseExpired, Message: "attempt lease has expired",
		}})
	})
	client, stopServer := startEvidenceReplayServer(t, handler, time.Second)
	defer stopServer()
	defer client.Close()
	outbox, err := newEvidenceOutbox(t.TempDir(), "stable-node", 1024, systemClock{}, 8, time.Hour, time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	defer outbox.Close()
	outbox.startRecovery(t.Context(), client, func(err error) { t.Errorf("recover durable evidence: %v", err) })
	claim := l1.Claim{
		Job: l1.Job{JobID: "job-live-handoff", Spec: contract.JobSpec{
			Class: contract.JobClassOneShot, Kind: contract.JobKindProcess,
			Execution: contract.ExecutionSpec{Executable: contract.ExecutableSpec{Path: "ignored-by-fake-runner"},
				Argv: []string{"ignored-by-fake-runner"}, WorkingDirectory: t.TempDir()},
		}},
		Lease: l1.AttemptLease{AttemptID: "attempt-live-handoff", FencingToken: "fence-live-handoff", LeaseTTL: time.Minute},
	}
	lifecycle := newAttemptLifecycle(attemptLifecycleDependencies{
		client: client, runtimes: testRuntimeSet(instantResultRunner{}), outbox: outbox,
		clock: systemClock{}, renewalInterval: 10 * time.Second, completionRetry: time.Millisecond,
		observer: newLifecycleObserver(systemClock{}),
	})
	ctx, cancel := context.WithCancel(t.Context())
	executeDone := make(chan error, 1)
	go func() {
		_, executeErr := lifecycle.execute(ctx, claim, time.Now())
		executeDone <- executeErr
	}()
	select {
	case <-firstCompletionStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("live completion did not start")
	}
	cancel()
	select {
	case <-executeDone:
	case <-time.After(2 * time.Second):
		t.Fatal("lifecycle did not abandon canceled live completion")
	}
	select {
	case completion := <-replayed:
		if completion.Result.ExitCode == nil || *completion.Result.ExitCode != 0 {
			t.Fatalf("replayed result = %+v", completion.Result)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("abandoned lifecycle completion was not reconciled")
	}
	waitCompletionReceiptState(t, outbox, claim.Lease.AttemptID, "delivered", 2*time.Second)
}

func assertAttemptPersistsCompletionBeforeDelivery(t *testing.T) {
	t.Helper()
	completionStarted := make(chan struct{}, 1)
	releaseCompletion := make(chan struct{})
	handler := http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if !strings.HasSuffix(request.URL.Path, "/complete") {
			http.NotFound(w, request)
			return
		}
		completionStarted <- struct{}{}
		<-releaseCompletion
		_ = json.NewEncoder(w).Encode(l1.Job{})
	})
	client, stopServer := startEvidenceReplayServer(t, handler, time.Second)
	defer stopServer()
	defer client.Close()
	outbox, err := newEvidenceOutbox(t.TempDir(), "stable-node", 1024, systemClock{}, 8, time.Hour, time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	defer outbox.Close()
	now := time.Now().UTC()
	claim := l1.Claim{
		Job: l1.Job{
			JobID: "job-durable-before-delivery", CreatedAt: now,
			Spec: contract.JobSpec{
				Class: contract.JobClassOneShot, Kind: "process",
				Execution: contract.ExecutionSpec{
					Executable: contract.ExecutableSpec{Path: "ignored-by-fake-runner"},
					Argv:       []string{"ignored-by-fake-runner"}, WorkingDirectory: t.TempDir(),
				},
			},
		},
		Lease: l1.AttemptLease{
			AttemptID: "attempt-durable-before-delivery", FencingToken: "fence",
			LeaseTTL: time.Minute,
		},
	}
	lifecycle := newAttemptLifecycle(attemptLifecycleDependencies{
		client: client, runtimes: testRuntimeSet(instantResultRunner{}), outbox: outbox,
		clock: systemClock{}, renewalInterval: 10 * time.Second, completionRetry: time.Millisecond,
		observer: newLifecycleObserver(systemClock{}),
	})
	executeDone := make(chan error, 1)
	go func() {
		_, err := lifecycle.execute(context.Background(), claim, now)
		executeDone <- err
	}()
	select {
	case <-completionStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("completion delivery did not start")
	}
	stored, _, present, err := outbox.spool.completion(context.Background(), claim.Lease.AttemptID)
	if err != nil {
		t.Fatal(err)
	}
	if !present || stored.ExitCode == nil || *stored.ExitCode != 0 {
		t.Fatalf("completion was not durable before delivery: %#v present=%t", stored, present)
	}
	close(releaseCompletion)
	select {
	case err := <-executeDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("attempt did not finish after completion acknowledgement")
	}
}

type instantResultRunner struct{}

func (instantResultRunner) Run(_ context.Context, request processrunner.Request, _ processrunner.OutputSink) (contract.ProcessResult, error) {
	if request.Started != nil {
		request.Started()
	}
	exitCode := 0
	return contract.ProcessResult{ExitCode: &exitCode}, nil
}

func TestEvidenceRecoveryIsolatesTransientPoisonAttempt(t *testing.T) {
	assertEvidenceRecoveryIsolatesTransientPoisonAttempt(t)
}

func TestEvidenceRecoveryWakesForCompletionStoredAfterInitialScan(t *testing.T) {
	initialRecoveryFinished := make(chan struct{})
	releaseInitialRecovery := make(chan struct{})
	var finishOnce sync.Once
	var releaseOnce sync.Once
	releaseRecovery := func() { releaseOnce.Do(func() { close(releaseInitialRecovery) }) }
	completionReceived := make(chan l1.CompletionRequest, 1)
	handler := http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if strings.HasSuffix(request.URL.Path, "/logs") {
			_ = json.NewEncoder(w).Encode(l1.AppendLogsResponse{Acknowledged: map[contract.LogStream]uint64{contract.LogStdout: 0}})
			return
		}
		var completion l1.CompletionRequest
		if err := json.NewDecoder(request.Body).Decode(&completion); err != nil {
			t.Error(err)
			return
		}
		completionReceived <- completion
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusConflict)
		_ = json.NewEncoder(w).Encode(contract.ErrorResponse{Error: contract.APIError{
			Code: contract.ErrorLeaseExpired, Message: "attempt lease has expired",
		}})
	})
	client, stopServer := startEvidenceReplayServer(t, handler, time.Second)
	defer stopServer()
	defer client.Close()
	outbox, err := newEvidenceOutbox(t.TempDir(), "stable-node", 1024, systemClock{}, 8, time.Hour, time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	defer outbox.Close()
	defer releaseRecovery()
	claim := spoolTestClaim("attempt-completion-after-scan")
	if err := outbox.ensureAttempt(t.Context(), claim); err != nil {
		t.Fatal(err)
	}
	if err := outbox.spool.append(t.Context(), spoolTestEvent(claim.Lease.AttemptID, contract.LogStdout, 0, "load")); err != nil {
		t.Fatal(err)
	}
	outbox.recoveryAttemptFinished = func(attemptID string) {
		if attemptID != claim.Lease.AttemptID {
			return
		}
		finishOnce.Do(func() {
			close(initialRecoveryFinished)
			<-releaseInitialRecovery
		})
	}

	outbox.startRecovery(t.Context(), client, func(err error) { t.Errorf("recover durable evidence: %v", err) })
	select {
	case <-initialRecoveryFinished:
	case <-time.After(2 * time.Second):
		t.Fatal("initial recovery did not reach its active-attempt retirement edge")
	}
	result := l1.ProcessResult{RuntimeFailure: &contract.RuntimeFailure{
		Code: contract.RuntimeFailureUnavailable, Message: "helper generation lost",
	}}
	if err := outbox.storeCompletion(t.Context(), claim.Lease.AttemptID, result, time.Now(), l1.RuntimeQuiescenceOCISweep); err != nil {
		t.Fatal(err)
	}
	outbox.scheduleRecovery()
	releaseRecovery()

	select {
	case delivered := <-completionReceived:
		if delivered.Result.RuntimeFailure == nil || delivered.Result.RuntimeFailure.Code != contract.RuntimeFailureUnavailable ||
			delivered.RuntimeQuiescenceEvidence != l1.RuntimeQuiescenceOCISweep {
			t.Fatalf("replayed completion = %+v", delivered)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("same-attempt completion wake was swallowed at active recovery retirement")
	}
	deadline := time.Now().Add(2 * time.Second)
	for {
		receipt := outbox.spool.inspectCompletion(t.Context(), claim.Lease.AttemptID)
		if receipt.State == "delivered" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("late completion receipt = %+v, want delivered", receipt)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestEvidenceRecoveryDoesNotReplayLiveAttempt(t *testing.T) {
	liveRequest := make(chan string, 1)
	lateCompletion := make(chan struct{}, 1)
	handler := http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if strings.Contains(request.URL.Path, "attempt-live") {
			liveRequest <- request.URL.Path
			http.Error(w, "live evidence must stay with its sink", http.StatusInternalServerError)
			return
		}
		lateCompletion <- struct{}{}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusConflict)
		_ = json.NewEncoder(w).Encode(contract.ErrorResponse{Error: contract.APIError{
			Code: contract.ErrorLeaseExpired, Message: "attempt lease has expired",
		}})
	})
	client, stopServer := startEvidenceReplayServer(t, handler, time.Second)
	defer stopServer()
	defer client.Close()
	outbox, err := newEvidenceOutbox(t.TempDir(), "stable-node", 1024, systemClock{}, 8, time.Hour, time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	defer outbox.Close()
	live := spoolTestClaim("attempt-live")
	if err := outbox.ensureAttempt(t.Context(), live); err != nil {
		t.Fatal(err)
	}
	if err := outbox.spool.append(t.Context(), spoolTestEvent(live.Lease.AttemptID, contract.LogStdout, 0, "owned")); err != nil {
		t.Fatal(err)
	}
	outbox.ownAttempt(live.Lease.AttemptID)
	defer outbox.releaseAttempt(live.Lease.AttemptID, false)
	late := spoolTestClaim("attempt-late")
	if err := outbox.ensureAttempt(t.Context(), late); err != nil {
		t.Fatal(err)
	}
	if err := outbox.storeCompletion(t.Context(), late.Lease.AttemptID, l1.ProcessResult{RuntimeFailure: &contract.RuntimeFailure{
		Code: contract.RuntimeFailureUnavailable, Message: "authority lost",
	}}, time.Now(), l1.RuntimeQuiescenceOCISweep); err != nil {
		t.Fatal(err)
	}
	outbox.startRecovery(t.Context(), client, func(err error) { t.Errorf("recover durable evidence: %v", err) })
	select {
	case <-lateCompletion:
	case <-time.After(2 * time.Second):
		t.Fatal("unowned late completion was not reconciled")
	}
	select {
	case path := <-liveRequest:
		t.Fatalf("reconciler touched live attempt at %s", path)
	case <-time.After(50 * time.Millisecond):
	}
}

func TestPendingEvidenceDoesNotBlockRegistration(t *testing.T) {
	assertPendingEvidenceDoesNotBlockRegistration(t)
}

func assertPendingEvidenceDoesNotBlockRegistration(t *testing.T) {
	t.Helper()
	spoolDirectory := t.TempDir()
	claim := spoolTestClaim("attempt-poison-boot")
	spool := openTestLogSpool(t, spoolDirectory, "stable-node", 1024)
	if err := spool.ensureAttempt(context.Background(), claim); err != nil {
		t.Fatal(err)
	}
	if err := spool.append(context.Background(), spoolTestEvent(claim.Lease.AttemptID, contract.LogStdout, 0, "pending")); err != nil {
		t.Fatal(err)
	}
	if err := spool.Close(); err != nil {
		t.Fatal(err)
	}

	poisonStarted := make(chan struct{}, 1)
	registered := make(chan struct{}, 1)
	handler := http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		switch {
		case strings.HasSuffix(request.URL.Path, "/logs"):
			select {
			case poisonStarted <- struct{}{}:
			default:
			}
			<-request.Context().Done()
		case strings.HasSuffix(request.URL.Path, "/register"):
			select {
			case registered <- struct{}{}:
			default:
			}
			_ = json.NewEncoder(w).Encode(l1.Node{
				NodeRegistration: contract.NodeRegistration{NodeID: "stable-node", BootSessionID: "boot-2"}, State: contract.NodeAlive,
				ClaimsEnabled: true, MaxOneshotSlots: 1, MaxServiceSlots: 1,
			})
		case strings.HasSuffix(request.URL.Path, "/heartbeat"):
			_ = json.NewEncoder(w).Encode(l1.Node{
				NodeRegistration: contract.NodeRegistration{NodeID: "stable-node", BootSessionID: "boot-2"}, State: contract.NodeAlive,
				ClaimsEnabled: true, MaxOneshotSlots: 1, MaxServiceSlots: 1,
			})
		case strings.HasSuffix(request.URL.Path, "/claim"):
			w.WriteHeader(http.StatusNoContent)
		default:
			http.NotFound(w, request)
		}
	})
	network := plain.NewNetwork()
	serverFabric := network.NewFabric(fabric.Identity{NodeID: "control-plane"})
	listener, err := serverFabric.Listen("tcp", "wefty://control-plane")
	if err != nil {
		t.Fatal(err)
	}
	server := &http.Server{Handler: handler}
	serverDone := make(chan error, 1)
	go func() { serverDone <- server.Serve(listener) }()
	defer func() {
		_ = server.Close()
		if err := <-serverDone; err != nil && err != http.ErrServerClosed && !strings.Contains(err.Error(), "read identity header: EOF") {
			t.Errorf("serve boot replay test: %v", err)
		}
	}()

	participant := network.NewFabric(fabric.Identity{NodeID: "agent", Tags: []string{l1.DefaultAgentPrincipalTag}})
	nodeAgent, err := New(Config{
		Fabric: participant, ControlPlaneAddress: "wefty://control-plane",
		NodeID: "stable-node", BootSessionID: "boot-2", Version: "test",
		OperationTimeout: 50 * time.Millisecond, LogRetryInterval: time.Millisecond,
		HeartbeatInterval: time.Second, ClaimInterval: 10 * time.Millisecond,
		LogSpoolDirectory: spoolDirectory, LogSpoolMaxBytes: 1024,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer nodeAgent.Close()
	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan error, 1)
	go func() { runDone <- nodeAgent.Run(ctx) }()
	select {
	case <-poisonStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("pending evidence recovery did not start")
	}
	select {
	case <-registered:
	case <-time.After(2 * time.Second):
		t.Fatal("pending evidence blocked registration")
	}
	select {
	case err := <-runDone:
		t.Fatalf("agent exited while poison replay was pending: %v", err)
	default:
	}
	cancel()
	select {
	case err := <-runDone:
		if err != nil {
			t.Fatalf("agent Run() after cancellation = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("agent did not stop")
	}
}

func assertEvidenceRecoveryIsolatesTransientPoisonAttempt(t *testing.T) {
	t.Helper()
	poisonRequestStarted := make(chan struct{}, 1)
	handler := http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if strings.Contains(request.URL.Path, "attempt-poison") {
			select {
			case poisonRequestStarted <- struct{}{}:
			default:
			}
			<-request.Context().Done()
			return
		}
		var appendRequest l1.AppendLogsRequest
		if err := json.NewDecoder(request.Body).Decode(&appendRequest); err != nil {
			t.Error(err)
			return
		}
		acknowledged := map[contract.LogStream]uint64{}
		for _, event := range appendRequest.Events {
			acknowledged[event.Stream] = eventEndSequence(event)
		}
		_ = json.NewEncoder(w).Encode(l1.AppendLogsResponse{Acknowledged: acknowledged})
	})
	client, stopServer := startEvidenceReplayServer(t, handler, 50*time.Millisecond)
	defer stopServer()
	defer client.Close()
	outbox, err := newEvidenceOutbox(t.TempDir(), "stable-node", 1024, systemClock{}, 8, time.Hour, time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	defer outbox.Close()
	for _, attemptID := range []string{"attempt-poison", "attempt-good"} {
		claim := spoolTestClaim(attemptID)
		if err := outbox.ensureAttempt(context.Background(), claim); err != nil {
			t.Fatal(err)
		}
		if err := outbox.spool.append(context.Background(), spoolTestEvent(attemptID, contract.LogStdout, 0, attemptID)); err != nil {
			t.Fatal(err)
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	recoveryDone := make(chan error, 1)
	go func() { recoveryDone <- outbox.recover(ctx, client) }()
	select {
	case <-poisonRequestStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("poison replay did not start")
	}
	waitForSpoolHighWater(t, outbox.spool, "attempt-good", contract.LogStdout, 0)
	cancel()
	select {
	case <-recoveryDone:
	case <-time.After(2 * time.Second):
		t.Fatal("recovery did not stop after cancellation")
	}
}

func TestEvidenceRecoveryReplacesPermanentReplayRejectionWithGap(t *testing.T) {
	assertEvidenceRecoveryReplacesPermanentReplayRejectionWithGap(t)
}

func TestEvidenceRecoverySealsAuthorityLossIncomplete(t *testing.T) {
	assertEvidenceRecoverySealsAuthorityLossIncomplete(t)
}

func TestLiveCompletionAuthorityLossSealsDurableEvidence(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		_ = json.NewEncoder(w).Encode(contract.ErrorResponse{Error: contract.APIError{
			Code: contract.ErrorAttemptNotFound, Message: "attempt was removed",
		}})
	})
	client, stopServer := startEvidenceReplayServer(t, handler, time.Second)
	defer stopServer()
	defer client.Close()
	outbox, err := newEvidenceOutbox(t.TempDir(), "stable-node", 1024, systemClock{}, 8, time.Hour, time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	defer outbox.Close()
	claim := spoolTestClaim("attempt-live-authority-loss")
	if err := outbox.ensureAttempt(context.Background(), claim); err != nil {
		t.Fatal(err)
	}
	if err := outbox.spool.append(context.Background(), spoolTestEvent(claim.Lease.AttemptID, contract.LogStdout, 0, "raw")); err != nil {
		t.Fatal(err)
	}
	exitCode := 0
	result := l1.ProcessResult{ExitCode: &exitCode}
	if err := outbox.storeCompletion(context.Background(), claim.Lease.AttemptID, result, time.Now()); err != nil {
		t.Fatal(err)
	}
	lifecycle := newAttemptLifecycle(attemptLifecycleDependencies{
		client: client, outbox: outbox, clock: systemClock{}, completionRetry: time.Millisecond,
	})
	failure := lifecycle.completeWithRetry(context.Background(), claim, l1.CompletionRequest{
		FencingToken: claim.Lease.FencingToken, IdempotencyKey: "completion:" + claim.Lease.AttemptID, Result: result,
	})
	if failure.destination != errorDestinationAttemptAuthority || failure.err == nil {
		t.Fatalf("completion failure = %#v, want attempt-authority", failure)
	}
	var eventCount int
	var incompleteJSON []byte
	if err := outbox.spool.db.QueryRow(`SELECT
  (SELECT COUNT(*) FROM spool_events WHERE attempt_id=spool_attempts.attempt_id), incomplete_json
FROM spool_attempts WHERE attempt_id=?`, claim.Lease.AttemptID).Scan(&eventCount, &incompleteJSON); err != nil {
		t.Fatal(err)
	}
	if eventCount != 0 || len(incompleteJSON) == 0 {
		t.Fatalf("live authority-loss seal = events %d tombstone %q", eventCount, incompleteJSON)
	}
}

func assertEvidenceRecoverySealsAuthorityLossIncomplete(t *testing.T) {
	t.Helper()
	handler := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		_ = json.NewEncoder(w).Encode(contract.ErrorResponse{Error: contract.APIError{
			Code: contract.ErrorAttemptNotFound, Message: "attempt was removed",
		}})
	})
	client, stopServer := startEvidenceReplayServer(t, handler, time.Second)
	defer stopServer()
	defer client.Close()
	outbox, err := newEvidenceOutbox(t.TempDir(), "stable-node", 1024, systemClock{}, 8, time.Hour, time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	defer outbox.Close()
	claim := spoolTestClaim("attempt-removed")
	if err := outbox.ensureAttempt(context.Background(), claim); err != nil {
		t.Fatal(err)
	}
	if err := outbox.spool.append(context.Background(), spoolTestEvent(claim.Lease.AttemptID, contract.LogStdout, 0, "raw")); err != nil {
		t.Fatal(err)
	}
	if err := outbox.recover(context.Background(), client); err == nil || !strings.Contains(err.Error(), "sealed incomplete") {
		t.Fatalf("recovery error = %v, want sealed-incomplete report", err)
	}
	var eventCount int
	var incompleteJSON []byte
	if err := outbox.spool.db.QueryRow(`SELECT
  (SELECT COUNT(*) FROM spool_events WHERE attempt_id=spool_attempts.attempt_id), incomplete_json
FROM spool_attempts WHERE attempt_id=?`, claim.Lease.AttemptID).Scan(&eventCount, &incompleteJSON); err != nil {
		t.Fatal(err)
	}
	if eventCount != 0 || len(incompleteJSON) == 0 {
		t.Fatalf("authority-loss seal = events %d tombstone %q", eventCount, incompleteJSON)
	}
}

func assertEvidenceRecoveryReplacesPermanentReplayRejectionWithGap(t *testing.T) {
	t.Helper()
	var mu sync.Mutex
	var uploads []l1.AppendLogsRequest
	handler := http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		payload, err := io.ReadAll(request.Body)
		if err != nil {
			t.Error(err)
			return
		}
		var appendRequest l1.AppendLogsRequest
		if err := json.Unmarshal(payload, &appendRequest); err != nil {
			t.Error(err)
			return
		}
		mu.Lock()
		uploads = append(uploads, appendRequest)
		requestNumber := len(uploads)
		mu.Unlock()
		if requestNumber == 1 {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(contract.ErrorResponse{Error: contract.APIError{
				Code: contract.ErrorInvalidRequest, Message: "raw event is permanently unsendable",
			}})
			return
		}
		acknowledged := map[contract.LogStream]uint64{}
		for _, event := range appendRequest.Events {
			acknowledged[event.Stream] = eventEndSequence(event)
		}
		_ = json.NewEncoder(w).Encode(l1.AppendLogsResponse{Acknowledged: acknowledged})
	})
	client, stopServer := startEvidenceReplayServer(t, handler, time.Second)
	defer stopServer()
	defer client.Close()
	outbox, err := newEvidenceOutbox(t.TempDir(), "stable-node", 1024, systemClock{}, 8, time.Hour, time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	defer outbox.Close()
	claim := spoolTestClaim("attempt-rejected")
	if err := outbox.ensureAttempt(context.Background(), claim); err != nil {
		t.Fatal(err)
	}
	if err := outbox.spool.append(context.Background(), spoolTestEvent(claim.Lease.AttemptID, contract.LogStdout, 0, "raw")); err != nil {
		t.Fatal(err)
	}
	if err := outbox.recover(context.Background(), client); err != nil {
		t.Fatal(err)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(uploads) != 2 {
		t.Fatalf("uploads = %d, want rejected raw event and replacement gap", len(uploads))
	}
	replacement := uploads[1].Events
	if len(replacement) != 1 || replacement[0].Gap == nil || replacement[0].Gap.Reason != contract.LogGapReplayRejected || string(replacement[0].Bytes) != "" {
		t.Fatalf("replacement upload = %#v", replacement)
	}
}

func startEvidenceReplayServer(t *testing.T, handler http.Handler, operationTimeout time.Duration) (*Client, func()) {
	t.Helper()
	network := plain.NewNetwork()
	serverFabric := network.NewFabric(fabric.Identity{NodeID: "control-plane"})
	listener, err := serverFabric.Listen("tcp", "wefty://control-plane")
	if err != nil {
		t.Fatal(err)
	}
	server := &http.Server{Handler: handler}
	done := make(chan error, 1)
	go func() { done <- server.Serve(listener) }()
	participant := network.NewFabric(fabric.Identity{NodeID: "agent"})
	client, err := newClient(participant, "wefty://control-plane", operationTimeout)
	if err != nil {
		t.Fatal(err)
	}
	return client, func() {
		_ = server.Close()
		if err := <-done; err != nil && err != http.ErrServerClosed && !strings.Contains(err.Error(), "read identity header: EOF") {
			t.Errorf("serve evidence replay test: %v", err)
		}
	}
}
