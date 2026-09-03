package agent

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/fabric/plain"
	"github.com/Derek-X-Wang/wefty/l1"
	workloadrunner "github.com/Derek-X-Wang/wefty/runner"
	"github.com/Derek-X-Wang/wefty/runner/lima"
	processrunner "github.com/Derek-X-Wang/wefty/runner/process"
)

func TestAttemptPersistsCompletionBeforeDelivery(t *testing.T) {
	assertAttemptPersistsCompletionBeforeDelivery(t)
}

func TestOCIServiceCompletionAuthorityUnavailableRetainsEvidence(t *testing.T) {
	var completionCalls int
	handler := http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if !strings.HasSuffix(request.URL.Path, "/complete") {
			http.NotFound(w, request)
			return
		}
		completionCalls++
		_ = json.NewEncoder(w).Encode(l1.Job{})
	})
	client, stopServer := startEvidenceReplayServer(t, handler, time.Second)
	defer stopServer()
	defer client.Close()

	newPending := func(t *testing.T, attemptID, kind string) (*evidenceOutbox, l1.Claim, logSpoolAttempt) {
		t.Helper()
		outbox, err := newEvidenceOutbox(t.TempDir(), "intent-authority-node", 1<<20, systemClock{}, 8, time.Hour, time.Millisecond)
		if err != nil {
			t.Fatal(err)
		}
		claim := l1.Claim{Job: l1.Job{JobID: "intent-authority-job-" + attemptID, Spec: contract.JobSpec{
			Kind: kind, Class: contract.JobClassService,
		}}, Lease: l1.AttemptLease{AttemptID: attemptID, FencingToken: "fence-" + attemptID}}
		if err := outbox.ensureAttempt(t.Context(), claim); err != nil {
			t.Fatal(err)
		}
		exitCode := 7
		if err := outbox.storeCompletion(t.Context(), attemptID, l1.ProcessResult{ExitCode: &exitCode}, time.Now(), l1.RuntimeQuiescenceAttempt); err != nil {
			t.Fatal(err)
		}
		return outbox, claim, logSpoolAttempt{jobID: claim.Job.JobID, attemptID: attemptID, fencingToken: claim.Lease.FencingToken, class: contract.JobClassService, kind: kind}
	}

	t.Run("transient read error retries intact payload", func(t *testing.T) {
		outbox, _, attempt := newPending(t, "transient", contract.JobKindOCI)
		defer outbox.Close()
		reads := 0
		outbox.ociIntentGate = &ociIntentCompletionGate{observe: func(context.Context) (OCIIntentObservation, error) {
			reads++
			if reads <= 2 {
				return OCIIntentObservation{}, errors.New("transient EIO")
			}
			return OCIIntentObservation{Enabled: true, Revision: 9}, nil
		}}
		err := outbox.recoverCompletion(t.Context(), client, attempt)
		var unavailable *OCIIntentAuthorityUnavailableError
		if !errors.As(err, &unavailable) || completionCalls != 0 {
			t.Fatalf("first recovery err=%v calls=%d", err, completionCalls)
		}
		receipt := outbox.spool.inspectCompletion(t.Context(), attempt.attemptID)
		if receipt.State != "withheld" || receipt.Reason != "intent_authority_unavailable" || receipt.Result.ExitCode == nil || *receipt.Result.ExitCode != 7 {
			t.Fatalf("withheld completion receipt=%+v", receipt)
		}
		var firstObservedNS int64
		if err := outbox.spool.db.QueryRowContext(t.Context(), `SELECT observed_ns FROM spool_completion_receipts WHERE attempt_id=?`, attempt.attemptID).Scan(&firstObservedNS); err != nil {
			t.Fatal(err)
		}
		if err := outbox.recoverCompletion(t.Context(), client, attempt); !errors.As(err, &unavailable) {
			t.Fatalf("second unavailable recovery err=%v", err)
		}
		var secondObservedNS int64
		if err := outbox.spool.db.QueryRowContext(t.Context(), `SELECT observed_ns FROM spool_completion_receipts WHERE attempt_id=?`, attempt.attemptID).Scan(&secondObservedNS); err != nil {
			t.Fatal(err)
		}
		if secondObservedNS != firstObservedNS {
			t.Fatalf("withheld disposition rewritten: before=%d after=%d", firstObservedNS, secondObservedNS)
		}
		if err := outbox.recoverCompletion(t.Context(), client, attempt); err != nil {
			t.Fatal(err)
		}
		receipt = outbox.spool.inspectCompletion(t.Context(), attempt.attemptID)
		if receipt.State != "delivered" || receipt.IntentRevision != 9 || completionCalls != 1 {
			t.Fatalf("retried completion receipt=%+v calls=%d", receipt, completionCalls)
		}
	})

	t.Run("nil authority leaves the fence inapplicable", func(t *testing.T) {
		for _, kind := range []string{contract.JobKindProcess, contract.JobKindOCI} {
			t.Run(kind, func(t *testing.T) {
				outbox, _, attempt := newPending(t, "nil-"+kind, kind)
				defer outbox.Close()
				callsBefore := completionCalls
				if err := outbox.recoverCompletion(t.Context(), client, attempt); err != nil {
					t.Fatalf("nil authority %s completion: %v", kind, err)
				}
				receipt := outbox.spool.inspectCompletion(t.Context(), attempt.attemptID)
				if receipt.State != "delivered" || completionCalls != callsBefore+1 {
					t.Fatalf("nil-authority %s receipt=%+v calls=%d", kind, receipt, completionCalls)
				}
			})
		}
	})

	t.Run("absent production marker is typed and withheld", func(t *testing.T) {
		outbox, _, attempt := newPending(t, "absent", contract.JobKindOCI)
		defer outbox.Close()
		source := lima.FileIntentSource{Path: filepath.Join(t.TempDir(), "missing-intent.json")}
		outbox.ociIntentGate = &ociIntentCompletionGate{observe: func(ctx context.Context) (OCIIntentObservation, error) {
			intent, err := source.ReadIntent(ctx)
			return OCIIntentObservation{Enabled: intent.Enabled, Revision: intent.Revision}, err
		}}
		err := outbox.recoverCompletion(t.Context(), client, attempt)
		var unavailable *OCIIntentAuthorityUnavailableError
		if !errors.As(err, &unavailable) {
			t.Fatalf("absent marker err=%T %v", err, err)
		}
		receipt := outbox.spool.inspectCompletion(t.Context(), attempt.attemptID)
		if receipt.State != "withheld" || receipt.Reason != "intent_authority_unavailable" || receipt.IntentRevision != 0 ||
			receipt.Result.ExitCode == nil || *receipt.Result.ExitCode != 7 {
			t.Fatalf("absent-marker receipt=%+v", receipt)
		}
	})
}

func TestSuppressedCompletionStillDrainsLogsWithoutRedispositionLoop(t *testing.T) {
	var logCalls, completionCalls int
	handler := http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		switch {
		case strings.HasSuffix(request.URL.Path, "/logs"):
			logCalls++
			var appendRequest l1.AppendLogsRequest
			if err := json.NewDecoder(request.Body).Decode(&appendRequest); err != nil {
				t.Error(err)
				return
			}
			_ = json.NewEncoder(w).Encode(l1.AppendLogsResponse{
				Acknowledged: map[contract.LogStream]uint64{contract.LogStdout: eventEndSequence(appendRequest.Events[0])},
				AttemptState: contract.AttemptRunning,
			})
		case strings.HasSuffix(request.URL.Path, "/complete"):
			completionCalls++
			_ = json.NewEncoder(w).Encode(l1.Job{})
		default:
			http.NotFound(w, request)
		}
	})
	client, stopServer := startEvidenceReplayServer(t, handler, time.Second)
	defer stopServer()
	defer client.Close()
	outbox, err := newEvidenceOutbox(t.TempDir(), "suppressed-logs-node", 1<<20, systemClock{}, 8, time.Hour, time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	defer outbox.Close()
	claim := l1.Claim{Job: l1.Job{JobID: "suppressed-logs-job", Spec: contract.JobSpec{Kind: contract.JobKindOCI, Class: contract.JobClassService}},
		Lease: l1.AttemptLease{AttemptID: "suppressed-logs-attempt", FencingToken: "fence"}}
	if err := outbox.ensureAttempt(t.Context(), claim); err != nil {
		t.Fatal(err)
	}
	if err := outbox.spool.append(t.Context(), spoolTestEvent(claim.Lease.AttemptID, contract.LogStdout, 0, "final log")); err != nil {
		t.Fatal(err)
	}
	exitCode := 7
	if err := outbox.storeCompletion(t.Context(), claim.Lease.AttemptID, l1.ProcessResult{ExitCode: &exitCode}, time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := outbox.suppressCompletion(t.Context(), claim.Lease.AttemptID, 4); err != nil {
		t.Fatal(err)
	}
	var observedNS int64
	if err := outbox.spool.db.QueryRowContext(t.Context(), `SELECT observed_ns FROM spool_completion_receipts WHERE attempt_id=?`, claim.Lease.AttemptID).Scan(&observedNS); err != nil {
		t.Fatal(err)
	}
	attempt := logSpoolAttempt{jobID: claim.Job.JobID, attemptID: claim.Lease.AttemptID, fencingToken: claim.Lease.FencingToken, class: contract.JobClassService, kind: contract.JobKindOCI}
	for range 3 {
		if err := outbox.recoverAttempt(t.Context(), client, attempt); err != nil {
			t.Fatal(err)
		}
	}
	if logCalls != 1 || completionCalls != 0 {
		t.Fatalf("recovery calls logs=%d complete=%d, want 1/0", logCalls, completionCalls)
	}
	if events, err := outbox.spool.pending(t.Context(), claim.Lease.AttemptID, 8); err != nil || len(events) != 0 {
		t.Fatalf("suppressed attempt retained events=%+v err=%v", events, err)
	}
	var observedAfter int64
	if err := outbox.spool.db.QueryRowContext(t.Context(), `SELECT observed_ns FROM spool_completion_receipts WHERE attempt_id=?`, claim.Lease.AttemptID).Scan(&observedAfter); err != nil {
		t.Fatal(err)
	}
	if observedAfter != observedNS {
		t.Fatalf("suppressed disposition rewritten: before=%d after=%d", observedNS, observedAfter)
	}
	if attempts, err := outbox.spool.pendingAttempts(t.Context()); err != nil || len(attempts) != 0 {
		t.Fatalf("drained suppressed attempt remained pending=%+v err=%v", attempts, err)
	}
}

func TestOCIIntentStopWaitsForInFlightCompletionRequest(t *testing.T) {
	requestEntered := make(chan struct{})
	releaseRequest := make(chan struct{})
	handler := http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if !strings.HasSuffix(request.URL.Path, "/complete") {
			http.NotFound(w, request)
			return
		}
		close(requestEntered)
		<-releaseRequest
		_ = json.NewEncoder(w).Encode(l1.Job{})
	})
	client, stopServer := startEvidenceReplayServer(t, handler, time.Second)
	defer stopServer()
	defer client.Close()
	outbox, err := newEvidenceOutbox(t.TempDir(), "inflight-fence-node", 1<<20, systemClock{}, 8, time.Hour, time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	defer outbox.Close()
	outbox.ociIntentGate = &ociIntentCompletionGate{observe: func(context.Context) (OCIIntentObservation, error) {
		return OCIIntentObservation{Enabled: true, Revision: 1}, nil
	}}
	claim := l1.Claim{Job: l1.Job{JobID: "inflight-fence-job", Spec: contract.JobSpec{Kind: contract.JobKindOCI, Class: contract.JobClassService}},
		Lease: l1.AttemptLease{AttemptID: "inflight-fence-attempt", FencingToken: "fence"}}
	if err := outbox.ensureAttempt(t.Context(), claim); err != nil {
		t.Fatal(err)
	}
	exitCode := 7
	if err := outbox.storeCompletion(t.Context(), claim.Lease.AttemptID, l1.ProcessResult{ExitCode: &exitCode}, time.Now()); err != nil {
		t.Fatal(err)
	}
	attempt := logSpoolAttempt{jobID: claim.Job.JobID, attemptID: claim.Lease.AttemptID, fencingToken: claim.Lease.FencingToken, class: contract.JobClassService, kind: contract.JobKindOCI}
	completionDone := make(chan error, 1)
	go func() { completionDone <- outbox.recoverCompletion(t.Context(), client, attempt) }()
	select {
	case <-requestEntered:
	case <-time.After(time.Second):
		t.Fatal("completion request did not enter L1")
	}
	stopDone := make(chan error, 1)
	go func() {
		release, err := outbox.ociIntentGate.beginStop(t.Context(), 2)
		if err == nil {
			release()
		}
		stopDone <- err
	}()
	select {
	case err := <-stopDone:
		t.Fatalf("stop crossed in-flight completion: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	close(releaseRequest)
	if err := <-completionDone; err != nil {
		t.Fatal(err)
	}
	if err := <-stopDone; err != nil {
		t.Fatal(err)
	}
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

func TestLogFinalizationDeadlineHandsCompletionToOrderedRecovery(t *testing.T) {
	var mu sync.Mutex
	var callOrder []string
	logCalls := 0
	completionRejected := 0
	completionSeen := make(chan l1.CompletionRequest, 1)
	handler := http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		switch {
		case strings.HasSuffix(request.URL.Path, "/logs"):
			var appendRequest l1.AppendLogsRequest
			if err := json.NewDecoder(request.Body).Decode(&appendRequest); err != nil {
				t.Error(err)
				return
			}
			mu.Lock()
			logCalls++
			call := logCalls
			callOrder = append(callOrder, "logs")
			mu.Unlock()
			if call == 1 {
				<-request.Context().Done()
				return
			}
			acknowledged := make(map[contract.LogStream]uint64)
			for _, event := range appendRequest.Events {
				acknowledged[event.Stream] = eventEndSequence(event)
			}
			_ = json.NewEncoder(w).Encode(l1.AppendLogsResponse{
				Acknowledged: acknowledged,
				AttemptState: contract.AttemptRunning,
			})
		case strings.HasSuffix(request.URL.Path, "/complete"):
			var completion l1.CompletionRequest
			if err := json.NewDecoder(request.Body).Decode(&completion); err != nil {
				t.Error(err)
				return
			}
			mu.Lock()
			callOrder = append(callOrder, "completion")
			logsReplayed := logCalls >= 2
			if !logsReplayed {
				completionRejected++
			}
			mu.Unlock()
			if !logsReplayed {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusConflict)
				_ = json.NewEncoder(w).Encode(contract.ErrorResponse{Error: contract.APIError{
					Code: contract.ErrorConflict, Message: "completion closed the live attempt before log recovery",
				}})
				return
			}
			completionSeen <- completion
			_ = json.NewEncoder(w).Encode(l1.Job{})
		default:
			http.NotFound(w, request)
		}
	})
	client, stopServer := startEvidenceReplayServer(t, handler, time.Second)
	defer stopServer()
	defer client.Close()
	outbox, err := newEvidenceOutbox(t.TempDir(), "ordered-deadline-node", 1024*1024, systemClock{}, 8, time.Hour, time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	defer outbox.Close()
	outbox.startRecovery(t.Context(), client, func(err error) { t.Errorf("recover durable evidence: %v", err) })
	claim := l1.Claim{
		Job: l1.Job{JobID: "ordered-deadline-job", Spec: contract.JobSpec{
			Kind: contract.JobKindProcess, Class: contract.JobClassOneShot,
		}},
		Lease: l1.AttemptLease{
			AttemptID: "ordered-deadline-attempt", FencingToken: "ordered-deadline-fence", LeaseTTL: time.Minute,
		},
	}
	lifecycle := newAttemptLifecycle(attemptLifecycleDependencies{
		client: client, outbox: outbox,
		runtimes:            workloadRuntimeSet{contract.JobKindProcess: &restartableCrashRuntime{}},
		clock:               systemClock{},
		renewalInterval:     10 * time.Second,
		completionRetry:     time.Millisecond,
		finalizationTimeout: 25 * time.Millisecond,
		observer:            newLifecycleObserver(systemClock{}),
	})
	if _, err := lifecycle.execute(t.Context(), claim, time.Now()); err != nil {
		t.Fatal(err)
	}
	select {
	case completion := <-completionSeen:
		if completion.Result.Signal != "killed" || !completion.Result.LogEvidenceIncomplete || completion.Result.OutputError != "" {
			t.Fatalf("ordered recovery completion = %#v", completion.Result)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("ordered recovery did not deliver completion")
	}
	waitCompletionReceiptState(t, outbox, claim.Lease.AttemptID, "delivered", 2*time.Second)
	mu.Lock()
	defer mu.Unlock()
	if completionRejected != 0 || !reflect.DeepEqual(callOrder, []string{"logs", "logs", "completion"}) {
		t.Fatalf("deadline recovery order = %v with %d rejected completions, want logs/logs/completion and no rejection", callOrder, completionRejected)
	}
}

func TestLogFinalizationDeadlineStillFinishesProcessHandoff(t *testing.T) {
	tests := []struct {
		name      string
		runtime   WorkloadRuntime
		succeeded bool
	}{
		{name: "successful handoff is removed", runtime: instantWorkloadRuntime{}, succeeded: true},
		{name: "failed handoff is retained with deadline", runtime: &restartableCrashRuntime{}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			handler := http.HandlerFunc(func(_ http.ResponseWriter, request *http.Request) {
				<-request.Context().Done()
			})
			client, stopServer := startEvidenceReplayServer(t, handler, time.Second)
			defer stopServer()
			defer client.Close()
			outbox, err := newEvidenceOutbox(t.TempDir(), "handoff-deadline-node", 1024*1024, systemClock{}, 8, time.Hour, time.Millisecond)
			if err != nil {
				t.Fatal(err)
			}
			defer outbox.Close()
			root := filepath.Join(t.TempDir(), "handoffs")
			runID := "run-handoff-deadline"
			path := filepath.Join(root, runID)
			preparedAt := time.Date(2026, 9, 3, 12, 0, 0, 0, time.UTC)
			finishedAt := preparedAt.Add(time.Minute)
			nowCalls := 0
			handoffs := newHandoffManager(root, time.Hour)
			handoffs.now = func() time.Time {
				nowCalls++
				if nowCalls == 1 {
					return preparedAt
				}
				return finishedAt
			}
			claim := l1.Claim{
				Job: l1.Job{JobID: "handoff-deadline-job", Spec: contract.JobSpec{
					Kind: contract.JobKindProcess, Class: contract.JobClassOneShot,
					Labels:    map[string]string{"run_id": runID},
					Execution: contract.ExecutionSpec{WorkingDirectory: t.TempDir(), HandoffDirectory: path},
				}},
				Lease: l1.AttemptLease{AttemptID: "handoff-deadline-attempt", FencingToken: "handoff-deadline-fence", LeaseTTL: time.Minute},
			}
			lifecycle := newAttemptLifecycle(attemptLifecycleDependencies{
				client: client, outbox: outbox, handoffs: handoffs,
				runtimes: workloadRuntimeSet{contract.JobKindProcess: test.runtime},
				clock:    systemClock{}, nodeID: "handoff-deadline-node", bootSessionID: "handoff-deadline-boot",
				renewalInterval: 10 * time.Second, finalizationTimeout: 25 * time.Millisecond,
				observer: newLifecycleObserver(systemClock{}),
			})
			if _, err := lifecycle.execute(t.Context(), claim, time.Now()); err != nil {
				t.Fatal(err)
			}
			if test.succeeded {
				if _, err := os.Stat(path); !os.IsNotExist(err) {
					t.Fatalf("successful deadline handoff still exists: %v", err)
				}
				return
			}
			marker, exists, err := readHandoffMarker(path)
			if err != nil || !exists {
				t.Fatalf("retained deadline handoff marker exists=%t err=%v", exists, err)
			}
			if want := finishedAt.Add(time.Hour); !marker.RetainUntil.Equal(want) {
				t.Fatalf("retained deadline handoff expires at %s, want %s", marker.RetainUntil, want)
			}
		})
	}
}

func TestLogFinalizationDeadlineStillFinalizesOCIManagedVolumes(t *testing.T) {
	handler := http.HandlerFunc(func(_ http.ResponseWriter, request *http.Request) {
		<-request.Context().Done()
	})
	client, stopServer := startEvidenceReplayServer(t, handler, time.Second)
	defer stopServer()
	defer client.Close()
	outbox, err := newEvidenceOutbox(t.TempDir(), "oci-deadline-node", 1024*1024, systemClock{}, 8, time.Hour, time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	defer outbox.Close()
	runtime := &deadlineManagedVolumeRuntime{}
	claim := l1.Claim{
		Job: l1.Job{JobID: "oci-deadline-job", Spec: contract.JobSpec{
			Kind: contract.JobKindOCI, Class: contract.JobClassOneShot,
			Labels: map[string]string{"run_id": "run-oci-deadline"},
			Execution: contract.ExecutionSpec{OCI: &contract.OCIExecutionSpec{
				Image: contract.OCIImageSpec{Reference: "example.invalid/deadline:v1"},
			}},
		}},
		Lease: l1.AttemptLease{AttemptID: "oci-deadline-attempt", FencingToken: "oci-deadline-fence", LeaseTTL: time.Minute},
	}
	lifecycle := newAttemptLifecycle(attemptLifecycleDependencies{
		client: client, outbox: outbox,
		runtimes: workloadRuntimeSet{contract.JobKindOCI: runtime},
		clock:    systemClock{}, nodeID: "oci-deadline-node", bootSessionID: "oci-deadline-boot",
		renewalInterval: 10 * time.Second, finalizationTimeout: 25 * time.Millisecond,
		observer: newLifecycleObserver(systemClock{}),
	})
	if _, err := lifecycle.execute(t.Context(), claim, time.Now()); err != nil {
		t.Fatal(err)
	}
	if len(runtime.finalizations) != 1 {
		t.Fatalf("managed-volume finalizations = %d, want 1", len(runtime.finalizations))
	}
	request := runtime.finalizations[0]
	if request.Authority.AttemptID != claim.Lease.AttemptID || len(request.Volumes) != 1 ||
		request.Volumes[0].Kind != workloadrunner.ManagedVolumeHandoff || request.Volumes[0].OwnerKey != "run-oci-deadline" {
		t.Fatalf("managed-volume finalization = %+v", request)
	}
}

type deadlineManagedVolumeRuntime struct {
	finalizations []workloadrunner.ManagedVolumeFinalizationRequest
}

func (*deadlineManagedVolumeRuntime) Preflight(_ context.Context, request workloadrunner.Request) (workloadrunner.Admission, workloadrunner.Result, error) {
	return workloadrunner.Admission{Request: request, Release: func() {}}, workloadrunner.Result{}, nil
}

func (*deadlineManagedVolumeRuntime) Run(ctx context.Context, request workloadrunner.Request, sink workloadrunner.OutputSink) (workloadrunner.Result, error) {
	if err := sink.WriteOutput(ctx, contract.LogEvent{AttemptID: request.Authority.AttemptID, Stream: contract.LogStdout, Bytes: []byte("tail")}); err != nil {
		return workloadrunner.Result{}, err
	}
	exitCode := 0
	return workloadrunner.Result{Outcome: contract.ProcessResult{ExitCode: &exitCode}}, nil
}

func (*deadlineManagedVolumeRuntime) ReapAndVerify(context.Context, workloadrunner.ReapRequest) (workloadrunner.ReapReceipt, error) {
	return workloadrunner.ReapReceipt{RuntimeQuiesced: true, Evidence: workloadrunner.ReapEvidenceAttempt}, nil
}

func (runtime *deadlineManagedVolumeRuntime) FinalizeManagedVolumes(_ context.Context, request workloadrunner.ManagedVolumeFinalizationRequest) error {
	runtime.finalizations = append(runtime.finalizations, request)
	return nil
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

func TestAttemptRenewalFailureAbandonsInFlightCompletionToRecovery(t *testing.T) {
	for _, test := range []struct {
		name string
		code contract.ErrorCode
	}{
		{name: "attempt mismatch", code: contract.ErrorAttemptMismatch},
		{name: "lease expired", code: contract.ErrorLeaseExpired},
	} {
		t.Run(test.name, func(t *testing.T) {
			firstCompletion := make(chan l1.CompletionRequest, 1)
			replayedCompletion := make(chan l1.CompletionRequest, 1)
			var mu sync.Mutex
			completionCalls := 0
			handler := http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
				switch {
				case strings.HasSuffix(request.URL.Path, "/complete"):
					var completion l1.CompletionRequest
					if err := json.NewDecoder(request.Body).Decode(&completion); err != nil {
						t.Error(err)
						return
					}
					mu.Lock()
					completionCalls++
					call := completionCalls
					mu.Unlock()
					if call == 1 {
						firstCompletion <- completion
						<-request.Context().Done()
						return
					}
					replayedCompletion <- completion
					_ = json.NewEncoder(w).Encode(l1.Job{})
				case strings.HasSuffix(request.URL.Path, "/lease"):
					select {
					case completion := <-firstCompletion:
						firstCompletion <- completion
					case <-time.After(2 * time.Second):
						t.Error("renewal arrived before the live completion request")
					}
					w.Header().Set("Content-Type", "application/json")
					w.WriteHeader(http.StatusConflict)
					_ = json.NewEncoder(w).Encode(contract.ErrorResponse{Error: contract.APIError{
						Code: test.code, Message: "renewal no longer owns the attempt",
					}})
				default:
					http.NotFound(w, request)
				}
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
			claim := spoolTestClaim("attempt-renewal-" + strings.ReplaceAll(test.name, " ", "-"))
			claim.Lease.LeaseTTL = time.Second
			lifecycle := newAttemptLifecycle(attemptLifecycleDependencies{
				client: client, runtimes: testRuntimeSet(runtimeFailureResultRunner{}), outbox: outbox,
				clock: systemClock{}, renewalInterval: time.Millisecond, completionRetry: time.Millisecond,
				observer: newLifecycleObserver(systemClock{}),
			})
			executeDone := make(chan error, 1)
			go func() {
				_, executeErr := lifecycle.execute(t.Context(), claim, time.Now())
				executeDone <- executeErr
			}()
			select {
			case err := <-executeDone:
				if err == nil {
					t.Fatal("renewal authority loss did not terminate the lifecycle")
				}
			case <-time.After(2 * time.Second):
				t.Fatal("lifecycle did not abandon the in-flight completion")
			}
			var live l1.CompletionRequest
			select {
			case live = <-firstCompletion:
			case <-time.After(2 * time.Second):
				t.Fatal("live completion did not reach L1")
			}
			select {
			case replayed := <-replayedCompletion:
				if !reflect.DeepEqual(replayed, live) {
					t.Fatalf("replayed completion = %+v, want exact live body %+v", replayed, live)
				}
			case <-time.After(2 * time.Second):
				receipt := outbox.spool.inspectCompletion(t.Context(), claim.Lease.AttemptID)
				t.Fatalf("abandoned completion was not reconciled; receipt=%+v", receipt)
			}
			waitCompletionReceiptState(t, outbox, claim.Lease.AttemptID, "delivered", 2*time.Second)
		})
	}
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

type runtimeFailureResultRunner struct{}

func (runtimeFailureResultRunner) Run(_ context.Context, request processrunner.Request, _ processrunner.OutputSink) (contract.ProcessResult, error) {
	if request.Started != nil {
		request.Started()
	}
	return contract.ProcessResult{RuntimeFailure: &contract.RuntimeFailure{
		Code: contract.RuntimeFailureUnavailable, Message: "helper generation lost",
	}}, nil
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

type boundedReplayL1Clock struct{ now time.Time }

func (clock *boundedReplayL1Clock) Now() time.Time { return clock.now }

func TestEvidenceRecoveryDrainsLiveLogBacklogBeforeCompletion(t *testing.T) {
	firstPassFinished := make(chan struct{})
	releaseFirstPass := make(chan struct{})
	var releaseFirstPassOnce sync.Once
	releaseFirstPassNow := func() { releaseFirstPassOnce.Do(func() { close(releaseFirstPass) }) }
	completionSeen := make(chan struct{}, 1)
	var finishOnce sync.Once
	var mu sync.Mutex
	logCalls := 0
	completionCalls := 0
	var logUploads []l1.AppendLogsRequest
	var callOrder []string
	l1Clock := &boundedReplayL1Clock{now: time.Date(2026, 9, 2, 12, 0, 0, 0, time.UTC)}
	store, err := l1.OpenStore(filepath.Join(t.TempDir(), "bounded-replay-l1.sqlite"), l1.StoreOptions{Clock: l1Clock, LeaseDuration: 10 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	identity := fabric.Identity{NodeID: "bounded-replay-agent"}
	registration := contract.NodeRegistration{
		NodeID: "bounded-replay-node", BootSessionID: "bounded-replay-boot",
		OS: "linux", Architecture: "amd64", AgentVersion: "test",
		Capabilities: map[string]bool{"kind:process": true},
	}
	if _, err := store.RegisterNode(t.Context(), identity, registration, l1.NodePolicy{MaxOneshotSlots: 1}, true); err != nil {
		t.Fatal(err)
	}
	workingDirectory := t.TempDir()
	job, _, err := store.CreateJob(t.Context(), contract.JobSpec{
		SchemaVersion: contract.SchemaVersionV1, DispatchKey: "bounded-log-replay",
		Kind: contract.JobKindProcess, Class: contract.JobClassOneShot,
		Execution: contract.ExecutionSpec{
			Executable: contract.ExecutableSpec{Path: "/bin/true"}, Argv: []string{"true"},
			WorkingDirectory: workingDirectory, HandoffDirectory: workingDirectory,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	claim, err := store.ClaimJob(t.Context(), identity.NodeID, registration.NodeID, registration.BootSessionID, contract.JobClassOneShot)
	if err != nil || claim == nil || claim.Job.JobID != job.JobID {
		t.Fatalf("bounded replay claim = %+v, err = %v", claim, err)
	}
	writeL1Error := func(w http.ResponseWriter, err error) {
		var protocolErr *l1.Error
		if !errors.As(err, &protocolErr) {
			t.Errorf("unexpected L1 error: %v", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		status := http.StatusConflict
		if protocolErr.Code == contract.ErrorInvalidRequest {
			status = http.StatusBadRequest
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_ = json.NewEncoder(w).Encode(contract.ErrorResponse{Error: contract.APIError{Code: protocolErr.Code, Message: protocolErr.Error()}})
	}
	handler := http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if strings.HasSuffix(request.URL.Path, "/logs") {
			var appendRequest l1.AppendLogsRequest
			if err := json.NewDecoder(request.Body).Decode(&appendRequest); err != nil {
				t.Error(err)
				return
			}
			mu.Lock()
			logCalls++
			logUploads = append(logUploads, appendRequest)
			callOrder = append(callOrder, "logs")
			mu.Unlock()
			response, appendErr := store.AppendLogs(request.Context(), identity.NodeID, job.JobID, claim.Lease.AttemptID, appendRequest)
			if appendErr != nil {
				writeL1Error(w, appendErr)
				return
			}
			_ = json.NewEncoder(w).Encode(response)
			return
		}
		var completion l1.CompletionRequest
		if err := json.NewDecoder(request.Body).Decode(&completion); err != nil {
			t.Error(err)
			return
		}
		mu.Lock()
		completionCalls++
		callOrder = append(callOrder, "completion")
		mu.Unlock()
		completionSeen <- struct{}{}
		completed, completionErr := store.CompleteAttempt(request.Context(), identity.NodeID, job.JobID, claim.Lease.AttemptID, completion)
		if completionErr != nil {
			writeL1Error(w, completionErr)
			return
		}
		_ = json.NewEncoder(w).Encode(completed)
	})
	client, stopServer := startEvidenceReplayServer(t, handler, time.Second)
	defer stopServer()
	defer client.Close()
	outbox, err := newEvidenceOutbox(t.TempDir(), "stable-node", 1<<20, systemClock{}, 32, time.Hour, time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	defer outbox.Close()
	defer releaseFirstPassNow()
	if err := outbox.ensureAttempt(t.Context(), *claim); err != nil {
		t.Fatal(err)
	}
	const backlogEvents = 300
	for sequence := uint64(0); sequence < backlogEvents; sequence++ {
		if err := outbox.spool.append(t.Context(), spoolTestEvent(claim.Lease.AttemptID, contract.LogStdout, sequence, "load")); err != nil {
			t.Fatal(err)
		}
	}
	exitCode := 0
	if err := outbox.storeCompletion(t.Context(), claim.Lease.AttemptID, l1.ProcessResult{ExitCode: &exitCode}, time.Now()); err != nil {
		t.Fatal(err)
	}
	outbox.recoveryAttemptFinished = func(attemptID string) {
		if attemptID == claim.Lease.AttemptID {
			finishOnce.Do(func() {
				close(firstPassFinished)
				<-releaseFirstPass
			})
		}
	}
	outbox.startRecovery(t.Context(), client, func(err error) { t.Errorf("recover durable evidence: %v", err) })
	select {
	case <-firstPassFinished:
	case <-time.After(2 * time.Second):
		t.Fatal("bounded recovery pass did not finish")
	}
	mu.Lock()
	firstPassLogCalls := logCalls
	firstPassCompletionCalls := completionCalls
	mu.Unlock()
	wantLogCalls := (backlogEvents + outbox.batchSize - 1) / outbox.batchSize
	if firstPassLogCalls != wantLogCalls || firstPassCompletionCalls != 1 {
		t.Fatalf("live recovery pass = %d log calls, %d completion calls; want %d and 1", firstPassLogCalls, firstPassCompletionCalls, wantLogCalls)
	}
	select {
	case <-completionSeen:
	default:
		t.Fatal("completion was not delivered at the bounded first-pass edge")
	}
	releaseFirstPassNow()
	deadline := time.Now().Add(2 * time.Second)
	for {
		mu.Lock()
		calls := logCalls
		mu.Unlock()
		if calls == wantLogCalls {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("remaining durable log batch was not continued on a later pass")
		}
		time.Sleep(time.Millisecond)
	}
	mu.Lock()
	defer mu.Unlock()
	if logCalls != wantLogCalls || completionCalls != 1 {
		t.Fatalf("complete recovery = %d log calls, %d completion calls; want %d and 1", logCalls, completionCalls, wantLogCalls)
	}
	if len(callOrder) == 0 || callOrder[len(callOrder)-1] != "completion" {
		t.Fatalf("live recovery order = %v, want completion last", callOrder)
	}
	finalUpload := logUploads[len(logUploads)-1]
	if len(finalUpload.Events) != backlogEvents%outbox.batchSize || finalUpload.Events[0].Gap != nil ||
		finalUpload.Events[0].Sequence != backlogEvents-uint64(len(finalUpload.Events)) ||
		finalUpload.Events[len(finalUpload.Events)-1].Sequence != backlogEvents-1 || string(finalUpload.Events[0].Bytes) != "load" {
		t.Fatalf("live recovery final upload = %#v, want raw event at sequence %d", finalUpload.Events, backlogEvents-1)
	}
	page, err := store.GetJobLogs(t.Context(), job.JobID, "", backlogEvents)
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Events) != backlogEvents {
		t.Fatalf("L1 retained %d live log events, want %d", len(page.Events), backlogEvents)
	}
	for _, event := range page.Events {
		if event.Gap != nil {
			t.Fatalf("live recovery stored unexpected gap at sequence %d: %+v", event.Sequence, event.Gap)
		}
	}
	receipt := outbox.spool.inspectCompletion(t.Context(), claim.Lease.AttemptID)
	if receipt.State != "delivered" || receipt.EventCount != 0 {
		t.Fatalf("live recovery receipt = %+v, want delivered with no retained events", receipt)
	}
}

func TestEvidenceRecoveryBoundsLostLogBacklogBeforeCompletion(t *testing.T) {
	firstPassFinished := make(chan struct{})
	releaseFirstPass := make(chan struct{})
	var releaseFirstPassOnce sync.Once
	releaseFirstPassNow := func() { releaseFirstPassOnce.Do(func() { close(releaseFirstPass) }) }
	var finishOnce sync.Once
	var mu sync.Mutex
	logCalls := 0
	completionCalls := 0
	var callOrder []string
	l1Clock := &boundedReplayL1Clock{now: time.Date(2026, 9, 2, 12, 0, 0, 0, time.UTC)}
	store, err := l1.OpenStore(filepath.Join(t.TempDir(), "bounded-lost-replay-l1.sqlite"), l1.StoreOptions{Clock: l1Clock, LeaseDuration: 10 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	identity := fabric.Identity{NodeID: "bounded-lost-replay-agent"}
	registration := contract.NodeRegistration{
		NodeID: "bounded-lost-replay-node", BootSessionID: "bounded-lost-replay-boot",
		OS: "linux", Architecture: "amd64", AgentVersion: "test",
		Capabilities: map[string]bool{"kind:process": true},
	}
	if _, err := store.RegisterNode(t.Context(), identity, registration, l1.NodePolicy{MaxOneshotSlots: 1}, true); err != nil {
		t.Fatal(err)
	}
	workingDirectory := t.TempDir()
	job, _, err := store.CreateJob(t.Context(), contract.JobSpec{
		SchemaVersion: contract.SchemaVersionV1, DispatchKey: "bounded-lost-log-replay",
		Kind: contract.JobKindProcess, Class: contract.JobClassOneShot,
		Execution: contract.ExecutionSpec{
			Executable: contract.ExecutableSpec{Path: "/bin/true"}, Argv: []string{"true"},
			WorkingDirectory: workingDirectory, HandoffDirectory: workingDirectory,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	claim, err := store.ClaimJob(t.Context(), identity.NodeID, registration.NodeID, registration.BootSessionID, contract.JobClassOneShot)
	if err != nil || claim == nil || claim.Job.JobID != job.JobID {
		t.Fatalf("bounded lost replay claim = %+v, err = %v", claim, err)
	}
	l1Clock.now = l1Clock.now.Add(11 * time.Second)
	if _, err := store.Reconcile(t.Context()); err != nil {
		t.Fatal(err)
	}
	writeL1Error := func(w http.ResponseWriter, err error) {
		var protocolErr *l1.Error
		if !errors.As(err, &protocolErr) {
			t.Errorf("unexpected L1 error: %v", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		status := http.StatusConflict
		if protocolErr.Code == contract.ErrorInvalidRequest {
			status = http.StatusBadRequest
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_ = json.NewEncoder(w).Encode(contract.ErrorResponse{Error: contract.APIError{Code: protocolErr.Code, Message: protocolErr.Error()}})
	}
	handler := http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if strings.HasSuffix(request.URL.Path, "/logs") {
			var appendRequest l1.AppendLogsRequest
			if err := json.NewDecoder(request.Body).Decode(&appendRequest); err != nil {
				t.Error(err)
				return
			}
			mu.Lock()
			logCalls++
			callOrder = append(callOrder, "logs")
			mu.Unlock()
			response, appendErr := store.AppendLogs(request.Context(), identity.NodeID, job.JobID, claim.Lease.AttemptID, appendRequest)
			if appendErr != nil {
				writeL1Error(w, appendErr)
				return
			}
			_ = json.NewEncoder(w).Encode(response)
			return
		}
		var completion l1.CompletionRequest
		if err := json.NewDecoder(request.Body).Decode(&completion); err != nil {
			t.Error(err)
			return
		}
		mu.Lock()
		completionCalls++
		callOrder = append(callOrder, "completion")
		mu.Unlock()
		completed, completionErr := store.CompleteAttempt(request.Context(), identity.NodeID, job.JobID, claim.Lease.AttemptID, completion)
		if completionErr != nil {
			writeL1Error(w, completionErr)
			return
		}
		_ = json.NewEncoder(w).Encode(completed)
	})
	client, stopServer := startEvidenceReplayServer(t, handler, time.Second)
	defer stopServer()
	defer client.Close()
	outbox, err := newEvidenceOutbox(t.TempDir(), "stable-node", 1<<20, systemClock{}, 32, time.Hour, time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	defer outbox.Close()
	defer releaseFirstPassNow()
	if err := outbox.ensureAttempt(t.Context(), *claim); err != nil {
		t.Fatal(err)
	}
	const backlogEvents = 300
	for sequence := uint64(0); sequence < backlogEvents; sequence++ {
		if err := outbox.spool.append(t.Context(), spoolTestEvent(claim.Lease.AttemptID, contract.LogStdout, sequence, "load")); err != nil {
			t.Fatal(err)
		}
	}
	exitCode := 0
	if err := outbox.storeCompletion(t.Context(), claim.Lease.AttemptID, l1.ProcessResult{ExitCode: &exitCode}, time.Now()); err != nil {
		t.Fatal(err)
	}
	outbox.recoveryAttemptFinished = func(attemptID string) {
		if attemptID == claim.Lease.AttemptID {
			finishOnce.Do(func() {
				close(firstPassFinished)
				<-releaseFirstPass
			})
		}
	}
	outbox.startRecovery(t.Context(), client, func(err error) { t.Errorf("recover durable evidence: %v", err) })
	select {
	case <-firstPassFinished:
	case <-time.After(2 * time.Second):
		t.Fatal("bounded lost recovery pass did not finish")
	}
	mu.Lock()
	firstPassLogCalls := logCalls
	firstPassCompletionCalls := completionCalls
	firstPassOrder := append([]string(nil), callOrder...)
	mu.Unlock()
	if firstPassLogCalls != maxEvidenceLogReplayBatchesPerPass || firstPassCompletionCalls != 1 ||
		len(firstPassOrder) != maxEvidenceLogReplayBatchesPerPass+1 || firstPassOrder[len(firstPassOrder)-1] != "completion" {
		t.Fatalf("lost first recovery pass = %d log calls, %d completion calls, order %v; want %d logs then completion",
			firstPassLogCalls, firstPassCompletionCalls, firstPassOrder, maxEvidenceLogReplayBatchesPerPass)
	}
	releaseFirstPassNow()
	wantLogCalls := (backlogEvents + outbox.batchSize - 1) / outbox.batchSize
	deadline := time.Now().Add(2 * time.Second)
	for {
		mu.Lock()
		calls := logCalls
		mu.Unlock()
		if calls == wantLogCalls {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("lost recovery did not continue all batches: got %d, want %d", calls, wantLogCalls)
		}
		time.Sleep(time.Millisecond)
	}
	deadline = time.Now().Add(2 * time.Second)
	for {
		receipt := outbox.spool.inspectCompletion(t.Context(), claim.Lease.AttemptID)
		if receipt.State == "delivered" && receipt.EventCount == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("lost recovery receipt = %+v, want delivered with drained row", receipt)
		}
		time.Sleep(time.Millisecond)
	}
	page, err := store.GetJobLogs(t.Context(), job.JobID, "", backlogEvents)
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Events) != backlogEvents {
		t.Fatalf("L1 retained %d lost-attempt log events, want %d", len(page.Events), backlogEvents)
	}
	for _, event := range page.Events {
		if event.Gap != nil {
			t.Fatalf("lost recovery stored unexpected gap at sequence %d: %+v", event.Sequence, event.Gap)
		}
	}
	attempts, err := store.ListJobAttempts(t.Context(), job.JobID)
	if err != nil {
		t.Fatal(err)
	}
	if len(attempts) != 1 || attempts[0].State != contract.AttemptLost || attempts[0].LateResult == nil ||
		attempts[0].LateResult.Kind != l1.LateResultObservation {
		t.Fatalf("lost completion evidence = %+v", attempts)
	}
}

func TestEvidenceRecoverySealsNonContiguousBacklogInsteadOfRetryingForever(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		var appendRequest l1.AppendLogsRequest
		if err := json.NewDecoder(request.Body).Decode(&appendRequest); err != nil {
			t.Error(err)
			return
		}
		if len(appendRequest.Events) == 0 {
			t.Error("empty recovery upload")
			return
		}
		if appendRequest.Events[0].Sequence >= 18 {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(contract.ErrorResponse{Error: contract.APIError{
				Code: contract.ErrorInvalidRequest, Message: "non-contiguous recovery replay",
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
	outbox, err := newEvidenceOutbox(t.TempDir(), "stable-node", 1<<20, systemClock{}, 2, time.Hour, time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	defer outbox.Close()
	claim := spoolTestClaim("attempt-non-contiguous-recovery")
	if err := outbox.ensureAttempt(t.Context(), claim); err != nil {
		t.Fatal(err)
	}
	for sequence := uint64(0); sequence < 16; sequence++ {
		if err := outbox.spool.append(t.Context(), spoolTestEvent(claim.Lease.AttemptID, contract.LogStdout, sequence, "load")); err != nil {
			t.Fatal(err)
		}
	}
	for _, sequence := range []uint64{18, 20} {
		if err := outbox.spool.append(t.Context(), spoolTestEvent(claim.Lease.AttemptID, contract.LogStdout, sequence, "corrupt")); err != nil {
			t.Fatal(err)
		}
	}
	reports := make(chan error, 1)
	outbox.startRecovery(t.Context(), client, func(err error) { reports <- err })
	select {
	case err := <-reports:
		if !strings.Contains(err.Error(), "durable evidence sealed incomplete") {
			t.Fatalf("non-contiguous recovery report = %v, want terminal incomplete-evidence seal", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("non-contiguous recovery backlog was not terminally sealed")
	}
	receipt := outbox.spool.inspectCompletion(t.Context(), claim.Lease.AttemptID)
	if receipt.State != "sealed_incomplete" || receipt.Incomplete.LostEventCount != 2 {
		t.Fatalf("non-contiguous recovery receipt = %+v", receipt)
	}
}

func TestEvidenceRecoveryRetriesScanErrorsWithInjectedClock(t *testing.T) {
	clock := newManualClock(time.Date(2026, 9, 2, 12, 0, 0, 0, time.UTC))
	retryInterval := 25 * time.Millisecond
	outbox, err := newEvidenceOutbox(t.TempDir(), "stable-node", 1024, clock, 1, time.Hour, retryInterval)
	if err != nil {
		t.Fatal(err)
	}
	defer outbox.Close()
	if err := outbox.spool.db.Close(); err != nil {
		t.Fatal(err)
	}
	reports := make(chan error, 2)
	outbox.startRecovery(t.Context(), nil, func(err error) { reports <- err })
	select {
	case <-reports:
	case <-time.After(2 * time.Second):
		t.Fatal("initial spool scan error was not reported")
	}
	clock.waitForDeadline(t, clock.Now().Add(retryInterval))
	clock.Advance(retryInterval)
	select {
	case <-reports:
	case <-time.After(2 * time.Second):
		t.Fatal("spool scan was not retried after injected-clock backoff")
	}
}

func TestEvidenceRecoveryDoesNotReplayLiveAttempt(t *testing.T) {
	liveRequest := make(chan string, 1)
	lateCompletion := make(chan struct{}, 1)
	handler := http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if strings.Contains(request.URL.Path, "attempt-live") {
			liveRequest <- request.URL.Path
			if strings.HasSuffix(request.URL.Path, "/logs") {
				_ = json.NewEncoder(w).Encode(l1.AppendLogsResponse{Acknowledged: map[contract.LogStream]uint64{contract.LogStdout: 0}})
				return
			}
			_ = json.NewEncoder(w).Encode(l1.Job{})
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
	live.Job.Spec.Kind = contract.JobKindProcess
	live.Job.Spec.Execution = contract.ExecutionSpec{
		Executable: contract.ExecutableSpec{Path: "ignored-by-fake-runner"},
		Argv:       []string{"ignored-by-fake-runner"}, WorkingDirectory: t.TempDir(),
	}
	live.Lease.LeaseTTL = time.Minute
	runner := &liveEvidenceRunner{started: make(chan struct{})}
	lifecycle := newAttemptLifecycle(attemptLifecycleDependencies{
		client: client, runtimes: testRuntimeSet(runner), outbox: outbox,
		clock: systemClock{}, renewalInterval: 10 * time.Second, completionRetry: time.Millisecond,
		observer: newLifecycleObserver(systemClock{}),
	})
	liveContext, cancelLive := context.WithCancel(t.Context())
	executeDone := make(chan error, 1)
	go func() {
		_, executeErr := lifecycle.execute(liveContext, live, time.Now())
		executeDone <- executeErr
	}()
	select {
	case <-runner.started:
	case err := <-executeDone:
		t.Fatalf("live lifecycle exited before emitting its log event: %v", err)
	case <-time.After(2 * time.Second):
		t.Fatal("live lifecycle did not durably emit its log event")
	}
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
	cancelLive()
	select {
	case <-executeDone:
	case <-time.After(2 * time.Second):
		t.Fatal("live lifecycle did not stop")
	}
}

type liveEvidenceRunner struct {
	started chan struct{}
}

func (runner *liveEvidenceRunner) Run(ctx context.Context, request processrunner.Request, sink processrunner.OutputSink) (contract.ProcessResult, error) {
	if request.Started != nil {
		request.Started()
	}
	if err := sink.WriteOutput(ctx, contract.LogEvent{
		AttemptID: request.AttemptID, Stream: contract.LogStdout, Sequence: 0,
		Timestamp: time.Now().UTC(), Bytes: []byte("owned"),
	}); err != nil {
		return contract.ProcessResult{}, err
	}
	close(runner.started)
	<-ctx.Done()
	return contract.ProcessResult{Signal: "terminated", TerminationCause: contract.TerminationCauseAgent}, ctx.Err()
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
	outbox.startRecovery(ctx, client, func(err error) { t.Errorf("recover durable evidence: %v", err) })
	select {
	case <-poisonRequestStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("poison replay did not start")
	}
	waitForSpoolHighWater(t, outbox.spool, "attempt-good", contract.LogStdout, 0)
	cancel()
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
	reports := make(chan error, 1)
	outbox.startRecovery(t.Context(), client, func(err error) { reports <- err })
	select {
	case err := <-reports:
		if !strings.Contains(err.Error(), "sealed incomplete") {
			t.Fatalf("recovery error = %v, want sealed-incomplete report", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for sealed-incomplete report")
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
	uploadSeen := make(chan struct{}, 2)
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
		uploadSeen <- struct{}{}
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
	outbox.startRecovery(t.Context(), client, func(err error) { t.Errorf("recover durable evidence: %v", err) })
	for range 2 {
		select {
		case <-uploadSeen:
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for rejected raw event and replacement gap")
		}
	}
	waitForSpoolHighWater(t, outbox.spool, claim.Lease.AttemptID, contract.LogStdout, 0)

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
