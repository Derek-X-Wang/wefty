package agent

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/fabric/plain"
	"github.com/Derek-X-Wang/wefty/l1"
)

// #553, end to end against a real L1: L1 records a completion but the agent
// sees an ambiguous 500 (the #548 shape) and keeps its spooled copy; the node
// then re-registers, which advances its authority generation. The replay of
// that same completion used to be refused node_session_replaced forever,
// stranding the copy on disk. L1 now answers it as an already-recorded replay,
// and recovery marks it delivered and retires the row: no further requests,
// no further log lines.
func TestEvidenceRecoveryRetiresCompletionL1AlreadyRecordedAfterReregistration(t *testing.T) {
	l1Clock := &boundedReplayL1Clock{now: time.Date(2026, 9, 23, 3, 13, 0, 0, time.UTC)}
	store, err := l1.OpenStore(filepath.Join(t.TempDir(), "already-recorded-l1.sqlite"), l1.StoreOptions{Clock: l1Clock, LeaseDuration: 30 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	network := plain.NewNetwork()
	serverFabric := network.NewFabric(fabric.Identity{NodeID: "control-plane"})
	l1Server, err := l1.NewServer(serverFabric, store, l1.ServerConfig{NodePolicies: map[string]l1.NodePolicy{"stable-node": l1.DefaultNodePolicy("linux")}})
	if err != nil {
		t.Fatal(err)
	}
	identity := fabric.Identity{NodeID: "fabric-node", Tags: []string{l1.DefaultAgentPrincipalTag}}
	register := func(bootSessionID string) {
		t.Helper()
		if _, err := store.RegisterNode(t.Context(), identity, contract.NodeRegistration{
			NodeID: "stable-node", BootSessionID: bootSessionID, OS: "linux", Architecture: "amd64", AgentVersion: "test",
			Capabilities: map[string]bool{"kind:process": true},
		}, l1.NodePolicy{MaxOneshotSlots: 1}, true); err != nil {
			t.Fatal(err)
		}
	}
	register("boot-before")
	workingDirectory := t.TempDir()
	job, _, err := store.CreateJob(t.Context(), contract.JobSpec{
		SchemaVersion: contract.SchemaVersionV1, DispatchKey: "already-recorded-553",
		Kind: contract.JobKindProcess, Class: contract.JobClassOneShot,
		Execution: contract.ExecutionSpec{
			Executable: contract.ExecutableSpec{Path: "/bin/false"}, Argv: []string{"false"},
			WorkingDirectory: workingDirectory, HandoffDirectory: workingDirectory,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	claim, err := store.ClaimJob(t.Context(), identity.NodeID, "stable-node", "boot-before", contract.JobClassOneShot)
	if err != nil || claim == nil || claim.Job.JobID != job.JobID {
		t.Fatalf("claim = %+v, err = %v", claim, err)
	}
	attemptID := claim.Lease.AttemptID
	const barrierAttempt = "attempt-barrier"

	// L1 serves every completion for real; the first answer is swapped for
	// an ambiguous 500 after L1 has committed it.
	var mu sync.Mutex
	var completionStatus []int
	var replayHeader []string
	handler := http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if strings.Contains(request.URL.Path, "/attempts/"+barrierAttempt+"/") {
			// The pass barrier is not an L1 attempt; accept it uncounted.
			_ = json.NewEncoder(w).Encode(l1.Job{})
			return
		}
		if !strings.HasSuffix(request.URL.Path, "/complete") {
			l1Server.Handler().ServeHTTP(w, request)
			return
		}
		recorder := httptest.NewRecorder()
		l1Server.Handler().ServeHTTP(recorder, request)
		mu.Lock()
		completionStatus = append(completionStatus, recorder.Code)
		replayHeader = append(replayHeader, recorder.Header().Get("Idempotent-Replay"))
		first := len(completionStatus) == 1
		mu.Unlock()
		if first {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte(`{"error":{"code":"internal","message":"ambiguous answer","retryable":true}}`))
			return
		}
		for key, values := range recorder.Header() {
			w.Header()[key] = values
		}
		w.WriteHeader(recorder.Code)
		_, _ = w.Write(recorder.Body.Bytes())
	})
	listener, err := serverFabric.Listen("tcp", "wefty://control-plane")
	if err != nil {
		t.Fatal(err)
	}
	httpServer := &http.Server{Handler: handler}
	served := make(chan error, 1)
	go func() { served <- httpServer.Serve(listener) }()
	defer func() {
		_ = httpServer.Close()
		if err := <-served; err != nil && !errors.Is(err, http.ErrServerClosed) {
			t.Errorf("serve already-recorded L1: %v", err)
		}
	}()
	client, err := newClient(network.NewFabric(identity), "wefty://control-plane", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	calls := func() []int {
		mu.Lock()
		defer mu.Unlock()
		return append([]int(nil), completionStatus...)
	}

	clock := newManualClock(time.Date(2026, 9, 23, 3, 13, 0, 0, time.UTC))
	retryInterval := 100 * time.Millisecond
	outbox, err := newEvidenceOutbox(t.TempDir(), "stable-node", 1<<20, clock, 8, time.Hour, retryInterval)
	if err != nil {
		t.Fatal(err)
	}
	defer outbox.Close()
	if err := outbox.ensureAttempt(t.Context(), *claim); err != nil {
		t.Fatal(err)
	}
	exitCode := 1
	if err := outbox.storeCompletion(t.Context(), attemptID, l1.ProcessResult{ExitCode: &exitCode}, time.Now()); err != nil {
		t.Fatal(err)
	}
	reports := make(chan error, 64)
	outbox.startRecovery(t.Context(), client, func(err error) { reports <- err })

	// L1 recorded it; the agent saw only the 500 and kept the copy.
	receiveReport(t, reports, attemptID, "ambiguous answer")
	if got := calls(); len(got) != 1 || got[0] != http.StatusOK {
		t.Fatalf("first completion reached L1 as %v, want one accepted request", got)
	}
	recorded, err := store.GetJob(t.Context(), job.JobID)
	if err != nil {
		t.Fatal(err)
	}
	if recorded.State != contract.JobFailed {
		t.Fatalf("L1 job state = %q, want the recorded failure", recorded.State)
	}
	waitForCompletionState(t, outbox, attemptID, "durable_completion")

	// The node re-registers before the retry.
	register("boot-after")
	clock.waitForDeadline(t, clock.Now().Add(retryInterval))
	clock.Advance(retryInterval)
	waitForCompletionState(t, outbox, attemptID, "delivered")
	attempts, err := outbox.spool.pendingAttempts(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	for _, pending := range attempts {
		if pending.attemptID == attemptID {
			t.Fatalf("attempt %s is still pending replay after L1 answered already-recorded", attemptID)
		}
	}
	mu.Lock()
	if len(completionStatus) != 2 || completionStatus[1] != http.StatusOK || replayHeader[1] != "true" {
		t.Fatalf("completion answers = %v (Idempotent-Replay %q), want an already-recorded replay second", completionStatus, replayHeader)
	}
	mu.Unlock()

	// Retired: an hour of re-check time asks L1 nothing more and logs nothing.
	for range 6 {
		clock.Advance(10 * time.Minute)
	}
	deliverBarrier(t, outbox, barrierAttempt)
	if got := calls(); len(got) != 2 {
		t.Fatalf("L1 was asked %d times after the replay was delivered, want 2 in total", len(got))
	}
	select {
	case err := <-reports:
		t.Fatalf("unexpected report after delivery: %v", err)
	default:
	}
	unchanged, err := store.GetJob(t.Context(), job.JobID)
	if err != nil {
		t.Fatal(err)
	}
	if unchanged.State != recorded.State || !unchanged.UpdatedAt.Equal(recorded.UpdatedAt) {
		t.Fatalf("already-recorded replay changed the job: %+v -> %+v", recorded, unchanged)
	}
}
