//go:build darwin || linux

package agent

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/l1"
)

func retirementReviewJob(t *testing.T, store *l1.Store, key string, restartable bool, sensitive map[string]string) (l1.Job, string, string) {
	t.Helper()
	dir := t.TempDir()
	ready, release := filepath.Join(dir, "ready"), filepath.Join(dir, "release")
	t.Cleanup(func() { _ = os.WriteFile(release, nil, 0600) })
	job, _, err := store.CreateJobAs(t.Context(), contract.JobSpec{SchemaVersion: contract.SchemaVersionV1, DispatchKey: key, Kind: contract.JobKindProcess, Class: contract.JobClassOneShot, Restartable: restartable, RoutingTags: []string{"linux"}, Execution: contract.ExecutionSpec{Executable: contract.ExecutableSpec{Path: "/bin/sh"}, Argv: []string{"sh", "-c", `printf tail; touch "$1"; while [ ! -e "$2" ]; do sleep 0.1; done; exit 17`, "sh", ready, release}, WorkingDirectory: dir, HandoffDirectory: dir, SensitiveEnv: sensitive}}, l1.JobOrigin{OriginatingSubmitter: "review-submitter"})
	if err != nil {
		t.Fatal(err)
	}
	return job, ready, release
}

func reviewWait(t *testing.T, name string, wait <-chan struct{}) {
	t.Helper()
	select {
	case <-wait:
	case <-time.After(15 * time.Second):
		t.Fatal(name)
	}
}

func reviewRetirementExit(t *testing.T, a *Agent, done <-chan error) {
	t.Helper()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(12 * time.Second):
		t.Fatalf("empty retiree did not close: %+v", a.Status())
	}
}

func TestRetirementWaitsForAsynchronousDurableAppend(t *testing.T) {
	lateStarted, releaseLate := make(chan struct{}), make(chan struct{})
	var once, releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(releaseLate) }) }
	defer unblock()
	var completions atomic.Int32
	a, store, done, _ := newRetirementFixtureWithHook(t, func(a *Agent, _ *l1.Store) {
		a.finalizationTimeout = 50 * time.Millisecond
		a.outbox.spool.appendCheckpoint = func(ctx context.Context) {
			if _, bounded := ctx.Deadline(); bounded {
				<-ctx.Done()
				return
			}
			once.Do(func() { close(lateStarted) })
			<-releaseLate
		}
		base := a.session.client.httpClient.Transport
		a.session.client.httpClient.Transport = captureRoundTrip(func(r *http.Request) (*http.Response, error) {
			if strings.HasSuffix(r.URL.Path, "/complete") {
				completions.Add(1)
			}
			return base.RoundTrip(r)
		})
	})
	job, ready, release := retirementReviewJob(t, store, "review-late-append", false, map[string]string{contract.EnvRunToken: "tail-secret"})
	retirementWait(t, "payload ready", func() bool { _, err := os.Stat(ready); return err == nil })
	promoteRetirementFixture(t, store, "node", "retirement-agent")
	if _, err := a.Retire(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(release, nil, 0600); err != nil {
		t.Fatal(err)
	}
	reviewWait(t, "unbounded durable append was never reached", lateStarted)
	before, _ := store.ListJobAttempts(t.Context(), job.JobID)
	time.Sleep(2200 * time.Millisecond)
	after, _ := store.ListJobAttempts(t.Context(), job.JobID)
	if completions.Load() != 0 {
		t.Fatal("completion overtook the blocked asynchronous durable append")
	}
	if len(after) != 1 || len(before) != 1 || !after[0].LeaseExpiresAt.After(before[0].LeaseExpiresAt) {
		t.Fatal("renewal stopped while joining late durable append")
	}
	select {
	case err := <-done:
		t.Fatalf("retirement closed before late append: %v", err)
	default:
	}
	unblock()
	reviewRetirementExit(t, a, done)
	page, err := store.GetJobLogs(t.Context(), job.JobID, "", l1.MaxLogPageLimit)
	if err != nil || len(page.Events) != 1 || string(page.Events[0].Bytes) != "tail" {
		t.Fatalf("late output=%+v %v", page, err)
	}
}

func blockReviewLogs(a *Agent, blocked *atomic.Bool) {
	base := a.session.client.httpClient.Transport
	a.session.client.httpClient.Transport = captureRoundTrip(func(r *http.Request) (*http.Response, error) {
		if strings.HasSuffix(r.URL.Path, "/logs") && blocked.Load() {
			return &http.Response{StatusCode: http.StatusServiceUnavailable, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(`{"error":{"code":"internal","message":"injected log outage","retryable":true}}`)), Request: r}, nil
		}
		return base.RoundTrip(r)
	})
	a.finalizationTimeout = 50 * time.Millisecond
}

func TestRetirementCancellationKeepsEvidencePastSettlementDeadline(t *testing.T) {
	clock := newManualClock(time.Now())
	var blocked atomic.Bool
	blocked.Store(true)
	a, store, done, _ := newRetirementFixtureWithStoreClock(t, clock, func(a *Agent, _ *l1.Store) { blockReviewLogs(a, &blocked) })
	job, ready, _ := retirementReviewJob(t, store, "review-cancel-evidence", false, nil)
	retirementWait(t, "payload ready", func() bool { _, err := os.Stat(ready); return err == nil })
	promoteRetirementFixture(t, store, "node", "retirement-agent")
	if _, err := a.Retire(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err := store.CancelJob(t.Context(), job.JobID, l1.JobCancelCaller{Submitter: "review-submitter"}); err != nil {
		t.Fatal(err)
	}
	retirementWait(t, "canceled runtime joined", func() bool { return a.Status().OneShot.Occupied == 0 })
	attempts, _ := store.ListJobAttempts(t.Context(), job.JobID)
	if len(attempts) != 1 {
		t.Fatalf("attempts=%+v", attempts)
	}
	attempt := attempts[0].AttemptID
	retirementWait(t, "durable canceled evidence", func() bool {
		return a.outbox.spool.inspectCompletion(t.Context(), attempt).State == "durable_completion"
	})
	clock.Advance(l1.CancelSettlementTimeout + time.Second)
	if _, err := store.Reconcile(t.Context()); err != nil {
		t.Fatal(err)
	}
	readyToClose, err := store.NodeGenerationClosureReady(t.Context(), "retirement-agent", "node", "generation")
	if err != nil || !readyToClose {
		t.Fatalf("L1 settlement did not end Lease: %v %v", readyToClose, err)
	}
	select {
	case err := <-done:
		t.Fatalf("retiree discarded pending canceled evidence after settlement: %v", err)
	case <-time.After(250 * time.Millisecond):
	}
	blocked.Store(false)
	a.outbox.scheduleRecovery()
	reviewRetirementExit(t, a, done)
	receipt := a.outbox.spool.inspectCompletion(t.Context(), attempt)
	if receipt.EventCount != 0 || receipt.State != "delivered" {
		t.Fatalf("evidence was not delivered: %+v", receipt)
	}
}

func TestRetirementRestartDirectiveClosesAfterDelivery(t *testing.T) {
	clock := newManualClock(time.Now())
	a, store, done, _ := newRetirementFixtureWithStoreClock(t, clock, nil)
	job, ready, _ := retirementReviewJob(t, store, "review-restart-close", true, nil)
	retirementWait(t, "payload ready", func() bool { _, err := os.Stat(ready); return err == nil })
	primary := promoteRetirementFixture(t, store, "node", "retirement-agent")
	if _, err := a.Retire(t.Context()); err != nil {
		t.Fatal(err)
	}
	node, err := store.GetNode(t.Context(), "node")
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = store.InterruptRestartableAttempt(t.Context(), "node", primary.ResidentAttempts[0].AttemptID, l1.InterruptRestartableAttemptRequest{JobID: job.JobID, BootSessionID: "boot", IntentRevision: node.IntentRevision, IdempotencyKey: "review-restart", Reason: l1.InterruptionUpgrade})
	if err != nil {
		t.Fatal(err)
	}
	retirementWait(t, "restart completion accepted", func() bool {
		attempts, _ := store.ListJobAttempts(t.Context(), job.JobID)
		return len(attempts) == 1 && attempts[0].InterruptionReason == l1.InterruptionUpgrade
	})
	reviewRetirementExit(t, a, done)
}

func TestRetirementLeaseLossClosesAfterEvidenceRecovery(t *testing.T) {
	clock := newManualClock(time.Now())
	var blocked atomic.Bool
	blocked.Store(true)
	a, store, done, _ := newRetirementFixtureWithStoreClock(t, clock, func(a *Agent, _ *l1.Store) { blockReviewLogs(a, &blocked) })
	job, ready, _ := retirementReviewJob(t, store, "review-lease-loss-close", true, nil)
	retirementWait(t, "payload ready", func() bool { _, err := os.Stat(ready); return err == nil })
	promoteRetirementFixture(t, store, "node", "retirement-agent")
	if _, err := a.Retire(t.Context()); err != nil {
		t.Fatal(err)
	}
	clock.Advance(61 * time.Second)
	if _, err := store.Reconcile(t.Context()); err != nil {
		t.Fatal(err)
	}
	retirementWait(t, "expired runtime joined", func() bool { return a.Status().OneShot.Occupied == 0 })
	select {
	case err := <-done:
		t.Fatalf("retirement closed with pending lost-attempt evidence: %v", err)
	default:
	}
	blocked.Store(false)
	a.outbox.scheduleRecovery()
	reviewRetirementExit(t, a, done)
	attempts, err := store.ListJobAttempts(t.Context(), job.JobID)
	if err != nil || len(attempts) != 1 || attempts[0].State != contract.AttemptLost {
		t.Fatalf("lost Attempt verdict changed: %+v %v", attempts, err)
	}
	receipt := a.outbox.spool.inspectCompletion(t.Context(), attempts[0].AttemptID)
	if receipt.EventCount != 0 || receipt.State != "delivered" {
		t.Fatalf("late evidence=%+v", receipt)
	}
}

func TestRetirementDeliveredProofSurvivesOutboxReceiptRetention(t *testing.T) {
	a, store, done, _ := newRetirementFixture(t)
	job, ready, release := retirementReviewJob(t, store, "review-receipt-retention", false, nil)
	retirementWait(t, "payload ready", func() bool { _, err := os.Stat(ready); return err == nil })
	if err := os.WriteFile(release, nil, 0600); err != nil {
		t.Fatal(err)
	}
	retirementWait(t, "natural completion joined", func() bool { return a.Status().OneShot.Occupied == 0 })
	attempts, err := store.ListJobAttempts(t.Context(), job.JobID)
	if err != nil || len(attempts) != 1 {
		t.Fatalf("attempts=%+v %v", attempts, err)
	}
	receipt := a.outbox.spool.inspectCompletion(t.Context(), attempts[0].AttemptID)
	if receipt.State != "delivered" {
		t.Fatalf("completion was not delivered: %+v", receipt)
	}
	// Exercise the bounded receipt-pruning edge without running 1024 payloads.
	if _, err := a.outbox.spool.db.ExecContext(t.Context(), "DELETE FROM spool_completion_receipts WHERE attempt_id=?", attempts[0].AttemptID); err != nil {
		t.Fatal(err)
	}
	promoteRetirementFixture(t, store, "node", "retirement-agent")
	if _, err := a.Retire(t.Context()); err != nil {
		t.Fatal(err)
	}
	reviewRetirementExit(t, a, done)
}

func TestGenerationDrainRefusalRequiresVerifiedDemotion(t *testing.T) {
	for _, tt := range []struct {
		name string
		code contract.ErrorCode
		role l1.NodeGenerationRole
		boot string
	}{
		{"still primary", contract.ErrorNodeSessionReplaced, l1.NodeGenerationPrimary, "boot"},
		{"wrong boot", contract.ErrorNodeSessionReplaced, l1.NodeGenerationRetiring, "other-boot"},
		{"other refusal", contract.ErrorInvalidRequest, l1.NodeGenerationRetiring, "boot"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var reads atomic.Int32
			client, stop := startEvidenceReplayServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if strings.HasSuffix(r.URL.Path, "/drain") {
					w.WriteHeader(http.StatusConflict)
					_ = json.NewEncoder(w).Encode(contract.ErrorResponse{Error: contract.APIError{Code: tt.code, Message: "injected refusal"}})
					return
				}
				reads.Add(1)
				_ = json.NewEncoder(w).Encode(l1.NodeGenerationInventory{Generations: []l1.NodeGeneration{{AgentGenerationID: "generation", BootSessionID: tt.boot, Role: tt.role, SelectedProtocol: 1, PromotionOperationID: "promotion"}}})
			}), time.Second)
			defer stop()
			defer client.Close()
			clock := newManualClock(mailboxTestClockOrigin)
			session := newAgentSession(client, contract.NodeRegistration{NodeID: "node", BootSessionID: "boot", AgentGenerationID: "generation"}, nil, time.Second, time.Second, clock, newLifecycleObserver(clock), nil, 1, 0)
			if _, err := session.drain(t.Context()); protocolErrorCode(err) != tt.code {
				t.Fatalf("ordinary refusal was swallowed: %v", err)
			}
			if session.isRetiring() {
				t.Fatal("unverified authority was treated as retirement")
			}
			if tt.code != contract.ErrorNodeSessionReplaced && reads.Load() != 0 {
				t.Fatal("unrelated refusal triggered demotion reconciliation")
			}
		})
	}
}

func TestRetirementPublicationRecoveryRequiresExactAttempt(t *testing.T) {
	outbox, err := newEvidenceOutbox(t.TempDir(), "node", 1<<20, systemClock{}, 8, time.Hour, time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	defer outbox.Close()
	claim := spoolTestClaim("publication-attempt")
	claim.Job.Spec.Labels = map[string]string{"run_id": "run-publication"}
	if err := outbox.ensureAttempt(t.Context(), claim); err != nil {
		t.Fatal(err)
	}
	zero := 0
	if err := outbox.storeCompletion(t.Context(), claim.Lease.AttemptID, l1.ProcessResult{ExitCode: &zero}, time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := outbox.completionDelivered(t.Context(), claim.Lease.AttemptID); err != nil {
		t.Fatal(err)
	}
	harness := newRetentionHarness(t, time.Hour)
	lifecycle := newAttemptLifecycle(attemptLifecycleDependencies{outbox: outbox, handoffs: harness.manager, nodeID: "node", clock: systemClock{}})
	for _, tt := range []struct {
		attempt string
		mailbox bool
		want    bool
	}{
		{"another-attempt", true, false}, {claim.Lease.AttemptID, false, false}, {claim.Lease.AttemptID, true, true},
	} {
		if err := harness.manager.recordUpload("run-publication", "node", tt.attempt, attemptResult{skip: contract.ResultUploadSkipAbsent}, tt.mailbox); err != nil {
			t.Fatal(err)
		}
		settled, err := lifecycle.retirementEvidenceSettled(t.Context(), claim)
		if err != nil || settled != tt.want {
			t.Fatalf("publication attempt=%s mailbox=%v: settled=%v err=%v", tt.attempt, tt.mailbox, settled, err)
		}
	}
}
