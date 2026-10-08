package l3

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/l1"
)

// Bootstrap a person on L1's existing public HTTP path. The tagged L3
// identity is retained only to prove that admin membership adds no L3 authority.
func cancelAdmin(t *testing.T, h *integrationHarness) (*http.Client, *http.Client) {
	t.Helper()
	identity := fabric.Identity{NodeID: "admin-device", UserID: "admin", DeviceID: "device"}
	person := h.client(identity, DefaultL1Address)
	identity.Tags = []string{DefaultCallerPrincipalTag}
	caller := h.client(identity, DefaultL3Address)
	challenge, err := h.l1Store.InitiateAdminBootstrap(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	status, _, body := h.do(person, http.MethodPost, "/v1/admin-bootstrap", l1.BootstrapAdminRequest{Nonce: challenge.Nonce}, nil)
	if status != http.StatusCreated {
		t.Fatalf("bootstrap=%d %s", status, body)
	}
	return caller, person
}

func TestPersonAdminCancelsRunJobAtL1HTTP(t *testing.T) {
	for _, computer := range []bool{false, true} {
		t.Run(fmt.Sprint("computer=", computer), func(t *testing.T) {
			h := newIntegrationHarnessWithL1Options(t, l1.StoreOptions{}, true)
			_, person := cancelAdmin(t, h)
			ctx := context.Background()
			runID := ""
			if computer {
				proof := ComputerTokenScopeProof{ComputerID: "computer-root", ComputerAttemptID: "attempt", ComputerStorageGeneration: 1, SubmitIntentRevision: 1, HostNodeID: "host", HostStableNodeID: "stable", HostBootSessionID: "boot", SubmitMaxInflight: 10}
				grant, err := h.l3Store.MintComputerToken(ctx, proof)
				if err != nil {
					t.Fatal(err)
				}
				h.l3Server.computerGrants = &controlledComputerGrantVerifier{proof: proof}
				host := h.client(fabric.Identity{NodeID: proof.HostNodeID, Kind: fabric.IdentityKindMachine}, DefaultL3Address)
				status, _, body := h.do(host, http.MethodPost, "/v1/runs", inlineRunRequest("exit 0\n"), http.Header{"Authorization": []string{"Bearer " + grant.Token}, "Idempotency-Key": []string{"admin-computer"}})
				if status != http.StatusCreated {
					t.Fatalf("Computer submit=%d %s", status, body)
				}
				var accepted RunAccepted
				if err := json.Unmarshal(body, &accepted); err != nil {
					t.Fatal(err)
				}
				runID = accepted.RunID
				run, err := h.l3Store.GetRun(ctx, runID)
				if err != nil || run.Trigger.ComputerID != proof.ComputerID || run.ParentRunID != "" {
					t.Fatalf("not a Computer root: %+v %v", run, err)
				}
			} else {
				runID = h.submit(inlineRunRequest("exit 0\n"), "admin-person").RunID
			}
			before, err := h.l3Store.GetRun(ctx, runID)
			if err != nil || before.L1JobID != "" {
				t.Fatalf("undispatched run has a job: %+v %v", before, err)
			}
			r, err := NewReconciler(h.l3Store, h.l1Client, ReconcilerConfig{})
			if err != nil {
				t.Fatal(err)
			}
			if err := r.ReconcileOnce(ctx); err != nil {
				t.Fatal(err)
			}
			before, err = h.l3Store.GetRun(ctx, runID)
			if err != nil || before.L1JobID == "" {
				t.Fatalf("dispatched run has no job: %+v %v", before, err)
			}
			status, _, body := h.do(person, http.MethodPost, "/v1/jobs/"+before.L1JobID+"/cancel", nil, nil)
			if status != http.StatusOK {
				t.Fatalf("person admin L1 cancel=%d %s", status, body)
			}
			var job l1.Job
			if err := json.Unmarshal(body, &job); err != nil || job.State != contract.JobFailed || job.Outcome != contract.JobOutcomeCanceled {
				t.Fatalf("L1 cancel response=%s error=%v", body, err)
			}
			if err := r.ReconcileOnce(ctx); err != nil {
				t.Fatal(err)
			}
			status, _, body = h.do(h.caller, http.MethodGet, "/v1/runs/"+runID, nil, nil)
			var settled contract.RunRecord
			if err := json.Unmarshal(body, &settled); err != nil || status != http.StatusOK || settled.Status != contract.RunFailed || settled.FailureReason != "the L1 job was canceled" || settled.L1JobID != before.L1JobID || settled.CancelStatus != "" {
				t.Fatalf("L3 projection=%d %s error=%v", status, body, err)
			}
		})
	}
}

func TestPersonAdminHasNoL3CancelAuthority(t *testing.T) {
	h := newIntegrationHarnessWithL1Options(t, l1.StoreOptions{}, true)
	caller, _ := cancelAdmin(t, h)
	run := h.submit(inlineRunRequest("exit 0\n"), "admin-l3-forbidden")
	status, _, body := h.do(caller, http.MethodPost, "/v1/runs/"+run.RunID+"/cancel", nil, nil)
	assertAPIError(t, status, body, http.StatusForbidden, contract.ErrorForbidden)
	var count int
	if err := h.l3Store.db.QueryRow(`SELECT COUNT(*) FROM run_cancellations WHERE run_id=?`, run.RunID).Scan(&count); err != nil || count != 0 {
		t.Fatalf("forbidden request recorded intent=%d %v", count, err)
	}
}

func TestCancelRound2ConflictingTerminalAcknowledgement(t *testing.T) {
	s, _, _ := recoveryStore(t)
	ctx := context.Background()
	run, _, err := s.CreateRun(ctx, CreateRunInput{Actor: "test", IdempotencyKey: "conflicting", Request: inlineRunRequest("exit 0\n")})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.beginDispatch(ctx, run.RunID); err != nil {
		t.Fatal(err)
	}
	failRunAfterDispatchAttempt(t, s, run.RunID)
	recordOutboxOnlyAcknowledgement(t, s, run.RunID, "recorded-Y")
	before := snapshotTerminalRun(t, s, run.RunID)
	_ = s.completeDispatch(ctx, run.RunID, "lookup-X")
	record, err := s.GetRun(ctx, run.RunID)
	if err != nil || record.L1JobID != "" {
		t.Fatalf("conflicting acknowledgement linked: %+v %v", record, err)
	}
	if after := snapshotTerminalRun(t, s, run.RunID); before != after {
		t.Fatalf("terminal outcome changed")
	}
}

type round2LinkedCanceler struct {
	*reviewCancelClient
	store *Store
	runID string
	jobID string
}

func (c *round2LinkedCanceler) CancelJob(ctx context.Context, id string) (l1.Job, error) {
	record, err := c.store.GetRun(ctx, c.runID)
	if err != nil || id != c.jobID || record.L1JobID != id {
		return l1.Job{}, fmt.Errorf("cancel before link: id=%s record=%+v error=%v", id, record, err)
	}
	return c.reviewCancelClient.CancelJob(ctx, id)
}
func (c *round2LinkedCanceler) LookupJobByDispatchKey(_ context.Context, key string) (l1.Job, error) {
	return l1.Job{}, &DispatchNotFoundError{DispatchKey: key}
}

func TestCancelRound2OutboxOnlyNotFound(t *testing.T) {
	h := newIntegrationHarness(t)
	ctx := context.Background()
	run := h.submit(inlineRunRequest("exit 0\n"), "legacy-no-lookup")
	crashing := &loseSubmitResponseClient{JobClient: h.l1Client}
	r, _ := NewReconciler(h.l3Store, crashing, ReconcilerConfig{})
	if err := r.ReconcileOnce(ctx); err == nil {
		t.Fatal("expected lost response")
	}
	failRunAfterDispatchAttempt(t, h.l3Store, run.RunID)
	recordOutboxOnlyAcknowledgement(t, h.l3Store, run.RunID, crashing.submitted.JobID)
	if _, err := h.l3Store.db.Exec(`INSERT INTO run_cancellations(run_id, requested_ns) VALUES(?, ?)`, run.RunID, h.l3Store.clock.Now().UnixNano()); err != nil {
		t.Fatal(err)
	}
	client := &round2LinkedCanceler{reviewCancelClient: &reviewCancelClient{L1Client: h.l1Client}, store: h.l3Store, runID: run.RunID, jobID: crashing.submitted.JobID}
	r, _ = NewReconciler(h.l3Store, client, ReconcilerConfig{})
	if err := r.ReconcileOnce(ctx); err != nil {
		t.Fatal(err)
	}
	if client.calls != 1 {
		t.Fatalf("outbox-only cancellation deferred: calls=%d", client.calls)
	}
	job, err := h.l1Client.GetJob(ctx, crashing.submitted.JobID)
	if err != nil || job.Outcome != contract.JobOutcomeCanceled {
		t.Fatalf("legacy job=%+v %v", job, err)
	}
}

func TestCancelRound2Ledger401HTTP(t *testing.T) {
	for _, linked := range []bool{false, true} {
		t.Run(fmt.Sprint("linked=", linked), func(t *testing.T) {
			h := newIntegrationHarness(t)
			ctx := context.Background()
			run := h.submit(inlineRunRequest("exit 0\n"), "ledger-401")
			if linked {
				r, _ := NewReconciler(h.l3Store, h.l1Client, ReconcilerConfig{})
				if err := r.ReconcileOnce(ctx); err != nil {
					t.Fatal(err)
				}
			} else if _, err := h.l3Store.beginDispatch(ctx, run.RunID); err != nil {
				t.Fatal(err)
			}
			h.l3Server.jobs = &L1Client{operationTimeout: time.Second, client: &http.Client{Transport: recoveryRoundTripper(func(*http.Request) (*http.Response, error) {
				return &http.Response{StatusCode: 401, Body: io.NopCloser(strings.NewReader(`{"error":{"code":"unauthorized","message":"ledger unknown","retryable":false}}`)), Header: make(http.Header)}, nil
			})}}
			var serverLog bytes.Buffer
			previous := log.Writer()
			log.SetOutput(&serverLog)
			t.Cleanup(func() { log.SetOutput(previous) })
			status, _, body := h.do(h.caller, http.MethodPost, "/v1/runs/"+run.RunID+"/cancel", nil, nil)
			if !strings.Contains(serverLog.String(), "ledger unknown") || !strings.Contains(serverLog.String(), run.RunID) {
				t.Fatalf("missing server-side cause: %s", serverLog.String())
			}
			if strings.Contains(string(body), "ledger unknown") {
				t.Fatalf("internal cause leaked: %s", body)
			}
			assertAPIError(t, status, body, http.StatusServiceUnavailable, contract.ErrorUnavailable)
			var response contract.ErrorResponse
			_ = json.Unmarshal(body, &response)
			if !response.Error.Retryable {
				t.Fatalf("not retryable: %s", body)
			}
			var completed sql.NullInt64
			if err := h.l3Store.db.QueryRow(`SELECT completed_ns FROM run_cancellations WHERE run_id=?`, run.RunID).Scan(&completed); err != nil || completed.Valid {
				t.Fatalf("401 settled intent: %v %v", completed, err)
			}

		})
	}
}

type round2RunningCanceler struct{ *fixedJobClient }

func (c round2RunningCanceler) CancelJob(ctx context.Context, id string) (l1.Job, error) {
	return c.GetJob(ctx, id)
}

func TestCancelRound2SuccessfulDeliveryClearsFailure(t *testing.T) {
	s, _, clock := recoveryStore(t)
	ctx := context.Background()
	run, _ := dispatchedRecoveryRun(t, s, "running-success")
	if err := s.requestRunCancellation(ctx, run.RunID, "test"); err != nil {
		t.Fatal(err)
	}
	if err := s.deferRunCancellation(ctx, run.RunID, errors.New("earlier outage")); err != nil {
		t.Fatal(err)
	}
	clock.now = clock.now.Add(unrecordedDispatchRetryBase)
	client := round2RunningCanceler{&fixedJobClient{jobs: map[string]l1.Job{run.JobID: {JobID: run.JobID, State: contract.JobRunning, Outcome: contract.JobOutcomeCanceled}}}}
	r, _ := NewReconciler(s, client, ReconcilerConfig{})
	pending, err := s.pendingRunCancellations(ctx, run.RunID)
	if err != nil || len(pending) != 1 {
		t.Fatalf("pending=%+v %v", pending, err)
	}
	if err := r.cancelRunJob(ctx, pending[0]); err != nil {
		t.Fatal(err)
	}
	var failures int
	var last sql.NullString
	var completed sql.NullInt64
	if err := s.db.QueryRow(`SELECT failures,last_error,completed_ns FROM run_cancellations WHERE run_id=?`, run.RunID).Scan(&failures, &last, &completed); err != nil {
		t.Fatal(err)
	}
	if failures != 0 || last.Valid || completed.Valid {
		t.Fatalf("successful running cancel retained failure: failures=%d last=%v completed=%v", failures, last, completed)
	}
	record, err := s.GetRun(ctx, run.RunID)
	if err != nil || record.CancelReason != "" {
		t.Fatalf("stale visible error: %+v %v", record, err)
	}
}

func TestCompleteDispatchRejectsEmptyJobID(t *testing.T) {
	for _, terminal := range []bool{false, true} {
		t.Run(fmt.Sprint("terminal=", terminal), func(t *testing.T) {
			s, _, _ := recoveryStore(t)
			ctx := context.Background()
			run, _, err := s.CreateRun(ctx, CreateRunInput{Actor: "test", IdempotencyKey: "empty-job", Request: inlineRunRequest("exit 0\n")})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := s.beginDispatch(ctx, run.RunID); err != nil {
				t.Fatal(err)
			}
			if terminal {
				failRunAfterDispatchAttempt(t, s, run.RunID)
			}
			snapshot := func() string {
				var value string
				err := s.db.QueryRow(`SELECT json_object('state', r.status, 'job', r.l1_job_id, 'updated', r.updated_ns, 'outbox_job', o.job_id, 'dispatched', o.dispatched_ns, 'delivery', o.token_delivery) FROM runs r JOIN dispatch_outbox o USING(run_id) WHERE r.run_id=?`, run.RunID).Scan(&value)
				if err != nil {
					t.Fatal(err)
				}
				return value
			}
			before := snapshot()
			if err := s.completeDispatch(ctx, run.RunID, ""); err == nil {
				t.Fatal("empty job ID was accepted")
			}
			if after := snapshot(); after != before {
				t.Fatal("empty acknowledgement mutated durable state")
			}
		})
	}
}
