package l1

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
)

func assertCanceledJob(t *testing.T, status int, body []byte) Job {
	t.Helper()
	if status != http.StatusOK {
		t.Fatalf("cancel status=%d body=%s", status, body)
	}
	job := decodeJob(t, body)
	// Read the public wire field, so this assertion also runs on the old type.
	var wire struct {
		Outcome string `json:"outcome"`
	}
	if err := json.Unmarshal(body, &wire); err != nil {
		t.Fatal(err)
	}
	if job.State != contract.JobFailed || wire.Outcome != "canceled" || job.CurrentAttemptID != "" || job.NodeID != "" {
		t.Fatalf("cancel projection=%s", body)
	}
	return job
}

func TestCancelQueuedOneShotContract(t *testing.T) {
	h, client, _, node := credentialHarness(t)
	spec := validJobSpec("cancel-queued", []string{"linux"})
	spec.Execution.SensitiveEnv = map[string]string{"SECRET": "never-retain"}
	spec.Labels = map[string]string{"run_params_json": `{"secret":"value"}`}
	status, _, body := h.do(client, http.MethodPost, "/v1/jobs", spec)
	if status != http.StatusCreated {
		t.Fatalf("submit=%d %s", status, body)
	}
	job := decodeJob(t, body)
	status, _, body = h.do(client, http.MethodPost, "/v1/jobs/"+job.JobID+"/cancel", nil)
	canceled := assertCanceledJob(t, status, body)
	if len(canceled.Attempts) != 0 || canceled.SecretsScrubbedAt == nil {
		t.Fatalf("canceled=%#v", canceled)
	}
	var stored []byte
	if err := h.store.db.QueryRow("SELECT spec_json FROM jobs WHERE job_id=?", job.JobID).Scan(&stored); err != nil {
		t.Fatal(err)
	}
	var record contract.JobSpec
	if err := json.Unmarshal(stored, &record); err != nil {
		t.Fatal(err)
	}
	if len(record.Execution.SensitiveEnv) != 0 || record.Labels["run_params_json"] != "" {
		t.Fatalf("secrets retained: %s", stored)
	}
	h.clock.Advance(time.Second)
	status, _, body = h.do(client, http.MethodPost, "/v1/jobs/"+job.JobID+"/cancel", nil)
	replay := assertCanceledJob(t, status, body)
	if !reflect.DeepEqual(canceled, replay) {
		t.Fatalf("retry mutated job: before=%#v after=%#v", canceled, replay)
	}
	status, _, body = h.do(client, http.MethodGet, "/v1/jobs/"+job.JobID, nil)
	assertCanceledJob(t, status, body)
	status, _, body = h.do(client, http.MethodPost, "/v1/jobs", spec)
	assertCanceledJob(t, status, body)
	claim, err := h.store.ClaimJob(t.Context(), "node-1", node.NodeID, node.BootSessionID, contract.JobClassOneShot)
	if err != nil || claim != nil {
		t.Fatalf("claim after cancel=%#v err=%v", claim, err)
	}
}

func TestCancelAuthorizationRoutes(t *testing.T) {
	h, client, agent, node := credentialHarness(t)
	parent := h.submit(client, "cancel-parent", []string{"linux"})
	claim := claimClass(t, h, agent, node, contract.JobClassOneShot)
	status, body := h.credentialRequest(agent, http.MethodPost, "/v1/jobs", claim.AttemptToken, validJobSpec("cancel-child", []string{"linux"}))
	if status != http.StatusCreated {
		t.Fatalf("child=%d %s", status, body)
	}
	child := decodeJob(t, body)
	unrelated := h.submit(client, "cancel-unrelated", []string{"linux"})
	for _, id := range []string{parent.JobID, unrelated.JobID, "missing"} {
		status, body = h.credentialRequest(agent, http.MethodPost, "/v1/jobs/"+id+"/cancel", claim.AttemptToken, nil)
		assertAPIError(t, status, body, http.StatusNotFound, contract.ErrorNotFound)
	}
	other := h.client(fabric.Identity{NodeID: "other", Tags: []string{DefaultClientPrincipalTag}})
	for _, id := range []string{child.JobID, "missing"} {
		status, _, body = h.do(other, http.MethodPost, "/v1/jobs/"+id+"/cancel", nil)
		assertAPIError(t, status, body, http.StatusNotFound, contract.ErrorNotFound)
	}
	wrongNode := h.client(fabric.Identity{NodeID: "node-other", Tags: []string{DefaultAgentPrincipalTag}})
	status, body = h.credentialRequest(wrongNode, http.MethodPost, "/v1/jobs/"+child.JobID+"/cancel", claim.AttemptToken, nil)
	assertAPIError(t, status, body, http.StatusForbidden, contract.ErrorForbidden)
	status, body = h.credentialRequest(agent, http.MethodPost, "/v1/jobs/"+child.JobID+"/cancel", claim.AttemptToken, nil)
	assertCanceledJob(t, status, body)
	// Canceling a parent never cascades. A current admin can reach cancel with
	// an untagged person device, but cannot borrow the ordinary job read route.
	adminID := fabric.Identity{NodeID: "admin-device", UserID: "person-admin", DeviceID: "device-admin"}
	admin := h.client(adminID)
	challenge, err := h.store.InitiateAdminBootstrap(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	status, _, body = h.do(admin, http.MethodPost, "/v1/admin-bootstrap", BootstrapAdminRequest{Nonce: challenge.Nonce})
	if status != http.StatusCreated {
		t.Fatalf("bootstrap=%d %s", status, body)
	}
	person := h.client(fabric.Identity{NodeID: "other-device", UserID: "person-other", DeviceID: "device-other"})
	status, _, body = h.do(person, http.MethodPost, "/v1/jobs/"+unrelated.JobID+"/cancel", nil)
	assertAPIError(t, status, body, http.StatusNotFound, contract.ErrorNotFound)
	status, _, body = h.do(admin, http.MethodGet, "/v1/jobs/"+unrelated.JobID, nil)
	assertAPIError(t, status, body, http.StatusForbidden, contract.ErrorPrincipalForbidden)
	status, _, body = h.do(admin, http.MethodPost, "/v1/jobs/"+unrelated.JobID+"/cancel", nil)
	assertCanceledJob(t, status, body)
	// The stale credential is refused, even on a target already terminal.
	h.clock.Advance(time.Minute)
	status, body = h.credentialRequest(agent, http.MethodPost, "/v1/jobs/"+child.JobID+"/cancel", claim.AttemptToken, nil)
	assertAPIError(t, status, body, http.StatusUnauthorized, contract.ErrorUnauthorized)
}

func TestCancelRefusalsAndTerminalReplay(t *testing.T) {
	h, client, agent, node := credentialHarness(t)
	service := submitRestartService(t, h, client, "cancel-service", []string{"linux"}, nil)
	before, err := h.store.GetJob(t.Context(), service.JobID)
	if err != nil {
		t.Fatal(err)
	}
	status, _, body := h.do(client, http.MethodPost, "/v1/jobs/"+service.JobID+"/cancel", nil)
	assertAPIError(t, status, body, http.StatusConflict, contract.ErrorCode("cancel_service"))
	var refusal contract.ErrorResponse
	if err := json.Unmarshal(body, &refusal); err != nil {
		t.Fatal(err)
	}
	if refusal.Error.Retryable || refusal.Error.Details["desired_state_path"] != "/v1/jobs/"+service.JobID+"/desired-state?class=service" || refusal.Error.Details["remove_path"] != "/v1/jobs/"+service.JobID+"/remove?class=service" {
		t.Fatalf("refusal=%s", body)
	}
	after, err := h.store.GetJob(t.Context(), service.JobID)
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatalf("service changed=%#v err=%v", after, err)
	}
	job := h.submit(client, "cancel-live", []string{"linux"})
	claim := claimClass(t, h, agent, node, contract.JobClassOneShot)
	zero := 0
	if _, err := h.store.CompleteAttempt(t.Context(), "node-1", job.JobID, claim.Lease.AttemptID, CompletionRequest{FencingToken: claim.Lease.FencingToken, IdempotencyKey: "finish", Result: ProcessResult{ExitCode: &zero}}); err != nil {
		t.Fatal(err)
	}
	status, _, body = h.do(client, http.MethodPost, "/v1/jobs/"+job.JobID+"/cancel", nil)
	if status != http.StatusOK {
		t.Fatalf("terminal cancel=%d %s", status, body)
	}
	terminal := decodeJob(t, body)
	if terminal.State != contract.JobSucceeded || len(terminal.Attempts) != 1 || terminal.Attempts[0].Result == nil || *terminal.Attempts[0].Result.ExitCode != 0 {
		t.Fatalf("terminal overwritten=%s", body)
	}
}

func TestCancelRequeuedOCIPreservesAttemptEvidence(t *testing.T) {
	h := newIntegrationHarness(t, map[string][]string{"node-1": {}})
	registerOCIFixtureNode(t, h)
	client := h.client(fabric.Identity{NodeID: runLedgerOrigin.OriginatingSubmitter, Tags: []string{DefaultClientPrincipalTag}})
	spec := contract.JobSpec{SchemaVersion: contract.SchemaVersionV1, DispatchKey: "cancel-oci", Kind: contract.JobKindOCI, Class: contract.JobClassOneShot, RuntimeHandler: "io.containerd.runc.v2", Labels: map[string]string{contract.LabelRunID: "run-cancel-oci"}, Execution: contract.ExecutionSpec{OCI: &contract.OCIExecutionSpec{Image: contract.OCIImageSpec{Reference: "ghcr.io/example/tool:latest"}}}}
	status, _, body := h.do(client, http.MethodPost, "/v1/jobs", spec)
	if status != http.StatusCreated {
		t.Fatalf("oci submit=%d %s", status, body)
	}
	job := decodeJob(t, body)
	claim := claimOCIFixture(t, h, contract.JobClassOneShot)
	if _, err := h.store.ObserveAttemptImage(t.Context(), "agent", job.JobID, claim.Lease.AttemptID, testImageObservation(claim.Lease.FencingToken)); err != nil {
		t.Fatal(err)
	}
	completion := CompletionRequest{FencingToken: claim.Lease.FencingToken, IdempotencyKey: "cancel-oci-prestart", Result: ProcessResult{SpawnError: &contract.SpawnFailure{Code: contract.SpawnFailureRuntimeUnavailable, Message: "engine unavailable"}}}
	requeued, err := h.store.CompleteAttempt(t.Context(), "agent", job.JobID, claim.Lease.AttemptID, completion)
	if err != nil || requeued.State != contract.JobQueued {
		t.Fatalf("requeue=%#v err=%v", requeued, err)
	}
	attempts, err := h.store.ListJobAttempts(t.Context(), job.JobID)
	if err != nil {
		t.Fatal(err)
	}
	status, _, body = h.do(client, http.MethodPost, "/v1/jobs/"+job.JobID+"/cancel", nil)
	canceled := assertCanceledJob(t, status, body)
	if !reflect.DeepEqual(attempts, canceled.Attempts) {
		t.Fatalf("attempt evidence changed before=%#v after=%#v", attempts, canceled.Attempts)
	}
	replay, err := h.store.CompleteAttempt(t.Context(), "agent", job.JobID, claim.Lease.AttemptID, completion)
	if err != nil || replay.State != contract.JobFailed {
		t.Fatalf("completion replay revived job=%#v err=%v", replay, err)
	}
	h.clock.Advance(time.Second)

	next, err := h.store.ClaimJob(t.Context(), "agent", "node-1", "boot-node-1", contract.JobClassOneShot)
	if err != nil || next != nil {
		t.Fatalf("reclaimed canceled OCI=%#v err=%v", next, err)
	}
}

func TestCancelClaimRace(t *testing.T) {
	h, client, _, node := credentialHarness(t)
	for i := 0; i < 12; i++ {
		job := h.submit(client, fmt.Sprintf("cancel-race-%d", i), []string{"linux"})
		var claim *Claim
		var claimErr error
		var status int
		var body []byte
		start := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			<-start
			claim, claimErr = h.store.ClaimJob(context.Background(), "node-1", node.NodeID, node.BootSessionID, contract.JobClassOneShot)
		}()
		go func() {
			defer wg.Done()
			<-start
			status, _, body = h.do(client, http.MethodPost, "/v1/jobs/"+job.JobID+"/cancel", nil)
		}()
		close(start)
		wg.Wait()
		if claimErr != nil {
			t.Fatal(claimErr)
		}
		if status != http.StatusOK {
			t.Fatalf("cancel=%d %s", status, body)
		}
		if claim == nil {
			assertCanceledJob(t, status, body)
		} else {
			pending := decodeJob(t, body)
			if pending.Outcome != "canceled" || pending.State != contract.JobClaimed {
				t.Fatalf("claimed cancellation=%s", body)
			}
			zero := 0
			done, err := h.store.CompleteAttempt(t.Context(), "node-1", job.JobID, claim.Lease.AttemptID, CompletionRequest{FencingToken: claim.Lease.FencingToken, IdempotencyKey: "race-finish", Result: ProcessResult{ExitCode: &zero}})
			if err != nil || done.State != contract.JobFailed || done.Outcome != "canceled" {
				t.Fatalf("claim race=%+v %v", done, err)
			}
		}
	}
}

// Resolve succeeds using the first clock read. The cancellation transaction
// sees lease expiry on its own clock read, so middleware approval cannot be
// cached as authority for the write.
func TestCancelRevalidatesParentInsideTransaction(t *testing.T) {
	h := newIntegrationHarnessWithReconcileInterval(t, StoreOptions{}, map[string]NodePolicy{"node-1": DefaultNodePolicy()}, true, time.Hour)
	client := h.client(fabric.Identity{NodeID: "submitter", Tags: []string{DefaultClientPrincipalTag}})
	agent := h.client(fabric.Identity{NodeID: "node-1", Tags: []string{DefaultAgentPrincipalTag}})
	node := h.register(agent, "node-1")
	h.submit(client, "cancel-parent-expiring", nil)
	claim := claimClass(t, h, agent, node, contract.JobClassOneShot)
	status, body := h.credentialRequest(agent, http.MethodPost, "/v1/jobs", claim.AttemptToken, validJobSpec("cancel-expiring-child", nil))
	if status != http.StatusCreated {
		t.Fatalf("child=%d %s", status, body)
	}
	child := decodeJob(t, body)
	now := h.clock.Now()
	var mu sync.Mutex
	reads := 0
	h.store.clock = ClockFunc(func() time.Time {
		mu.Lock()
		defer mu.Unlock()
		reads++
		if reads == 1 {
			return now
		}
		return now.Add(time.Minute)
	})
	status, body = h.credentialRequest(agent, http.MethodPost, "/v1/jobs/"+child.JobID+"/cancel", claim.AttemptToken, nil)
	assertAPIError(t, status, body, http.StatusUnauthorized, contract.ErrorUnauthorized)
	// Read directly rather than inviting the lease-expiry reconciler to run.
	var state string
	if err := h.store.db.QueryRow("SELECT state FROM jobs WHERE job_id=?", child.JobID).Scan(&state); err != nil {
		t.Fatal(err)
	}
	if state != "queued" {
		t.Fatalf("expired parent canceled child: %s", state)
	}
}

func TestCancelRevokedAdminIsNotCached(t *testing.T) {
	h, client, _, _ := credentialHarness(t)
	job := h.submit(client, "cancel-revoked-admin", []string{"linux"})
	admin := h.client(fabric.Identity{NodeID: "admin", UserID: "person-admin", DeviceID: "device-admin"})
	challenge, err := h.store.InitiateAdminBootstrap(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	status, _, body := h.do(admin, http.MethodPost, "/v1/admin-bootstrap", BootstrapAdminRequest{Nonce: challenge.Nonce})
	if status != http.StatusCreated {
		t.Fatalf("bootstrap=%d %s", status, body)
	}
	var policy AdminPolicy
	if err := json.Unmarshal(body, &policy); err != nil {
		t.Fatal(err)
	}
	status, _, body = h.do(admin, http.MethodPut, "/v1/admin-policy/admins/person-second", AdminPolicyMutationRequest{PolicyRevision: policy.Revision})
	if status != http.StatusOK {
		t.Fatalf("add admin=%d %s", status, body)
	}
	if err := json.Unmarshal(body, &policy); err != nil {
		t.Fatal(err)
	}
	status, _, body = h.do(admin, http.MethodDelete, "/v1/admin-policy/admins/person-admin", AdminPolicyMutationRequest{PolicyRevision: policy.Revision})
	if status != http.StatusOK {
		t.Fatalf("remove admin=%d %s", status, body)
	}
	status, _, body = h.do(admin, http.MethodPost, "/v1/jobs/"+job.JobID+"/cancel", nil)
	assertAPIError(t, status, body, http.StatusNotFound, contract.ErrorNotFound)
	status, _, body = h.do(client, http.MethodGet, "/v1/jobs/"+job.JobID, nil)
	if status != http.StatusOK || decodeJob(t, body).State != contract.JobQueued {
		t.Fatalf("revoked admin mutated=%d %s", status, body)
	}
}

func TestCancelDoesNotApplyReadClassSelector(t *testing.T) {
	h, client, _, _ := credentialHarness(t)
	job := h.submit(client, "cancel-class-selector", []string{"linux"})
	// Cancel determines the target class in its own transaction. A read-route
	// selector must not turn an already-committed cancellation into a 404.
	status, _, body := h.do(client, http.MethodPost, "/v1/jobs/"+job.JobID+"/cancel?class=service", nil)
	assertCanceledJob(t, status, body)
}
