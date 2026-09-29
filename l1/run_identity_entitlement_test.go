package l1

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
)

// runLedgerOrigin creates a store fixture as the trusted run ledger does. A
// fixture that names a run in run_id or handoff_owner_run_id must: no other
// root submitter is entitled to (wefty #583).
var runLedgerOrigin = JobOrigin{OriginatingSubmitter: "run-ledger", SubmittedByRunLedger: true}

// runIdentityOCIOneShot is the shape whose handoff volume a run identity
// names: an image one-shot carrying the given labels.
func runIdentityOCIOneShot(dispatchKey string, labels map[string]string) contract.JobSpec {
	return contract.JobSpec{
		SchemaVersion: contract.SchemaVersionV1, DispatchKey: dispatchKey,
		Kind: contract.JobKindOCI, Class: contract.JobClassOneShot, Labels: labels,
		Execution: contract.ExecutionSpec{OCI: &contract.OCIExecutionSpec{
			Image: contract.OCIImageSpec{Reference: "ghcr.io/example/tool:latest"},
		}},
	}
}

func labelledProcessOneShot(dispatchKey string, tags []string, labels map[string]string) contract.JobSpec {
	spec := validJobSpec(dispatchKey, tags)
	spec.Labels = labels
	return spec
}

func storedJobCount(t *testing.T, h *integrationHarness, dispatchKey string) int {
	t.Helper()
	var count int
	if err := h.store.db.QueryRow(`SELECT COUNT(*) FROM jobs WHERE dispatch_key=?`, dispatchKey).Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}

func requireNotEntitled(t *testing.T, h *integrationHarness, status int, body []byte, dispatchKey string) {
	t.Helper()
	var response contract.ErrorResponse
	if err := json.Unmarshal(body, &response); err != nil {
		t.Fatalf("decode refusal %s: %v", body, err)
	}
	if status != http.StatusForbidden || response.Error.Code != contract.ErrorRunIdentityNotEntitled || response.Error.Retryable {
		t.Fatalf("submit status = %d body=%s, want 403 non-retryable %s", status, body, contract.ErrorRunIdentityNotEntitled)
	}
	if stored := storedJobCount(t, h, dispatchKey); stored != 0 {
		t.Fatalf("a refused submission stored %d jobs", stored)
	}
}

// TestOnlyTheRunLedgerMayNameARunOnARootSubmission is wefty #583. The node keys
// a one-shot's retained handoff by its run_id / handoff_owner_run_id and
// attributes the attempt's results to that run, so naming a run is a claim to
// speak for it. The ledger dispatches every run and may name any; any other
// root submitter naming one is refused and nothing is stored, and a submission
// naming no run is exactly what it was before.
func TestOnlyTheRunLedgerMayNameARunOnARootSubmission(t *testing.T) {
	h := runLedgerHarness(t, "run-ledger")
	ledger := h.client(fabric.Identity{NodeID: "run-ledger", Tags: []string{DefaultClientPrincipalTag}})
	operator := h.client(fabric.Identity{NodeID: "operator-laptop", Tags: []string{DefaultClientPrincipalTag}})

	for _, accepted := range []struct {
		name string
		spec contract.JobSpec
	}{
		{name: "OCI one-shot naming any run", spec: runIdentityOCIOneShot("ledger-oci", map[string]string{contract.LabelRunID: "run-any"})},
		{name: "OCI rerun naming the run it reuses", spec: runIdentityOCIOneShot("ledger-rerun", map[string]string{
			contract.LabelRunID: "run-rerun", contract.LabelHandoffOwnerRunID: "run-original",
		})},
		{name: "process one-shot naming a run", spec: labelledProcessOneShot("ledger-process", nil, map[string]string{contract.LabelRunID: "run-process"})},
	} {
		t.Run("ledger/"+accepted.name, func(t *testing.T) {
			if status, _, body := h.do(ledger, http.MethodPost, "/v1/jobs", accepted.spec); status != http.StatusCreated {
				t.Fatalf("ledger submit status = %d body=%s, want 201", status, body)
			}
		})
	}

	serviceDigest := testTopDigest
	for _, refused := range []struct {
		name string
		spec contract.JobSpec
	}{
		{name: "OCI one-shot naming another run", spec: runIdentityOCIOneShot("forged-oci", map[string]string{contract.LabelRunID: "run-any"})},
		{name: "OCI one-shot naming only a handoff owner", spec: runIdentityOCIOneShot("forged-owner", map[string]string{contract.LabelHandoffOwnerRunID: "run-original"})},
		{name: "process one-shot naming a run", spec: labelledProcessOneShot("forged-process", nil, map[string]string{contract.LabelRunID: "run-process"})},
		{name: "process one-shot naming a handoff owner", spec: labelledProcessOneShot("forged-process-owner", nil, map[string]string{contract.LabelHandoffOwnerRunID: "run-process"})},
		{name: "service naming a run", spec: contract.JobSpec{
			SchemaVersion: contract.SchemaVersionV1, DispatchKey: "forged-service",
			Kind: contract.JobKindOCI, Class: contract.JobClassService, Restart: contract.RestartAlways,
			Labels: map[string]string{contract.LabelRunID: "run-any"},
			Execution: contract.ExecutionSpec{OCI: &contract.OCIExecutionSpec{
				Image: contract.OCIImageSpec{Reference: "ghcr.io/example/tool:latest", Digest: &serviceDigest},
			}},
		}},
	} {
		t.Run("operator/refused/"+refused.name, func(t *testing.T) {
			status, _, body := h.do(operator, http.MethodPost, "/v1/jobs", refused.spec)
			requireNotEntitled(t, h, status, body, refused.spec.DispatchKey)
		})
	}

	// Naming no run is unchanged: a process one-shot is accepted, a blank
	// label names nothing, and an OCI one-shot is still refused by #578's rule
	// for what it lacks rather than for what it claims.
	if status, _, body := h.do(operator, http.MethodPost, "/v1/jobs", validJobSpec("operator-process", nil)); status != http.StatusCreated {
		t.Fatalf("unlabelled process one-shot status = %d body=%s, want 201", status, body)
	}
	blank := labelledProcessOneShot("operator-blank", nil, map[string]string{contract.LabelRunID: "  ", contract.LabelHandoffOwnerRunID: ""})
	if status, _, body := h.do(operator, http.MethodPost, "/v1/jobs", blank); status != http.StatusCreated {
		t.Fatalf("blank run identity status = %d body=%s, want 201", status, body)
	}
	status, _, body := h.do(operator, http.MethodPost, "/v1/jobs", runIdentityOCIOneShot("operator-ownerless-oci", nil))
	var response contract.ErrorResponse
	if err := json.Unmarshal(body, &response); err != nil {
		t.Fatalf("decode refusal %s: %v", body, err)
	}
	if status != http.StatusConflict || response.Error.Code != contract.ErrorRunIdentityRequired {
		t.Fatalf("unlabelled OCI one-shot status = %d body=%s, want 409 %s", status, body, contract.ErrorRunIdentityRequired)
	}
}

// TestAChildMayNameOnlyItsParentsRun is the attempt-credential half of #583.
// A child speaks for its parent's run: it may name that run, and a rerun's
// child may share the handoff its parent reuses, but a child can name no other
// run -- and a child of a job that names no run can name none.
func TestAChildMayNameOnlyItsParentsRun(t *testing.T) {
	h := runLedgerHarness(t, "run-ledger")
	ledger := h.client(fabric.Identity{NodeID: "run-ledger", Tags: []string{DefaultClientPrincipalTag}})
	operator := h.client(fabric.Identity{NodeID: "operator-laptop", Tags: []string{DefaultClientPrincipalTag}})
	agent := h.client(fabric.Identity{NodeID: "node-1", Tags: []string{DefaultAgentPrincipalTag}})
	node := h.register(agent, "node-1")

	parents := map[string]Claim{}
	for _, root := range []struct {
		name   string
		client *http.Client
		labels map[string]string
	}{
		{name: "run", client: ledger, labels: map[string]string{contract.LabelRunID: "run-parent"}},
		{name: "rerun", client: ledger, labels: map[string]string{contract.LabelRunID: "run-rerun", contract.LabelHandoffOwnerRunID: "run-original"}},
		{name: "runless", client: operator},
	} {
		spec := labelledProcessOneShot("parent-"+root.name, []string{"linux"}, root.labels)
		if status, _, body := h.do(root.client, http.MethodPost, "/v1/jobs", spec); status != http.StatusCreated {
			t.Fatalf("parent %s status = %d body=%s", root.name, status, body)
		}
		claim := claimOneShot(t, h, agent, node)
		if claim.Job.Spec.DispatchKey != spec.DispatchKey || claim.AttemptToken == "" {
			t.Fatalf("claimed %q (token %t), want parent %q with a credential", claim.Job.Spec.DispatchKey, claim.AttemptToken != "", spec.DispatchKey)
		}
		parents[root.name] = claim
	}

	for _, accepted := range []struct {
		name   string
		parent string
		spec   contract.JobSpec
	}{
		{name: "OCI child naming its parent's run", parent: "run",
			spec: runIdentityOCIOneShot("child-oci-parent-run", map[string]string{contract.LabelRunID: "run-parent"})},
		{name: "process child naming its parent's run", parent: "run",
			spec: labelledProcessOneShot("child-process-parent-run", nil, map[string]string{contract.LabelRunID: " run-parent "})},
		{name: "OCI child naming its parent's run as handoff owner", parent: "run",
			spec: runIdentityOCIOneShot("child-owner-parent-run", map[string]string{contract.LabelHandoffOwnerRunID: "run-parent"})},
		{name: "rerun child sharing the handoff its parent reuses", parent: "rerun",
			spec: runIdentityOCIOneShot("child-rerun-owner", map[string]string{contract.LabelRunID: "run-rerun", contract.LabelHandoffOwnerRunID: "run-original"})},
		{name: "child naming no run", parent: "runless", spec: validJobSpec("child-runless", nil)},
	} {
		t.Run("accepted/"+accepted.name, func(t *testing.T) {
			status, body := h.credentialRequest(agent, http.MethodPost, "/v1/jobs", parents[accepted.parent].AttemptToken, accepted.spec)
			if status != http.StatusCreated {
				t.Fatalf("child submit status = %d body=%s, want 201", status, body)
			}
		})
	}

	for _, refused := range []struct {
		name   string
		parent string
		spec   contract.JobSpec
	}{
		{name: "child naming another run", parent: "run",
			spec: runIdentityOCIOneShot("child-other-run", map[string]string{contract.LabelRunID: "run-other"})},
		{name: "child naming another run's handoff", parent: "run",
			spec: runIdentityOCIOneShot("child-other-owner", map[string]string{contract.LabelRunID: "run-parent", contract.LabelHandoffOwnerRunID: "run-other"})},
		{name: "rerun child attributing itself to the reused run", parent: "rerun",
			spec: runIdentityOCIOneShot("child-rerun-as-original", map[string]string{contract.LabelRunID: "run-original"})},
		{name: "child of a runless parent naming a run", parent: "runless",
			spec: labelledProcessOneShot("child-runless-forged", nil, map[string]string{contract.LabelRunID: "run-parent"})},
	} {
		t.Run("refused/"+refused.name, func(t *testing.T) {
			status, body := h.credentialRequest(agent, http.MethodPost, "/v1/jobs", parents[refused.parent].AttemptToken, refused.spec)
			requireNotEntitled(t, h, status, body, refused.spec.DispatchKey)
		})
	}
}

// TestALabelledJobStoredBeforeTheEntitlementCheckStillReplays keeps #583 from
// breaking dispatch-key replay, as #580 did for #578: a job some submitter
// labelled with a run before L1 checked is still stored, and an identical
// replay returns it; only a genuinely new job of that shape is refused.
func TestALabelledJobStoredBeforeTheEntitlementCheckStillReplays(t *testing.T) {
	h := runLedgerHarness(t, "run-ledger")
	operator := h.client(fabric.Identity{NodeID: "operator-laptop", Tags: []string{DefaultClientPrincipalTag}})
	spec := runIdentityOCIOneShot("labelled-before-upgrade", map[string]string{contract.LabelRunID: "run-someone-elses"})

	stored := spec
	stored.RoutingTags = NormalizeTags(stored.RoutingTags)
	if err := contract.ValidateJobSpec(&stored); err != nil {
		t.Fatal(err)
	}
	specJSON, err := json.Marshal(stored)
	if err != nil {
		t.Fatal(err)
	}
	hash := sha256.Sum256(specJSON)
	now := h.clock.Now().UnixNano()
	const storedJobID = "job_labelled_before_upgrade"
	if _, err := h.store.db.Exec(`
INSERT INTO jobs(job_id, dispatch_key, request_hash, spec_json, state,
                 parent_job_id, parent_attempt_id, originating_submitter, submitted_by_run_ledger, spawn_depth, created_ns, updated_ns)
VALUES(?, ?, ?, ?, ?, NULL, NULL, 'operator-laptop', 0, 0, ?, ?)`, storedJobID, stored.DispatchKey, hex.EncodeToString(hash[:]), specJSON,
		contract.JobQueued, now, now); err != nil {
		t.Fatal(err)
	}
	if _, err := h.store.db.Exec(`INSERT INTO job_log_jsonl(job_id, jsonl) VALUES(?, ?)`, storedJobID, []byte{}); err != nil {
		t.Fatal(err)
	}
	for _, capability := range RequiredCapabilities(stored) {
		if _, err := h.store.db.Exec(`INSERT INTO job_required_capabilities(job_id, capability) VALUES(?, ?)`, storedJobID, capability); err != nil {
			t.Fatal(err)
		}
	}

	status, headers, body := h.do(operator, http.MethodPost, "/v1/jobs", spec)
	if status != http.StatusOK || headers.Get("Idempotent-Replay") != "true" {
		t.Fatalf("identical replay status = %d replay header %q body=%s, want 200 replay", status, headers.Get("Idempotent-Replay"), body)
	}
	var replayed Job
	if err := json.Unmarshal(body, &replayed); err != nil {
		t.Fatal(err)
	}
	if replayed.JobID != storedJobID {
		t.Fatalf("identical replay returned job %q, want the stored %q", replayed.JobID, storedJobID)
	}

	fresh := spec
	fresh.DispatchKey = "labelled-after-upgrade"
	status, _, body = h.do(operator, http.MethodPost, "/v1/jobs", fresh)
	requireNotEntitled(t, h, status, body, fresh.DispatchKey)
	if !strings.Contains(string(body), contract.LabelRunID) {
		t.Fatalf("refusal %s does not name the label it refused", body)
	}
}
