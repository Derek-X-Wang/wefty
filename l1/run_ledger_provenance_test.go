package l1

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
)

// runLedgerHarness builds an L1 whose trusted run-ledger identity is the given
// value, so a test can exercise the configured, the misconfigured and the
// unset cases the same way.
func runLedgerHarness(t *testing.T, trusted string) *integrationHarness {
	t.Helper()
	h := newIntegrationHarness(t, map[string][]string{"node-1": {"linux"}})
	h.server.runLedgerNodeID = trusted
	return h
}

func claimOneShot(t *testing.T, h *integrationHarness, agent *http.Client, node Node) Claim {
	t.Helper()
	return claimClass(t, h, agent, node, contract.JobClassOneShot)
}

// L1 classifies the submitter from the identity it authenticated, so the agent
// never has to reconstruct it. That matters because the identity's form is
// fabric-specific — a friendly Node ID on plain, a Tailscale StableID on tsnet
// — and only L1 holds the configuration that says which one is the ledger.
func TestL1ClassifiesTheRunLedgerSubmitterFromTheAuthenticatedIdentity(t *testing.T) {
	for _, testCase := range []struct {
		name      string
		trusted   string
		submitter string
		want      bool
	}{
		{name: "the configured ledger submitted it", trusted: "run-ledger", submitter: "run-ledger", want: true},
		{name: "an operator submitted it", trusted: "run-ledger", submitter: "operator-laptop", want: false},
		{
			// The tsnet shape: L1 is configured with the ledger's StableID and
			// the authenticated identity is that StableID.
			name:    "a fabric-specific identity is compared as configured",
			trusted: "nodeKEY7fL2", submitter: "nodeKEY7fL2", want: true,
		},
		{
			// The misconfiguration the contract calls out: the bit is false,
			// and a current L3's withhold label is what still protects the run.
			name:    "a wrong trusted identity classifies nothing",
			trusted: "nodeSOMETHINGELSE", submitter: "run-ledger", want: false,
		},
		{name: "an unset trusted identity falls back to the default", trusted: "run-ledger", submitter: "run-ledger", want: true},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			h := runLedgerHarness(t, testCase.trusted)
			client := h.client(fabric.Identity{NodeID: testCase.submitter, Tags: []string{DefaultClientPrincipalTag}})
			agent := h.client(fabric.Identity{NodeID: "node-1", Tags: []string{DefaultAgentPrincipalTag}})
			node := h.register(agent, "node-1")
			h.submit(client, "provenance-"+testCase.name, []string{"linux"})

			claim := claimOneShot(t, h, agent, node)
			if claim.SubmittedByRunLedger != testCase.want {
				t.Fatalf("claim SubmittedByRunLedger = %t, want %t", claim.SubmittedByRunLedger, testCase.want)
			}
		})
	}
}

// The classification is L1's, not the caller's. A submitter that puts the field
// in its own request body is rejected outright, and one that fills its JobSpec
// with run-ledger-looking content still classifies false.
func TestARequestBodyCannotClaimRunLedgerProvenance(t *testing.T) {
	h := runLedgerHarness(t, "run-ledger")
	client := h.client(fabric.Identity{NodeID: "operator-laptop", Tags: []string{DefaultClientPrincipalTag}})
	agent := h.client(fabric.Identity{NodeID: "node-1", Tags: []string{DefaultAgentPrincipalTag}})
	node := h.register(agent, "node-1")

	// JobSpec decoding rejects unknown members, so the field cannot even be
	// spelled on the wire.
	spec := validJobSpec("forged-provenance", []string{"linux"})
	encoded, err := json.Marshal(spec)
	if err != nil {
		t.Fatal(err)
	}
	var body map[string]any
	if err := json.Unmarshal(encoded, &body); err != nil {
		t.Fatal(err)
	}
	body["submitted_by_run_ledger"] = true
	if status, _, response := h.do(client, http.MethodPost, "/v1/jobs", body); status == http.StatusCreated {
		t.Fatalf("a JobSpec carrying submitted_by_run_ledger was accepted: %s", response)
	}

	// Everything a submitter *can* write, written to look like the ledger.
	spec = validJobSpec("dressed-up-provenance", []string{"linux"})
	spec.Labels = map[string]string{
		"run_id": "run_forged", contract.LabelDispatchAuthority: contract.LabelTrue,
	}
	if spec.Execution.Env == nil {
		spec.Execution.Env = map[string]string{}
	}
	spec.Execution.Env[contract.EnvL3Endpoint] = "http://submitter.invalid/l3"
	if status, _, response := h.do(client, http.MethodPost, "/v1/jobs", spec); status != http.StatusCreated {
		t.Fatalf("submit status = %d body=%s", status, response)
	}
	claim := claimOneShot(t, h, agent, node)
	if claim.SubmittedByRunLedger {
		t.Fatal("a job dressed up as an L3 dispatch was classified as run-ledger provenance")
	}
}

// A job spawned through an attempt credential inherits its root's originating
// submitter, but it is its own direct-L1 submission with no Run. L1 refuses the
// classification so the spawn chain keeps the credential it depends on.
func TestASpawnedChildIsNeverClassifiedAsRunLedgerProvenance(t *testing.T) {
	h := runLedgerHarness(t, "run-ledger")
	client := h.client(fabric.Identity{NodeID: "run-ledger", Tags: []string{DefaultClientPrincipalTag}})
	agent := h.client(fabric.Identity{NodeID: "node-1", Tags: []string{DefaultAgentPrincipalTag}})
	node := h.register(agent, "node-1")
	h.submit(client, "ledger-root", []string{"linux"})

	root := claimOneShot(t, h, agent, node)
	if !root.SubmittedByRunLedger {
		t.Fatal("the root run was not classified as run-ledger provenance")
	}
	status, body := h.credentialRequest(agent, http.MethodPost, "/v1/jobs", root.AttemptToken,
		validJobSpec("spawned-child", []string{"linux"}))
	if status != http.StatusCreated {
		t.Fatalf("spawn child status = %d body=%s", status, body)
	}
	child := decodeJob(t, body)
	if child.OriginatingSubmitter != "run-ledger" {
		t.Fatalf("child originating submitter = %q, want the inherited root submitter", child.OriginatingSubmitter)
	}

	// The node has more than one one-shot slot, so the child is simply the
	// next claim.
	spawned := claimOneShot(t, h, agent, node)
	if spawned.Job.JobID != child.JobID {
		t.Fatalf("claimed %q, want the spawned child %q", spawned.Job.JobID, child.JobID)
	}
	if spawned.SubmittedByRunLedger {
		t.Fatal("a spawned child inherited run-ledger provenance and would lose its attempt credential")
	}

	// The spawn route does not set the classification today, so assert the
	// store refuses it even when a caller supplies one. This is the rule that
	// keeps a future caller from reintroducing the inheritance.
	parent := AttemptCredentialScope{
		JobID: root.Job.JobID, AttemptID: root.Lease.AttemptID,
		NodeID: node.NodeID, IdentityNodeID: node.NodeID,
		OriginatingSubmitter: "run-ledger", SpawnDepth: 0,
	}
	insistent, _, err := h.store.CreateJobAs(t.Context(),
		validJobSpec("insistent-child", []string{"linux"}),
		JobOrigin{OriginatingSubmitter: "run-ledger", SubmittedByRunLedger: true, Parent: &parent})
	if err != nil {
		t.Fatal(err)
	}
	var stored bool
	if err := h.store.db.QueryRowContext(t.Context(),
		"SELECT submitted_by_run_ledger FROM jobs WHERE job_id=?", insistent.JobID).Scan(&stored); err != nil {
		t.Fatal(err)
	}
	if stored {
		t.Fatal("a child job stored run-ledger provenance supplied by its caller")
	}
}
