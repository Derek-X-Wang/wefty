package l1

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
)

// TestRestoreReceiptRacingARemovalNeverFailsTheHeartbeat is wefty #600's
// first race. The heartbeat revokes a restoring Computer's grants at the run
// ledger, and the operator removes that Computer before the receipt is
// written. The receipt's CAS then fails stale. That is one Computer's write,
// not the node's: the heartbeat still answers 200, withholds only that
// restore, and the next heartbeat carries the removal. Before the fix the
// whole heartbeat answered 409 stale_intent_revision, and the agent backed
// off and withdrew kind:oci over one Computer (the #548 invariant).
func TestRestoreReceiptRacingARemovalNeverFailsTheHeartbeat(t *testing.T) {
	h, node, computer, source, _ := publishedBackupForStorageCopy(t, 2)
	if _, _, err := h.store.BeginComputerRestore(context.Background(), computer.ComputerID,
		ComputerRestoreRequest{ComputerMutationPrecondition: computerPrecondition(computer, "operator"),
			BackupID: source.BackupID, IdempotencyKey: "race"}); err != nil {
		t.Fatal(err)
	}
	logs := &recordedLog{}
	h.server.logf = logs.record
	var mu sync.Mutex
	restoreRevocations := 0
	h.server.computerTokenRevoker = recordingComputerTokenRevoker{revoke: func(_ context.Context, request ComputerTokenRevocation) (contract.ComputerTokenRevocationReceipt, error) {
		if request.RestoreOperationRevision != 0 {
			mu.Lock()
			restoreRevocations++
			first := restoreRevocations == 1
			mu.Unlock()
			if first {
				current, err := h.store.GetComputer(context.Background(), computer.ComputerID)
				if err != nil {
					t.Error(err)
				} else if _, err := h.store.RemoveComputer(context.Background(), computer.ComputerID,
					ComputerRemoveRequest{ComputerMutationPrecondition: computerPrecondition(current, "operator")}); err != nil {
					t.Errorf("remove the restoring Computer while its revocation is at the run ledger: %v", err)
				}
			}
		}
		return contract.ComputerTokenRevocationReceipt{ComputerID: request.ComputerID, ComputerAttemptID: request.ComputerAttemptID,
			RestoreOperationRevision: request.RestoreOperationRevision, SubmitIntentRevision: request.NewSubmitIntentRevision,
			CommittedAt: h.clock.Now()}, nil
	}}
	agentClient := h.client(fabric.Identity{NodeID: "fabric-computer-node", Tags: []string{DefaultAgentPrincipalTag}})

	status, _, body := h.do(agentClient, http.MethodPost, "/v1/agent/nodes/"+node.NodeID+"/heartbeat", heartbeatRequestForNode(node))
	var racing HeartbeatResponse
	if status != http.StatusOK || json.Unmarshal(body, &racing) != nil {
		t.Fatalf("heartbeat racing a removal status=%d body=%s", status, body)
	}
	if racing.NodeID != node.NodeID {
		t.Fatalf("heartbeat racing a removal answered for node %q", racing.NodeID)
	}
	if len(racing.StorageCopyDirectives) != 0 {
		t.Fatalf("heartbeat handed out a restore whose receipt was never written: %#v", racing.StorageCopyDirectives)
	}
	if !strings.Contains(logs.text(), "event=l1_restore_revocation_receipt_deferred computer_id="+computer.ComputerID) {
		t.Fatalf("the deferred restore receipt was not named in the log: %s", logs.text())
	}

	status, _, body = h.do(agentClient, http.MethodPost, "/v1/agent/nodes/"+node.NodeID+"/heartbeat", heartbeatRequestForNode(node))
	var next HeartbeatResponse
	if status != http.StatusOK || json.Unmarshal(body, &next) != nil {
		t.Fatalf("heartbeat after the removal status=%d body=%s", status, body)
	}
	delivered := false
	for _, directive := range next.RemovalDirectives {
		delivered = delivered || directive.JobID == computer.CurrentJobID
	}
	if !delivered {
		t.Fatalf("the removed Computer's removal directive was not delivered: %#v", next.RemovalDirectives)
	}
	if len(next.StorageCopyDirectives) != 0 {
		t.Fatalf("a removed Computer's restore reached the node: %#v", next.StorageCopyDirectives)
	}
	mu.Lock()
	defer mu.Unlock()
	// A superseded restore is owed nothing: the next heartbeat does not ask
	// the run ledger again.
	if restoreRevocations != 1 {
		t.Fatalf("pre-restore revocations sent = %d, want 1", restoreRevocations)
	}
	removed, err := h.store.GetComputer(context.Background(), computer.ComputerID)
	if err != nil || removed.LastRestoreRevocation != nil {
		t.Fatalf("a removed restore recorded a receipt = %#v err=%v", removed.LastRestoreRevocation, err)
	}
}

// TestSubmissionChangeRevokesOnlyAfterItCommits ports the audit probe for
// wefty #600's second race. The route used to revoke the Computer's passes at
// L3 before its CAS commit, so another Computer's change bumping the global
// policy revision in that window left C's live pass revoked while C's L1
// authority stood still, and the agent never re-minted. Now the change
// commits first: every revocation for C finds C's committed revision, and a
// change that loses its CAS revokes nothing.
func TestSubmissionChangeRevokesOnlyAfterItCommits(t *testing.T) {
	h := newIntegrationHarnessWithPolicies(t, map[string]NodePolicy{})
	ctx := context.Background()
	admin := fabric.Identity{NodeID: "admin-device", UserID: "admin", DeviceID: "device-1"}
	client := h.client(admin)
	challenge, err := h.store.InitiateAdminBootstrap(ctx)
	if err != nil {
		t.Fatal(err)
	}
	status, _, body := h.do(client, http.MethodPost, "/v1/admin-bootstrap", BootstrapAdminRequest{Nonce: challenge.Nonce})
	if status != http.StatusCreated {
		t.Fatalf("bootstrap status=%d body=%s", status, body)
	}
	var policy AdminPolicy
	if err := json.Unmarshal(body, &policy); err != nil {
		t.Fatal(err)
	}
	c, _, err := h.store.CreateComputer(ctx, CreateComputerRequest{Name: "c", Spec: computerCapabilityJobSpec("computer:c"), Actor: "operator"})
	if err != nil {
		t.Fatal(err)
	}
	d, _, err := h.store.CreateComputer(ctx, CreateComputerRequest{Name: "d", Spec: computerCapabilityJobSpec("computer:d"), Actor: "operator"})
	if err != nil {
		t.Fatal(err)
	}
	type sent struct {
		request           ComputerTokenRevocation
		committedRevision int64
	}
	var revocations []sent
	h.server.computerTokenRevoker = recordingComputerTokenRevoker{revoke: func(_ context.Context, request ComputerTokenRevocation) (contract.ComputerTokenRevocationReceipt, error) {
		if request.ComputerID == c.ComputerID {
			current, err := h.store.GetComputer(ctx, c.ComputerID)
			if err != nil {
				t.Error(err)
			}
			revocations = append(revocations, sent{request, current.SubmitIntentRevision})
			// Another administrator changes Computer D while C's revocation
			// is at the run ledger.
			_, _, _, _ = doRequest(client, http.MethodPut, "/v1/computers/"+d.ComputerID+"/submission",
				ComputerSubmissionRequest{PolicyRevision: policy.Revision, SubmitIntentRevision: 0, SubmitEnabled: boolPointer(true),
					IdempotencyKey: "d-enable"})
		}
		return contract.ComputerTokenRevocationReceipt{ComputerID: request.ComputerID,
			SubmitIntentRevision: request.NewSubmitIntentRevision, CommittedAt: h.clock.Now()}, nil
	}}
	status, _, body = h.do(client, http.MethodPut, "/v1/computers/"+c.ComputerID+"/submission",
		ComputerSubmissionRequest{PolicyRevision: policy.Revision, SubmitIntentRevision: 0, SubmitEnabled: boolPointer(true),
			IdempotencyKey: "c-enable"})
	after, err := h.store.GetComputer(ctx, c.ComputerID)
	if err != nil {
		t.Fatal(err)
	}
	for _, revocation := range revocations {
		if revocation.request.RevokeAll || revocation.committedRevision != revocation.request.NewSubmitIntentRevision {
			t.Fatalf("C's passes below revision %d were revoked while C's committed revision was %d (revoke_all=%t); C's change answered %d %s",
				revocation.request.NewSubmitIntentRevision, revocation.committedRevision, revocation.request.RevokeAll, status, body)
		}
	}
	if status != http.StatusOK || after.SubmitIntentRevision != 1 || !after.SubmitEnabled || len(revocations) != 1 {
		t.Fatalf("C's change status=%d body=%s revision=%d revocations=%d", status, body, after.SubmitIntentRevision, len(revocations))
	}

	// A change that loses its CAS to another Computer's change revokes
	// nothing and leaves the Computer's authority where it was.
	status, _, body = h.do(client, http.MethodPut, "/v1/computers/"+d.ComputerID+"/submission",
		ComputerSubmissionRequest{PolicyRevision: after.SubmitPolicyRevision, SubmitIntentRevision: 0, SubmitEnabled: boolPointer(true),
			IdempotencyKey: "d-enable-current"})
	if status != http.StatusOK {
		t.Fatalf("D's change status=%d body=%s", status, body)
	}
	status, _, body = h.do(client, http.MethodPut, "/v1/computers/"+c.ComputerID+"/submission",
		ComputerSubmissionRequest{PolicyRevision: after.SubmitPolicyRevision, SubmitIntentRevision: 1, SubmitEnabled: boolPointer(false),
			IdempotencyKey: "c-disable-stale"})
	var refusal contract.ErrorResponse
	if status != http.StatusConflict || json.Unmarshal(body, &refusal) != nil || refusal.Error.Code != contract.ErrorStalePolicyRevision {
		t.Fatalf("C's stale change status=%d body=%s", status, body)
	}
	unchanged, err := h.store.GetComputer(ctx, c.ComputerID)
	if err != nil || unchanged.SubmitIntentRevision != 1 || !unchanged.SubmitEnabled {
		t.Fatalf("a change that lost its CAS moved C's authority = %#v err=%v", unchanged, err)
	}
	if len(revocations) != 1 {
		t.Fatalf("a change that lost its CAS revoked C's passes: %#v", revocations[1:])
	}
}

// revisionGrantLedger models L3's grant table for one Computer closely enough
// to tell a revision-bound revocation from a revoke-all.
type revisionGrantLedger struct {
	mu     sync.Mutex
	grants map[int64]bool // submit intent revision -> revoked
}

func (ledger *revisionGrantLedger) mint(revision int64) {
	ledger.mu.Lock()
	defer ledger.mu.Unlock()
	ledger.grants[revision] = false
}

func (ledger *revisionGrantLedger) active(revision int64) bool {
	ledger.mu.Lock()
	defer ledger.mu.Unlock()
	revoked, minted := ledger.grants[revision]
	return minted && !revoked
}

func (ledger *revisionGrantLedger) revoke(request ComputerTokenRevocation) int {
	ledger.mu.Lock()
	defer ledger.mu.Unlock()
	count := 0
	for revision, revoked := range ledger.grants {
		if !revoked && (request.RevokeAll || revision < request.NewSubmitIntentRevision) {
			ledger.grants[revision] = true
			count++
		}
	}
	return count
}

// TestSubmissionChangeRevokesTheOldPassAndSparesTheReMint is the normal path:
// the change commits, the agent may re-mint at the new revision before the
// revocation reaches the run ledger, and the revision-bound revocation ends
// the superseded pass without touching the re-minted one.
func TestSubmissionChangeRevokesTheOldPassAndSparesTheReMint(t *testing.T) {
	h, computer, _ := liveComputerTokenScope(t, "spares-the-remint")
	ledger := &revisionGrantLedger{grants: map[int64]bool{}}
	ledger.mint(computer.SubmitIntentRevision)
	var requests []ComputerTokenRevocation
	h.server.computerTokenRevoker = recordingComputerTokenRevoker{revoke: func(_ context.Context, request ComputerTokenRevocation) (contract.ComputerTokenRevocationReceipt, error) {
		requests = append(requests, request)
		// The agent saw the committed change and re-minted first.
		current, err := h.store.GetComputer(context.Background(), computer.ComputerID)
		if err != nil {
			return contract.ComputerTokenRevocationReceipt{}, err
		}
		ledger.mint(current.SubmitIntentRevision)
		return contract.ComputerTokenRevocationReceipt{ComputerID: request.ComputerID, SubmitIntentRevision: request.NewSubmitIntentRevision,
			RevokedGrantCount: ledger.revoke(request), CommittedAt: h.clock.Now()}, nil
	}}
	status, body, mutation := resizeComputerSubmission(t, h, computer, 7)
	if status != http.StatusOK || !mutation.MutationApplied || mutation.Revoked == nil || mutation.RevocationNotice != "" ||
		mutation.Revoked.SubmitIntentRevision != computer.SubmitIntentRevision+1 || mutation.Revoked.RevokedGrantCount != 1 {
		t.Fatalf("resize status=%d body=%s", status, body)
	}
	if len(requests) != 1 || requests[0].RevokeAll || requests[0].NewSubmitIntentRevision != computer.SubmitIntentRevision+1 {
		t.Fatalf("revocations sent = %#v, want one bound below revision %d", requests, computer.SubmitIntentRevision+1)
	}
	if ledger.active(computer.SubmitIntentRevision) {
		t.Fatal("the superseded pass is still active")
	}
	if !ledger.active(computer.SubmitIntentRevision + 1) {
		t.Fatal("the revocation ended the pass the agent re-minted at the committed revision")
	}
}

// TestSubmissionChangeWhoseRevocationFailsStillFencesTheOldPass is the
// failure between commit and revoke. The change stands and says its
// revocation was not recorded, and L1's live scope proof no longer admits the
// old pass's revision, which is what L3 re-proves on every use.
func TestSubmissionChangeWhoseRevocationFailsStillFencesTheOldPass(t *testing.T) {
	h, computer, claim := liveComputerTokenScope(t, "revocation-fails")
	h.server.computerTokenRevoker = recordingComputerTokenRevoker{revoke: func(context.Context, ComputerTokenRevocation) (contract.ComputerTokenRevocationReceipt, error) {
		return contract.ComputerTokenRevocationReceipt{}, errors.New("run ledger unreachable")
	}}
	status, body, mutation := resizeComputerSubmission(t, h, computer, 7)
	if status != http.StatusOK || !mutation.MutationApplied || mutation.Revoked != nil || mutation.RevocationNotice == "" ||
		mutation.SubmitIntentRevision != computer.SubmitIntentRevision+1 || mutation.SubmitMaxInflight != 7 {
		t.Fatalf("resize with a failing revocation status=%d body=%s", status, body)
	}
	ctx := context.Background()
	// Until the node installs the new policy, L1 proves no scope at all.
	if _, err := h.store.ProveComputerTokenScope(ctx, computer.ComputerID, claim.Lease.AttemptID,
		"fabric-computer-node", ""); errorCode(err) != contract.ErrorForbidden {
		t.Fatalf("scope proof before the new policy is installed = %v, want forbidden", err)
	}
	nodes, err := h.store.ListNodes(ctx)
	if err != nil || len(nodes) != 1 {
		t.Fatalf("nodes = %#v err=%v", nodes, err)
	}
	node := nodes[0]
	snapshot, err := h.store.IssueComputerPolicySnapshot(ctx, "fabric-computer-node", "fabric-test",
		node.NodeID, node.BootSessionID, time.Minute)
	if err != nil || snapshot == nil {
		t.Fatalf("snapshot = (%#v, %v)", snapshot, err)
	}
	if err := h.store.AcknowledgeComputerPolicyInstallation(ctx, "fabric-computer-node", acknowledgementFor(*snapshot)); err != nil {
		t.Fatal(err)
	}
	// Then it proves only the committed revision and limit, which the old
	// pass's grant does not carry, so L3 refuses it.
	proof, err := h.store.ProveComputerTokenScope(ctx, computer.ComputerID, claim.Lease.AttemptID, "fabric-computer-node", "")
	if err != nil || proof.SubmitIntentRevision != computer.SubmitIntentRevision+1 || proof.SubmitMaxInflight != 7 {
		t.Fatalf("scope proof after the change = (%#v, %v)", proof, err)
	}
}

func resizeComputerSubmission(t *testing.T, h *integrationHarness, computer Computer, inflight int) (int, []byte, ComputerSubmissionMutationResult) {
	t.Helper()
	var policyRevision int64
	if err := h.store.db.QueryRow(`SELECT revision FROM admin_policy WHERE singleton=1`).Scan(&policyRevision); err != nil {
		t.Fatal(err)
	}
	// liveComputerTokenScope makes "admin" an administrator of the store's
	// test Fabric; the route sees the harness network's Fabric instead.
	client := h.client(fabric.Identity{UserID: "admin", DeviceID: "device-1"})
	status, _, body := h.do(client, http.MethodGet, "/v1/whoami", nil)
	var person AuthenticatedPerson
	if status != http.StatusOK || json.Unmarshal(body, &person) != nil {
		t.Fatalf("whoami status=%d body=%s", status, body)
	}
	if _, err := h.store.db.Exec(`UPDATE admins SET fabric_id=? WHERE user_id='admin'`, person.FabricID); err != nil {
		t.Fatal(err)
	}
	status, _, body = h.do(client, http.MethodPut, "/v1/computers/"+computer.ComputerID+"/submission",
		ComputerSubmissionRequest{PolicyRevision: policyRevision, SubmitIntentRevision: computer.SubmitIntentRevision,
			SubmitMaxInflight: intPointer(inflight), IdempotencyKey: "resize-" + computer.ComputerID})
	var mutation ComputerSubmissionMutationResult
	if status == http.StatusOK {
		if err := json.Unmarshal(body, &mutation); err != nil {
			t.Fatal(err)
		}
	}
	return status, body, mutation
}
