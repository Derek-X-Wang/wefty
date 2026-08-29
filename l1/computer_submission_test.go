package l1

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
)

type recordingComputerTokenRevoker struct {
	revoke func(context.Context, ComputerTokenRevocation) (contract.ComputerTokenRevocationReceipt, error)
	count  func(context.Context, string) (int, error)
}

func (revoker recordingComputerTokenRevoker) RevokeComputerTokens(ctx context.Context, request ComputerTokenRevocation) (contract.ComputerTokenRevocationReceipt, error) {
	return revoker.revoke(ctx, request)
}

func (revoker recordingComputerTokenRevoker) CountComputerInflight(ctx context.Context, computerID string) (int, error) {
	if revoker.count == nil {
		return 0, nil
	}
	return revoker.count(ctx, computerID)
}

func intPointer(value int) *int { return &value }

func TestComputerSubmissionIntentDefaultsOffAndAdvancesWithAudit(t *testing.T) {
	h := newIntegrationHarnessWithPolicies(t, map[string]NodePolicy{})
	ctx := context.Background()
	admin := fabric.Identity{FabricID: "fabric-test", UserID: "admin", DeviceID: "device-1"}
	challenge, err := h.store.InitiateAdminBootstrap(ctx)
	if err != nil {
		t.Fatal(err)
	}
	policy, err := h.store.BootstrapAdmin(ctx, admin, challenge.Nonce)
	if err != nil {
		t.Fatal(err)
	}
	computer, replayed, err := h.store.CreateComputer(ctx, CreateComputerRequest{
		Name: "submission-computer", Spec: computerCapabilityJobSpec("computer:submission"), Actor: "operator",
	})
	if err != nil || replayed {
		t.Fatalf("create Computer = (%#v, %v, %v)", computer, replayed, err)
	}
	if computer.SubmitEnabled || computer.SubmitIntentRevision != 0 ||
		computer.SubmitMaxInflight != DefaultComputerSubmitMaxInflight || computer.SubmitPolicyRevision != 0 {
		t.Fatalf("default submission authority = %#v", computer)
	}
	request := ComputerSubmissionRequest{PolicyRevision: policy.Revision, SubmitIntentRevision: 0,
		SubmitEnabled: boolPointer(true), IdempotencyKey: "enable-submit"}
	enabled, replayed, applied, err := h.store.MutateComputerSubmission(ctx, admin, computer.ComputerID, request)
	if err != nil || replayed || !applied {
		t.Fatalf("enable submission = (%#v, replay=%v, applied=%v, %v)", enabled, replayed, applied, err)
	}
	if !enabled.SubmitEnabled || enabled.SubmitIntentRevision != 1 || enabled.SubmitPolicyRevision != 2 {
		t.Fatalf("enabled submission authority = %#v", enabled)
	}
	replayedComputer, replayed, applied, err := h.store.MutateComputerSubmission(ctx, admin, computer.ComputerID, request)
	if err != nil || !replayed || replayedComputer.SubmitIntentRevision != enabled.SubmitIntentRevision {
		t.Fatalf("submission replay = (%#v, %v, %v)", replayedComputer, replayed, err)
	}
	if _, _, _, err := h.store.MutateComputerSubmission(ctx, admin, computer.ComputerID,
		ComputerSubmissionRequest{PolicyRevision: 2, SubmitIntentRevision: 1, SubmitMaxInflight: intPointer(19),
			IdempotencyKey: "enable-submit"}); errorCode(err) != contract.ErrorIdempotencyConflict {
		t.Fatalf("changed idempotency replay error = %v", err)
	}
	nonAdmin := fabric.Identity{FabricID: "fabric-test", UserID: "viewer", DeviceID: "device-2"}
	if _, _, _, err := h.store.MutateComputerSubmission(ctx, nonAdmin, computer.ComputerID,
		ComputerSubmissionRequest{PolicyRevision: 2, SubmitIntentRevision: 1, SubmitEnabled: boolPointer(false),
			IdempotencyKey: "disable-submit"}); errorCode(err) != contract.ErrorAdminRequired {
		t.Fatalf("non-admin submission error = %v", err)
	}
	var rows int
	if err := h.store.db.QueryRow(`SELECT COUNT(*) FROM computer_submission_audit WHERE computer_id=?`, computer.ComputerID).Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if rows != 1 {
		t.Fatalf("submission audit rows = %d, want 1", rows)
	}
	if _, err := h.store.db.Exec(`UPDATE computer_submission_audit SET submit_enabled=0 WHERE computer_id=?`, computer.ComputerID); err == nil {
		t.Fatal("Computer submission audit accepted mutation")
	}
}

func TestComputerSubmissionRouteRevokesL3BeforeReportingSuccess(t *testing.T) {
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
	computer, _, err := h.store.CreateComputer(ctx, CreateComputerRequest{
		Name: "submission-route", Spec: computerCapabilityJobSpec("computer:submission-route"), Actor: "operator",
	})
	if err != nil {
		t.Fatal(err)
	}
	h.server.computerTokenRevoker = recordingComputerTokenRevoker{}
	status, _, body = h.do(client, http.MethodGet, "/v1/computers/"+computer.ComputerID+"/submission", nil)
	if status != http.StatusOK {
		t.Fatalf("read submission state status=%d body=%s", status, body)
	}
	var state ComputerSubmissionState
	if err := json.Unmarshal(body, &state); err != nil {
		t.Fatal(err)
	}
	if state.ComputerID != computer.ComputerID || state.SubmitEnabled || state.SubmitIntentRevision != 0 ||
		state.SubmitMaxInflight != DefaultComputerSubmitMaxInflight || state.PolicyRevision != policy.Revision {
		t.Fatalf("submission state = %#v", state)
	}
	var narrow map[string]json.RawMessage
	if err := json.Unmarshal(body, &narrow); err != nil {
		t.Fatal(err)
	}
	for _, forbidden := range []string{"grants", "current_job", "storage_id", "display_endpoint"} {
		if _, present := narrow[forbidden]; present {
			t.Fatalf("submission state leaked %q: %s", forbidden, body)
		}
	}
	revokedBeforeMutation := false
	h.server.computerTokenRevoker = recordingComputerTokenRevoker{revoke: func(_ context.Context, request ComputerTokenRevocation) (contract.ComputerTokenRevocationReceipt, error) {
		current, readErr := readComputerAuthority(ctx, h.store.db, computer.ComputerID, h.clock.Now())
		if readErr != nil {
			return contract.ComputerTokenRevocationReceipt{}, readErr
		}
		revokedBeforeMutation = !current.SubmitEnabled && current.SubmitIntentRevision == 0 &&
			request.ComputerID == computer.ComputerID && request.NewSubmitIntentRevision == 1
		return contract.ComputerTokenRevocationReceipt{ComputerID: request.ComputerID,
			SubmitIntentRevision: request.NewSubmitIntentRevision, CommittedAt: h.clock.Now()}, nil
	}}
	status, headers, body := h.do(client, http.MethodPut, "/v1/computers/"+computer.ComputerID+"/submission",
		ComputerSubmissionRequest{PolicyRevision: policy.Revision, SubmitIntentRevision: 0, SubmitEnabled: boolPointer(true),
			IdempotencyKey: "enable-route"})
	if status != http.StatusOK || !revokedBeforeMutation || headers.Get("Idempotent-Replay") != "" {
		t.Fatalf("enable route status=%d body=%s revoked-before=%t headers=%v", status, body, revokedBeforeMutation, headers)
	}
	var mutation ComputerSubmissionMutationResult
	if err := json.Unmarshal(body, &mutation); err != nil || !mutation.MutationApplied || mutation.Revoked == nil ||
		mutation.Revoked.SubmitIntentRevision != 1 || mutation.PolicyRevision != 2 {
		t.Fatalf("enable route mutation = %#v err=%v body=%s", mutation, err, body)
	}
	status, headers, body = h.do(client, http.MethodPut, "/v1/computers/"+computer.ComputerID+"/submission",
		ComputerSubmissionRequest{PolicyRevision: policy.Revision, SubmitIntentRevision: 0, SubmitEnabled: boolPointer(true),
			IdempotencyKey: "enable-route"})
	mutation = ComputerSubmissionMutationResult{}
	if err := json.Unmarshal(body, &mutation); status != http.StatusOK || err != nil || headers.Get("Idempotent-Replay") != "true" ||
		mutation.MutationApplied || mutation.Revoked != nil {
		t.Fatalf("enable replay status=%d headers=%v mutation=%#v err=%v body=%s", status, headers, mutation, err, body)
	}
	h.server.computerTokenRevoker = recordingComputerTokenRevoker{revoke: func(context.Context, ComputerTokenRevocation) (contract.ComputerTokenRevocationReceipt, error) {
		return contract.ComputerTokenRevocationReceipt{}, errors.New("L3 unavailable")
	}}
	status, _, body = h.do(client, http.MethodPut, "/v1/computers/"+computer.ComputerID+"/submission",
		ComputerSubmissionRequest{PolicyRevision: 2, SubmitIntentRevision: 1, SubmitEnabled: boolPointer(false),
			IdempotencyKey: "disable-route"})
	if status != http.StatusInternalServerError {
		t.Fatalf("disable without L3 status=%d body=%s", status, body)
	}
	current, err := readComputerAuthority(ctx, h.store.db, computer.ComputerID, h.clock.Now())
	if err != nil {
		t.Fatal(err)
	}
	if !current.SubmitEnabled || current.SubmitIntentRevision != 1 {
		t.Fatalf("failed revocation mutated submission authority: %#v", current)
	}
}

func TestComputerSubmissionAuthorityChangeInvalidatesPublishedReadiness(t *testing.T) {
	h := newIntegrationHarness(t, map[string][]string{"computer-node": {"computer"}})
	ctx := context.Background()
	node := registerCapabilityNodeWithTags(t, h, "computer-node", map[string]bool{
		"kind:oci": true, "cgroup_v2": true, "computer": true,
	}, []string{contract.StableNodeTagPrefix + "computer-node"})
	computer, _, err := h.store.CreateComputer(ctx, CreateComputerRequest{
		Name: "readiness-revision", Spec: computerCapabilityJobSpec("computer:readiness-revision"), Actor: "operator",
	})
	if err != nil {
		t.Fatal(err)
	}
	claim, err := h.store.ClaimJob(ctx, "fabric-computer-node", node.NodeID, node.BootSessionID, contract.JobClassService)
	if err != nil || claim == nil {
		t.Fatalf("claim Computer = %#v err=%v", claim, err)
	}
	ready := true
	endpoint := "ws://computer-node.private:43123/websockify"
	if _, err := h.store.SetAttemptPublication(ctx, "fabric-computer-node", claim.Job.JobID, claim.Lease.AttemptID,
		PublicationRequest{FencingToken: claim.Lease.FencingToken, Ready: &ready, DisplayEndpoint: &endpoint}); err != nil {
		t.Fatal(err)
	}
	admin := fabric.Identity{FabricID: "fabric-test", UserID: "admin", DeviceID: "device-1"}
	challenge, err := h.store.InitiateAdminBootstrap(ctx)
	if err != nil {
		t.Fatal(err)
	}
	policy, err := h.store.BootstrapAdmin(ctx, admin, challenge.Nonce)
	if err != nil {
		t.Fatal(err)
	}
	mutated, replayed, applied, err := h.store.MutateComputerSubmission(ctx, admin, computer.ComputerID,
		ComputerSubmissionRequest{PolicyRevision: policy.Revision, SubmitIntentRevision: 0,
			SubmitEnabled: boolPointer(true), IdempotencyKey: "invalidate-ready"})
	if err != nil || replayed || !applied || (mutated.CurrentJob.Ready != nil && *mutated.CurrentJob.Ready) || mutated.DisplayEndpoint != nil {
		t.Fatalf("mutated readiness = %#v replay=%v applied=%v err=%v", mutated, replayed, applied, err)
	}
	var publishedAttempt *string
	if err := h.store.db.QueryRow(`SELECT published_attempt_id FROM service_jobs WHERE job_id=?`, computer.CurrentJobID).Scan(&publishedAttempt); err != nil {
		t.Fatal(err)
	}
	if publishedAttempt != nil {
		t.Fatalf("published attempt survived authority change: %q", *publishedAttempt)
	}
}

func TestComputerTokenScopeProofRequiresLiveAttemptAndInstalledPolicy(t *testing.T) {
	h := newIntegrationHarnessWithOptions(t, StoreOptions{LeaseDuration: 2 * time.Second}, map[string]NodePolicy{
		"computer-node": DefaultNodePolicy(contract.StableNodeTagPrefix + "computer-node"),
	})
	ctx := context.Background()
	node := registerCapabilityNodeWithTags(t, h, "computer-node", map[string]bool{
		"kind:oci": true, "cgroup_v2": true, "computer": true,
	}, []string{contract.StableNodeTagPrefix + "computer-node"})
	admin := fabric.Identity{FabricID: "fabric-test", UserID: "admin", DeviceID: "device-1"}
	challenge, err := h.store.InitiateAdminBootstrap(ctx)
	if err != nil {
		t.Fatal(err)
	}
	policy, err := h.store.BootstrapAdmin(ctx, admin, challenge.Nonce)
	if err != nil {
		t.Fatal(err)
	}
	computer, _, err := h.store.CreateComputer(ctx, CreateComputerRequest{Name: "scope-proof",
		Spec: computerCapabilityJobSpec("computer:scope-proof"), Actor: "operator"})
	if err != nil {
		t.Fatal(err)
	}
	computer, _, _, err = h.store.MutateComputerSubmission(ctx, admin, computer.ComputerID, ComputerSubmissionRequest{
		PolicyRevision: policy.Revision, SubmitIntentRevision: 0, SubmitEnabled: boolPointer(true),
		IdempotencyKey: "scope-enable",
	})
	if err != nil {
		t.Fatal(err)
	}
	claim, err := h.store.ClaimJob(ctx, "fabric-computer-node", node.NodeID, node.BootSessionID, contract.JobClassService)
	if err != nil || claim == nil {
		t.Fatalf("claim = (%#v, %v)", claim, err)
	}
	if _, err := h.store.ProveComputerTokenScope(ctx, computer.ComputerID, claim.Lease.AttemptID,
		"fabric-computer-node", ""); errorCode(err) != contract.ErrorForbidden {
		t.Fatalf("scope proof before installed policy = %v", err)
	}
	snapshot, err := h.store.IssueComputerPolicySnapshot(ctx, "fabric-computer-node", "fabric-test",
		node.NodeID, node.BootSessionID, time.Minute)
	if err != nil || snapshot == nil {
		t.Fatalf("snapshot = (%#v, %v)", snapshot, err)
	}
	if err := h.store.AcknowledgeComputerPolicyInstallation(ctx, "fabric-computer-node", acknowledgementFor(*snapshot)); err != nil {
		t.Fatal(err)
	}
	proof, err := h.store.ProveComputerTokenScope(ctx, computer.ComputerID, claim.Lease.AttemptID,
		"fabric-computer-node", "")
	if err != nil || proof.ComputerStorageGeneration != computer.StorageGeneration ||
		proof.SubmitIntentRevision != computer.SubmitIntentRevision || proof.SubmitMaxInflight != 20 {
		t.Fatalf("scope proof = (%#v, %v)", proof, err)
	}
	if _, err := h.store.ProveComputerTokenScope(ctx, computer.ComputerID, claim.Lease.AttemptID,
		"foreign-node", ""); errorCode(err) != contract.ErrorForbidden {
		t.Fatalf("foreign Node scope proof = %v", err)
	}
	if stableProof, err := h.store.ProveComputerTokenScope(ctx, computer.ComputerID, claim.Lease.AttemptID,
		"", node.NodeID); err != nil || stableProof.HostNodeID != "fabric-computer-node" {
		t.Fatalf("stable Node revalidation proof = (%#v, %v)", stableProof, err)
	}
	h.clock.Advance(2 * time.Second)
	if _, err := h.store.ProveComputerTokenScope(ctx, computer.ComputerID, claim.Lease.AttemptID,
		"", node.NodeID); errorCode(err) != contract.ErrorForbidden {
		t.Fatalf("expired lease scope proof = %v", err)
	}
}
