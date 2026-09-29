package l1

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
)

// revocationLog records every Computer-wide token revocation L1 sends to the
// run ledger.
type revocationLog struct {
	mu    sync.Mutex
	calls []ComputerTokenRevocation
}

func (log *revocationLog) install(h *integrationHarness) {
	h.server.computerTokenRevoker = recordingComputerTokenRevoker{revoke: func(_ context.Context, request ComputerTokenRevocation) (contract.ComputerTokenRevocationReceipt, error) {
		log.mu.Lock()
		defer log.mu.Unlock()
		log.calls = append(log.calls, request)
		return contract.ComputerTokenRevocationReceipt{ComputerID: request.ComputerID, CommittedAt: time.Now()}, nil
	}}
}

// count returns how many revocations were sent, failing on any that is not
// scoped to exactly attemptID of computerID.
func (log *revocationLog) count(t *testing.T, computerID, attemptID string) int {
	t.Helper()
	log.mu.Lock()
	defer log.mu.Unlock()
	for _, call := range log.calls {
		if call.ComputerID != computerID || call.ComputerAttemptID != attemptID || call.RevokeAll ||
			call.NewSubmitIntentRevision != 0 || call.RestoreOperationRevision != 0 || call.Reason != "attempt_terminal" {
			t.Fatalf("completion sent %#v, want a revocation scoped to attempt %s only", call, attemptID)
		}
	}
	return len(log.calls)
}

func computerCompletionHarness(t *testing.T) (*integrationHarness, *revocationLog, Node, *http.Client) {
	t.Helper()
	h := newIntegrationHarnessWithOptions(t, StoreOptions{LeaseDuration: 3 * time.Second}, map[string]NodePolicy{
		"computer-node": {Tags: []string{contract.StableNodeTagPrefix + "computer-node"}, MaxOneshotSlots: 1, MaxServiceSlots: 1},
	})
	revocations := &revocationLog{}
	revocations.install(h)
	node := registerCapabilityNodeWithTags(t, h, "computer-node", computerNodeCapabilities(),
		[]string{contract.StableNodeTagPrefix + "computer-node"})
	agent := h.client(fabric.Identity{NodeID: "fabric-computer-node", Tags: []string{DefaultAgentPrincipalTag}})
	return h, revocations, node, agent
}

func computerNodeCapabilities() map[string]bool {
	return map[string]bool{"kind:oci": true, "cgroup_v2": true, "computer": true, "runtime_platform:linux/amd64": true}
}

func reregisterComputerNode(t *testing.T, h *integrationHarness, bootSessionID string) Node {
	t.Helper()
	node, err := h.store.RegisterNode(t.Context(), fabric.Identity{NodeID: "fabric-computer-node"}, contract.NodeRegistration{
		NodeID: "computer-node", BootSessionID: bootSessionID, RootInstanceID: "root-computer-node",
		OS: "linux", Architecture: "amd64", AgentVersion: "test", Capabilities: computerNodeCapabilities(),
		CapabilityRevision: 1, CapabilityObservedAt: h.clock.Now(), MissingCapabilities: []string{},
	}, NodePolicy{Tags: []string{contract.StableNodeTagPrefix + "computer-node"}, MaxOneshotSlots: 1, MaxServiceSlots: 1}, true)
	if err != nil {
		t.Fatal(err)
	}
	return node
}

func startComputerAttempt(t *testing.T, h *integrationHarness, node Node, image *contract.OCIImageSpec) *Claim {
	t.Helper()
	claim, err := h.store.ClaimJob(t.Context(), "fabric-computer-node", node.NodeID, node.BootSessionID, contract.JobClassService)
	if err != nil || claim == nil {
		t.Fatalf("Computer claim = %#v err=%v", claim, err)
	}
	observation := testImageObservation(claim.Lease.FencingToken)
	if image != nil {
		observation.SubmittedReference = image.Reference
		observation.TopLevelDigest = *image.Digest
		observation.PlatformManifestDigest = *image.Digest
	}
	if _, err := h.store.ObserveAttemptImage(t.Context(), "fabric-computer-node", claim.Job.JobID,
		claim.Lease.AttemptID, observation); err != nil {
		t.Fatal(err)
	}
	if _, err := h.store.StartAttempt(t.Context(), "fabric-computer-node", claim.Job.JobID,
		claim.Lease.AttemptID, StartedRequest{FencingToken: claim.Lease.FencingToken}); err != nil {
		t.Fatal(err)
	}
	return claim
}

func postCompletion(t *testing.T, h *integrationHarness, agent *http.Client, claim *Claim, request CompletionRequest) http.Header {
	t.Helper()
	status, headers, body := h.do(agent, http.MethodPost,
		fmt.Sprintf("/v1/agent/jobs/%s/attempts/%s/complete", claim.Job.JobID, claim.Lease.AttemptID), request)
	if status != http.StatusOK {
		t.Fatalf("completion status = %d body=%s", status, body)
	}
	return headers
}

// #553 review: a reimage installs a new current Job while the old Job keeps
// its completion replay binding. An accepted replay of the old Job's
// completion -- from the same registration, as before #553, or after the node
// re-registered -- still re-drives its revocation, but scoped to the old
// attempt, never Computer-wide, so the replacement attempt's tokens survive.
func TestCompletionReplayAfterReimageRevokesOnlyTheCompletedAttempt(t *testing.T) {
	for _, reregister := range []bool{false, true} {
		t.Run(fmt.Sprintf("reregistered=%t", reregister), func(t *testing.T) {
			h, revocations, node, agent := computerCompletionHarness(t)
			computer, _, err := h.store.CreateComputer(t.Context(), CreateComputerRequest{
				Name: "replay-after-reimage", Spec: computerCapabilityJobSpec("computer:replay-reimage:v1"), Actor: "operator"})
			if err != nil {
				t.Fatal(err)
			}
			old := startComputerAttempt(t, h, node, nil)
			computer, err = h.store.GetComputer(t.Context(), computer.ComputerID)
			if err != nil {
				t.Fatal(err)
			}
			reimage := ComputerReimageRequest{ComputerMutationPrecondition: computerPrecondition(computer, "operator"),
				Image: reimageTarget('f'), IdempotencyKey: "replay-reimage", TerminateSessions: true}
			if _, err := h.store.ReimageComputer(t.Context(), computer.ComputerID, reimage); err != nil {
				t.Fatal(err)
			}
			exitCode := 0
			completion := CompletionRequest{FencingToken: old.Lease.FencingToken, IdempotencyKey: "completion:" + old.Lease.AttemptID,
				Result: ProcessResult{ExitCode: &exitCode}, RuntimeQuiescenceEvidence: RuntimeQuiescenceAttempt}
			postCompletion(t, h, agent, old, completion)
			if got := revocations.count(t, computer.ComputerID, old.Lease.AttemptID); got != 1 {
				t.Fatalf("first completion sent %d revocations, want 1", got)
			}

			acknowledgeReimagePreflight(t, h, node, computer.ComputerID, old.Lease.AttemptID, old.Lease.FencingToken)
			projected, err := h.store.ReimageComputer(t.Context(), computer.ComputerID, reimage)
			if err != nil || projected.CurrentJobID == old.Job.JobID {
				t.Fatalf("activated reimage = %#v err=%v", projected, err)
			}
			if reregister {
				node = reregisterComputerNode(t, h, "boot-computer-node-restarted")
			}
			replacement := startComputerAttempt(t, h, node, &reimage.Image)
			if replacement.Job.JobID != projected.CurrentJobID {
				t.Fatalf("replacement claimed Job %s, want the reimaged Job %s", replacement.Job.JobID, projected.CurrentJobID)
			}

			headers := postCompletion(t, h, agent, old, completion)
			if headers.Get("Idempotent-Replay") != "true" {
				t.Fatalf("old completion replay Idempotent-Replay = %q, want true", headers.Get("Idempotent-Replay"))
			}
			if got := revocations.count(t, computer.ComputerID, old.Lease.AttemptID); got != 2 {
				t.Fatalf("replaying the superseded Job's completion left %d revocations, want 2", got)
			}
			assertJobAndAttemptState(t, h.store, replacement.Job.JobID, replacement.Lease.AttemptID,
				contract.JobRunning, contract.AttemptRunning)
		})
	}
}

// A replay re-drives the completed attempt's revocation -- the retry of a
// revocation that failed after the completion committed (#548) -- including
// after the node re-registered.
func TestCompletionReplayOfCurrentComputerJobStillRevokes(t *testing.T) {
	h, revocations, node, agent := computerCompletionHarness(t)
	computer, _, err := h.store.CreateComputer(t.Context(), CreateComputerRequest{
		Name: "replay-current", Spec: computerCapabilityJobSpec("computer:replay-current:v1"), Actor: "operator"})
	if err != nil {
		t.Fatal(err)
	}
	claim := startComputerAttempt(t, h, node, nil)
	computer, err = h.store.GetComputer(t.Context(), computer.ComputerID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.store.SetComputerDesiredState(t.Context(), computer.ComputerID,
		computerDesiredRequest(computer, contract.ServiceDesiredStopped, "stop")); err != nil {
		t.Fatal(err)
	}
	completion := CompletionRequest{FencingToken: claim.Lease.FencingToken, IdempotencyKey: "completion:" + claim.Lease.AttemptID,
		Result: ProcessResult{OutputError: "logs finalized after positive reap"}, RuntimeQuiescenceEvidence: RuntimeQuiescenceAttempt}
	if headers := postCompletion(t, h, agent, claim, completion); headers.Get("Idempotent-Replay") != "" {
		t.Fatalf("first completion Idempotent-Replay = %q", headers.Get("Idempotent-Replay"))
	}
	if got := revocations.count(t, computer.ComputerID, claim.Lease.AttemptID); got != 1 {
		t.Fatalf("first completion sent %d revocations, want 1", got)
	}
	if headers := postCompletion(t, h, agent, claim, completion); headers.Get("Idempotent-Replay") != "true" {
		t.Fatalf("in-session replay Idempotent-Replay = %q, want true", headers.Get("Idempotent-Replay"))
	}
	if got := revocations.count(t, computer.ComputerID, claim.Lease.AttemptID); got != 2 {
		t.Fatalf("in-session replay left %d revocations, want 2", got)
	}
	reregisterComputerNode(t, h, "boot-computer-node-restarted")
	if headers := postCompletion(t, h, agent, claim, completion); headers.Get("Idempotent-Replay") != "true" {
		t.Fatalf("replay after re-registration Idempotent-Replay = %q, want true", headers.Get("Idempotent-Replay"))
	}
	if got := revocations.count(t, computer.ComputerID, claim.Lease.AttemptID); got != 3 {
		t.Fatalf("replay after re-registration left %d revocations, want 3", got)
	}
}

// grantLedger stands in for the run ledger's Computer grant table with the
// same rules L3 applies: minting revokes the Computer's older grants, a
// revoke_all revokes every grant of the Computer, and an attempt-scoped
// revocation revokes only that attempt's grants (l3/computer_tokens.go). Its
// hold, when armed, parks the next attempt_terminal revocation until released,
// which is the delayed request of the #553 review race.
type grantLedger struct {
	mu       sync.Mutex
	grants   []ledgerGrant
	requests []ComputerTokenRevocation
	hold     chan struct{}
	arrived  chan struct{}
}

type ledgerGrant struct {
	computerID, attemptID string
	revoked               bool
}

func (ledger *grantLedger) mint(computerID, attemptID string) {
	ledger.mu.Lock()
	defer ledger.mu.Unlock()
	for index := range ledger.grants {
		if ledger.grants[index].computerID == computerID {
			ledger.grants[index].revoked = true
		}
	}
	ledger.grants = append(ledger.grants, ledgerGrant{computerID: computerID, attemptID: attemptID})
}

func (ledger *grantLedger) active(computerID, attemptID string) bool {
	ledger.mu.Lock()
	defer ledger.mu.Unlock()
	for _, grant := range ledger.grants {
		if grant.computerID == computerID && grant.attemptID == attemptID && !grant.revoked {
			return true
		}
	}
	return false
}

func (ledger *grantLedger) revoke(_ context.Context, request ComputerTokenRevocation) (contract.ComputerTokenRevocationReceipt, error) {
	ledger.mu.Lock()
	hold, arrived := ledger.hold, ledger.arrived
	if request.Reason == "attempt_terminal" {
		ledger.hold, ledger.arrived = nil, nil
	} else {
		hold, arrived = nil, nil
	}
	ledger.mu.Unlock()
	if hold != nil {
		close(arrived)
		<-hold
	}
	ledger.mu.Lock()
	defer ledger.mu.Unlock()
	ledger.requests = append(ledger.requests, request)
	count := 0
	for index, grant := range ledger.grants {
		if grant.revoked || grant.computerID != request.ComputerID {
			continue
		}
		if request.ComputerAttemptID != "" && grant.attemptID != request.ComputerAttemptID {
			continue
		}
		if request.ComputerAttemptID == "" && !request.RevokeAll {
			continue
		}
		ledger.grants[index].revoked = true
		count++
	}
	return contract.ComputerTokenRevocationReceipt{ComputerID: request.ComputerID, ComputerAttemptID: request.ComputerAttemptID,
		RevokedGrantCount: count, CommittedAt: time.Now()}, nil
}

// #553 review round 2, made deterministic: the completion commits, its
// revocation request is delayed, and meanwhile the reimage activates and the
// replacement attempt is minted a pass. When the delayed revocation arrives it
// must end only the completed attempt's pass: the replacement's grant stays
// active and its scope proof still holds. The same goes for a later replay.
func TestDelayedCompletionRevocationSparesTheReplacementAttemptsPass(t *testing.T) {
	h := newIntegrationHarnessWithOptions(t, StoreOptions{LeaseDuration: 3 * time.Second}, map[string]NodePolicy{
		"computer-node": {Tags: []string{contract.StableNodeTagPrefix + "computer-node"}, MaxOneshotSlots: 1, MaxServiceSlots: 1},
	})
	ledger := &grantLedger{}
	h.server.computerTokenRevoker = recordingComputerTokenRevoker{revoke: ledger.revoke}
	ctx := t.Context()
	node := registerCapabilityNodeWithTags(t, h, "computer-node", computerNodeCapabilities(),
		[]string{contract.StableNodeTagPrefix + "computer-node"})
	agent := h.client(fabric.Identity{NodeID: "fabric-computer-node", Tags: []string{DefaultAgentPrincipalTag}})
	admin := fabric.Identity{FabricID: "fabric-test", UserID: "admin", DeviceID: "device-1"}
	challenge, err := h.store.InitiateAdminBootstrap(ctx)
	if err != nil {
		t.Fatal(err)
	}
	policy, err := h.store.BootstrapAdmin(ctx, admin, challenge.Nonce)
	if err != nil {
		t.Fatal(err)
	}
	computer, _, err := h.store.CreateComputer(ctx, CreateComputerRequest{Name: "delayed-revocation",
		Spec: computerCapabilityJobSpec("computer:delayed-revocation:v1"), Actor: "operator"})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := h.store.MutateComputerSubmission(ctx, admin, computer.ComputerID, ComputerSubmissionRequest{
		PolicyRevision: policy.Revision, SubmitIntentRevision: 0, SubmitEnabled: boolPointer(true),
		IdempotencyKey: "delayed-revocation-enable",
	}); err != nil {
		t.Fatal(err)
	}
	snapshot, err := h.store.IssueComputerPolicySnapshot(ctx, "fabric-computer-node", "fabric-test",
		node.NodeID, node.BootSessionID, time.Minute)
	if err != nil || snapshot == nil {
		t.Fatalf("snapshot = (%#v, %v)", snapshot, err)
	}
	if err := h.store.AcknowledgeComputerPolicyInstallation(ctx, "fabric-computer-node", acknowledgementFor(*snapshot)); err != nil {
		t.Fatal(err)
	}
	mintProven := func(attemptID string) {
		t.Helper()
		if _, err := h.store.ProveComputerTokenScope(ctx, computer.ComputerID, attemptID, "fabric-computer-node", ""); err != nil {
			t.Fatalf("scope proof for attempt %s: %v", attemptID, err)
		}
		ledger.mint(computer.ComputerID, attemptID)
	}

	old := startComputerAttempt(t, h, node, nil)
	mintProven(old.Lease.AttemptID)
	computer, err = h.store.GetComputer(ctx, computer.ComputerID)
	if err != nil {
		t.Fatal(err)
	}
	reimage := ComputerReimageRequest{ComputerMutationPrecondition: computerPrecondition(computer, "operator"),
		Image: reimageTarget('f'), IdempotencyKey: "delayed-revocation-reimage", TerminateSessions: true}
	if staged, err := h.store.ReimageComputer(ctx, computer.ComputerID, reimage); err != nil ||
		staged.ReconfigurationPhase != ComputerReconfigurationReimaging {
		t.Fatalf("staged reimage = %#v err=%v", staged, err)
	}

	// The completion commits; its revocation is held on its way to the ledger.
	hold, arrived := make(chan struct{}), make(chan struct{})
	ledger.mu.Lock()
	ledger.hold, ledger.arrived = hold, arrived
	ledger.mu.Unlock()
	exitCode := 0
	completion := CompletionRequest{FencingToken: old.Lease.FencingToken, IdempotencyKey: "completion:" + old.Lease.AttemptID,
		Result: ProcessResult{ExitCode: &exitCode}, RuntimeQuiescenceEvidence: RuntimeQuiescenceAttempt}
	path := fmt.Sprintf("/v1/agent/jobs/%s/attempts/%s/complete", old.Job.JobID, old.Lease.AttemptID)
	type answer struct {
		status int
		body   []byte
		err    error
	}
	answered := make(chan answer, 1)
	go func() {
		status, _, body, err := doRequest(agent, http.MethodPost, path, completion)
		answered <- answer{status, body, err}
	}()
	select {
	case <-arrived:
	case <-time.After(5 * time.Second):
		t.Fatal("the completion's revocation never reached the ledger")
	}

	// Meanwhile the reimage activates and the replacement is minted a pass.
	acknowledgeReimagePreflight(t, h, node, computer.ComputerID, old.Lease.AttemptID, old.Lease.FencingToken)
	projected, err := h.store.ReimageComputer(ctx, computer.ComputerID, reimage)
	if err != nil || projected.CurrentJobID == old.Job.JobID {
		t.Fatalf("activated reimage = %#v err=%v", projected, err)
	}
	replacement := startComputerAttempt(t, h, node, &reimage.Image)
	mintProven(replacement.Lease.AttemptID)

	// The delayed revocation arrives.
	close(hold)
	result := <-answered
	if result.err != nil || result.status != http.StatusOK {
		t.Fatalf("completion = %d %s err=%v", result.status, result.body, result.err)
	}
	assertReplacementPassSurvives := func(stage string) {
		t.Helper()
		if !ledger.active(computer.ComputerID, replacement.Lease.AttemptID) {
			t.Fatalf("%s: the replacement attempt's pass was revoked; requests = %#v", stage, ledger.requests)
		}
		if _, err := h.store.ProveComputerTokenScope(ctx, computer.ComputerID, replacement.Lease.AttemptID,
			"fabric-computer-node", ""); err != nil {
			t.Fatalf("%s: replacement scope proof = %v", stage, err)
		}
		if ledger.active(computer.ComputerID, old.Lease.AttemptID) {
			t.Fatalf("%s: the completed attempt's pass is still active", stage)
		}
		if _, err := h.store.ProveComputerTokenScope(ctx, computer.ComputerID, old.Lease.AttemptID,
			"fabric-computer-node", ""); errorCode(err) != contract.ErrorForbidden {
			t.Fatalf("%s: completed attempt scope proof = %v, want forbidden", stage, err)
		}
		ledger.mu.Lock()
		defer ledger.mu.Unlock()
		for _, request := range ledger.requests {
			if request.ComputerAttemptID != old.Lease.AttemptID || request.RevokeAll {
				t.Fatalf("%s: completion sent %#v, want only the completed attempt revoked", stage, request)
			}
		}
	}
	assertReplacementPassSurvives("delayed revocation")

	// A later replay of the old completion re-drives the same scoped request.
	status, headers, body := h.do(agent, http.MethodPost, path, completion)
	if status != http.StatusOK || headers.Get("Idempotent-Replay") != "true" {
		t.Fatalf("replay = %d %s (Idempotent-Replay %q)", status, body, headers.Get("Idempotent-Replay"))
	}
	assertReplacementPassSurvives("replay")
	if len(ledger.requests) != 2 {
		t.Fatalf("ledger saw %d revocations, want 2", len(ledger.requests))
	}
}
