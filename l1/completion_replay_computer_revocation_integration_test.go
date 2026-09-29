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

func (log *revocationLog) count(t *testing.T, computerID string) int {
	t.Helper()
	log.mu.Lock()
	defer log.mu.Unlock()
	for _, call := range log.calls {
		if call.ComputerID != computerID || !call.RevokeAll || call.Reason != "attempt_terminal" {
			t.Fatalf("unexpected Computer token revocation %#v", call)
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
// re-registered -- must not send the Computer-wide revocation, which would
// revoke the replacement attempt's live tokens.
func TestCompletionReplayAfterReimageNeverRevokesTheReplacementAttempt(t *testing.T) {
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
			// The first completion ends the Computer's current attempt: revoked.
			if got := revocations.count(t, computer.ComputerID); got != 1 {
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
			if got := revocations.count(t, computer.ComputerID); got != 1 {
				t.Fatalf("replaying the superseded Job's completion sent %d revocations in total, want still 1", got)
			}
			assertJobAndAttemptState(t, h.store, replacement.Job.JobID, replacement.Lease.AttemptID,
				contract.JobRunning, contract.AttemptRunning)
		})
	}
}

// The revocation a replay re-drives is still sent while the completed Job is
// the Computer's current Job -- the retry of a revocation that failed after
// the completion committed (#548) -- including after the node re-registered.
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
	if got := revocations.count(t, computer.ComputerID); got != 1 {
		t.Fatalf("first completion sent %d revocations, want 1", got)
	}
	if headers := postCompletion(t, h, agent, claim, completion); headers.Get("Idempotent-Replay") != "true" {
		t.Fatalf("in-session replay Idempotent-Replay = %q, want true", headers.Get("Idempotent-Replay"))
	}
	if got := revocations.count(t, computer.ComputerID); got != 2 {
		t.Fatalf("in-session replay left %d revocations, want 2", got)
	}
	reregisterComputerNode(t, h, "boot-computer-node-restarted")
	if headers := postCompletion(t, h, agent, claim, completion); headers.Get("Idempotent-Replay") != "true" {
		t.Fatalf("replay after re-registration Idempotent-Replay = %q, want true", headers.Get("Idempotent-Replay"))
	}
	if got := revocations.count(t, computer.ComputerID); got != 3 {
		t.Fatalf("replay after re-registration left %d revocations, want 3", got)
	}
}
