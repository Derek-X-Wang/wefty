package l1

import (
	"context"
	"encoding/json"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
)

func TestClaimRequiresClassSelector(t *testing.T) {
	assertClaimRequiresClassSelector(t)
}

func assertClaimRequiresClassSelector(t *testing.T) {
	t.Helper()
	h := newIntegrationHarness(t, map[string][]string{"node-1": {"linux"}})
	agent := h.client(fabric.Identity{NodeID: "agent", Tags: []string{DefaultAgentPrincipalTag}})
	h.register(agent, "node-1")

	for _, class := range []string{"", "scheduled"} {
		t.Run(class, func(t *testing.T) {
			status, _, body := h.do(agent, http.MethodPost, "/v1/agent/jobs/claim", ClaimRequest{
				NodeID:        "node-1",
				BootSessionID: "boot-node-1",
				Class:         class,
			})
			assertAPIError(t, status, body, http.StatusBadRequest, contract.ErrorInvalidRequest)
		})
	}
}

func TestClaimSelectorsSeparateWorkloadClasses(t *testing.T) {
	assertClaimSelectorsSeparateWorkloadClasses(t)
}

func assertClaimSelectorsSeparateWorkloadClasses(t *testing.T) {
	t.Helper()
	t.Run("one-shot excludes older service", func(t *testing.T) {
		h := newIntegrationHarness(t, map[string][]string{"node-1": {"linux"}})
		client := h.client(fabric.Identity{NodeID: "client", Tags: []string{DefaultClientPrincipalTag}})
		agent := h.client(fabric.Identity{NodeID: "agent", Tags: []string{DefaultAgentPrincipalTag}})
		node := h.register(agent, "node-1")
		service := submitRestartService(t, h, client, "older-service", []string{"linux"}, nil)
		h.clock.Advance(time.Nanosecond)
		oneshot := h.submit(client, "younger-one-shot", []string{"linux"})

		claim := claimClass(t, h, agent, node, contract.JobClassOneShot)
		if claim.Job.JobID != oneshot.JobID {
			t.Fatalf("one-shot claim job = %q, want %q; older service %q must be excluded", claim.Job.JobID, oneshot.JobID, service.JobID)
		}
	})

	t.Run("service excludes older one-shot and binds atomically", func(t *testing.T) {
		h := newIntegrationHarness(t, map[string][]string{"node-1": {"linux"}})
		client := h.client(fabric.Identity{NodeID: "client", Tags: []string{DefaultClientPrincipalTag}})
		agent := h.client(fabric.Identity{NodeID: "agent", Tags: []string{DefaultAgentPrincipalTag}})
		node := h.register(agent, "node-1")
		oneshot := h.submit(client, "older-one-shot", []string{"linux"})
		h.clock.Advance(time.Nanosecond)
		service := submitRestartService(t, h, client, "younger-service", []string{"linux"}, nil)

		claim := claimClass(t, h, agent, node, contract.JobClassService)
		if claim.Job.JobID != service.JobID {
			t.Fatalf("service claim job = %q, want %q; older one-shot %q must be excluded", claim.Job.JobID, service.JobID, oneshot.JobID)
		}
		bound := getRestartService(t, h, service.JobID)
		if bound.BoundNodeID != node.NodeID {
			t.Fatalf("service binding = %q, want %q after first claim", bound.BoundNodeID, node.NodeID)
		}
	})
}

func TestConcurrentClassClaimsStopAtConfiguredCapacity(t *testing.T) {
	assertConcurrentClassClaimsStopAtConfiguredCapacity(t)
}

func assertConcurrentClassClaimsStopAtConfiguredCapacity(t *testing.T) {
	t.Helper()
	for _, class := range []string{contract.JobClassOneShot, contract.JobClassService} {
		t.Run(class, func(t *testing.T) {
			h := newIntegrationHarnessWithPolicies(t, map[string]NodePolicy{
				"node-1": {Tags: []string{"linux"}, MaxOneshotSlots: 2, MaxServiceSlots: 2},
			})
			client := h.client(fabric.Identity{NodeID: "client", Tags: []string{DefaultClientPrincipalTag}})
			agent := h.client(fabric.Identity{NodeID: "agent", Tags: []string{DefaultAgentPrincipalTag}})
			node := h.register(agent, "node-1")
			for index := range 3 {
				dispatchKey := class + "-saturation-" + string(rune('a'+index))
				if class == contract.JobClassService {
					submitRestartService(t, h, client, dispatchKey, []string{"linux"}, nil)
				} else {
					h.submit(client, dispatchKey, []string{"linux"})
				}
			}

			start := make(chan struct{})
			claims := make(chan *Claim, 3)
			errors := make(chan error, 3)
			var wait sync.WaitGroup
			for range 3 {
				wait.Add(1)
				go func() {
					defer wait.Done()
					<-start
					claim, err := h.store.ClaimJob(context.Background(), "agent", node.NodeID, node.BootSessionID, class)
					claims <- claim
					errors <- err
				}()
			}
			close(start)
			wait.Wait()
			close(claims)
			close(errors)

			for err := range errors {
				if err != nil {
					t.Fatal(err)
				}
			}
			claimedIDs := map[string]struct{}{}
			empty := 0
			for claim := range claims {
				if claim == nil {
					empty++
					continue
				}
				claimedIDs[claim.Job.JobID] = struct{}{}
			}
			if len(claimedIDs) != 2 || empty != 1 {
				t.Fatalf("concurrent %s claims = %d claimed/%d empty, want 2/1", class, len(claimedIDs), empty)
			}
			next, err := h.store.ClaimJob(context.Background(), "agent", node.NodeID, node.BootSessionID, class)
			if err != nil {
				t.Fatal(err)
			}
			if next != nil {
				t.Fatalf("%s capacity admitted N+1th claim: %#v", class, next)
			}
		})
	}
}

func TestServiceClaimEligibilityStaysInsideFIFOSelection(t *testing.T) {
	assertServiceClaimEligibilityStaysInsideFIFOSelection(t)
}

func assertServiceClaimEligibilityStaysInsideFIFOSelection(t *testing.T) {
	t.Helper()
	t.Run("bound elsewhere does not head-of-line block", func(t *testing.T) {
		h := newIntegrationHarnessWithPolicies(t, map[string]NodePolicy{
			"node-1": {Tags: []string{"linux"}, MaxOneshotSlots: 1, MaxServiceSlots: 1},
			"node-2": {Tags: []string{"linux"}, MaxOneshotSlots: 1, MaxServiceSlots: 1},
		})
		client := h.client(fabric.Identity{NodeID: "client", Tags: []string{DefaultClientPrincipalTag}})
		agent1 := h.client(fabric.Identity{NodeID: "agent-1", Tags: []string{DefaultAgentPrincipalTag}})
		agent2 := h.client(fabric.Identity{NodeID: "agent-2", Tags: []string{DefaultAgentPrincipalTag}})
		node1 := h.register(agent1, "node-1")
		node2 := h.register(agent2, "node-2")
		blocked := submitRestartService(t, h, client, "oldest-bound-elsewhere", []string{"linux"}, nil)
		eligible := submitRestartService(t, h, client, "younger-unbound", []string{"linux"}, nil)
		if _, err := h.store.db.Exec("UPDATE service_jobs SET bound_node_id=? WHERE job_id=?", node2.NodeID, blocked.JobID); err != nil {
			t.Fatal(err)
		}

		claim := claimClass(t, h, agent1, node1, contract.JobClassService)
		if claim.Job.JobID != eligible.JobID {
			t.Fatalf("service claim = %q, want eligible %q past bound-elsewhere %q", claim.Job.JobID, eligible.JobID, blocked.JobID)
		}
	})

	t.Run("due binding outranks older capacity-blocked newcomer", func(t *testing.T) {
		h := newIntegrationHarnessWithPolicies(t, map[string]NodePolicy{
			"node-1": {Tags: []string{"linux"}, MaxOneshotSlots: 1, MaxServiceSlots: 1},
		})
		client := h.client(fabric.Identity{NodeID: "client", Tags: []string{DefaultClientPrincipalTag}})
		agent := h.client(fabric.Identity{NodeID: "agent", Tags: []string{DefaultAgentPrincipalTag}})
		node := h.register(agent, "node-1")
		newcomer := submitRestartService(t, h, client, "older-unbound-newcomer", []string{"linux"}, nil)
		bound := submitRestartService(t, h, client, "younger-due-binding", []string{"linux"}, nil)
		if _, err := h.store.db.Exec("UPDATE service_jobs SET bound_node_id=?, next_restart_at=? WHERE job_id=?", node.NodeID, h.clock.Now().UnixNano(), bound.JobID); err != nil {
			t.Fatal(err)
		}
		if _, err := h.store.db.Exec("UPDATE nodes SET max_service_slots=0 WHERE node_id=?", node.NodeID); err != nil {
			t.Fatal(err)
		}

		claim := claimClass(t, h, agent, node, contract.JobClassService)
		if claim.Job.JobID != bound.JobID {
			t.Fatalf("service claim = %q, want due bound service %q before older newcomer %q", claim.Job.JobID, bound.JobID, newcomer.JobID)
		}
		queued, err := h.store.GetJob(context.Background(), newcomer.JobID)
		if err != nil {
			t.Fatal(err)
		}
		if queued.State != contract.JobQueued {
			t.Fatalf("capacity-blocked newcomer state = %q, want queued", queued.State)
		}
		status, _, body := h.do(agent, http.MethodPost, "/v1/agent/jobs/claim", ClaimRequest{
			NodeID: node.NodeID, BootSessionID: node.BootSessionID, Class: contract.JobClassService,
		})
		if status != http.StatusNoContent {
			t.Fatalf("overcommitted node admitted newcomer status = %d body=%s, want 204", status, body)
		}
	})
}

func TestBoundServiceRestartsWhileOneShotCapacityIsFull(t *testing.T) {
	assertBoundServiceRestartsWhileOneShotCapacityIsFull(t)
}

func assertBoundServiceRestartsWhileOneShotCapacityIsFull(t *testing.T) {
	t.Helper()
	h := newIntegrationHarnessWithOptions(t, StoreOptions{
		Jitter: func(delay time.Duration) time.Duration { return delay },
	}, map[string]NodePolicy{
		"node-1": {Tags: []string{"linux"}, MaxOneshotSlots: 2, MaxServiceSlots: 1},
	})
	client := h.client(fabric.Identity{NodeID: "client", Tags: []string{DefaultClientPrincipalTag}})
	agent := h.client(fabric.Identity{NodeID: "agent", Tags: []string{DefaultAgentPrincipalTag}})
	node := h.register(agent, "node-1")
	service := submitRestartService(t, h, client, "bound-restart", []string{"linux"}, nil)
	first := claimClass(t, h, agent, node, contract.JobClassService)
	exitCode := 1
	path := "/v1/agent/jobs/" + service.JobID + "/attempts/" + first.Lease.AttemptID + "/complete"
	status, _, body := h.do(agent, http.MethodPost, path, CompletionRequest{
		FencingToken: first.Lease.FencingToken, IdempotencyKey: "bound-restart-exit",
		Result: ProcessResult{ExitCode: &exitCode},
	})
	if status != http.StatusOK {
		t.Fatalf("complete bound service status = %d body=%s", status, body)
	}

	for index := range 3 {
		h.submit(client, "one-shot-"+string(rune('a'+index)), []string{"linux"})
	}
	claimClass(t, h, agent, node, contract.JobClassOneShot)
	claimClass(t, h, agent, node, contract.JobClassOneShot)
	status, _, body = h.do(agent, http.MethodPost, "/v1/agent/jobs/claim", ClaimRequest{
		NodeID: node.NodeID, BootSessionID: node.BootSessionID, Class: contract.JobClassOneShot,
	})
	if status != http.StatusNoContent {
		t.Fatalf("N+1th one-shot claim status = %d body=%s, want 204", status, body)
	}

	h.clock.Advance(time.Second)
	restarted := claimClass(t, h, agent, node, contract.JobClassService)
	if restarted.Job.JobID != service.JobID || restarted.Lease.AttemptID == first.Lease.AttemptID {
		t.Fatalf("bound restart = job:%q attempt:%q, want job %q with fresh attempt", restarted.Job.JobID, restarted.Lease.AttemptID, service.JobID)
	}
}

func claimClass(t *testing.T, h *integrationHarness, agent *http.Client, node Node, class string) Claim {
	t.Helper()
	status, _, body := h.do(agent, http.MethodPost, "/v1/agent/jobs/claim", ClaimRequest{
		NodeID: node.NodeID, BootSessionID: node.BootSessionID, Class: class,
	})
	if status != http.StatusOK {
		t.Fatalf("claim class %q status = %d body=%s", class, status, body)
	}
	var claim Claim
	if err := json.Unmarshal(body, &claim); err != nil {
		t.Fatal(err)
	}
	return claim
}
