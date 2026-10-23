package l1

import (
	"context"
	"encoding/json"
	"net/http"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
)

type generationFixture struct {
	h         *integrationHarness
	node      Node
	inventory NodeGenerationInventory
}

func generationRegistration(h *integrationHarness, id string) contract.NodeRegistration {
	return contract.NodeRegistration{NodeID: "node", AgentGenerationID: id, BootSessionID: "boot-" + id, OS: "darwin", Architecture: "arm64", AgentVersion: id, Capabilities: map[string]bool{"kind:process": true, "upgrade:retirement:v1": true}, CapabilityRevision: 1, CapabilityObservedAt: h.clock.Now(), MissingCapabilities: []string{}}
}
func newGenerationFixture(t *testing.T) *generationFixture {
	t.Helper()
	h := newIntegrationHarnessWithReconcileInterval(t, StoreOptions{}, map[string]NodePolicy{"node": {MaxOneshotSlots: 3, MaxServiceSlots: 2}}, true, time.Hour)
	h.stopServer()
	node, err := h.store.RegisterNode(t.Context(), fabric.Identity{NodeID: "agent"}, generationRegistration(h, "g1"), NodePolicy{MaxOneshotSlots: 3, MaxServiceSlots: 2}, true)
	if err != nil {
		t.Fatal(err)
	}
	if node.AgentGenerationID != "g1" {
		t.Fatalf("registered Node omitted its primary generation: %+v", node)
	}
	f := &generationFixture{h: h, node: node}
	f.refresh(t)
	return f
}
func (f *generationFixture) refresh(t *testing.T) {
	t.Helper()
	var err error
	f.inventory, err = f.h.store.NodeGenerations(t.Context(), "agent", "node")
	if err != nil {
		t.Fatal(err)
	}
	f.node, err = f.h.store.GetNode(t.Context(), "node")
	if err != nil {
		t.Fatal(err)
	}
}
func (f *generationFixture) intent(t *testing.T, enabled bool) {
	t.Helper()
	var err error
	f.node, err = f.h.store.SetNodeClaimsByOperator(t.Context(), "node", "operator", NodeIntentRequest{ClaimsEnabled: enabled, IntentRevision: f.node.IntentRevision, Reason: "upgrade"})
	if err != nil {
		t.Fatal(err)
	}
}
func (f *generationFixture) claim(t *testing.T, key string) *Claim {
	t.Helper()
	if _, _, err := f.h.store.CreateJobAs(t.Context(), validJobSpec(key, nil), JobOrigin{OriginatingSubmitter: "operator"}); err != nil {
		t.Fatal(err)
	}
	c, err := f.h.store.ClaimJob(t.Context(), "agent", "node", f.node.BootSessionID, contract.JobClassOneShot)
	if err != nil || c == nil {
		t.Fatalf("claim=%v err=%v", c, err)
	}
	return c
}
func (f *generationFixture) prepare(t *testing.T, id string) PromoteNodeGenerationRequest {
	t.Helper()
	f.intent(t, false)
	var err error
	f.inventory, err = f.h.store.StageNodeGeneration(t.Context(), "agent", generationRegistration(f.h, id))
	if err != nil {
		t.Fatal(err)
	}
	request := PromoteNodeGenerationRequest{OperationID: "promote-" + id, CandidateGenerationID: id, CandidateBootSessionID: "boot-" + id, ExpectedCatalogRevision: f.inventory.CatalogRevision, CatalogRevision: f.inventory.CatalogRevision + 1, IntentRevision: f.node.IntentRevision, SelectedProtocol: 1, ResidentAttempts: []ResidentAttempt{}, RetiringGenerationIDs: []string{}}
	for _, g := range f.inventory.Generations {
		if g.Role == NodeGenerationPrimary {
			request.ExpectedPrimaryGenerationID = g.AgentGenerationID
			request.ExpectedPrimaryBootSessionID = g.BootSessionID
			request.ExpectedAuthorityGeneration = g.AuthorityGeneration
			request.ResidentAttempts = g.ResidentAttempts
		}
		if g.Role == NodeGenerationRetiring {
			request.RetiringGenerationIDs = append(request.RetiringGenerationIDs, g.AgentGenerationID)
		}
	}
	return request
}
func (f *generationFixture) promote(t *testing.T, id string) PromoteNodeGenerationRequest {
	t.Helper()
	request := f.prepare(t, id)
	inventory, err := f.h.store.PromoteNodeGeneration(t.Context(), "agent", "node", request)
	if err != nil {
		t.Fatal(err)
	}
	f.refresh(t)
	if !reflect.DeepEqual(inventory, f.inventory) {
		t.Fatalf("promotion differs from committed inventory")
	}
	return request
}
func completeGenerationAttempt(t *testing.T, f *generationFixture, c *Claim) {
	t.Helper()
	exit := 0
	_, err := f.h.store.CompleteAttempt(t.Context(), "agent", c.Job.JobID, c.Lease.AttemptID, CompletionRequest{FencingToken: c.Lease.FencingToken, IdempotencyKey: "done-" + c.Lease.AttemptID, Result: ProcessResult{ExitCode: &exit}})
	if err != nil {
		t.Fatal(err)
	}
}

func TestNodeGenerationAtomicPromotionRetryAndThreeGenerations(t *testing.T) {
	f := newGenerationFixture(t)
	a := f.claim(t, "a")
	request := f.prepare(t, "g2")
	before := f.inventory
	for _, mutate := range []func(*PromoteNodeGenerationRequest){func(r *PromoteNodeGenerationRequest) { r.ExpectedAuthorityGeneration++ }, func(r *PromoteNodeGenerationRequest) { r.ResidentAttempts = nil }, func(r *PromoteNodeGenerationRequest) { r.IntentRevision++ }, func(r *PromoteNodeGenerationRequest) { r.ExpectedCatalogRevision++; r.CatalogRevision++ }, func(r *PromoteNodeGenerationRequest) { r.CandidateBootSessionID = "wrong" }} {
		bad := request
		mutate(&bad)
		if _, err := f.h.store.PromoteNodeGeneration(t.Context(), "agent", "node", bad); err == nil {
			t.Fatal("stale fence accepted")
		}
		f.refresh(t)
		if !reflect.DeepEqual(before, f.inventory) {
			t.Fatal("refused promotion changed inventory")
		}
	}
	var wg sync.WaitGroup
	results := make(chan NodeGenerationInventory, 2)
	failures := make(chan error, 2)
	for range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			v, err := f.h.store.PromoteNodeGeneration(t.Context(), "agent", "node", request)
			results <- v
			failures <- err
		}()
	}
	wg.Wait()
	close(results)
	close(failures)
	for err := range failures {
		if err != nil {
			t.Fatal(err)
		}
	}
	var first NodeGenerationInventory
	for v := range results {
		if first.NodeID == "" {
			first = v
		} else if !reflect.DeepEqual(first, v) {
			t.Fatal("retry changed receipt")
		}
	}
	f.refresh(t)
	if f.inventory.CatalogRevision != 1 {
		t.Fatal(f.inventory)
	}
	f.intent(t, true)
	b := f.claim(t, "b")
	f.promote(t, "g3")
	f.intent(t, true)
	c := f.claim(t, "c")
	f.refresh(t)
	if f.node.AgentGenerationID != "g3" {
		t.Fatal("Node projection did not follow committed primary")
	}
	if len(f.inventory.Generations) != 3 {
		t.Fatal(f.inventory)
	}
	for _, g := range f.inventory.Generations {
		if len(g.ResidentAttempts) != 1 {
			t.Fatalf("exact residents=%+v", g)
		}
		if g.AgentGenerationID != "g3" && g.Role != NodeGenerationRetiring {
			t.Fatal(g)
		}
	}
	for _, c := range []*Claim{a, b, c} {
		if _, err := f.h.store.RenewLease(t.Context(), "agent", c.Job.JobID, c.Lease.AttemptID, c.Lease.FencingToken); err != nil {
			t.Fatal(err)
		}
	}
	retry, err := f.h.store.PromoteNodeGeneration(t.Context(), "agent", "node", request)
	if err != nil || !reflect.DeepEqual(retry, first) {
		t.Fatalf("original receipt lost after further promotion: %v", err)
	}
	stale := request
	stale.OperationID = "stale"
	if _, err = f.h.store.PromoteNodeGeneration(t.Context(), "agent", "node", stale); err == nil {
		t.Fatal("stale promotion accepted")
	}
	changed := request
	changed.CandidateBootSessionID = "other"
	if _, err = f.h.store.PromoteNodeGeneration(t.Context(), "agent", "node", changed); err == nil {
		t.Fatal("reused operation accepted different body")
	}
	f.refresh(t)
	if f.inventory.CatalogRevision != 2 {
		t.Fatal("retry mutated cohort")
	}
}

func TestNodeGenerationRetiringAuthorityAndSharedSlots(t *testing.T) {
	f := newGenerationFixture(t)
	a := f.claim(t, "a")
	f.promote(t, "g2")
	f.intent(t, true)
	b := f.claim(t, "b")
	f.promote(t, "g3")
	f.intent(t, true)
	f.claim(t, "c")
	_, _, err := f.h.store.CreateJob(t.Context(), validJobSpec("queued", nil))
	if err != nil {
		t.Fatal(err)
	}
	if c, err := f.h.store.ClaimJob(t.Context(), "agent", "node", "boot-g3", contract.JobClassOneShot); err != nil || c != nil {
		t.Fatalf("shared Slots over-admitted: %v %v", c, err)
	}
	for _, id := range []string{"g1", "g2"} {
		if _, err := f.h.store.ClaimJob(t.Context(), "agent", "node", "boot-"+id, contract.JobClassOneShot); errorCode(err) != contract.ErrorNodeSessionReplaced {
			t.Fatal(err)
		}
		for _, boot := range []string{"boot-" + id, "restarted-" + id} {
			r := generationRegistration(f.h, id)
			r.BootSessionID = boot
			r.AgentVersion = "overwrite"
			if _, err := f.h.store.RegisterNode(t.Context(), fabric.Identity{NodeID: "agent"}, r, NodePolicy{MaxOneshotSlots: 99}, true); errorCode(err) != contract.ErrorNodeSessionReplaced {
				t.Fatalf("retiree registration: %v", err)
			}
		}
		if _, err := f.h.store.HeartbeatNodeWithCapabilityObservation(t.Context(), "agent", "node", "boot-"+id, contract.CapabilityObservation{Capabilities: map[string]bool{"kind:oci": true}, Revision: 99, ObservedAt: f.h.clock.Now(), MissingCapabilities: []string{}}, NodePolicy{MaxOneshotSlots: 99}); errorCode(err) != contract.ErrorNodeSessionReplaced {
			t.Fatal(err)
		}
		if _, err := f.h.store.DrainNode(t.Context(), "agent", "node", "boot-"+id); errorCode(err) != contract.ErrorNodeSessionReplaced {
			t.Fatal(err)
		}
	}
	f.refresh(t)
	old := f.node
	f.h.clock.Advance(time.Second)
	if _, err := f.h.store.HeartbeatRetiringNodeGeneration(t.Context(), "agent", "node", "g1", "boot-g1"); err != nil {
		t.Fatal(err)
	}
	f.refresh(t)
	if !reflect.DeepEqual(old, f.node) {
		t.Fatal("retiring heartbeat changed Node facts")
	}
	for _, c := range []*Claim{a, b} {
		if _, err := f.h.store.RenewLease(t.Context(), "agent", c.Job.JobID, c.Lease.AttemptID, c.Lease.FencingToken); err != nil {
			t.Fatal(err)
		}
		scope, err := f.h.store.ResolveAttemptCredential(t.Context(), c.AttemptToken, "agent")
		if err != nil {
			t.Fatal(err)
		}
		if _, _, err = f.h.store.CreateJobAs(t.Context(), validJobSpec("child-"+c.Lease.AttemptID, nil), JobOrigin{Parent: &scope}); err != nil {
			t.Fatalf("retiring child submission: %v", err)
		}
		completeGenerationAttempt(t, f, c)
		if _, err = f.h.store.ResolveAttemptCredential(t.Context(), c.AttemptToken, "agent"); errorCode(err) != contract.ErrorUnauthorized {
			t.Fatalf("terminal credential: %v", err)
		}
	}
	if c, err := f.h.store.ClaimJob(t.Context(), "agent", "node", "boot-g3", contract.JobClassOneShot); err != nil || c == nil {
		t.Fatalf("completion did not release shared Slot: %v %v", c, err)
	}
}

func TestNodeGenerationClosureRequiresEveryResident(t *testing.T) {
	for _, finish := range []string{"accepted", "expired", "cancel_deadline", "lost"} {
		t.Run(finish, func(t *testing.T) {
			f := newGenerationFixture(t)
			a := f.claim(t, "a")
			b := f.claim(t, "b")
			f.promote(t, "g2")
			request := CloseNodeGenerationRequest{BootSessionID: "boot-g1", PromotionOperationID: "promote-g2", RuntimesReaped: true, EvidenceSettled: true}
			for _, g := range f.inventory.Generations {
				if g.AgentGenerationID == "g1" {
					request.ResidentAttempts = g.ResidentAttempts
				}
			}
			assertBlocked := func() {
				t.Helper()
				if ready, err := f.h.store.NodeGenerationClosureReady(t.Context(), "agent", "node", "g1"); err != nil || ready {
					t.Fatalf("premature closure prerequisite: %t %v", ready, err)
				}
				if _, err := f.h.store.CloseNodeGeneration(t.Context(), "agent", "node", "g1", request); err == nil {
					t.Fatal("custody/reap assertion accepted without L1 settlement")
				}
			}
			assertBlocked()
			completeGenerationAttempt(t, f, a)
			assertBlocked()
			switch finish {
			case "accepted":
				completeGenerationAttempt(t, f, b)
			case "expired":
				f.h.clock.Advance(DefaultLeaseDuration)
			case "cancel_deadline", "lost":
				f.h.store.leaseDuration = 2 * time.Minute
				lease, err := f.h.store.RenewLease(t.Context(), "agent", b.Job.JobID, b.Lease.AttemptID, b.Lease.FencingToken)
				if err != nil {
					t.Fatal(err)
				}
				if _, err = f.h.store.CancelJob(t.Context(), b.Job.JobID, JobCancelCaller{Submitter: "operator"}); err != nil {
					t.Fatal(err)
				}
				f.h.clock.Advance(CancelSettlementTimeout)
				if !lease.LeaseExpires.After(f.h.clock.Now()) {
					t.Fatal("test requires cancellation deadline before absolute Lease expiry")
				}
				if finish == "lost" {
					if _, err = f.h.store.Reconcile(t.Context()); err != nil {
						t.Fatal(err)
					}
					a, err := readAttemptAuthority(t.Context(), f.h.store.db, b.Lease.AttemptID)
					if err != nil || a.state != contract.AttemptLost || !a.leaseExpires.After(f.h.clock.Now()) {
						t.Fatalf("production cancellation settlement: %+v %v", a, err)
					}
				}

			}
			if ready, err := f.h.store.NodeGenerationClosureReady(t.Context(), "agent", "node", "g1"); err != nil || !ready {
				t.Fatalf("closure prerequisite: %t %v", ready, err)
			}
			for _, mutate := range []func(*CloseNodeGenerationRequest){func(r *CloseNodeGenerationRequest) { r.RuntimesReaped = false }, func(r *CloseNodeGenerationRequest) { r.EvidenceSettled = false }, func(r *CloseNodeGenerationRequest) { r.ResidentAttempts = nil }, func(r *CloseNodeGenerationRequest) { r.PromotionOperationID = "wrong" }} {
				bad := request
				mutate(&bad)
				if _, err := f.h.store.CloseNodeGeneration(t.Context(), "agent", "node", "g1", bad); err == nil {
					t.Fatal("unproven closure accepted")
				}
			}
			closed, err := f.h.store.CloseNodeGeneration(t.Context(), "agent", "node", "g1", request)
			if err != nil {
				t.Fatal(err)
			}
			again, err := f.h.store.CloseNodeGeneration(t.Context(), "agent", "node", "g1", request)
			if err != nil || !reflect.DeepEqual(closed, again) {
				t.Fatal("closure replay changed result", err)
			}
			if _, err := f.h.store.HeartbeatRetiringNodeGeneration(t.Context(), "agent", "node", "g1", "boot-g1"); err == nil {
				t.Fatal("closed session reopened")
			}
			if _, err := f.h.store.RegisterNode(t.Context(), fabric.Identity{NodeID: "agent"}, generationRegistration(f.h, "g1"), DefaultNodePolicy(), true); err == nil {
				t.Fatal("closed session became primary")
			}
			// O/R retain their existing predicates even after the session closes.
			exit := 0
			if _, err := f.h.store.CompleteAttempt(t.Context(), "agent", a.Job.JobID, a.Lease.AttemptID, CompletionRequest{FencingToken: a.Lease.FencingToken, IdempotencyKey: "done-" + a.Lease.AttemptID, Result: ProcessResult{ExitCode: &exit}}); err != nil {
				t.Fatalf("accepted completion replay after closure: %v", err)
			}
		})
	}
}

func TestNodeGenerationUnknownPredecessorKeepsEmbargo(t *testing.T) {
	f := newGenerationFixture(t)
	unknown := f.claim(t, "unknown")
	r := generationRegistration(f.h, "g1")
	r.BootSessionID = "restart-g1"
	var err error
	f.node, err = f.h.store.RegisterNode(t.Context(), fabric.Identity{NodeID: "agent"}, r, NodePolicy{MaxOneshotSlots: 3}, true)
	if err != nil {
		t.Fatal(err)
	}
	f.promote(t, "g2")
	f.intent(t, true)
	_, _, err = f.h.store.CreateJob(t.Context(), validJobSpec("queued", nil))
	if err != nil {
		t.Fatal(err)
	}
	if c, err := f.h.store.ClaimJob(t.Context(), "agent", "node", f.node.BootSessionID, contract.JobClassOneShot); err != nil || c != nil {
		t.Fatalf("unknown predecessor escaped embargo: %v %v", c, err)
	}
	if _, err := f.h.store.RenewLease(t.Context(), "agent", unknown.Job.JobID, unknown.Lease.AttemptID, unknown.Lease.FencingToken); errorCode(err) != contract.ErrorNodeSessionReplaced {
		t.Fatal(err)
	}
	if _, err := f.h.store.ResolveAttemptCredential(t.Context(), unknown.AttemptToken, "agent"); errorCode(err) != contract.ErrorUnauthorized {
		t.Fatal(err)
	}
	// Even a corrupted membership with a mismatched original fence grants nothing.
	if _, err = f.h.store.db.ExecContext(t.Context(), `INSERT INTO node_generation_residents(node_id,generation_id,attempt_id,job_id,boot_session_id,authority_generation,fencing_token) VALUES('node','g1',?,?,?,?,?)`, unknown.Lease.AttemptID, unknown.Job.JobID, "boot-g1", 1, "wrong"); err != nil {
		t.Fatal(err)
	}
	if _, err := f.h.store.RenewLease(t.Context(), "agent", unknown.Job.JobID, unknown.Lease.AttemptID, unknown.Lease.FencingToken); errorCode(err) != contract.ErrorNodeSessionReplaced {
		t.Fatal(err)
	}
}

func TestNodeGenerationLegacyRegistrationUnchanged(t *testing.T) {
	h := newIntegrationHarness(t, map[string][]string{"node": nil})
	h.stopServer()
	r := generationRegistration(h, "g1")
	r.AgentGenerationID = ""
	r.Capabilities = map[string]bool{"kind:process": true}
	register := func() {
		t.Helper()
		if _, err := h.store.RegisterNode(t.Context(), fabric.Identity{NodeID: "agent"}, r, DefaultNodePolicy(), true); err != nil {
			t.Fatal(err)
		}
	}
	register()
	_, _, err := h.store.CreateJob(t.Context(), validJobSpec("legacy", nil))
	if err != nil {
		t.Fatal(err)
	}
	claim, err := h.store.ClaimJob(t.Context(), "agent", "node", r.BootSessionID, contract.JobClassOneShot)
	if err != nil || claim == nil {
		t.Fatal(claim, err)
	}
	register() // Legacy same-boot registration still advances the authority fence.
	if _, err := h.store.RenewLease(t.Context(), "agent", claim.Job.JobID, claim.Lease.AttemptID, claim.Lease.FencingToken); errorCode(err) != contract.ErrorNodeSessionReplaced {
		t.Fatal(err)
	}
	inventory, err := h.store.NodeGenerations(t.Context(), "agent", "node")
	if err != nil || len(inventory.Generations) != 0 {
		t.Fatal(inventory, err)
	}
	// Preserve the legacy boot-only embargo even though re-registration
	// fenced the earlier Attempt's authority counter.
	if _, _, err := h.store.CreateJob(t.Context(), validJobSpec("legacy-same-boot-claim", nil)); err != nil {
		t.Fatal(err)
	}
	if c, err := h.store.ClaimJob(t.Context(), "agent", "node", r.BootSessionID, contract.JobClassOneShot); err != nil || c == nil {
		t.Fatalf("legacy same-boot claim changed: %+v %v", c, err)
	}
	r.AgentGenerationID = "g1"
	if _, err := h.store.RegisterNode(t.Context(), fabric.Identity{NodeID: "agent"}, r, DefaultNodePolicy(), true); err == nil {
		t.Fatal("adopted legacy live work")
	}
}

func TestNodeGenerationProtocolPromisesAndSameBootRejoin(t *testing.T) {
	f := newGenerationFixture(t)
	c := f.claim(t, "a")
	r := generationRegistration(f.h, "g1")
	if _, err := f.h.store.RegisterNode(t.Context(), fabric.Identity{NodeID: "agent"}, r, NodePolicy{MaxOneshotSlots: 3}, true); err != nil {
		t.Fatal(err)
	}
	if _, err := f.h.store.RenewLease(t.Context(), "agent", c.Job.JobID, c.Lease.AttemptID, c.Lease.FencingToken); err != nil {
		t.Fatalf("same generation rejoin fenced Attempt: %v", err)
	}
	for _, name := range []string{"upgrade:retirement:v0", "upgrade:retirement:v01", "upgrade:retirement:vx"} {
		bad := generationRegistration(f.h, "g2")
		bad.Capabilities[name] = true
		if _, err := f.h.store.StageNodeGeneration(t.Context(), "agent", bad); err == nil {
			t.Fatalf("malformed %s accepted", name)
		}
	}
	observation := contract.CapabilityObservation{Capabilities: map[string]bool{"kind:process": true}, Revision: 2, ObservedAt: f.h.clock.Now(), MissingCapabilities: []string{}}
	if _, err := f.h.store.HeartbeatNodeWithCapabilityObservation(t.Context(), "agent", "node", "boot-g1", observation, NodePolicy{MaxOneshotSlots: 3}); err == nil {
		t.Fatal("live promise withdrawn")
	}
	f.promote(t, "g2")
	observation.Capabilities["upgrade:retirement:v1"] = true
	if _, err := f.h.store.HeartbeatNodeWithCapabilityObservation(t.Context(), "agent", "node", "boot-g2", observation, NodePolicy{MaxOneshotSlots: 3}); err != nil {
		t.Fatal(err)
	}
	f.refresh(t)
	for _, g := range f.inventory.Generations {
		if g.AgentGenerationID == "g2" && g.Registration.CapabilityRevision != 2 {
			t.Fatal("primary session observation stale")
		}
	}
}

func TestNodeGenerationHTTPIdentityAndClosedRequestSchemas(t *testing.T) {
	h := newIntegrationHarness(t, map[string][]string{"node": nil})
	agent := h.client(fabric.Identity{NodeID: "agent", Tags: []string{DefaultAgentPrincipalTag}})
	other := h.client(fabric.Identity{NodeID: "other", Tags: []string{DefaultAgentPrincipalTag}})
	status, _, body := h.do(agent, http.MethodPost, "/v1/agent/nodes/register", generationRegistration(h, "g1"))
	if status != 200 {
		t.Fatalf("register=%d %s", status, body)
	}
	status, _, body = h.do(agent, http.MethodGet, "/v1/agent/nodes/node/generations", nil)
	if status != 200 {
		t.Fatalf("inventory=%d %s", status, body)
	}
	var inventory NodeGenerationInventory
	if err := json.Unmarshal(body, &inventory); err != nil || len(inventory.Generations) != 1 {
		t.Fatal(inventory, err)
	}
	for _, route := range []string{"/generations", "/generations/stage", "/generations/promote", "/generations/g1/heartbeat", "/generations/g1/close"} {
		method := http.MethodPost
		var request any = map[string]any{}
		if route == "/generations" {
			method = http.MethodGet
			request = nil
		}
		if route == "/generations/stage" {
			request = generationRegistration(h, "g2")
		}
		if route == "/generations/promote" {
			request = PromoteNodeGenerationRequest{OperationID: "x", ExpectedPrimaryGenerationID: "g1", CandidateGenerationID: "g2", CatalogRevision: 1, SelectedProtocol: 1}
		}
		status, _, body = h.do(other, method, "/v1/agent/nodes/node"+route, request)
		if status != 403 {
			t.Fatalf("other identity %s=%d %s", route, status, body)
		}
	}
	for _, route := range []string{"stage", "promote", "g1/heartbeat", "g1/close"} {
		status, _, body = h.do(agent, http.MethodPost, "/v1/agent/nodes/node/generations/"+route, map[string]any{"unknown": true})
		if status != 400 {
			t.Fatalf("unknown field %s=%d %s", route, status, body)
		}
	}
	status, _, body = h.do(agent, http.MethodPost, "/v1/agent/nodes/node/generations/stage", generationRegistration(h, "g2"))
	if status != 200 {
		t.Fatalf("stage=%d %s", status, body)
	}
	status, _, body = h.do(agent, http.MethodPost, "/v1/agent/jobs/claim", ClaimRequest{NodeID: "node", BootSessionID: "boot-g2", Class: contract.JobClassOneShot})
	if status != 409 {
		t.Fatalf("staged claim=%d %s", status, body)
	}
}

func TestNodeGenerationRetiringHeartbeatCannotMaskPrimaryOrExpiredLease(t *testing.T) {
	f := newGenerationFixture(t)
	c := f.claim(t, "a")
	f.promote(t, "g2")
	f.h.clock.Advance(DefaultNodeDeadAfter)
	if _, err := f.h.store.HeartbeatRetiringNodeGeneration(t.Context(), "agent", "node", "g1", "boot-g1"); err != nil {
		t.Fatal(err)
	}
	if _, err := f.h.store.Reconcile(t.Context()); err != nil {
		t.Fatal(err)
	}
	f.refresh(t)
	if f.node.State != contract.NodeDead {
		t.Fatalf("retiree masked unreachable primary: %s", f.node.State)
	}
	if _, err := f.h.store.ResolveAttemptCredential(t.Context(), c.AttemptToken, "agent"); errorCode(err) != contract.ErrorUnauthorized {
		t.Fatalf("retiring heartbeat revived credential: %v", err)
	}
	if _, err := f.h.store.RenewLease(t.Context(), "agent", c.Job.JobID, c.Lease.AttemptID, c.Lease.FencingToken); errorCode(err) != contract.ErrorLeaseExpired {
		t.Fatalf("retiring heartbeat revived Lease: %v", err)
	}
}

func TestNodeGenerationReadOnlyInventoryAndClosureDoNotAcquireWriter(t *testing.T) {
	f := newGenerationFixture(t)
	f.promote(t, "g2")
	write, err := f.h.store.beginWriteTransaction(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer write.rollback()
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	if _, err = f.h.store.NodeGenerations(ctx, "agent", "node"); err != nil {
		t.Fatalf("inventory waited for writer: %v", err)
	}
	if ready, err := f.h.store.NodeGenerationClosureReady(ctx, "agent", "node", "g1"); err != nil || !ready {
		t.Fatalf("closure prerequisite waited for writer: ready=%t err=%v", ready, err)
	}
}

func TestNodeGenerationHTTPPromotionRetiringCompletionAndClosure(t *testing.T) {
	h := newIntegrationHarness(t, map[string][]string{"node": nil})
	agent := h.client(fabric.Identity{NodeID: "agent", Tags: []string{DefaultAgentPrincipalTag}})
	post := func(path string, request any) []byte {
		t.Helper()
		status, _, body := h.do(agent, http.MethodPost, path, request)
		if status != http.StatusOK {
			t.Fatalf("%s=%d %s", path, status, body)
		}
		return body
	}
	var node Node
	if err := json.Unmarshal(post("/v1/agent/nodes/register", generationRegistration(h, "g1")), &node); err != nil {
		t.Fatal(err)
	}
	job, _, err := h.store.CreateJob(t.Context(), validJobSpec("http-generation", nil))
	if err != nil {
		t.Fatal(err)
	}
	var claim Claim
	if err = json.Unmarshal(post("/v1/agent/jobs/claim", ClaimRequest{NodeID: "node", BootSessionID: "boot-g1", Class: contract.JobClassOneShot}), &claim); err != nil {
		t.Fatal(err)
	}
	node, err = h.store.SetNodeClaimsByOperator(t.Context(), "node", "operator", NodeIntentRequest{ClaimsEnabled: false, IntentRevision: node.IntentRevision, Reason: "upgrade"})
	if err != nil {
		t.Fatal(err)
	}
	var staged NodeGenerationInventory
	if err = json.Unmarshal(post("/v1/agent/nodes/node/generations/stage", generationRegistration(h, "g2")), &staged); err != nil {
		t.Fatal(err)
	}
	request := PromoteNodeGenerationRequest{OperationID: "http-promote", ExpectedPrimaryGenerationID: "g1", ExpectedPrimaryBootSessionID: "boot-g1", ExpectedAuthorityGeneration: node.AuthorityGeneration, CandidateGenerationID: "g2", CandidateBootSessionID: "boot-g2", ExpectedCatalogRevision: staged.CatalogRevision, CatalogRevision: staged.CatalogRevision + 1, IntentRevision: node.IntentRevision, SelectedProtocol: 1, RetiringGenerationIDs: []string{}}
	for _, g := range staged.Generations {
		if g.AgentGenerationID == "g1" {
			request.ResidentAttempts = g.ResidentAttempts
		}
	}
	post("/v1/agent/nodes/node/generations/promote", request)
	post("/v1/agent/nodes/node/generations/g1/heartbeat", NodeGenerationHeartbeatRequest{BootSessionID: "boot-g1"})
	post("/v1/agent/jobs/"+job.JobID+"/attempts/"+claim.Lease.AttemptID+"/lease", RenewalRequest{FencingToken: claim.Lease.FencingToken})
	exit := 0
	completion := CompletionRequest{FencingToken: claim.Lease.FencingToken, IdempotencyKey: "http-done", Result: ProcessResult{ExitCode: &exit}}
	post("/v1/agent/jobs/"+job.JobID+"/attempts/"+claim.Lease.AttemptID+"/complete", completion)
	var closed NodeGenerationInventory
	if err = json.Unmarshal(post("/v1/agent/nodes/node/generations/g1/close", CloseNodeGenerationRequest{BootSessionID: "boot-g1", PromotionOperationID: request.OperationID, ResidentAttempts: request.ResidentAttempts, RuntimesReaped: true, EvidenceSettled: true}), &closed); err != nil {
		t.Fatal(err)
	}
	for _, g := range closed.Generations {
		if g.AgentGenerationID == "g1" && g.Role != NodeGenerationClosed {
			t.Fatal("HTTP closure did not commit")
		}
	}
	post("/v1/agent/jobs/"+job.JobID+"/attempts/"+claim.Lease.AttemptID+"/complete", completion)
}

func TestNodeGenerationReusedBootCannotHideUnknownPredecessor(t *testing.T) {
	f := newGenerationFixture(t)
	c := f.claim(t, "unknown-boot-reuse")
	registration := generationRegistration(f.h, "g1")
	for _, boot := range []string{"boot-g1-restart", "boot-g1"} {
		registration.BootSessionID = boot
		var err error
		f.node, err = f.h.store.RegisterNode(t.Context(), fabric.Identity{NodeID: "agent"}, registration, NodePolicy{MaxOneshotSlots: 3}, true)
		if err != nil {
			t.Fatal(err)
		}
	}
	if _, _, err := f.h.store.CreateJob(t.Context(), validJobSpec("queued-after-boot-reuse", nil)); err != nil {
		t.Fatal(err)
	}
	if _, err := f.h.store.RenewLease(t.Context(), "agent", c.Job.JobID, c.Lease.AttemptID, c.Lease.FencingToken); errorCode(err) != contract.ErrorNodeSessionReplaced {
		t.Fatalf("predecessor regained execution authority: %v", err)
	}
	if claimed, err := f.h.store.ClaimJob(t.Context(), "agent", "node", f.node.BootSessionID, contract.JobClassOneShot); err != nil || claimed != nil {
		t.Fatalf("reused boot bypassed unknown-predecessor embargo: claim=%+v err=%v", claimed, err)
	}
}
