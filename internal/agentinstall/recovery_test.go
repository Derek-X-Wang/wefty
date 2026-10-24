package agentinstall

import (
	"encoding/json"
	"errors"
	"os"
	"strings"
	"testing"

	"github.com/Derek-X-Wang/wefty/l1"
)

func TestPromotionRecoveryEveryLocalPublication(t *testing.T) {
	for _, phase := range []string{"suppression_prepared", "retiree_plist", "catalog_published", "receipt_committed", "pending_cleared"} {
		t.Run(phase, func(t *testing.T) {
			f := newFixture(t)
			first := f.prepare(t, true)
			second := f.prepare(t, false)
			f.promote(t, second)
			candidate := f.prepare(t, false)
			c, err := f.s.Read()
			must(t, err)
			receipt, err := f.s.BeginPromotion(candidate.ID, testBoot, "operation", c.Revision, 1, emptyResidents(c))
			must(t, err)
			hit := false
			f.s.afterPromotionWrite = func(at string) error {
				if at == phase || strings.HasPrefix(at, phase+":") {
					hit = true
					return errors.New("kill")
				}
				return nil
			}
			if err = f.s.CommitPromotion(t.Context(), receipt.ID, "operation", testBoot, 1); err == nil || !hit {
				t.Fatal("missing kill", err)
			}
			f.s.afterPromotionWrite = nil
			must(t, f.s.CommitPromotion(t.Context(), receipt.ID, "operation", testBoot, 1))
			mutations := len(f.f.mutations)
			must(t, f.s.CommitPromotion(t.Context(), receipt.ID, "operation", testBoot, 1))
			if len(f.f.mutations) != mutations {
				t.Fatal("completed retry repeated launchd effects")
			}
			c, err = f.s.Read()
			must(t, err)
			if c.Primary != candidate.ID || c.PendingReceipt != "" || len(c.Generations) != 3 {
				t.Fatalf("catalog=%+v", c)
			}
			for _, old := range []Generation{first, second} {
				if !f.f.states[old.Label].Running || c.Generations[old.ID].Role != "retiring" {
					t.Fatal("old work stopped")
				}
			}
			var refused *VerificationRefusal
			if err = f.s.CommitPromotion(t.Context(), receipt.ID, "changed", testBoot, 1); !errors.As(err, &refused) {
				t.Fatal("changed operation accepted", err)
			}
		})
	}
}

func TestCleanupNeedsPositiveL1ClosureAndExitedProcess(t *testing.T) {
	f := newFixture(t)
	old := f.prepare(t, true)
	candidate := f.prepare(t, false)
	f.promote(t, candidate)
	c, err := f.s.Read()
	must(t, err)
	g := c.Generations[old.ID]
	remote := l1.NodeGeneration{AgentGenerationID: old.ID, BootSessionID: g.Process.BootSessionID, AuthorityGeneration: 1, PromotionOperationID: "promotion", Role: l1.NodeGenerationRetiring}
	inventory := l1.NodeGenerationInventory{NodeID: "node", Generations: []l1.NodeGeneration{remote}}
	must(t, f.s.CleanupClosed(t.Context(), inventory))
	remote.Role = l1.NodeGenerationClosed
	must(t, f.s.PublishClosedGeneration(remote))
	// A positive local closure receipt cannot replace positive current L1 closure.
	must(t, f.s.CleanupClosed(t.Context(), inventory))
	if _, err = os.Stat(old.Executable); err != nil {
		t.Fatal("receipt alone removed files", err)
	}
	inventory.Generations[0] = remote
	must(t, f.s.CleanupClosed(t.Context(), inventory))
	if _, err = os.Stat(old.Executable); err != nil {
		t.Fatal("running process removed", err)
	}
	f.lifetimes[old.ID]()
	delete(f.lifetimes, old.ID)
	state := f.f.states[old.Label]
	state.Running = false
	state.PID = 0
	f.f.states[old.Label] = state
	must(t, f.s.CleanupClosed(t.Context(), inventory))
	mutations := len(f.f.mutations)
	must(t, f.s.CleanupClosed(t.Context(), inventory))
	if len(f.f.mutations) != mutations {
		t.Fatal("duplicate cleanup")
	}
	if _, err = os.Stat(old.Executable); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("old executable retained", err)
	}
	if _, err = os.Stat(candidate.Executable); err != nil {
		t.Fatal("primary removed", err)
	}
}

func TestRebootReconcilesCommittedPrimaryWithoutOldReadiness(t *testing.T) {
	f := newFixture(t)
	old := f.prepare(t, true)
	candidate := f.prepare(t, false)
	c, err := f.s.Read()
	must(t, err)
	evidence := emptyResidents(c)
	receipt, err := f.s.BeginPromotion(candidate.ID, testBoot, "operation", c.Revision, 1, evidence)
	must(t, err)
	// Simulate reboot: all process lifetime handles and manager identities vanish.
	for id, release := range f.lifetimes {
		release()
		delete(f.lifetimes, id)
	}
	clear(f.f.states)
	in := l1.NodeGenerationInventory{NodeID: "node", CatalogRevision: 1, Generations: []l1.NodeGeneration{{AgentGenerationID: candidate.ID, BootSessionID: candidate.Process.BootSessionID, Role: l1.NodeGenerationPrimary, PromotionOperationID: "operation"}, {AgentGenerationID: old.ID, BootSessionID: old.Process.BootSessionID, Role: l1.NodeGenerationRetiring, PromotionOperationID: "operation", ResidentAttempts: []l1.ResidentAttempt{}}}}
	must(t, f.s.ReconcilePromoted(t.Context(), in))
	c, err = f.s.Read()
	must(t, err)
	if c.Primary != candidate.ID || c.PendingReceipt != "" {
		t.Fatal("primary not reconciled", c)
	}
	release, err := f.s.Startup(candidate.ID, "node", candidate.Executable, nextBoot)
	must(t, err)
	release()
	_, err = f.s.Startup(old.ID, "node", old.Executable, nextBoot)
	if !errors.Is(err, ErrStartupSuppressed) {
		t.Fatal("stale retiree restarted", err)
	}
	receipt.Committed = true
}

func TestReconcilePromotedPreservesCorrelatedSuccess(t *testing.T) {
	f := newFixture(t)
	old := f.prepare(t, true)
	candidate := f.prepare(t, false)
	c, err := f.s.Read()
	must(t, err)
	activation, err := f.s.BeginPromotion(candidate.ID, testBoot, "operation", c.Revision, 1, emptyResidents(c))
	must(t, err)
	request := l1.PromoteNodeGenerationRequest{OperationID: "operation", ExpectedPrimaryGenerationID: old.ID, ExpectedPrimaryBootSessionID: old.Process.BootSessionID, ExpectedAuthorityGeneration: 1, CandidateGenerationID: candidate.ID, CandidateBootSessionID: candidate.Process.BootSessionID, ExpectedCatalogRevision: 0, CatalogRevision: 1, IntentRevision: 1, SelectedProtocol: 1, ResidentAttempts: []l1.ResidentAttempt{}, RetiringGenerationIDs: []string{}}
	must(t, f.s.BindPromotionRequest(activation.ID, request))
	in := l1.NodeGenerationInventory{NodeID: "node", CatalogRevision: 1, Generations: []l1.NodeGeneration{{AgentGenerationID: candidate.ID, BootSessionID: candidate.Process.BootSessionID, Role: l1.NodeGenerationPrimary, PromotionOperationID: "operation"}, {AgentGenerationID: old.ID, BootSessionID: old.Process.BootSessionID, Role: l1.NodeGenerationRetiring, PromotionOperationID: "operation", ResidentAttempts: []l1.ResidentAttempt{}}}}
	body, err := json.Marshal(in)
	must(t, err)
	payload, err := json.Marshal(request)
	must(t, err)
	control := ControlResult{Request: ControlRequest{ID: "operation", RequestID: "live-cli", NodeID: "node", GenerationID: old.ID, BootSessionID: old.Process.BootSessionID, Action: "promote_generation", Payload: payload}, Payload: body}
	must(t, f.s.PublishControl(control))
	must(t, f.s.ReconcilePromoted(t.Context(), in))
	result, err := f.s.ReadPromotionResult(old.ID)
	must(t, err)
	if result.Request.RequestID != "live-cli" {
		t.Fatal("startup replaced a live CLI's correlated reply")
	}
}

func TestCandidateOperationBindingAndPayloadReplays(t *testing.T) {
	f := newFixture(t)
	primary := f.prepare(t, true)
	candidate, err := f.s.PrepareCandidate(f.source, "v2", testBoot, []int{1}, "operation")
	must(t, err)
	replay, err := f.s.PrepareCandidate(f.source, "v2", testBoot, []int{1}, "operation")
	must(t, err)
	if replay.ID != candidate.ID {
		t.Fatal("preparation repeated")
	}
	must(t, os.WriteFile(f.source, []byte("changed source"), 0700))
	var refusal *VerificationRefusal
	_, err = f.s.PrepareCandidate(f.source, "v2", testBoot, []int{1}, "operation")
	if !errors.As(err, &refusal) {
		t.Fatal("changed preparation payload accepted", err)
	}
	c, err := f.s.Read()
	must(t, err)
	root, close, err := f.s.open(false)
	must(t, err)
	old := c.Generations[candidate.ID]
	old.PreparationOperation = ""
	c.Generations[candidate.ID] = old
	must(t, writePrivate(root, catalogName, c))
	close()
	must(t, f.s.BindCandidateOperation(old, primary.ID, "operation", c.Revision))
	bound, err := f.s.Read()
	must(t, err)
	if bound.Generations[candidate.ID].PreparationOperation != "operation" {
		t.Fatal("legacy candidate not bound")
	}
	wrong := old
	wrong.Digest = strings.Repeat("0", 64)
	if err = f.s.BindCandidateOperation(wrong, primary.ID, "operation", bound.Revision); !errors.As(err, &refusal) {
		t.Fatal("changed legacy candidate bound", err)
	}
}

func TestCleanupRecoveryAfterClosureRecordsWereRemoved(t *testing.T) {
	f := newFixture(t)
	old := f.prepare(t, true)
	candidate := f.prepare(t, false)
	f.promote(t, candidate)
	c, err := f.s.Read()
	must(t, err)
	g := c.Generations[old.ID]
	remote := l1.NodeGeneration{AgentGenerationID: g.ID, BootSessionID: g.Process.BootSessionID, Role: l1.NodeGenerationClosed, PromotionOperationID: "promotion", ResidentAttempts: []l1.ResidentAttempt{}}
	closed, err := json.Marshal(remote)
	must(t, err)
	ack := remote
	ack.Role = l1.NodeGenerationRetiring
	acknowledged, err := json.Marshal(ack)
	must(t, err)
	proof := CompletionProof{GenerationID: g.ID, CatalogRevision: c.Revision, Binding: *g.Process, PayloadsAbsent: true, L1SessionClosed: true, EvidenceSafe: true, EvidenceDigest: digestBytes(closed)}
	receipt := completionReceipt{Version: 1, Generation: g, Proof: proof, PlistDigest: g.PlistDigest, ControlFiles: map[string][]byte{"retirement-closed.json": closed, "retirement-ack.json": acknowledged}}
	root, close, err := f.s.open(false)
	must(t, err)
	must(t, writePrivate(root, "finished-"+g.ID+".json", receipt))
	close()
	f.lifetimes[g.ID]()
	delete(f.lifetimes, g.ID)
	state := f.f.states[g.Label]
	state.Running = false
	state.PID = 0
	f.f.states[g.Label] = state
	// Exact control files are already absent, as after their deletion in cleanup.
	historical, err := f.s.ReadRetirement(g.ID)
	must(t, err)
	if historical.AgentGenerationID != g.ID {
		t.Fatal("partial cleanup lost the retained acknowledgment")
	}
	in := l1.NodeGenerationInventory{NodeID: "node", Generations: []l1.NodeGeneration{ack}}
	must(t, f.s.CleanupClosed(t.Context(), in))
	if _, err = os.Stat(g.Executable); err != nil {
		t.Fatal("completion audit alone removed files", err)
	}
	in.Generations[0] = remote
	must(t, f.s.CleanupClosed(t.Context(), in))
	if _, err = os.Stat(g.Executable); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("partial cleanup did not converge", err)
	}
}

func TestPromotionIntentRecoveryAfterEachPublication(t *testing.T) {
	for _, phase := range []string{"activation_prepared", "activation_pending"} {
		t.Run(phase, func(t *testing.T) {
			f := newFixture(t)
			f.prepare(t, true)
			candidate := f.prepare(t, false)
			c, err := f.s.Read()
			must(t, err)
			evidence := emptyResidents(c)
			f.s.afterPromotionWrite = func(at string) error {
				if at == phase {
					return errors.New("kill")
				}
				return nil
			}
			first, err := f.s.BeginPromotion(candidate.ID, testBoot, "operation", c.Revision, 1, evidence)
			if err == nil || first.ID == "" {
				t.Fatal("missing interrupted intent")
			}
			f.s.afterPromotionWrite = nil
			replay, err := f.s.BeginPromotion(candidate.ID, testBoot, "operation", c.Revision, 1, evidence)
			must(t, err)
			if replay.ID != first.ID {
				t.Fatal("duplicate activation intent")
			}
			pending, err := f.s.Read()
			must(t, err)
			if pending.PendingReceipt != first.ID {
				t.Fatal("pending operation lost")
			}
			wrong := evidence
			wrong.IntentRevision++
			var refused *VerificationRefusal
			if _, err = f.s.BeginPromotion(candidate.ID, testBoot, "operation", c.Revision, 1, wrong); !errors.As(err, &refused) {
				t.Fatal("changed intent replay accepted", err)
			}
			retained, err := f.s.ReadActivationReceipt(first.ID)
			must(t, err)
			if retained.IntentRevision != evidence.IntentRevision {
				t.Fatal("refusal overwrote recorded intent")
			}
		})
	}
}

func TestPreparedPromotionAbortRequiresClosureAndRecoversEveryPublication(t *testing.T) {
	for _, phase := range []string{"activation_aborted", "abort_pending_cleared"} {
		t.Run(phase, func(t *testing.T) {
			f := newFixture(t)
			primary := f.prepare(t, true)
			candidate := f.prepare(t, false)
			c, err := f.s.Read()
			must(t, err)
			receipt, err := f.s.BeginPromotion(candidate.ID, testBoot, "operation", c.Revision, 1, emptyResidents(c))
			must(t, err)
			in := l1.NodeGenerationInventory{NodeID: c.NodeID, CatalogRevision: receipt.L1CatalogRevision, Generations: []l1.NodeGeneration{{AgentGenerationID: primary.ID, BootSessionID: receipt.Processes[primary.ID].BootSessionID, AuthorityGeneration: receipt.ExpectedAuthorityGeneration, Role: l1.NodeGenerationPrimary}, {AgentGenerationID: candidate.ID, BootSessionID: receipt.Processes[candidate.ID].BootSessionID, Role: l1.NodeGenerationStaged, PromotionOperationID: receipt.L1OperationID}}}
			var refused *VerificationRefusal
			if err = f.s.AbortPreparedPromotion(receipt.ID, in); !errors.As(err, &refused) {
				t.Fatal("receipt without closure cleared activation", err)
			}
			in.Generations[1].Role = l1.NodeGenerationClosed
			hit := false
			f.s.afterPromotionWrite = func(at string) error {
				if at == phase {
					hit = true
					return errors.New("interrupt")
				}
				return nil
			}
			if err = f.s.AbortPreparedPromotion(receipt.ID, in); err == nil || !hit {
				t.Fatal("missing interruption", err)
			}
			f.s.afterPromotionWrite = nil
			must(t, f.s.AbortPreparedPromotion(receipt.ID, in))
			must(t, f.s.AbortPreparedPromotion(receipt.ID, in))
			after, err := f.s.Read()
			must(t, err)
			if after.Primary != primary.ID || after.PendingReceipt != "" || len(after.Generations) != 2 {
				t.Fatal("abort changed cohort", after)
			}
			retained, err := f.s.ReadActivationReceipt(receipt.ID)
			must(t, err)
			if !retained.Aborted || retained.Committed {
				t.Fatal("wrong activation result")
			}
		})
	}
}
