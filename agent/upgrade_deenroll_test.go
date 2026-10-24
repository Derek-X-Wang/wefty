package agent

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/fabric/plain"
	"github.com/Derek-X-Wang/wefty/internal/agentinstall"
	"github.com/Derek-X-Wang/wefty/l1"
)

func TestDeEnrollmentAdapterAttestsAndPreservesResult(t *testing.T) {
	installed, g := newUpgradeControlStore(t)
	catalog, e := installed.Read()
	if e != nil {
		t.Fatal(e)
	}
	g.Process = &agentinstall.Binding{PID: 123, ProcessStartID: "fixture", BootSessionID: "boot", SystemBootID: g.StartBootID, ReadyAt: time.Now()}
	catalog.Generations[g.ID] = g
	bytes, _ := json.Marshal(catalog)
	if e = os.WriteFile(filepath.Join(installed.Root(), "catalog.json"), bytes, 0600); e != nil {
		t.Fatal(e)
	}
	network := plain.NewNetwork()
	authority, stop := startFailureServerWithPoliciesAndLease(t, network, nil, nil, 2*time.Second)
	defer stop()
	identity := fabric.Identity{NodeID: "installed-control-agent", Tags: []string{l1.DefaultAgentPrincipalTag}}
	client, err := newClient(network.NewFabric(identity), "wefty://control-plane", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	clock := systemClock{}
	capabilities := newCapabilityState(map[string]bool{"kind:process": true, "upgrade:retirement:v1": true}, nil, clock, time.Second, nil)
	observation := capabilities.snapshot()
	registration := contract.NodeRegistration{NodeID: "node", AgentGenerationID: g.ID, BootSessionID: "boot", OS: "darwin", Architecture: "arm64", AgentVersion: "test", Capabilities: observation.Capabilities, CapabilityRevision: observation.Revision, CapabilityObservedAt: observation.ObservedAt}
	node, err := authority.RegisterNode(t.Context(), identity, registration, l1.DefaultNodePolicy(), true)
	if err != nil {
		t.Fatal(err)
	}
	node, err = authority.SetNodeClaimsByOperator(t.Context(), "node", "operator", l1.NodeIntentRequest{IntentRevision: node.IntentRevision, ClaimsEnabled: false, Reason: "wefty maintenance operation"})
	if err != nil {
		t.Fatal(err)
	}
	session := newAgentSession(client, registration, capabilities, time.Second, time.Millisecond, clock, newLifecycleObserver(clock), nil, 1, 1)
	a := &Agent{session: session}
	inventory, err := client.NodeGenerations(t.Context(), "node")
	if err != nil {
		t.Fatal(err)
	}
	request := l1.DeEnrollNodeGenerationRequest{OperationID: "operation", PrimaryGenerationID: g.ID, PrimaryBootSessionID: "boot", AuthorityGeneration: node.AuthorityGeneration, CatalogRevision: inventory.CatalogRevision, IntentRevision: node.IntentRevision, IntentReason: node.IntentReason}
	exchange := func(r l1.DeEnrollNodeGenerationRequest) (l1.NodeGenerationInventory, error) {
		body, _ := json.Marshal(r)
		return a.deEnrollGeneration(context.Background(), installed, agentinstall.ControlRequest{ID: r.OperationID, NodeID: "node", GenerationID: g.ID, BootSessionID: "boot", Action: "de_enroll_generation", Payload: body})
	}
	bad := request
	bad.CatalogRevision++
	if _, err = exchange(bad); err == nil {
		t.Fatal("L1 fence refusal lost")
	}
	if record, e := installed.ReadDeEnrollment(); e != nil || record != nil {
		t.Fatal("refusal stranded primary startup", e)
	}
	// A terminal rejection binds its operation ID. A corrected attempt uses
	// a new operation and closes admission under that operation's exact reason.
	request.OperationID = "retry-operation"
	node, err = authority.SetNodeClaimsByOperator(t.Context(), "node", "operator", l1.NodeIntentRequest{IntentRevision: node.IntentRevision, ClaimsEnabled: false, Reason: "wefty maintenance " + request.OperationID})
	if err != nil {
		t.Fatal(err)
	}
	request.IntentRevision, request.IntentReason = node.IntentRevision, node.IntentReason
	outbox, e := newEvidenceOutbox(t.TempDir(), "node", 1<<20, clock, 1, time.Second, time.Second)
	if e != nil {
		t.Fatal(e)
	}
	defer outbox.Close()
	outbox.spool.generationID = g.ID
	if e = outbox.spool.ensureAttempt(t.Context(), l1.Claim{Job: l1.Job{JobID: "unaccounted", Spec: contract.JobSpec{Class: contract.JobClassOneShot, Kind: contract.JobKindProcess}}, Lease: l1.AttemptLease{AttemptID: "unaccounted", FencingToken: "unaccounted-fence"}}); e != nil {
		t.Fatal(e)
	}
	a.outbox = outbox
	session.retirementEvidenceSettled = outbox.settleRetirementEvidence(client, "node")
	if _, e = exchange(request); e == nil {
		t.Fatal("empty process map certified an unaccounted empty spool row")
	}
	if record, e := installed.ReadDeEnrollment(); e != nil || record != nil {
		t.Fatal("unknown runtime row created an attestation", e)
	}
	// This is an isolated row with no real payload. Remove the fixture only
	// after proving it blocks the real adapter/L1 path.
	if _, e = outbox.spool.db.ExecContext(t.Context(), `DELETE FROM spool_attempts WHERE attempt_id='unaccounted'`); e != nil {
		t.Fatal(e)
	}
	session.claimMu.Lock()
	session.residentJobID["one-shot"] = struct{}{}
	session.retirementAttempts["one-shot"] = &retirementAttempt{jobID: "one-shot", class: contract.JobClassOneShot, kind: contract.JobKindProcess}
	session.claimMu.Unlock()
	type exchangeResult struct {
		inventory l1.NodeGenerationInventory
		err       error
	}
	done := make(chan exchangeResult, 1)
	go func() { inventory, err := exchange(request); done <- exchangeResult{inventory, err} }()
	select {
	case answer := <-done:
		t.Fatalf("did not wait for resident work: %v", answer.err)
	case <-time.After(150 * time.Millisecond):
	}
	if record, e := installed.ReadDeEnrollment(); e != nil || record != nil {
		t.Fatal("attestation preceded runtime reap", e)
	}
	session.claimMu.Lock()
	delete(session.residentJobID, "one-shot")
	record := session.retirementAttempts["one-shot"]
	record.reaped, record.finished, record.evidenceSettled = true, true, true
	session.claimMu.Unlock()
	var answer exchangeResult
	select {
	case answer = <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("finished primary did not de-enroll")
	}
	result, err := answer.inventory, answer.err
	if err != nil {
		t.Fatal(err)
	}
	if result.NodeID != "node" {
		t.Fatal("wrong result")
	}
	saved, err := installed.ReadDeEnrollment()
	if err != nil || saved.Result == nil || !saved.Request.RuntimesReaped || !saved.Request.EvidenceSettled {
		t.Fatal("positive local proof/result not durable", err)
	}
	if _, err = exchange(request); err != nil {
		t.Fatal("exact replay", err)
	}
	if _, err = exchange(bad); err == nil {
		t.Fatal("changed replay accepted")
	}
	saved, err = installed.ReadDeEnrollment()
	if err != nil || saved.Result == nil {
		t.Fatal("changed replay erased success", err)
	}
}
