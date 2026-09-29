package l1

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
)

// outageLedger is a run ledger for one Computer that can be taken down (it
// refuses), hung (it never answers), and brought back. Up, it applies L3's
// revocation rules to its grants (l3/computer_tokens.go): an attempt-scoped
// request ends only that attempt, a revoke-all ends every grant except the
// attempts it preserves, and every request is idempotent.
type outageLedger struct {
	mu       sync.Mutex
	down     bool
	hang     bool
	grants   map[string]bool
	requests []ComputerTokenRevocation
}

func newOutageLedger(h *integrationHarness) *outageLedger {
	ledger := &outageLedger{grants: map[string]bool{}}
	h.server.computerTokenRevoker = recordingComputerTokenRevoker{revoke: ledger.revoke}
	return ledger
}

func (ledger *outageLedger) set(down, hang bool) {
	ledger.mu.Lock()
	defer ledger.mu.Unlock()
	ledger.down, ledger.hang = down, hang
}

func (ledger *outageLedger) mint(attemptID string) {
	ledger.mu.Lock()
	defer ledger.mu.Unlock()
	ledger.grants[attemptID] = false
}

func (ledger *outageLedger) active(attemptID string) bool {
	ledger.mu.Lock()
	defer ledger.mu.Unlock()
	revoked, minted := ledger.grants[attemptID]
	return minted && !revoked
}

func (ledger *outageLedger) taken() []ComputerTokenRevocation {
	ledger.mu.Lock()
	defer ledger.mu.Unlock()
	return slices.Clone(ledger.requests)
}

func (ledger *outageLedger) revoke(ctx context.Context, request ComputerTokenRevocation) (contract.ComputerTokenRevocationReceipt, error) {
	ledger.mu.Lock()
	down, hang := ledger.down, ledger.hang
	ledger.mu.Unlock()
	if hang {
		<-ctx.Done()
		return contract.ComputerTokenRevocationReceipt{}, ctx.Err()
	}
	if down {
		return contract.ComputerTokenRevocationReceipt{}, errors.New(
			`l1: revoke Computer tokens: Post "http://run-ledger.invalid/v1/computer-token/revoke": dial: connection refused`)
	}
	ledger.mu.Lock()
	defer ledger.mu.Unlock()
	ledger.requests = append(ledger.requests, request)
	count := 0
	for attemptID, revoked := range ledger.grants {
		if revoked {
			continue
		}
		switch {
		case request.ComputerAttemptID != "":
			if attemptID != request.ComputerAttemptID {
				continue
			}
		case request.RevokeAll:
			if slices.Contains(request.PreserveComputerAttemptIDs, attemptID) {
				continue
			}
		default:
			continue
		}
		ledger.grants[attemptID] = true
		count++
	}
	return contract.ComputerTokenRevocationReceipt{ComputerID: request.ComputerID, ComputerAttemptID: request.ComputerAttemptID,
		PreservedComputerAttemptIDs: request.PreserveComputerAttemptIDs, SubmitIntentRevision: request.NewSubmitIntentRevision,
		RevokedGrantCount: count, CommittedAt: time.Now()}, nil
}

type owedRevocationAudit struct {
	revocationID int64
	verb         ComputerRevocationVerb
	scope        ComputerRevocationScope
	attemptID    string
	settlement   string
	receipt      *contract.ComputerTokenRevocationReceipt
	rawReceipt   string
}

func owedRevocationAuditRows(t *testing.T, h *integrationHarness, computerID string) []owedRevocationAudit {
	t.Helper()
	rows, err := h.store.db.Query(`SELECT revocation_id, verb, scope, computer_attempt_id, settlement, receipt_json
		FROM computer_owed_revocations WHERE computer_id=? ORDER BY revocation_id`, computerID)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var audit []owedRevocationAudit
	for rows.Next() {
		var row owedRevocationAudit
		var payload []byte
		if err := rows.Scan(&row.revocationID, &row.verb, &row.scope, &row.attemptID, &row.settlement, &payload); err != nil {
			t.Fatal(err)
		}
		if payload != nil {
			row.rawReceipt = string(payload)
			row.receipt = &contract.ComputerTokenRevocationReceipt{}
			if err := json.Unmarshal(payload, row.receipt); err != nil {
				t.Fatal(err)
			}
		}
		audit = append(audit, row)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return audit
}

func heartbeatComputerNode(t *testing.T, h *integrationHarness, node Node) time.Duration {
	t.Helper()
	agent := h.client(fabric.Identity{NodeID: "fabric-computer-node", Tags: []string{DefaultAgentPrincipalTag}})
	started := time.Now()
	status, _, body := h.do(agent, http.MethodPost, "/v1/agent/nodes/"+node.NodeID+"/heartbeat", heartbeatRequestForNode(node))
	if status != http.StatusOK {
		t.Fatalf("heartbeat status=%d body=%s", status, body)
	}
	return time.Since(started)
}

func mustGetComputer(t *testing.T, h *integrationHarness, computerID string) Computer {
	t.Helper()
	computer, err := h.store.GetComputer(t.Context(), computerID)
	if err != nil {
		t.Fatal(err)
	}
	return computer
}

// TestOwedRevocationsSettleOnHeartbeatAfterRunLedgerOutage is the #554
// acceptance: with the run ledger down, a stop, the completion that stop
// drives, and a restart each commit, answer exactly as #548 made them answer,
// and leave their revocation owed on the Computer. Meanwhile the restart's
// next attempt is minted a pass. When the run ledger returns, the host Node's
// heartbeat settles every owed revocation once, with a receipt: the
// completion's stays scoped to its attempt, and the Computer-wide ones spare
// the attempt that began after they were owed.
func TestOwedRevocationsSettleOnHeartbeatAfterRunLedgerOutage(t *testing.T) {
	h, _, node, agent := computerCompletionHarness(t)
	ledger := newOutageLedger(h)
	client := h.client(fabric.Identity{NodeID: "computer-client", Tags: []string{DefaultClientPrincipalTag}})
	computer, _, err := h.store.CreateComputer(t.Context(), CreateComputerRequest{Name: "owed-outage",
		Spec: computerCapabilityJobSpec("computer:owed-outage:v1"), Actor: "operator"})
	if err != nil {
		t.Fatal(err)
	}
	first := startComputerAttempt(t, h, node, nil)
	ledger.mint(first.Lease.AttemptID)
	computer = mustGetComputer(t, h, computer.ComputerID)

	ledger.set(true, false)
	status, _, body := h.do(client, http.MethodPut, "/v1/computers/"+computer.ComputerID+"/desired-state",
		computerDesiredRequest(computer, contract.ServiceDesiredStopped, "operator"))
	assertRunLedgerUnavailable(t, status, body, false, "the Computer mutation applied", "is owed")
	completion := CompletionRequest{FencingToken: first.Lease.FencingToken, IdempotencyKey: "completion:" + first.Lease.AttemptID,
		Result: ProcessResult{OutputError: "logs finalized after positive reap"}, RuntimeQuiescenceEvidence: RuntimeQuiescenceAttempt}
	status, _, body = h.do(agent, http.MethodPost,
		fmt.Sprintf("/v1/agent/jobs/%s/attempts/%s/complete", first.Job.JobID, first.Lease.AttemptID), completion)
	assertRunLedgerUnavailable(t, status, body, false, "the Computer mutation applied", "is owed")
	computer = mustGetComputer(t, h, computer.ComputerID)
	if computer.CurrentJob.State != contract.JobStopped {
		t.Fatalf("completion refused but did not apply: job state = %q", computer.CurrentJob.State)
	}
	status, _, body = h.do(client, http.MethodPost, "/v1/computers/"+computer.ComputerID+"/restart",
		ComputerRestartRequest{ComputerMutationPrecondition: computerPrecondition(computer, "operator"), IdempotencyKey: "owed-outage-restart"})
	assertRunLedgerUnavailable(t, status, body, false, "the Computer mutation applied", "is owed")
	second := startComputerAttempt(t, h, node, nil)
	ledger.mint(second.Lease.AttemptID)

	// Down: the heartbeat still answers, and everything stays owed.
	heartbeatComputerNode(t, h, node)
	computer = mustGetComputer(t, h, computer.ComputerID)
	wantOwed := []struct {
		verb      ComputerRevocationVerb
		scope     ComputerRevocationScope
		attemptID string
	}{
		{ComputerRevocationVerbStop, ComputerRevocationScopeRevokeAll, ""},
		{ComputerRevocationVerbAttemptCompletion, ComputerRevocationScopeAttempt, first.Lease.AttemptID},
		{ComputerRevocationVerbRestart, ComputerRevocationScopeRevokeAll, ""},
	}
	if len(computer.OwedRevocations) != len(wantOwed) {
		t.Fatalf("owed revocations during the outage = %#v", computer.OwedRevocations)
	}
	for index, want := range wantOwed {
		owed := computer.OwedRevocations[index]
		if owed.Verb != want.verb || owed.Scope != want.scope || owed.ComputerAttemptID != want.attemptID ||
			owed.HostNodeID != node.NodeID || owed.SettleFailures != 2 || !strings.Contains(owed.LastFailure, "connection refused") {
			t.Fatalf("owed revocation %d = %#v, want %s/%s/%q failed by the handler and one heartbeat", index, owed,
				want.verb, want.scope, want.attemptID)
		}
	}
	if len(ledger.taken()) != 0 || !ledger.active(first.Lease.AttemptID) {
		t.Fatalf("a down run ledger took revocations: %#v", ledger.taken())
	}

	// Back: one heartbeat settles every row, each with its receipt.
	ledger.set(false, false)
	heartbeatComputerNode(t, h, node)
	if owed := mustGetComputer(t, h, computer.ComputerID).OwedRevocations; len(owed) != 0 {
		t.Fatalf("owed revocations after recovery = %#v", owed)
	}
	audit := owedRevocationAuditRows(t, h, computer.ComputerID)
	if len(audit) != len(wantOwed) {
		t.Fatalf("audit rows = %#v", audit)
	}
	for index, row := range audit {
		if row.settlement != owedRevocationSettledRevoked || row.receipt == nil || row.receipt.ComputerID != computer.ComputerID ||
			row.receipt.CommittedAt.IsZero() {
			t.Fatalf("audit row %d = %#v, want a revoked settlement with a receipt", index, row)
		}
		switch row.scope {
		case ComputerRevocationScopeAttempt:
			if row.receipt.ComputerAttemptID != first.Lease.AttemptID || len(row.receipt.PreservedComputerAttemptIDs) != 0 {
				t.Fatalf("attempt-scoped settlement receipt = %#v", row.receipt)
			}
		case ComputerRevocationScopeRevokeAll:
			if row.receipt.ComputerAttemptID != "" ||
				!slices.Equal(row.receipt.PreservedComputerAttemptIDs, []string{second.Lease.AttemptID}) {
				t.Fatalf("late Computer-wide settlement receipt = %#v, want it to preserve %s", row.receipt, second.Lease.AttemptID)
			}
		}
	}
	taken := ledger.taken()
	if len(taken) != len(wantOwed) {
		t.Fatalf("run ledger took %d revocations, want %d: %#v", len(taken), len(wantOwed), taken)
	}
	for _, request := range taken {
		if request.ComputerAttemptID != "" && (request.RevokeAll || request.ComputerAttemptID != first.Lease.AttemptID ||
			len(request.PreserveComputerAttemptIDs) != 0 || request.Reason != "attempt_terminal") {
			t.Fatalf("attempt-scoped settlement widened: %#v", request)
		}
	}
	if ledger.active(first.Lease.AttemptID) || !ledger.active(second.Lease.AttemptID) {
		t.Fatalf("after settlement first active=%t second active=%t, want the completed attempt revoked and the new one live",
			ledger.active(first.Lease.AttemptID), ledger.active(second.Lease.AttemptID))
	}

	// Settled rows are settled once: the next heartbeat asks for nothing
	// and rewrites no receipt.
	heartbeatComputerNode(t, h, node)
	if len(ledger.taken()) != len(wantOwed) {
		t.Fatalf("a settled revocation was sent again: %#v", ledger.taken())
	}
	for index, row := range owedRevocationAuditRows(t, h, computer.ComputerID) {
		if row.rawReceipt != audit[index].rawReceipt {
			t.Fatalf("audit row %d receipt changed from %s to %s", index, audit[index].rawReceipt, row.rawReceipt)
		}
	}

	// Healthy, a stop settles its own row before answering, and now it
	// covers the running attempt.
	computer = mustGetComputer(t, h, computer.ComputerID)
	status, _, body = h.do(client, http.MethodPut, "/v1/computers/"+computer.ComputerID+"/desired-state",
		computerDesiredRequest(computer, contract.ServiceDesiredStopped, "operator"))
	if status != http.StatusAccepted {
		t.Fatalf("healthy stop status=%d body=%s", status, body)
	}
	if owed := mustGetComputer(t, h, computer.ComputerID).OwedRevocations; len(owed) != 0 {
		t.Fatalf("a healthy stop left its revocation owed: %#v", owed)
	}
	audit = owedRevocationAuditRows(t, h, computer.ComputerID)
	last := audit[len(audit)-1]
	if len(audit) != len(wantOwed)+1 || last.verb != ComputerRevocationVerbStop || last.settlement != owedRevocationSettledRevoked ||
		last.receipt == nil || len(last.receipt.PreservedComputerAttemptIDs) != 0 || ledger.active(second.Lease.AttemptID) {
		t.Fatalf("healthy stop audit = %#v, second active=%t", audit, ledger.active(second.Lease.AttemptID))
	}
}

// TestHungRunLedgerKeepsOwedRevocationsWithinHeartbeatBudget: a run ledger
// that accepts the connection and never answers costs a heartbeat no more
// than the revocation budget #548 set, and the revocation stays owed.
func TestHungRunLedgerKeepsOwedRevocationsWithinHeartbeatBudget(t *testing.T) {
	h, _, node, _ := computerCompletionHarness(t)
	ledger := newOutageLedger(h)
	client := h.client(fabric.Identity{NodeID: "computer-client", Tags: []string{DefaultClientPrincipalTag}})
	computer, _, err := h.store.CreateComputer(t.Context(), CreateComputerRequest{Name: "owed-hung",
		Spec: computerCapabilityJobSpec("computer:owed-hung:v1"), Actor: "operator"})
	if err != nil {
		t.Fatal(err)
	}
	ledger.set(true, false)
	status, _, body := h.do(client, http.MethodPut, "/v1/computers/"+computer.ComputerID+"/desired-state",
		computerDesiredRequest(computer, contract.ServiceDesiredStopped, "operator"))
	assertRunLedgerUnavailable(t, status, body, false, "is owed")

	const budget = 300 * time.Millisecond
	h.server.restoreRevocationBudget = budget
	ledger.set(false, true)
	if elapsed := heartbeatComputerNode(t, h, node); elapsed > budget+2*time.Second || elapsed >= ComputerPolicyClientTimeout {
		t.Fatalf("heartbeat took %s against a %s revocation budget", elapsed, budget)
	}
	owed := mustGetComputer(t, h, computer.ComputerID).OwedRevocations
	if len(owed) != 1 || owed[0].SettleFailures != 2 || !strings.Contains(owed[0].LastFailure, "heartbeat revocation budget") {
		t.Fatalf("owed revocations after a hung run ledger = %#v", owed)
	}

	ledger.set(false, false)
	heartbeatComputerNode(t, h, node)
	if owed := mustGetComputer(t, h, computer.ComputerID).OwedRevocations; len(owed) != 0 {
		t.Fatalf("owed revocations after the run ledger answered = %#v", owed)
	}
}

// TestOwedRevocationSettlementIsExactlyOnceAndNeverWidens covers the store
// half: a row settles once and its receipt is then immutable, a replayed
// settlement is a no-op, and a receipt that does not match the row's scope is
// refused rather than recorded.
func TestOwedRevocationSettlementIsExactlyOnceAndNeverWidens(t *testing.T) {
	h := newIntegrationHarnessWithPolicies(t, map[string]NodePolicy{})
	ctx := t.Context()
	computer, _, err := h.store.CreateComputer(ctx, CreateComputerRequest{Name: "owed-once",
		Spec: computerCapabilityJobSpec("computer:owed-once:v1"), Actor: "operator"})
	if err != nil {
		t.Fatal(err)
	}
	stopped, err := h.store.SetComputerDesiredState(ctx, computer.ComputerID,
		computerDesiredRequest(computer, contract.ServiceDesiredStopped, "operator"))
	if err != nil || stopped.owedRevocationID == 0 {
		t.Fatalf("stop = owed %d err=%v, want an owed revocation", stopped.owedRevocationID, err)
	}
	// A stop that changes nothing loses no authority and owes nothing.
	if again, err := h.store.SetComputerDesiredState(ctx, computer.ComputerID,
		computerDesiredRequest(stopped, contract.ServiceDesiredStopped, "operator")); err != nil || again.owedRevocationID != 0 {
		t.Fatalf("no-op stop = owed %d err=%v", again.owedRevocationID, err)
	}
	work, err := h.store.owedComputerRevocationWork(ctx, stopped.owedRevocationID)
	if err != nil || work.settled || !work.request.RevokeAll || work.request.ComputerAttemptID != "" ||
		len(work.request.PreserveComputerAttemptIDs) != 0 || work.request.Reason != "computer_stopped" {
		t.Fatalf("stop settlement work = %#v err=%v", work, err)
	}
	receipt := contract.ComputerTokenRevocationReceipt{ComputerID: computer.ComputerID, SubmitIntentRevision: 1,
		RevokedGrantCount: 1, CommittedAt: h.clock.Now()}
	wrongScope := receipt
	wrongScope.ComputerAttemptID = "attempt-elsewhere"
	if settled, err := h.store.SettleOwedComputerRevocation(ctx, stopped.owedRevocationID, work.request, wrongScope); err == nil || settled {
		t.Fatalf("an attempt-scoped receipt settled a Computer-wide row: settled=%t err=%v", settled, err)
	}
	if settled, err := h.store.SettleOwedComputerRevocation(ctx, stopped.owedRevocationID, work.request, receipt); err != nil || !settled {
		t.Fatalf("first settlement = %t err=%v", settled, err)
	}
	replayed := receipt
	replayed.RevokedGrantCount, replayed.CommittedAt = 0, h.clock.Now().Add(time.Minute)
	if settled, err := h.store.SettleOwedComputerRevocation(ctx, stopped.owedRevocationID, work.request, replayed); err != nil || settled {
		t.Fatalf("replayed settlement = %t err=%v, want a no-op", settled, err)
	}
	audit := owedRevocationAuditRows(t, h, computer.ComputerID)
	if len(audit) != 1 || audit[0].receipt == nil || audit[0].receipt.RevokedGrantCount != 1 {
		t.Fatalf("audit after a replayed settlement = %#v", audit)
	}
	if _, err := h.store.db.Exec(`UPDATE computer_owed_revocations SET receipt_json=X'7B7D' WHERE revocation_id=?`,
		stopped.owedRevocationID); err == nil {
		t.Fatal("a settled revocation's receipt was rewritable")
	}
	if _, err := h.store.db.Exec(`DELETE FROM computer_owed_revocations WHERE revocation_id=?`, stopped.owedRevocationID); err == nil {
		t.Fatal("a revocation audit row was deletable")
	}
	if err := h.store.RecordOwedComputerRevocationFailure(ctx, stopped.owedRevocationID, "late failure"); err != nil {
		t.Fatalf("a failure note on a settled row = %v, want a silent no-op", err)
	}

	// An attempt-scoped row asks for exactly its attempt, and only an
	// exactly matching receipt settles it.
	tx, err := h.store.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	attemptRow, err := recordOwedComputerRevocation(ctx, tx, owedRevocationRecord{computerID: computer.ComputerID,
		hostNodeID: "computer-node", verb: ComputerRevocationVerbAttemptCompletion, reason: "attempt_terminal",
		attemptID: "attempt-done"}, h.clock.Now())
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	work, err = h.store.owedComputerRevocationWork(ctx, attemptRow)
	if err != nil || work.request.RevokeAll || work.request.ComputerAttemptID != "attempt-done" ||
		work.request.NewSubmitIntentRevision != 0 || len(work.request.PreserveComputerAttemptIDs) != 0 {
		t.Fatalf("attempt settlement work = %#v err=%v", work, err)
	}
	widened := ComputerTokenRevocation{ComputerID: computer.ComputerID, NewSubmitIntentRevision: 1, RevokeAll: true, Reason: "attempt_terminal"}
	if settled, err := h.store.SettleOwedComputerRevocation(ctx, attemptRow, widened, receipt); err == nil || settled {
		t.Fatalf("a revoke-all settled an attempt-scoped row: settled=%t err=%v", settled, err)
	}
	attemptReceipt := contract.ComputerTokenRevocationReceipt{ComputerID: computer.ComputerID, ComputerAttemptID: "attempt-done",
		CommittedAt: h.clock.Now()}
	if settled, err := h.store.SettleOwedComputerRevocation(ctx, attemptRow, work.request, attemptReceipt); err != nil || !settled {
		t.Fatalf("attempt settlement = %t err=%v", settled, err)
	}
}

// TestOwedRevocationWithoutRunLedgerIsClosedNotOwed: an installation that
// names no run ledger mints no Computer passes, so a stop owes nothing and
// the row says why.
func TestOwedRevocationWithoutRunLedgerIsClosedNotOwed(t *testing.T) {
	h := newIntegrationHarnessWithPolicies(t, map[string]NodePolicy{})
	client := h.client(fabric.Identity{NodeID: "computer-client", Tags: []string{DefaultClientPrincipalTag}})
	computer, _, err := h.store.CreateComputer(t.Context(), CreateComputerRequest{Name: "owed-no-ledger",
		Spec: computerCapabilityJobSpec("computer:owed-no-ledger:v1"), Actor: "operator"})
	if err != nil {
		t.Fatal(err)
	}
	status, _, body := h.do(client, http.MethodPut, "/v1/computers/"+computer.ComputerID+"/desired-state",
		computerDesiredRequest(computer, contract.ServiceDesiredStopped, "operator"))
	if status != http.StatusAccepted {
		t.Fatalf("stop without a run ledger status=%d body=%s", status, body)
	}
	if owed := mustGetComputer(t, h, computer.ComputerID).OwedRevocations; len(owed) != 0 {
		t.Fatalf("owed revocations without a run ledger = %#v", owed)
	}
	audit := owedRevocationAuditRows(t, h, computer.ComputerID)
	if len(audit) != 1 || audit[0].settlement != owedRevocationSettledNoRunLedger || audit[0].receipt != nil {
		t.Fatalf("audit without a run ledger = %#v", audit)
	}
}
