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
// refuses), hung (it never answers), held (each request waits for release),
// and brought back. Up, it applies L3's revocation rules to its grants
// (l3/computer_tokens.go): an attempt-scoped request ends only that attempt,
// a revoke-all ends every grant, and every request is idempotent.
type outageLedger struct {
	mu       sync.Mutex
	down     bool
	hang     bool
	hold     chan struct{}
	arrived  chan struct{}
	onAnswer func()
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

// holdRequests parks every request until the returned release is called;
// arrived closes when the first one is parked.
func (ledger *outageLedger) holdRequests() (arrived <-chan struct{}, release func()) {
	ledger.mu.Lock()
	defer ledger.mu.Unlock()
	ledger.hold, ledger.arrived = make(chan struct{}), make(chan struct{})
	hold := ledger.hold
	return ledger.arrived, func() {
		ledger.mu.Lock()
		ledger.hold = nil
		ledger.mu.Unlock()
		close(hold)
	}
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
	down, hang, hold, arrived, onAnswer := ledger.down, ledger.hang, ledger.hold, ledger.arrived, ledger.onAnswer
	if hold != nil && arrived != nil {
		close(arrived)
		ledger.arrived = nil
	}
	ledger.mu.Unlock()
	if hang {
		<-ctx.Done()
		return contract.ComputerTokenRevocationReceipt{}, ctx.Err()
	}
	if down {
		return contract.ComputerTokenRevocationReceipt{}, errors.New(
			`l1: revoke Computer tokens: Post "http://run-ledger.invalid/v1/computer-token/revoke": dial: connection refused`)
	}
	if hold != nil {
		<-hold
	}
	ledger.mu.Lock()
	ledger.requests = append(ledger.requests, request)
	count := 0
	for attemptID, revoked := range ledger.grants {
		if revoked || (request.ComputerAttemptID != "" && attemptID != request.ComputerAttemptID) ||
			(request.ComputerAttemptID == "" && !request.RevokeAll) {
			continue
		}
		ledger.grants[attemptID] = true
		count++
	}
	ledger.mu.Unlock()
	if onAnswer != nil {
		onAnswer()
	}
	return contract.ComputerTokenRevocationReceipt{ComputerID: request.ComputerID, ComputerAttemptID: request.ComputerAttemptID,
		SubmitIntentRevision: request.NewSubmitIntentRevision, RevokedGrantCount: count, CommittedAt: time.Now()}, nil
}

type owedRevocationAudit struct {
	revocationID int64
	verb         ComputerRevocationVerb
	scope        ComputerRevocationScope
	recorded     []string
	settlement   string
	record       *ComputerRevocationSettlement
	rawRecord    string
}

func owedRevocationAuditRows(t *testing.T, h *integrationHarness, computerID string) []owedRevocationAudit {
	t.Helper()
	rows, err := h.store.db.Query(`SELECT revocation_id, verb, scope, recorded_attempt_ids_json, settlement, receipt_json
		FROM computer_owed_revocations WHERE computer_id=? ORDER BY revocation_id`, computerID)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var audit []owedRevocationAudit
	for rows.Next() {
		var row owedRevocationAudit
		var recorded, payload []byte
		if err := rows.Scan(&row.revocationID, &row.verb, &row.scope, &recorded, &row.settlement, &payload); err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal(recorded, &row.recorded); err != nil {
			t.Fatal(err)
		}
		if payload != nil {
			row.rawRecord = string(payload)
			row.record = &ComputerRevocationSettlement{}
			if err := json.Unmarshal(payload, row.record); err != nil {
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

// owedOutage drives a running Computer through a run-ledger outage: a stop
// and the completion it drives each commit, answer as #548 made them, and
// owe a revocation of the running attempt; the restart that follows owes
// nothing, because no attempt could hold a pass when it began.
func owedOutage(t *testing.T, name string) (*integrationHarness, *outageLedger, Node, Computer, *Claim) {
	t.Helper()
	h, _, node, agent := computerCompletionHarness(t)
	ledger := newOutageLedger(h)
	client := h.client(fabric.Identity{NodeID: "computer-client", Tags: []string{DefaultClientPrincipalTag}})
	computer, _, err := h.store.CreateComputer(t.Context(), CreateComputerRequest{Name: name,
		Spec: computerCapabilityJobSpec("computer:" + name + ":v1"), Actor: "operator"})
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
		ComputerRestartRequest{ComputerMutationPrecondition: computerPrecondition(computer, "operator"), IdempotencyKey: name + "-restart"})
	assertRunLedgerUnavailable(t, status, body, false, "the Computer mutation applied", "nothing is owed")
	return h, ledger, node, mustGetComputer(t, h, computer.ComputerID), first
}

// TestOwedRevocationsSettleOnHeartbeatAfterRunLedgerOutage is the #554
// acceptance: every revocation owed through a run-ledger outage lands, once,
// with a receipt, when the run ledger returns, and a late settlement is
// always attempt-scoped: it never sends a revoke-all.
func TestOwedRevocationsSettleOnHeartbeatAfterRunLedgerOutage(t *testing.T) {
	h, ledger, node, computer, first := owedOutage(t, "owed-outage")
	second := startComputerAttempt(t, h, node, nil)
	ledger.mint(second.Lease.AttemptID)

	// Down: the heartbeat still answers, and everything stays owed.
	heartbeatComputerNode(t, h, node)
	owed := mustGetComputer(t, h, computer.ComputerID).OwedRevocations
	if len(owed) != 2 || owed[0].Verb != ComputerRevocationVerbStop || owed[0].Scope != ComputerRevocationScopeRevokeAll ||
		owed[1].Verb != ComputerRevocationVerbAttemptCompletion || owed[1].Scope != ComputerRevocationScopeAttempt ||
		owed[1].ComputerAttemptID != first.Lease.AttemptID {
		t.Fatalf("owed revocations during the outage = %#v", owed)
	}
	for _, revocation := range owed {
		if !slices.Equal(revocation.RecordedAttemptIDs, []string{first.Lease.AttemptID}) || revocation.HostNodeID != node.NodeID ||
			revocation.SettleFailures != 2 || !strings.Contains(revocation.LastFailure, "connection refused") {
			t.Fatalf("owed revocation = %#v, want the running attempt recorded and failed by the handler and one heartbeat", revocation)
		}
	}
	if len(ledger.taken()) != 0 || !ledger.active(first.Lease.AttemptID) {
		t.Fatalf("a down run ledger took revocations: %#v", ledger.taken())
	}

	// Back: one heartbeat settles every row with its receipts.
	ledger.set(false, false)
	heartbeatComputerNode(t, h, node)
	if owed := mustGetComputer(t, h, computer.ComputerID).OwedRevocations; len(owed) != 0 {
		t.Fatalf("owed revocations after recovery = %#v", owed)
	}
	audit := owedRevocationAuditRows(t, h, computer.ComputerID)
	wantSettlements := []string{owedRevocationSettledRevoked, owedRevocationSettledRevoked, owedRevocationSettledNothingToRevoke}
	if len(audit) != len(wantSettlements) {
		t.Fatalf("audit rows = %#v", audit)
	}
	for index, row := range audit {
		if row.settlement != wantSettlements[index] {
			t.Fatalf("audit row %d settlement = %q, want %q", index, row.settlement, wantSettlements[index])
		}
		if row.settlement != owedRevocationSettledRevoked {
			continue
		}
		if row.record == nil || row.record.RevokeAll != nil || len(row.record.Attempts) != 1 ||
			row.record.Attempts[0].ComputerAttemptID != first.Lease.AttemptID || row.record.Attempts[0].CommittedAt.IsZero() {
			t.Fatalf("late settlement record %d = %#v, want one receipt for %s", index, row.record, first.Lease.AttemptID)
		}
	}
	for _, request := range ledger.taken() {
		if request.RevokeAll || request.ComputerAttemptID != first.Lease.AttemptID || request.NewSubmitIntentRevision != 0 {
			t.Fatalf("a late settlement sent %#v, want only attempt-scoped revocations of %s", request, first.Lease.AttemptID)
		}
	}
	if taken := len(ledger.taken()); taken != 2 {
		t.Fatalf("run ledger took %d revocations, want 2", taken)
	}
	if ledger.active(first.Lease.AttemptID) || !ledger.active(second.Lease.AttemptID) {
		t.Fatalf("after settlement first active=%t second active=%t", ledger.active(first.Lease.AttemptID), ledger.active(second.Lease.AttemptID))
	}

	// Settled once: the next heartbeat asks for nothing and rewrites nothing.
	heartbeatComputerNode(t, h, node)
	if len(ledger.taken()) != 2 {
		t.Fatalf("a settled revocation was sent again: %#v", ledger.taken())
	}
	for index, row := range owedRevocationAuditRows(t, h, computer.ComputerID) {
		if row.rawRecord != audit[index].rawRecord {
			t.Fatalf("audit row %d changed from %s to %s", index, audit[index].rawRecord, row.rawRecord)
		}
	}

	// Healthy, a stop still sends its revoke-all right after it commits and
	// settles with that receipt.
	computer = mustGetComputer(t, h, computer.ComputerID)
	client := h.client(fabric.Identity{NodeID: "computer-client", Tags: []string{DefaultClientPrincipalTag}})
	status, _, body := h.do(client, http.MethodPut, "/v1/computers/"+computer.ComputerID+"/desired-state",
		computerDesiredRequest(computer, contract.ServiceDesiredStopped, "operator"))
	if status != http.StatusAccepted {
		t.Fatalf("healthy stop status=%d body=%s", status, body)
	}
	audit = owedRevocationAuditRows(t, h, computer.ComputerID)
	last := audit[len(audit)-1]
	if len(audit) != 4 || last.verb != ComputerRevocationVerbStop || last.settlement != owedRevocationSettledRevoked ||
		last.record == nil || last.record.RevokeAll == nil || !slices.Equal(last.recorded, []string{second.Lease.AttemptID}) ||
		ledger.active(second.Lease.AttemptID) {
		t.Fatalf("healthy stop audit = %#v, second active=%t", audit, ledger.active(second.Lease.AttemptID))
	}
}

// TestLateSettlementSparesAnAttemptMintedWhileItIsInFlight is the round-1
// review race made deterministic: the late settlement is in flight at the run
// ledger when the Computer's next attempt is claimed and minted a pass, and
// only then does the settlement complete. The next attempt's pass survives,
// because a late settlement names the attempts it revokes and that attempt
// was never one of them.
func TestLateSettlementSparesAnAttemptMintedWhileItIsInFlight(t *testing.T) {
	h, ledger, node, computer, first := owedOutage(t, "owed-race")
	ledger.set(false, false)
	arrived, release := ledger.holdRequests()
	agent := h.client(fabric.Identity{NodeID: "fabric-computer-node", Tags: []string{DefaultAgentPrincipalTag}})
	heartbeat := make(chan error, 1)
	go func() {
		status, _, body, err := doRequest(agent, http.MethodPost, "/v1/agent/nodes/"+node.NodeID+"/heartbeat", heartbeatRequestForNode(node))
		if err == nil && status != http.StatusOK {
			err = fmt.Errorf("heartbeat status=%d body=%s", status, body)
		}
		heartbeat <- err
	}()
	select {
	case <-arrived:
	case <-time.After(5 * time.Second):
		t.Fatal("the late settlement never reached the run ledger")
	}
	// In flight: the next attempt is claimed and minted its pass.
	second := startComputerAttempt(t, h, node, nil)
	ledger.mint(second.Lease.AttemptID)
	release()
	if err := <-heartbeat; err != nil {
		t.Fatal(err)
	}

	if !ledger.active(second.Lease.AttemptID) {
		t.Fatalf("the late settlement revoked the pass of attempt %s, minted while it was in flight: %#v",
			second.Lease.AttemptID, ledger.taken())
	}
	if ledger.active(first.Lease.AttemptID) {
		t.Fatal("the late settlement left the stopped attempt's pass active")
	}
	if owed := mustGetComputer(t, h, computer.ComputerID).OwedRevocations; len(owed) != 0 {
		t.Fatalf("owed revocations after the race = %#v", owed)
	}
}

// TestManyHistoricalAttemptsNeverBlockOtherOwedRevocations: a Computer with
// hundreds of past attempts records only the attempts that could hold a pass,
// so its row settles with one request, and rows beyond one heartbeat's share
// settle on the next.
func TestManyHistoricalAttemptsNeverBlockOtherOwedRevocations(t *testing.T) {
	h, _, node, _ := computerCompletionHarness(t)
	ledger := newOutageLedger(h)
	client := h.client(fabric.Identity{NodeID: "computer-client", Tags: []string{DefaultClientPrincipalTag}})
	ctx := t.Context()
	computer, _, err := h.store.CreateComputer(ctx, CreateComputerRequest{Name: "owed-history",
		Spec: computerCapabilityJobSpec("computer:owed-history:v1"), Actor: "operator"})
	if err != nil {
		t.Fatal(err)
	}
	live := startComputerAttempt(t, h, node, nil)
	ledger.mint(live.Lease.AttemptID)
	for index := range 300 {
		if _, err := h.store.db.Exec(`INSERT INTO attempts(attempt_id, job_id, node_id, boot_session_id, state, fencing_token,
			lease_expires_ns, created_ns, updated_ns) VALUES(?, ?, ?, ?, ?, '1', 0, 0, 0)`,
			fmt.Sprintf("attempt-history-%03d", index), live.Job.JobID, node.NodeID, node.BootSessionID, contract.AttemptLost); err != nil {
			t.Fatal(err)
		}
	}
	computer = mustGetComputer(t, h, computer.ComputerID)
	ledger.set(true, false)
	status, _, body := h.do(client, http.MethodPut, "/v1/computers/"+computer.ComputerID+"/desired-state",
		computerDesiredRequest(computer, contract.ServiceDesiredStopped, "operator"))
	assertRunLedgerUnavailable(t, status, body, false, "is owed")
	owed := mustGetComputer(t, h, computer.ComputerID).OwedRevocations
	if len(owed) != 1 || !slices.Equal(owed[0].RecordedAttemptIDs, []string{live.Lease.AttemptID}) {
		t.Fatalf("owed revocation over 300 past attempts = %#v, want only the live attempt recorded", owed)
	}
	// Twenty more owed rows on the same host, more than one heartbeat's share.
	tx, err := h.store.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	for index := range 20 {
		if _, err := recordOwedComputerRevocation(ctx, tx, owedRevocationRecord{computerID: fmt.Sprintf("computer-other-%02d", index),
			hostNodeID: node.NodeID, verb: ComputerRevocationVerbStop, reason: "computer_stopped",
			holdingAttempts: []string{fmt.Sprintf("attempt-other-%02d", index)}}, h.clock.Now()); err != nil {
			t.Fatal(err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	ledger.set(false, false)
	heartbeatComputerNode(t, h, node)
	if taken := ledger.taken(); len(taken) != MaxOwedRevocationsPerHeartbeat || taken[0].ComputerID == "" {
		t.Fatalf("first heartbeat sent %d revocations, want %d", len(taken), MaxOwedRevocationsPerHeartbeat)
	}
	if owed := mustGetComputer(t, h, computer.ComputerID).OwedRevocations; len(owed) != 0 {
		t.Fatalf("the oldest row, over 300 past attempts, is still owed: %#v", owed)
	}
	heartbeatComputerNode(t, h, node)
	var remaining int
	if err := h.store.db.QueryRow(`SELECT COUNT(*) FROM computer_owed_revocations WHERE settled_ns IS NULL`).Scan(&remaining); err != nil {
		t.Fatal(err)
	}
	if remaining != 0 || len(ledger.taken()) != 21 {
		t.Fatalf("after two heartbeats %d rows owed and %d revocations sent, want 0 and 21", remaining, len(ledger.taken()))
	}
}

// TestHeartbeatAnswersInTimeWhileSettlementWritesAreLocked: the run ledger
// answers, and SQLite is write-locked from that moment until well past the
// heartbeat's revocation budget, across the owed-revocation writes. The
// driver does not interrupt a lock wait when a context ends, so without a
// bounded wait the writes would sit on SQLite's 5s busy timeout. They are
// skipped at the budget instead: the row stays owed, the heartbeat answers,
// and the next heartbeat records the settlement.
func TestHeartbeatAnswersInTimeWhileSettlementWritesAreLocked(t *testing.T) {
	h, _, node, _ := computerCompletionHarness(t)
	ledger := newOutageLedger(h)
	client := h.client(fabric.Identity{NodeID: "computer-client", Tags: []string{DefaultClientPrincipalTag}})
	computer, _, err := h.store.CreateComputer(t.Context(), CreateComputerRequest{Name: "owed-locked",
		Spec: computerCapabilityJobSpec("computer:owed-locked:v1"), Actor: "operator"})
	if err != nil {
		t.Fatal(err)
	}
	live := startComputerAttempt(t, h, node, nil)
	ledger.mint(live.Lease.AttemptID)
	computer = mustGetComputer(t, h, computer.ComputerID)
	ledger.set(true, false)
	status, _, body := h.do(client, http.MethodPut, "/v1/computers/"+computer.ComputerID+"/desired-state",
		computerDesiredRequest(computer, contract.ServiceDesiredStopped, "operator"))
	assertRunLedgerUnavailable(t, status, body, false, "is owed")

	const budget = 600 * time.Millisecond
	const lockHeld = budget + 400*time.Millisecond
	h.server.restoreRevocationBudget = budget
	locked, released := make(chan struct{}), make(chan error, 1)
	var once sync.Once
	ledger.mu.Lock()
	ledger.onAnswer = func() {
		once.Do(func() {
			conn, err := h.store.db.Conn(context.Background())
			if err == nil {
				_, err = conn.ExecContext(context.Background(), `BEGIN IMMEDIATE`)
			}
			if err != nil {
				released <- err
				close(locked)
				return
			}
			close(locked)
			go func() {
				time.Sleep(lockHeld)
				_, err := conn.ExecContext(context.Background(), `ROLLBACK`)
				conn.Close()
				released <- err
			}()
		})
	}
	ledger.down = false
	ledger.mu.Unlock()

	elapsed := heartbeatComputerNode(t, h, node)
	select {
	case <-locked:
	default:
		t.Fatal("the run ledger never answered, so SQLite was never locked")
	}
	if err := <-released; err != nil {
		t.Fatal(err)
	}
	// The rest of the heartbeat writes too, so it can finish only once the
	// lock is gone; what must not happen is a wait on the busy timeout.
	if elapsed > lockHeld+time.Second || elapsed >= ComputerPolicyClientTimeout {
		t.Fatalf("heartbeat took %s with SQLite locked for %s against a %s budget", elapsed, lockHeld, budget)
	}
	if owed := mustGetComputer(t, h, computer.ComputerID).OwedRevocations; len(owed) != 1 {
		t.Fatalf("owed revocations after a locked settlement = %#v, want the writes skipped and the row still owed", owed)
	}
	ledger.mu.Lock()
	ledger.onAnswer = nil
	ledger.mu.Unlock()
	heartbeatComputerNode(t, h, node)
	if owed := mustGetComputer(t, h, computer.ComputerID).OwedRevocations; len(owed) != 0 {
		t.Fatalf("owed revocations after the lock cleared = %#v", owed)
	}
	if ledger.active(live.Lease.AttemptID) {
		t.Fatal("the stopped attempt's pass is still active")
	}
}

// TestHungRunLedgerKeepsOwedRevocationsWithinHeartbeatBudget: a run ledger
// that accepts the connection and never answers costs a heartbeat no more
// than its revocation budget, and the revocation stays owed.
func TestHungRunLedgerKeepsOwedRevocationsWithinHeartbeatBudget(t *testing.T) {
	h, _, node, _ := computerCompletionHarness(t)
	ledger := newOutageLedger(h)
	client := h.client(fabric.Identity{NodeID: "computer-client", Tags: []string{DefaultClientPrincipalTag}})
	computer, _, err := h.store.CreateComputer(t.Context(), CreateComputerRequest{Name: "owed-hung",
		Spec: computerCapabilityJobSpec("computer:owed-hung:v1"), Actor: "operator"})
	if err != nil {
		t.Fatal(err)
	}
	live := startComputerAttempt(t, h, node, nil)
	ledger.mint(live.Lease.AttemptID)
	computer = mustGetComputer(t, h, computer.ComputerID)
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

// TestOwedRevocationRecordsAttemptsBeforeTheMutation: removal marks the
// running attempt lost in its own transaction, so the attempts it owes a
// revocation for must be read before that; so must a stop's.
func TestOwedRevocationRecordsAttemptsBeforeTheMutation(t *testing.T) {
	for _, verb := range []ComputerRevocationVerb{ComputerRevocationVerbStop, ComputerRevocationVerbRemove} {
		t.Run(string(verb), func(t *testing.T) {
			h, _, node, _ := computerCompletionHarness(t)
			ctx := t.Context()
			computer, _, err := h.store.CreateComputer(ctx, CreateComputerRequest{Name: "owed-before-" + string(verb),
				Spec: computerCapabilityJobSpec("computer:owed-before-" + string(verb) + ":v1"), Actor: "operator"})
			if err != nil {
				t.Fatal(err)
			}
			live := startComputerAttempt(t, h, node, nil)
			computer = mustGetComputer(t, h, computer.ComputerID)
			var mutated Computer
			if verb == ComputerRevocationVerbStop {
				mutated, err = h.store.SetComputerDesiredState(ctx, computer.ComputerID,
					computerDesiredRequest(computer, contract.ServiceDesiredStopped, "operator"))
			} else {
				mutated, err = h.store.RemoveComputer(ctx, computer.ComputerID,
					ComputerRemoveRequest{ComputerMutationPrecondition: computerPrecondition(computer, "operator")})
			}
			if err != nil || mutated.owedRevocationID == 0 {
				t.Fatalf("%s = owed %d err=%v", verb, mutated.owedRevocationID, err)
			}
			var state contract.AttemptState
			if err := h.store.db.QueryRow(`SELECT state FROM attempts WHERE attempt_id=?`, live.Lease.AttemptID).Scan(&state); err != nil {
				t.Fatal(err)
			}
			if verb == ComputerRevocationVerbRemove && state != contract.AttemptLost {
				t.Fatalf("removal left the attempt %q, so this test no longer proves the read comes first", state)
			}
			audit := owedRevocationAuditRows(t, h, computer.ComputerID)
			if len(audit) != 1 || audit[0].verb != verb || !slices.Equal(audit[0].recorded, []string{live.Lease.AttemptID}) {
				t.Fatalf("%s audit = %#v, want the attempt that was running before it recorded", verb, audit)
			}
		})
	}
}

// TestOwedRevocationSettlementIsExactlyOnceAndNeverWidens covers the store
// half: a row settles once and is then immutable, a replayed settlement is a
// no-op, receipts accumulate until every recorded attempt has one, and a
// receipt for an attempt the row did not record is refused.
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
	receipt := contract.ComputerTokenRevocationReceipt{ComputerID: computer.ComputerID, SubmitIntentRevision: 1,
		RevokedGrantCount: 1, CommittedAt: h.clock.Now()}
	wrongScope := receipt
	wrongScope.ComputerAttemptID = "attempt-elsewhere"
	if settled, err := h.store.SettleOwedComputerRevocationRevokeAll(ctx, stopped.owedRevocationID, wrongScope); err == nil || settled {
		t.Fatalf("an attempt-scoped receipt settled a revoke-all: settled=%t err=%v", settled, err)
	}
	if settled, err := h.store.SettleOwedComputerRevocationRevokeAll(ctx, stopped.owedRevocationID, receipt); err != nil || !settled {
		t.Fatalf("first settlement = %t err=%v", settled, err)
	}
	replayed := receipt
	replayed.RevokedGrantCount, replayed.CommittedAt = 0, h.clock.Now().Add(time.Minute)
	if settled, err := h.store.SettleOwedComputerRevocationRevokeAll(ctx, stopped.owedRevocationID, replayed); err != nil || settled {
		t.Fatalf("replayed settlement = %t err=%v, want a no-op", settled, err)
	}
	audit := owedRevocationAuditRows(t, h, computer.ComputerID)
	if len(audit) != 1 || audit[0].record == nil || audit[0].record.RevokeAll == nil || audit[0].record.RevokeAll.RevokedGrantCount != 1 {
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

	// Two recorded attempts: receipts accumulate, only recorded attempts
	// count, and the row settles when both are in.
	tx, err := h.store.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	twoAttempts, err := recordOwedComputerRevocation(ctx, tx, owedRevocationRecord{computerID: computer.ComputerID,
		hostNodeID: "computer-node", verb: ComputerRevocationVerbReimage, reason: "computer_reimaged",
		holdingAttempts: []string{"attempt-a", "attempt-b"}}, h.clock.Now())
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	attemptReceipt := func(attemptID string) contract.ComputerTokenRevocationReceipt {
		return contract.ComputerTokenRevocationReceipt{ComputerID: computer.ComputerID, ComputerAttemptID: attemptID, CommittedAt: h.clock.Now()}
	}
	if settled, err := h.store.RecordOwedComputerAttemptRevocations(ctx, twoAttempts,
		[]contract.ComputerTokenRevocationReceipt{attemptReceipt("attempt-successor")}); err == nil || settled {
		t.Fatalf("a receipt for an unrecorded attempt was recorded: settled=%t err=%v", settled, err)
	}
	if settled, err := h.store.RecordOwedComputerAttemptRevocations(ctx, twoAttempts,
		[]contract.ComputerTokenRevocationReceipt{attemptReceipt("attempt-b")}); err != nil || settled {
		t.Fatalf("one of two receipts = settled %t err=%v, want recorded and still owed", settled, err)
	}
	owed := mustGetComputer(t, h, computer.ComputerID).OwedRevocations
	if len(owed) != 1 || !slices.Equal(owed[0].RevokedAttemptIDs, []string{"attempt-b"}) {
		t.Fatalf("partially settled revocation = %#v", owed)
	}
	if settled, err := h.store.RecordOwedComputerAttemptRevocations(ctx, twoAttempts,
		[]contract.ComputerTokenRevocationReceipt{attemptReceipt("attempt-b"), attemptReceipt("attempt-a")}); err != nil || !settled {
		t.Fatalf("both receipts = settled %t err=%v", settled, err)
	}
	audit = owedRevocationAuditRows(t, h, computer.ComputerID)
	if last := audit[len(audit)-1]; last.record == nil || len(last.record.Attempts) != 2 ||
		last.record.Attempts[0].ComputerAttemptID != "attempt-a" || last.record.Attempts[1].ComputerAttemptID != "attempt-b" {
		t.Fatalf("two-attempt settlement = %#v", last)
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
	if len(audit) != 1 || audit[0].settlement != owedRevocationSettledNoRunLedger || audit[0].record != nil {
		t.Fatalf("audit without a run ledger = %#v", audit)
	}
}
