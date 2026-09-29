package l1

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
)

// unreachableRunLedger is the deployment wefty #548 ran for two hours without
// being able to name: L1 holding a run-ledger address that never answers.
func unreachableRunLedger() ComputerTokenRevoker {
	return recordingComputerTokenRevoker{revoke: func(context.Context, ComputerTokenRevocation) (contract.ComputerTokenRevocationReceipt, error) {
		return contract.ComputerTokenRevocationReceipt{}, errors.New(
			`l1: revoke Computer tokens: Post "http://run-ledger.invalid/v1/computer-token/revoke": dial: connection refused`)
	}}
}

type recordedLog struct {
	mu    sync.Mutex
	lines []string
}

func (log *recordedLog) record(format string, args ...any) {
	log.mu.Lock()
	defer log.mu.Unlock()
	log.lines = append(log.lines, fmt.Sprintf(format, args...))
}

func (log *recordedLog) text() string {
	log.mu.Lock()
	defer log.mu.Unlock()
	return strings.Join(log.lines, "\n")
}

func assertRunLedgerUnavailable(t *testing.T, status int, body []byte, wantRetryable bool, wantMessageFragments ...string) {
	t.Helper()
	if status != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d body=%s", status, http.StatusServiceUnavailable, body)
	}
	var response contract.ErrorResponse
	if err := json.Unmarshal(body, &response); err != nil {
		t.Fatal(err)
	}
	if response.Error.Code != contract.ErrorRunLedgerUnavailable {
		t.Fatalf("error code = %q, want %q body=%s", response.Error.Code, contract.ErrorRunLedgerUnavailable, body)
	}
	if response.Error.Retryable != wantRetryable {
		t.Fatalf("retryable = %t, want %t; body=%s", response.Error.Retryable, wantRetryable, body)
	}
	if response.Error.Message == "internal server error" {
		t.Fatalf("the refusal was scrubbed instead of typed: %s", body)
	}
	if !strings.Contains(response.Error.Message, "run ledger") {
		t.Fatalf("error message = %q, want it to name the run ledger", response.Error.Message)
	}
	for _, fragment := range wantMessageFragments {
		if !strings.Contains(response.Error.Message, fragment) {
			t.Fatalf("error message = %q, want it to say %q", response.Error.Message, fragment)
		}
	}
}

// TestComputerAuthorityLossRefusesTypedWhenRunLedgerIsUnreachable reproduces
// the shape wefty #548 found in three wedged L1 databases: every operator verb
// that takes a running Computer's authority away applied its mutation and then
// answered an untyped, scrubbed "internal server error, retryable: true",
// which invited a retry that could only conflict. The verbs may refuse, but
// they must refuse by name, say that the mutation applied, and not invite a
// retry that will not perform the revocation.
func TestComputerAuthorityLossRefusesTypedWhenRunLedgerIsUnreachable(t *testing.T) {
	for _, verb := range []struct {
		name    string
		mutate  func(t *testing.T, h *integrationHarness, client *http.Client, computer Computer) (int, []byte)
		applied func(t *testing.T, computer Computer)
	}{
		{
			name: "stop",
			mutate: func(t *testing.T, h *integrationHarness, client *http.Client, computer Computer) (int, []byte) {
				status, _, body := h.do(client, http.MethodPut, "/v1/computers/"+computer.ComputerID+"/desired-state",
					computerDesiredRequest(computer, contract.ServiceDesiredStopped, "operator"))
				return status, body
			},
			applied: func(t *testing.T, computer Computer) {
				t.Helper()
				if computer.DesiredState != contract.ServiceDesiredStopped {
					t.Fatalf("stop refused but did not apply: desired state = %q", computer.DesiredState)
				}
			},
		},
		{
			name: "remove",
			mutate: func(t *testing.T, h *integrationHarness, client *http.Client, computer Computer) (int, []byte) {
				status, _, body := h.do(client, http.MethodPost, "/v1/computers/"+computer.ComputerID+"/remove",
					ComputerRemoveRequest{ComputerMutationPrecondition: computerPrecondition(computer, "operator")})
				return status, body
			},
			applied: func(t *testing.T, computer Computer) {
				t.Helper()
				if computer.ReconfigurationPhase == ComputerReconfigurationStable {
					t.Fatalf("remove refused but did not apply: phase = %q", computer.ReconfigurationPhase)
				}
			},
		},
	} {
		t.Run(verb.name, func(t *testing.T) {
			h := newIntegrationHarnessWithPolicies(t, map[string]NodePolicy{})
			logs := &recordedLog{}
			h.server.logf = logs.record
			h.server.computerTokenRevoker = unreachableRunLedger()
			client := h.client(fabric.Identity{NodeID: "computer-client", Tags: []string{DefaultClientPrincipalTag}})
			computer, _, err := h.store.CreateComputer(context.Background(), CreateComputerRequest{
				Name: "wedge-" + verb.name, Spec: computerCapabilityJobSpec("computer:" + verb.name), Actor: "operator",
			})
			if err != nil {
				t.Fatal(err)
			}
			status, body := verb.mutate(t, h, client, computer)
			// Post-commit: not retryable, because nothing the caller repeats
			// performs the revocation, and it does not need to: L1 holds it
			// owed (#554), and L3's live-scope check already refuses the old
			// tokens.
			assertRunLedgerUnavailable(t, status, body, false, "the Computer mutation applied",
				"is owed", "L1 recorded it and will retry it", "do not retry the request",
				"live-scope check already refuses")
			after, err := h.store.GetComputer(context.Background(), computer.ComputerID)
			if err != nil {
				t.Fatal(err)
			}
			verb.applied(t, after)
			if len(after.OwedRevocations) != 1 || after.OwedRevocations[0].Verb != ComputerRevocationVerb(verb.name) ||
				after.OwedRevocations[0].Scope != ComputerRevocationScopeRevokeAll || after.OwedRevocations[0].SettleFailures != 1 ||
				!strings.Contains(after.OwedRevocations[0].LastFailure, "connection refused") {
				t.Fatalf("owed revocations after a refused %s = %#v", verb.name, after.OwedRevocations)
			}
			if strings.Contains(logs.text(), "event=l1_internal_error_scrubbed") {
				t.Fatalf("a typed refusal was still logged as a scrubbed internal error: %s", logs.text())
			}
		})
	}
}

// TestHeartbeatSurvivesAnUnreachableRunLedger is the node-health half of
// wefty #548. One Computer waiting on a pre-restore revocation must not take
// the node's whole convergence surface with it: the agent that cannot finish a
// heartbeat withdraws kind:oci while still reporting itself alive and claiming,
// and no operator action short of a fresh L1 database brought it back.
func TestHeartbeatSurvivesAnUnreachableRunLedger(t *testing.T) {
	h, node, computer, source, _ := publishedBackupForStorageCopy(t, 2)
	logs := &recordedLog{}
	h.server.logf = logs.record
	h.server.computerTokenRevoker = unreachableRunLedger()
	if _, _, err := h.store.BeginComputerRestore(context.Background(), computer.ComputerID,
		ComputerRestoreRequest{ComputerMutationPrecondition: computerPrecondition(computer, "operator"),
			BackupID: source.BackupID, IdempotencyKey: "unreachable-run-ledger"}); err != nil {
		t.Fatal(err)
	}
	agentClient := h.client(fabric.Identity{NodeID: "fabric-computer-node", Tags: []string{DefaultAgentPrincipalTag}})
	for heartbeat := 1; heartbeat <= 2; heartbeat++ {
		status, _, body := h.do(agentClient, http.MethodPost, "/v1/agent/nodes/"+node.NodeID+"/heartbeat", heartbeatRequestForNode(node))
		var response HeartbeatResponse
		if status != http.StatusOK || json.Unmarshal(body, &response) != nil {
			t.Fatalf("heartbeat %d status=%d body=%s", heartbeat, status, body)
		}
		// Fails closed on the one blocked restore, open on everything else.
		if len(response.StorageCopyDirectives) != 0 {
			t.Fatalf("heartbeat %d handed out a restore whose authority was never revoked: %#v", heartbeat, response.StorageCopyDirectives)
		}
	}
	if !strings.Contains(logs.text(), "event=l1_restore_revocation_deferred") ||
		!strings.Contains(logs.text(), computer.ComputerID) {
		t.Fatalf("a deferred restore revocation was not named in the log: %s", logs.text())
	}
	owed, err := h.store.GetComputer(context.Background(), computer.ComputerID)
	if err != nil || owed.LastRestoreRevocation != nil {
		t.Fatalf("restore revocation recorded against an unreachable run ledger = %#v err=%v", owed.LastRestoreRevocation, err)
	}
}

// TestScrubbedInternalErrorNamesItsCauseInTheLog covers the fault L1 still
// scrubs. The response stays opaque on purpose; the log must not. In wefty
// #548 L1's entire stderr for a two-hour session was two "listening on" lines
// while it answered thousands of 500s.
func TestScrubbedInternalErrorNamesItsCauseInTheLog(t *testing.T) {
	logs := &recordedLog{}
	server := &Server{logf: logs.record}
	handler := server.observeInternalErrors(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		writeError(w, internalError(errors.New("no such column: retained_result_generation"), "list retained results"))
	}))
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodPut, "/v1/computers/computer-1/desired-state", nil))

	if recorder.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusInternalServerError)
	}
	var response contract.ErrorResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	if response.Error.Message != "internal server error" {
		t.Fatalf("the response stopped scrubbing internal causes: %s", recorder.Body.String())
	}
	line := logs.text()
	for _, want := range []string{
		"event=l1_internal_error_scrubbed",
		"method=PUT",
		"path=/v1/computers/computer-1/desired-state",
		"list retained results",
		"no such column: retained_result_generation",
	} {
		if !strings.Contains(line, want) {
			t.Fatalf("scrubbed-error log = %q, want it to contain %q", line, want)
		}
	}
}

// TestTypedErrorsAreNotLoggedAsScrubbedCauses keeps the new log honest: only
// what the response loses is worth a line.
func TestTypedErrorsAreNotLoggedAsScrubbedCauses(t *testing.T) {
	logs := &recordedLog{}
	server := &Server{logf: logs.record}
	handler := server.observeInternalErrors(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		writeError(w, protocolError(contract.ErrorStaleIntentRevision, "stale Computer intent"))
	}))
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodPut, "/v1/computers/computer-1/desired-state", nil))
	if logs.text() != "" {
		t.Fatalf("a typed refusal was logged as a scrubbed cause: %s", logs.text())
	}
}

// stoppedComputerWithPublishedBackup adds a second restorable Computer to a
// harness built by publishedBackupForStorageCopy, on the same Node.
func stoppedComputerWithPublishedBackup(t *testing.T, h *integrationHarness, node Node, name string) (Computer, Backup) {
	t.Helper()
	ctx := context.Background()
	computer, _, err := h.store.CreateComputer(ctx, CreateComputerRequest{
		Name: name, Spec: computerCapabilityJobSpec("computer:" + name), Actor: "operator",
	})
	if err != nil {
		t.Fatal(err)
	}
	computer, claim := startBackupComputer(t, h, node, computer)
	if _, _, err := h.store.BeginComputerBackup(ctx, computer.ComputerID, ComputerBackupCreateRequest{
		ComputerMutationPrecondition: computerPrecondition(computer, "operator"), IdempotencyKey: name + "-source", AllowPowerOff: true}); err != nil {
		t.Fatal(err)
	}
	finishBackupQuiescence(t, h, claim, name+"-stop")
	directives, err := h.store.ListNodeComputerBackupDirectives(ctx, "fabric-computer-node", node.NodeID, node.BootSessionID)
	if err != nil {
		t.Fatal(err)
	}
	var directive *ComputerBackupDirective
	for index := range directives {
		if directives[index].ComputerID == computer.ComputerID {
			directive = &directives[index]
		}
	}
	if directive == nil {
		t.Fatalf("no Backup directive for %s in %#v", computer.ComputerID, directives)
	}
	backup, resumed := acknowledgeBackup(t, h, node, *directive, successfulBackupReceipt(*directive))
	stopped, err := h.store.SetComputerDesiredState(ctx, resumed.ComputerID, ComputerDesiredStateRequest{
		ComputerMutationPrecondition: computerPrecondition(resumed, "operator"), DesiredState: contract.ServiceDesiredStopped})
	if err != nil {
		t.Fatal(err)
	}
	return stopped, backup
}

// TestHeartbeatBoundsAStalledRunLedger is the review finding on the first
// #548 fix: a run ledger that hangs instead of refusing held the heartbeat for
// the run-ledger client's full 10s, which is also the agent's whole heartbeat
// deadline, so the node failed exactly as before. The heartbeat must answer
// within its revocation budget and withhold only the stalled Computer's
// restore; a Computer whose revocation did answer gets its directive in the
// same pass.
func TestHeartbeatBoundsAStalledRunLedger(t *testing.T) {
	h, node, stalled, stalledBackup, _ := publishedBackupForStorageCopy(t, 2)
	healthy, healthyBackup := stoppedComputerWithPublishedBackup(t, h, node, "healthy-restore")
	ctx := context.Background()
	for _, restore := range []struct {
		computer Computer
		backup   Backup
	}{{stalled, stalledBackup}, {healthy, healthyBackup}} {
		if _, _, err := h.store.BeginComputerRestore(ctx, restore.computer.ComputerID, ComputerRestoreRequest{
			ComputerMutationPrecondition: computerPrecondition(restore.computer, "operator"),
			BackupID:                     restore.backup.BackupID, IdempotencyKey: "stall-" + restore.computer.ComputerID}); err != nil {
			t.Fatal(err)
		}
	}
	const budget = 300 * time.Millisecond
	h.server.restoreRevocationBudget = budget
	logs := &recordedLog{}
	h.server.logf = logs.record
	var stallMu sync.Mutex
	stall := true
	h.server.computerTokenRevoker = recordingComputerTokenRevoker{revoke: func(ctx context.Context, request ComputerTokenRevocation) (contract.ComputerTokenRevocationReceipt, error) {
		stallMu.Lock()
		stalling := stall && request.ComputerID == stalled.ComputerID
		stallMu.Unlock()
		if stalling {
			// A run ledger that accepted the connection and never answers.
			<-ctx.Done()
			return contract.ComputerTokenRevocationReceipt{}, ctx.Err()
		}
		return contract.ComputerTokenRevocationReceipt{ComputerID: request.ComputerID,
			RestoreOperationRevision: request.RestoreOperationRevision, SubmitIntentRevision: request.NewSubmitIntentRevision,
			CommittedAt: h.clock.Now()}, nil
	}}
	agentClient := h.client(fabric.Identity{NodeID: "fabric-computer-node", Tags: []string{DefaultAgentPrincipalTag}})
	heartbeat := func() HeartbeatResponse {
		t.Helper()
		started := time.Now()
		status, _, body := h.do(agentClient, http.MethodPost, "/v1/agent/nodes/"+node.NodeID+"/heartbeat", heartbeatRequestForNode(node))
		elapsed := time.Since(started)
		var response HeartbeatResponse
		if status != http.StatusOK || json.Unmarshal(body, &response) != nil {
			t.Fatalf("heartbeat status=%d body=%s", status, body)
		}
		// Generous against a loaded runner, and still far inside the agent's
		// deadline, which is what the budget exists to protect.
		if elapsed > budget+2*time.Second || elapsed >= ComputerPolicyClientTimeout {
			t.Fatalf("heartbeat took %s against a %s revocation budget", elapsed, budget)
		}
		return response
	}
	restoresFor := func(response HeartbeatResponse) map[string]int {
		byComputer := map[string]int{}
		for _, directive := range response.StorageCopyDirectives {
			if directive.Operation == "restore" {
				byComputer[directive.DestinationComputerID]++
			}
		}
		return byComputer
	}

	first := restoresFor(heartbeat())
	if first[stalled.ComputerID] != 0 {
		t.Fatalf("a restore whose revocation never answered was handed out: %#v", first)
	}
	if first[healthy.ComputerID] != 1 {
		t.Fatalf("the healthy Computer's restore was withheld with the stalled one: %#v", first)
	}
	if text := logs.text(); !strings.Contains(text, "event=l1_restore_revocation_deferred") ||
		!strings.Contains(text, stalled.ComputerID) || !strings.Contains(text, "heartbeat revocation budget") {
		t.Fatalf("the stalled revocation was not named as a budget deferral: %s", text)
	}
	owed, err := h.store.GetComputer(ctx, stalled.ComputerID)
	if err != nil || owed.LastRestoreRevocation != nil {
		t.Fatalf("a stalled revocation was recorded as done = %#v err=%v", owed.LastRestoreRevocation, err)
	}

	stallMu.Lock()
	stall = false
	stallMu.Unlock()
	second := restoresFor(heartbeat())
	if second[stalled.ComputerID] != 1 || second[healthy.ComputerID] != 1 {
		t.Fatalf("the owed revocation was not re-driven on the next pass: %#v", second)
	}
	settled, err := h.store.GetComputer(ctx, stalled.ComputerID)
	if err != nil || settled.LastRestoreRevocation == nil {
		t.Fatalf("the re-driven revocation was not recorded = %#v err=%v", settled.LastRestoreRevocation, err)
	}
}
