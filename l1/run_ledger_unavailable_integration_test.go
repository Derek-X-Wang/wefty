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

func assertRunLedgerUnavailable(t *testing.T, status int, body []byte, wantMessageFragment string) {
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
	if !response.Error.Retryable {
		t.Fatalf("an unreachable run ledger is retryable; body=%s", body)
	}
	if response.Error.Message == "internal server error" {
		t.Fatalf("the refusal was scrubbed instead of typed: %s", body)
	}
	if !strings.Contains(response.Error.Message, "run ledger") || !strings.Contains(response.Error.Message, wantMessageFragment) {
		t.Fatalf("error message = %q, want it to name the run ledger and %q", response.Error.Message, wantMessageFragment)
	}
}

// TestComputerAuthorityLossRefusesTypedWhenRunLedgerIsUnreachable reproduces
// the shape wefty #548 found in three wedged L1 databases: every operator verb
// that takes a running Computer's authority away applied its mutation and then
// answered an untyped, scrubbed "internal server error, retryable: true",
// which invited a retry that could only conflict. The verbs may refuse, but
// they must refuse by name and must say that the mutation applied.
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
			assertRunLedgerUnavailable(t, status, body, "the Computer mutation applied")
			after, err := h.store.GetComputer(context.Background(), computer.ComputerID)
			if err != nil {
				t.Fatal(err)
			}
			verb.applied(t, after)
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
