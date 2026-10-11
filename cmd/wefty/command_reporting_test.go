package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/runner/ocicontrol"
)

// These expectations characterize the process boundary before consolidating
// its two classifiers. Keep the exit numbers and JSON literals independent of
// the production decision and its envelope constructors.
func TestCommandReportingCharacterization(t *testing.T) {
	type reportingCase struct {
		name             string
		err              error
		exit             int
		text             string
		json             string
		jsonOuterContext bool
	}
	cases := []reportingCase{
		{"usage", usageError("bad arguments"), 2, "wefty: bad arguments\n", `{"error":{"code":"invalid_request","message":"bad arguments","retryable":false}}`, false},
		{"run-failed", &runOutcomeError{runID: "run-1", status: contract.RunFailed, reason: "job failed"}, 10, "wefty: run run-1 failed: job failed\n", "", false},
		{"run-no-reason", &runOutcomeError{runID: "run-1", status: contract.RunFailed}, 10, "wefty: run run-1 failed (no reason was recorded; see `wefty inspect run-1 --execution`)\n", "", false},
		{"not-ready", &notReadyError{verdict: "not ready: no nodes"}, 12, "wefty: not ready: no nodes\n", "", false},
		{"wait-timeout", &waitTimeoutError{runID: "run-1", status: contract.RunQueued, waited: time.Second}, 11, "wefty: run run-1 is still queued after 1s\n", "", false},
		{"unavailable", &unavailableError{cause: errors.New("connection refused")}, 13, "wefty: connection refused\n", `{"error":{"code":"unavailable","message":"connection refused","retryable":true}}`, true},
		{"plain", errors.New("local failure"), 1, "wefty: local failure\n", `{"error":{"code":"internal","message":"local failure","retryable":false}}`, true},
		{"deadline", context.DeadlineExceeded, 1, "wefty: context deadline exceeded\n", `{"error":{"code":"internal","message":"context deadline exceeded","retryable":false}}`, true},
		{"canceled", context.Canceled, 1, "wefty: context canceled\n", `{"error":{"code":"internal","message":"context canceled","retryable":false}}`, true},
		{"usage-under-unavailable", &unavailableError{cause: usageError("bad arguments")}, 2, "wefty: bad arguments\n", `{"error":{"code":"unavailable","message":"bad arguments","retryable":true}}`, true},
		{"usage-and-verdict", errors.Join(usageError("bad arguments"), &notReadyError{verdict: "not ready: no nodes"}), 2, "wefty: bad arguments\nnot ready: no nodes\n", "", false},
		{"unknown-custody-and-mutation", errors.Join(&custodyImportOutcomeError{importID: "import-1", outcome: "future-outcome"}, &mutationWaitTimeoutError{message: "observation timed out"}), 14, "wefty: Custody import \"import-1\" is future-outcome\nobservation timed out\n", "", false},
	}
	for _, c := range []struct {
		outcome custodyImportOutcome
		exit    int
	}{
		{custodyImportDeferred, 6}, {custodyImportQuarantined, 7},
		{custodyImportFailed, 8}, {custodyImportSuperseded, 9},
		// Even an unrecognized custody verdict suppresses a second JSON document.
		{"future-outcome", 1},
	} {
		cases = append(cases, reportingCase{
			name: "custody-" + string(c.outcome), exit: c.exit,
			err:  &custodyImportOutcomeError{importID: "import-1", outcome: c.outcome, detail: "recorded verdict"},
			text: fmt.Sprintf("wefty: Custody import %q is %s: recorded verdict\n", "import-1", c.outcome),
		})
	}
	for _, applied := range []struct {
		name  string
		value *bool
	}{
		{"true", reportingBool(true)}, {"false", reportingBool(false)}, {"null", nil},
	} {
		cases = append(cases, reportingCase{
			name: "mutation-timeout-" + applied.name, exit: 14, jsonOuterContext: true,
			err:  &mutationWaitTimeoutError{message: "accepted; observation timed out", mutationApplied: applied.value},
			text: "wefty: accepted; observation timed out\n",
			json: `{"error":{"code":"wait_timeout","message":"accepted; observation timed out","retryable":false,"details":{"mutation_applied":` + applied.name + `}}}`,
		})
		cases = append(cases, reportingCase{
			name: "revocation-timeout-" + applied.name, exit: 14,
			err: &apiResponseError{Service: "L1", StatusCode: 503, APIError: contract.APIError{
				Code: contract.ErrorRevocationWaitTimeout, Message: "revocation pending", Retryable: true,
				Details: map[string]any{"mutation_applied": applied.value, "observation_failure": "last read unavailable"}, RequestID: "revocation-request",
			}},
			text: `wefty: revocation_wait_timeout: revocation pending; retryable=true; request_id=revocation-request; details={"mutation_applied":` + applied.name + `,"observation_failure":"last read unavailable"}` + "\n",
			json: `{"error":{"code":"revocation_wait_timeout","message":"revocation pending","retryable":true,"details":{"mutation_applied":` + applied.name + `,"observation_failure":"last read unavailable"},"request_id":"revocation-request"}}`,
		})
	}

	// All current protocol codes, plus an unknown code, across each reporting
	// family. Retryability never overrides structured refusals. Only L1/L3
	// retryable internal/run_ledger_unavailable responses at HTTP >= 500 qualify.
	protocolCodes := []struct {
		code contract.ErrorCode
		exit int
	}{
		{contract.ErrorInvalidRequest, 2},
		{contract.ErrorUnauthorized, 3}, {contract.ErrorForbidden, 3}, {contract.ErrorPrincipalForbidden, 3},
		{contract.ErrorPersonIdentityRequired, 3}, {contract.ErrorAdminRequired, 3}, {contract.ErrorControlNotAuthorized, 3},
		{contract.ErrorNotFound, 4}, {contract.ErrorAttemptNotFound, 4}, {contract.ErrorTakeoverSessionEnded, 4},
		{contract.ErrorCancelService, 5}, {contract.ErrorCancelNotQueued, 5}, {contract.ErrorConflict, 5},
		{contract.ErrorStalePolicyRevision, 5}, {contract.ErrorStaleIntentRevision, 5}, {contract.ErrorStorageReferenceConflict, 5},
		{contract.ErrorIdempotencyConflict, 5}, {contract.ErrorDispatchKeyConflict, 5}, {contract.ErrorInstanceKeyConflict, 5},
		{contract.ErrorInstanceKeyNotSupported, 5}, {contract.ErrorFinalAdmin, 5}, {contract.ErrorCapacityExhausted, 5},
		{contract.ErrorComputerResourceRequired, 5}, {contract.ErrorComputerTraitRequired, 5},
		{contract.ErrorControllerBusy, 5}, {contract.ErrorControllerAlreadyHeld, 5},
		{contract.ErrorRevocationWaitTimeout, 14}, {contract.ErrorUnavailable, 13},
		{contract.ErrorInternal, 1}, {contract.ErrorRunLedgerUnavailable, 1}, {contract.ErrorTenureUnavailable, 1},
		{contract.ErrorRevocationObservationFailed, 1}, {contract.ErrorIdentityBound, 1},
		{contract.ErrorNodeNotRegistered, 1}, {contract.ErrorNodeDead, 1}, {contract.ErrorNodeDraining, 1},
		{contract.ErrorNodeSessionReplaced, 1}, {contract.ErrorAttemptNotOwned, 1}, {contract.ErrorStaleFence, 1},
		{contract.ErrorLeaseExpired, 1}, {contract.ErrorAttemptMismatch, 1}, {contract.ErrorSupersededAttempt, 1},
		{contract.ErrorSpawnDepthExceeded, 1}, {contract.ErrorRunIdentityRequired, 1}, {contract.ErrorRunIdentityNotEntitled, 1},
		{contract.ErrorAdminBootstrapInvalid, 1}, {contract.ErrorAdminBootstrapClosed, 1}, {contract.ErrorPassUnavailable, 1},
		{contract.ErrorSubmitInflightLimit, 1}, {contract.ErrorUnsupportedKind, 1}, {contract.ErrorUnsupportedClass, 1},
		{contract.ErrorUnsupportedRuntimeHandler, 1}, {contract.ErrorNoResolvedImageSnapshot, 1}, {contract.ErrorNotImplemented, 1},
		{"future-code", 1},
	}
	receipt := &contract.ComputerControlReceipt{
		ComputerID: "computer-1", Action: "take", HolderSessionID: "session-1", AdmittedMode: "control",
		TenureState: contract.ComputerControlTenureHeld, PolicyRevision: 7, OverrideDisplacedSessionID: "session-0",
		HumanDriving: true, SignalStayedTrue: true, SessionEndReason: "revoked",
	}
	const receiptJSON = `,"receipt":{"computer_id":"computer-1","action":"take","holder_session_id":"session-1","admitted_mode":"control","tenure_state":"held","policy_revision":7,"override_displaced_session_id":"session-0","human_driving":true,"signal_stayed_true":true,"session_end_reason":"revoked"}`
	const receiptText = "COMPUTER ID\tcomputer-1\nACTION\ttake\nHOLDER SESSION ID\tsession-1\nADMITTED MODE\tcontrol\nTENURE STATE\theld\nPOLICY REVISION\t7\nOVERRIDE DISPLACED SESSION ID\tsession-0\nHUMAN DRIVING\ttrue\nSIGNAL STAYED TRUE\ttrue\nSESSION END REASON\trevoked\n"
	// Joined pairs pin precedence independently of traversal order. The exit
	// numbers and stderr literals were verified with main.go at 75c815aa.
	mutation := &mutationWaitTimeoutError{message: "observation timed out", mutationApplied: reportingBool(true)}
	unavailable := &unavailableError{cause: errors.New("connection refused")}
	local := &ocicontrol.ResponseError{Status: 403, APIError: contract.APIError{
		Code: contract.ErrorForbidden, Message: "local refusal",
	}}
	takeover := &takeoverActionError{APIError: contract.APIError{
		Code: contract.ErrorConflict, Message: "takeover refusal",
	}, Receipt: receipt}
	outcome := &runOutcomeError{runID: "run-1", status: contract.RunFailed, reason: "job failed"}
	const localJSON = `{"error":{"code":"forbidden","message":"local refusal","retryable":false}}`
	type joinedPair struct {
		name                  string
		first, second         error
		exit                  int
		firstText, secondText string
		json                  string
		jsonOuterContext      bool
		receiptText           string
	}
	pairs := []joinedPair{
		{"mutation-timeout-and-unavailable", mutation, unavailable, 14, "observation timed out", "connection refused", `{"error":{"code":"wait_timeout","message":"observation timed out\nconnection refused","retryable":false,"details":{"mutation_applied":true}}}`, true, ""},
		{"node-local-and-takeover-receipt", local, takeover, 3, "local refusal", "conflict: takeover refusal", localJSON, false, receiptText},
		{"run-outcome-and-not-ready", outcome, &notReadyError{verdict: "not ready: no nodes"}, 10, "run run-1 failed: job failed", "not ready: no nodes", "", false, ""},
		{"custody-deferred-and-run-outcome", &custodyImportOutcomeError{importID: "import-1", outcome: custodyImportDeferred}, outcome, 6, `Custody import "import-1" is deferred`, "run run-1 failed: job failed", "", false, ""},
		{"usage-and-node-local", usageError("bad arguments"), local, 2, "bad arguments", "local refusal", `{"error":{"code":"invalid_request","message":"bad arguments","retryable":false}}`, false, ""},
		{"run-outcome-and-mutation-timeout", outcome, mutation, 10, "run run-1 failed: job failed", "observation timed out", "", false, ""},
		{"run-outcome-and-unavailable", outcome, unavailable, 10, "run run-1 failed: job failed", "connection refused", "", false, ""},
	}
	for _, service := range []string{"L1", "L3"} {
		response := &apiResponseError{Service: service, StatusCode: 404, APIError: contract.APIError{
			Code: contract.ErrorNotFound, Message: "server refusal",
		}}
		pairs = append(pairs,
			joinedPair{"node-local-and-" + service, local, response, 3, "local refusal", "not_found: server refusal; retryable=false", localJSON, false, ""},
			joinedPair{service + "-and-takeover-receipt", response, takeover, 4, "not_found: server refusal; retryable=false", "conflict: takeover refusal", `{"error":{"code":"not_found","message":"server refusal","retryable":false}}`, false, receiptText},
		)
	}
	for _, pair := range pairs {
		for _, reverse := range []bool{false, true} {
			first, second := pair.first, pair.second
			firstText, secondText := pair.firstText, pair.secondText
			jsonLiteral := pair.json
			if reverse {
				first, second = second, first
				firstText, secondText = secondText, firstText
				if pair.jsonOuterContext {
					jsonLiteral = strings.Replace(jsonLiteral, pair.firstText+`\n`+pair.secondText, firstText+`\n`+secondText, 1)
				}
			}
			cases = append(cases, reportingCase{
				name: fmt.Sprintf("joined/%s/reverse=%t", pair.name, reverse),
				err:  errors.Join(first, second), exit: pair.exit,
				text: "wefty: " + firstText + "\n" + secondText + "\n" + pair.receiptText,
				json: jsonLiteral, jsonOuterContext: pair.jsonOuterContext,
			})
		}
	}
	for _, code := range protocolCodes {
		for _, retryable := range []bool{false, true} {
			for _, family := range []string{"node-local", "L1", "L3", "takeover", "takeover-receipt"} {
				statuses := []int{503}
				if family == "L1" || family == "L3" {
					statuses = []int{499, 500, 503}
				}
				for _, status := range statuses {
					apiError := contract.APIError{
						Code: code.code, Message: "server refusal", Retryable: retryable,
						Details: map[string]any{"l3_request_id": "ledger-request", "reason": "recorded reason"}, RequestID: "server-request",
					}
					c := reportingCase{
						name: fmt.Sprintf("%s/%s/retryable=%t/status=%d", family, code.code, retryable, status), exit: code.exit,
						json: fmt.Sprintf(`{"error":{"code":%q,"message":"server refusal","retryable":%t,"details":{"l3_request_id":"ledger-request","reason":"recorded reason"},"request_id":"server-request"}`, code.code, retryable),
					}
					switch family {
					case "node-local":
						c.err = &ocicontrol.ResponseError{Status: status, APIError: apiError}
						c.text = "wefty: server refusal\n"
					case "L1", "L3":
						c.err = &apiResponseError{Service: family, StatusCode: status, APIError: apiError}
						c.text = fmt.Sprintf(`wefty: %s: server refusal; retryable=%t; request_id=server-request; details={"l3_request_id":"ledger-request","reason":"recorded reason"}`+"\n", code.code, retryable)
						if status >= 500 && retryable && (code.code == "internal" || code.code == "run_ledger_unavailable") {
							c.exit = 13
						}
					case "takeover", "takeover-receipt":
						failure := &takeoverActionError{APIError: apiError}
						c.err = failure
						c.text = fmt.Sprintf("wefty: %s: server refusal\n", code.code)
						if family == "takeover-receipt" {
							failure.Receipt = receipt
							c.json += receiptJSON
							c.text += receiptText
						}
					}
					c.json += "}"
					cases = append(cases, c)
				}
			}
		}
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			for _, wrapped := range []bool{false, true} {
				err := c.err
				wantText := c.text
				if wrapped {
					err = fmt.Errorf("outer context: %w", err)
					wantText = "wefty: outer context: " + c.text[len("wefty: "):]
				}
				if got := commandExitCode(err); got != c.exit {
					t.Errorf("wrapped=%t exit=%d, want %d", wrapped, got, c.exit)
				}
				if got := commandExitCodeForArgs(err, []string{"--json", "future-command"}); got != c.exit {
					t.Errorf("wrapped=%t process exit=%d, want %d", wrapped, got, c.exit)
				}
				for _, jsonOutput := range []bool{false, true} {
					want := wantText
					if jsonOutput {
						want = c.json
						// These local error documents carry the outer context. Usage
						// and server envelopes keep their original inner messages.
						if wrapped && c.jsonOuterContext {
							want = strings.Replace(want, `"message":"`, `"message":"outer context: `, 1)
						}
						want = reportingJSON(t, want)
					}
					var stderr bytes.Buffer
					writeCommandError(&stderr, err, jsonOutput)
					if got := stderr.String(); got != want {
						t.Errorf("wrapped=%t json=%t stderr:\n%q\nwant:\n%q", wrapped, jsonOutput, got, want)
					}
					if jsonOutput && stderr.Len() != 0 {
						decoder := json.NewDecoder(&stderr)
						var document any
						if err := decoder.Decode(&document); err != nil {
							t.Fatal(err)
						}
						if err := decoder.Decode(&document); err != io.EOF {
							t.Fatalf("second stderr JSON document: %v", err)
						}
					}
				}
			}
		})
	}
}

func reportingBool(value bool) *bool { return &value }

func reportingJSON(t *testing.T, literal string) string {
	t.Helper()
	if literal == "" {
		return ""
	}
	var formatted bytes.Buffer
	if err := json.Indent(&formatted, []byte(literal), "", "  "); err != nil {
		t.Fatal(err)
	}
	return formatted.String() + "\n"
}
