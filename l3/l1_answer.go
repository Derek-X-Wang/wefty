package l3

import (
	"errors"
	"net/http"
	"net/url"
	"strings"

	"github.com/Derek-X-Wang/wefty/contract"
)

// An answer retains the requested operation, target and response origin. Only
// the client seam can supply HTTP evidence; transport and unknown errors remain
// transient. Classification never decides a Run's outcome.
type l1AnswerKind uint8

const (
	l1Transient l1AnswerKind = iota
	l1LedgerNotAdmitted
	l1WorkRefused
	l1AuthoritativeAbsence
	l1ProtocolViolation
)

type l1Answer struct {
	Kind           l1AnswerKind
	Method, Target string
	Status         int
	Reason         string
	Response       *l1ResponseError
}

func classifyL1Answer(err error) l1Answer {
	a := l1Answer{Kind: l1Transient}
	var response *l1ResponseError
	if !errors.As(err, &response) {
		return a
	}
	a.Response, a.Method, a.Target, a.Status = response, response.method, response.path, response.status
	if response.requestMethod != "" {
		a.Method, a.Target = response.requestMethod, response.requestPath
	}
	if a.Status == 0 || response.transportFailure {
		return a
	}
	a.Reason, _ = response.protocol.Details["reason"].(string)
	if !response.validEnvelope || response.redirected {
		a.Kind = l1ProtocolViolation
		return a
	}
	if reason, ok := response.protocol.Details["reason"]; ok {
		if _, valid := reason.(string); !valid {
			a.Kind = l1ProtocolViolation
			return a
		}
	}
	code := response.protocol.Code
	submit := a.Method == http.MethodPost && a.Target == "/v1/jobs"
	path, _, _ := strings.Cut(a.Target, "?")
	ledgerGate := (a.Method == http.MethodGet && strings.HasPrefix(path, "/v1/dispatch-keys/") && strings.HasSuffix(path, "/job")) ||
		(a.Method == http.MethodPost && ((strings.HasPrefix(path, "/v1/computers/") && strings.HasSuffix(path, "/token-scope-proof")) || path == "/v1/host-boot-session-proof"))
	if (a.Status == 503 && code == contract.ErrorUnavailable && a.Reason == "identity_unverifiable") ||
		(a.Status == 401 && code == contract.ErrorUnauthorized) ||
		((a.Status == 401 || a.Status == 403) && code == contract.ErrorPrincipalForbidden) ||
		(submit && a.Status == 403 && code == contract.ErrorRunIdentityNotEntitled) ||
		(ledgerGate && a.Status == 403 && code == contract.ErrorForbidden && a.Reason == "run_ledger_not_admitted") {
		a.Kind = l1LedgerNotAdmitted
		if a.Reason == "" {
			a.Reason = string(code)
		}
		return a
	}
	// Older identity-routing refusals are dependency errors, but do not
	// supply the affirmative evidence needed for a ledger-wide hold.
	if a.Status == 401 && code == contract.ErrorPersonIdentityRequired {
		return a
	}
	if a.Reason == "no_route" {
		a.Kind = l1ProtocolViolation
		return a
	}
	if a.Status == 404 && code == contract.ErrorNotFound {
		if a.Method == http.MethodGet && (jobReadPath(path) || ledgerGate) {
			a.Kind = l1AuthoritativeAbsence
		} else {
			a.Kind = l1WorkRefused
		}
		return a
	}
	if response.protocol.Retryable && knownL1Code(code) {
		return a
	}
	if code == contract.ErrorNotImplemented && a.Status == 501 && !response.protocol.Retryable {
		a.Kind = l1WorkRefused
		return a
	}
	if a.Status >= 500 || a.Status == 429 {
		if knownL1Code(code) {
			return a
		}
		a.Kind = l1ProtocolViolation
		return a
	}
	if knownWorkRefusal(code) && refusalStatusMatches(a.Status, code) {
		a.Kind = l1WorkRefused
		return a
	}
	a.Kind = l1ProtocolViolation
	return a
}

func jobReadPath(path string) bool {
	if !strings.HasPrefix(path, "/v1/jobs/") {
		return false
	}
	tail := strings.TrimPrefix(path, "/v1/jobs/")
	parts := strings.Split(tail, "/")
	if len(parts) == 0 || parts[0] == "" {
		return false
	}
	_, err := url.PathUnescape(parts[0])
	if err != nil {
		return false
	}
	return len(parts) == 1 || (len(parts) == 2 && (parts[1] == "logs" || parts[1] == "result"))
}
func refusalStatusMatches(status int, code contract.ErrorCode) bool {
	switch code {
	case contract.ErrorInvalidRequest:
		return status == 400
	case contract.ErrorForbidden, contract.ErrorRunIdentityNotEntitled:
		return status == 403
	case contract.ErrorPersonIdentityRequired:
		return status == 401
	case contract.ErrorUnsupportedKind, contract.ErrorUnsupportedRuntimeHandler:
		return status == 422
	case contract.ErrorNotFound:
		return status == 404
	case contract.ErrorNotImplemented:
		return status == 501
	default:
		return status == 409
	}
}
func knownWorkRefusal(code contract.ErrorCode) bool {
	switch code {
	case contract.ErrorInvalidRequest, contract.ErrorForbidden, contract.ErrorNotFound, contract.ErrorConflict,
		contract.ErrorDispatchKeyConflict, contract.ErrorIdempotencyConflict, contract.ErrorInstanceKeyConflict,
		contract.ErrorInstanceKeyNotSupported, contract.ErrorSpawnDepthExceeded, contract.ErrorUnsupportedKind,
		contract.ErrorUnsupportedClass, contract.ErrorUnsupportedRuntimeHandler, contract.ErrorRunIdentityRequired, contract.ErrorRunIdentityNotEntitled,
		contract.ErrorPersonIdentityRequired, contract.ErrorControlNotAuthorized, contract.ErrorStalePolicyRevision,
		contract.ErrorStaleIntentRevision, contract.ErrorStorageReferenceConflict, contract.ErrorComputerResourceRequired,
		contract.ErrorComputerTraitRequired, contract.ErrorCancelService, contract.ErrorCancelNotQueued, contract.ErrorNotImplemented:
		return true
	}
	return false
}
func knownL1Code(code contract.ErrorCode) bool {
	return knownWorkRefusal(code) || code == contract.ErrorInternal || code == contract.ErrorUnavailable || code == contract.ErrorCapacityExhausted
}
func proxyL1Error(err error) error {
	a := classifyL1Answer(err)
	if a.Kind == l1LedgerNotAdmitted {
		return &Error{Code: contract.ErrorUnavailable, Message: "control plane cannot currently admit the run ledger", Retryable: true, RequestID: a.Response.protocol.RequestID, Details: map[string]any{"reason": a.Reason}, Cause: err}
	}
	if a.Response != nil && a.Status == 401 && a.Response.protocol.Code == contract.ErrorPersonIdentityRequired {
		return &Error{Code: contract.ErrorUnavailable, Message: "control plane requires a different identity", Retryable: true, Cause: err}
	}
	if a.Kind == l1Transient && a.Response != nil {
		return &Error{Code: a.Response.protocol.Code, Message: a.Response.protocol.Message, Retryable: true, Details: a.Response.protocol.Details, RequestID: a.Response.protocol.RequestID, Cause: err}
	}
	if a.Kind == l1ProtocolViolation {
		return internalError(err, "invalid control plane response")
	}
	return err
}
