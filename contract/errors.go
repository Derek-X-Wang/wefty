package contract

import "fmt"

// ErrorCode is a stable, machine-readable protocol error identifier.
type ErrorCode string

const (
	ErrorInvalidRequest      ErrorCode = "invalid_request"
	ErrorNotFound            ErrorCode = "not_found"
	ErrorUnauthorized        ErrorCode = "unauthorized"
	ErrorForbidden           ErrorCode = "forbidden"
	ErrorPrincipalForbidden  ErrorCode = "principal_forbidden"
	ErrorIdentityBound       ErrorCode = "identity_bound"
	ErrorConflict            ErrorCode = "conflict"
	ErrorNodeNotRegistered   ErrorCode = "node_not_registered"
	ErrorNodeDead            ErrorCode = "node_dead"
	ErrorNodeDraining        ErrorCode = "node_draining"
	ErrorNodeSessionReplaced ErrorCode = "node_session_replaced"
	ErrorAttemptNotFound     ErrorCode = "attempt_not_found"
	ErrorAttemptNotOwned     ErrorCode = "attempt_not_owned"
	ErrorStaleFence          ErrorCode = "stale_fence"
	ErrorLeaseExpired        ErrorCode = "lease_expired"
	ErrorAttemptMismatch     ErrorCode = "attempt_mismatch"
	// ErrorSupersededAttempt refuses evidence that would overwrite what a
	// later attempt of the same job already established. It is not an
	// authority failure: the attempt was real and its evidence was accepted
	// while it was current. It is simply no longer the attempt that speaks for
	// the job.
	ErrorSupersededAttempt           ErrorCode = "superseded_attempt"
	ErrorDispatchKeyConflict         ErrorCode = "dispatch_key_conflict"
	ErrorSpawnDepthExceeded          ErrorCode = "spawn_depth_exceeded"
	ErrorIdempotencyConflict         ErrorCode = "idempotency_conflict"
	ErrorStaleIntentRevision         ErrorCode = "stale_intent_revision"
	ErrorStalePolicyRevision         ErrorCode = "stale_policy_revision"
	ErrorStorageReferenceConflict    ErrorCode = "storage_reference_conflict"
	ErrorComputerResourceRequired    ErrorCode = "computer_resource_required"
	ErrorComputerTraitRequired       ErrorCode = "computer_trait_required"
	ErrorPersonIdentityRequired      ErrorCode = "person_identity_required"
	ErrorAdminRequired               ErrorCode = "admin_required"
	ErrorAdminBootstrapInvalid       ErrorCode = "admin_bootstrap_invalid"
	ErrorAdminBootstrapClosed        ErrorCode = "admin_bootstrap_closed"
	ErrorFinalAdmin                  ErrorCode = "final_admin"
	ErrorControlNotAuthorized        ErrorCode = "control_not_authorized"
	ErrorControllerBusy              ErrorCode = "controller_busy"
	ErrorControllerAlreadyHeld       ErrorCode = "controller_already_held"
	ErrorTakeoverSessionEnded        ErrorCode = "takeover_session_ended"
	ErrorTenureUnavailable           ErrorCode = "tenure_unavailable"
	ErrorRevocationObservationFailed ErrorCode = "revocation_observation_failed"
	ErrorRevocationWaitTimeout       ErrorCode = "revocation_wait_timeout"
	ErrorCapacityExhausted           ErrorCode = "capacity_exhausted"
	ErrorPassUnavailable             ErrorCode = "pass_unavailable"
	ErrorSubmitInflightLimit         ErrorCode = "submit_inflight_limit"
	ErrorUnsupportedKind             ErrorCode = "unsupported_kind"
	ErrorUnsupportedClass            ErrorCode = "unsupported_class"
	ErrorUnsupportedRuntimeHandler   ErrorCode = "unsupported_runtime_handler"
	ErrorNoResolvedImageSnapshot     ErrorCode = "no_resolved_image_snapshot"
	ErrorNotImplemented              ErrorCode = "not_implemented"
	// ErrorRunLedgerUnavailable names the one dependency L1 cannot substitute
	// for: the run ledger that holds Computer submission tokens. L1 must reach
	// it to revoke a Computer's authority, and when it cannot, the caller is
	// owed that fact by name. Answering "internal" instead told operators L1
	// had a bug and told node agents to retry a call that could never succeed
	// until a human changed the deployment (wefty #548).
	ErrorRunLedgerUnavailable ErrorCode = "run_ledger_unavailable"
	ErrorInternal             ErrorCode = "internal"
)

// APIError is the single error shape shared by every HTTP protocol. Retryable
// advises whether repeating the same request may succeed; consumers must still
// honor the authority scope expressed by a known Code.
type APIError struct {
	Code      ErrorCode      `json:"code"`
	Message   string         `json:"message"`
	Retryable bool           `json:"retryable"`
	Details   map[string]any `json:"details,omitempty"`
	RequestID string         `json:"request_id,omitempty"`
}

// ErrorResponse wraps APIError so all protocol errors have the same envelope.
type ErrorResponse struct {
	Error APIError `json:"error"`
}

type ComputerControlErrorResponse struct {
	Error   APIError                `json:"error"`
	Receipt *ComputerControlReceipt `json:"receipt,omitempty"`
}

// JobSpecValidationError carries the stable protocol code for a structurally
// invalid job specification across every construction surface.
type JobSpecValidationError struct {
	code    ErrorCode
	message string
}

func (e *JobSpecValidationError) Error() string { return e.message }

func (e *JobSpecValidationError) Code() ErrorCode { return e.code }

func invalidJobSpecf(format string, args ...any) error {
	return &JobSpecValidationError{code: ErrorInvalidRequest, message: fmt.Sprintf(format, args...)}
}

func unsupportedRuntimeHandlerf(format string, args ...any) error {
	return &JobSpecValidationError{code: ErrorUnsupportedRuntimeHandler, message: fmt.Sprintf(format, args...)}
}

// ExecutionError reports that a syntactically valid job cannot be executed by
// this version of the agent.
type ExecutionError struct {
	Kind string
}

func (e *ExecutionError) Error() string {
	return fmt.Sprintf("job kind %q is not supported by this agent", e.Kind)
}

func (e *ExecutionError) Code() ErrorCode {
	return ErrorUnsupportedKind
}

// ClassExecutionError reports that an open workload class cannot be executed
// by this version of the agent.
type ClassExecutionError struct {
	Class string
}

func (e *ClassExecutionError) Error() string {
	return fmt.Sprintf("job class %q is not supported by this agent", e.Class)
}

func (e *ClassExecutionError) Code() ErrorCode {
	return ErrorUnsupportedClass
}

// CheckWorkloadClass applies execution-layer support policy after an open job
// class has been decoded. The current agent executes one-shot and service
// process payloads; L1 remains responsible for service lifecycle policy.
func CheckWorkloadClass(class string) error {
	if class == JobClassOneShot || class == JobClassService {
		return nil
	}

	return &ClassExecutionError{Class: class}
}
