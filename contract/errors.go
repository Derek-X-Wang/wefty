package contract

import "fmt"

// ErrorCode is a stable, machine-readable protocol error identifier.
type ErrorCode string

const (
	ErrorInvalidRequest            ErrorCode = "invalid_request"
	ErrorNotFound                  ErrorCode = "not_found"
	ErrorUnauthorized              ErrorCode = "unauthorized"
	ErrorForbidden                 ErrorCode = "forbidden"
	ErrorPrincipalForbidden        ErrorCode = "principal_forbidden"
	ErrorIdentityBound             ErrorCode = "identity_bound"
	ErrorConflict                  ErrorCode = "conflict"
	ErrorNodeNotRegistered         ErrorCode = "node_not_registered"
	ErrorNodeDead                  ErrorCode = "node_dead"
	ErrorNodeDraining              ErrorCode = "node_draining"
	ErrorNodeSessionReplaced       ErrorCode = "node_session_replaced"
	ErrorAttemptNotFound           ErrorCode = "attempt_not_found"
	ErrorAttemptNotOwned           ErrorCode = "attempt_not_owned"
	ErrorStaleFence                ErrorCode = "stale_fence"
	ErrorLeaseExpired              ErrorCode = "lease_expired"
	ErrorAttemptMismatch           ErrorCode = "attempt_mismatch"
	ErrorDispatchKeyConflict       ErrorCode = "dispatch_key_conflict"
	ErrorIdempotencyConflict       ErrorCode = "idempotency_conflict"
	ErrorStaleIntentRevision       ErrorCode = "stale_intent_revision"
	ErrorStalePolicyRevision       ErrorCode = "stale_policy_revision"
	ErrorStorageReferenceConflict  ErrorCode = "storage_reference_conflict"
	ErrorComputerResourceRequired  ErrorCode = "computer_resource_required"
	ErrorComputerTraitRequired     ErrorCode = "computer_trait_required"
	ErrorPersonIdentityRequired    ErrorCode = "person_identity_required"
	ErrorAdminRequired             ErrorCode = "admin_required"
	ErrorAdminBootstrapInvalid     ErrorCode = "admin_bootstrap_invalid"
	ErrorAdminBootstrapClosed      ErrorCode = "admin_bootstrap_closed"
	ErrorFinalAdmin                ErrorCode = "final_admin"
	ErrorCapacityExhausted         ErrorCode = "capacity_exhausted"
	ErrorPassUnavailable           ErrorCode = "pass_unavailable"
	ErrorSubmitInflightLimit       ErrorCode = "submit_inflight_limit"
	ErrorUnsupportedKind           ErrorCode = "unsupported_kind"
	ErrorUnsupportedClass          ErrorCode = "unsupported_class"
	ErrorUnsupportedRuntimeHandler ErrorCode = "unsupported_runtime_handler"
	ErrorNoResolvedImageSnapshot   ErrorCode = "no_resolved_image_snapshot"
	ErrorNotImplemented            ErrorCode = "not_implemented"
	ErrorInternal                  ErrorCode = "internal"
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
