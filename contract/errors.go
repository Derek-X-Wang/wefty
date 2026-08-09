package contract

import "fmt"

// ErrorCode is a stable, machine-readable protocol error identifier.
type ErrorCode string

const (
	ErrorInvalidRequest            ErrorCode = "invalid_request"
	ErrorNotFound                  ErrorCode = "not_found"
	ErrorUnauthorized              ErrorCode = "unauthorized"
	ErrorForbidden                 ErrorCode = "forbidden"
	ErrorConflict                  ErrorCode = "conflict"
	ErrorStaleFence                ErrorCode = "stale_fence"
	ErrorLeaseExpired              ErrorCode = "lease_expired"
	ErrorAttemptMismatch           ErrorCode = "attempt_mismatch"
	ErrorDispatchKeyConflict       ErrorCode = "dispatch_key_conflict"
	ErrorIdempotencyConflict       ErrorCode = "idempotency_conflict"
	ErrorUnsupportedKind           ErrorCode = "unsupported_kind"
	ErrorUnsupportedRuntimeHandler ErrorCode = "unsupported_runtime_handler"
	ErrorNotImplemented            ErrorCode = "not_implemented"
	ErrorInternal                  ErrorCode = "internal"
)

// APIError is the single error shape shared by every HTTP protocol.
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

// CheckExecutableKind applies execution-layer support policy after an open job
// kind has been decoded. The schema and Go types intentionally accept any
// non-empty kind; v0.1 agents execute only process jobs.
func CheckExecutableKind(kind string) error {
	if kind == "process" {
		return nil
	}

	return &ExecutionError{Kind: kind}
}
