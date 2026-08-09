package l3

import (
	"errors"
	"fmt"

	"github.com/Derek-X-Wang/wefty/contract"
)

// Error carries a stable protocol code through storage, reconciliation, and
// the HTTP layer.
type Error struct {
	Code      contract.ErrorCode
	Message   string
	Retryable bool
	Cause     error
}

func (e *Error) Error() string {
	if e.Message != "" {
		return e.Message
	}
	return string(e.Code)
}

func (e *Error) Unwrap() error { return e.Cause }

func protocolError(code contract.ErrorCode, format string, args ...any) error {
	return &Error{Code: code, Message: fmt.Sprintf(format, args...)}
}

func internalError(err error, message string) error {
	return &Error{Code: contract.ErrorInternal, Message: message, Cause: err}
}

func errorDetails(err error) (contract.ErrorCode, bool) {
	var protocolErr *Error
	if errors.As(err, &protocolErr) {
		return protocolErr.Code, protocolErr.Retryable
	}
	return contract.ErrorInternal, false
}
