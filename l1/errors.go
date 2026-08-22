package l1

import (
	"errors"
	"fmt"

	"github.com/Derek-X-Wang/wefty/contract"
)

// Error carries a stable protocol code from storage through the HTTP layer.
type Error struct {
	Code    contract.ErrorCode
	Message string
	Cause   error
	Details map[string]any
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

func protocolErrorWithDetails(code contract.ErrorCode, details map[string]any, format string, args ...any) error {
	return &Error{Code: code, Message: fmt.Sprintf(format, args...), Details: details}
}

func internalError(err error, message string) error {
	return &Error{Code: contract.ErrorInternal, Message: message, Cause: err}
}

func errorCode(err error) contract.ErrorCode {
	var protocolErr *Error
	if errors.As(err, &protocolErr) {
		return protocolErr.Code
	}
	var coded interface{ Code() contract.ErrorCode }
	if errors.As(err, &coded) {
		return coded.Code()
	}
	return contract.ErrorInternal
}
