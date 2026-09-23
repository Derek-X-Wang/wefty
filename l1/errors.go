package l1

import (
	"errors"
	"fmt"
	"strings"

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

// runLedgerUnavailable refuses in the caller's own terms when L1 cannot reach
// the run ledger to revoke Computer authority. The cause travels in the
// message on purpose: it is L1's own description of a deployment dependency,
// carries no caller secret, and is the only thing that tells an operator to
// look at the run-ledger address instead of at L1.
func runLedgerUnavailable(err error, message string) error {
	if err == nil {
		return &Error{Code: contract.ErrorRunLedgerUnavailable, Message: message}
	}
	return &Error{Code: contract.ErrorRunLedgerUnavailable, Message: fmt.Sprintf("%s: %v", message, err), Cause: err}
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

// scrubbedCause renders the whole wrapped chain behind an error whose message
// the response will not carry. A *Error prints only its own message, so the
// operation label and the driver failure underneath it are two different
// links; joining them is the only way the log names the failing statement.
func scrubbedCause(err error) string {
	if err == nil {
		return ""
	}
	links := make([]string, 0, 4)
	for link := err; link != nil; link = errors.Unwrap(link) {
		text := strings.TrimSpace(link.Error())
		if text == "" {
			continue
		}
		if len(links) > 0 && strings.Contains(links[len(links)-1], text) {
			continue
		}
		links = append(links, text)
	}
	return strings.Join(links, ": ")
}

// scrubbedClass names the concrete type of the deepest error in the chain,
// which is what distinguishes a driver fault from a programming fault when
// the messages alone read the same.
func scrubbedClass(err error) string {
	deepest := err
	for link := err; link != nil; link = errors.Unwrap(link) {
		deepest = link
	}
	return fmt.Sprintf("%T", deepest)
}
