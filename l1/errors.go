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
	// notRetryable overrides the code's default retryable answer when the
	// refusal is about work that already happened and a retry cannot change.
	notRetryable bool
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

// computerRevocationOwed refuses after an authority-losing Computer mutation
// has committed, together with the owed revocation it now carries, and the
// run ledger could not take that revocation. It is not retryable: nothing the
// caller can repeat performs the revocation, and nothing needs it to. L1
// holds the revocation durably and settles it on the host Node's heartbeat
// once the run ledger answers (#554), and L3 already refuses the old tokens
// because it revalidates the live L1 scope on every bearer request and L1 no
// longer proves scope for a Computer that is not meant to be running.
func computerRevocationOwed(err error) error {
	message := "the Computer mutation applied, but the run ledger could not be reached, so the explicit revocation " +
		"of its token grants is owed: L1 recorded it and will retry it until the run ledger takes it, so do not " +
		"retry the request for it, and L3's live-scope check already refuses the Computer's old tokens"
	if err != nil {
		message = fmt.Sprintf("%s: %v", message, err)
	}
	return &Error{Code: contract.ErrorRunLedgerUnavailable, Message: message, Cause: err, notRetryable: true}
}

// computerRevocationNothingOwed refuses after an authority-losing Computer
// mutation committed while the run ledger could not take its revoke-all, and
// no attempt of the Computer could hold a pass when the mutation began. The
// owed row is closed as nothing to revoke: L3 mints a pass only for a claimed
// or running attempt, and re-proves that scope on every use.
func computerRevocationNothingOwed(err error) error {
	message := "the Computer mutation applied, but the run ledger could not be reached, so the explicit revocation " +
		"of its token grants was not recorded; no attempt could hold a token when the mutation began, so nothing " +
		"is owed, and L3's live-scope check already refuses the Computer's old tokens"
	if err != nil {
		message = fmt.Sprintf("%s: %v", message, err)
	}
	return &Error{Code: contract.ErrorRunLedgerUnavailable, Message: message, Cause: err, notRetryable: true}
}

// computerRevocationNotRecorded refuses when a call that committed no new
// authority loss -- an accepted replay, or a stop or remove that found
// nothing to change -- could not re-drive the explicit revocation. The
// original mutation's owed revocation, if it has one, is unaffected and still
// settles on the heartbeat.
func computerRevocationNotRecorded(err error) error {
	message := "the Computer mutation applied, but the run ledger could not be reached, so the explicit revocation " +
		"of its token grants was not recorded; retrying the request is not guaranteed to perform it, and L3's " +
		"live-scope check already refuses the Computer's old tokens"
	if err != nil {
		message = fmt.Sprintf("%s: %v", message, err)
	}
	return &Error{Code: contract.ErrorRunLedgerUnavailable, Message: message, Cause: err, notRetryable: true}
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
