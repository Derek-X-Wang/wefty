package main

import (
	"context"
	"errors"
	"io"
	"net"
	"syscall"
)

// transientExchangeError marks an exchange that a bounded startup observer
// can retry. It changes neither the message nor the normal CLI availability
// classification. A decoded API refusal never receives this marker.
type transientExchangeError struct{ cause error }

func (e *transientExchangeError) Error() string { return e.cause.Error() }
func (e *transientExchangeError) Unwrap() error { return e.cause }

// The HTTP adapter knows these are request/body transport failures, not a
// decoded refusal, a malformed response, or a local identity/sandbox error.
func markTransientTransport(ctx context.Context, err error) error {
	var sandbox *networkSandboxError
	if errors.Is(ctx.Err(), context.Canceled) || errors.Is(err, context.Canceled) || errors.As(err, &sandbox) {
		return err
	}
	var timeout net.Error
	if errors.Is(err, syscall.ECONNREFUSED) || errors.Is(err, syscall.ECONNRESET) || errors.Is(err, syscall.ETIMEDOUT) ||
		errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) || (errors.As(err, &timeout) && timeout.Timeout()) {
		return &transientExchangeError{cause: err}
	}
	return err
}
