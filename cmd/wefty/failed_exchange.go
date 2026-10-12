package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"syscall"

	"github.com/Derek-X-Wang/wefty/internal/httpexchange"
)

// classifyFailedExchange is the availability decision for every CLI HTTP
// adapter. A completed protocol refusal is decoded by the adapter instead.
func classifyFailedExchange(ctx context.Context, phase httpexchange.Phase, err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.Canceled) {
		return err
	}
	// Preserve caller cancellation that arrives after the exchange completes
	// but before its failure is classified.
	if errors.Is(ctx.Err(), context.Canceled) {
		return fmt.Errorf("%w: %w", err, ctx.Err())
	}
	switch phase {
	case httpexchange.Request:
		var dial *net.OpError
		if errors.As(err, &dial) && dial.Op == "dial" && dial.Net == "unix" {
			if errors.Is(err, syscall.EACCES) || errors.Is(err, syscall.EPERM) {
				return err
			}
			if errors.Is(err, syscall.ECONNREFUSED) || errors.Is(err, syscall.ENOENT) {
				err = fmt.Errorf("%w; check that the node agent is running", err)
			}
		}
		return &unavailableError{cause: err}
	case httpexchange.BareStatus, httpexchange.Body:
		return &unavailableError{cause: err}
	case httpexchange.Banner:
		var timeout net.Error
		if errors.Is(err, context.DeadlineExceeded) || (errors.As(err, &timeout) && timeout.Timeout()) {
			return &unavailableError{cause: err}
		}
	}
	return err
}

func classifyTakeoverTransportError(ctx context.Context, err error) error {
	return classifyRecordedExchangeError(ctx, err)
}

func classifyRecordedExchangeError(ctx context.Context, err error) error {
	var failure *httpexchange.Failure
	if errors.As(err, &failure) {
		return classifyFailedExchange(ctx, failure.Phase, err)
	}
	return err
}
