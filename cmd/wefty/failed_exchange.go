package main

import (
	"context"
	"errors"
	"fmt"
	"net"

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
	// Some protocol libraries discard the underlying body read error. Preserve
	// cancellation even when the surviving error only describes HTTP status.
	if errors.Is(ctx.Err(), context.Canceled) {
		return fmt.Errorf("%w: %w", err, ctx.Err())
	}
	switch phase {
	case httpexchange.Request, httpexchange.Headers, httpexchange.Body:
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
	var failure *httpexchange.Failure
	if errors.As(err, &failure) {
		return classifyFailedExchange(ctx, failure.Phase, err)
	}
	return err
}
