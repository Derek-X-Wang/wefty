package ocihelper

import (
	"context"
	"errors"
	"time"
)

// ExecComputerSeat streams until one terminal event and the final success
// frame. Cancelling ctx or returning a consumer error closes this command's
// connection, which cancels only the corresponding exec process.
func (session *Session) ExecComputerSeat(ctx context.Context, request ExecComputerSeatRequest, receive func(SeatCommandEvent) error) error {
	terminal := false
	err := session.stream(ctx, MethodExecComputerSeat, request, false, func(_ *framedConn, raw frame) error {
		var event SeatCommandEvent
		if err := decodeBody(raw.Body, &event); err != nil {
			return err
		}
		if err := event.validate(); err != nil {
			return err
		}
		if terminal {
			return errors.New("Seat command output after exit")
		}
		terminal = event.Exit != nil
		if receive != nil {
			return receive(event)
		}
		return nil
	})
	if err == nil && !terminal {
		return errors.New("Seat command stream ended without exit evidence")
	}
	return err
}

func (server *Server) dispatchExecComputerSeat(operation *sessionOperation, wire *framedConn, request frame) {
	var body ExecComputerSeatRequest
	if !decodeRequest(wire, request.Body, &body) {
		return
	}
	if err := body.validate(); err != nil {
		_ = writeFailure(wire, CodeInvalidRequest, "invalid Seat command")
		return
	}
	attempt, rpcErr := operation.session.authorizeAttempt(body.Authority)
	if rpcErr != nil {
		_ = writeRPCError(wire, rpcErr)
		return
	}
	if !attempt.computer {
		_ = writeFailure(wire, CodeUnauthorizedAttempt, "Seat command requires a live Computer attempt")
		return
	}
	engine, ok := server.engine.(ComputerSeatExecEngine)
	if !ok {
		_ = writeFailure(wire, CodeUnsupportedOperation, "Seat commands unavailable")
		return
	}
	release, refusal := server.admitSeatCommand(body)
	if refusal != nil {
		_ = writeRPCError(wire, refusal)
		return
	}
	defer release()
	operation.monitorEOF()
	ctx, cancel := context.WithTimeoutCause(operation.ctx, body.MaxRuntime, ErrSeatCommandRuntimeLimit)
	defer cancel()
	// A limit stops new events but gives the entire in-flight frame a bounded
	// chance to finish. Never append a diagnostic behind a truncated frame.
	deadlineApplied := make(chan struct{})
	stop := context.AfterFunc(ctx, func() {
		_ = wire.conn.SetWriteDeadline(time.Now().Add(seatCommandFrameFinishTimeout))
		close(deadlineApplied)
	})
	defer stop()
	terminal := false
	var frameWriteErr error
	err := engine.ExecComputerSeat(ctx, body, func(event SeatCommandEvent) error {
		if frameWriteErr != nil {
			return frameWriteErr
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := event.validate(); err != nil {
			return err
		}
		if terminal {
			return errors.New("Seat command output after exit")
		}
		terminal = event.Exit != nil
		frameWriteErr = writeSuccess(wire, event)
		return frameWriteErr
	})
	if !stop() {
		<-deadlineApplied
	}
	if frameWriteErr != nil {
		return
	}
	// A command deadline is command-scoped. Keep the final diagnostic frame
	// deliverable so it cannot look like loss of the helper session.
	_ = wire.conn.SetWriteDeadline(time.Now().Add(defaultRequestTimeout))
	if err == nil && !terminal {
		err = errors.New("Seat command has no exit evidence")
	}
	if err != nil {
		if !seatCommandCleanupUnconfirmed(err) && (errors.Is(err, ErrSeatCommandRuntimeLimit) || (errors.Is(context.Cause(ctx), ErrSeatCommandRuntimeLimit) && errors.Is(err, context.DeadlineExceeded))) {
			_ = writeRPCError(wire, &RPCError{Code: CodeDiagnosticFailure, Detail: DetailSeatCommandRuntimeLimit, Message: ErrSeatCommandRuntimeLimit.Error()})
		} else if !seatCommandCleanupUnconfirmed(err) && errors.Is(err, errSeatCommandStartTimeout) {
			_ = writeRPCError(wire, &RPCError{Code: CodeDiagnosticFailure, Detail: DetailSeatCommandStartTimeout, Message: errSeatCommandStartTimeout.Error()})
		} else {
			_ = writeSeatOperationFailure(wire, err)
		}
		return
	}
	_ = writeSuccess(wire, nil)
}
