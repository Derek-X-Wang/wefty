package ocihelper

import (
	"context"
	"errors"
	"io"
	"time"
)

func (s *Server) dispatchCopyComputerSeat(op *sessionOperation, wire *framedConn, raw frame) {
	stopWriteDeadline := context.AfterFunc(op.ctx, func() { _ = wire.conn.SetWriteDeadline(time.Now().Add(seatCommandFrameFinishTimeout)) })
	defer stopWriteDeadline()
	var r CopyComputerSeatRequest
	if !decodeRequest(wire, raw.Body, &r) {
		return
	}
	if err := r.validate(); err != nil {
		_ = writeFailure(wire, CodeInvalidRequest, err.Error())
		return
	}
	a, rpc := op.session.authorizeAttempt(r.Authority)
	if rpc != nil {
		_ = writeRPCError(wire, rpc)
		return
	}
	if !a.computer {
		_ = writeFailure(wire, CodeUnauthorizedAttempt, "copy requires a live Computer attempt")
		return
	}
	engine, ok := s.engine.(ComputerSeatCopyEngine)
	if !ok {
		_ = writeFailure(wire, CodeUnsupportedOperation, "Seat copies unavailable")
		return
	}
	release, rpc := s.admitSeatCommand(r.command())
	if rpc != nil {
		_ = writeRPCError(wire, rpc)
		return
	}
	defer release()
	if err := writeSuccess(wire, struct{}{}); err != nil {
		return
	}
	reader, writer := io.Pipe()
	defer reader.Close()
	if r.Direction == "out" {
		var part seatCopyInput
		if err := wire.read(&part); err != nil || !part.End || len(part.Data) != 0 {
			_ = writeFailure(wire, CodeInvalidRequest, "copy-out input must be empty")
			return
		}
		writer.Close()
		op.monitorEOF()
	} else {
		go func() {
			for {
				var part seatCopyInput
				if err := wire.read(&part); err != nil {
					writer.CloseWithError(err)
					op.cancel()
					return
				}
				if part.End {
					if len(part.Data) != 0 {
						writer.CloseWithError(errors.New("copy data after end"))
						op.cancel()
						return
					}
					writer.Close()
					op.monitorEOF()
					return
				}
				if r.Direction != "in" || len(part.Data) == 0 || len(part.Data) > SeatCopyChunkBytes {
					writer.CloseWithError(errors.New("invalid copy input frame"))
					op.cancel()
					return
				}
				if _, err := writer.Write(part.Data); err != nil {
					return
				}
			}
		}()
	}
	size, err := engine.CopyComputerSeat(op.ctx, r, reader, func(data []byte) error {
		if len(data) == 0 || len(data) > SeatCommandChunkBytes {
			return errors.New("invalid copy output chunk")
		}
		return writeSuccess(wire, SeatCopyEvent{Data: data})
	})
	if err != nil {
		var refusal *SeatOperationRefusalError
		if errors.As(err, &refusal) {
			_ = writeSeatOperationFailure(wire, err)
		} else {
			_ = writeFailure(wire, CodeDiagnosticFailure, "Seat copy failed: "+err.Error())
		}
		return
	}
	if err = writeSuccess(wire, SeatCopyEvent{SizeBytes: &size}); err == nil {
		_ = writeSuccess(wire, nil)
	}
}
