package ocihelper

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
)

const MethodSetSeatDriver Method = "SetSeatDriver"

type SetSeatDriverRequest struct {
	Authority    AttemptAuthority `json:"authority"`
	Generation   uint64           `json:"generation"`
	Place        int              `json:"place"`
	HumanDriving bool             `json:"human_driving"`
}
type SeatDriverEngine interface {
	SetSeatDriver(context.Context, SetSeatDriverRequest) error
}

func (s *Session) SetSeatDriver(ctx context.Context, r SetSeatDriverRequest) error {
	return s.call(ctx, MethodSetSeatDriver, r, &struct{}{})
}
func (s *computerSeats) writeDriver(place int, driving bool) error {
	directory := filepath.Join(s.directory, "seats", fmt.Sprint(place))
	if err := os.MkdirAll(directory, 0755); err != nil {
		return err
	}
	return atomicWriteComputerControlState(directory, driving)
}
func (s *computerSeats) setDriver(generation uint64, place int, driving bool) error {
	if s == nil || generation == 0 || generation != s.generation {
		return &SeatOperationRefusalError{Reason: SeatRefusalGenerationMismatch}
	}
	e := s.entries[place]
	if e == nil || !e.wanted || e.cleaned {
		return &SeatOperationRefusalError{Reason: SeatRefusalEndpointUnavailable}
	}
	return s.writeDriver(place, driving)
}
func (server *Server) serveSeatDriver(session *serverSession, operation *sessionOperation, wire *framedConn, request frame) {
	var body SetSeatDriverRequest
	if !decodeRequest(wire, request.Body, &body) {
		return
	}
	if body.Generation == 0 || body.Place < 1 || body.Place > 4 {
		_ = writeFailure(wire, CodeInvalidRequest, "Seat generation and place are required")
		return
	}
	attempt, rpcErr := session.authorizeAttempt(body.Authority)
	if rpcErr != nil {
		_ = writeRPCError(wire, rpcErr)
		return
	}
	if !attempt.computer {
		_ = writeFailure(wire, CodeUnauthorizedAttempt, "Seat driver requires a live Computer attempt")
		return
	}
	engine, ok := server.engine.(SeatDriverEngine)
	if !ok {
		_ = writeFailure(wire, CodeUnsupportedOperation, "Seat driver unavailable")
		return
	}
	operation.monitorEOF()
	if err := engine.SetSeatDriver(operation.ctx, body); err != nil {
		_ = writeSeatOperationFailure(wire, err)
		return
	}
	_ = writeSuccess(wire, struct{}{})
}
