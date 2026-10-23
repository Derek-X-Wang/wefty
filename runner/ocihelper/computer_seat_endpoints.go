package ocihelper

import (
	"context"
	"fmt"
	"io"
	"net"
)

const MethodDialSeatEndpoint Method = "DialSeatEndpoint"
const SeatRefusalEndpointMismatch SeatOperationRefusalReason = "endpoint_mismatch"
const SeatRefusalEndpointChanged SeatOperationRefusalReason = "endpoint_changed"
const SeatRefusalEndpointUnavailable SeatOperationRefusalReason = "endpoint_unavailable"

// No path or port supplied by a caller is ever dialled. Generation and place
// select one exact Seat; Name must be an endpoint published for that incarnation.
type DialSeatEndpointRequest struct {
	Authority  AttemptAuthority `json:"authority"`
	Generation uint64           `json:"generation"`
	Place      int              `json:"place"`
	Name       string           `json:"name"`
}
type ComputerSeatEndpointEngine interface {
	DialSeatEndpoint(context.Context, DialSeatEndpointRequest, io.ReadWriteCloser) error
}

func seatEndpointName(place int, incarnation uint64, kind string) string {
	return fmt.Sprintf("seat-%d-%d-%s", place, incarnation, kind)
}
func seatEndpointPath(place int, kind string) string {
	return fmt.Sprintf("/wefty/service/seats/%d/run/%s.ws", place, kind)
}
func (s *computerSeats) endpoint(r DialSeatEndpointRequest) (string, error) {
	if r.Generation == 0 || r.Generation != s.generation {
		return "", &SeatOperationRefusalError{Reason: SeatRefusalGenerationMismatch}
	}
	e := s.entries[r.Place]
	if e == nil || !e.wanted || !e.ready || e.failure != "" || e.readyGeneration != r.Generation {
		return "", &SeatOperationRefusalError{Reason: SeatRefusalEndpointUnavailable}
	}
	for _, kind := range []string{"view", "control"} {
		if r.Name == e.endpointName(kind) {
			return kind, nil
		}
	}
	return "", &SeatOperationRefusalError{Reason: SeatRefusalEndpointMismatch}
}
func (e *computerSeatEntry) closeEndpointPins() {
	for _, f := range e.endpointPins {
		f.Close()
	}
	e.endpointPins = nil
}
func (session *Session) DialSeatEndpoint(ctx context.Context, request DialSeatEndpointRequest) (net.Conn, error) {
	return session.openStream(ctx, MethodDialSeatEndpoint, request, attemptPortBackendReady, true)
}

func (e *computerSeatEntry) endpointName(kind string) string {
	name := e.endpointScope + "-" + seatEndpointName(e.config.Place, e.incarnation, kind)
	if e.endpointRevision > 0 {
		name = fmt.Sprintf("%s-%d", name, e.endpointRevision)
	}
	return name
}
