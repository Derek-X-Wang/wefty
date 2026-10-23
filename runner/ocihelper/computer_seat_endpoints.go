package ocihelper

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
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
		if e.pinOwner != nil {
			e.pinOwner.closeEndpointPin(f)
		} else {
			f.Close()
		}
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

// beforeDiskDetach runs after attempt authority is retired, without seatsMu.
// Keeping the detach call here makes the descriptor-release order explicit.
func (s *computerSeats) beforeDiskDetach(detach func() error) error {
	s.closeEndpointPins()
	return detach()
}

// Retirement is permanent and independent of entries, which may be changing
// under seatsMu. It closes ready AND provisional pins before unmount begins.
func (s *computerSeats) closeEndpointPins() {
	if s == nil {
		return
	}
	s.pinMu.Lock()
	defer s.pinMu.Unlock()
	s.pinsRetired = true
	for pin := range s.pins {
		pin.Close()
		delete(s.pins, pin)
	}
}

// openEndpointPin keeps the short no-follow filesystem walk atomic with
// retirement. The callback must only open/validate local descriptors; it must
// never probe a listener, dial, call containerd or wait for another goroutine.
func (s *computerSeats) openEndpointPin(open func() (*os.File, error)) (*os.File, error) {
	s.pinMu.Lock()
	defer s.pinMu.Unlock()
	if s.pinsRetired {
		return nil, errors.New("Computer Seat endpoints retired")
	}
	pin, err := open()
	if err != nil {
		return nil, err
	}
	if s.pins == nil {
		s.pins = map[*os.File]struct{}{}
	}
	s.pins[pin] = struct{}{}
	return pin, nil
}
func (s *computerSeats) closeEndpointPin(pin *os.File) {
	s.pinMu.Lock()
	defer s.pinMu.Unlock()
	pin.Close()
	delete(s.pins, pin)
}
