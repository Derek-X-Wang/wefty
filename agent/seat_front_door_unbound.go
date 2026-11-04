package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/l1"
	workloadrunner "github.com/Derek-X-Wang/wefty/runner"
	"github.com/Derek-X-Wang/wefty/runner/ocihelper"
)

func unknownSeatError() *ProtocolError {
	return &ProtocolError{StatusCode: http.StatusNotFound, APIError: contract.APIError{Code: contract.ErrorNotFound, Message: "unknown Seat"}}
}

// A null binding can authenticate, but can never authorize a runtime operation.
// No Computer, Attempt, session, timer or durable audit target is created.
type unboundSeatBinding struct{ authentication seatFrontDoorAuthentication }

func (b unboundSeatBinding) AuthorizeSeatScreen(ctx context.Context, id string, input l1.SeatScreenAuthorityInput) (l1.SeatScreenAuthority, error) {
	if b.authentication.control == nil {
		return l1.SeatScreenAuthority{}, errors.New("Seat authentication unavailable")
	}
	if err := b.authentication.control.AuthenticateSeatScreen(ctx, id, input.Credential); err != nil {
		return l1.SeatScreenAuthority{}, err
	}
	return l1.SeatScreenAuthority{}, unknownSeatError()
}
func (b unboundSeatBinding) AuthorizeSeatCopy(ctx context.Context, id string, input l1.SeatCopyAuthorityInput) (l1.SeatCopyAuthority, error) {
	if b.authentication.control == nil {
		return l1.SeatCopyAuthority{}, errors.New("Seat authentication unavailable")
	}
	if err := b.authentication.control.AuthenticateSeatCopy(ctx, id, input.Credential); err != nil {
		return l1.SeatCopyAuthority{}, err
	}
	return l1.SeatCopyAuthority{}, unknownSeatError()
}
func (unboundSeatBinding) RecordSeatScreenEvent(context.Context, string, l1.SeatScreenEventRequest) (l1.SeatScreenEventReceipt, error) {
	return l1.SeatScreenEventReceipt{}, unknownSeatError()
}
func (unboundSeatBinding) RecordSeatCopy(context.Context, string, l1.SeatCopyReceipt) error {
	return unknownSeatError()
}
func (unboundSeatBinding) CopyComputerSeat(context.Context, ocihelper.CopyComputerSeatRequest, io.Reader, func([]byte) error) (int64, error) {
	return 0, unknownSeatError()
}
func (b unboundSeatBinding) Revision() int64 {
	if b.authentication.policy == nil {
		return 0
	}
	return b.authentication.policy.Revision()
}
func (b unboundSeatBinding) AcquireGrant(_ string, identity fabric.Identity) (*ComputerGrantAuthorization, error) {
	if b.authentication.policy == nil {
		return nil, nil
	}
	administrator, revision := b.authentication.policy.CheckAdministrator(identity)
	if !administrator {
		return nil, nil
	}
	return &ComputerGrantAuthorization{decision: computerGrantDecision{Permission: l1.ComputerGrantControl, Administrator: true, PolicyRevision: revision}}, nil
}

// Reuse the bound handlers for validation, admission and error encoding. The
// person handler stops after policy admission; the screen/copy control always
// returns a refusal or unknown Seat. Thus no constructor starts lifetime work.
func (d *seatFrontDoor) serveUnbound(w http.ResponseWriter, r *http.Request, id string, person bool, identity fabric.Identity) {
	binding := unboundSeatBinding{authentication: d.authentication}
	front := &computerFrontDoor{unbound: true, ready: true, config: computerFrontDoorConfig{fabric: d.fabric, clock: systemClock{}, authorizer: binding, administratorsOnly: true}}
	screens := &seatScreenHandler{config: seatScreenConfig{ctx: r.Context(), control: binding}, seats: map[string]*seatScreen{id: {ctx: r.Context(), front: front}}}
	mux := newSeatFeatureMux(screens, newSeatCopyHandler(binding, binding, workloadrunner.AttemptAuthority{}, ""))
	response := &unboundSeatResponse{header: make(http.Header)}
	mux.ServeHTTP(response, r)
	var failure contract.ErrorResponse
	if json.Unmarshal(response.body.Bytes(), &failure) == nil && failure.Error.Code != "" {
		if failure.Error.Code != contract.ErrorNotFound || failure.Error.Message != "unknown Seat" {
			d.logRefusal(seatDoorPathKind(person), identity, failure.Error.Code)
		}
	} else if response.status >= 400 {
		code := contract.ErrorInvalidRequest
		switch response.status {
		case http.StatusUnauthorized:
			code = contract.ErrorUnauthorized
		case http.StatusForbidden:
			code = contract.ErrorForbidden
		case http.StatusNotFound:
			code = contract.ErrorNotFound
		case http.StatusServiceUnavailable:
			code = contract.ErrorUnavailable
		}
		d.logRefusal(seatDoorPathKind(person), identity, code)
	}
	for name, values := range response.header {
		w.Header()[name] = values
	}
	w.WriteHeader(response.status)
	_, _ = w.Write(response.body.Bytes())
}

// Unbound requests can only produce finite refusals, never a stream or upgrade.
// Capture their existing response shape to log exactly one final refusal code.
type unboundSeatResponse struct {
	header http.Header
	body   bytes.Buffer
	status int
}

func (w *unboundSeatResponse) Header() http.Header { return w.header }
func (w *unboundSeatResponse) WriteHeader(status int) {
	if w.status == 0 {
		w.status = status
	}
}
func (w *unboundSeatResponse) Write(data []byte) (int, error) {
	if w.status == 0 {
		w.status = http.StatusOK
	}
	return w.body.Write(data)
}
