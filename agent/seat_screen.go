package agent

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/l1"
	workloadrunner "github.com/Derek-X-Wang/wefty/runner"
	"github.com/Derek-X-Wang/wefty/runner/ocihelper"
	"github.com/coder/websocket"
)

type seatScreenControl interface {
	AuthorizeSeatScreen(context.Context, string, l1.SeatScreenAuthorityInput) (l1.SeatScreenAuthority, error)
	RecordSeatScreenEvent(context.Context, string, l1.SeatScreenEventRequest) (l1.SeatScreenEventReceipt, error)
}
type seatScreenRuntime interface {
	ObserveComputerSeats(context.Context, ocihelper.ObserveComputerSeatsRequest) (ocihelper.ObserveComputerSeatsResponse, error)
	DialSeatEndpoint(context.Context, ocihelper.DialSeatEndpointRequest) (net.Conn, error)
	SetSeatDriver(context.Context, ocihelper.SetSeatDriverRequest) error
}
type seatScreenConfig struct {
	ctx               context.Context
	fabric            fabric.Fabric
	authorizer        computerGrantAuthorizer
	clock             Clock
	runtime           seatScreenRuntime
	control           seatScreenControl
	computer, storage string
	storageGeneration int64
	authority         ocihelper.AttemptAuthority
	fatal             func(error)
	logFailure        func(error)
}
type seatScreenHandler struct {
	config seatScreenConfig
	mu     sync.Mutex
	seats  map[string]*seatScreen
}
type seatScreen struct {
	ctx   context.Context
	front *computerFrontDoor
	gate  seatScreenGate
}
type seatAuthorityContextKey struct{}
type seatReleaseNoteKey struct{}

// A gate covers admission, not I/O. A person's take cancels and observes all
// previously admitted AI agent streams before the person gets a control dial.
type seatScreenGate struct {
	mu      sync.Mutex
	driving bool
	streams map[*seatScreenStream]struct{}
}
type seatScreenStream struct {
	cancel context.CancelFunc
	done   chan struct{}
}

func (g *seatScreenGate) admit(ctx context.Context) (context.Context, func(), error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.driving {
		return nil, nil, &ProtocolError{StatusCode: http.StatusConflict, APIError: contract.APIError{Code: "person_driving", Message: "a person is driving"}}
	}
	if g.streams == nil {
		g.streams = map[*seatScreenStream]struct{}{}
	}
	ctx, cancel := context.WithCancel(ctx)
	stream := &seatScreenStream{cancel: cancel, done: make(chan struct{})}
	g.streams[stream] = struct{}{}
	return ctx, func() {
		cancel()
		g.mu.Lock()
		delete(g.streams, stream)
		close(stream.done)
		g.mu.Unlock()
	}, nil
}
func (g *seatScreenGate) block(ctx context.Context) error {
	g.mu.Lock()
	g.driving = true
	streams := make([]*seatScreenStream, 0, len(g.streams))
	for s := range g.streams {
		streams = append(streams, s)
	}
	g.mu.Unlock()
	for _, s := range streams {
		s.cancel()
	}
	for _, s := range streams {
		select {
		case <-s.done:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}
func (g *seatScreenGate) unblock() {
	g.mu.Lock()
	g.driving = false
	g.mu.Unlock()
}

func newSeatScreenHandler(config seatScreenConfig) (*seatScreenHandler, error) {
	if config.ctx == nil || config.fabric == nil || config.authorizer == nil || config.runtime == nil || config.control == nil {
		return nil, errors.New("Seat screen dependencies are incomplete")
	}
	if config.clock == nil {
		config.clock = systemClock{}
	}
	return &seatScreenHandler{config: config, seats: map[string]*seatScreen{}}, nil
}
func (h *seatScreenHandler) input(credential string) l1.SeatCopyAuthorityInput {
	a := h.config.authority
	return l1.SeatCopyAuthorityInput{ComputerSeatAttemptRequest: l1.ComputerSeatAttemptRequest{NodeID: a.NodeID, BootSessionID: a.BootSessionID, FencingToken: a.FencingToken}, ComputerID: h.config.computer, JobID: a.JobID, AttemptID: a.AttemptID, Credential: credential}
}
func (h *seatScreenHandler) record(ctx context.Context, id string, e l1.SeatLogEvent) (l1.SeatScreenEventReceipt, error) {
	e.SeatID = id
	return h.config.control.RecordSeatScreenEvent(ctx, id, l1.SeatScreenEventRequest{SeatCopyAuthorityInput: h.input(""), Event: e})
}

// All helper names come from this generation's own observation, never a URL.
func (h *seatScreenHandler) dial(ctx context.Context, binding l1.SeatScreenAuthority, kind string) (net.Conn, error) {
	observed, err := h.config.runtime.ObserveComputerSeats(ctx, ocihelper.ObserveComputerSeatsRequest{Authority: h.config.authority, Generation: binding.Generation})
	if err != nil {
		return nil, err
	}
	if observed.Generation != binding.Generation {
		return nil, &ProtocolError{StatusCode: http.StatusServiceUnavailable, APIError: contract.APIError{Code: contract.ErrorUnavailable, Message: "Seat screen generation changed", Retryable: true}}
	}
	for _, seat := range observed.Seats {
		if seat.Place == binding.Place && seat.State == "ready" && seat.Endpoints[kind] != "" {
			return h.config.runtime.DialSeatEndpoint(ctx, ocihelper.DialSeatEndpointRequest{Authority: h.config.authority, Generation: binding.Generation, Place: binding.Place, Name: seat.Endpoints[kind]})
		}
	}
	return nil, &ProtocolError{StatusCode: http.StatusServiceUnavailable, APIError: contract.APIError{Code: contract.ErrorUnavailable, Message: "Seat screen is not ready", Retryable: true}}
}
func (h *seatScreenHandler) seat(ctx context.Context, id string) (*seatScreen, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if seat := h.seats[id]; seat != nil {
		return seat, nil
	}
	lifetime, _ := ctx.Value(seatAuthorityContextKey{}).(context.Context)
	if lifetime == nil {
		lifetime = h.config.ctx
	}
	seat := &seatScreen{ctx: lifetime}
	auth := func(ctx context.Context) (l1.SeatScreenAuthority, error) {
		return h.config.control.AuthorizeSeatScreen(ctx, id, l1.SeatScreenAuthorityInput{SeatCopyAuthorityInput: h.input(""), Person: true})
	}
	dial := func(ctx context.Context, kind string) (net.Conn, error) {
		binding, err := auth(ctx)
		if err != nil {
			return nil, err
		}
		return h.dial(ctx, binding, kind)
	}
	auditor := seatScreenAuditor{handler: h, id: id}
	front, err := newComputerFrontDoor(computerFrontDoorConfig{
		authorityContext: lifetime, fabric: h.config.fabric, authorizer: h.config.authorizer,
		auditor: auditor, clock: h.config.clock, computerID: h.config.computer,
		jobID: h.config.authority.JobID, attemptID: h.config.authority.AttemptID,
		storageID: h.config.storage, storageGeneration: h.config.storageGeneration,
		fencingToken: h.config.authority.FencingToken, dial: dial, administratorsOnly: true,
		seatViewerStatus: func(ctx context.Context) error { _, err := auth(ctx); return err },
		onReport: func(err error) {
			failure := fmt.Errorf("computer=%q seat=%q: %w", h.config.computer, id, err)
			var audit *computerAuditFailure
			if errors.As(err, &audit) && !isSeatScreenRefusal(err) && lifetime.Err() == nil && h.config.fatal != nil {
				h.config.fatal(failure)
			}
			if h.config.logFailure != nil {
				h.config.logFailure(failure)
			}
		},
	})
	if err != nil {
		return nil, err
	}
	var lastSignalBinding l1.SeatScreenAuthority
	tenure, err := newControllerTenure(controllerTenureConfig{authorityContext: lifetime, clock: h.config.clock, dial: dial,
		setControlState: func(ctx context.Context, driving bool) error {
			if !driving {
				err := h.clearSeatDriver(ctx, id, lastSignalBinding, lifetime)
				if err == nil {
					seat.gate.unblock()
				}
				return err
			}
			if err := seat.gate.block(ctx); err != nil {
				seat.gate.unblock()
				return err
			}
			binding, err := auth(ctx)
			attempted := err == nil
			if attempted {
				lastSignalBinding = binding
				err = h.config.runtime.SetSeatDriver(ctx, ocihelper.SetSeatDriverRequest{Authority: h.config.authority, Generation: binding.Generation, Place: binding.Place, HumanDriving: true})
			}
			if err != nil {
				// A transport failure can lose the acknowledgement after true
				// was published. Only a typed helper refusal proves no write.
				var refusal *ocihelper.RPCError
				if attempted && !errors.As(err, &refusal) {
					clearContext, cancelClear := context.WithTimeout(context.WithoutCancel(ctx), controllerTenureFinalizationLimit)
					clearErr := h.clearDriver(clearContext, id, binding, true)
					cancelClear()
					if clearErr != nil {
						failure := errors.Join(err, fmt.Errorf("Seat driver clear unconfirmed: %w", clearErr))
						front.SetReady(false)
						if lifetime.Err() == nil && h.config.fatal != nil {
							h.config.fatal(failure)
						}
						return failure
					}
				}
				seat.gate.unblock()
			}
			return err
		},
		record: func(ctx context.Context, e l1.ComputerTakeoverAuditEvent) (l1.ComputerTakeoverAuditReceipt, error) {
			return auditor.AppendComputerTakeoverAudit(ctx, h.config.computer, h.config.authority.JobID, h.config.authority.AttemptID, l1.ComputerTakeoverAuditRequest{FencingToken: h.config.authority.FencingToken, Event: e})
		},
		// Exhausted control-audit writes use the same policy as session audits.
		// A definitive authority refusal means this action had no authority;
		// an unavailable server or missing receipt cannot prove durability.
		onAuditFailure: func(err error) {
			if !isSeatScreenRefusal(err) && lifetime.Err() == nil && h.config.fatal != nil {
				h.config.fatal(fmt.Errorf("Seat control audit unconfirmed: %w", err))
			}
		},
		report: front.report, onUnconfirmedClear: func(err error) {
			front.SetReady(false)
			if lifetime.Err() == nil && h.config.fatal != nil {
				h.config.fatal(err)
			}
		},
	})
	if err != nil {
		return nil, err
	}
	front.config.controlTenure = tenure
	front.SetReady(true)
	seat.front = front
	h.seats[id] = seat
	go func() {
		for lifetime.Err() == nil {
			select {
			case <-lifetime.Done():
			case <-front.Errors():
				// onReport logs and classifies each report synchronously,
				// including reports emitted during session teardown. Draining
				// the notification queue does not discard audit failures.
				_ = front.takeErrors()
			}
		}
		front.EndSessions(l1.ComputerTakeoverAttemptAuthorityLost)
		front.WaitForSessions()
		h.mu.Lock()
		if h.seats[id] == seat {
			delete(h.seats, id)
		}
		h.mu.Unlock()
	}()
	return seat, nil
}

func isSeatScreenRefusal(err error) bool {
	if protocol, ok := err.(*ProtocolError); ok {
		return protocol.StatusCode >= 400 && protocol.StatusCode < 500
	}
	if refusal, ok := err.(*ocihelper.RPCError); ok {
		return refusal.Code == ocihelper.CodeSeatOperationRefused
	}
	if joined, ok := err.(interface{ Unwrap() []error }); ok {
		causes := joined.Unwrap()
		if len(causes) == 0 {
			return false
		}
		for _, cause := range causes {
			if !isSeatScreenRefusal(cause) {
				return false
			}
		}
		return true
	}
	if wrapped, ok := err.(interface{ Unwrap() error }); ok {
		return isSeatScreenRefusal(wrapped.Unwrap())
	}
	return false
}

const seatDriverClearLimit = 10 * time.Second

// A live desktop can briefly lose its ready observation during restart. Keep
// AI agent input blocked while retrying the clear, including its authority
// reads. The injected clock bounds both backoff and in-flight RPC cancellation.
func (h *seatScreenHandler) clearSeatDriver(parent context.Context, id string, last l1.SeatScreenAuthority, lifetime context.Context) error {
	ctx, cancel := context.WithCancel(parent)
	deadline := h.config.clock.Now().Add(seatDriverClearLimit)
	limit := h.config.clock.NewTimer(seatDriverClearLimit)
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		select {
		case <-limit.C():
			cancel()
		case <-ctx.Done():
		}
	}()
	defer func() { cancel(); stopTimer(limit); <-finished }()
	delay := 100 * time.Millisecond
	var lastErr error
	for {
		if lastErr != nil && (ctx.Err() != nil || !h.config.clock.Now().Before(deadline)) {
			return lastErr
		}
		binding, err := h.config.control.AuthorizeSeatScreen(ctx, id, l1.SeatScreenAuthorityInput{SeatCopyAuthorityInput: h.input(""), Person: true})
		var protocol *ProtocolError
		ended := errors.As(err, &protocol) && protocol.APIError.Code == "seat_ended"
		if err != nil && (ended || lifetime.Err() != nil) && last.Generation != 0 {
			return h.clearDriver(ctx, id, last, false)
		}
		if err == nil {
			err = h.clearDriver(ctx, id, binding, true)
			// The Seat can end during the second authority check, too.
			if last.Generation != 0 && errors.As(err, &protocol) && protocol.APIError.Code == "seat_ended" {
				return h.clearDriver(ctx, id, last, false)
			}
		}
		if err == nil {
			return nil
		}
		lastErr = err
		remaining := deadline.Sub(h.config.clock.Now())
		if remaining <= 0 {
			return err
		}
		timer := h.config.clock.NewTimer(min(delay, remaining))
		select {
		case <-timer.C():
		case <-ctx.Done():
			stopTimer(timer)
			return errors.Join(err, ctx.Err())
		}
		stopTimer(timer)
		delay = min(delay*2, time.Second)
	}
}

// Membership changes can advance the helper generation before L1's next
// observation. Clearing rechecks the live Seat identity after discovering the
// generation, then pins that generation for publication. A reused place can
// never inherit the old Seat's clear. Ended Seats use only their earlier pin.
func (h *seatScreenHandler) clearDriver(ctx context.Context, id string, binding l1.SeatScreenAuthority, live bool) error {
	var err error
	for range 3 {
		generation := binding.Generation
		if live {
			discovered, readErr := h.config.runtime.ObserveComputerSeats(ctx, ocihelper.ObserveComputerSeatsRequest{Authority: h.config.authority, Discover: true})
			if readErr != nil {
				return readErr
			}
			current, readErr := h.config.control.AuthorizeSeatScreen(ctx, id, l1.SeatScreenAuthorityInput{SeatCopyAuthorityInput: h.input(""), Person: true})
			if readErr != nil {
				return readErr
			}
			present := false
			for _, seat := range discovered.Seats {
				if seat.Place == binding.Place {
					present = true
				}
			}
			if !present || current.Place != binding.Place || discovered.Generation == 0 {
				return &ocihelper.RPCError{Code: ocihelper.CodeSeatOperationRefused, Detail: ocihelper.ErrorDetail(ocihelper.SeatRefusalEndpointUnavailable)}
			}
			generation = discovered.Generation
		}
		err = h.config.runtime.SetSeatDriver(ctx, ocihelper.SetSeatDriverRequest{Authority: h.config.authority, Generation: generation, Place: binding.Place, HumanDriving: false})
		var refusal *ocihelper.RPCError
		if !live && errors.As(err, &refusal) && refusal.Code == ocihelper.CodeSeatOperationRefused {
			// L1 has ended this Seat (or revoked its lifetime). The old pin
			// cannot affect a replacement. A helper refusal proves it is gone.
			return nil
		}
		if !live || !errors.As(err, &refusal) || refusal.Code != ocihelper.CodeSeatOperationRefused || refusal.Detail != ocihelper.ErrorDetail(ocihelper.SeatRefusalGenerationMismatch) {
			return err
		}
	}
	return err
}

type seatScreenAuditor struct {
	handler *seatScreenHandler
	id      string
}

func (a seatScreenAuditor) AppendComputerTakeoverAudit(ctx context.Context, _, _, _ string, r l1.ComputerTakeoverAuditRequest) (l1.ComputerTakeoverAuditReceipt, error) {
	kind := l1.SeatEventTakeover
	if r.Event.Kind == l1.ComputerTakeoverControlAcquired {
		kind = l1.SeatEventControlTaken
	}
	if r.Event.Kind == l1.ComputerTakeoverControlReleased {
		kind = l1.SeatEventControlReleased
	}
	note, _ := ctx.Value(seatReleaseNoteKey{}).(string)
	if kind != l1.SeatEventControlReleased {
		note = ""
	}
	e := l1.SeatLogEvent{EventID: r.Event.EventID, Kind: kind, Reason: string(r.Event.Reason), Note: note, Audit: &r.Event}
	receipt, err := a.handler.record(ctx, a.id, e)
	if err != nil {
		return l1.ComputerTakeoverAuditReceipt{}, err
	}
	if receipt.Event.Audit == nil {
		return l1.ComputerTakeoverAuditReceipt{}, errors.New("Seat log receipt has no take-over evidence")
	}
	return l1.ComputerTakeoverAuditReceipt{Event: *receipt.Event.Audit, Replayed: receipt.Replayed}, nil
}

func (h *seatScreenHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	match := seatFrontDoorPath.FindStringSubmatch(r.URL.Path)
	if len(match) != 2 {
		http.NotFound(w, r)
		return
	}
	id := match[1]
	suffix := strings.TrimPrefix(r.URL.Path, "/v1/seats/"+id)
	person := strings.HasPrefix(suffix, "/person/") && r.Header.Get(l1.SeatCredentialHeader) == ""
	if person {
		seat, err := h.seat(r.Context(), id)
		if err != nil {
			writeSeatScreenError(w, err)
			return
		}
		rewritten := r.Clone(r.Context())
		copiedURL := *r.URL
		rewritten.URL = &copiedURL
		rewritten.URL.Path = strings.TrimPrefix(suffix, "/person")
		seat.front.ServeHTTP(w, rewritten)
		return
	}
	if r.Method != http.MethodGet || (suffix != "/screen/view" && suffix != "/screen/input") || r.URL.RawQuery != "" {
		http.NotFound(w, r)
		return
	}
	if !exactComputerSubprotocol(r.Header.Values("Sec-WebSocket-Protocol")) {
		http.Error(w, "Seat screen requires the binary WebSocket subprotocol", http.StatusBadRequest)
		return
	}
	input := suffix == "/screen/input"
	binding, err := h.config.control.AuthorizeSeatScreen(r.Context(), id, l1.SeatScreenAuthorityInput{SeatCopyAuthorityInput: h.input(r.Header.Get(l1.SeatCredentialHeader)), Input: input})
	if err != nil {
		writeSeatScreenError(w, err)
		return
	}
	seat, err := h.seat(r.Context(), id)
	if err != nil {
		writeSeatScreenError(w, err)
		return
	}
	ctx := r.Context()
	finish := func() {}
	if input {
		ctx, finish, err = seat.gate.admit(ctx)
		if err != nil {
			writeSeatScreenError(w, err)
			return
		}
	}
	defer finish()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	stopSeat := context.AfterFunc(seat.ctx, cancel)
	defer stopSeat()
	kind := workloadrunner.AttemptEndpointView
	if input {
		kind = workloadrunner.AttemptEndpointControl
	}
	backend, backendWS, banner, err := dialComputerBackend(ctx, func(ctx context.Context, _ string) (net.Conn, error) { return h.dial(ctx, binding, kind) }, kind)
	if err != nil {
		writeSeatScreenError(w, err)
		return
	}
	defer backendWS.CloseNow()
	defer backend.Close()
	stopBackend := context.AfterFunc(ctx, func() { _ = backendWS.CloseNow() })
	defer stopBackend()
	if input {
		if _, err = h.record(ctx, id, l1.SeatLogEvent{EventID: rand.Text(), Kind: l1.SeatEventScreenInput, Reason: "ai_agent"}); err != nil {
			writeSeatScreenError(w, err)
			return
		}
	}
	if binding.ReleaseNote != "" {
		w.Header().Set("Wefty-Seat-Release-Note", base64.RawURLEncoding.EncodeToString([]byte(binding.ReleaseNote)))
	}
	w.Header().Set("Wefty-Seat-Unread-Events", fmt.Sprint(binding.UnreadEvents))
	clientWS, err := websocket.Accept(w, r, &websocket.AcceptOptions{Subprotocols: []string{computerWebSocketSubprotocol}})
	if err != nil {
		return
	}
	defer clientWS.CloseNow()
	stopClient := context.AfterFunc(ctx, func() { _ = clientWS.CloseNow() })
	defer stopClient()
	client := websocket.NetConn(ctx, clientWS, websocket.MessageBinary)
	if _, err = client.Write(banner); err != nil {
		return
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = io.Copy(backend, client)
		cancel()
	}()
	_, _ = io.Copy(client, backend)
	cancel()
	_ = clientWS.CloseNow()
	_ = backendWS.CloseNow()
	<-done
}
func writeSeatScreenError(w http.ResponseWriter, err error) {
	api := contract.APIError{Code: contract.ErrorUnavailable, Message: "Seat screen unavailable", Retryable: true}
	status := http.StatusServiceUnavailable
	var protocol *ProtocolError
	if errors.As(err, &protocol) {
		api = protocol.APIError
		status = protocol.StatusCode
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(contract.ErrorResponse{Error: api})
}
func (c *Client) AuthorizeSeatScreen(ctx context.Context, id string, r l1.SeatScreenAuthorityInput) (l1.SeatScreenAuthority, error) {
	var value l1.SeatScreenAuthority
	err := c.post(ctx, "/v1/agent/seats/"+id+"/screen-authority", r, &value)
	return value, err
}
func (c *Client) RecordSeatScreenEvent(ctx context.Context, id string, r l1.SeatScreenEventRequest) (l1.SeatScreenEventReceipt, error) {
	var receipt l1.SeatScreenEventReceipt
	err := c.post(ctx, "/v1/agent/seats/"+id+"/screen-event", r, &receipt)
	return receipt, err
}
