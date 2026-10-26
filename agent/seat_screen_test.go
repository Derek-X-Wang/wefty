package agent

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/l1"
	"github.com/Derek-X-Wang/wefty/runner/ocihelper"
	"github.com/coder/websocket"
)

type screenControlFixture struct {
	mu                              sync.Mutex
	events                          []l1.SeatLogEvent
	failControlAckOnce              bool
	generation                      uint64
	authorityFailure                error
	authoritySuccessesBeforeFailure int
	authorityFailuresRemaining      int
	takeFailure                     error
	sessionFailure                  error
	sessionFailureKind              l1.ComputerTakeoverAuditEventKind
	sessionAttempts                 chan struct{}
}

func (f *screenControlFixture) AuthorizeSeatScreen(_ context.Context, id string, r l1.SeatScreenAuthorityInput) (l1.SeatScreenAuthority, error) {
	place := 1
	if strings.HasSuffix(id, "2") {
		place = 2
	}
	if !r.Person || r.Credential != "" {
		if r.Credential != id {
			return l1.SeatScreenAuthority{}, &ProtocolError{StatusCode: 403, APIError: contract.APIError{Code: contract.ErrorForbidden, Message: "invalid Seat credential"}}
		}
	}
	note := ""
	f.mu.Lock()
	if strings.HasSuffix(id, "1") && f.authorityFailure != nil && f.authoritySuccessesBeforeFailure == 0 {
		err := f.authorityFailure
		if f.authorityFailuresRemaining > 0 {
			f.authorityFailuresRemaining--
			if f.authorityFailuresRemaining == 0 {
				f.authorityFailure = nil
			}
		}
		f.mu.Unlock()
		return l1.SeatScreenAuthority{}, err
	}
	if strings.HasSuffix(id, "1") && f.authoritySuccessesBeforeFailure > 0 {
		f.authoritySuccessesBeforeFailure--
	}
	generation := f.generation
	if generation == 0 {
		generation = 1
	}
	for _, e := range f.events {
		if e.SeatID == id && e.Kind == l1.SeatEventControlReleased {
			note = e.Note
		}
	}
	f.mu.Unlock()
	return l1.SeatScreenAuthority{SeatCopyAuthority: l1.SeatCopyAuthority{Place: place, Generation: generation}, ReleaseNote: note}, nil
}
func (f *screenControlFixture) RecordSeatScreenEvent(_ context.Context, _ string, r l1.SeatScreenEventRequest) (l1.SeatScreenEventReceipt, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if strings.HasSuffix(r.Event.SeatID, "1") && r.Event.Kind == l1.SeatEventControlTaken && f.takeFailure != nil {
		var protocol *ProtocolError
		if errors.As(f.takeFailure, &protocol) && protocol.APIError.Code == "seat_ended" {
			f.authorityFailure = f.takeFailure
		}
		return l1.SeatScreenEventReceipt{}, f.takeFailure
	}
	if r.Event.Audit != nil && r.Event.Audit.Kind == f.sessionFailureKind && f.sessionFailure != nil {
		if f.sessionAttempts != nil {
			f.sessionAttempts <- struct{}{}
		}
		return l1.SeatScreenEventReceipt{}, f.sessionFailure
	}
	found := false
	for _, event := range f.events {
		if event.EventID == r.Event.EventID {
			found = true
			break
		}
	}
	if !found {
		f.events = append(f.events, r.Event)
	}
	if r.Event.Kind == l1.SeatEventControlTaken && f.failControlAckOnce {
		f.failControlAckOnce = false
		return l1.SeatScreenEventReceipt{}, errors.New("take-over audit acknowledgement lost")
	}
	receipt := l1.SeatScreenEventReceipt{Event: r.Event}
	if r.Event.Audit != nil {
		audit := *r.Event.Audit
		audit.AuthorityGeneration = 1
		receipt.Event.Audit = &audit
	}
	return receipt, nil
}

type screenRuntimeFixture struct {
	view, control      *computerBackendServer
	mu                 sync.Mutex
	drivers            map[int]bool
	failSignalTrue     bool
	failSignalFalse    bool
	generation         uint64
	hidePlaceOne       bool
	changedObservation bool
	endpointLookup     bool
}

func (f *screenRuntimeFixture) ObserveComputerSeats(_ context.Context, r ocihelper.ObserveComputerSeatsRequest) (ocihelper.ObserveComputerSeatsResponse, error) {
	f.mu.Lock()
	f.endpointLookup = r.EndpointLookup
	generation := f.generation
	if generation == 0 {
		generation = 1
	}
	hidePlaceOne := f.hidePlaceOne
	changedObservation := f.changedObservation
	f.changedObservation = false
	f.mu.Unlock()
	if changedObservation {
		return ocihelper.ObserveComputerSeatsResponse{Generation: generation + 1}, nil
	}
	if hidePlaceOne {
		return ocihelper.ObserveComputerSeatsResponse{Generation: generation, Seats: []ocihelper.ComputerSeatObservation{{Place: 2, State: "ready", Endpoints: map[string]string{"control": "two-control"}}}}, nil
	}
	if !r.Discover && r.Generation != generation {
		return ocihelper.ObserveComputerSeatsResponse{}, &ocihelper.RPCError{Code: ocihelper.CodeSeatOperationRefused, Detail: ocihelper.ErrorDetail(ocihelper.SeatRefusalGenerationMismatch)}
	}
	return ocihelper.ObserveComputerSeatsResponse{Generation: generation, Seats: []ocihelper.ComputerSeatObservation{{Place: 1, State: "ready", Endpoints: map[string]string{"view": "one-view", "control": "one-control"}}, {Place: 2, State: "ready", Endpoints: map[string]string{"view": "two-view", "control": "two-control"}}}}, nil
}
func (f *screenRuntimeFixture) DialSeatEndpoint(ctx context.Context, r ocihelper.DialSeatEndpointRequest) (net.Conn, error) {
	if r.Place == 1 && !strings.HasPrefix(r.Name, "one-") || r.Place == 2 && !strings.HasPrefix(r.Name, "two-") {
		return nil, errors.New("endpoint crossover")
	}
	if strings.HasSuffix(r.Name, "control") {
		return f.control.dial(ctx)
	}
	return f.view.dial(ctx)
}
func (f *screenRuntimeFixture) SetSeatDriver(_ context.Context, r ocihelper.SetSeatDriverRequest) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	generation := f.generation
	if generation == 0 {
		generation = 1
	}
	if r.Generation != generation {
		return &ocihelper.RPCError{Code: ocihelper.CodeSeatOperationRefused, Detail: ocihelper.ErrorDetail(ocihelper.SeatRefusalGenerationMismatch)}
	}
	if !r.HumanDriving && f.failSignalFalse {
		return errors.New("signal clear unconfirmed")
	}
	f.drivers[r.Place] = r.HumanDriving
	if r.HumanDriving && f.failSignalTrue {
		return errors.New("signal write acknowledgement lost")
	}
	return nil
}

func TestSeatScreenFrontDoorPersonWinsOnlyOwnSeat(t *testing.T) {
	fixture, cache, _, _, oldServer, identity := computerFrontDoorFixture(t, l1.ComputerGrantControl)
	oldServer.Close()
	snapshot := policySnapshot(t, fixture.clock.Now(), 1, 2, []l1.ComputerPolicyAdmin{{FabricID: identity.FabricID, UserID: identity.UserID}})
	if _, err := cache.Install(snapshot); err != nil {
		t.Fatal(err)
	}
	runtime := &screenRuntimeFixture{view: newComputerBackend(t, computerBackendOptions{}), control: newComputerBackend(t, computerBackendOptions{rfbHandshake: true}), drivers: map[int]bool{}}
	defer runtime.view.Close()
	defer runtime.control.Close()
	control := &screenControlFixture{failControlAckOnce: true}
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	handler, err := newSeatScreenHandler(seatScreenConfig{ctx: ctx, fabric: fixture.identity, authorizer: cache, clock: fixture.clock, runtime: runtime, control: control, fatal: func(error) { cancel() }, computer: "computer-1", storage: "storage-1", storageGeneration: 1, authority: ocihelper.AttemptAuthority{NodeID: "node-1", BootSessionID: "boot-1", JobID: "job-1", AttemptID: "attempt-1", FencingToken: "fence-1"}})
	if err != nil {
		t.Fatal(err)
	}
	door := newSeatFrontDoor(fixture.identity)
	registration, err := door.register(ctx, "computer-1", "attempt-1", handler)
	if err != nil {
		t.Fatal(err)
	}
	defer registration.close()
	one := "seat_00000000000000000000000000000001"
	two := "seat_00000000000000000000000000000002"
	if err = registration.bind([]string{one, two}); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(door)
	defer server.Close()
	dial := func(id, kind, credential string) (*websocket.Conn, *http.Response, error) {
		headers := http.Header{}
		if credential != "" {
			headers.Set(l1.SeatCredentialHeader, credential)
		}
		return websocket.Dial(ctx, "ws"+strings.TrimPrefix(server.URL, "http")+"/v1/seats/"+id+"/"+kind, &websocket.DialOptions{Subprotocols: []string{"binary"}, HTTPHeader: headers})
	}
	ai, _, err := dial(one, "screen/input", one)
	if err != nil {
		t.Fatal(err)
	}
	defer ai.CloseNow()
	_, _, err = ai.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}
	person, response, err := dial(one, "person/websockify", "")
	if err != nil {
		t.Fatal(err)
	}
	defer person.CloseNow()
	_, _, err = person.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}
	token := response.Header.Get(computerControlTokenHeader)
	if status := postComputerControl(t, server.URL, "/v1/seats/"+one+"/person"+computerControlTakePath, token); status != 200 {
		t.Fatalf("take status=%d", status)
	}
	if _, _, err = ai.Read(ctx); err == nil {
		t.Fatal("existing AI agent input connection survived take")
	}
	blocked, reply, err := dial(one, "screen/input", one)
	if blocked != nil {
		blocked.CloseNow()
	}
	if err == nil || reply.StatusCode != 409 {
		t.Fatalf("person-driving refusal: %v %+v", err, reply)
	}
	var failure contract.ErrorResponse
	_ = json.NewDecoder(reply.Body).Decode(&failure)
	reply.Body.Close()
	if failure.Error.Code != "person_driving" || failure.Error.Message != "a person is driving" {
		t.Fatalf("refusal=%+v", failure)
	}
	for _, tc := range []struct{ id, kind string }{{one, "screen/view"}, {two, "screen/input"}} {
		conn, _, err := dial(tc.id, tc.kind, tc.id)
		if err != nil {
			t.Fatalf("other screen blocked: %v", err)
		}
		conn.CloseNow()
	}
	conn, reply, err := dial(two, "screen/view", one)
	if conn != nil {
		conn.CloseNow()
	}
	if err == nil || reply.StatusCode != 403 {
		t.Fatalf("credential crossover=%v", err)
	}
	reply.Body.Close()
	releaseRequest, _ := http.NewRequestWithContext(ctx, http.MethodPost, server.URL+"/v1/seats/"+one+"/person"+computerControlReleasePath, strings.NewReader(`{"note":"Browser is ready."}`))
	releaseRequest.Header.Set(computerControlTokenHeader, token)
	released, err := http.DefaultClient.Do(releaseRequest)
	if err != nil {
		t.Fatal(err)
	}
	released.Body.Close()
	if released.StatusCode != 200 {
		t.Fatalf("release status=%d", released.StatusCode)
	}
	conn, response, err = dial(one, "screen/input", one)
	if err != nil {
		t.Fatal(err)
	}
	conn.CloseNow()
	note, err := base64.RawURLEncoding.DecodeString(response.Header.Get("Wefty-Seat-Release-Note"))
	if err != nil || string(note) != "Browser is ready." {
		t.Fatalf("release note on next action = %q %v", note, err)
	}
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	if runtime.drivers[1] || runtime.drivers[2] {
		t.Fatalf("driver signal=%v", runtime.drivers)
	}
}

func TestSeatScreenDoorPreservesIdentityLookupFailure(t *testing.T) {
	id := "seat_00000000000000000000000000000001"
	identity := &mutableWhoIsFabric{}
	door := newSeatFrontDoor(identity)
	registration, err := door.register(t.Context(), "computer", "attempt", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(204) }))
	if err != nil {
		t.Fatal(err)
	}
	defer registration.close()
	if err = registration.bind([]string{id}); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		failure error
		status  int
	}{{errors.New("private identity lookup cause"), 503}, {fabric.ErrIdentityNotFound, 401}} {
		identity.set(fabric.Identity{}, tc.failure)
		request := httptest.NewRequest(http.MethodGet, "/v1/seats/"+id+"/person/websockify", nil)
		request.Host = "127.0.0.1" // A valid door host keeps this test at the identity boundary.
		reply := httptest.NewRecorder()
		door.ServeHTTP(reply, request)
		if reply.Code != tc.status || strings.Contains(reply.Body.String(), "private identity lookup cause") {
			t.Fatalf("identity refusal=%d %q", reply.Code, reply.Body.String())
		}
	}
}

func TestSeatScreenFailedSignalWriteClearsBeforeReturningFree(t *testing.T) {
	for _, clearFails := range []bool{false, true} {
		t.Run(fmt.Sprint("clear_failure=", clearFails), func(t *testing.T) {
			fixture, cache, _, _, oldServer, identity := computerFrontDoorFixture(t, l1.ComputerGrantControl)
			oldServer.Close()
			if _, err := cache.Install(policySnapshot(t, fixture.clock.Now(), 1, 2, []l1.ComputerPolicyAdmin{{FabricID: identity.FabricID, UserID: identity.UserID}})); err != nil {
				t.Fatal(err)
			}
			runtime := &screenRuntimeFixture{view: newComputerBackend(t, computerBackendOptions{}), control: newComputerBackend(t, computerBackendOptions{rfbHandshake: true}), drivers: map[int]bool{}, failSignalTrue: true, failSignalFalse: clearFails}
			defer runtime.view.Close()
			defer runtime.control.Close()
			ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
			defer cancel()
			fatal := make(chan error, 1)
			handler, err := newSeatScreenHandler(seatScreenConfig{ctx: ctx, fabric: fixture.identity, authorizer: cache, clock: fixture.clock, runtime: runtime, control: &screenControlFixture{}, computer: "computer-1", storage: "storage-1", storageGeneration: 1, authority: ocihelper.AttemptAuthority{NodeID: "node-1", BootSessionID: "boot-1", JobID: "job-1", AttemptID: "attempt-1", FencingToken: "fence-1"}, fatal: func(err error) { fatal <- err; cancel() }})
			if err != nil {
				t.Fatal(err)
			}
			door := newSeatFrontDoor(fixture.identity)
			registration, err := door.register(ctx, "computer-1", "attempt-1", handler)
			if err != nil {
				t.Fatal(err)
			}
			defer registration.close()
			id := "seat_00000000000000000000000000000001"
			if err = registration.bind([]string{id}); err != nil {
				t.Fatal(err)
			}
			server := httptest.NewServer(door)
			defer server.Close()
			person, response, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(server.URL, "http")+"/v1/seats/"+id+"/person/websockify", &websocket.DialOptions{Subprotocols: []string{"binary"}})
			if err != nil {
				t.Fatal(err)
			}
			defer person.CloseNow()
			if _, _, err = person.Read(ctx); err != nil {
				t.Fatal(err)
			}
			if status := postComputerControl(t, server.URL, "/v1/seats/"+id+"/person"+computerControlTakePath, response.Header.Get(computerControlTokenHeader)); status != 503 {
				t.Fatalf("take after ambiguous signal=%d", status)
			}
			if clearFails {
				select {
				case <-fatal:
				default:
					t.Fatal("unconfirmed driver clear did not withdraw Attempt")
				}
				return
			}
			runtime.mu.Lock()
			driving := runtime.drivers[1]
			runtime.mu.Unlock()
			if driving {
				t.Fatal("failed take left driver true without Controller tenure")
			}
		})
	}
}

func TestSeatScreenMembershipChangeRefusesOnlyTheActionAndReleaseUsesCurrentGeneration(t *testing.T) {
	fixture, cache, _, _, oldServer, identity := computerFrontDoorFixture(t, l1.ComputerGrantControl)
	oldServer.Close()
	if _, err := cache.Install(policySnapshot(t, fixture.clock.Now(), 1, 2, []l1.ComputerPolicyAdmin{{FabricID: identity.FabricID, UserID: identity.UserID}})); err != nil {
		t.Fatal(err)
	}
	runtime := &screenRuntimeFixture{view: newComputerBackend(t, computerBackendOptions{}), control: newComputerBackend(t, computerBackendOptions{rfbHandshake: true}), drivers: map[int]bool{}}
	defer runtime.view.Close()
	defer runtime.control.Close()
	control := &screenControlFixture{}
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	handler, err := newSeatScreenHandler(seatScreenConfig{ctx: ctx, fabric: fixture.identity, authorizer: cache, clock: fixture.clock, runtime: runtime, control: control, computer: "computer-1", storage: "storage-1", storageGeneration: 1, authority: ocihelper.AttemptAuthority{NodeID: "node-1", BootSessionID: "boot-1", JobID: "job-1", AttemptID: "attempt-1", FencingToken: "fence-1"}, fatal: func(error) { cancel() }})
	if err != nil {
		t.Fatal(err)
	}
	door := newSeatFrontDoor(fixture.identity)
	registration, err := door.register(ctx, "computer-1", "attempt-1", handler)
	if err != nil {
		t.Fatal(err)
	}
	defer registration.close()
	one := "seat_00000000000000000000000000000001"
	two := "seat_00000000000000000000000000000002"
	if err = registration.bind([]string{one, two}); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(door)
	defer server.Close()
	person, response, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(server.URL, "http")+"/v1/seats/"+one+"/person/websockify", &websocket.DialOptions{Subprotocols: []string{"binary"}})
	if err != nil {
		t.Fatal(err)
	}
	defer person.CloseNow()
	if _, _, err = person.Read(ctx); err != nil {
		t.Fatal(err)
	}
	token := response.Header.Get(computerControlTokenHeader)
	runtime.mu.Lock()
	runtime.generation = 2
	runtime.mu.Unlock()
	refused := postComputerControlFailure(t, server.URL, "/v1/seats/"+one+"/person"+computerControlTakePath, token)
	if refused.status != 503 || !refused.body.Error.Retryable {
		t.Fatalf("stale binding take=%+v", refused)
	}
	control.mu.Lock()
	control.generation = 2
	control.mu.Unlock()
	if status := postComputerControl(t, server.URL, "/v1/seats/"+one+"/person"+computerControlTakePath, token); status != 200 {
		t.Fatalf("retry after refreshed binding=%d", status)
	}
	runtime.mu.Lock()
	runtime.generation = 3
	runtime.mu.Unlock()
	if status := postComputerControl(t, server.URL, "/v1/seats/"+one+"/person"+computerControlReleasePath, token); status != 200 {
		t.Fatalf("release after membership change=%d", status)
	}
	runtime.mu.Lock()
	driving := runtime.drivers[1]
	runtime.mu.Unlock()
	if driving {
		t.Fatal("release left driver true")
	}
	control.mu.Lock()
	control.generation = 3
	control.mu.Unlock()
	headers := http.Header{l1.SeatCredentialHeader: []string{two}}
	other, _, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(server.URL, "http")+"/v1/seats/"+two+"/screen/input", &websocket.DialOptions{Subprotocols: []string{"binary"}, HTTPHeader: headers})
	if err != nil {
		t.Fatalf("other Seat lost with refused take: %v", err)
	}
	other.CloseNow()
}

// These requests use the shared door and the real person takeover handler. A
// fatal callback cancels the whole Computer, just as the lifecycle owner does.
func TestSeatScreenActionRefusalsLeaveOtherSeatWorking(t *testing.T) {
	for _, scenario := range []string{"starting_view", "ended_release", "ended_release_helper_refusal", "ended_release_during_clear", "ended_take", "unavailable_view", "internal_view", "attempt_mismatch_view", "helper_notready_view", "helper_generation_view", "audit_unavailable"} {
		t.Run(scenario, func(t *testing.T) {
			fixture, cache, _, _, oldServer, identity := computerFrontDoorFixture(t, l1.ComputerGrantControl)
			oldServer.Close()
			if _, err := cache.Install(policySnapshot(t, fixture.clock.Now(), 1, 2, []l1.ComputerPolicyAdmin{{FabricID: identity.FabricID, UserID: identity.UserID}})); err != nil {
				t.Fatal(err)
			}
			runtime := &screenRuntimeFixture{view: newComputerBackend(t, computerBackendOptions{}), control: newComputerBackend(t, computerBackendOptions{rfbHandshake: true}), drivers: map[int]bool{}}
			defer runtime.view.Close()
			defer runtime.control.Close()
			control := &screenControlFixture{}
			ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
			defer cancel()
			fatal := make(chan error, 10)
			h, err := newSeatScreenHandler(seatScreenConfig{ctx: ctx, fabric: fixture.identity, authorizer: cache, clock: fixture.clock, runtime: runtime, control: control, computer: "computer-1", storage: "storage-1", storageGeneration: 1, authority: ocihelper.AttemptAuthority{NodeID: "node-1", BootSessionID: "boot-1", JobID: "job-1", AttemptID: "attempt-1", FencingToken: "fence-1"}, fatal: func(err error) { fatal <- err; cancel() }})
			if err != nil {
				t.Fatal(err)
			}
			door := newSeatFrontDoor(fixture.identity)
			registration, err := door.register(ctx, "computer-1", "attempt-1", h)
			if err != nil {
				t.Fatal(err)
			}
			defer registration.close()
			one, two := "seat_00000000000000000000000000000001", "seat_00000000000000000000000000000002"
			if err = registration.bind([]string{one, two}); err != nil {
				t.Fatal(err)
			}
			server := httptest.NewServer(door)
			defer server.Close()
			dial := func(id, path string, ai bool) (*websocket.Conn, *http.Response, error) {
				headers := http.Header{}
				if ai {
					headers.Set(l1.SeatCredentialHeader, id)
				}
				return websocket.Dial(ctx, "ws"+strings.TrimPrefix(server.URL, "http")+"/v1/seats/"+id+path, &websocket.DialOptions{Subprotocols: []string{"binary"}, HTTPHeader: headers})
			}
			ended := &ProtocolError{StatusCode: 409, APIError: contract.APIError{Code: "seat_ended", Message: "Seat has ended"}}
			if strings.Contains(scenario, "view") {
				failure := &ProtocolError{StatusCode: 409, APIError: contract.APIError{Code: contract.ErrorConflict, Message: "Seat is not ready"}}
				if scenario == "unavailable_view" {
					failure = &ProtocolError{StatusCode: 503, APIError: contract.APIError{Code: contract.ErrorUnavailable, Message: "L1 temporarily unavailable", Retryable: true}}
				}
				if scenario == "internal_view" {
					failure = &ProtocolError{StatusCode: 500, APIError: contract.APIError{Code: contract.ErrorInternal, Message: "L1 temporarily failed", Retryable: true}}
				}
				if scenario == "attempt_mismatch_view" {
					failure = &ProtocolError{StatusCode: 403, APIError: contract.APIError{Code: contract.ErrorForbidden, Message: "Seat screen Attempt mismatch"}}
				}
				if strings.HasPrefix(scenario, "helper_") {
					failure = &ProtocolError{StatusCode: 503, APIError: contract.APIError{Code: contract.ErrorUnavailable}}
					runtime.mu.Lock()
					runtime.hidePlaceOne = scenario == "helper_notready_view"
					runtime.changedObservation = scenario == "helper_generation_view"
					runtime.mu.Unlock()
				} else {
					control.mu.Lock()
					control.authorityFailure = failure
					control.mu.Unlock()
				}
				conn, reply, err := dial(one, "/person/websockify", false)
				if conn != nil {
					conn.CloseNow()
				}
				if err == nil {
					t.Fatal("person view unexpectedly accepted")
				}
				if reply == nil {
					t.Error("person view returned no typed HTTP refusal")
				} else {
					var body contract.ErrorResponse
					decodeErr := json.NewDecoder(reply.Body).Decode(&body)
					reply.Body.Close()
					if decodeErr != nil || body.Error.Code != failure.APIError.Code || reply.StatusCode != failure.StatusCode {
						t.Errorf("typed view refusal: status=%d body=%+v error=%v", reply.StatusCode, body, decodeErr)
					}
				}
			} else {
				person, response, err := dial(one, "/person/websockify", false)
				if err != nil {
					t.Fatal(err)
				}
				defer person.CloseNow()
				if _, _, err = person.Read(ctx); err != nil {
					t.Fatal(err)
				}
				token := response.Header.Get(computerControlTokenHeader)
				if strings.HasPrefix(scenario, "ended_release") {
					if status := postComputerControl(t, server.URL, "/v1/seats/"+one+"/person"+computerControlTakePath, token); status != 200 {
						t.Fatalf("take=%d", status)
					}
					control.mu.Lock()
					control.authorityFailure = ended
					if scenario == "ended_release_during_clear" {
						control.authoritySuccessesBeforeFailure = 1
					}
					control.mu.Unlock()
					if scenario == "ended_release_helper_refusal" {
						runtime.mu.Lock()
						runtime.generation = 2
						runtime.mu.Unlock()
						control.mu.Lock()
						control.generation = 2
						control.mu.Unlock()
					}
					if status := postComputerControl(t, server.URL, "/v1/seats/"+one+"/person"+computerControlReleasePath, token); status != 200 {
						t.Errorf("ended Seat release=%d", status)
					}
					if scenario != "ended_release_helper_refusal" {
						runtime.mu.Lock()
						driving := runtime.drivers[1]
						runtime.mu.Unlock()
						if driving {
							t.Error("ended Seat driver signal was not cleared")
						}
					}
				} else {
					control.mu.Lock()
					if scenario == "audit_unavailable" {
						control.takeFailure = &ProtocolError{StatusCode: 503, APIError: contract.APIError{Code: contract.ErrorUnavailable}}
					} else {
						control.takeFailure = ended
					}
					control.mu.Unlock()
					failure := postComputerControlFailure(t, server.URL, "/v1/seats/"+one+"/person"+computerControlTakePath, token)
					if scenario == "audit_unavailable" {
						select {
						case <-fatal:
						case <-ctx.Done():
							select {
							case <-fatal:
							default:
								t.Fatal("exhausted control audit did not end Computer")
							}
						}
						return
					}
					if failure.body.Error.Code != "seat_ended" {
						t.Errorf("take refusal=%+v", failure)
					}
				}
			}
			// A complete handshake and input round trip prove Seat 2 still
			// works after Seat 1's action. Cancellation from fatal breaks it.
			other, _, err := dial(two, "/screen/input", true)
			if err != nil {
				t.Errorf("Seat 2 input lost: %v", err)
			} else {
				if _, _, err = other.Read(ctx); err != nil {
					t.Errorf("Seat 2 backend lost: %v", err)
				}
				completeInitialRFBHandshake(t, other)
				assertRelayRoundTrip(t, other, "Seat 2 input", "Seat 2 input")
				other.CloseNow()
			}
			select {
			case err := <-fatal:
				t.Errorf("Seat refusal ended Computer: %v", err)
			default:
			}
		})
	}
}

type seatScreenReviewFixture struct {
	ctx         context.Context
	clock       *manualClock
	control     *screenControlFixture
	runtime     *screenRuntimeFixture
	handler     *seatScreenHandler
	server      *httptest.Server
	fatal, logs chan error
}

func newSeatScreenReviewFixture(t *testing.T) *seatScreenReviewFixture {
	t.Helper()
	fixture, cache, _, _, oldServer, identity := computerFrontDoorFixture(t, l1.ComputerGrantControl)
	oldServer.Close()
	if _, err := cache.Install(policySnapshot(t, fixture.clock.Now(), 1, 2, []l1.ComputerPolicyAdmin{{FabricID: identity.FabricID, UserID: identity.UserID}})); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	f := &seatScreenReviewFixture{ctx: ctx, clock: fixture.clock, control: &screenControlFixture{}, runtime: &screenRuntimeFixture{view: newComputerBackend(t, computerBackendOptions{}), control: newComputerBackend(t, computerBackendOptions{rfbHandshake: true}), drivers: map[int]bool{}}, fatal: make(chan error, 20), logs: make(chan error, 20)}
	var err error
	f.handler, err = newSeatScreenHandler(seatScreenConfig{ctx: ctx, fabric: fixture.identity, authorizer: cache, clock: fixture.clock, runtime: f.runtime, control: f.control, computer: "computer-1", storage: "storage-1", storageGeneration: 1, authority: ocihelper.AttemptAuthority{NodeID: "node-1", BootSessionID: "boot-1", JobID: "job-1", AttemptID: "attempt-1", FencingToken: "fence-1"}, fatal: func(err error) { f.fatal <- err; cancel() }, logFailure: func(err error) { f.logs <- err }})
	if err != nil {
		t.Fatal(err)
	}
	door := newSeatFrontDoor(fixture.identity)
	registration, err := door.register(ctx, "computer-1", "attempt-1", f.handler)
	if err != nil {
		t.Fatal(err)
	}
	if err = registration.bind([]string{"seat_00000000000000000000000000000001", "seat_00000000000000000000000000000002"}); err != nil {
		t.Fatal(err)
	}
	f.server = httptest.NewServer(door)
	t.Cleanup(func() {
		cancel()
		registration.close()
		f.server.Close()
		f.runtime.view.Close()
		f.runtime.control.Close()
	})
	return f
}

func (f *seatScreenReviewFixture) open(t *testing.T, id, path string, ai bool) (*websocket.Conn, *http.Response) {
	t.Helper()
	headers := http.Header{}
	if ai {
		headers.Set(l1.SeatCredentialHeader, id)
	}
	conn, reply, err := websocket.Dial(f.ctx, "ws"+strings.TrimPrefix(f.server.URL, "http")+"/v1/seats/"+id+path, &websocket.DialOptions{Subprotocols: []string{"binary"}, HTTPHeader: headers})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.CloseNow() })
	return conn, reply
}

func TestSeatScreenSessionAuditFailureIsLoggedAndClassified(t *testing.T) {
	for _, status := range []int{503, 409} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			f := newSeatScreenReviewFixture(t)
			id := "seat_00000000000000000000000000000001"
			f.control.mu.Lock()
			f.control.sessionFailure = &ProtocolError{StatusCode: status, APIError: contract.APIError{Code: contract.ErrorUnavailable, Message: "session audit refused"}}
			f.control.sessionFailureKind = l1.ComputerTakeoverSessionClose
			f.control.sessionAttempts = make(chan struct{}, computerAuditAttempts)
			f.control.mu.Unlock()
			person, _ := f.open(t, id, "/person/websockify", false)
			if _, _, err := person.Read(f.ctx); err != nil {
				t.Fatal(err)
			}
			person.CloseNow()
			for range computerAuditAttempts {
				select {
				case <-f.control.sessionAttempts:
				case <-time.After(5 * time.Second):
					t.Fatal("session close audit did not exhaust its retries")
				}
			}
			select {
			case err := <-f.logs:
				if !strings.Contains(err.Error(), "computer-1") || !strings.Contains(err.Error(), id) || !strings.Contains(err.Error(), "record Computer session close") {
					t.Errorf("audit report lacks Computer, Seat or cause: %v", err)
				}
			case <-time.After(5 * time.Second):
				t.Error("exhausted session close audit was silently discarded")
			}
			if status == 503 {
				select {
				case <-f.fatal:
				case <-time.After(time.Second):
					t.Error("exhausted session close audit did not call fatal")
				}
				return
			}
			// A later report is a processing barrier for the preceding 4xx.
			// Queue two reports to verify they are logged individually, too.
			seat, err := f.handler.seat(f.ctx, id)
			if err != nil {
				t.Fatal(err)
			}
			seat.front.report(errors.New("first audit barrier"))
			seat.front.report(errors.New("second audit barrier"))
			for range 2 {
				select {
				case <-f.logs:
				case <-time.After(5 * time.Second):
					t.Fatal("front-door reports were not logged individually")
				}
			}
			select {
			case err := <-f.fatal:
				t.Fatalf("4xx session audit refusal ended Computer: %v", err)
			default:
			}
			other, _ := f.open(t, "seat_00000000000000000000000000000002", "/screen/input", true)
			if _, _, err = other.Read(f.ctx); err != nil {
				t.Fatal(err)
			}
			completeInitialRFBHandshake(t, other)
			assertRelayRoundTrip(t, other, "Seat 2 input", "Seat 2 input")
		})
	}
}

func TestSeatScreenReleaseRetriesLiveClearBeforeWithdrawing(t *testing.T) {
	for _, persistent := range []bool{false, true} {
		t.Run(fmt.Sprint("persistent=", persistent), func(t *testing.T) {
			f := newSeatScreenReviewFixture(t)
			clock := &seatLeaseTestClock{manualClock: f.clock, scheduled: make(chan time.Duration, 100)}
			f.handler.mu.Lock()
			f.handler.config.clock = clock
			f.handler.mu.Unlock()
			id := "seat_00000000000000000000000000000001"
			person, reply := f.open(t, id, "/person/websockify", false)
			if _, _, err := person.Read(f.ctx); err != nil {
				t.Fatal(err)
			}
			token := reply.Header.Get(computerControlTokenHeader)
			if status := postComputerControl(t, f.server.URL, "/v1/seats/"+id+"/person"+computerControlTakePath, token); status != 200 {
				t.Fatalf("take=%d", status)
			}
			f.control.mu.Lock()
			f.control.authorityFailure = &ProtocolError{StatusCode: 409, APIError: contract.APIError{Code: contract.ErrorConflict, Message: "Seat is not ready"}}
			if !persistent {
				f.control.authorityFailuresRemaining = 1
			}
			f.control.mu.Unlock()
			type result struct {
				status int
				err    error
			}
			done := make(chan result, 1)
			start := clock.Now()
			go func() {
				r, _ := http.NewRequestWithContext(f.ctx, http.MethodPost, f.server.URL+"/v1/seats/"+id+"/person"+computerControlReleasePath, nil)
				r.Header.Set(computerControlTokenHeader, token)
				response, err := http.DefaultClient.Do(r)
				if err != nil {
					done <- result{err: err}
					return
				}
				response.Body.Close()
				done <- result{status: response.StatusCode}
			}()
			for {
				select {
				case failure := <-f.fatal:
					if !persistent {
						t.Fatalf("one clear refusal ended Computer: %v", failure)
					}
					if elapsed := clock.Now().Sub(start); elapsed != 10*time.Second {
						t.Fatalf("clear withdrew before retry bound: %s %v", elapsed, failure)
					}
					return
				case outcome := <-done:
					if persistent {
						select {
						case <-f.fatal:
						default:
							t.Fatal("unconfirmed clear did not withdraw Computer")
						}
						if elapsed := clock.Now().Sub(start); elapsed != 10*time.Second {
							t.Fatalf("clear bound=%s", elapsed)
						}
						return
					}
					if outcome.err != nil || outcome.status != 200 {
						t.Fatalf("release after one refusal=%+v", outcome)
					}
					f.runtime.mu.Lock()
					driving := f.runtime.drivers[1]
					f.runtime.mu.Unlock()
					if driving {
						t.Fatal("successful retry left the driver signal true")
					}
					select {
					case failure := <-f.fatal:
						t.Fatalf("confirmed clear ended Computer: %v", failure)
					default:
					}
					other, _ := f.open(t, "seat_00000000000000000000000000000002", "/screen/input", true)
					if _, _, err := other.Read(f.ctx); err != nil {
						t.Fatal(err)
					}
					completeInitialRFBHandshake(t, other)
					assertRelayRoundTrip(t, other, "Seat 2 input", "Seat 2 input")
					return
				case delay := <-clock.scheduled:
					// Only retry-sized timers advance this test. The bound and
					// existing session/policy timers share the injected clock.
					if delay > 0 && delay <= time.Second {
						if remaining := 10*time.Second - clock.Now().Sub(start); delay > remaining {
							t.Fatalf("retry scheduled beyond the clear bound: %s", delay)
						}
						clock.Advance(delay)
					}
				case <-time.After(5 * time.Second):
					t.Fatal("clear did not retry or finish")
				}
			}
		})
	}
}

func TestSeatScreenOpenUsesFencedEndpointLookup(t *testing.T) {
	runtime := &screenRuntimeFixture{view: newComputerBackend(t, computerBackendOptions{}), control: newComputerBackend(t, computerBackendOptions{})}
	defer runtime.view.Close()
	defer runtime.control.Close()
	h := &seatScreenHandler{config: seatScreenConfig{runtime: runtime}}
	conn, err := h.dial(t.Context(), l1.SeatScreenAuthority{SeatCopyAuthority: l1.SeatCopyAuthority{Place: 1, Generation: 1}}, "view")
	if err != nil {
		t.Fatal(err)
	}
	conn.Close()
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	if !runtime.endpointLookup {
		t.Fatal("screen open repeated all Seats' readiness probes instead of looking up the fenced endpoint")
	}
}
