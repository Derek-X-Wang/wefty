package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/fabric/plain"
	"github.com/Derek-X-Wang/wefty/l1"
	workloadrunner "github.com/Derek-X-Wang/wefty/runner"
	"github.com/Derek-X-Wang/wefty/runner/ocihelper"
)

func TestSeatFrontDoorRoutesRegisteredFeaturesAndClosesExactAttempt(t *testing.T) {
	door := newSeatFrontDoor(nil)
	id := "seat_" + strings.Repeat("a", 32)
	ctx, cancel := context.WithCancel(t.Context())
	registration, err := door.register(ctx, "computer", "attempt", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(204) }))
	if err != nil {
		t.Fatal(err)
	}
	if err := registration.bind([]string{id}); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		path string
		want int
	}{{"/v1/seats/" + id + "/future-feature", 204}, {"/unknown", 404}, {"/v1/seats/seat_" + strings.Repeat("b", 32) + "/copy", 404}} {
		w := httptest.NewRecorder()
		req := httptest.NewRequest("POST", tc.path, nil)
		req.Header.Set(l1.SeatCredentialHeader, "secret")
		door.ServeHTTP(w, req)
		if w.Code != tc.want {
			t.Fatalf("%s: %d", tc.path, w.Code)
		}
	}
	cancel()
	w := httptest.NewRecorder()
	req := httptest.NewRequest("POST", "/v1/seats/"+id+"/copy", nil)
	req.Header.Set(l1.SeatCredentialHeader, "secret")
	door.ServeHTTP(w, req)
	if w.Code != 404 {
		t.Fatal("cancelled Attempt still served")
	}
	next, err := door.register(t.Context(), "computer", "next", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(202) }))
	if err != nil {
		t.Fatal(err)
	}
	defer next.close()
	if err := next.bind([]string{id}); err != nil {
		t.Fatal(err)
	}
	registration.close()
	w = httptest.NewRecorder()
	req = httptest.NewRequest("POST", "/v1/seats/"+id+"/copy", nil)
	req.Header.Set(l1.SeatCredentialHeader, "secret")
	door.ServeHTTP(w, req)
	if w.Code != 202 {
		t.Fatal("old cleanup removed replacement")
	}
}

func TestSeatFrontDoorPublicationPrecedesReadinessAndWithdraws(t *testing.T) {
	for _, stale := range []bool{false, true} {
		t.Run(map[bool]string{false: "live", true: "fencing-refusal"}[stale], func(t *testing.T) {
			c, api, helper := newSeatTestController()
			published, withdrawn := false, false
			observed := make(chan struct{}, 1)
			c.afterStep = func() { observed <- struct{}{} }
			door := newSeatFrontDoor(nil)
			control := &copyControlFixture{}
			runtime := &seatLifecycleRuntime{seatTestRuntime: helper}
			runtime.run = func(ctx context.Context, r workloadrunner.Request) (workloadrunner.Result, error) {
				r.Started()
				if stale {
					<-ctx.Done()
				} else {
					awaitSeatTestSignal(t, observed)
					if !published {
						t.Error("ready before publication")
					}
					response := httptest.NewRecorder()
					request := httptest.NewRequest("POST", "/v1/seats/"+api.list.Seats[0].SeatID+"/copy?direction=in&path=file", nil)
					request.Header.Set("X-Wefty-Seat-Credential", "secret")
					door.ServeHTTP(response, request)
					if len(control.receipts) != 2 || !bytes.Contains(response.Body.Bytes(), []byte(`"size_bytes":7`)) {
						t.Errorf("copy handler not attached to registered Seat: %s", response.Body.String())
					}
				}
				return workloadrunner.Result{Outcome: contract.ProcessResult{}}, nil
			}
			copyRuntime := copyRuntimeFunc(func(context.Context, ocihelper.CopyComputerSeatRequest, io.Reader, func([]byte) error) (int64, error) {
				return 7, nil
			})
			_, err := runComputerSeats(t.Context(), runtime, workloadrunner.Request{}, nil, c, seatComputerServiceConfig{router: door, copyRuntime: copyRuntime, copyControl: control, publish: func(ctx context.Context, ready bool, endpoint string) error {
				if ready {
					if stale {
						return &ProtocolError{StatusCode: 409, APIError: contract.APIError{Code: contract.ErrorStaleFence}}
					}
					published = true
				} else {
					withdrawn = true
				}
				return nil
			}})
			if stale && (err == nil || len(api.reports) != 0 || published) {
				t.Fatalf("stale Attempt published: %v", err)
			}
			if !stale && (err != nil || !published || len(api.reports) != 1) {
				t.Fatalf("ready/publication: %v", err)
			}
			if !withdrawn {
				t.Fatal("publication not withdrawn")
			}
			response := httptest.NewRecorder()
			door.ServeHTTP(response, httptest.NewRequest("POST", "/v1/seats/"+api.list.Seats[0].SeatID+"/copy", nil))
			if response.Code != 404 {
				t.Fatal("cleanup left a Seat route")
			}
		})
	}
}

type seatListenerRecorder struct {
	fabric.Fabric
	addresses []string
}

func (f *seatListenerRecorder) Listen(network, address string) (net.Listener, error) {
	f.addresses = append(f.addresses, address)
	return f.Fabric.Listen(network, address)
}

func TestSeatFrontDoorOneFabricListenerForMultipleComputers(t *testing.T) {
	network := plain.NewNetwork()
	serverFabric := &seatListenerRecorder{Fabric: network.NewFabric(fabric.Identity{NodeID: "node"})}
	door, closeDoor, err := startSeatFrontDoor(t.Context(), serverFabric, "node")
	if err != nil {
		t.Fatal(err)
	}
	defer closeDoor()
	clientFabric := network.NewFabric(fabric.Identity{NodeID: "client"})
	endpoint, err := url.Parse(door.endpoint)
	if err != nil {
		t.Fatal(err)
	}
	transport := &http.Transport{DialContext: func(ctx context.Context, network, _ string) (net.Conn, error) {
		return clientFabric.Dial(ctx, network, endpoint.Host)
	}}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport}
	for _, digit := range []string{"a", "b"} {
		id := "seat_" + strings.Repeat(digit, 32)
		mux := http.NewServeMux()
		mux.HandleFunc("GET /v1/seats/{seat_id}/future-feature", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(204) })
		r, err := door.register(t.Context(), "computer-"+digit, "attempt-"+digit, mux)
		if err != nil {
			t.Fatal(err)
		}
		defer r.close()
		if err := r.bind([]string{id}); err != nil {
			t.Fatal(err)
		}
		for _, tc := range []struct {
			suffix string
			status int
		}{{"future-feature", 204}, {"unknown", 404}} {
			req, _ := http.NewRequest("GET", "http://node/v1/seats/"+id+"/"+tc.suffix, nil)
			req.Header.Set(l1.SeatCredentialHeader, "secret")
			response, err := client.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			response.Body.Close()
			if response.StatusCode != tc.status {
				t.Fatalf("%s: %d", tc.suffix, response.StatusCode)
			}
		}
	}
	if len(serverFabric.addresses) != 1 || !strings.HasPrefix(serverFabric.addresses[0], "wefty://node/node-seat-") {
		t.Fatalf("listeners=%v", serverFabric.addresses)
	}
}

type seatNamedFabric struct{ fabric.Fabric }

func (f seatNamedFabric) LocalServiceAddress() string { return "wefty://node/enrolled-name" }
func (f seatNamedFabric) Listen(network, address string) (net.Listener, error) {
	if address != f.LocalServiceAddress() {
		return nil, errors.New("cannot listen as a different participant")
	}
	return f.Fabric.Listen(network, address)
}

func TestSeatFrontDoorRespectsFabricServiceAddress(t *testing.T) {
	f := seatNamedFabric{plain.NewNetwork().NewFabric(fabric.Identity{NodeID: "fabric-identity"})}
	_, closeDoor, err := startSeatFrontDoor(t.Context(), f, "different-wefty-node-id")
	if err != nil {
		t.Fatal(err)
	}
	closeDoor()
}

func TestSeatFrontDoorDoesNotPublishBeforeRuntimeStarts(t *testing.T) {
	c, _, helper := newSeatTestController()
	runtime := &seatLifecycleRuntime{seatTestRuntime: helper, run: func(context.Context, workloadrunner.Request) (workloadrunner.Result, error) {
		return workloadrunner.Result{}, nil
	}}
	calls := 0
	_, err := runComputerSeats(t.Context(), runtime, workloadrunner.Request{}, nil, c, seatComputerServiceConfig{router: newSeatFrontDoor(nil), copyRuntime: copyRuntimeFixture{}, copyControl: &copyControlFixture{}, publish: func(context.Context, bool, string) error { calls++; return nil }})
	if err != nil || calls != 0 {
		t.Fatalf("publication before runtime start: calls=%d err=%v", calls, err)
	}
}

func TestSeatFrontDoorRequiresCredentialBeforeHandler(t *testing.T) {
	door := newSeatFrontDoor(nil)
	id := "seat_" + strings.Repeat("a", 32)
	called := false
	reg, err := door.register(t.Context(), "computer", "attempt", http.HandlerFunc(func(http.ResponseWriter, *http.Request) { called = true }))
	if err != nil {
		t.Fatal(err)
	}
	defer reg.close()
	if err := reg.bind([]string{id}); err != nil {
		t.Fatal(err)
	}
	w := httptest.NewRecorder()
	door.ServeHTTP(w, httptest.NewRequest("POST", "/v1/seats/"+id+"/copy", nil))
	var failure contract.ErrorResponse
	if err := json.Unmarshal(w.Body.Bytes(), &failure); err != nil {
		t.Fatal(err)
	}
	if w.Code != 401 || called || failure.Error.Code != contract.ErrorUnauthorized {
		t.Fatalf("unguarded door: %d %s called=%v", w.Code, w.Body.String(), called)
	}
}
func TestSeatFrontDoorNodeRefusesPublishedPort80(t *testing.T) {
	port := 80
	a := &Agent{fabric: plain.NewNetwork().NewFabric(fabric.Identity{NodeID: "node"})}
	listener, failure := a.reservePublishedPort(l1.Claim{Job: l1.Job{Spec: contract.JobSpec{Class: contract.JobClassService, PublishedPort: &port}}})
	if listener != nil {
		listener.Close()
	}
	if failure == nil || !strings.Contains(failure.Message, "reserved for wefty's Seat front door") {
		t.Fatalf("node accepted reserved port: %+v", failure)
	}
}

func TestAgentRunWithoutFabricLeavesSeatDoorUnavailable(t *testing.T) {
	clock := systemClock{}
	client, err := newClient(plain.NewNetwork().NewFabric(fabric.Identity{NodeID: "node"}), "wefty://control-plane", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	registration := contract.NodeRegistration{NodeID: "node", BootSessionID: "boot"}
	observer := newLifecycleObserver(clock)
	capabilities := newCapabilityState(nil, nil, clock, 0, nil)
	session := newAgentSession(client, registration, capabilities, time.Second, time.Second, clock, observer, nil, 1, 0)
	defer session.close()
	a := &Agent{session: session, registration: registration, clock: clock, observer: observer, capabilities: capabilities}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_ = a.Run(ctx)
	if a.seatDoor != nil {
		t.Fatal("node without Fabric acquired a Seat door")
	}
	if _, err := a.seatDoor.register(t.Context(), "computer", "attempt", http.NotFoundHandler()); err == nil {
		t.Fatal("missing door accepted registration")
	}
}

func TestSeatFrontDoorStaleRegistrationCannotPublishOrWithdrawReplacement(t *testing.T) {
	door := newSeatFrontDoor(nil)
	ctx, cancel := context.WithCancel(t.Context())
	old, err := door.register(ctx, "computer", "old", http.NotFoundHandler())
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	next, err := door.register(t.Context(), "computer", "next", http.NotFoundHandler())
	if err != nil {
		t.Fatal(err)
	}
	defer next.close()
	calls := 0
	ready := false
	publish := func(context.Context, bool, string) error { calls++; ready = true; return nil }
	if err := next.publish(t.Context(), true, "endpoint", publish); err != nil {
		t.Fatal(err)
	}
	for _, value := range []bool{true, false} {
		if err := old.publish(t.Context(), value, "endpoint", publish); !errors.Is(err, errStaleSeatPublication) {
			t.Fatalf("stale mutation: %v", err)
		}
	}
	old.close()
	if calls != 1 || !ready || door.computers["computer"] != next {
		t.Fatal("stale registration changed replacement")
	}
}

func TestSeatFrontDoorRetriesPublicationAndWithdrawal(t *testing.T) {
	c, _, helper := newSeatTestController()
	observed := make(chan struct{}, 1)
	c.afterStep = func() { observed <- struct{}{} }
	runtime := &seatLifecycleRuntime{seatTestRuntime: helper, run: func(ctx context.Context, r workloadrunner.Request) (workloadrunner.Result, error) {
		r.Started()
		awaitSeatTestSignal(t, observed)
		return workloadrunner.Result{}, nil
	}}
	calls := map[bool]int{}
	clock := &delayRecordingClock{}
	_, err := runComputerSeats(t.Context(), runtime, workloadrunner.Request{}, nil, c, seatComputerServiceConfig{
		router: newSeatFrontDoor(nil), copyRuntime: copyRuntimeFixture{}, copyControl: &copyControlFixture{}, clock: clock,
		publish: func(ctx context.Context, ready bool, endpoint string) error {
			calls[ready]++
			if calls[ready] == 1 {
				return &ProtocolError{StatusCode: 503, APIError: contract.APIError{Code: contract.ErrorUnavailable}}
			}
			return nil
		}})
	if err != nil || calls[true] != 2 || calls[false] != 2 {
		t.Fatalf("publication not recovered: %v calls=%v", err, calls)
	}
	delays := clock.recorded()
	if len(delays) != 2 || delays[0] != DefaultPublicationRetryInterval || delays[1] != DefaultPublicationRetryInterval {
		t.Fatalf("retry schedule=%v", delays)
	}
}

func TestSeatFrontDoorSeatRemovalCancelsInFlightCopy(t *testing.T) {
	door := newSeatFrontDoor(nil)
	id := "seat_" + strings.Repeat("a", 32)
	started, done := make(chan struct{}), make(chan struct{})
	reg, err := door.register(t.Context(), "computer", "attempt", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { close(started); <-r.Context().Done(); close(done) }))
	if err != nil {
		t.Fatal(err)
	}
	defer reg.close()
	if err := reg.bind([]string{id}); err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest("POST", "/v1/seats/"+id+"/copy", nil)
	req.Header.Set(l1.SeatCredentialHeader, "secret")
	returned := make(chan struct{})
	go func() { defer close(returned); door.ServeHTTP(httptest.NewRecorder(), req) }()
	awaitSeatTestSignal(t, started)
	// A repeat wanted list must preserve a running operation.
	if err := reg.bind([]string{id}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-done:
		t.Fatal("unchanged list cancelled copy")
	default:
	}
	if err := reg.bind(nil); err != nil {
		t.Fatal(err)
	}
	awaitSeatTestSignal(t, done)
	awaitSeatTestSignal(t, returned)
}

func TestSeatFrontDoorPlainGenerationsUsePublishedEndpoints(t *testing.T) {
	network := plain.NewNetwork()
	serverFabric := &seatListenerRecorder{Fabric: network.NewFabric(fabric.Identity{NodeID: "node"})}
	clientFabric := network.NewFabric(fabric.Identity{NodeID: "client"})
	for range 3 {
		door, closeDoor, err := startSeatFrontDoor(t.Context(), serverFabric, "same-node")
		if err != nil {
			t.Fatal(err)
		}
		defer closeDoor()
		endpoint, err := url.Parse(door.endpoint)
		if err != nil {
			t.Fatal(err)
		}
		transport := &http.Transport{DialContext: func(ctx context.Context, network, _ string) (net.Conn, error) {
			return clientFabric.Dial(ctx, network, endpoint.Host)
		}}
		defer transport.CloseIdleConnections()
		req, _ := http.NewRequestWithContext(t.Context(), "GET", "http://node/unknown", nil)
		response, err := (&http.Client{Transport: transport}).Do(req)
		if err != nil {
			t.Fatal(err)
		}
		response.Body.Close()
		if response.StatusCode != http.StatusNotFound {
			t.Fatalf("published endpoint status %d", response.StatusCode)
		}
	}
	if len(serverFabric.addresses) != 3 {
		t.Fatalf("listeners=%v", serverFabric.addresses)
	}
	for i, address := range serverFabric.addresses {
		for _, old := range serverFabric.addresses[:i] {
			if old == address {
				t.Fatalf("generations share %q", address)
			}
		}
	}
}

type seatDoorRefusingFabric struct{ fabric.Fabric }

func (f seatDoorRefusingFabric) Listen(string, string) (net.Listener, error) {
	return nil, errors.New("injected Seat listener failure")
}

func TestSeatFrontDoorListenFailureKeepsNodeSessionRunning(t *testing.T) {
	network := plain.NewNetwork()
	store, stop := startFailureServerWithPoliciesAndLease(t, network, nil, map[string]l1.NodePolicy{"door-unavailable": {Tags: []string{"linux"}, MaxOneshotSlots: 1}}, l1.DefaultLeaseDuration)
	defer stop()
	logs := make(chan string, 20)
	// macOS reaches t.TempDir through the /var symlink; New refuses it.
	managedRoot, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	node, err := New(Config{Fabric: seatDoorRefusingFabric{network.NewFabric(fabric.Identity{NodeID: "node-fabric", Tags: []string{l1.DefaultAgentPrincipalTag}})}, ControlPlaneAddress: "wefty://control-plane", NodeID: "door-unavailable", BootSessionID: "boot", Version: "test", Capabilities: map[string]bool{"kind:process": true}, ManagedRootDirectory: managedRoot, LogSpoolDirectory: t.TempDir(), HandoffRoot: t.TempDir(), Logf: func(format string, args ...any) {
		select {
		case logs <- fmt.Sprintf(format, args...):
		default:
		}
	}})
	if err != nil {
		t.Fatal(err)
	}
	defer node.Close()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() { defer close(done); done <- node.Run(ctx) }()
	defer func() {
		cancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("node did not stop")
		}
	}()
	select {
	case <-node.Registered():
	case err := <-done:
		t.Fatalf("Seat door stopped non-Seat registration: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("node did not register")
	}
	if node.currentSeatFrontDoor() != nil {
		t.Fatal("failed listener left a Seat door")
	}
	if _, err := store.GetNode(t.Context(), "door-unavailable"); err != nil {
		t.Fatal(err)
	}
	found := false
	for len(logs) > 0 {
		line := <-logs
		found = found || strings.Contains(line, "injected Seat listener failure")
	}
	if !found {
		t.Fatal("listener failure not logged")
	}
}

func TestSeatFrontDoorPublicationFailuresAreLoggedWithRateLimit(t *testing.T) {
	c, _, helper := newSeatTestController()
	clock := &seatLeaseTestClock{manualClock: newManualClock(time.Unix(0, 0)), scheduled: make(chan time.Duration, 100)}
	logs := make(chan string, 10)
	c.logf = func(format string, args ...any) { logs <- fmt.Sprintf(format, args...) }
	observed := make(chan struct{}, 1)
	c.afterStep = func() {
		select {
		case observed <- struct{}{}:
		default:
		}
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan struct{})
	var recovered atomic.Bool
	runtime := &seatLifecycleRuntime{seatTestRuntime: helper, run: func(ctx context.Context, r workloadrunner.Request) (workloadrunner.Result, error) {
		r.Started()
		select {
		case <-observed:
		case <-ctx.Done():
		}
		return workloadrunner.Result{}, nil
	}}
	result := make(chan error, 1)
	go func() {
		defer close(done)
		_, err := runComputerSeats(ctx, runtime, workloadrunner.Request{}, nil, c, seatComputerServiceConfig{
			router: newSeatFrontDoor(nil), copyRuntime: copyRuntimeFixture{}, copyControl: &copyControlFixture{}, clock: clock,
			publish: func(_ context.Context, ready bool, _ string) error {
				if !ready || recovered.Load() {
					return nil
				}
				return &ProtocolError{StatusCode: 503, APIError: contract.APIError{Code: contract.ErrorUnavailable, Message: "injected publication outage"}}
			},
		})
		result <- err
	}()
	defer func() { cancel(); awaitSeatTestSignal(t, done) }()
	for {
		d := awaitSeatLeaseSchedule(t, clock, done)
		// Scheduling happens after failure handling, so the log must already exist.
		if clock.Now().Equal(time.Unix(0, 0)) {
			select {
			case line := <-logs:
				if !strings.Contains(line, c.computerID) || !strings.Contains(line, "injected publication outage") {
					t.Fatalf("publication log=%q", line)
				}
			default:
				t.Fatal("first publication failure was silent")
			}
		} else if clock.Now().Sub(time.Unix(0, 0)) >= time.Minute {
			select {
			case line := <-logs:
				if !strings.Contains(line, c.computerID) {
					t.Fatalf("publication log=%q", line)
				}
			default:
				t.Fatal("ongoing publication outage was silent")
			}
			recovered.Store(true)
			clock.Advance(d)
			break
		} else {
			select {
			case line := <-logs:
				t.Fatalf("publication retry flooded logs: %s", line)
			default:
			}
		}
		clock.Advance(d)
	}
	awaitSeatTestSignal(t, done)
	if err := <-result; err != nil {
		t.Fatal(err)
	}
	select {
	case line := <-logs:
		t.Fatalf("unexpected extra log: %s", line)
	default:
	}
}
