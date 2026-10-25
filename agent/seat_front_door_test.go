package agent

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/fabric/plain"
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
		door.ServeHTTP(w, httptest.NewRequest("POST", tc.path, nil))
		if w.Code != tc.want {
			t.Fatalf("%s: %d", tc.path, w.Code)
		}
	}
	cancel()
	w := httptest.NewRecorder()
	door.ServeHTTP(w, httptest.NewRequest("POST", "/v1/seats/"+id+"/copy", nil))
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
	door.ServeHTTP(w, httptest.NewRequest("POST", "/v1/seats/"+id+"/copy", nil))
	if w.Code != 202 {
		t.Fatal("old cleanup removed replacement")
	}
}

func TestSeatFrontDoorPublicationPrecedesReadinessAndWithdraws(t *testing.T) {
	for _, stale := range []bool{false, true} {
		t.Run(map[bool]string{false: "live", true: "stale"}[stale], func(t *testing.T) {
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
						return errors.New("stale Attempt")
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
	transport := &http.Transport{DialContext: func(ctx context.Context, network, _ string) (net.Conn, error) {
		return clientFabric.Dial(ctx, network, "wefty://node/node")
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
			response, err := client.Get("http://node/v1/seats/" + id + "/" + tc.suffix)
			if err != nil {
				t.Fatal(err)
			}
			response.Body.Close()
			if response.StatusCode != tc.status {
				t.Fatalf("%s: %d", tc.suffix, response.StatusCode)
			}
		}
	}
	if len(serverFabric.addresses) != 1 || serverFabric.addresses[0] != "wefty://node/node" {
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
