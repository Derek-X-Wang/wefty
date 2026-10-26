package ocihelper

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
	"testing/synctest"
	"time"
)

func TestSeatEndpointNamesBindPlaceAndIncarnation(t *testing.T) {
	s := newComputerSeats(nil, t.TempDir())
	if _, err := s.apply([]ComputerSeat{{1, "seat1", 7102, 7103}, {2, "seat2", 7104, 7105}}); err != nil {
		t.Fatal(err)
	}
	// This is the endpoint name returned for the first Seat, never a raw port.
	name := s.entries[1].endpointName("view")
	s.entries[1].ready, s.entries[1].readyGeneration = true, 1
	s.entries[2].ready, s.entries[2].readyGeneration = true, 1
	if _, err := s.endpoint(DialSeatEndpointRequest{Generation: 1, Place: 1, Name: name}); err != nil {
		t.Fatal(err)
	}
	for _, req := range []DialSeatEndpointRequest{
		{Generation: 1, Place: 2, Name: name}, {Generation: 2, Place: 1, Name: name},
		{Generation: 1, Place: 1, Name: "7102"}, {Generation: 1, Place: 1, Name: "../2/view"},
	} {
		if _, err := s.endpoint(req); err == nil {
			t.Fatalf("accepted mismatched endpoint: %+v", req)
		}
	}
	if _, err := s.apply(nil); err != nil {
		t.Fatal(err)
	}
	if _, err := s.endpoint(DialSeatEndpointRequest{Generation: 2, Place: 1, Name: name}); err == nil {
		t.Fatal("ended Seat accepted")
	}
	s.entries[1].cleaned = true
	if _, err := s.apply([]ComputerSeat{{1, "seat1", 7102, 7103}}); err != nil {
		t.Fatal(err)
	}
	s.entries[1].ready, s.entries[1].readyGeneration = true, 3
	_, err := s.endpoint(DialSeatEndpointRequest{Generation: 3, Place: 1, Name: name})
	var refusal *SeatOperationRefusalError
	if !errors.As(err, &refusal) {
		t.Fatalf("old incarnation accepted: %v", err)
	}
	if _, err := s.endpoint(DialSeatEndpointRequest{Generation: 3, Place: 1, Name: s.entries[1].endpointName("view")}); err != nil {
		t.Fatal(err)
	}
}

type seatEndpointProtocolEngine struct {
	*seatProtocolEngine
	dials int
}

func (e *seatEndpointProtocolEngine) DialSeatEndpoint(ctx context.Context, r DialSeatEndpointRequest, stream io.ReadWriteCloser) error {
	if _, err := e.seats.endpoint(r); err != nil {
		return err
	}
	e.dials++
	if _, err := stream.Write([]byte{attemptPortBackendReady}); err != nil {
		return err
	}
	_, err := io.Copy(stream, stream)
	return err
}
func TestSeatEndpointRPCRejectsOldAttemptAndOtherSeatBeforeStreaming(t *testing.T) {
	e := &seatEndpointProtocolEngine{seatProtocolEngine: &seatProtocolEngine{newFakeEngine(), newComputerSeats(nil, t.TempDir())}}
	client, stop := startTestServer(t, e, ServerConfig{})
	defer stop()
	session, err := client.OpenSession(t.Context(), testSessionRequest())
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	requireSweep(t, session)
	a := testAuthority()
	a.Class = "service"
	r := testRunRequest(a, time.Minute)
	r.Workload.Computer = true
	r.EnableHostBridgeFallback = true
	r.ActivateHostBridgeFallback = true
	r.AllocateEndpoints = []string{"view", "control"}
	r.Workload.Limits.MemoryBytes = 1 << 30
	r.Workload.ManagedVolumes = []ManagedVolumeDescriptor{{Kind: ManagedVolumeComputerDisk, ComputerStorage: &ComputerStorageReference{ComputerID: "computer", StorageID: "storage", StorageGeneration: 1, IntentRevision: 1, DiskBytes: 1 << 30}}}
	e.runResponse.Endpoints = map[string]uint16{"view": 42011, "control": 42012}
	if _, err := session.Run(t.Context(), r); err != nil {
		t.Fatal(err)
	}
	if _, err := session.ApplyComputerSeats(t.Context(), ApplyComputerSeatsRequest{a, []ComputerSeat{{1, "seat1", 7102, 7103}, {2, "seat2", 7104, 7105}}}); err != nil {
		t.Fatal(err)
	}
	e.seats.entries[1].ready, e.seats.entries[1].readyGeneration = true, 1
	e.seats.entries[2].ready, e.seats.entries[2].readyGeneration = true, 1
	request := DialSeatEndpointRequest{Authority: a, Generation: 1, Place: 1, Name: e.seats.entries[1].endpointName("view")}
	wrong := request
	wrong.Authority.FencingToken = "old"
	if c, err := session.DialSeatEndpoint(t.Context(), wrong); err == nil {
		c.Close()
		t.Fatal("old attempt reached endpoint")
	}
	wrong = request
	wrong.Place = 2
	if c, err := session.DialSeatEndpoint(t.Context(), wrong); err == nil {
		c.Close()
		t.Fatal("other Seat reached endpoint")
	}
	if e.dials != 0 {
		t.Fatal("refusal connected backend")
	}
	setup, cancel := context.WithCancel(t.Context())
	c, err := session.DialSeatEndpoint(setup, request)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	cancel()
	c.SetDeadline(time.Now().Add(time.Second))
	if _, err := c.Write([]byte("own Seat")); err != nil {
		t.Fatal(err)
	}
	b := make([]byte, 8)
	if _, err := io.ReadFull(c, b); err != nil || string(b) != "own Seat" {
		t.Fatalf("stream: %q %v", b, err)
	}
	if session.HealthError() != nil {
		t.Fatalf("refusal lost session: %v", session.HealthError())
	}
	if !dataStreamMethod(MethodDialSeatEndpoint) {
		t.Fatal("Seat streams consumed control reserve")
	}
}

func TestSeatDirectivePublishesOnlyFixedPrivateSocketPaths(t *testing.T) {
	dir := t.TempDir()
	s := newComputerSeats(nil, dir)
	if _, err := s.apply([]ComputerSeat{{1, "seat1", 7102, 7103}}); err != nil {
		t.Fatal(err)
	}
	payload, err := os.ReadFile(filepath.Join(dir, "seats.json"))
	if err != nil {
		t.Fatal(err)
	}
	var doc struct {
		Seats []struct {
			ViewSocket    string `json:"view_socket"`
			ControlSocket string `json:"control_socket"`
		}
	}
	if err := json.Unmarshal(payload, &doc); err != nil {
		t.Fatal(err)
	}
	if len(doc.Seats) != 1 || doc.Seats[0].ViewSocket != "/wefty/service/seats/1/run/view.ws" || doc.Seats[0].ControlSocket != "/wefty/service/seats/1/run/control.ws" {
		t.Fatalf("missing confined paths: %s", payload)
	}
}

func TestSeatEndpointFromNeighbourComputerIsRefused(t *testing.T) {
	first, second := newComputerSeats(nil, t.TempDir()), newComputerSeats(nil, t.TempDir())
	for _, s := range []*computerSeats{first, second} {
		if _, err := s.apply([]ComputerSeat{{1, "seat1", 7102, 7103}}); err != nil {
			t.Fatal(err)
		}
		s.entries[1].ready, s.entries[1].readyGeneration = true, 1
	}
	name := first.entries[1].endpointName("view")
	if _, err := second.endpoint(DialSeatEndpointRequest{Generation: 1, Place: 1, Name: name}); err == nil {
		t.Fatal("neighbour Computer endpoint accepted")
	}
}

func TestSeatEndpointPinsCloseBeforeDiskDetachEvenWhenDetachFails(t *testing.T) {
	s := newComputerSeats(nil, t.TempDir())
	var pins []*os.File
	for _, place := range []int{1, 2} {
		entry := &computerSeatEntry{endpointPins: map[string]*os.File{}, pinOwner: s}
		s.entries[place] = entry
		for _, kind := range []string{"view", "control"} {
			pin, err := s.openEndpointPin(func() (*os.File, error) { return os.Open(t.TempDir()) })
			if err != nil {
				t.Fatal(err)
			}
			defer pin.Close()
			pins = append(pins, pin)
			entry.endpointPins[kind] = pin
		}
	}
	detachFailure := errors.New("detach failed after descriptor release")
	called := false
	err := s.beforeDiskDetach(func() error {
		called = true
		for _, pin := range pins {
			if _, err := pin.Stat(); !errors.Is(err, os.ErrClosed) {
				t.Errorf("socket descriptor still open when detach begins: %v", err)
			}
		}
		return detachFailure
	})
	if !called || !errors.Is(err, detachFailure) {
		t.Fatalf("detach outcome lost: called=%v err=%v", called, err)
	}
	for _, entry := range s.entries {
		for _, pin := range entry.endpointPins {
			if _, err := pin.Stat(); !errors.Is(err, os.ErrClosed) {
				t.Errorf("published pin remains usable: %v", err)
			}
		}
	}
	// A failed detach can be retried; no descriptors may be reopened by retry.
	if err := s.beforeDiskDetach(func() error { return nil }); err != nil {
		t.Fatal(err)
	}
}

// A greeting can stall before an opened endpoint becomes a ready Seat pin.
// Detach must release that filesystem reference too, and a late probe must
// never reopen the retired Computer's filesystem.
func TestSeatEndpointRetirementClosesProvisionalPinsAndRefusesLateOpen(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := newComputerSeats(nil, t.TempDir())
		open := func() (*os.File, error) { return os.Open(t.TempDir()) }
		provisional, err := s.openEndpointPin(open)
		if err != nil {
			t.Fatal(err)
		}
		defer provisional.Close()
		probeEntered, finishProbe := make(chan struct{}), make(chan struct{})
		go func() { close(probeEntered); <-finishProbe; s.closeEndpointPin(provisional) }()
		<-probeEntered
		detached := make(chan error, 1)
		go func() {
			detached <- s.beforeDiskDetach(func() error {
				if _, err := provisional.Stat(); !errors.Is(err, os.ErrClosed) {
					t.Errorf("in-flight probe retained a filesystem reference at detach: %v", err)
				}
				return nil
			})
		}()
		synctest.Wait()
		select {
		case err := <-detached:
			if err != nil {
				t.Error(err)
			}
		default:
			t.Error("disk detach waited behind an in-flight probe")
		}
		opened := false
		late, err := s.openEndpointPin(func() (*os.File, error) { opened = true; return open() })
		if late != nil {
			late.Close()
		}
		if err == nil || opened {
			t.Errorf("retired attempt reopened an endpoint: opened=%v err=%v", opened, err)
		}
		close(finishProbe)
	})
}

func TestSeatEndpointLookupRequiresPublishedCurrentGeneration(t *testing.T) {
	s := newComputerSeats(nil, "")
	s.generation = 2
	view, err := os.CreateTemp(t.TempDir(), "view")
	if err != nil {
		t.Fatal(err)
	}
	defer view.Close()
	control, err := os.CreateTemp(t.TempDir(), "control")
	if err != nil {
		t.Fatal(err)
	}
	defer control.Close()
	s.entries[1] = &computerSeatEntry{wanted: true, ready: true, readyGeneration: 2, config: ComputerSeat{Place: 1}, endpointScope: "scope", incarnation: 1, endpointPins: map[string]*os.File{"view": view, "control": control}}
	for _, tc := range []struct {
		name       string
		generation uint64
		ready      bool
		wantReady  bool
		wantError  bool
	}{
		{"published", 2, true, true, false}, {"unpublished", 2, false, false, false}, {"stale", 1, true, false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s.entries[1].ready = tc.ready
			got, err := s.lookupEndpoints(tc.generation)
			if (err != nil) != tc.wantError {
				t.Fatalf("lookup error = %v", err)
			}
			if tc.wantError {
				return
			}
			if len(got.Seats) != 1 || (got.Seats[0].State == "ready") != tc.wantReady {
				t.Fatalf("lookup = %+v", got)
			}
			if tc.wantReady && got.Seats[0].Endpoints["control"] != s.entries[1].endpointName("control") {
				t.Fatalf("missing fenced endpoint: %+v", got)
			}
			if !tc.wantReady && len(got.Seats[0].Endpoints) != 0 {
				t.Fatal("unready endpoint published")
			}
		})
	}
}
