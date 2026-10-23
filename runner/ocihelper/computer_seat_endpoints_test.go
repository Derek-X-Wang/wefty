package ocihelper

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
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
