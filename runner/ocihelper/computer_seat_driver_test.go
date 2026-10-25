package ocihelper

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestSeatDriverSignalIsPerPlaceAndRejectsStaleGeneration(t *testing.T) {
	dir := t.TempDir()
	seats := newComputerSeats(nil, dir)
	configured := []ComputerSeat{{Place: 1, User: "seat1", ViewPort: 7102, ControlPort: 7103}, {Place: 2, User: "seat2", ViewPort: 7104, ControlPort: 7105}}
	applied, err := seats.apply(configured)
	if err != nil {
		t.Fatal(err)
	}
	read := func(place string, want string) {
		t.Helper()
		data, err := os.ReadFile(filepath.Join(dir, "seats", place, "driver.json"))
		if err != nil || string(data) != want {
			t.Fatalf("Seat %s driver = %s, %v; want %s", place, data, err, want)
		}
	}
	read("1", `{"version":1,"human_driving":false}`)
	read("2", `{"version":1,"human_driving":false}`)
	if err := seats.setDriver(applied.Generation, 1, true); err != nil {
		t.Fatal(err)
	}
	read("1", `{"version":1,"human_driving":true}`)
	read("2", `{"version":1,"human_driving":false}`)
	if err := seats.setDriver(applied.Generation+1, 1, false); err == nil {
		t.Fatal("stale generation changed driver")
	}
	read("1", `{"version":1,"human_driving":true}`)
	if err := seats.setDriver(applied.Generation, 1, false); err != nil {
		t.Fatal(err)
	}
	read("1", `{"version":1,"human_driving":false}`)
}

func TestSeatDriverSignalClearsOnRemovalWithoutChangingNeighbour(t *testing.T) {
	dir := t.TempDir()
	seats := newComputerSeats(nil, dir)
	if _, err := seats.apply([]ComputerSeat{{1, "seat1", 7102, 7103}, {2, "seat2", 7104, 7105}}); err != nil {
		t.Fatal(err)
	}
	if err := seats.setDriver(1, 1, true); err != nil {
		t.Fatal(err)
	}
	if err := seats.setDriver(1, 2, true); err != nil {
		t.Fatal(err)
	}
	if _, err := seats.apply([]ComputerSeat{{2, "seat2", 7104, 7105}}); err != nil {
		t.Fatal(err)
	}
	one, err := os.ReadFile(filepath.Join(dir, "seats", "1", "driver.json"))
	if err != nil || string(one) != `{"version":1,"human_driving":false}` {
		t.Fatalf("removed Seat driver=%s %v", one, err)
	}
	two, err := os.ReadFile(filepath.Join(dir, "seats", "2", "driver.json"))
	if err != nil || string(two) != `{"version":1,"human_driving":true}` {
		t.Fatalf("neighbour driver=%s %v", two, err)
	}
}

type seatDriverProtocolEngine struct{ *seatProtocolEngine }

func (e *seatDriverProtocolEngine) SetSeatDriver(_ context.Context, r SetSeatDriverRequest) error {
	return e.seats.setDriver(r.Generation, r.Place, r.HumanDriving)
}
func TestSeatDriverRPCRequiresExactLiveComputerAuthority(t *testing.T) {
	e := &seatDriverProtocolEngine{&seatProtocolEngine{newFakeEngine(), newComputerSeats(nil, t.TempDir())}}
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
	run := testRunRequest(a, time.Minute)
	run.Workload.Computer = true
	run.EnableHostBridgeFallback = true
	run.ActivateHostBridgeFallback = true
	run.AllocateEndpoints = []string{"view", "control"}
	run.Workload.Limits.MemoryBytes = 1 << 30
	run.Workload.ManagedVolumes = testComputerManagedVolumes()
	e.runResponse.Endpoints = map[string]uint16{"view": 42011, "control": 42012}
	if _, err = session.Run(t.Context(), run); err != nil {
		t.Fatal(err)
	}
	if _, err = session.ApplyComputerSeats(t.Context(), ApplyComputerSeatsRequest{a, []ComputerSeat{{1, "seat1", 7102, 7103}}}); err != nil {
		t.Fatal(err)
	}
	request := SetSeatDriverRequest{Authority: a, Generation: 1, Place: 1, HumanDriving: true}
	if err = session.SetSeatDriver(t.Context(), request); err != nil {
		t.Fatal(err)
	}
	for _, change := range []func(*SetSeatDriverRequest){func(r *SetSeatDriverRequest) { r.Authority.FencingToken = "old" }, func(r *SetSeatDriverRequest) { r.Authority.BootSessionID = "old" }, func(r *SetSeatDriverRequest) { r.Generation = 2 }, func(r *SetSeatDriverRequest) { r.Place = 2 }} {
		wrong := request
		change(&wrong)
		if err = session.SetSeatDriver(t.Context(), wrong); err == nil {
			t.Fatalf("stale or foreign Seat driver accepted: %+v", wrong)
		}
	}
	if session.HealthError() != nil {
		t.Fatalf("request refusal lost helper session: %v", session.HealthError())
	}
	if _, err = session.Delete(t.Context(), DeleteRequest{Authority: a}); err != nil {
		t.Fatal(err)
	}
	if err = session.SetSeatDriver(t.Context(), request); err == nil {
		t.Fatal("ended Attempt changed driver")
	}
}
