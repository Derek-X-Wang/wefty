package ocihelper

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestComputerSeatOwnDeadlineAndBothGreetings(t *testing.T) {
	clock := newManualClock(time.Unix(1000, 0))
	dir := t.TempDir()
	answers := map[string][]byte{}
	probes := []uint16{}
	seats := newComputerSeats(clock, dir)
	read := func(name string) ([]byte, error) {
		b, ok := answers[name]
		if !ok {
			return nil, os.ErrNotExist
		}
		return b, nil
	}
	probe := func(_ context.Context, port uint16) error { probes = append(probes, port); return nil }
	first := ComputerSeat{Place: 1, User: "seat1", ViewPort: 7102, ControlPort: 7103}
	if _, err := seats.apply([]ComputerSeat{first}); err != nil {
		t.Fatal(err)
	}
	clock.Advance(61 * time.Second)
	r, err := seats.observe(t.Context(), 1, read, probe, func(ComputerSeat) error { return nil })
	if err != nil || r.Seats[0].State != "failed" || r.Seats[0].Reason != "readiness_deadline_exceeded" || r.Seats[0].CleanupComplete {
		t.Fatalf("first deadline = %+v, %v", r, err)
	}
	second := ComputerSeat{Place: 2, User: "seat2", ViewPort: 7104, ControlPort: 7105}
	if _, err := seats.apply([]ComputerSeat{first, second}); err != nil {
		t.Fatal(err)
	}
	answers["2.json"] = []byte(`{"version":1,"generation":2,"place":2,"state":"ready","reason":"","cleanup_complete":false}`)
	r, err = seats.observe(t.Context(), 2, read, probe, func(ComputerSeat) error { return nil })
	if err != nil || r.Seats[1].State != "ready" {
		t.Fatalf("later Seat inherited boot deadline: %+v %v", r, err)
	}
	if len(probes) != 2 || probes[0] != 7104 || probes[1] != 7105 {
		t.Fatalf("both greetings not checked: %v", probes)
	}
	var directive struct {
		Version    int            `json:"version"`
		Generation uint64         `json:"generation"`
		Seats      []ComputerSeat `json:"seats"`
	}
	b, err := os.ReadFile(filepath.Join(dir, "seats.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(b, &directive); err != nil || directive.Generation != 2 || len(directive.Seats) != 2 {
		t.Fatalf("directive = %s, %v", b, err)
	}
	info, _ := os.Stat(filepath.Join(dir, "seats.json"))
	if info.Mode().Perm() != 0444 {
		t.Fatal("directive not image-readable")
	}
}

func TestComputerSeatLateProbeCannotPublishReady(t *testing.T) {
	clock := newManualClock(time.Unix(1000, 0))
	seats := newComputerSeats(clock, t.TempDir())
	seats.apply([]ComputerSeat{{1, "seat1", 7102, 7103}})
	read := func(string) ([]byte, error) {
		return []byte(`{"version":1,"generation":1,"place":1,"state":"ready","reason":"","cleanup_complete":false}`), nil
	}
	r, err := seats.observe(t.Context(), 1, read, func(context.Context, uint16) error { clock.Advance(31 * time.Second); return nil }, func(ComputerSeat) error { return nil })
	if err != nil || r.Seats[0].State != "failed" || r.Seats[0].Reason != "readiness_deadline_exceeded" {
		t.Fatalf("late probe = %+v, %v", r, err)
	}
}

func TestComputerSeatRemovalRequiresProofAndIndependentAbsenceBeforeReuse(t *testing.T) {
	clock := newManualClock(time.Unix(1000, 0))
	dir := t.TempDir()
	seats := newComputerSeats(clock, dir)
	first := ComputerSeat{1, "seat1", 7102, 7103}
	if _, err := seats.apply([]ComputerSeat{first}); err != nil {
		t.Fatal(err)
	}
	changed := first
	changed.ControlPort = 7203
	if _, err := seats.apply([]ComputerSeat{changed}); err == nil {
		t.Fatal("in-place config change was published")
	}
	if _, err := seats.apply(nil); err != nil {
		t.Fatal(err)
	}
	if _, err := seats.apply([]ComputerSeat{changed}); err == nil {
		t.Fatal("place reused without positive cleanup evidence")
	}
	answers := map[string][]byte{}
	read := func(name string) ([]byte, error) {
		if b, ok := answers[name]; ok {
			return b, nil
		}
		return nil, os.ErrNotExist
	}
	absent := false
	check := func(ComputerSeat) error {
		if !absent {
			return errors.New("Seat processes, sockets or folder remain")
		}
		return nil
	}
	observe := func() ComputerSeatObservation {
		t.Helper()
		r, err := seats.observe(t.Context(), 2, read, func(context.Context, uint16) error { return nil }, check)
		if err != nil {
			t.Fatal(err)
		}
		return r.Seats[0]
	}
	clock.Advance(10 * time.Minute)
	if r := observe(); r.CleanupComplete {
		t.Fatalf("missing answers or elapsed deadline released cleanup: %+v", r)
	}
	answers["1.json"] = []byte(`{"version":1,"generation":2,"place":1,"state":"removed","reason":"","cleanup_complete":true}`)
	answers["applied.json"] = []byte(`{"version":1,"generation":1,"wanted_places":[],"cleanup_complete":true}`)
	absent = true
	if r := observe(); r.CleanupComplete {
		t.Fatal("stale applied proof accepted")
	}
	answers["applied.json"] = []byte(`{"version":1,"generation":2,"wanted_places":[],"cleanup_complete":true}`)
	absent = false
	if r := observe(); r.CleanupComplete {
		t.Fatal("image proof alone accepted")
	}
	absent = true
	if r := observe(); !r.CleanupComplete || r.State != "cleaned" {
		t.Fatalf("positive cleanup not published: %+v", r)
	}
	if _, err := seats.apply([]ComputerSeat{changed}); err != nil {
		t.Fatal(err)
	}
	if _, err := seats.observe(t.Context(), 2, read, nil, check); err == nil {
		t.Fatal("stale observation accepted")
	}
}

type seatProtocolEngine struct {
	*fakeEngine
	seats *computerSeats
}

func (e *seatProtocolEngine) ApplyComputerSeats(_ context.Context, r ApplyComputerSeatsRequest) (ApplyComputerSeatsResponse, error) {
	return e.seats.apply(r.Seats)
}
func (e *seatProtocolEngine) ObserveComputerSeats(ctx context.Context, r ObserveComputerSeatsRequest) (ObserveComputerSeatsResponse, error) {
	return e.seats.observe(ctx, r.Generation, func(string) ([]byte, error) { return nil, os.ErrNotExist }, func(context.Context, uint16) error { return nil }, func(ComputerSeat) error { return nil })
}
func TestComputerSeatProtocolRequiresExactLiveComputerAndGeneration(t *testing.T) {
	e := &seatProtocolEngine{newFakeEngine(), newComputerSeats(newManualClock(time.Unix(1000, 0)), t.TempDir())}
	client, stop := startTestServer(t, e, ServerConfig{})
	defer stop()
	session, err := client.OpenSession(t.Context(), testSessionRequest())
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	requireSweep(t, session)
	authority := testAuthority()
	request := testRunRequest(authority, time.Minute)
	if _, err := session.Run(t.Context(), request); err != nil {
		t.Fatal(err)
	}
	apply := ApplyComputerSeatsRequest{Authority: authority, Seats: []ComputerSeat{{1, "seat1", 7102, 7103}}}
	if _, err := session.ApplyComputerSeats(t.Context(), apply); err == nil {
		t.Fatal("non-Computer accepted")
	}
	authority.AttemptID = "computer-seats"
	authority.Class = "service"
	request = testRunRequest(authority, time.Minute)
	request.Workload.Computer = true
	request.EnableHostBridgeFallback = true
	request.ActivateHostBridgeFallback = true
	request.AllocateEndpoints = []string{"view", "control"}
	request.Workload.Limits.MemoryBytes = 1 << 30
	request.Workload.ManagedVolumes = []ManagedVolumeDescriptor{{Kind: ManagedVolumeComputerDisk, ComputerStorage: &ComputerStorageReference{ComputerID: "computer", StorageID: "storage", StorageGeneration: 1, IntentRevision: 1, DiskBytes: 1 << 30}}}
	e.runResponse.Endpoints = map[string]uint16{"view": 42011, "control": 42012}
	if _, err := session.Run(t.Context(), request); err != nil {
		t.Fatal(err)
	}
	apply.Authority = authority
	reply, err := session.ApplyComputerSeats(t.Context(), apply)
	if err != nil || reply.Generation != 1 {
		t.Fatalf("apply = %+v, %v", reply, err)
	}
	wrong := authority
	wrong.FencingToken = "wrong"
	apply.Authority = wrong
	if _, err := session.ApplyComputerSeats(t.Context(), apply); err == nil {
		t.Fatal("wrong attempt accepted")
	}
	if _, err := session.ObserveComputerSeats(t.Context(), ObserveComputerSeatsRequest{authority, 99}); err == nil {
		t.Fatal("stale generation accepted")
	}
	r, err := session.ObserveComputerSeats(t.Context(), ObserveComputerSeatsRequest{authority, 1})
	if err != nil || len(r.Seats) != 1 || r.Seats[0].State != "starting" {
		t.Fatalf("observe = %+v %v", r, err)
	}
}

func TestComputerSeatUnchangedStartKeepsDeadlineAndGenerationBindsReadiness(t *testing.T) {
	clock := newManualClock(time.Unix(1000, 0))
	seats := newComputerSeats(clock, t.TempDir())
	first := ComputerSeat{1, "seat1", 7102, 7103}
	second := ComputerSeat{2, "seat2", 7104, 7105}
	seats.apply([]ComputerSeat{first})
	clock.Advance(40 * time.Second)
	seats.apply([]ComputerSeat{first, second})
	clock.Advance(21 * time.Second)
	r, err := seats.observe(t.Context(), 2, func(string) ([]byte, error) { return nil, os.ErrNotExist }, func(context.Context, uint16) error { return nil }, func(ComputerSeat) error { return nil })
	if err != nil || r.Seats[0].State != "failed" || r.Seats[1].State != "starting" {
		t.Fatalf("unchanged start deadline = %+v %v", r, err)
	}
	// A previously ready place is not fresh readiness for a new generation.
	clock = newManualClock(time.Unix(1000, 0))
	seats = newComputerSeats(clock, t.TempDir())
	seats.apply([]ComputerSeat{first})
	read := func(string) ([]byte, error) {
		return []byte(`{"version":1,"generation":1,"place":1,"state":"ready","reason":"","cleanup_complete":false}`), nil
	}
	seats.observe(t.Context(), 1, read, func(context.Context, uint16) error { return nil }, func(ComputerSeat) error { return nil })
	seats.apply([]ComputerSeat{first, second})
	r, err = seats.observe(t.Context(), 2, read, func(context.Context, uint16) error { return nil }, func(ComputerSeat) error { return nil })
	if err != nil || r.Seats[0].State == "ready" {
		t.Fatalf("stale ready accepted in a new generation: %+v %v", r, err)
	}
}

func TestComputerSeatCleanupFailureKeepsReasonAndCannotReleaseRoom(t *testing.T) {
	seats := newComputerSeats(newManualClock(time.Unix(1000, 0)), t.TempDir())
	seats.apply([]ComputerSeat{{1, "seat1", 7102, 7103}})
	seats.apply(nil)
	read := func(name string) ([]byte, error) {
		if name == "1.json" {
			return []byte(`{"version":1,"generation":2,"place":1,"state":"failed","reason":"cleanup_failed","cleanup_complete":false}`), nil
		}
		return nil, os.ErrNotExist
	}
	r, err := seats.observe(t.Context(), 2, read, nil, func(ComputerSeat) error { return nil })
	if err != nil || r.Seats[0].State != "failed" || r.Seats[0].Reason != "cleanup_failed" || r.Seats[0].CleanupComplete {
		t.Fatalf("cleanup failure = %+v %v", r, err)
	}
}

func TestComputerSeatImagePortRangeAndEndpointReuseAreRefusedBeforePublication(t *testing.T) {
	seats := newComputerSeats(newManualClock(time.Unix(1000, 0)), t.TempDir())
	if _, err := seats.apply([]ComputerSeat{{1, "seat1", 80, 7103}}); err == nil {
		t.Fatal("helper published a directive the image rejects: privileged port")
	}
	if _, err := seats.apply([]ComputerSeat{{1, "seat1", 7102, 7103}}); err != nil {
		t.Fatal(err)
	}
	if _, err := seats.apply([]ComputerSeat{{2, "seat2", 7102, 7103}}); err == nil {
		t.Fatal("ports reused during removal without cleanup")
	}
}

func TestComputerSeatCurrentAppliedProofCoversEarlierCompletedRemoval(t *testing.T) {
	seats := newComputerSeats(newManualClock(time.Unix(1000, 0)), t.TempDir())
	seats.apply([]ComputerSeat{{1, "seat1", 7102, 7103}})
	seats.apply(nil)
	// The image removed place 1 in generation 2, but a later list arrived
	// before the helper sampled that answer. Omitted places are not rewritten.
	seats.apply([]ComputerSeat{{2, "seat2", 7104, 7105}})
	read := func(name string) ([]byte, error) {
		switch name {
		case "1.json":
			return []byte(`{"version":1,"generation":2,"place":1,"state":"removed","reason":"","cleanup_complete":true}`), nil
		case "applied.json":
			return []byte(`{"version":1,"generation":3,"wanted_places":[2],"cleanup_complete":true}`), nil
		}
		return nil, os.ErrNotExist
	}
	r, err := seats.observe(t.Context(), 3, read, func(context.Context, uint16) error { return nil }, func(ComputerSeat) error { return nil })
	if err != nil || r.Seats[0].State != "cleaned" || !r.Seats[0].CleanupComplete {
		t.Fatalf("current applied proof did not cover completed removal: %+v %v", r, err)
	}
}

func TestComputerSeatInvalidReadyAnswerCannotReuseCachedReadiness(t *testing.T) {
	seats := newComputerSeats(newManualClock(time.Unix(1000, 0)), t.TempDir())
	seats.apply([]ComputerSeat{{1, "seat1", 7102, 7103}})
	answer := []byte(`{"version":1,"generation":1,"place":1,"state":"ready","reason":"","cleanup_complete":false}`)
	read := func(string) ([]byte, error) { return answer, nil }
	probe := func(context.Context, uint16) error { return nil }
	cleanup := func(ComputerSeat) error { return nil }
	r, err := seats.observe(t.Context(), 1, read, probe, cleanup)
	if err != nil || r.Seats[0].State != "ready" {
		t.Fatal("fixture did not become ready")
	}
	answer = []byte(`{"version":1,"generation":1,"place":1,"state":"ready","reason":"contradictory","cleanup_complete":true}`)
	r, err = seats.observe(t.Context(), 1, read, probe, cleanup)
	if err != nil || r.Seats[0].State == "ready" {
		t.Fatalf("invalid answer reused cached readiness: %+v %v", r, err)
	}
}
