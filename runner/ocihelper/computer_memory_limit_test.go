package ocihelper

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
)

type fakeComputerMemory struct {
	thawFailure                    error
	limit, usage                   int64
	writes                         []int64
	frozen, failWrite, badReadback bool
	reserved                       *int64
}

func (f *fakeComputerMemory) readLimit() (int64, error) {
	if f.badReadback && len(f.writes) > 0 {
		return 0, errors.New("unreadable")
	}
	return f.limit, nil
}
func (f *fakeComputerMemory) readUsage() (int64, error) { return f.usage, nil }
func (f *fakeComputerMemory) writeLimit(n int64) error {
	if n > *f.reserved {
		return errors.New("reservation below limit")
	}
	f.writes = append(f.writes, n)
	if f.failWrite {
		return errors.New("write failed")
	}
	if n < f.limit && !f.frozen {
		return errors.New("shrink raced running allocations")
	}
	f.limit = n
	return nil
}
func (f *fakeComputerMemory) freeze(context.Context) (func() error, error) {
	f.frozen = true
	return func() error { f.frozen = false; return f.thawFailure }, nil
}
func memoryLimitFixture(t *testing.T) (*computerSeats, *fakeComputerMemory, *int64, Clock) {
	t.Helper()
	seats, _ := cleanedComputerSeatFixture(t)
	reserved := int64(1 << 30)
	return seats, &fakeComputerMemory{limit: reserved, usage: 128 << 20, reserved: &reserved}, &reserved, newManualClock(time.Unix(1234, 0))
}
func TestComputerMemoryLimitGrowShrinkAndHeldRoom(t *testing.T) {
	s, f, reserved, clock := memoryLimitFixture(t)
	call := func(n int64) (SetComputerMemoryLimitResponse, error) {
		return changeComputerMemoryLimit(t.Context(), SetComputerMemoryLimitRequest{MemoryBytes: n, Generation: s.generation}, s, 233<<20, reserved, 2<<30, f, clock, func() error { return nil })
	}
	r, err := call(2 << 30)
	if err != nil || r.MemoryLimitBytes != 2<<30 || r.ReservedMemoryBytes != 2<<30 || *reserved != f.limit || !r.ObservedAt.Equal(clock.Now()) {
		t.Fatalf("grow: %+v %v reserved=%d", r, err, *reserved)
	}
	// A new Seat joins after the grow, then is omitted and independently
	// observed cleaned. Historical cleanup must not authorize this shrink.
	if _, err := s.apply([]ComputerSeat{{1, "seat1", 7102, 7103}}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.apply(nil); err != nil {
		t.Fatal(err)
	}
	_, err = s.observe(t.Context(), s.generation, func(name string) ([]byte, error) {
		if name == "applied.json" {
			return []byte(fmt.Sprintf(`{"version":1,"generation":%d,"wanted_places":[],"cleanup_complete":true}`, s.generation)), nil
		}
		return []byte(fmt.Sprintf(`{"version":1,"generation":%d,"place":1,"state":"removed","reason":"","cleanup_complete":true}`, s.generation)), nil
	}, nil, func(ComputerSeat) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	f.usage = 1500 << 20
	r, err = call(1 << 30)
	if err != nil || r.Reason != "memory_in_use" || r.HeldMemoryBytes != 1<<30 || *reserved != 2<<30 || f.limit != 2<<30 || f.frozen {
		t.Fatalf("held: %+v %v", r, err)
	}
	f.usage = 128 << 20
	r, err = call(1 << 30)
	if err != nil || r.Reason != "" || r.HeldMemoryBytes != 0 || r.MemoryLimitBytes != 1<<30 || *reserved != 1<<30 || f.frozen {
		t.Fatalf("shrink: %+v %v", r, err)
	}
	r, err = call(1 << 30)
	if err != nil || r.ReservedMemoryBytes != 1<<30 {
		t.Fatalf("replay: %+v %v", r, err)
	}
}
func TestComputerMemoryLimitRefusalsPreserveLimitAndRoom(t *testing.T) {
	for _, tc := range []struct {
		name, reason string
		target       int64
		mutate       func(*computerSeats, *fakeComputerMemory)
		available    int64
	}{
		{"failed grow", "memory_grow_failed", 2 << 30, func(_ *computerSeats, f *fakeComputerMemory) { f.failWrite = true }, 2 << 30},
		{"capacity", "memory_capacity", 2 << 30, func(*computerSeats, *fakeComputerMemory) {}, 0},
		{"cleanup", "cleanup_not_confirmed", 512 << 20, func(s *computerSeats, _ *fakeComputerMemory) { s.entries[1].cleaned = false }, 2 << 30},
		{"tmpfs", "tmpfs_floor", 256 << 20, func(*computerSeats, *fakeComputerMemory) {}, 2 << 30},
		{"generation", "generation_mismatch", 2 << 30, func(s *computerSeats, _ *fakeComputerMemory) { s.generation++ }, 2 << 30},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, f, reserved, clock := memoryLimitFixture(t)
			tc.mutate(s, f)
			_, err := changeComputerMemoryLimit(t.Context(), SetComputerMemoryLimitRequest{MemoryBytes: tc.target, Generation: 2}, s, 233<<20, reserved, tc.available, f, clock, func() error { return nil })
			var refusal *SeatOperationRefusalError
			if !errors.As(err, &refusal) || string(refusal.Reason) != tc.reason || *reserved != 1<<30 || f.limit != 1<<30 || f.frozen {
				t.Fatalf("err=%v reserved=%d limit=%d", err, *reserved, f.limit)
			}
		})
	}
}
func TestComputerMemoryLimitLostAuthorityAndUncertainReadback(t *testing.T) {
	s, f, reserved, clock := memoryLimitFixture(t)
	validate := func() error { return &SeatOperationRefusalError{Reason: SeatRefusalAttemptNotLive} }
	_, err := changeComputerMemoryLimit(t.Context(), SetComputerMemoryLimitRequest{MemoryBytes: 2 << 30, Generation: 2}, s, 233<<20, reserved, 2<<30, f, clock, validate)
	if err == nil || len(f.writes) != 0 || *reserved != 1<<30 {
		t.Fatalf("stale authority mutated limit: %v", err)
	}
	f.badReadback = true
	_, err = changeComputerMemoryLimit(t.Context(), SetComputerMemoryLimitRequest{MemoryBytes: 2 << 30, Generation: 2}, s, 233<<20, reserved, 2<<30, f, clock, func() error { return nil })
	var refusal *SeatOperationRefusalError
	if err == nil || errors.As(err, &refusal) || *reserved != 2<<30 {
		t.Fatalf("uncertainty released room or claimed recoverable refusal: %v reserved=%d", err, *reserved)
	}
}

// The private wire seam must preserve refusal reasons and authorize before any
// runtime call. It does not require a Linux runtime or modify public CLI exits.
type memoryProtocolEngine struct {
	*fakeEngine
	calls   int
	failure error
}

func (e *memoryProtocolEngine) SetComputerMemoryLimit(_ context.Context, r SetComputerMemoryLimitRequest) (SetComputerMemoryLimitResponse, error) {
	e.calls++
	return SetComputerMemoryLimitResponse{MemoryLimitBytes: r.MemoryBytes, ReservedMemoryBytes: r.MemoryBytes}, e.failure
}
func TestComputerMemoryLimitProtocolAuthorizationAndRefusals(t *testing.T) {
	e := &memoryProtocolEngine{fakeEngine: newFakeEngine()}
	e.runResponse.Endpoints = map[string]uint16{"view": 42011, "control": 42012}
	client, stop := startTestServer(t, e, ServerConfig{})
	defer stop()
	session, err := client.OpenSession(t.Context(), testSessionRequest())
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	a := testAuthority()
	a.Class = "service"
	req := SetComputerMemoryLimitRequest{Authority: a, MemoryBytes: 2 << 30}
	_, err = session.SetComputerMemoryLimit(t.Context(), req)
	if err == nil || e.calls != 0 {
		t.Fatal("sweep bypass")
	}
	requireSweep(t, session)
	run := testRunRequest(a, time.Minute)
	run.Workload.Computer = true
	run.Workload.Limits.MemoryBytes = 1 << 30
	run.Workload.ManagedVolumes = testComputerManagedVolumes()
	run.AllocateEndpoints = []string{"view", "control"}
	run.EnableHostBridgeFallback = true
	run.ActivateHostBridgeFallback = true
	if _, err = session.Run(t.Context(), run); err != nil {
		t.Fatal(err)
	}
	wrong := req
	wrong.Authority.FencingToken = "wrong"
	_, err = session.SetComputerMemoryLimit(t.Context(), wrong)
	var rpc *RPCError
	if !errors.As(err, &rpc) || rpc.Code != CodeAttemptOutsideSession || e.calls != 0 {
		t.Fatalf("foreign attempt: %v calls=%d", err, e.calls)
	}
	r, err := session.SetComputerMemoryLimit(t.Context(), req)
	if err != nil || r.MemoryLimitBytes != req.MemoryBytes || e.calls != 1 {
		t.Fatalf("wire: %+v %v", r, err)
	}
	e.failure = &SeatOperationRefusalError{Reason: SeatRefusalCleanupNotConfirmed}
	_, err = session.SetComputerMemoryLimit(t.Context(), req)
	if !errors.As(err, &rpc) || rpc.Code != CodeSeatOperationRefused || rpc.Detail != "cleanup_not_confirmed" {
		t.Fatalf("refusal: %v", err)
	}
	e.failure = errors.New("readback failed")
	_, err = session.SetComputerMemoryLimit(t.Context(), req)
	if !errors.As(err, &rpc) || rpc.Code != CodeDiagnosticFailure {
		t.Fatalf("broken readback: %v", err)
	}
}

func TestComputerMemoryFreezeWaitUsesInjectedClock(t *testing.T) {
	clock := newManualClock(time.Unix(1234, 0))
	if err := waitComputerMemoryFrozen(t.Context(), clock, func() ([]byte, error) { return []byte("populated 1\nfrozen 1\n"), nil }); err != nil {
		t.Fatal(err)
	}
	err := waitComputerMemoryFrozen(t.Context(), clock, func() ([]byte, error) { clock.Advance(3 * time.Second); return []byte("populated 1\nfrozen 0\n"), nil })
	var refusal *SeatOperationRefusalError
	if !errors.As(err, &refusal) || refusal.Reason != SeatRefusalMemoryBusy {
		t.Fatalf("freeze deadline: %v", err)
	}
}

func TestComputerMemoryLimitCannotReuseReturnedCleanupAfterLaterGrow(t *testing.T) {
	s, f, reserved, clock := memoryLimitFixture(t)
	*reserved = 2 << 30
	f.limit = 2 << 30
	call := func(n int64) (SetComputerMemoryLimitResponse, error) {
		return changeComputerMemoryLimit(t.Context(), SetComputerMemoryLimitRequest{MemoryBytes: n, Generation: s.generation}, s, 233<<20, reserved, 2<<30, f, clock, func() error { return nil })
	}
	if _, err := call(1 << 30); err != nil {
		t.Fatal(err)
	}
	if _, err := call(2 << 30); err != nil {
		t.Fatal(err)
	}
	_, err := call(1 << 30)
	var refusal *SeatOperationRefusalError
	if !errors.As(err, &refusal) || refusal.Reason != SeatRefusalCleanupNotConfirmed || *reserved != 2<<30 {
		t.Fatalf("historical cleanup returned newly grown room: %v reserved=%d", err, *reserved)
	}
}

func TestComputerMemoryFreezeMalformedEvidenceIsBroken(t *testing.T) {
	clock := newManualClock(time.Unix(1234, 0))
	err := waitComputerMemoryFrozen(t.Context(), clock, func() ([]byte, error) { clock.Advance(3 * time.Second); return []byte("populated 1\n"), nil })
	var refusal *SeatOperationRefusalError
	if err == nil || errors.As(err, &refusal) {
		t.Fatalf("malformed freezer evidence became a retry: %v", err)
	}
}

func TestComputerMemoryLimitThawFailureOverridesRecoverableRefusal(t *testing.T) {
	s, f, reserved, clock := memoryLimitFixture(t)
	f.failWrite = true
	f.thawFailure = errors.New("thaw failed")
	_, err := changeComputerMemoryLimit(t.Context(), SetComputerMemoryLimitRequest{MemoryBytes: 2 << 30, Generation: s.generation}, s, 233<<20, reserved, 2<<30, f, clock, func() error { return nil })
	var refusal *SeatOperationRefusalError
	if err == nil || errors.As(err, &refusal) {
		t.Fatalf("broken thaw became a recoverable refusal: %v", err)
	}
}

func TestComputerMemoryLimitHeldCleanupSurvivesInterleavedGrow(t *testing.T) {
	s, f, reserved, clock := memoryLimitFixture(t)
	*reserved = 2 << 30
	f.limit = 2 << 30
	f.usage = 1500 << 20
	call := func(n int64) (SetComputerMemoryLimitResponse, error) {
		return changeComputerMemoryLimit(t.Context(), SetComputerMemoryLimitRequest{MemoryBytes: n, Generation: s.generation}, s, 233<<20, reserved, 2<<30, f, clock, func() error { return nil })
	}
	held, err := call(1 << 30)
	if err != nil || held.Reason != "memory_in_use" {
		t.Fatalf("hold: %+v %v", held, err)
	}
	// A third Seat can require a grow while the ended second Seat still holds
	// its share. The second Seat's unreturned room must remain reclaimable.
	if _, err = call(3 << 30); err != nil {
		t.Fatal(err)
	}
	f.usage = 128 << 20
	r, err := call(2 << 30)
	if err != nil || r.MemoryLimitBytes != 2<<30 || *reserved != 2<<30 {
		t.Fatalf("grow erased unreturned cleanup proof: %+v %v", r, err)
	}
}
