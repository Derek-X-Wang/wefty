package ocihelper

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
)

type fakeComputerMemory struct {
	clock                          Clock
	freezeCalls                    int
	applyWriteError                bool
	readFailures                   int
	thawFailure                    error
	limit, usage                   int64
	writes                         []int64
	frozen, failWrite, badReadback bool
	reserved                       *int64
}

func (f *fakeComputerMemory) readLimit() (int64, error) {
	if f.readFailures > 0 && len(f.writes) > 0 {
		f.readFailures--
		return 0, errors.New("one readback failed")
	}
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
	if f.applyWriteError {
		return errors.New("write applied then returned an error")
	}
	return nil
}
func (f *fakeComputerMemory) freezerIO() computerMemoryFreezerIO {
	return computerMemoryFreezerIO{
		readRequested: func() (bool, error) { return f.frozen, nil },
		readEvents: func() ([]byte, error) {
			v := 0
			if f.frozen {
				v = 1
			}
			return []byte(fmt.Sprintf("frozen %d\n", v)), nil
		},
		write: func(on bool) error {
			if on {
				f.freezeCalls++
				f.frozen = true
				return nil
			}
			if f.thawFailure != nil {
				return f.thawFailure
			}
			f.frozen = false
			return nil
		},
	}
}
func (f *fakeComputerMemory) recoverFreeze(ctx context.Context) (bool, error) {
	return recoverComputerMemoryFreeze(ctx, f.clock, f.freezerIO())
}
func (f *fakeComputerMemory) shrink(ctx context.Context, target int64) (computerMemoryShrinkResult, error) {
	return shrinkComputerMemory(ctx, f.clock, f.freezerIO(), f.readUsage, f.readLimit, f.writeLimit, target)
}
func (f *fakeComputerMemory) freezerRequests() int { return f.freezeCalls }

func memoryLimitFixture(t *testing.T) (*computerSeats, *fakeComputerMemory, *int64, Clock) {
	t.Helper()
	seats, _ := cleanedComputerSeatFixture(t)
	reserved := int64(1 << 30)
	clock := newManualClock(time.Unix(1234, 0))
	return seats, &fakeComputerMemory{clock: clock, limit: reserved, usage: 128 << 20, reserved: &reserved}, &reserved, clock
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
	e.failure = &SeatOperationRefusalError{Reason: SeatRefusalMemoryThawFailed}
	_, err = session.SetComputerMemoryLimit(t.Context(), req)
	if !errors.As(err, &rpc) || rpc.Code != CodeSeatOperationRefused || rpc.Detail != "memory_thaw_failed" {
		t.Fatalf("frozen-state wire detail: %v", err)
	}
	e.failure = errors.New("readback failed")
	_, err = session.SetComputerMemoryLimit(t.Context(), req)
	if !errors.As(err, &rpc) || rpc.Code != CodeDiagnosticFailure {
		t.Fatalf("broken readback: %v", err)
	}
}

func TestComputerMemoryFreezeWaitUsesInjectedClock(t *testing.T) {
	clock := newManualClock(time.Unix(1234, 0))
	if err := waitComputerMemoryFrozen(t.Context(), clock, clock.Now().Add(2*time.Second), true, func() ([]byte, error) { return []byte("populated 1\nfrozen 1\n"), nil }); err != nil {
		t.Fatal(err)
	}
	err := waitComputerMemoryFrozen(t.Context(), clock, clock.Now().Add(2*time.Second), true, func() ([]byte, error) { clock.Advance(3 * time.Second); return []byte("populated 1\nfrozen 0\n"), nil })
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
	err := waitComputerMemoryFrozen(t.Context(), clock, clock.Now().Add(2*time.Second), true, func() ([]byte, error) { clock.Advance(3 * time.Second); return []byte("populated 1\n"), nil })
	var refusal *SeatOperationRefusalError
	if err == nil || errors.As(err, &refusal) {
		t.Fatalf("malformed freezer evidence became a retry: %v", err)
	}
}

func TestComputerMemoryLimitThawFailureOverridesShrinkWriteFailure(t *testing.T) {
	s, f, reserved, clock := memoryLimitFixture(t)
	*reserved = 2 << 30
	f.limit = 2 << 30
	f.failWrite = true
	f.thawFailure = errors.New("thaw failed")
	_, err := changeComputerMemoryLimit(t.Context(), SetComputerMemoryLimitRequest{MemoryBytes: 1 << 30, Generation: s.generation}, s, 233<<20, reserved, 2<<30, f, clock, func() error { return nil })
	var refusal *SeatOperationRefusalError
	if !errors.As(err, &refusal) || refusal.Reason != SeatRefusalMemoryThawFailed || *reserved != 2<<30 {
		t.Fatalf("lost possible-frozen state: %v reserved=%d", err, *reserved)
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

func TestComputerMemoryReworkGrowAndHeldNeverFreeze(t *testing.T) {
	s, f, reserved, clock := memoryLimitFixture(t)
	call := func(n int64) (SetComputerMemoryLimitResponse, error) {
		return changeComputerMemoryLimit(t.Context(), SetComputerMemoryLimitRequest{MemoryBytes: n, Generation: s.generation}, s, 233<<20, reserved, 2<<30, f, clock, func() error { return nil })
	}
	if _, err := call(2 << 30); err != nil {
		t.Fatal(err)
	}
	f.usage = 1500 << 20
	for i := 0; i < 5; i++ {
		r, err := call(1 << 30)
		if err != nil || r.Reason != "memory_in_use" {
			t.Fatalf("held: %+v %v", r, err)
		}
	}
	if f.freezeCalls != 0 {
		t.Fatalf("grow and held retries froze %d times", f.freezeCalls)
	}
}
func TestComputerMemoryReworkAppliedGrowErrorKeepsLargerLimit(t *testing.T) {
	s, f, reserved, clock := memoryLimitFixture(t)
	f.applyWriteError = true
	r, err := changeComputerMemoryLimit(t.Context(), SetComputerMemoryLimitRequest{MemoryBytes: 2 << 30, Generation: s.generation}, s, 233<<20, reserved, 2<<30, f, clock, func() error { return nil })
	if err != nil || r.MemoryLimitBytes != 2<<30 || *reserved != 2<<30 || len(f.writes) != 1 || f.freezeCalls != 0 {
		t.Fatalf("applied grow was undone: %+v %v limit=%d writes=%v freezes=%d", r, err, f.limit, f.writes, f.freezeCalls)
	}
}
func TestComputerMemoryReworkLowerReadbackReconcilesReservation(t *testing.T) {
	s, f, reserved, clock := memoryLimitFixture(t)
	*reserved = 2 << 30
	f.limit = 2 << 30
	f.readFailures = 1
	call := func(n int64) (SetComputerMemoryLimitResponse, error) {
		return changeComputerMemoryLimit(t.Context(), SetComputerMemoryLimitRequest{MemoryBytes: n, Generation: s.generation}, s, 233<<20, reserved, 2<<30, f, clock, func() error { return nil })
	}
	if _, err := call(1 << 30); err == nil || *reserved != 2<<30 || f.limit != 1<<30 {
		t.Fatalf("uncertainty did not hold reservation: %v reserved=%d limit=%d", err, *reserved, f.limit)
	}
	r, err := call(1 << 30)
	if err != nil || r.MemoryLimitBytes != 1<<30 || *reserved != 1<<30 {
		t.Fatalf("exact later readback cannot reconcile: %+v %v reserved=%d", r, err, *reserved)
	}
	if _, err = call(2 << 30); err != nil {
		t.Fatalf("later resize blocked: %v", err)
	}
	_, err = call(1 << 30)
	var refusal *SeatOperationRefusalError
	if !errors.As(err, &refusal) || refusal.Reason != SeatRefusalCleanupNotConfirmed {
		t.Fatalf("reconciled shrink left already-returned cleanup reusable: %v", err)
	}
}
func TestComputerMemoryReworkNeverValidatesWhileFrozen(t *testing.T) {
	s, f, reserved, clock := memoryLimitFixture(t)
	*reserved = 2 << 30
	f.limit = 2 << 30
	validations := 0
	r, err := changeComputerMemoryLimit(t.Context(), SetComputerMemoryLimitRequest{MemoryBytes: 1 << 30, Generation: s.generation}, s, 233<<20, reserved, 2<<30, f, clock, func() error {
		validations++
		if f.frozen {
			return errors.New("containerd call inside freeze")
		}
		return nil
	})
	if err != nil || r.MemoryLimitBytes != 1<<30 || validations < 2 {
		t.Fatalf("validation span: %+v %v calls=%d", r, err, validations)
	}
}

func TestComputerMemoryReworkAbandonedFreezeIsReported(t *testing.T) {
	s, f, reserved, clock := memoryLimitFixture(t)
	f.frozen = true
	r, err := changeComputerMemoryLimit(t.Context(), SetComputerMemoryLimitRequest{MemoryBytes: 2 << 30, Generation: s.generation}, s, 233<<20, reserved, 2<<30, f, clock, func() error { return nil })
	if err != nil || !r.RecoveredFreeze || r.FreezeRequests != 0 || f.frozen {
		t.Fatalf("abandoned freeze not recovered and reported: %+v %v", r, err)
	}
}

func TestComputerMemoryReworkFailedGrowReconcileKeepsCleanup(t *testing.T) {
	s, f, reserved, clock := memoryLimitFixture(t)
	*reserved = 2 << 30
	f.limit = 2 << 30
	f.failWrite = true
	f.readFailures = 1
	call := func(n int64) (SetComputerMemoryLimitResponse, error) {
		return changeComputerMemoryLimit(t.Context(), SetComputerMemoryLimitRequest{MemoryBytes: n, Generation: s.generation}, s, 233<<20, reserved, 2<<30, f, clock, func() error { return nil })
	}
	if _, err := call(3 << 30); err == nil || *reserved != 3<<30 {
		t.Fatalf("failed grow did not retain uncertainty: %v reserved=%d", err, *reserved)
	}
	f.failWrite = false
	if _, err := call(2 << 30); err != nil || *reserved != 2<<30 {
		t.Fatalf("failed grow did not reconcile: %v", err)
	}
	r, err := call(1 << 30)
	if err != nil || r.MemoryLimitBytes != 1<<30 {
		t.Fatalf("reconciling failed grow consumed unrelated cleanup: %+v %v", r, err)
	}
}
