package agent

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/l1"
	"github.com/Derek-X-Wang/wefty/runner/ocihelper"
)

func (f *seatTestRuntime) SetComputerMemoryLimit(_ context.Context, r ocihelper.SetComputerMemoryLimitRequest) (ocihelper.SetComputerMemoryLimitResponse, error) {
	f.memoryRequests = append(f.memoryRequests, r)
	if f.memoryResponse != nil {
		return *f.memoryResponse, f.memoryError
	}
	return ocihelper.SetComputerMemoryLimitResponse{RequestedMemoryBytes: r.MemoryBytes, MemoryLimitBytes: r.MemoryBytes, ReservedMemoryBytes: r.MemoryBytes}, f.memoryError
}
func (f *seatTestControl) ReportComputerRoomMemory(_ context.Context, _, _, _ string, r l1.SeatRoomMemoryReport) (l1.SeatRoomMemoryReceipt, error) {
	f.memoryReports = append(f.memoryReports, r)
	return l1.SeatRoomMemoryReceipt{HeldMemoryBytes: r.HeldMemoryBytes, TargetMemoryBytes: r.RequestedMemoryBytes, RoomRevision: r.RoomRevision}, f.memoryError
}
func TestComputerSeatsRestartRecoversCleanupAndSequence(t *testing.T) {
	for _, boundary := range []string{"list", "discover", "memory-limit", "memory-report", "apply", "observe", "cleanup-report", "shrink", "shrink-report"} {
		t.Run(boundary, func(t *testing.T) { testSeatRestartBoundary(t, boundary) })
	}
	c, api, runtime := newSeatTestController()
	c.clock = newManualClock(time.Unix(100, 0))
	old := api.list.Seats[0]
	api.list.Seats = nil
	api.list.Recovery = &l1.ComputerSeatRecovery{Bindings: []l1.WantedComputerSeat{old}, Sequence: 9, Room: &l1.SeatRoomMemoryReceipt{RoomRevision: 3, HeldMemoryBytes: 2 << 30, TargetMemoryBytes: 2 << 30}}
	runtime.generation = 2
	runtime.observations = []ocihelper.ComputerSeatObservation{{Place: 1, State: "cleaned", CleanupComplete: true}}
	api.onReport = func(r l1.ComputerSeatObservationReport) {
		api.list.Recovery.Sequence = r.Sequence
		api.list.Recovery.Bindings = nil
		api.list.Recovery.Room = &l1.SeatRoomMemoryReceipt{RoomRevision: 4, HeldMemoryBytes: 2 << 30, TargetMemoryBytes: 1 << 30}
	}
	if err := c.step(t.Context()); err != nil {
		t.Fatal(err)
	}
	if len(api.reports) != 1 || api.reports[0].Sequence != 10 || api.reports[0].Seats[0].SeatID != old.SeatID {
		t.Fatalf("cleanup binding or sequence lost: %+v", api.reports)
	}
	// The cleanup write committed, but its memory return did not. Rebuild from L1.
	c = newComputerSeatController(api, runtime, c.authority, c.computerID, c.clock, nil)
	if err := c.step(t.Context()); err != nil {
		t.Fatal(err)
	}
	if len(runtime.memoryRequests) == 0 || runtime.memoryRequests[len(runtime.memoryRequests)-1].MemoryBytes != 1<<30 {
		t.Fatalf("true final target was not applied: %+v", runtime.memoryRequests)
	}
	if len(api.memoryReports) == 0 || api.memoryReports[len(api.memoryReports)-1].HeldMemoryBytes != 1<<30 || api.memoryReports[len(api.memoryReports)-1].RoomRevision != 4 {
		t.Fatalf("readback was not reported: %+v", api.memoryReports)
	}
	for _, r := range runtime.applies {
		if len(r.Seats) != 0 {
			t.Fatalf("cleanup resurrected a wanted Seat: %+v", r)
		}
	}
}

func (f *seatTestRuntime) VerifyComputerSeatAttemptAbsent(_ context.Context, a ocihelper.AttemptAuthority) (bool, error) {
	f.verifiedAttempts = append(f.verifiedAttempts, a)
	return f.absent, f.absenceError
}
func (f *seatTestControl) ReportComputerSeatAttemptAbsence(_ context.Context, _, _, _ string, r l1.ComputerSeatAttemptAbsenceReport) (l1.ComputerSeatObservationReceipt, error) {
	f.absenceReports = append(f.absenceReports, r)
	if f.onAbsence != nil {
		f.onAbsence(r)
	}
	return l1.ComputerSeatObservationReceipt{Accepted: true}, nil
}
func TestComputerSeatsOldAttemptAbsencePrecedesCleanupAndMemory(t *testing.T) {
	c, api, runtime := newSeatTestController()
	c.clock = newManualClock(time.Unix(100, 0))
	old := api.list.Seats[0]
	api.list.Seats = nil
	retired := l1.ComputerSeatRetiredAttempt{NodeID: "node", BootSessionID: "old-boot", JobID: "job", AttemptID: "old-attempt", FencingToken: "old-fence", RemovalGeneration: fmt.Sprint(l1.InitialServiceRemovalGeneration)}
	api.list.Recovery = &l1.ComputerSeatRecovery{Bindings: []l1.WantedComputerSeat{}, RetiredAttempts: []l1.ComputerSeatRetiredAttempt{retired}, Room: &l1.SeatRoomMemoryReceipt{RoomRevision: 1, HeldMemoryBytes: 2 << 30, TargetMemoryBytes: 2 << 30}}
	runtime.generation = 2
	runtime.observations = []ocihelper.ComputerSeatObservation{{Place: 1, State: "cleaned", CleanupComplete: true}}
	if err := c.step(t.Context()); err == nil {
		t.Fatal("missing old Attempt absence was hidden")
	}
	if len(runtime.applies) != 0 || len(runtime.memoryRequests) != 0 || len(api.reports) != 0 || len(api.memoryReports) != 0 {
		t.Fatal("work or room changed before old Attempt absence")
	}
	if len(runtime.verifiedAttempts) != 1 || runtime.verifiedAttempts[0].RemovalGeneration != fmt.Sprint(l1.InitialServiceRemovalGeneration) {
		t.Fatalf("wrong old runtime identity: %+v", runtime.verifiedAttempts)
	}
	runtime.absent = true
	api.onAbsence = func(r l1.ComputerSeatAttemptAbsenceReport) {
		if r.Retired != retired || !r.Absent {
			t.Fatalf("wrong absence identity: %+v", r)
		}
		api.list.Recovery.RetiredAttempts = nil
		api.list.Recovery.Bindings = []l1.WantedComputerSeat{old}
	}
	if err := c.step(t.Context()); err != nil {
		t.Fatal(err)
	}
	if len(api.absenceReports) != 1 || len(api.memoryReports) != 0 {
		t.Fatal("absence alone released room")
	}
	api.onReport = func(r l1.ComputerSeatObservationReport) {
		api.list.Recovery.Bindings = nil
		api.list.Recovery.Room = &l1.SeatRoomMemoryReceipt{RoomRevision: 2, HeldMemoryBytes: 2 << 30, TargetMemoryBytes: 1 << 30}
	}
	if err := c.step(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := c.step(t.Context()); err != nil {
		t.Fatal(err)
	}
	if len(api.reports) != 1 || len(api.memoryReports) == 0 || api.memoryReports[len(api.memoryReports)-1].HeldMemoryBytes != 1<<30 {
		t.Fatalf("cleanup and shrink did not settle: %+v %+v", api.reports, api.memoryReports)
	}
}

func TestComputerSeatsFailedGrowCannotPublishEndedRecoveryPlan(t *testing.T) {
	c, api, runtime := newSeatTestController()
	c.clock = newManualClock(time.Unix(100, 0))
	old := api.list.Seats[0]
	api.list.Recovery = &l1.ComputerSeatRecovery{Bindings: []l1.WantedComputerSeat{old}, Room: &l1.SeatRoomMemoryReceipt{RoomRevision: 1, HeldMemoryBytes: 1 << 30, TargetMemoryBytes: 2 << 30}}
	runtime.memoryError = errors.New("grow response lost")
	if err := c.step(t.Context()); err == nil {
		t.Fatal("grow failure hidden")
	}
	api.list.Revision++
	api.list.Seats = nil
	api.list.Recovery.Room.TargetMemoryBytes = 1 << 30
	runtime.memoryError = nil
	if err := c.step(t.Context()); err != nil {
		t.Fatal(err)
	}
	for _, apply := range runtime.applies {
		if len(apply.Seats) != 0 {
			t.Fatalf("published ended Seat after failed grow: %+v", apply)
		}
	}
}

func TestComputerSeatsDiskFullCleanupContinuesAfterMemoryReportFailure(t *testing.T) {
	c, api, runtime := newSeatTestController()
	c.clock = newManualClock(time.Unix(100, 0))
	old := api.list.Seats[0]
	api.list.Seats = nil
	api.list.Recovery = &l1.ComputerSeatRecovery{Bindings: []l1.WantedComputerSeat{old}, Room: &l1.SeatRoomMemoryReceipt{RoomRevision: 3, HeldMemoryBytes: 2 << 30, TargetMemoryBytes: 2 << 30}}
	api.memoryError = errors.New("memory receipt unavailable")
	runtime.observations = []ocihelper.ComputerSeatObservation{{Place: 1, State: "failed", Reason: "no space left on device"}}
	if err := c.step(t.Context()); err == nil {
		t.Fatal("memory report failure hidden")
	}
	if len(api.reports) != 1 || api.reports[0].Seats[0].CleanupComplete || api.reports[0].Seats[0].Reason != "no space left on device" {
		t.Fatalf("cleanup stopped behind failed memory report: %+v", api.reports)
	}
	api.list.Recovery.Sequence = api.reports[0].Sequence
	// Disk space is available again after a process restart. Only cleanup may lower the target.
	c = newComputerSeatController(api, runtime, c.authority, c.computerID, c.clock, nil)
	api.memoryError = nil
	runtime.observations = []ocihelper.ComputerSeatObservation{{Place: 1, State: "cleaned", CleanupComplete: true}}
	api.onReport = func(r l1.ComputerSeatObservationReport) {
		api.list.Recovery.Bindings = nil
		api.list.Recovery.Sequence = r.Sequence
		api.list.Recovery.Room = &l1.SeatRoomMemoryReceipt{RoomRevision: 4, HeldMemoryBytes: 2 << 30, TargetMemoryBytes: 1 << 30}
	}
	if err := c.step(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := c.step(t.Context()); err != nil {
		t.Fatal(err)
	}
	if len(api.reports) != 2 || api.reports[1].Sequence != 2 || !api.reports[1].Seats[0].CleanupComplete {
		t.Fatalf("cleanup did not resume: %+v", api.reports)
	}
	for _, r := range runtime.memoryRequests[:len(runtime.memoryRequests)-1] {
		if r.MemoryBytes != 2<<30 {
			t.Fatal("room returned before positive cleanup")
		}
	}
	if runtime.memoryRequests[len(runtime.memoryRequests)-1].MemoryBytes != 1<<30 {
		t.Fatal("cleanup target was not applied")
	}
}

func TestComputerSeatsLateReadyRecoveryOnlyCleans(t *testing.T) {
	c, api, runtime := newSeatTestController()
	c.clock = newManualClock(time.Unix(100, 0))
	old := api.list.Seats[0]
	// L1's injected deadline reconciliation has refused the request and omitted it.
	api.list.Seats = nil
	api.list.Recovery = &l1.ComputerSeatRecovery{Bindings: []l1.WantedComputerSeat{old}, Sequence: 4}
	runtime.observations = []ocihelper.ComputerSeatObservation{{Place: 1, State: "ready"}}
	if err := c.step(t.Context()); err != nil {
		t.Fatal(err)
	}
	if len(api.reports) != 0 {
		t.Fatalf("late ready was offered as a live Seat: %+v", api.reports)
	}
	if len(runtime.applies) != 1 || len(runtime.applies[0].Seats) != 0 {
		t.Fatalf("late ready remained wanted: %+v", runtime.applies)
	}
	c = newComputerSeatController(api, runtime, c.authority, c.computerID, c.clock, nil)
	runtime.observations = []ocihelper.ComputerSeatObservation{{Place: 1, State: "cleaned", CleanupComplete: true}}
	if err := c.step(t.Context()); err != nil {
		t.Fatal(err)
	}
	if len(api.reports) != 1 || api.reports[0].Sequence != 5 || api.reports[0].Seats[0].State != l1.ComputerSeatCleaned {
		t.Fatalf("late ready cleanup lost: %+v", api.reports)
	}
}

// Each operation commits its side effect before the one-shot lost response.
// Replacing the controller discards all local maps, counters and pending calls.
type seatRestartFault struct {
	boundary string
	hit      bool
}

func (f *seatRestartFault) after(boundary string, err error) error {
	if !f.hit && f.boundary == boundary {
		f.hit = true
		return errors.New("simulated process exit after " + boundary)
	}
	return err
}

type seatRestartRuntime struct {
	*seatTestRuntime
	fault *seatRestartFault
}

func (r seatRestartRuntime) ApplyComputerSeats(ctx context.Context, q ocihelper.ApplyComputerSeatsRequest) (ocihelper.ApplyComputerSeatsResponse, error) {
	v, e := r.seatTestRuntime.ApplyComputerSeats(ctx, q)
	return v, r.fault.after("apply", e)
}
func (r seatRestartRuntime) ObserveComputerSeats(ctx context.Context, q ocihelper.ObserveComputerSeatsRequest) (ocihelper.ObserveComputerSeatsResponse, error) {
	v, e := r.seatTestRuntime.ObserveComputerSeats(ctx, q)
	b := "observe"
	if q.Discover {
		b = "discover"
	}
	return v, r.fault.after(b, e)
}
func (r seatRestartRuntime) SetComputerMemoryLimit(ctx context.Context, q ocihelper.SetComputerMemoryLimitRequest) (ocihelper.SetComputerMemoryLimitResponse, error) {
	v, e := r.seatTestRuntime.SetComputerMemoryLimit(ctx, q)
	b := "memory-limit"
	if q.MemoryBytes == 1<<30 {
		b = "shrink"
	}
	return v, r.fault.after(b, e)
}

type seatRestartControl struct {
	*seatTestControl
	fault *seatRestartFault
}

func (a seatRestartControl) GetComputerSeatList(ctx context.Context, c, j, i string, q l1.ComputerSeatAttemptRequest) (l1.ComputerSeatList, error) {
	v, e := a.seatTestControl.GetComputerSeatList(ctx, c, j, i, q)
	return v, a.fault.after("list", e)
}
func (a seatRestartControl) ReportComputerSeats(ctx context.Context, c, j, i string, q l1.ComputerSeatObservationReport) (l1.ComputerSeatObservationReceipt, error) {
	v, e := a.seatTestControl.ReportComputerSeats(ctx, c, j, i, q)
	return v, a.fault.after("cleanup-report", e)
}
func (a seatRestartControl) ReportComputerRoomMemory(ctx context.Context, c, j, i string, q l1.SeatRoomMemoryReport) (l1.SeatRoomMemoryReceipt, error) {
	v, e := a.seatTestControl.ReportComputerRoomMemory(ctx, c, j, i, q)
	a.list.Recovery.Room.HeldMemoryBytes = q.HeldMemoryBytes
	b := "memory-report"
	if q.RequestedMemoryBytes == 1<<30 {
		b = "shrink-report"
	}
	return v, a.fault.after(b, e)
}
func testSeatRestartBoundary(t *testing.T, boundary string) {
	t.Helper()
	c, api, runtime := newSeatTestController()
	clock := newManualClock(time.Unix(100, 0))
	old := api.list.Seats[0]
	api.list.Seats = nil
	api.list.Recovery = &l1.ComputerSeatRecovery{Bindings: []l1.WantedComputerSeat{old}, Sequence: 9, Room: &l1.SeatRoomMemoryReceipt{RoomRevision: 3, HeldMemoryBytes: 2 << 30, TargetMemoryBytes: 2 << 30}}
	runtime.observations = []ocihelper.ComputerSeatObservation{{Place: 1, State: "cleaned", CleanupComplete: true}}
	api.onReport = func(q l1.ComputerSeatObservationReport) {
		api.list.Recovery.Sequence = q.Sequence
		api.list.Recovery.Bindings = nil
		api.list.Recovery.Room = &l1.SeatRoomMemoryReceipt{RoomRevision: 4, HeldMemoryBytes: 2 << 30, TargetMemoryBytes: 1 << 30}
	}
	fault := &seatRestartFault{boundary: boundary}
	control := seatRestartControl{api, fault}
	host := seatRestartRuntime{runtime, fault}
	for i := 0; i < 5; i++ {
		c = newComputerSeatController(control, host, c.authority, c.computerID, clock, nil)
		_ = c.step(t.Context())
	}
	if !fault.hit {
		t.Fatal("crash boundary was not exercised")
	}
	if api.list.Recovery.Sequence != 10 || api.list.Recovery.Room.HeldMemoryBytes != 1<<30 {
		t.Fatalf("restart did not settle cleanup and room: %+v", api.list.Recovery)
	}
	if len(api.reports) != 1 || api.reports[0].Seats[0].SeatID != old.SeatID {
		t.Fatalf("cleanup identity or sequence replay lost: %+v", api.reports)
	}
	for _, q := range runtime.applies {
		if len(q.Seats) != 0 {
			t.Fatal("restart revived ended Seat")
		}
	}
}

func TestComputerSeatsRecoveryMemoryConflictsHaveRetryFloor(t *testing.T) {
	c, api, runtime := newSeatTestController()
	clock := newManualClock(time.Unix(100, 0))
	c.clock = clock
	old := api.list.Seats[0]
	api.list.Seats = nil
	api.list.Recovery = &l1.ComputerSeatRecovery{Bindings: []l1.WantedComputerSeat{old}, Room: &l1.SeatRoomMemoryReceipt{RoomRevision: 1, HeldMemoryBytes: 2 << 30, TargetMemoryBytes: 2 << 30}}
	api.memoryError = &ProtocolError{StatusCode: 409, APIError: contract.APIError{Code: contract.ErrorConflict, Message: "room revision changed"}}
	runtime.observations = nil
	for range 8 {
		_ = c.step(t.Context())
		clock.Advance(time.Second)
	}
	if len(api.memoryReports) >= 8 {
		t.Fatalf("persistent recovery conflict made %d memory reports", len(api.memoryReports))
	}
	if len(runtime.observes) < 8 {
		t.Fatal("memory report backoff stopped helper cleanup")
	}
}
