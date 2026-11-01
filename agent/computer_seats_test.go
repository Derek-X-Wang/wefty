package agent

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/internal/testwait"
	"github.com/Derek-X-Wang/wefty/l1"
	workloadrunner "github.com/Derek-X-Wang/wefty/runner"
	"github.com/Derek-X-Wang/wefty/runner/ocihelper"
)

type seatTestControl struct {
	absenceReports []l1.ComputerSeatAttemptAbsenceReport
	onAbsence      func(l1.ComputerSeatAttemptAbsenceReport)
	memoryReports  []l1.SeatRoomMemoryReport
	memoryError    error
	onReport       func(l1.ComputerSeatObservationReport)
	list           l1.ComputerSeatList
	reports        []l1.ComputerSeatObservationReport
	reportError    error
	readError      error
	onRead         func()
}

func (f *seatTestControl) GetComputerSeatList(_ context.Context, _, _, _ string, _ l1.ComputerSeatAttemptRequest) (l1.ComputerSeatList, error) {
	if f.onRead != nil {
		f.onRead()
	}
	return f.list, f.readError
}
func (f *seatTestControl) ReportComputerSeats(_ context.Context, _, _, _ string, r l1.ComputerSeatObservationReport) (l1.ComputerSeatObservationReceipt, error) {
	f.reports = append(f.reports, r)
	if f.reportError == nil && f.onReport != nil {
		f.onReport(r)
	}
	return l1.ComputerSeatObservationReceipt{Accepted: f.reportError == nil}, f.reportError
}

type seatTestRuntime struct {
	absent           bool
	absenceError     error
	verifiedAttempts []ocihelper.AttemptAuthority
	memoryRequests   []ocihelper.SetComputerMemoryLimitRequest
	memoryError      error
	memoryResponse   *ocihelper.SetComputerMemoryLimitResponse
	applies          []ocihelper.ApplyComputerSeatsRequest
	applyError       error
	observeError     error
	observations     []ocihelper.ComputerSeatObservation
	observes         []ocihelper.ObserveComputerSeatsRequest
	generation       uint64
	onObserve        func()
}

func (f *seatTestRuntime) ApplyComputerSeats(_ context.Context, r ocihelper.ApplyComputerSeatsRequest) (ocihelper.ApplyComputerSeatsResponse, error) {
	f.applies = append(f.applies, r)
	return ocihelper.ApplyComputerSeatsResponse{Generation: f.generation}, f.applyError
}
func (f *seatTestRuntime) ObserveComputerSeats(_ context.Context, r ocihelper.ObserveComputerSeatsRequest) (ocihelper.ObserveComputerSeatsResponse, error) {
	f.observes = append(f.observes, r)
	if r.Discover {
		seats := []ocihelper.ComputerSeatObservation{}
		if len(f.applies) > 0 {
			for _, seat := range f.applies[len(f.applies)-1].Seats {
				seats = append(seats, ocihelper.ComputerSeatObservation{Place: seat.Place, State: "starting"})
			}
		}
		return ocihelper.ObserveComputerSeatsResponse{Generation: f.generation, Seats: seats}, f.observeError
	}
	if f.onObserve != nil {
		f.onObserve()
	}
	return ocihelper.ObserveComputerSeatsResponse{Generation: r.Generation, Seats: f.observations}, f.observeError
}
func newSeatTestController() (*computerSeatController, *seatTestControl, *seatTestRuntime) {
	api := &seatTestControl{list: l1.ComputerSeatList{Revision: 1, Seats: []l1.WantedComputerSeat{{SeatID: "seat_" + strings.Repeat("a", 32), Place: 1}}}}
	runtime := &seatTestRuntime{generation: 1, observations: []ocihelper.ComputerSeatObservation{{Place: 1, State: "ready"}}}
	controller := newComputerSeatController(api, runtime, ocihelper.AttemptAuthority{NodeID: "node", BootSessionID: "boot", JobID: "job", AttemptID: "attempt", FencingToken: "fence", Class: contract.JobClassService}, "computer", systemClock{}, nil)
	return controller, api, runtime
}
func TestComputerSeatsRetryUncertainApplyBeforeChangingList(t *testing.T) {
	c, api, runtime := newSeatTestController()
	runtime.applyError = &ocihelper.RPCError{Code: ocihelper.CodeDiagnosticFailure, Message: "answer read failed after rename"}
	if err := c.step(t.Context()); err == nil {
		t.Fatal("diagnostic failure hidden")
	}
	api.list = l1.ComputerSeatList{Revision: 2, Seats: []l1.WantedComputerSeat{{SeatID: "seat_" + strings.Repeat("b", 32), Place: 2}}}
	runtime.applyError = nil
	if err := c.step(t.Context()); err != nil {
		t.Fatal(err)
	}
	if len(runtime.applies) != 2 || !reflect.DeepEqual(runtime.applies[0], runtime.applies[1]) {
		t.Fatalf("uncertain Apply changed: %+v", runtime.applies)
	}
	if runtime.observes[0].Authority.AttemptID != "attempt" || runtime.observes[0].Generation != 1 {
		t.Fatalf("authority %+v", runtime.observes)
	}
	if len(api.reports) != 0 {
		t.Fatalf("old revision reported as new wanted list: %+v", api.reports)
	}
}
func TestComputerSeatsRefusalWaitsAndReadyRegressionIsReported(t *testing.T) {
	c, api, runtime := newSeatTestController()
	runtime.applyError = &ocihelper.RPCError{Code: ocihelper.CodeSeatOperationRefused, Detail: ocihelper.ErrorDetail(ocihelper.SeatRefusalCleanupNotConfirmed)}
	if err := c.step(t.Context()); err != nil || len(api.reports) != 0 {
		t.Fatalf("refusal should wait: %v %+v", err, api.reports)
	}
	runtime.applyError = nil
	if err := c.step(t.Context()); err != nil {
		t.Fatal(err)
	}
	runtime.observations[0].State = "starting"
	if err := c.step(t.Context()); err != nil {
		t.Fatal(err)
	}
	if len(api.reports) != 2 || api.reports[1].Seats[0].State != l1.ComputerSeatStarting || api.reports[1].Seats[0].Reason != "" {
		t.Fatalf("ready regression %+v", api.reports)
	}
	runtime.observations[0].State = "failed"
	runtime.observations[0].Reason = "readiness_deadline"
	if err := c.step(t.Context()); err != nil {
		t.Fatal(err)
	}
	if api.reports[2].Seats[0].Reason != "readiness_deadline" {
		t.Fatal("reason lost")
	}
	runtime.observeError = &ocihelper.RPCError{Code: ocihelper.CodeSeatOperationRefused, Detail: ocihelper.ErrorDetail(ocihelper.SeatRefusalGenerationMismatch)}
	if err := c.step(t.Context()); err != nil || len(api.reports) != 3 {
		t.Fatalf("observe refusal should wait: %v %+v", err, api.reports)
	}
	runtime.observeError = &ocihelper.RPCError{Code: ocihelper.CodeDiagnosticFailure, Message: "answer read failed"}
	if err := c.step(t.Context()); err == nil || len(api.reports) != 3 {
		t.Fatalf("read failure must be an error without a Seat failure report: %v %+v", err, api.reports)
	}
	runtime.observeError = nil
	api.readError = errors.New("L1 unavailable")
	before := len(runtime.observes)
	if err := c.step(t.Context()); err == nil || len(runtime.observes) != before+1 || len(api.reports) != 3 {
		t.Fatalf("L1 read failure stopped helper observations or published stale state: %v", err)
	}

}
func TestComputerSeatsReplacementWaitsForAcknowledgedCleanup(t *testing.T) {
	c, api, runtime := newSeatTestController()
	c.clock = newManualClock(time.Unix(100, 0))
	if err := c.step(t.Context()); err != nil {
		t.Fatal(err)
	}
	old := api.list.Seats[0]
	replacement := l1.WantedComputerSeat{SeatID: "seat_" + strings.Repeat("b", 32), Place: 1}
	api.list = l1.ComputerSeatList{Revision: 2, Seats: []l1.WantedComputerSeat{replacement}}
	runtime.generation = 2
	runtime.observations = []ocihelper.ComputerSeatObservation{{Place: 1, State: "cleaned", CleanupComplete: true}}
	api.reportError = errors.New("receipt lost")
	if err := c.step(t.Context()); err == nil {
		t.Fatal("report failure hidden")
	}
	if len(runtime.applies[1].Seats) != 0 {
		t.Fatalf("replacement added before cleanup: %+v", runtime.applies)
	}
	if api.reports[1].Seats[0].SeatID != old.SeatID {
		t.Fatal("old cleanup relabeled as replacement")
	}
	api.reportError = nil
	c.clock.(*manualClock).Advance(2 * time.Second)
	if err := c.step(t.Context()); err != nil {
		t.Fatal(err)
	}
	if len(runtime.observes) != 3 || len(runtime.applies) != 2 || !reflect.DeepEqual(api.reports[1], api.reports[2]) {
		t.Fatal("cleanup not retried exactly")
	}
	runtime.generation = 3
	runtime.observations = []ocihelper.ComputerSeatObservation{{Place: 1, State: "ready"}}
	if err := c.step(t.Context()); err != nil {
		t.Fatal(err)
	}
	if len(runtime.applies) != 3 || len(runtime.applies[2].Seats) != 1 || api.reports[3].Seats[0].SeatID != replacement.SeatID || api.reports[3].Generation != 3 {
		t.Fatalf("replacement %+v %+v", runtime.applies, api.reports)
	}
}
func TestComputerSeatsPollImmediatelyAndEverySecond(t *testing.T) {
	c, api, _ := newSeatTestController()
	clock := newManualClock(time.Unix(100, 0))
	c.clock = clock
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	// The callback runs on the loop's goroutine; synchronize before reading fakes.
	reports := make(chan struct{}, 2)
	c.afterStep = func() { reports <- struct{}{} }
	done := make(chan struct{})
	go func() { defer close(done); c.run(ctx) }()
	awaitSeatTestSignal(t, reports)
	if len(api.reports) != 1 {
		t.Fatal("first observation was delayed")
	}
	clock.Advance(time.Second)
	awaitSeatTestSignal(t, reports)
	if len(api.reports) != 1 || len(c.runtime.(*seatTestRuntime).observes) != 2 {
		t.Fatal("one-second observation missing or unchanged report repeated")
	}
	cancel()
	awaitSeatTestSignal(t, done)
}

type seatLifecycleRuntime struct {
	WorkloadRuntime
	*seatTestRuntime
	run func(context.Context, workloadrunner.Request) (workloadrunner.Result, error)
}

func (r *seatLifecycleRuntime) Run(ctx context.Context, request workloadrunner.Request, _ workloadrunner.OutputSink) (workloadrunner.Result, error) {
	return r.run(ctx, request)
}
func TestComputerSeatsLifecycleStartsAfterRuntimeAndStopsWithAttempt(t *testing.T) {
	c, api, helper := newSeatTestController()
	observed := make(chan struct{}, 1)
	c.afterStep = func() { observed <- struct{}{} }
	started := false
	runtime := &seatLifecycleRuntime{seatTestRuntime: helper}
	runtime.run = func(ctx context.Context, request workloadrunner.Request) (workloadrunner.Result, error) {
		if len(helper.applies) != 0 {
			t.Error("Seat directive before Started")
		}
		request.Started()
		awaitSeatTestSignal(t, observed)
		if !started || len(api.reports) != 1 {
			t.Error("Started callback or observation missing")
		}
		return workloadrunner.Result{}, nil
	}
	_, err := runComputerSeats(t.Context(), runtime, workloadrunner.Request{Started: func() { started = true }}, nil, c, seatComputerServiceConfig{router: newSeatFrontDoor(nil), copyRuntime: copyRuntimeFixture{t.TempDir()}, copyControl: &copyControlFixture{}, publish: func(context.Context, bool, string) error { return nil }})
	if err != nil {
		t.Fatal(err)
	}
	if len(api.reports) != 1 {
		t.Fatal("Seat work continued after runtime returned")
	}
}

// The clock controls the polling assertion; this wall-time bound only prevents
// a broken loop from hanging the package indefinitely.
func awaitSeatTestSignal(t *testing.T, signal <-chan struct{}) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(testwait.Bound):
		t.Fatal("Seat test did not make progress")
	}
}

func TestComputerSeatsUnchangedObservationsDoNotWriteOrApply(t *testing.T) {
	c, api, runtime := newSeatTestController()
	clock := newManualClock(time.Unix(100, 0))
	c.clock = clock
	for i := 0; i < 5; i++ {
		if err := c.step(t.Context()); err != nil {
			t.Fatal(err)
		}
		clock.Advance(time.Second)
	}
	if len(api.reports) != 1 || len(runtime.applies) != 1 || len(runtime.observes) != 5 {
		t.Fatalf("unchanged Seat repeated work: reports=%d applies=%d observes=%d", len(api.reports), len(runtime.applies), len(runtime.observes))
	}
	runtime.observations[0].State = "failed"
	runtime.observations[0].Reason = "readiness_deadline_exceeded"
	if err := c.step(t.Context()); err != nil {
		t.Fatal(err)
	}
	runtime.observations[0].Reason = "cleanup_failed"
	if err := c.step(t.Context()); err != nil {
		t.Fatal(err)
	}
	api.list.Revision = 2 // A revision change still needs a report when the plan is identical.
	if err := c.step(t.Context()); err != nil {
		t.Fatal(err)
	}
	if len(api.reports) != 4 || api.reports[3].ListRevision != 2 || len(runtime.applies) != 1 {
		t.Fatalf("changed observation/revision was suppressed: %+v applies=%d", api.reports, len(runtime.applies))
	}
}

func TestComputerSeatsL1BackoffKeepsDeadlineObservationsAndBoundsLogs(t *testing.T) {
	c, api, runtime := newSeatTestController()
	start := time.Unix(100, 0)
	clock := newManualClock(start)
	c.clock = clock
	var reads, observations, logs []time.Time
	api.onRead = func() { reads = append(reads, clock.Now()) }
	var readyObserved time.Time
	runtime.observations[0].State = "starting"
	runtime.onObserve = func() {
		observations = append(observations, clock.Now())
		if runtime.observations[0].State == "ready" && readyObserved.IsZero() {
			readyObserved = clock.Now()
		}
	}
	c.logf = func(string, ...any) { logs = append(logs, clock.Now()) }
	if err := c.step(t.Context()); err != nil {
		t.Fatal(err)
	}
	api.readError = errors.New("L1 down")
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	stepped := make(chan struct{}, 1)
	c.afterStep = func() { stepped <- struct{}{} }
	done := make(chan struct{})
	go func() { defer close(done); c.run(ctx) }()
	awaitSeatTestSignal(t, stepped)
	for second := 1; second <= 120; second++ {
		if second == 30 {
			runtime.observations[0].State = "ready"
		}
		if second == 96 {
			api.readError = nil
		}
		clock.Advance(time.Second)
		awaitSeatTestSignal(t, stepped)
	}
	cancel()
	awaitSeatTestSignal(t, done)
	expected := []time.Duration{0, 0, 2 * time.Second, 6 * time.Second, 14 * time.Second, 30 * time.Second, 60 * time.Second, 90 * time.Second, 120 * time.Second}
	if len(reads) != len(expected) {
		t.Fatalf("L1 backoff calls=%d want %d", len(reads), len(expected))
	}
	for i, stamp := range reads {
		if stamp.Sub(start) != expected[i] {
			t.Fatalf("read %d at %s want %s", i, stamp.Sub(start), expected[i])
		}
	}
	if len(observations) != 122 || readyObserved.Sub(start) != 30*time.Second || !readyObserved.Before(start.Add(60*time.Second)) {
		t.Fatalf("readiness observation missed deadline: count=%d ready=%s", len(observations), readyObserved.Sub(start))
	}
	for i := 1; i < len(observations); i++ {
		if observations[i].Sub(observations[i-1]) > time.Second {
			t.Fatal("L1 backoff delayed helper observation")
		}
	}
	if len(logs) != 4 {
		t.Fatalf("outage logs=%d want 4", len(logs))
	}
	for i := 1; i < len(logs); i++ {
		if logs[i].Sub(logs[i-1]) < 30*time.Second {
			t.Fatal("outage log rate exceeded")
		}
	}
	if len(api.reports) != 2 || api.reports[1].Seats[0].State != l1.ComputerSeatReady || len(runtime.applies) != 1 {
		t.Fatalf("recovery reports=%+v applies=%d", api.reports, len(runtime.applies))
	}
}

func TestComputerSeatsConflictRebuildsFromFreshObservation(t *testing.T) {
	c, api, runtime := newSeatTestController()
	if err := c.step(t.Context()); err != nil {
		t.Fatal(err)
	} // Acknowledged ready.
	runtime.observations[0].State = "starting"
	api.reportError = &ProtocolError{StatusCode: 409, APIError: contract.APIError{Code: contract.ErrorConflict, Message: "sequence already used"}}
	if err := c.step(t.Context()); err == nil {
		t.Fatal("conflict hidden")
	}
	api.reportError = nil
	runtime.observations[0].State = "ready" // Matches an old acknowledgement, not the conflicted report.
	if err := c.step(t.Context()); err != nil {
		t.Fatal(err)
	}
	if len(api.reports) != 3 || api.reports[2].Sequence <= api.reports[1].Sequence || api.reports[2].Seats[0].State != l1.ComputerSeatReady {
		t.Fatalf("conflict suppressed or retried a stale report: %+v", api.reports)
	}
}

func TestComputerSeatsUncertainApplyCannotRestoreAcknowledgedCleanup(t *testing.T) {
	c, api, runtime := newSeatTestController()
	old2 := l1.WantedComputerSeat{SeatID: "seat_" + strings.Repeat("b", 32), Place: 2}
	api.list.Seats = append(api.list.Seats, old2)
	runtime.observations = append(runtime.observations, ocihelper.ComputerSeatObservation{Place: 2, State: "ready"})
	if err := c.step(t.Context()); err != nil {
		t.Fatal(err)
	}
	fresh1 := l1.WantedComputerSeat{SeatID: "seat_" + strings.Repeat("c", 32), Place: 1}
	fresh2 := l1.WantedComputerSeat{SeatID: "seat_" + strings.Repeat("d", 32), Place: 2}
	api.list = l1.ComputerSeatList{Revision: 2, Seats: []l1.WantedComputerSeat{fresh1, fresh2}}
	runtime.generation = 2
	runtime.observations = []ocihelper.ComputerSeatObservation{{Place: 1, State: "cleaned", CleanupComplete: true}, {Place: 2, State: "removing"}}
	if err := c.step(t.Context()); err != nil {
		t.Fatal(err)
	}
	// Place 1's replacement changes the plan. Its Apply is uncertain while the
	// old generation can now prove place 2's cleanup, which L1 acknowledges.
	runtime.applyError = &ocihelper.RPCError{Code: ocihelper.CodeDiagnosticFailure, Message: "directive publication uncertain"}
	runtime.observations[1] = ocihelper.ComputerSeatObservation{Place: 2, State: "cleaned", CleanupComplete: true}
	if err := c.step(t.Context()); err == nil {
		t.Fatal("uncertain Apply hidden")
	}
	if api.reports[len(api.reports)-1].Seats[0].SeatID != old2.SeatID {
		t.Fatal("place 2 cleanup not reported")
	}
	runtime.applyError = nil
	runtime.generation = 3
	runtime.observations[0] = ocihelper.ComputerSeatObservation{Place: 1, State: "ready"}
	if err := c.step(t.Context()); err != nil {
		t.Fatal(err)
	}
	last := api.reports[len(api.reports)-1]
	if len(last.Seats) != 1 || last.Seats[0].SeatID != fresh1.SeatID {
		t.Fatalf("uncertain Apply restored cleaned binding: %+v", last)
	}
	if len(runtime.applies) != 4 || !reflect.DeepEqual(runtime.applies[2], runtime.applies[3]) {
		t.Fatalf("uncertain directive was changed: %+v", runtime.applies)
	}
}

func TestComputerSeatsRepeatedConflictHasRetryFloor(t *testing.T) {
	c, api, runtime := newSeatTestController()
	clock := newManualClock(time.Unix(100, 0))
	c.clock = clock
	reads := 0
	api.onRead = func() { reads++ }
	if err := c.step(t.Context()); err != nil {
		t.Fatal(err)
	}
	api.readError = &ProtocolError{StatusCode: 409, APIError: contract.APIError{Code: contract.ErrorConflict, Message: "attempt no longer owns the Seat list"}}
	for range 15 {
		_ = c.step(t.Context())
	}
	if reads != 3 {
		t.Fatalf("persistent 409 made %d L1 reads, want initial + two conflicts", reads)
	}
	if len(runtime.observes) != 16 {
		t.Fatalf("backoff stopped helper cleanup observation: %d", len(runtime.observes))
	}
	clock.Advance(2 * time.Second)
	_ = c.step(t.Context())
	if reads != 4 {
		t.Fatalf("conflict was not retried after the floor: %d", reads)
	}
}

func TestComputerSeatsRevisionRaceDoesNotBackOff(t *testing.T) {
	c, api, runtime := newSeatTestController()
	c.clock = newManualClock(time.Unix(100, 0))
	if err := c.step(t.Context()); err != nil {
		t.Fatal(err)
	}
	reads := 0
	api.onRead = func() { reads++ }
	api.readError = &ProtocolError{StatusCode: 409, APIError: contract.APIError{Code: contract.ErrorConflict, Details: map[string]any{"reason": "seat_list_revision_changed"}}}
	for range 8 {
		_ = c.step(t.Context())
	}
	if reads != 8 {
		t.Fatalf("normal revision races backed off after %d reads", reads)
	}
	if len(runtime.observes) != 9 {
		t.Fatal("revision race stopped cleanup observations")
	}
}
