package agent

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/l1"
	workloadrunner "github.com/Derek-X-Wang/wefty/runner"
	"github.com/Derek-X-Wang/wefty/runner/ocihelper"
)

type seatTestControl struct {
	list        l1.ComputerSeatList
	reports     []l1.ComputerSeatObservationReport
	reportError error
	readError   error
}

func (f *seatTestControl) GetComputerSeatList(_ context.Context, _, _, _ string, _ l1.ComputerSeatAttemptRequest) (l1.ComputerSeatList, error) {
	return f.list, f.readError
}
func (f *seatTestControl) ReportComputerSeats(_ context.Context, _, _, _ string, r l1.ComputerSeatObservationReport) (l1.ComputerSeatObservationReceipt, error) {
	f.reports = append(f.reports, r)
	return l1.ComputerSeatObservationReceipt{Accepted: f.reportError == nil}, f.reportError
}

type seatTestRuntime struct {
	applies      []ocihelper.ApplyComputerSeatsRequest
	applyError   error
	observeError error
	observations []ocihelper.ComputerSeatObservation
	observes     []ocihelper.ObserveComputerSeatsRequest
	generation   uint64
}

func (f *seatTestRuntime) ApplyComputerSeats(_ context.Context, r ocihelper.ApplyComputerSeatsRequest) (ocihelper.ApplyComputerSeatsResponse, error) {
	f.applies = append(f.applies, r)
	return ocihelper.ApplyComputerSeatsResponse{Generation: f.generation}, f.applyError
}
func (f *seatTestRuntime) ObserveComputerSeats(_ context.Context, r ocihelper.ObserveComputerSeatsRequest) (ocihelper.ObserveComputerSeatsResponse, error) {
	f.observes = append(f.observes, r)
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
	if err := c.step(t.Context()); err != nil {
		t.Fatal(err)
	}
	if len(runtime.observes) != 3 || len(runtime.applies[2].Seats) != 0 || !reflect.DeepEqual(api.reports[1], api.reports[2]) {
		t.Fatal("cleanup not retried exactly")
	}
	runtime.generation = 3
	runtime.observations = []ocihelper.ComputerSeatObservation{{Place: 1, State: "ready"}}
	if err := c.step(t.Context()); err != nil {
		t.Fatal(err)
	}
	if len(runtime.applies[3].Seats) != 1 || api.reports[3].Seats[0].SeatID != replacement.SeatID || api.reports[3].Generation != 3 {
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
	if len(api.reports) != 2 {
		t.Fatal("one-second poll missing")
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
	_, err := runComputerSeats(t.Context(), runtime, workloadrunner.Request{Started: func() { started = true }}, nil, c)
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
	case <-time.After(5 * time.Second):
		t.Fatal("Seat test did not make progress")
	}
}
