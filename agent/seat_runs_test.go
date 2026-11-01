package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/internal/testwait"
	"github.com/Derek-X-Wang/wefty/l1"
	workloadrunner "github.com/Derek-X-Wang/wefty/runner"
	"github.com/Derek-X-Wang/wefty/runner/ocihelper"
)

type seatRunTestControl struct{ calls []string }

func (c *seatRunTestControl) ControlSeatRun(_ context.Context, job, attempt string, r l1.SeatRunControlRequest) (l1.SeatRunCommand, error) {
	c.calls = append(c.calls, r.Phase)
	return l1.SeatRunCommand{Binding: contract.SeatRunBinding{ComputerJobID: "computer-job", ComputerAttemptID: "computer-attempt"}, ComputerSeatAttemptRequest: l1.ComputerSeatAttemptRequest{NodeID: "node", BootSessionID: "boot", FencingToken: "computer-fence"}, Generation: 3, Place: 1}, nil
}

type seatRunTestRuntime struct {
	request ocihelper.ExecComputerSeatRequest
}

func (r *seatRunTestRuntime) ExecComputerSeat(ctx context.Context, request ocihelper.ExecComputerSeatRequest, emit func(ocihelper.SeatCommandEvent) error) error {
	r.request = request
	if err := emit(ocihelper.SeatCommandEvent{Stream: "stdout", Data: []byte("seat-only\n")}); err != nil {
		return err
	}
	code := 7
	return emit(ocihelper.SeatCommandEvent{Exit: &ocihelper.SeatCommandExit{Code: &code}})
}
func TestSeatRunUsesHelperAuthorityAndOrdinaryOutput(t *testing.T) {
	control := &seatRunTestControl{}
	helper := &seatRunTestRuntime{}
	runtime := &seatRunRuntime{control: control, helper: helper, clock: systemClock{}, params: contract.SeatRunParams{Version: 1, SeatRun: contract.SeatRunInput{SeatID: "seat", Argv: []string{"env"}, MaxRuntimeSeconds: 3600}}}
	handoff := t.TempDir()
	var logs []contract.LogEvent
	result, err := runtime.Run(t.Context(), workloadrunner.Request{Authority: workloadrunner.AttemptAuthority{NodeID: "node", BootSessionID: "boot", JobID: "run-job", AttemptID: "run-attempt", FencingToken: "run-fence"}, Execution: contract.ExecutionSpec{HandoffDirectory: handoff, Env: map[string]string{"CALLER_SECRET": "must-not-cross"}}}, workloadrunner.OutputSinkFunc(func(_ context.Context, e contract.LogEvent) error { logs = append(logs, e); return nil }))
	if err != nil || result.Outcome.ExitCode == nil || *result.Outcome.ExitCode != 7 {
		t.Fatalf("result=%+v %v", result, err)
	}
	if helper.request.Authority.AttemptID != "computer-attempt" || helper.request.Authority.FencingToken != "computer-fence" || helper.request.MaxRuntime != time.Hour {
		t.Fatalf("helper=%+v", helper.request)
	}
	if len(logs) != 1 || logs[0].AttemptID != "run-attempt" || string(logs[0].Bytes) != "seat-only\n" {
		t.Fatalf("logs=%+v", logs)
	}
	raw, readErr := os.ReadFile(filepath.Join(handoff, "result.json"))
	if readErr != nil {
		t.Fatal(readErr)
	}
	var document struct {
		ExitCode int `json:"exit_code"`
	}
	if json.Unmarshal(raw, &document) != nil || document.ExitCode != 7 {
		t.Fatalf("result document=%s", raw)
	}
	if len(control.calls) != 2 || control.calls[0] != "start" || control.calls[1] != "finish" {
		t.Fatalf("calls=%v", control.calls)
	}
}

type seatRunBlockingHelper struct{ started, stopped chan struct{} }

func (h *seatRunBlockingHelper) ExecComputerSeat(ctx context.Context, _ ocihelper.ExecComputerSeatRequest, _ func(ocihelper.SeatCommandEvent) error) error {
	close(h.started)
	<-ctx.Done()
	close(h.stopped)
	return ctx.Err()
}

type seatRunBlockingPoll struct{ entered chan struct{} }

func (c *seatRunBlockingPoll) ControlSeatRun(ctx context.Context, _, _ string, r l1.SeatRunControlRequest) (l1.SeatRunCommand, error) {
	if r.Phase == "poll" {
		close(c.entered)
		<-ctx.Done()
		return l1.SeatRunCommand{}, ctx.Err()
	}
	return l1.SeatRunCommand{}, nil
}
func TestSeatRunDeadlineDoesNotWaitForAuthorityPoll(t *testing.T) {
	clock := newManualClock(time.Unix(0, 0))
	control := &seatRunBlockingPoll{entered: make(chan struct{})}
	helper := &seatRunBlockingHelper{started: make(chan struct{}), stopped: make(chan struct{})}
	runtime := &seatRunRuntime{control: control, helper: helper, clock: clock, params: contract.SeatRunParams{Version: 1, SeatRun: contract.SeatRunInput{Argv: []string{"sleep"}, MaxRuntimeSeconds: 3600}}}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan contract.ProcessResult, 1)
	go func() { result, _ := runtime.Run(ctx, workloadrunner.Request{}, nil); done <- result.Outcome }()
	<-helper.started
	clock.advanceArmedTimer(time.Second)
	<-control.entered
	clock.advanceArmedTimer(3599 * time.Second)
	select {
	case <-helper.stopped:
	case <-time.After(testwait.Bound):
		t.Fatal("deadline waited for an outstanding L1 poll")
	}
	result := <-done
	if result.ExitCode != nil || result.LimitReached != contract.LimitReachedMaxRuntime {
		t.Fatalf("deadline=%+v", result)
	}
}

type seatRunLifecycleHelper struct {
	captureRuntime
	seatRunTestRuntime
}

func TestSeatRunLifecycleNeverFallsBackToHostExecution(t *testing.T) {
	for _, helperAvailable := range []bool{false, true} {
		t.Run(fmt.Sprint(helperAvailable), func(t *testing.T) {
			host := &captureRuntime{}
			runtimes := workloadRuntimeSet{contract.JobKindProcess: host}
			if helperAvailable {
				runtimes[contract.JobKindOCI] = &seatRunLifecycleHelper{}
			}
			lifecycle := newAttemptLifecycle(attemptLifecycleDependencies{runtimes: runtimes, clock: systemClock{}, nodeID: "node", bootSessionID: "boot"})
			p := contract.SeatRunParams{Version: 1, SeatRun: contract.SeatRunInput{SeatID: "seat_00000000000000000000000000000001", Argv: []string{"env"}}, Binding: contract.SeatRunBinding{SeatID: "seat_00000000000000000000000000000001", NodeID: "node", ComputerID: "computer", ComputerJobID: "computer-job", ComputerAttemptID: "computer-attempt"}}
			raw, err := json.Marshal(p)
			if err != nil {
				t.Fatal(err)
			}
			claim := l1.Claim{SubmittedByRunLedger: true, Job: l1.Job{JobID: "job", Spec: contract.JobSpec{Kind: contract.JobKindProcess, Class: contract.JobClassOneShot, Labels: map[string]string{contract.LabelRunParams: string(raw)}}}, Lease: l1.AttemptLease{AttemptID: "attempt", FencingToken: "fence"}}
			result, err := lifecycle.runWorkload(t.Context(), claim)
			if err == nil || result.SpawnError == nil || result.SpawnError.Code != contract.SpawnFailureRuntimeUnavailable {
				t.Fatalf("missing Seat dependencies=%+v %v", result, err)
			}
			if host.request.Authority.AttemptID != "" {
				t.Fatal("Seat command reached the host process runtime")
			}
		})
	}
}

type seatRunTransientControl struct{ seatRunTestControl }

func (c *seatRunTransientControl) ControlSeatRun(ctx context.Context, job, attempt string, r l1.SeatRunControlRequest) (l1.SeatRunCommand, error) {
	if r.Phase == "poll" {
		return l1.SeatRunCommand{}, &ProtocolError{StatusCode: 503, APIError: contract.APIError{Code: contract.ErrorUnavailable, Retryable: true, Message: "L1 restarting"}}
	}
	return c.seatRunTestControl.ControlSeatRun(ctx, job, attempt, r)
}
func TestSeatRunTransientPollUsesExistingAttemptLease(t *testing.T) {
	clock := &seatLeaseTestClock{manualClock: newManualClock(time.Unix(0, 0)), scheduled: make(chan time.Duration, 8)}
	helper := &seatRunBlockingHelper{started: make(chan struct{}), stopped: make(chan struct{})}
	runtime := &seatRunRuntime{control: &seatRunTransientControl{}, helper: helper, clock: clock, params: contract.SeatRunParams{Version: 1, SeatRun: contract.SeatRunInput{Argv: []string{"sleep"}, MaxRuntimeSeconds: 3600}}}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan struct{})
	go func() { defer close(done); _, _ = runtime.Run(ctx, workloadrunner.Request{}, nil) }()
	<-helper.started
	awaitSeatLeaseSchedule(t, clock, done)
	awaitSeatLeaseSchedule(t, clock, done)
	clock.Advance(time.Second)
	if next := awaitSeatLeaseSchedule(t, clock, done); next != time.Second {
		t.Fatalf("retry=%s", next)
	}
	cancel()
	<-done
}

type seatRunRefusingHelper struct{}

func (seatRunRefusingHelper) ExecComputerSeat(context.Context, ocihelper.ExecComputerSeatRequest, func(ocihelper.SeatCommandEvent) error) error {
	return &ocihelper.RPCError{Code: ocihelper.CodeSeatOperationRefused, Message: "Seat is not ready"}
}
func TestSeatRunHelperRefusalDoesNotAcknowledgePayloadStart(t *testing.T) {
	runtime := &seatRunRuntime{control: &seatRunTestControl{}, helper: seatRunRefusingHelper{}, clock: systemClock{}, params: contract.SeatRunParams{Version: 1, SeatRun: contract.SeatRunInput{Argv: []string{"true"}, MaxRuntimeSeconds: 3600}}}
	started := false
	result, err := runtime.Run(t.Context(), workloadrunner.Request{Started: func() { started = true }}, nil)
	if err == nil || result.Outcome.ExitCode != nil || started {
		t.Fatalf("refused command: started=%v result=%+v err=%v", started, result, err)
	}
}
