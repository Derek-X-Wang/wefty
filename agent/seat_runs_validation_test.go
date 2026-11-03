//go:build darwin || linux

package agent

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	workloadrunner "github.com/Derek-X-Wang/wefty/runner"
	"github.com/Derek-X-Wang/wefty/runner/ocihelper"
)

// Use the real helper dispatch boundary without starting a container. A valid
// request reaches attempt authorization; an incomplete authority is rejected
// earlier as invalid_request.
type seatRunValidatingHelper struct {
	session *ocihelper.Session
	seatRunTestRuntime
}

func (h *seatRunValidatingHelper) ExecComputerSeat(ctx context.Context, request ocihelper.ExecComputerSeatRequest, emit func(ocihelper.SeatCommandEvent) error) error {
	err := h.session.ExecComputerSeat(ctx, request, nil)
	var rpc *ocihelper.RPCError
	if !errors.As(err, &rpc) || rpc.Code != ocihelper.CodeAttemptOutsideSession {
		return err
	}
	return h.seatRunTestRuntime.ExecComputerSeat(ctx, request, emit)
}

func TestSeatRunRequestPassesRealHelperValidation(t *testing.T) {
	clock := hostedHelperClock{newManualClock(time.Now())}
	barrier, stop := startPreAdmissionHelper(t, newPreAdmissionRenewalEngine(), clock.Now, clock)
	defer stop()
	if err := barrier.Ensure(t.Context()); err != nil {
		t.Fatal(err)
	}
	session, err := barrier.Session()
	if err != nil {
		t.Fatal(err)
	}
	helper := &seatRunValidatingHelper{session: session}
	runtime := &seatRunRuntime{control: &seatRunTestControl{}, helper: helper, clock: systemClock{}, params: contract.SeatRunParams{SeatRun: contract.SeatRunInput{Argv: []string{"id"}, MaxRuntimeSeconds: 60}}}
	result, err := runtime.Run(t.Context(), workloadrunner.Request{}, nil)
	if err != nil || result.Outcome.ExitCode == nil || *result.Outcome.ExitCode != 7 {
		t.Fatalf("agent Seat command failed real helper validation: result=%+v err=%v", result, err)
	}
}
