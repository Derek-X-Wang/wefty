package agent

import (
	"context"
	"encoding/json"
	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/l1"
	workloadrunner "github.com/Derek-X-Wang/wefty/runner"
	"testing"
)

type directoryRunControl struct{ seatRunTestControl }

func (c *directoryRunControl) ControlSeatRun(ctx context.Context, job, attempt string, r l1.SeatRunControlRequest) (l1.SeatRunCommand, error) {
	reply, err := c.seatRunTestControl.ControlSeatRun(ctx, job, attempt, r)
	raw, _ := json.Marshal(reply)
	var fields map[string]any
	_ = json.Unmarshal(raw, &fields)
	fields["working_directory"] = "work/pkg"
	raw, _ = json.Marshal(fields)
	_ = json.Unmarshal(raw, &reply)
	return reply, err
}
func TestSeatRunWorkingDirectoryReachesHelper(t *testing.T) {
	helper := &seatRunTestRuntime{}
	runtime := &seatRunRuntime{control: &directoryRunControl{}, helper: helper, clock: systemClock{}, params: contract.SeatRunParams{SeatRun: contract.SeatRunInput{Argv: []string{"pwd"}, MaxRuntimeSeconds: 60}}}
	if _, err := runtime.Run(t.Context(), workloadrunner.Request{}, nil); err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(helper.request)
	var got map[string]any
	_ = json.Unmarshal(raw, &got)
	if got["working_directory"] != "work/pkg" {
		t.Fatalf("helper folder: %s", raw)
	}
}
