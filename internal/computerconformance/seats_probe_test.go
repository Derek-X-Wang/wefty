package computerconformance_test

import (
	"context"
	"fmt"
	"os/exec"
	"path/filepath"
	"slices"
	"testing"
)

// Choose the endpoint here so a caller cannot accidentally test control input
// against the view-only endpoint. Both paths stay beneath the exact task root.
func seatScreenProbeCommand(ctx context.Context, pid uint32, place int, mode, colour string) (*exec.Cmd, error) {
	script, err := filepath.Abs("../../examples/computer/seat-probe.py")
	if err != nil {
		return nil, err
	}
	kind := "view"
	if mode == "input" {
		kind = "control"
	}
	endpoint := fmt.Sprintf("/proc/%d/root/wefty/service/seats/%d/run/%s.ws", pid, place, kind)
	return exec.CommandContext(ctx, "python3", script, mode, endpoint, colour), nil
}

func TestSeatProbeCommandsUseControlOnlyForControlInput(t *testing.T) {
	for _, mode := range []string{"input", "view-input", "frame"} {
		for _, place := range []int{1, 2} {
			command, err := seatScreenProbeCommand(context.Background(), 123, place, mode, "ff0000")
			if err != nil {
				t.Fatal(err)
			}
			kind := "view"
			if mode == "input" {
				kind = "control"
			}
			endpoint := fmt.Sprintf("/proc/123/root/wefty/service/seats/%d/run/%s.ws", place, kind)
			if len(command.Args) != 5 || command.Args[0] != "python3" || filepath.Base(command.Args[1]) != "seat-probe.py" || !slices.Equal(command.Args[2:], []string{mode, endpoint, "ff0000"}) {
				t.Fatalf("wrong private endpoint: %q", command.Args)
			}
		}
	}
}
