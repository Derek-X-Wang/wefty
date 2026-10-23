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
// against the view-only endpoint. The directive assigns adjacent port pairs.
func seatScreenProbeCommand(ctx context.Context, pid uint32, place int, mode, colour string) (*exec.Cmd, error) {
	script, err := filepath.Abs("../../examples/computer/seat-probe.py")
	if err != nil {
		return nil, err
	}
	port := 7100 + place*2
	if mode == "input" {
		port++
	}
	return exec.CommandContext(ctx, "nsenter", "-t", fmt.Sprint(pid), "-n", "python3", script, mode, fmt.Sprint(port), colour), nil
}

func TestSeatProbeCommandsUseControlOnlyForControlInput(t *testing.T) {
	for _, tc := range []struct {
		mode  string
		place int
		port  string
	}{
		{"input", 1, "7103"},
		{"view-input", 1, "7102"},
		{"frame", 1, "7102"},
		{"input", 2, "7105"},
		{"view-input", 2, "7104"},
		{"frame", 2, "7104"},
	} {
		t.Run(fmt.Sprintf("%s/seat%d", tc.mode, tc.place), func(t *testing.T) {
			command, err := seatScreenProbeCommand(context.Background(), 123, tc.place, tc.mode, "ff0000")
			if err != nil {
				t.Fatal(err)
			}
			want := []string{"-t", "123", "-n", "python3"}
			if !slices.Equal(command.Args[1:5], want) || filepath.Base(command.Args[5]) != "seat-probe.py" || !slices.Equal(command.Args[6:], []string{tc.mode, tc.port, "ff0000"}) {
				t.Fatalf("wrong screen/input probe command: %q", command.Args)
			}
		})
	}
}
