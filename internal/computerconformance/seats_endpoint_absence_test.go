package computerconformance_test

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func seatEndpointAbsenceCommand(ctx context.Context, root, table string, place int) (*exec.Cmd, error) {
	script, err := filepath.Abs("../../examples/computer/seat-endpoint-absence.py")
	if err != nil {
		return nil, err
	}
	return exec.CommandContext(ctx, "python3", script, root, table, fmt.Sprint(place)), nil
}

func TestSeatRemovalProbeRejectsSocketPathsAndUnlinkedKernelSockets(t *testing.T) {
	for _, residue := range []string{"view.ws", "control.ws", "view.sock", "control.sock", "unlinked", "display", "other-seat", "none"} {
		t.Run(residue, func(t *testing.T) {
			root := t.TempDir()
			table := filepath.Join(t.TempDir(), "unix")
			rows := "Num RefCount Protocol Flags Type St Inode Path\n"
			switch residue {
			case "unlinked":
				rows += "0000: 00000002 00000000 00010000 0001 01 123 /wefty/service/seats/2/run/view.ws\n"
			case "display":
				rows += "0000: 00000002 00000000 00010000 0001 01 123 @/tmp/.X11-unix/X2\n"
			case "other-seat":
				rows += "0000: 00000002 00000000 00010000 0001 01 123 /wefty/service/seats/1/run/view.ws\n"
			case "none":
			default:
				path := filepath.Join(root, "wefty/service/seats/2/run", residue)
				if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
					t.Fatal(err)
				}
				// Any leftover path, even a stale file or dangling link, is residue.
				if err := os.Symlink("missing-backend", path); err != nil {
					t.Fatal(err)
				}
			}
			if err := os.WriteFile(table, []byte(rows), 0600); err != nil {
				t.Fatal(err)
			}
			command, err := seatEndpointAbsenceCommand(t.Context(), root, table, 2)
			if err != nil {
				t.Fatal(err)
			}
			out, err := command.CombinedOutput()
			if residue == "none" || residue == "other-seat" {
				if err != nil || strings.TrimSpace(string(out)) != "absent" {
					t.Fatalf("empty Seat refused: %v %s", err, out)
				}
			} else if err == nil {
				t.Fatalf("Seat residue %s reported absent: %s", residue, out)
			}
		})
	}
}
