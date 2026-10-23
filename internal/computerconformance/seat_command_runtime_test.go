package computerconformance

import (
	"errors"
	"os"
	"slices"
	"strings"
	"testing"
)

func TestSeatCommandRuntimeRequiresAbsoluteIsolatedPython(t *testing.T) {
	var argv []string
	missing := func(args ...string) error { argv = slices.Clone(args); return os.ErrNotExist }
	err := CheckSeatCommandRuntime(missing)
	if err == nil || !errors.Is(err, os.ErrNotExist) || !strings.Contains(err.Error(), "/usr/bin/python3") {
		t.Fatalf("missing runtime accepted or unclear reason: %v", err)
	}
	if len(argv) != 5 || !slices.Equal(argv[:4], []string{"/usr/bin/python3", "-I", "-S", "-c"}) {
		t.Fatalf("unisolated or PATH-dependent probe: %v", argv)
	}
	if err := CheckSeatCommandRuntime(func(args ...string) error { return nil }); err != nil {
		t.Fatal(err)
	}
}
func TestSeatReportRefusesMissingCommandRuntime(t *testing.T) {
	for _, observed := range []bool{false, true} {
		evidence := SeatEvidence{Version: 1, Image: "example.invalid/seats", Observations: map[string]bool{}}
		if observed {
			evidence.Observations["seat_command_python"] = false
		}
		report := CheckSeats(evidence)
		found := false
		for _, check := range report.Checks {
			if check.ID != "seats.command-runtime" {
				continue
			}
			found = true
			if observed && (check.Status != StatusFail || !strings.Contains(check.Detail, "/usr/bin/python3")) {
				t.Fatalf("missing interpreter not refused clearly: %+v", check)
			}
			if !observed && check.Status != StatusNotRun {
				t.Fatalf("missing evidence promoted: %+v", check)
			}
		}
		if !found {
			t.Fatal("no command runtime conformance check")
		}
	}
}
