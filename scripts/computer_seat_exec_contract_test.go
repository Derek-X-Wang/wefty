package scripts

import (
	"strings"
	"testing"
)

func TestHelperSeatExecContractAndRootOnlyLane(t *testing.T) {
	protocol := string(mustReadFile(t, "../docs/contracts/oci-helper-protocol.md"))
	for _, token := range []string{"`ExecComputerSeat`", "`seat_not_ready`", "`generation_mismatch`", "4096", "8192", "/usr/bin/python3", "new session", "max_runtime"} {
		if !strings.Contains(protocol, token) {
			t.Errorf("Seat command contract missing %s", token)
		}
	}
	image := string(mustReadFile(t, "../docs/contracts/computer-image.md"))
	if !strings.Contains(image, "Every multi-Seat image must provide executable `/usr/bin/python3`") || !strings.Contains(image, "seat_command_python") {
		t.Error("image runtime requirement or its conformance evidence missing")
	}
	workflow := string(mustReadFile(t, "../.github/workflows/computer-seats.yml"))
	for _, token := range []string{"TestHelperComputerSeatExecIM3", "WEFTY_HELPER_SEAT_EXEC_EVIDENCE", "helper-seat-exec.test"} {
		if !strings.Contains(workflow, token) {
			t.Errorf("root lane missing %s", token)
		}
	}
	test := string(mustReadFile(t, "../runner/ocihelper/computer_seat_exec_lane_linux_test.go"))
	if !strings.HasPrefix(test, "//go:build linux && computer_seat_lane\n") {
		t.Fatal("IM-3 root test escaped Seat lane")
	}
}
