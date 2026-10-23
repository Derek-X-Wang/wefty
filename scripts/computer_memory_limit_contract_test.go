package scripts

import (
	"strings"
	"testing"
)

func TestComputerMemoryLimitRM5LaneAndContract(t *testing.T) {
	workflow := string(mustReadFile(t, "../.github/workflows/computer-seats.yml"))
	for _, required := range []string{"TestHelperComputerMemoryRM5", "WEFTY_HELPER_MEMORY_EVIDENCE", "-tags computer_seat_lane", "sudo env PATH="} {
		if !strings.Contains(workflow, required) {
			t.Fatalf("RM-5 lane missing %s", required)
		}
	}
	test := string(mustReadFile(t, "../runner/ocihelper/computer_memory_limit_lane_linux_test.go"))
	if !strings.HasPrefix(test, "//go:build linux && computer_seat_lane\n") {
		t.Fatal("root-only RM-5 test escaped its dedicated lane")
	}
	for _, required := range []string{"cgroup.freeze", "cgroup.events", "960<<20", "grown.FreezeRequests != 0", "held.FreezeRequests != 0"} {
		if !strings.Contains(test, required) {
			t.Fatalf("real RM-5 freezer coverage missing %s", required)
		}
	}
	doc := string(mustReadFile(t, "../docs/contracts/oci-helper-protocol.md"))
	for _, required := range []string{"`SetComputerMemoryLimit`", "`held_memory_bytes`", "`tmpfs_floor`", "`memory_in_use`", "twice the creation-time total", "`memory_thaw_failed`", "`recovered_freeze`", "`freeze_requests`", "1.5 seconds", "500 ms"} {
		if !strings.Contains(doc, required) {
			t.Fatalf("memory operation contract missing %s", required)
		}
	}
}
