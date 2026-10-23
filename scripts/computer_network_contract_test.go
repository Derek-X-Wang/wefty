package scripts

import (
	"strings"
	"testing"
)

func TestComputerNetworkNoneContractAndRootOnlyLane(t *testing.T) {
	image := string(mustReadFile(t, "../docs/contracts/computer-image.md"))
	for _, token := range []string{"`network=none`", "loopback only", "AP-1", "AP-2", "AP-3", "No guest proxy", "command-mediated copies"} {
		if !strings.Contains(image, token) {
			t.Errorf("Computer image network contract missing %s", token)
		}
	}
	protocol := string(mustReadFile(t, "../docs/contracts/oci-helper-protocol.md"))
	for _, token := range []string{"`computer_network`", "`computer_loopback_only`", "`none`", "`internet`"} {
		if !strings.Contains(protocol, token) {
			t.Errorf("helper network contract missing %s", token)
		}
	}
	workflow := string(mustReadFile(t, "../.github/workflows/computer-seats.yml"))
	for _, token := range []string{"TestHelperComputerNetworkNoneAP1AP2AP3", "WEFTY_HELPER_NETWORK_EVIDENCE", "helper-network.test", "scripts/computer_network_contract_test.go"} {
		if !strings.Contains(workflow, token) {
			t.Errorf("root-only network lane missing %s", token)
		}
	}
	lane := string(mustReadFile(t, "../runner/ocihelper/computer_network_lane_linux_test.go"))
	if !strings.HasPrefix(lane, "//go:build linux && computer_seat_lane\n") {
		t.Fatal("root-only network proof escaped the Seat lane")
	}
}
