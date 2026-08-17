//go:build service_acceptance

package l1

import "testing"

func TestServiceAcceptanceHeartbeatNodeChannel(t *testing.T) {
	assertHeartbeatIsTheNodeDirectiveAndCapacityChannel(t)
}
