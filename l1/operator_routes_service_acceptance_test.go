//go:build service_acceptance

package l1

import "testing"

func TestServiceAcceptanceOperatorRoutes(t *testing.T) {
	assertServiceOperatorRouteOwnershipListAndProjection(t)
	assertServiceOperatorDesiredStateRestartCapacityAndLogs(t)
}
