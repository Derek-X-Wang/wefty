//go:build service_acceptance && (darwin || linux)

package agent

import "testing"

func TestServiceAcceptanceConnectHostComesFromFabric(t *testing.T) {
	assertAgentRegistrationUsesFabricConnectHost(t)
}
