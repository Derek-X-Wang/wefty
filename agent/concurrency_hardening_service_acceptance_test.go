//go:build service_acceptance && (darwin || linux)

package agent

import "testing"

func TestServiceAcceptanceSharedDependencyConcurrency(t *testing.T) {
	t.Run("handoff path is excluded through finish", assertHandoffPathLockSpansPrepareThroughFinish)
	t.Run("output sink factories isolate concurrent attempts", assertOutputSinkFactorySupportsConcurrentAttemptIsolation)
	t.Run("configured logging is serialized", assertSerialLogfContainsConcurrentCallers)
	t.Run("HTTP idle pool supports concurrent attempt traffic", assertClientTransportSupportsConcurrentAttemptTraffic)
}
