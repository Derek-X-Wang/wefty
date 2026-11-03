//go:build service_acceptance_realtiming && linux

package ocihelper_test

import (
	"testing"

	"github.com/Derek-X-Wang/wefty/agent"
	"github.com/Derek-X-Wang/wefty/internal/testevidence"
)

// capturedAgentConfig preserves a caller's logger while retaining failed-row evidence.
func capturedAgentConfig(t *testing.T, config agent.Config) agent.Config {
	t.Helper()
	config.Logf = testevidence.CaptureLogf(t, config.Logf)
	return config
}
