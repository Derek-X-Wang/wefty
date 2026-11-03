//go:build darwin || linux

package agent

import (
	"testing"

	"github.com/Derek-X-Wang/wefty/internal/testevidence"
)

// capturedAgentConfig preserves a caller's logger while retaining failed-row evidence.
func capturedAgentConfig(t *testing.T, config Config) Config {
	t.Helper()
	config.Logf = testevidence.CaptureLogf(t, config.Logf)
	return config
}
