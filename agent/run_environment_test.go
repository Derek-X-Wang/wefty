package agent

import (
	"os"
	"strings"
	"testing"
)

// Clear the outer job's context before a runner captures os.Environ(). Tests
// must set their own WEFTY_* values after this call. Setenv registers restoration
// with the test; Unsetenv also makes shell ${NAME-unset} checks see true absence.
func clearInheritedWeftyEnvironment(t *testing.T) {
	t.Helper()
	for _, entry := range os.Environ() {
		name, _, _ := strings.Cut(entry, "=")
		if !strings.HasPrefix(name, "WEFTY_") {
			continue
		}
		t.Setenv(name, "")
		if err := os.Unsetenv(name); err != nil {
			t.Fatalf("unset %s: %v", name, err)
		}
	}
}
