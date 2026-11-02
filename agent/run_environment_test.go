package agent

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Clear the outer job's context before a runner captures os.Environ(). Tests
// must set their own job context after this call; keep the temp sweep isolated.
// Setenv registers restoration with the test; Unsetenv also makes shell
// ${NAME-unset} checks see true absence.
func clearInheritedWeftyEnvironment(t *testing.T) {
	t.Helper()
	for _, entry := range os.Environ() {
		name, _, _ := strings.Cut(entry, "=")
		if !strings.HasPrefix(name, "WEFTY_") || name == "WEFTY_EXEC_TEMP_ROOT" {
			continue
		}
		t.Setenv(name, "")
		if err := os.Unsetenv(name); err != nil {
			t.Fatalf("unset %s: %v", name, err)
		}
	}
}

func TestClearInheritedWeftyEnvironmentKeepsTempRoot(t *testing.T) {
	root := os.Getenv("WEFTY_EXEC_TEMP_ROOT")
	if !filepath.IsAbs(root) {
		t.Fatalf("package temp root is not absolute: %q", root)
	}
	clearInheritedWeftyEnvironment(t)
	if got := os.Getenv("WEFTY_EXEC_TEMP_ROOT"); got != root {
		t.Fatalf("temp isolation lost: got %q, want %q", got, root)
	}
}
