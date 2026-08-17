package agent

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
)

// DefaultManagedRootDirectory returns the platform state root. Service data is
// persistent state, unlike the agent's loss-tolerant transport spool cache.
func DefaultManagedRootDirectory() (string, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("agent: locate user home for managed service root: %w", err)
	}
	return managedRootDirectory(runtime.GOOS, home, os.Getenv)
}

func managedRootDirectory(goos, home string, getenv func(string) string) (string, error) {
	switch goos {
	case "darwin":
		return filepath.Join(home, "Library", "Application Support", "wefty"), nil
	case "linux":
		stateHome := getenv("XDG_STATE_HOME")
		if stateHome == "" {
			stateHome = filepath.Join(home, ".local", "state")
		} else if !filepath.IsAbs(stateHome) {
			return "", fmt.Errorf("agent: XDG_STATE_HOME must be an absolute path")
		}
		return filepath.Join(stateHome, "wefty"), nil
	default:
		return "", fmt.Errorf("agent: managed service root is unsupported on %s", goos)
	}
}
