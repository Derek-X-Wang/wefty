package tsnet

import (
	"os"
	"path/filepath"
	"testing"
)

func TestSavedParticipantRequiresActualBackendState(t *testing.T) {
	for _, name := range []string{"lock-only", "log-only", "empty", "directory", "link", "public", "private"} {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			if err := os.Chmod(dir, 0700); err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(dir, "tailscaled.state")
			switch name {
			case "lock-only":
				if err := os.WriteFile(filepath.Join(dir, ".participant.lock"), nil, 0600); err != nil {
					t.Fatal(err)
				}
			case "log-only":
				if err := os.WriteFile(filepath.Join(dir, "backend.log"), []byte("diagnostics"), 0600); err != nil {
					t.Fatal(err)
				}
			case "empty":
				if err := os.WriteFile(path, nil, 0600); err != nil {
					t.Fatal(err)
				}
			case "directory":
				if err := os.Mkdir(path, 0700); err != nil {
					t.Fatal(err)
				}
			case "link":
				other := filepath.Join(dir, "other")
				if err := os.WriteFile(other, []byte(`{"saved":"value"}`), 0600); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(other, path); err != nil {
					t.Fatal(err)
				}
			default:
				mode := os.FileMode(0600)
				if name == "public" {
					mode = 0644
				}
				if err := os.WriteFile(path, []byte(`{"saved":"value"}`), mode); err != nil {
					t.Fatal(err)
				}
			}
			if got := HasSavedState(dir); got != (name == "private") {
				t.Fatalf("saved state = %v", got)
			}
		})
	}
}
