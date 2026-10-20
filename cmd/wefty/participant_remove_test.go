package main

import (
	"errors"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/fabric/participantstate"
	"os"
	"path/filepath"
	"testing"
)

func TestRemoveKeepsParticipantStateAndPurgeDeletesOnlyParticipantFolders(t *testing.T) {
	for _, purge := range []bool{false, true} {
		t.Run(map[bool]string{false: "remove", true: "purge"}[purge], func(t *testing.T) {
			f := newSetupFixture(t)
			f.setup(t, setupRequest{})
			paths, err := resolveSetupPaths(f.home)
			if err != nil {
				t.Fatal(err)
			}
			for _, role := range []string{"node-agent", "client", "person", "l1", "l3", "foreign"} {
				folder := filepath.Join(paths.data, "fabric", role)
				if err := os.MkdirAll(folder, 0o700); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(folder, "saved-state"), []byte("opaque-state"), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := runSetupRemove(t.Context(), f.host, purge); err != nil {
				t.Fatal(err)
			}
			for _, role := range []string{"node-agent", "client", "person", "l1", "l3"} {
				_, err := os.Stat(filepath.Join(paths.data, "fabric", role, "saved-state"))
				if purge && !os.IsNotExist(err) || !purge && err != nil {
					t.Fatalf("purge=%v role=%s err=%v", purge, role, err)
				}
			}
			if _, err := os.Stat(filepath.Join(paths.data, "fabric", "foreign", "saved-state")); err != nil {
				t.Fatal("removed foreign Fabric state:", err)
			}
		})
	}
}

func TestPurgeRefusesAnOwnedParticipantFolder(t *testing.T) {
	f := newSetupFixture(t)
	paths, err := resolveSetupPaths(f.home)
	if err != nil {
		t.Fatal(err)
	}
	state := filepath.Join(paths.data, "fabric", "client")
	release, err := participantstate.Acquire(state, "client")
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	marker := filepath.Join(state, "saved-state")
	if err := os.WriteFile(marker, []byte("saved"), 0o600); err != nil {
		t.Fatal(err)
	}
	root, err := os.OpenRoot(paths.home)
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	err = purgeSetupParticipants(root, paths, &setupRemoveReport{})
	var inUse *fabric.ParticipantInUseError
	if !errors.As(err, &inUse) {
		t.Fatalf("purge did not refuse owner: %v", err)
	}
	if _, err := os.Stat(marker); err != nil {
		t.Fatalf("purge deleted owned state: %v", err)
	}
}
