//go:build darwin || linux

package managedroot

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func TestProcessInventoryDurableAcrossBoots(t *testing.T) {
	root := syntheticRoot(t)
	config := Config{Root: root, NodeID: "node", BootSessionID: "first", HostBootIdentity: func() (string, error) { return "darwin:1:0:00000000-0000-0000-0000-000000000001", nil }}
	m, err := Initialize(config)
	if err != nil {
		t.Fatal(err)
	}
	paths, err := m.CreateService("job", 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := m.RecordProcessAttempt("job", "attempt", 0); err != nil {
		t.Fatal(err)
	}
	records, err := m.ProcessAttempts("job")
	if err != nil || len(records) != 1 || records[0].ProcessGroupID != 0 {
		t.Fatalf("pending inventory=%+v err=%v", records, err)
	}
	if err := m.RecordProcessAttempt("job", "attempt", 12345); err != nil {
		t.Fatal(err)
	}
	config.BootSessionID = "second"
	m2, err := Open(config)
	if err != nil {
		t.Fatal(err)
	}
	records, err = m2.ProcessAttempts("job")
	if err != nil || len(records) != 1 || records[0].ProcessGroupID != 12345 || records[0].BootSessionID != "first" || records[0].HostBootID != "darwin:1:0:00000000-0000-0000-0000-000000000001" {
		t.Fatalf("reopened inventory=%+v err=%v", records, err)
	}
	if err := m.RecordProcessAttempt("job", "stale", 12346); !errors.Is(err, ErrStaleBootSession) {
		t.Fatalf("stale writer err=%v", err)
	}
	if err := m2.RecordProcessAttempt("job", "second-attempt", 12346); err != nil {
		t.Fatal(err)
	}
	config.BootSessionID = "third"
	m3, err := Open(config)
	if err != nil {
		t.Fatal(err)
	}
	records, err = m3.ProcessAttempts("job")
	if err != nil || len(records) != 2 {
		t.Fatalf("multi-boot inventory=%+v err=%v", records, err)
	}
	data, err := os.ReadFile(filepath.Join(paths.Root, OwnershipManifestName))
	if err != nil {
		t.Fatal(err)
	}
	var manifest OwnershipManifest
	if err := json.Unmarshal(data, &manifest); err != nil {
		t.Fatal(err)
	}
	if manifest.Version != 2 || manifest.LegacyProcessInventory {
		t.Fatalf("manifest=%+v", manifest)
	}
}

func TestProcessInventoryLegacyAndConflictsFailClosed(t *testing.T) {
	fixture := newFixture(t, "boot", "job", 1)
	fixture.manager.hostBootIdentity = func() (string, error) { return "darwin:1:0:00000000-0000-0000-0000-000000000001", nil }
	// Model a pre-#871 ownership record, with no inventory.
	path := filepath.Join(fixture.paths.Root, OwnershipManifestName)
	data, _ := json.Marshal(OwnershipManifest{Version: 1, JobID: "job", RemovalGeneration: 1})
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.manager.ProcessAttempts("job"); !errors.Is(err, ErrProcessTreeNotReaped) {
		t.Fatalf("legacy err=%v", err)
	}
	if err := fixture.manager.RecordProcessAttempt("job", "attempt", 12345); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.manager.ProcessAttempts("job"); !errors.Is(err, ErrProcessTreeNotReaped) {
		t.Fatalf("legacy promotion erased uncertainty: %v", err)
	}
	if err := fixture.manager.RecordProcessAttempt("job", "attempt", 12346); !errors.Is(err, ErrOwnershipManifest) {
		t.Fatalf("conflicting group err=%v", err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var manifest OwnershipManifest
	if err := json.Unmarshal(data, &manifest); err != nil {
		t.Fatal(err)
	}
	if manifest.Version != 2 || !manifest.LegacyProcessInventory {
		t.Fatalf("manifest=%+v", manifest)
	}
}

func TestLegacyProcessInventoryResolvedOnlyByReboot(t *testing.T) {
	for _, promoted := range []bool{false, true} {
		t.Run(fmt.Sprint(promoted), func(t *testing.T) {
			fixture := newFixture(t, "first", "job", 1)
			host := "linux:00000000-0000-0000-0000-000000000001"
			fixture.manager.hostBootIdentity = func() (string, error) { return host, nil }
			path := filepath.Join(fixture.paths.Root, OwnershipManifestName)
			data, _ := json.Marshal(OwnershipManifest{Version: 1, JobID: "job", RemovalGeneration: 1})
			if err := os.WriteFile(path, data, 0600); err != nil {
				t.Fatal(err)
			}
			if promoted {
				if err := fixture.manager.RecordProcessAttempt("job", "attempt", 12345); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := fixture.manager.ProcessAttempts("job"); !errors.Is(err, ErrProcessTreeNotReaped) {
				t.Fatalf("same boot err=%v", err)
			}
			// Opening under another agent boot must retain the originally observed host.
			config := Config{Root: fixture.manager.root, NodeID: fixture.manager.nodeID, BootSessionID: "second", HostBootIdentity: func() (string, error) { return host, nil }}
			manager, err := Open(config)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := manager.ProcessAttempts("job"); !errors.Is(err, ErrProcessTreeNotReaped) {
				t.Fatalf("agent restart err=%v", err)
			}
			host = "linux:00000000-0000-0000-0000-000000000002"
			records, err := manager.ProcessAttempts("job")
			if err != nil || len(records) == 0 || records[0].HostBootID == host {
				t.Fatalf("reboot inventory=%+v err=%v", records, err)
			}
		})
	}
}

func TestProcessInventoryIgnoresUnacknowledgedTempFiles(t *testing.T) {
	fixture := newFixture(t, "boot", "job", 1)
	fixture.manager.hostBootIdentity = func() (string, error) { return "linux:00000000-0000-0000-0000-000000000001", nil }
	if err := fixture.manager.RecordProcessAttempt("job", "attempt", 12345); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(fixture.paths.Root, processAttemptsDirectory, ".tmp-crash"), []byte("partial"), 0600); err != nil {
		t.Fatal(err)
	}
	records, err := fixture.manager.ProcessAttempts("job")
	if err != nil || len(records) != 1 || records[0].ProcessGroupID != 12345 {
		t.Fatalf("inventory=%+v err=%v", records, err)
	}
}
