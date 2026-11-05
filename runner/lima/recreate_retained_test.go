package lima

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"

	"github.com/Derek-X-Wang/wefty/runner/ocihelper"
)

func recreateServiceRecord(t *testing.T) (ocihelper.ResourceIdentity, any) {
	t.Helper()
	authority := ocihelper.AttemptAuthority{NodeID: "node", JobID: "job_stopped_service", AttemptID: "attempt_stopped",
		FencingToken: "fence", BootSessionID: "boot", Class: "service", RemovalGeneration: "generation"}
	resources, err := ocihelper.DeterministicResourceIdentity(authority)
	if err != nil {
		t.Fatal(err)
	}
	return resources, map[string]any{"version": 1, "authority": authority, "resources": resources}
}

func mustRecreateJSON(t *testing.T, f *recreateFixture, path string, record any) {
	t.Helper()
	payload, err := json.Marshal(record)
	if err != nil {
		t.Fatal(err)
	}
	mustRecreateFile(t, f.root, path)
	if err := os.WriteFile(filepath.Join(f.root, path), payload, 0o600); err != nil {
		t.Fatal(err)
	}
}

func recreateCustodyRecord(phase string) (string, any) {
	digest := sha256.Sum256([]byte("computer_exporting\x00export_kept"))
	return fmt.Sprintf("%x.json", digest), map[string]any{"version": 1, "phase": phase,
		"computer_id": "computer_exporting", "export_id": "export_kept", "storage_id": "storage_kept",
		"storage_generation": 1, "external_path": "/operator/export", "started_ns": 1}
}

func requireRecreateRefusal(t *testing.T, f *recreateFixture, want string) *RecreateBlockedError {
	t.Helper()
	err := f.recreate(t)
	var blocked *RecreateBlockedError
	if !errors.As(err, &blocked) || !strings.Contains(err.Error(), want) {
		t.Fatalf("recreate = %v, want typed refusal containing %q", err, want)
	}
	if f.helperStopped {
		t.Fatal("refusal left helper socket activation stopped")
	}
	for _, command := range f.commands {
		if command[0] == "delete" || command[0] == "start" {
			t.Fatalf("refusal changed host VM: %v", f.commands)
		}
	}
	return blocked
}

func mustRecreateServiceOwner(t *testing.T, f *recreateFixture, resources ocihelper.ResourceIdentity, jobID string) map[string]any {
	t.Helper()
	record := map[string]any{"version": 1, "device": uint64(1), "inode": uint64(2), "uid": 0, "gid": 0}
	path := filepath.Join(f.root, "service-data", resources.ServiceVolumeDirectory)
	var stat syscall.Stat_t
	if err := syscall.Stat(path, &stat); err == nil {
		record["device"], record["inode"] = uint64(stat.Dev), uint64(stat.Ino)
	} else if !errors.Is(err, os.ErrNotExist) {
		t.Fatal(err)
	}
	if jobID != "" {
		record["job_id"] = jobID
	}
	mustRecreateJSON(t, f, "service-data-state/"+resources.ServiceVolumeOwnerRecord, record)
	return record
}

func requireRecreateInventoryFailure(t *testing.T, f *recreateFixture) *RecreateBlockedError {
	t.Helper()
	blocked := requireRecreateRefusal(t, f, "cannot prove")
	if blocked.Reason == "" || len(blocked.Blockers) != 0 || !strings.Contains(blocked.Error(), "node-owner recovery") ||
		!strings.Contains(blocked.Error(), "keep the host VM") || strings.Contains(blocked.Error(), "wefty services remove") {
		t.Fatalf("ambiguous inventory got actionable blockers instead of inventory failure: %v", blocked)
	}
	if blocked.Reason != "guest disk inventory is incomplete or invalid" && blocked.Reason != "guest disk inspection failed or helper writes could not be stopped; no absence proof" {
		t.Fatalf("refusal was not an inventory failure: %v", blocked)
	}
	return blocked
}

// Each case executes the shipped script in real sh against durable files.
func TestRecreateKeepsStoppedServiceData(t *testing.T) {
	for _, shape := range []string{"volume and owner", "volume only", "owner only", "legacy Attempt fallback", "job record absent"} {
		t.Run(shape, func(t *testing.T) {
			f := newRecreateFixture(t)
			resources, record := recreateServiceRecord(t)
			if shape != "owner only" {
				mustRecreateFile(t, f.root, "service-data/"+resources.ServiceVolumeDirectory+"/retained")
			}
			if shape != "volume only" {
				job := "job_stopped_service"
				if shape == "legacy Attempt fallback" || shape == "job record absent" {
					job = ""
				}
				mustRecreateServiceOwner(t, f, resources, job)
			}
			want := "service data for job job_stopped_service"
			if shape == "job record absent" {
				want = "job is unknown"
			}
			if shape == "volume only" || shape == "legacy Attempt fallback" {
				mustRecreateJSON(t, f, "attempt-ownership/"+resources.ContainerID+".json", record)
			}
			blocked := requireRecreateRefusal(t, f, want)
			if len(blocked.Blockers) != 1 || !strings.Contains(blocked.Error(), resources.ServiceVolumeDirectory) {
				t.Fatalf("service refusal = %v", blocked)
			}
			if shape == "volume only" || shape == "job record absent" {
				if !strings.Contains(blocked.Error(), "node-owner recovery") || !strings.Contains(blocked.Error(), "keep the host VM") || strings.Contains(blocked.Error(), "wefty services remove") {
					t.Fatalf("unremovable service got remove advice: %v", blocked)
				}
			} else if !strings.Contains(blocked.Error(), "save any data you need, then run wefty services remove job_stopped_service --wait 60s") {
				t.Fatalf("service lacks actionable remove command: %v", blocked)
			}
			if shape != "owner only" {
				data, err := os.ReadFile(filepath.Join(f.root, "service-data", resources.ServiceVolumeDirectory, "retained"))
				if err != nil || string(data) != "retained bytes" {
					t.Fatalf("service data changed: %q, %v", data, err)
				}
			}
		})
	}
}

func TestRecreateKeepsCustodyWrites(t *testing.T) {
	// Completion replaces the marker, but only Computer removal deletes it.
	for _, phase := range []string{"started", "completed"} {
		t.Run(phase, func(t *testing.T) {
			f := newRecreateFixture(t)
			name, record := recreateCustodyRecord(phase)
			path := "computer-custody-writes/" + name
			mustRecreateJSON(t, f, path, record)
			before, err := os.ReadFile(filepath.Join(f.root, path))
			if err != nil {
				t.Fatal(err)
			}
			blocked := requireRecreateRefusal(t, f, "Custody write record for Computer computer_exporting, export export_kept ("+phase+")")
			if !strings.Contains(blocked.Error(), "wefty services remove computer_exporting --expect-current --wait 60s") || strings.Contains(blocked.Error(), "/operator/export") {
				t.Fatalf("Custody refusal = %v", blocked)
			}
			if !strings.Contains(blocked.Error(), "If the Computer is already removed, the helper clears this record on its next Computer Storage sweep (a helper restart) or Computer disk removal; if it remains after that, keep the VM and use node-owner recovery.") {
				t.Fatalf("Custody refusal lacks post-removal recovery: %v", blocked)
			}
			after, err := os.ReadFile(filepath.Join(f.root, path))
			if err != nil || string(before) != string(after) {
				t.Fatalf("Custody record changed: %q, %v", after, err)
			}
		})
	}
}

func TestRecreateAcceptsEmptyServiceAndCustodyNamespaces(t *testing.T) {
	f := newRecreateFixture(t)
	for _, kind := range []string{"service-data", "service-data-state", "computer-custody-writes"} {
		if err := os.Mkdir(filepath.Join(f.root, kind), 0o700); err != nil {
			t.Fatal(err)
		}
	}
	// Attempt records alone are not retained service data.
	resources, record := recreateServiceRecord(t)
	mustRecreateJSON(t, f, "attempt-ownership/"+resources.ContainerID+".json", record)
	if err := f.recreate(t); err != nil || !f.recovered {
		t.Fatalf("empty namespaces: recreate=%v recovered=%t", err, f.recovered)
	}
}

// No case starts with a valid blocker: the artifact under test must itself
// make absence unprovable, rather than being masked by retained good data.
func TestRecreateRefusesAmbiguousServiceAndCustodyFolders(t *testing.T) {
	for _, kind := range []string{"service-data", "service-data-state", "computer-custody-writes"} {
		for _, shape := range []string{"unreadable folder", "symlink folder", "non-directory", "unknown entry", "hidden file", "space in name", "newline in name", "non-UTF-8 name", "nested entry", "symlink entry", "unreadable entry", "malformed record", "oversized record", "truncated inventory", "enumeration failure", "read failure"} {
			t.Run(kind+"/"+shape, func(t *testing.T) {
				f := newRecreateFixture(t)
				resources, _ := recreateServiceRecord(t)
				name := resources.ServiceVolumeDirectory
				record := any(map[string]any{"device": 1, "inode": 2})
				if kind == "service-data-state" {
					name += ".owner"
					record = map[string]any{"version": 1, "device": 1, "inode": 2, "uid": 0, "gid": 0}
				} else if kind == "computer-custody-writes" {
					name, record = recreateCustodyRecord("started")
				}
				directory := filepath.Join(f.root, kind)
				if err := os.Mkdir(directory, 0o700); err != nil {
					t.Fatal(err)
				}
				entry := filepath.Join(directory, name)
				switch shape {
				case "unreadable folder":
					if err := os.Chmod(directory, 0); err != nil {
						t.Fatal(err)
					}
					t.Cleanup(func() { _ = os.Chmod(directory, 0o700) })
					if os.Geteuid() == 0 {
						t.Skip("root bypasses unreadable permissions; injected failures run separately")
					}
				case "symlink folder", "non-directory":
					if err := os.Remove(directory); err != nil {
						t.Fatal(err)
					}
					if shape == "non-directory" {
						mustRecreateFile(t, f.root, kind)
					} else if err := os.Symlink(t.TempDir(), directory); err != nil {
						t.Fatal(err)
					}
				case "unknown entry", "hidden file", "space in name", "newline in name", "nested entry":
					badName := map[string]string{"unknown entry": "unexpected", "hidden file": ".unexpected", "space in name": "unexpected entry", "newline in name": "unexpected\nentry", "nested entry": "unexpected/nested"}[shape]
					mustRecreateFile(t, f.root, kind+"/"+badName)
				case "non-UTF-8 name":
					// Mac filesystems cannot create every Linux filename. Exercise
					// the guest's raw name at the inventory transport boundary.
					f.inventoryOutput = []byte("wefty-recreate-inventory-v3\x00" + kind + "/unexpected\xff\x00{}\x00complete\x00")
				case "symlink entry":
					if err := os.Symlink(t.TempDir(), entry); err != nil {
						t.Fatal(err)
					}
				case "unreadable entry", "read failure":
					command := "head"
					if kind == "service-data" {
						command = "stat"
						if err := os.Mkdir(entry, 0o700); err != nil {
							t.Fatal(err)
						}
					} else {
						mustRecreateJSON(t, f, kind+"/"+name, record)
					}
					if shape == "read failure" {
						f.inventoryFailureCommand, f.inventoryFailurePath = command, entry
					} else {
						if err := os.Chmod(entry, 0); err != nil {
							t.Fatal(err)
						}
						t.Cleanup(func() { _ = os.Chmod(entry, 0o700) })
						if os.Geteuid() == 0 {
							t.Skip("root bypasses unreadable permissions; injected failures run separately")
						}
					}
				case "malformed record", "oversized record":
					payload := []byte(`{"version":`)
					if shape == "oversized record" {
						valid, err := json.Marshal(record)
						if err != nil {
							t.Fatal(err)
						}
						payload = append([]byte(strings.Repeat(" ", 65537)), valid...)
					}
					if kind == "service-data" {
						f.inventoryOutput = []byte("wefty-recreate-inventory-v3\x00" + kind + "/" + name + "\x00" + string(payload) + "\x00complete\x00")
					} else if err := os.WriteFile(entry, payload, 0o600); err != nil {
						t.Fatal(err)
					}
				case "truncated inventory":
					f.afterInventory = func(out []byte) ([]byte, error) { return out[:len(out)-len("complete\x00")], nil }
				case "enumeration failure":
					f.inventoryFailureCommand, f.inventoryFailurePath = "find", directory
				}
				blocked := requireRecreateInventoryFailure(t, f)
				if (shape == "malformed record" || shape == "oversized record" || shape == "truncated inventory") && blocked.Reason != "guest disk inventory is incomplete or invalid" {
					t.Fatalf("invalid output got wrong failure reason: %v", blocked)
				}
			})
		}
	}
}

func TestRecreateRejectsAmbiguousServiceOwnerBinding(t *testing.T) {
	for _, shape := range []string{"job mismatch", "inode mismatch"} {
		t.Run(shape, func(t *testing.T) {
			f := newRecreateFixture(t)
			resources, _ := recreateServiceRecord(t)
			if shape == "inode mismatch" {
				mustRecreateFile(t, f.root, "service-data/"+resources.ServiceVolumeDirectory+"/retained")
			}
			record := mustRecreateServiceOwner(t, f, resources, "job_stopped_service")
			if shape == "job mismatch" {
				record["job_id"] = "job_unrelated"
			} else {
				record["inode"] = record["inode"].(uint64) + 1
			}
			mustRecreateJSON(t, f, "service-data-state/"+resources.ServiceVolumeOwnerRecord, record)
			requireRecreateInventoryFailure(t, f)
		})
	}
}

func TestRecreateRejectsUnprovenServiceJobBinding(t *testing.T) {
	for _, shape := range []string{"malformed", "wrong filename", "wrong resource"} {
		t.Run(shape, func(t *testing.T) {
			f := newRecreateFixture(t)
			resources, record := recreateServiceRecord(t)
			path := "attempt-ownership/" + resources.ContainerID + ".json"
			switch shape {
			case "malformed":
				record = "invalid record"
			case "wrong filename":
				path = "attempt-ownership/unexpected.json"
			case "wrong resource":
				resources.ServiceVolumeDirectory = "wefty-service-volume-" + strings.Repeat("a", 32)
				record.(map[string]any)["resources"] = resources
			}
			payload, err := json.Marshal(record)
			if err != nil {
				t.Fatal(err)
			}
			f.inventoryOutput = []byte("wefty-recreate-inventory-v3\x00" + path + "\x00" + string(payload) + "\x00complete\x00")
			requireRecreateInventoryFailure(t, f)
		})
	}
}

func TestRecreateInventoryRejectsOlderProofAndInvalidRecords(t *testing.T) {
	for _, output := range []string{
		"wefty-recreate-inventory-v2\x00complete\x00",
		"wefty-recreate-inventory-v4\x00complete\x00",
		"wefty-recreate-inventory-v3\x00computer-custody-writes/unknown.json\x00{}\x00complete\x00",
		"wefty-recreate-inventory-v3\x00service-data-state/wefty-service-volume-" + strings.Repeat("a", 32) + ".owner\x00{}\x00complete\x00",
	} {
		err := authorizeRecreateInventory([]byte(output))
		var blocked *RecreateBlockedError
		if !errors.As(err, &blocked) || blocked.Reason == "" {
			t.Fatalf("invalid inventory authorized: %v", err)
		}
		for _, want := range []string{"Computer Storage", "Backup copies", "service data", "Custody write records"} {
			if !strings.Contains(blocked.Error(), want) {
				t.Fatalf("cannot-prove refusal omitted %s: %v", want, blocked)
			}
		}
	}
}

func TestRecreateMixedInventoryNamesEveryBlocker(t *testing.T) {
	f := newRecreateFixture(t)
	storage := ocihelper.ComputerStorageReference{ComputerID: "computer_owner", StorageID: "storage_kept", StorageGeneration: 7}
	for path, record := range map[string]any{
		"computer-disks/retained-generation/attachment.json": map[string]any{"version": 1, "storage": storage},
		"computer-backups/kept-copy/copy.json":               map[string]any{"version": 1, "storage": storage, "backup_id": "backup_kept", "copy_id": "copy_kept"},
		"computer-disk-quarantine/held/quarantine.json":      map[string]any{"storage": storage},
		"computer-storage-resets/reset.json":                 map[string]any{"version": 1, "storage": storage},
		"computer-disks/refused-copy/copy-refused.json":      map[string]any{"version": 1, "storage": storage},
	} {
		mustRecreateJSON(t, f, path, record)
	}
	mustRecreateFile(t, f.root, "computer-disks/refused-copy/attachment.lock")
	mustRecreateFile(t, f.root, "computer-backups/supersessions/unexpected.ext4")
	resources, _ := recreateServiceRecord(t)
	mustRecreateFile(t, f.root, "service-data/"+resources.ServiceVolumeDirectory+"/retained")
	mustRecreateServiceOwner(t, f, resources, "job_stopped_service")
	name, record := recreateCustodyRecord("started")
	mustRecreateJSON(t, f, "computer-custody-writes/"+name, record)
	blocked := requireRecreateRefusal(t, f, "job_stopped_service")
	if blocked.Reason != "" || len(blocked.Blockers) != 8 {
		t.Fatalf("mixed inventory = %v, want eight named blockers", blocked)
	}
	for _, want := range []string{"Computer computer_owner, Storage storage_kept generation 7", "Backup backup_kept, copy copy_kept", "quarantined Computer", "unresolved reset", "refused Computer copy record", "unexpected Backup supersession entry unexpected.ext4", "job_stopped_service", "Computer computer_exporting, export export_kept"} {
		if !strings.Contains(blocked.Error(), want) {
			t.Fatalf("mixed refusal omitted %q: %v", want, blocked)
		}
	}
}
