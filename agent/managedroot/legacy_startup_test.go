//go:build darwin || linux

package managedroot

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const legacyStartupHost = "linux:00000000-0000-0000-0000-000000000001"

func TestLegacyStartupSkipsCorruptService(t *testing.T) {
	for name, open := range map[string]func(Config) (*Manager, error){"Initialize": Initialize, "Open": Open} {
		t.Run(name, func(t *testing.T) {
			config, paths := legacyStartupFixture(t)
			corrupt := []byte(`{"version":1,"job_id":"bad",`)
			badPath := filepath.Join(paths["bad"].Root, OwnershipManifestName)
			if err := os.WriteFile(badPath, corrupt, 0o600); err != nil {
				t.Fatal(err)
			}
			manager, err := open(config)
			if err != nil {
				t.Fatalf("one corrupt service stopped startup: %v", err)
			}
			assertLegacyStartupStamp(t, paths["valid"], config.BootSessionID)
			failures := manager.LegacyStampErrors()
			if len(failures) != 1 || !errors.Is(failures[0], ErrOwnershipManifest) || !strings.Contains(failures[0].Error(), EncodeID("bad")) {
				t.Fatalf("missing skipped-service diagnostic: %v", failures)
			}
			assertLegacyStartupBytes(t, badPath, corrupt)
			if err := manager.Remove(context.Background(), removalFor(manager, "bad", 1)); !errors.Is(err, ErrOwnershipManifest) {
				t.Fatalf("corrupt service removal = %v, want ownership refusal", err)
			}
			assertLegacyStartupBytes(t, badPath, corrupt)
		})
	}
}

func TestLegacyStartupSkipsUnavailableHostBoot(t *testing.T) {
	for name, open := range map[string]func(Config) (*Manager, error){"Initialize": Initialize, "Open": Open} {
		for failure, reader := range map[string]func() (string, error){
			"unreadable": func() (string, error) { return "", errors.New("injected boot identity read failure") },
			"invalid":    func() (string, error) { return "not-a-host-boot", nil },
		} {
			t.Run(name+"/"+failure, func(t *testing.T) {
				config, paths := legacyStartupFixture(t)
				before := make(map[string][]byte)
				for job, path := range paths {
					before[job] = readLegacyStartupBytes(t, filepath.Join(path.Root, OwnershipManifestName))
				}
				config.HostBootIdentity = reader
				manager, err := open(config)
				if err != nil {
					t.Fatalf("boot identity failure stopped startup: %v", err)
				}
				failures := manager.LegacyStampErrors()
				if len(failures) != 1 || !strings.Contains(failures[0].Error(), "host boot identity") {
					t.Fatalf("missing skipped-scan diagnostic: %v", failures)
				}
				for job, path := range paths {
					assertLegacyStartupBytes(t, filepath.Join(path.Root, OwnershipManifestName), before[job])
				}
				if _, err := manager.ProcessAttempts("valid"); err == nil {
					t.Fatal("unavailable identity authorized legacy inventory")
				}
				// Recovering the reader lets the existing lazy path stamp, while
				// still refusing unknown processes on this host boot.
				manager.hostBootIdentity = func() (string, error) { return legacyStartupHost, nil }
				if _, err := manager.ProcessAttempts("valid"); !errors.Is(err, ErrProcessTreeNotReaped) {
					t.Fatalf("lazy legacy inventory = %v, want process uncertainty", err)
				}
				assertLegacyStartupStamp(t, paths["valid"], config.BootSessionID)
			})
		}
	}
}

func TestLegacyStartupSkipsServiceIOAndValidationFailures(t *testing.T) {
	for name, open := range map[string]func(Config) (*Manager, error){"Initialize": Initialize, "Open": Open} {
		for _, failure := range []string{"service-open", "manifest-read", "manifest-validation", "stamp-write"} {
			t.Run(name+"/"+failure, func(t *testing.T) {
				if os.Geteuid() == 0 && (failure == "manifest-read" || failure == "stamp-write") {
					t.Skip("root bypasses permission-based I/O failure injection")
				}
				config, paths := legacyStartupFixture(t)
				path := filepath.Join(paths["bad"].Root, OwnershipManifestName)
				switch failure {
				case "service-open":
					// The scan must refuse a service symlink without following it.
					saved := filepath.Join(filepath.Dir(filepath.Dir(paths["bad"].Root)), stagingDirectory, "saved-service")
					if err := os.Rename(paths["bad"].Root, saved); err != nil {
						t.Fatal(err)
					}
					if err := os.Symlink(saved, paths["bad"].Root); err != nil {
						t.Fatal(err)
					}
				case "manifest-read":
					if err := os.Chmod(path, 0); err != nil {
						t.Fatal(err)
					}
					t.Cleanup(func() { _ = os.Chmod(path, 0o600) })
				case "manifest-validation":
					if err := os.WriteFile(path, []byte(`{"version":1,"job_id":"wrong-job","removal_generation":1}`), 0o600); err != nil {
						t.Fatal(err)
					}
				case "stamp-write":
					if err := os.Chmod(paths["bad"].Root, 0o500); err != nil {
						t.Fatal(err)
					}
					t.Cleanup(func() { _ = os.Chmod(paths["bad"].Root, 0o700) })
				}
				// The unreadable file is compared after restoring read permission.
				var before []byte
				if failure != "manifest-read" {
					before = readLegacyStartupBytes(t, path)
				}
				manager, err := open(config)
				if err != nil {
					t.Fatalf("service failure stopped startup: %v", err)
				}
				failures := manager.LegacyStampErrors()
				if len(failures) != 1 || !strings.Contains(failures[0].Error(), EncodeID("bad")) {
					t.Fatalf("missing skipped-service diagnostic: %v", failures)
				}
				assertLegacyStartupStamp(t, paths["valid"], config.BootSessionID)
				if failure == "manifest-read" {
					if err := os.Chmod(path, 0o600); err != nil {
						t.Fatal(err)
					}
					var manifest OwnershipManifest
					if err := json.Unmarshal(readLegacyStartupBytes(t, path), &manifest); err != nil || manifest.Version != 1 || manifest.LegacyHostBootID != "" {
						t.Fatalf("unreadable manifest was stamped: %+v err=%v", manifest, err)
					}
				} else {
					assertLegacyStartupBytes(t, path, before)
				}
			})
		}
	}
}

func TestLegacyStartupStampsAllServicesOnlyOnce(t *testing.T) {
	for name, open := range map[string]func(Config) (*Manager, error){"Initialize": Initialize, "Open": Open} {
		t.Run(name, func(t *testing.T) {
			config, paths := legacyStartupFixture(t)
			manager, err := open(config)
			if err != nil || len(manager.LegacyStampErrors()) != 0 {
				t.Fatalf("readable startup manager=%v err=%v", manager, err)
			}
			for _, path := range paths {
				assertLegacyStartupStamp(t, path, "second")
			}
			config.BootSessionID = "third"
			config.HostBootIdentity = func() (string, error) { return "linux:00000000-0000-0000-0000-000000000002", nil }
			manager, err = open(config)
			if err != nil || len(manager.LegacyStampErrors()) != 0 {
				t.Fatalf("reopened manager=%v err=%v", manager, err)
			}
			for _, path := range paths {
				assertLegacyStartupStamp(t, path, "second")
			}
		})
	}
}

func TestLegacyStartupKeepsLayoutFailuresFatal(t *testing.T) {
	for name, open := range map[string]func(Config) (*Manager, error){"Initialize": Initialize, "Open": Open} {
		t.Run(name, func(t *testing.T) {
			config, paths := legacyStartupFixture(t)
			services := filepath.Dir(paths["bad"].Root)
			if err := os.Rename(services, services+"-saved"); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(services+"-saved", services); err != nil {
				t.Fatal(err)
			}
			if manager, err := open(config); err == nil || manager != nil {
				t.Fatalf("unsafe services layout admitted: manager=%v err=%v", manager, err)
			}
		})
	}
}

func legacyStartupFixture(t *testing.T) (Config, map[string]ServicePaths) {
	t.Helper()
	config := Config{Root: syntheticRoot(t), NodeID: "node", BootSessionID: "first", HostBootIdentity: func() (string, error) { return legacyStartupHost, nil }}
	manager, err := Initialize(config)
	if err != nil {
		t.Fatal(err)
	}
	paths := make(map[string]ServicePaths)
	for _, job := range []string{"bad", "valid"} {
		paths[job], err = manager.CreateService(job, 1)
		if err != nil {
			t.Fatal(err)
		}
		data, err := json.Marshal(OwnershipManifest{Version: 1, JobID: job, RemovalGeneration: 1})
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(paths[job].Root, OwnershipManifestName), data, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	config.BootSessionID = "second"
	return config, paths
}

func assertLegacyStartupStamp(t *testing.T, paths ServicePaths, boot string) {
	t.Helper()
	var manifest OwnershipManifest
	if err := json.Unmarshal(readLegacyStartupBytes(t, filepath.Join(paths.Root, OwnershipManifestName)), &manifest); err != nil {
		t.Fatal(err)
	}
	if manifest.Version != 2 || !manifest.LegacyProcessInventory || manifest.LegacyHostBootID != legacyStartupHost || manifest.LegacyBootSessionID != boot {
		t.Fatalf("legacy stamp = %+v, want first observation in %s", manifest, boot)
	}
}

func readLegacyStartupBytes(t *testing.T, path string) []byte {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func assertLegacyStartupBytes(t *testing.T, path string, want []byte) {
	t.Helper()
	if got := readLegacyStartupBytes(t, path); !bytes.Equal(got, want) {
		t.Fatalf("skipped manifest changed: got %q, want %q", got, want)
	}
}
