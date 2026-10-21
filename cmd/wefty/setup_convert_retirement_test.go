package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"testing"

	"github.com/Derek-X-Wang/wefty/internal/serviceplan"
)

func assertConversionRetired(t *testing.T, paths setupPaths, history string) {
	t.Helper()
	if _, err := os.Lstat(filepath.Join(paths.config, plainConversionName)); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("active conversion record remains: %v", err)
	}
	for _, name := range []string{history, filepath.Join(history, "conversion-record.json")} {
		info, err := os.Lstat(name)
		if err != nil || info.Mode().Perm()&0222 != 0 {
			t.Fatalf("missing or writable retired evidence %s: %v", name, err)
		}
	}
}

func TestSetupConvertRetiresCompletedRecord(t *testing.T) {
	for _, legacy := range []bool{false, true} {
		t.Run(map[bool]string{false: "new-completion", true: "old-completion-rerun"}[legacy], func(t *testing.T) {
			f := newConversionFixture(t)
			paths, _ := resolveSetupPaths(f.home)
			request := f.request(t)
			if legacy {
				request.afterFleetStep = func(step string) error {
					if step == "committed" {
						return errors.New("simulated old completion")
					}
					return nil
				}
			}
			report, err := runSetup(t.Context(), f.host, request)
			if legacy {
				if err == nil {
					t.Fatal("missing simulated interruption")
				}
				if _, err := os.Stat(filepath.Join(paths.config, plainConversionName)); err != nil {
					t.Fatal(err)
				}
				report, err = runSetup(t.Context(), f.host, setupRequest{role: "join"})
			}
			if err != nil {
				t.Fatal(err)
			}
			history := filepath.Join(paths.data, plainHistoryFolder, "studio-0123abcd")
			assertConversionRetired(t, paths, history)
			if report.History == nil {
				t.Fatal("history no longer reported")
			}
			role, err := selectUpgradeRole(paths)
			if err != nil || role != serviceplan.Join {
				t.Fatalf("completed conversion cannot upgrade: %s %v", role, err)
			}
		})
	}
}

func TestFleetUpgradeRetiresOldCompletedConversion(t *testing.T) {
	f := newFleetUpgradeFixture(t, serviceplan.Join)
	paths, _ := resolveSetupPaths(f.home)
	// An old completed conversion retains its original plain profile and history
	// metadata. No history database is needed or opened by the upgrade.
	plain := newConversionFixture(t)
	request := plain.request(t)
	request.afterFleetStep = func(step string) error {
		if step == "committed" {
			return errors.New("old completion")
		}
		return nil
	}
	if _, err := runSetup(t.Context(), plain.host, request); err == nil {
		t.Fatal("missing interruption")
	}
	oldPaths, _ := resolveSetupPaths(plain.home)
	record, err := readPlainConversion(oldPaths)
	if err != nil || record == nil {
		t.Fatalf("conversion record: %v", err)
	}
	record.NodeID = f.profile(t).Identities.Node
	record.Profile.Identities.Node = record.NodeID
	record.History.Directory = filepath.Join(paths.data, plainHistoryFolder, record.NodeID)
	stage, _ := readFleetSetupRecord(paths)
	record.JoinCode = stage.JoinCode
	if err := os.MkdirAll(record.History.Directory, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(record.History.Directory, 0500); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(record.History.Directory, 0700) })
	receiptPath := filepath.Join(paths.config, setupManifestName)
	payload, _ := os.ReadFile(receiptPath)
	var receipt setupManifest
	if err := json.Unmarshal(payload, &receipt); err != nil {
		t.Fatal(err)
	}
	receipt.History = &record.History
	payload, _ = json.Marshal(receipt)
	if err := os.WriteFile(receiptPath, payload, 0600); err != nil {
		t.Fatal(err)
	}
	if err := savePlainConversion(paths, record); err != nil {
		t.Fatal(err)
	}
	if _, err := runUpgrade(t.Context(), f.host, false, io.Discard); err != nil {
		t.Fatalf("completed conversion refused upgrade: %v", err)
	}
	assertConversionRetired(t, paths, record.History.Directory)
}

// Publication adds a separate archival record to the closed dataset. Keep the
// original inventory assertions and additionally verify this read-only record.
func verifyRetiredPlainHistory(root *os.Root, folder string, history plainHistory) error {
	payload, err := root.ReadFile(filepath.Join(folder, "conversion-record.json"))
	if err != nil {
		return err
	}
	var record plainConversion
	if err := json.Unmarshal(payload, &record); err != nil {
		return err
	}
	original, _ := json.Marshal(history)
	recorded, _ := json.Marshal(record.History)
	if string(original) != string(recorded) {
		return errors.New("archived history inventory changed")
	}
	sum := sha256.Sum256(payload)
	history.Files = append(append([]plainHistoryFile(nil), history.Files...), plainHistoryFile{Name: "conversion-record.json", Size: int64(len(payload)), SHA256: hex.EncodeToString(sum[:]), Mode: 0400})
	return verifyPlainHistory(root, folder, history, true)
}

func TestFleetUpgradeDoesNotRetireIncompleteConversion(t *testing.T) {
	f := newConversionFixture(t)
	paths, _ := resolveSetupPaths(f.home)
	request := f.request(t)
	request.afterFleetStep = func(step string) error {
		if step == "stage-record" {
			return errors.New("incomplete join")
		}
		return nil
	}
	if _, err := runSetup(t.Context(), f.host, request); err == nil {
		t.Fatal("missing interruption")
	}
	f.launchd.commands = nil
	if _, err := runUpgrade(t.Context(), f.host, false, io.Discard); err == nil {
		t.Fatal("incomplete conversion upgraded")
	}
	if _, err := os.Lstat(filepath.Join(paths.config, plainConversionName)); err != nil {
		t.Fatal("incomplete record retired")
	}
	if _, err := os.Lstat(filepath.Join(paths.data, plainHistoryFolder, "studio-0123abcd", "conversion-record.json")); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("incomplete record archived: %v", err)
	}
	if len(f.launchd.commands) != 0 {
		t.Fatal("incomplete upgrade touched services")
	}
}

func TestSetupConvertRetirementCrashAndMismatch(t *testing.T) {
	for _, choice := range []string{"identical", "different", "linked", "history-absent"} {
		t.Run(choice, func(t *testing.T) {
			f := newConversionFixture(t)
			paths, _ := resolveSetupPaths(f.home)
			r := f.request(t)
			r.afterFleetStep = func(step string) error {
				if step == "committed" {
					return errors.New("interrupted before retirement")
				}
				return nil
			}
			if _, err := runSetup(t.Context(), f.host, r); err == nil {
				t.Fatal("missing interruption")
			}
			record, err := readPlainConversion(paths)
			if err != nil {
				t.Fatal(err)
			}
			payload, err := os.ReadFile(filepath.Join(paths.config, plainConversionName))
			if err != nil {
				t.Fatal(err)
			}
			history := record.History.Directory
			if err := os.Chmod(history, 0700); err != nil {
				t.Fatal(err)
			}
			archive := filepath.Join(history, "conversion-record.json")
			switch choice {
			case "identical":
				if err := os.WriteFile(archive, payload, 0400); err != nil {
					t.Fatal(err)
				}
			case "different":
				if err := os.WriteFile(archive, []byte("different record"), 0400); err != nil {
					t.Fatal(err)
				}
			case "linked":
				if err := os.Symlink(filepath.Join(paths.config, plainConversionName), archive); err != nil {
					t.Fatal(err)
				}
			case "history-absent":
				if err := os.Rename(history, filepath.Join(f.home, "owner-archived-history")); err != nil {
					t.Fatal(err)
				}
			}
			if choice != "history-absent" {
				if err := os.Chmod(history, 0500); err != nil {
					t.Fatal(err)
				}
			}
			_, err = runSetup(t.Context(), f.host, setupRequest{role: "join"})
			if choice == "different" || choice == "linked" {
				if err == nil {
					t.Fatal("unsafe retired record accepted")
				}
				got, readErr := os.ReadFile(filepath.Join(paths.config, plainConversionName))
				if readErr != nil || string(got) != string(payload) {
					t.Fatal("active conversion evidence lost")
				}
				got, readErr = os.ReadFile(archive)
				want := string(payload)
				if choice == "different" {
					want = "different record"
				}
				if readErr != nil || string(got) != want {
					t.Fatal("existing archival evidence overwritten")
				}
			} else {
				if err != nil {
					t.Fatal(err)
				}
				assertConversionRetired(t, paths, history)
				if choice == "history-absent" {
					entries, err := os.ReadDir(history)
					if err != nil || len(entries) != 1 || entries[0].Name() != "conversion-record.json" {
						t.Fatal("historical data reconstructed")
					}
				}
			}
		})
	}
}
