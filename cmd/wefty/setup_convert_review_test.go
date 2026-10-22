package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestSetupConvertBuildMismatchRefusesBeforeDrain(t *testing.T) {
	f := newConversionFixture(t)
	f.writeBinary(t, "wefty-l1", "different-build")
	before := installedUpgradeFiles(t, &upgradeFixture{setupFixture: f.setupFixture})
	revision := f.node.IntentRevision
	_, err := runSetup(t.Context(), f.host, f.request(t))
	if err == nil || commandExitCode(err) != exitConflict || !strings.Contains(err.Error(), "same build") {
		t.Fatalf("mismatched L1 build did not refuse clearly with exit 5: %v", err)
	}
	if f.node.IntentRevision != revision || f.waits != 0 || len(f.launchd.mutating()) != 0 {
		t.Fatal("mismatch changed admission or drained/stopped the stack")
	}
	f.assertPlain(t, before)
}

func TestSetupConvertSchemaErrorGivesUpgradeAdvice(t *testing.T) {
	f := newConversionFixture(t)
	before := installedUpgradeFiles(t, &upgradeFixture{setupFixture: f.setupFixture})
	f.host.upgrade.command = func(context.Context, string, []string, string, ...string) ([]byte, error) {
		return nil, errors.New("L1 job inventory: SQL logic error: no such table: service_removals")
	}
	_, err := runSetup(t.Context(), f.host, f.request(t))
	if err == nil || commandExitCode(err) != exitConflict || !strings.Contains(err.Error(), "upgrade the plain install to this build first") {
		t.Fatalf("schema error has no actionable upgrade advice: %v", err)
	}
	f.assertPlain(t, before)
}

func TestSetupConvertOfflineComputerRetirementAdvice(t *testing.T) {
	for _, mixed := range []bool{false, true} {
		t.Run(map[bool]string{false: "Computer", true: "mixed"}[mixed], func(t *testing.T) {
			f := newConversionFixture(t)
			before := installedUpgradeFiles(t, &upgradeFixture{setupFixture: f.setupFixture})
			blockers := []string{"Computer retained-computer"}
			if mixed {
				blockers = append(blockers, "queued job late-job")
			}
			payload, _ := json.Marshal(map[string]any{"version": 1, "blockers": blockers})
			f.host.upgrade.command = func(context.Context, string, []string, string, ...string) ([]byte, error) { return payload, nil }
			_, err := runSetup(t.Context(), f.host, f.request(t))
			if err == nil || commandExitCode(err) != exitConflict || !strings.Contains(err.Error(), "stop or retire these through their normal lifecycle") {
				t.Fatalf("Computer has no live retirement advice: %v", err)
			}
			if !mixed && strings.Contains(err.Error(), "wefty cancel") {
				t.Fatalf("Computer received job cancellation advice: %v", err)
			}
			if mixed && !strings.Contains(err.Error(), "wefty cancel") {
				t.Fatalf("mixed job blocker lost cancellation advice: %v", err)
			}
			f.assertPlain(t, before)
		})
	}
}

func TestSetupConvertInventoryFolderRecordedBeforeCopy(t *testing.T) {
	f := newConversionFixture(t)
	paths, _ := resolveSetupPaths(f.home)
	observed := false
	f.host.conversionCopy = &plainConversionCopyOps{clone: func(ctx context.Context, source *os.File, to string) error {
		observed = true
		b, err := os.ReadFile(filepath.Join(paths.config, plainConversionName))
		if err != nil {
			t.Fatal(err)
		}
		var record map[string]any
		if err := json.Unmarshal(b, &record); err != nil {
			t.Fatal(err)
		}
		if record["inventory_directory"] != filepath.Dir(to) {
			t.Fatalf("copy directory was not durably recorded before creation: %v", record["inventory_directory"])
		}
		return cloneConversionInventoryFile(ctx, source, to)
	}}
	if _, err := runSetup(t.Context(), f.host, f.request(t)); err != nil {
		t.Fatal(err)
	}
	if !observed {
		t.Fatal("copy seam was not reached")
	}
}

func TestSetupConvertKilledCopyCleanupFromRealBinary(t *testing.T) {
	binary := buildFleetFixtureBinary(t)
	for _, tc := range []struct {
		name             string
		abandon, blocked bool
	}{{"resume", false, false}, {"abandon", true, false}, {"abandon with unremovable copy", true, true}, {"resume with unremovable copy", false, true}} {
		abandon := tc.abandon
		t.Run(tc.name, func(t *testing.T) {
			home := newFleetTestHome(t)
			t.Cleanup(func() {
				_ = filepath.WalkDir(home, func(path string, d os.DirEntry, err error) error {
					if err == nil && d.IsDir() {
						return os.Chmod(path, 0700)
					}
					return err
				})
			})
			command := func(input string, extra []string, args ...string) ([]byte, error) {
				c := exec.CommandContext(t.Context(), binary, args...)
				c.Env = append([]string{"HOME=" + home, "PATH=/usr/bin:/bin", "TMPDIR=" + os.TempDir()}, extra...)
				c.Stdin = strings.NewReader(input)
				return c.CombinedOutput()
			}
			if err := os.MkdirAll(filepath.Join(home, setupFolders.data, "setup-ready"), 0700); err != nil {
				t.Fatal(err)
			}
			if out, err := command("", nil, "setup", "--node-id", "studio-0123abcd"); err != nil {
				t.Fatalf("plain setup: %v %s", err, out)
			}
			paths, _ := resolveSetupPaths(home)
			if err := os.MkdirAll(paths.data, 0700); err != nil {
				t.Fatal(err)
			}
			for _, name := range []string{"l1.sqlite", "l3.sqlite"} {
				if err := os.WriteFile(filepath.Join(paths.data, name), []byte("closed fixture "+name), 0600); err != nil {
					t.Fatal(err)
				}
			}
			join := joinCodeForTest(t, "opaque-l1", "opaque-l3", "")
			args := []string{"--json", "setup", "--role", "join", "--join-code", join, "--convert-plain"}
			out, err := command("", []string{"WEFTY_FIXTURE_KILL_CONVERSION_COPY=1"}, args...)
			var exit *exec.ExitError
			if !errors.As(err, &exit) || exit.ExitCode() != 86 {
				t.Fatalf("copy helper did not exit abruptly: %v %s", err, out)
			}
			marker, err := os.ReadFile(filepath.Join(home, "fixture-killed-copy"))
			if err != nil {
				t.Fatal(err)
			}
			folder := string(marker)
			if info, err := os.Stat(folder); err != nil || !info.IsDir() {
				t.Fatalf("abrupt exit left no private copy: %v", err)
			}
			if b, err := os.ReadFile(filepath.Join(folder, "l1.sqlite")); err != nil || !bytes.Equal(b, []byte("closed fixture l1.sqlite")) {
				t.Fatalf("missing killed copy payload: %v", err)
			}
			if abandon {
				args = []string{"setup", "--role", "join", "--convert-plain", "--abandon-conversion"}
			}
			kept := filepath.Join(t.TempDir(), "kept")
			if tc.blocked {
				// A link at the recorded name cannot be removed safely. The plain
				// stack must still come back, and the link target stays untouched.
				if err := os.MkdirAll(kept, 0700); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(kept, "keep"), []byte("keep"), 0600); err != nil {
					t.Fatal(err)
				}
				if err := os.RemoveAll(folder); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(kept, folder); err != nil {
					t.Fatal(err)
				}
			}
			out, err = command("single-use-node\nsingle-use-client\n", nil, args...)
			if err != nil {
				t.Fatalf("recovery failed: %v %s", err, out)
			}
			if tc.blocked {
				if !bytes.Contains(out, []byte("the plain installation is restored, but the temporary conversion inventory copy")) {
					t.Fatalf("missing cleanup warning: %s", out)
				}
				if b, err := os.ReadFile(filepath.Join(kept, "keep")); err != nil || string(b) != "keep" {
					t.Fatalf("link target changed: %v", err)
				}
				if abandon {
					if _, err := os.Stat(filepath.Join(paths.config, plainConversionName)); !errors.Is(err, os.ErrNotExist) {
						t.Fatalf("conversion record kept the install in recovery: %v", err)
					}
					return
				}
				// The resumed conversion finishes and never carries the stale folder.
				if b, err := os.ReadFile(filepath.Join(paths.config, plainConversionName)); err == nil {
					var saved plainConversion
					if err := json.Unmarshal(b, &saved); err != nil || saved.InventoryDirectory != "" {
						t.Fatalf("resumed record carried the stale copy folder: %v %+v", err, saved)
					}
				} else if !errors.Is(err, os.ErrNotExist) {
					t.Fatal(err)
				}
				return
			}
			if _, err := os.Stat(folder); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("killed copy was not removed on recovery: %v", err)
			}
		})
	}
}

func TestSetupConvertInventoryDirectoryCleanupBoundaries(t *testing.T) {
	for _, mode := range []string{"absent", "present", "outside", "wrong prefix", "linked"} {
		t.Run(mode, func(t *testing.T) {
			home := t.TempDir()
			paths := setupPaths{home: home, data: filepath.Join(home, "data"), dataRel: "data", config: filepath.Join(home, "config")}
			for _, folder := range []string{paths.data, paths.config} {
				if err := os.Mkdir(folder, 0700); err != nil {
					t.Fatal(err)
				}
			}
			root, err := os.OpenRoot(home)
			if err != nil {
				t.Fatal(err)
			}
			defer root.Close()
			folder := filepath.Join(paths.data, plainConversionInventoryPrefix+"0123456789abcdef")
			sentinel := filepath.Join(home, "unrelated")
			if err := os.Mkdir(sentinel, 0700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(sentinel, "keep"), []byte("unrelated data"), 0600); err != nil {
				t.Fatal(err)
			}
			switch mode {
			case "present":
				if err := os.Mkdir(folder, 0700); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(folder, "copy"), []byte("private job specs"), 0600); err != nil {
					t.Fatal(err)
				}
			case "outside":
				folder = sentinel
			case "wrong prefix":
				folder = filepath.Join(paths.data, "unrelated")
			case "linked":
				if err := os.Symlink(sentinel, folder); err != nil {
					t.Fatal(err)
				}
			}
			record := &plainConversion{Version: 1, Phase: "stop", InventoryDirectory: folder}
			if err := savePlainConversion(paths, record); err != nil {
				t.Fatal(err)
			}
			err = cleanupPlainConversionInventory(root, paths, record)
			if mode == "absent" || mode == "present" {
				if err != nil || record.InventoryDirectory != "" {
					t.Fatalf("cleanup failed: %v", err)
				}
				var saved plainConversion
				b, readErr := os.ReadFile(filepath.Join(paths.config, plainConversionName))
				if readErr != nil || json.Unmarshal(b, &saved) != nil || saved.InventoryDirectory != "" {
					t.Fatal("reservation was not durably cleared")
				}
				if _, err := os.Lstat(folder); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("copy folder survived: %v", err)
				}
			} else if err == nil || record.InventoryDirectory != folder {
				t.Fatal("untrusted copy path was accepted or forgotten")
			}
			if b, err := os.ReadFile(filepath.Join(sentinel, "keep")); err != nil || string(b) != "unrelated data" {
				t.Fatalf("cleanup touched unrelated data: %v", err)
			}
		})
	}
}
