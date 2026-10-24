package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestReviewPurgeOwnEntriesOnly(t *testing.T) {
	f := newSetupFixture(t)
	f.setup(t, setupRequest{})
	paths, _ := resolveSetupPaths(f.home)
	owned := []string{".setup-lock", "l1.sqlite", "l1.sqlite-wal", "l1.sqlite-shm", "l1.sqlite-journal", "l3.sqlite", "l3.sqlite-wal", "l3.sqlite-shm", "l3.sqlite-journal", "node-agent/managed/job"}
	foreign := []string{"agent/keep", "capability/keep", "foreign.txt", "l1.sqlite.bak", "l1.sqlite.pre-upgrade", "l3.sqlite.bak", "l3.sqlite.pre-upgrade"}
	for _, name := range append(owned, foreign...) {
		path := filepath.Join(paths.data, name)
		if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
			t.Fatal(err)
		}
		mustWrite(t, path, "keep")
	}
	for _, name := range []string{"l1.log", "l3.log", "node-agent.log", "foreign.log"} {
		mustWrite(t, filepath.Join(paths.logs, name), "keep")
	}
	for attempt := 0; attempt < 2; attempt++ {
		report, err := runSetupRemove(t.Context(), f.host, true)
		if err != nil {
			t.Fatal(err)
		}
		for _, name := range owned {
			if _, err := os.Lstat(filepath.Join(paths.data, name)); !os.IsNotExist(err) {
				t.Errorf("purge left owned %s: %v", name, err)
			}
		}
		for _, name := range foreign {
			if _, err := os.Stat(filepath.Join(paths.data, name)); err != nil {
				t.Errorf("purge deleted foreign %s: %v", name, err)
			}
			if !strings.Contains(strings.Join(report.Warnings, "\n"), filepath.Join(paths.data, strings.Split(name, "/")[0])) {
				t.Errorf("foreign entry not named: %s %+v", name, report)
			}
		}
		for _, name := range []string{"l1.log", "l3.log", "node-agent.log"} {
			if _, err := os.Lstat(filepath.Join(paths.logs, name)); !os.IsNotExist(err) {
				t.Errorf("purge left setup log %s", name)
			}
		}
		if _, err := os.Stat(filepath.Join(paths.logs, "foreign.log")); err != nil || !strings.Contains(strings.Join(report.Warnings, "\n"), "foreign.log") {
			t.Errorf("foreign log not preserved and named: %v %+v", err, report)
		}
		if attempt == 1 && len(report.Changes) != 0 {
			t.Errorf("repeat claimed removals: %+v", report)
		}
	}
}

func TestReviewPurgeRequiresSetupEvidence(t *testing.T) {
	for _, evidence := range []string{"none", "receipt", "plist"} {
		t.Run(evidence, func(t *testing.T) {
			f := newSetupFixture(t)
			if evidence != "none" {
				f.setup(t, setupRequest{})
				if evidence == "plist" {
					mustRemove(t, filepath.Join(f.home, setupFolders.config, setupManifestName))
				}
				for _, label := range []string{setupL1Label, setupL3Label, setupNodeAgentLabel} {
					if evidence != "plist" || label != setupL1Label {
						mustRemove(t, filepath.Join(f.home, setupFolders.launchAgents, label+".plist"))
					}
					f.launchd.loaded[label] = false
				}
			}
			files := []string{filepath.Join(setupFolders.data, "l1.sqlite"), filepath.Join(setupFolders.data, "l3.sqlite-wal"), filepath.Join(setupFolders.data, "node-agent/managed/job"), filepath.Join(setupFolders.logs, "l1.log")}
			for _, name := range files {
				path := filepath.Join(f.home, name)
				if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
					t.Fatal(err)
				}
				mustWrite(t, path, "owner data")
			}
			report, err := runSetupRemove(t.Context(), f.host, true)
			if err != nil {
				t.Fatal(err)
			}
			for _, name := range files {
				payload, err := os.ReadFile(filepath.Join(f.home, name))
				if evidence == "none" {
					if err != nil || string(payload) != "owner data" {
						t.Errorf("purged data without setup evidence: %s: %q %v", name, payload, err)
					}
				} else if !os.IsNotExist(err) {
					t.Errorf("setup %s did not permit purge of %s: %v", evidence, name, err)
				}
			}
			if evidence == "none" && !strings.Contains(strings.Join(report.Warnings, "\n"), "no setup receipt or LaunchAgent") {
				t.Errorf("missing warning about setup evidence: %+v", report)
			}
		})
	}
}

func TestReviewRemoveEditedMetadata(t *testing.T) {
	for _, kind := range []string{"edited", "unreadable", "link"} {
		t.Run(kind, func(t *testing.T) {
			link := kind == "link"
			f := newSetupFixture(t)
			f.setup(t, setupRequest{})
			paths, _ := resolveSetupPaths(f.home)
			for _, name := range []string{paths.profile, paths.nodeConfig} {
				if link {
					mustRemove(t, name)
					target := filepath.Join(t.TempDir(), "owner-file")
					mustWrite(t, target, "edited")
					if err := os.Symlink(target, name); err != nil {
						t.Fatal(err)
					}
				} else {
					mustWrite(t, name, "edited")
					if kind == "unreadable" {
						if err := os.Chmod(name, 0); err != nil {
							t.Fatal(err)
						}
						t.Cleanup(func() { os.Chmod(name, 0o600) })
					}
				}
			}
			report, err := runSetupRemove(t.Context(), f.host, false)
			if err != nil {
				t.Fatal(err)
			}
			for _, name := range []string{paths.profile, paths.nodeConfig} {
				_, err := os.Lstat(name)
				if link && err != nil {
					t.Errorf("removed metadata link: %v", err)
				}
				if !link && !os.IsNotExist(err) {
					t.Errorf("edited regular metadata remains: %v %+v", err, report)
				}
			}
		})
	}
}

func TestReviewMaintenanceSkipsBrokenSkillPath(t *testing.T) {
	for _, outside := range []bool{false, true} {
		t.Run(fmt.Sprint(outside), func(t *testing.T) {
			f := newSetupFixture(t)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusServiceUnavailable) }))
			defer server.Close()
			f.host.l1Address = strings.TrimPrefix(server.URL, "http://")
			f.host.l3Address = f.host.l1Address
			f.setup(t, setupRequest{})
			paths, _ := resolveSetupPaths(f.home)
			target := filepath.Join(f.home, "checkout")
			if outside {
				target = t.TempDir()
			}
			if err := os.MkdirAll(target, 0o700); err != nil {
				t.Fatal(err)
			}
			mustWrite(t, filepath.Join(target, "SKILL.md"), "owner-checkout")
			skill := filepath.Join(f.home, setupFolders.skills[0])
			if err := os.RemoveAll(skill); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(target, skill); err != nil {
				t.Fatal(err)
			}
			report := runFixturePlainDoctor(t, f.host, globalOptions{profilePath: paths.profile, nodeConfigPath: paths.nodeConfig})
			for _, check := range []string{setupL1Label, "connection-profile", "node-config", "path:" + setupFolders.bin, "skill:" + filepath.Join(setupFolders.skills[1], "SKILL.md")} {
				found := false
				for _, finding := range report.Findings {
					if finding.Check == check {
						found = true
					}
				}
				if !found {
					t.Errorf("path failure prevented %s: %+v", check, report)
				}
			}
			removed, err := runSetupRemove(t.Context(), f.host, false)
			if err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(strings.Join(removed.Warnings, "\n"), skill) {
				t.Errorf("linked skill warning absent: %+v", removed)
			}
			for _, name := range []string{paths.profile, paths.nodeConfig, filepath.Join(paths.bin, "wefty"), filepath.Join(f.home, setupFolders.skills[1])} {
				if _, err := os.Lstat(name); !os.IsNotExist(err) {
					t.Errorf("independent removal skipped %s: %v", name, err)
				}
			}
			if got, err := os.ReadFile(filepath.Join(target, "SKILL.md")); err != nil || string(got) != "owner-checkout" {
				t.Errorf("checkout changed: %s %v", got, err)
			}
			if info, err := os.Lstat(skill); err != nil || info.Mode()&os.ModeSymlink == 0 {
				t.Errorf("skill link changed: %v", err)
			}
		})
	}
}

func TestReviewReceiptReloadRecoveryHint(t *testing.T) {
	f := newSetupFixture(t)
	f.setup(t, setupRequest{})
	f.writeBinary(t, "wefty-l1", "v2")
	f.launchd.failOnce["bootout"] = errors.New("reload refused")
	if _, err := runSetup(t.Context(), f.host, setupRequest{from: f.source}); err == nil {
		t.Fatal("fixture reload did not fail")
	}
	if _, err := runSetupRemove(t.Context(), f.host, false); err == nil || !strings.Contains(err.Error(), "run `wefty setup` again, then remove") {
		t.Fatalf("refusal lacks recovery: %v", err)
	}
	f.setup(t, setupRequest{})
	if _, err := runSetupRemove(t.Context(), f.host, false); err != nil {
		t.Fatalf("recovery failed: %v", err)
	}
}

func TestReviewReceiptRetainsOldSkillFiles(t *testing.T) {
	f := newSetupFixture(t)
	f.setup(t, setupRequest{})
	paths, _ := resolveSetupPaths(f.home)
	name := filepath.Join(setupFolders.skills[0], "old-version.txt")
	mustWrite(t, filepath.Join(f.home, name), "old skill")
	receipt := filepath.Join(paths.config, setupManifestName)
	payload, err := os.ReadFile(receipt)
	if err != nil {
		t.Fatal(err)
	}
	var manifest setupManifest
	if err := json.Unmarshal(payload, &manifest); err != nil {
		t.Fatal(err)
	}
	manifest.Files[name] = payloadDigest([]byte("old skill"))
	payload, err = json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	mustWrite(t, receipt, string(payload))
	f.setup(t, setupRequest{})
	report, err := runSetupRemove(t.Context(), f.host, false)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(filepath.Join(f.home, name)); !os.IsNotExist(err) {
		t.Errorf("previously recorded skill remains: %v %+v", err, report)
	}
}

func TestReviewDoctorSelection(t *testing.T) {
	for _, goos := range []string{"darwin", "linux"} {
		for _, kind := range []string{"missing", "plain", "OCI", "bad JSON", "truncated JSON", "invalid version", "invalid socket", "directory", "unreadable"} {
			t.Run(goos+"/"+kind, func(t *testing.T) {
				path := filepath.Join(t.TempDir(), "node.json")
				switch kind {
				case "plain":
					mustWrite(t, path, `{"version":1,"node_id":"node"}`)
				case "OCI":
					mustWrite(t, path, `{"version":1,"control_socket":"/tmp/test.sock"}`)
				case "bad JSON":
					mustWrite(t, path, "{")
				case "truncated JSON":
					mustWrite(t, path, `{"version":1,"node_id":"node"`)
				case "invalid version":
					mustWrite(t, path, `{"version":2,"node_id":"node"}`)
				case "invalid socket":
					mustWrite(t, path, `{"version":1,"control_socket":"relative"}`)
				case "directory":
					if err := os.Mkdir(path, 0o700); err != nil {
						t.Fatal(err)
					}
				case "unreadable":
					mustWrite(t, path, "{}")
					if err := os.Chmod(path, 0); err != nil {
						t.Fatal(err)
					}
					t.Cleanup(func() { os.Chmod(path, 0o600) })
				}
				plain, err := plainDoctorSelected(goos, path)
				want := goos == "darwin" && (kind == "missing" || kind == "plain" || kind == "bad JSON" || kind == "truncated JSON" || kind == "invalid version")
				if plain != want {
					t.Errorf("plain=%t want=%t err=%v", plain, want, err)
				}
				wantError := goos == "darwin" && (kind == "directory" || kind == "unreadable")
				if (err != nil) != wantError {
					t.Errorf("config error=%v wantError=%t for %s", err, wantError, kind)
				}
			})
		}
	}
}
