package main

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/internal/serviceplan"
)

func TestSetupLinuxPreflightsProgramBeforeKeys(t *testing.T) {
	f, e, _, _ := newLinuxFleetFixture(t, "join")
	f.home = filepath.Join(f.home, "o'brien")
	f.host.home = f.home
	if err := os.MkdirAll(f.home, 0700); err != nil {
		t.Fatal(err)
	}
	_, err := runSetup(t.Context(), f.host, linuxSetupRequest(t, "join"))
	if err == nil || !strings.Contains(err.Error(), "executable path") {
		t.Fatalf("unsafe program path not refused: %v", err)
	}
	if len(e.roles) != 0 {
		t.Fatal("used a key before unit validation", e.roles)
	}
	if _, err := os.Stat(filepath.Join(f.home, ".config", "wefty", fleetSetupRecordName)); !os.IsNotExist(err) {
		t.Fatal("staged before unit validation")
	}
}
func TestSetupLinuxPreflightsSystemdVersionBeforeKeys(t *testing.T) {
	for _, version := range []string{"systemd 241\n", "unknown\n", "systemd 242 (v242)\n+PAM\n", "systemd 255 (distribution)\n+PAM\n", "systemd 242evil\n"} {
		t.Run(strings.Fields(version)[0]+strings.ReplaceAll(strings.TrimSpace(version), "/", "-"), func(t *testing.T) {
			f, e, _, s := newLinuxFleetFixture(t, "join")
			probes := 0
			f.host.systemctl = func(ctx context.Context, name string, args ...string) ([]byte, error) {
				if len(args) == 1 && args[0] == "--version" {
					probes++
					if len(e.roles) > 0 {
						t.Fatal("version check after enrolment")
					}
					return []byte(version), nil
				}
				return s.run(ctx, name, args...)
			}
			r := linuxSetupRequest(t, "join")
			r.afterFleetStep = func(step string) error {
				if step == "stage-record" {
					return errLinuxStageReached
				}
				return nil
			}
			_, err := runSetup(t.Context(), f.host, r)
			accepted := strings.HasPrefix(version, "systemd 242 (") || strings.HasPrefix(version, "systemd 255 (")
			if accepted {
				if !errors.Is(err, errLinuxStageReached) {
					t.Fatal(err)
				}
			} else {
				if err == nil || !strings.Contains(err.Error(), "systemd 242") {
					t.Fatalf("version did not fail closed: %v", err)
				}
				if _, err := os.Stat(filepath.Join(f.home, ".config", "wefty", fleetSetupRecordName)); !os.IsNotExist(err) {
					t.Fatal("staged before version check")
				}
			}
			if probes == 0 || len(e.roles) != 0 {
				t.Fatal("version probe missing or used key", probes, e.roles)
			}
		})
	}
}
func TestSetupLinuxSecuresLogFolders(t *testing.T) {
	for _, kind := range []string{"private", "shared-parent", "linked-parent", "linked-logs", "foreign-owner"} {
		t.Run(kind, func(t *testing.T) {
			f, e, _, _ := newLinuxFleetFixture(t, "join")
			state := filepath.Join(f.home, "state")
			logs := filepath.Join(state, "wefty")
			if err := os.MkdirAll(logs, 0700); err != nil {
				t.Fatal(err)
			}
			f.host.xdgStateHome = state
			if err := os.Chmod(logs, 0777); err != nil {
				t.Fatal(err)
			}
			switch kind {
			case "shared-parent":
				if err := os.Chmod(state, 0777); err != nil {
					t.Fatal(err)
				}
			case "linked-parent":
				f.host.xdgStateHome = filepath.Join(f.home, "state-link")
				if err := os.Symlink(state, f.host.xdgStateHome); err != nil {
					t.Fatal(err)
				}
			case "linked-logs":
				if err := os.Remove(logs); err != nil {
					t.Fatal(err)
				}
				target := filepath.Join(f.home, "target")
				if err := os.Mkdir(target, 0700); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(target, logs); err != nil {
					t.Fatal(err)
				}
			case "foreign-owner":
				f.host.euid += 12345
			}
			_, err := runSetup(t.Context(), f.host, linuxSetupRequest(t, "join"))
			if kind == "private" {
				if err != nil {
					t.Fatal(err)
				}
				info, _ := os.Stat(logs)
				if info.Mode().Perm() != 0700 {
					t.Fatalf("logs mode=%o", info.Mode().Perm())
				}
			} else if err == nil || len(e.roles) != 0 {
				t.Fatalf("unsafe logs accepted or used key: %v %v", err, e.roles)
			}
		})
	}
}
func TestSetupLinuxLinkedHomeResolvesState(t *testing.T) {
	f, _, _, _ := newLinuxFleetFixture(t, "join")
	alias := filepath.Join(t.TempDir(), "home")
	if err := os.Symlink(f.home, alias); err != nil {
		t.Fatal(err)
	}
	f.host.home = alias
	f.host.xdgStateHome = filepath.Join(alias, "state")
	report, err := runSetup(t.Context(), f.host, linuxSetupRequest(t, "join"))
	if err != nil {
		t.Fatal(err)
	}
	if report.LogDir != filepath.Join(f.home, "state", "wefty") {
		t.Fatal(report.LogDir)
	}
}
func TestSetupLinuxRecordedLogRootSurvivesEnvironmentDrift(t *testing.T) {
	for _, boundary := range []string{"stage-record", "receipt", "complete"} {
		t.Run(boundary, func(t *testing.T) {
			f, e, _, s := newLinuxFleetFixture(t, "join")
			f.host.xdgStateHome = filepath.Join(f.home, "old-state")
			original := filepath.Join(f.host.xdgStateHome, "wefty")
			r := linuxSetupRequest(t, "join")
			if boundary != "complete" {
				r.afterFleetStep = func(step string) error {
					if step == boundary {
						return errLinuxStageReached
					}
					return nil
				}
			}
			_, err := runSetup(t.Context(), f.host, r)
			if boundary == "complete" {
				if err != nil {
					t.Fatal(err)
				}
			} else if !errors.Is(err, errLinuxStageReached) {
				t.Fatal(err)
			}
			keys := len(e.roles)
			f.host.xdgStateHome = filepath.Join(f.home, "new-state")
			// The fake ready observer uses configuration paths only, independent of logs.
			s.calls = nil
			r.afterFleetStep = nil
			report, err := runSetup(t.Context(), f.host, r)
			if err != nil {
				t.Fatal(err)
			}
			if report.LogDir != original {
				t.Fatalf("silently changed recorded root: %s", report.LogDir)
			}
			if boundary != "stage-record" && len(e.roles) != keys {
				t.Fatal("resume enrolled again")
			}
			for _, name := range []string{fleetSetupRecordName, setupManifestName} {
				b, err := os.ReadFile(filepath.Join(f.home, ".config", "wefty", name))
				if err != nil {
					t.Fatal(err)
				}
				var record map[string]any
				if err := json.Unmarshal(b, &record); err != nil {
					t.Fatal(err)
				}
				if record["linux_log_root"] != original {
					t.Fatalf("%s omitted log root", name)
				}
			}
			if _, err := os.Stat(f.host.xdgStateHome); !os.IsNotExist(err) {
				t.Fatal("created changed state root")
			}
			paths, _ := resolvePlatformMaintenancePaths(f.home, "linux", "")
			doctor, _ := runFleetDoctor(t.Context(), f.host, globalOptions{}, paths, serviceplan.Join)
			found := false
			for _, finding := range doctor.Findings {
				if finding.Check == "path:old-state/wefty" {
					found = true
					if finding.Severity != "ok" {
						t.Fatal(finding)
					}
				}
				if strings.Contains(finding.Check, "new-state") {
					t.Fatal("doctor followed environment", finding)
				}
			}
			if !found {
				t.Fatal("doctor ignored recorded log folder")
			}
		})
	}
}
func TestSetupLinuxReportIncludesLingering(t *testing.T) {
	for _, value := range []string{"yes", "no", "unknown"} {
		t.Run(value, func(t *testing.T) {
			f, _, _, _ := newLinuxFleetFixture(t, "join")
			f.host.doctorCommand = func(_ context.Context, name string, args ...string) ([]byte, error) {
				if name != "/usr/bin/loginctl" || strings.Join(args, " ") != "show-user owner -p Linger" {
					t.Fatal(name, args)
				}
				return []byte("Linger=" + value), nil
			}
			report, err := runSetup(t.Context(), f.host, linuxSetupRequest(t, "join"))
			if err != nil {
				t.Fatal(err)
			}
			notes := strings.Join(report.Notes, " ")
			if value == "yes" {
				if !strings.Contains(notes, "survive logout") {
					t.Fatal(notes)
				}
			} else if !strings.Contains(notes, "loginctl enable-linger 'owner'") {
				t.Fatal("missing lingering advice", notes)
			}
		})
	}
}
func TestSetupLinuxAbandonConversionNamesFlag(t *testing.T) {
	f, _, _, _ := newLinuxFleetFixture(t, "join")
	r := linuxSetupRequest(t, "join")
	r.abandonConversion = true
	_, err := runSetup(t.Context(), f.host, r)
	if err == nil || !strings.Contains(err.Error(), "--abandon-conversion is not supported on Linux") {
		t.Fatal(err)
	}
}
func TestSetupOwnerBrowserBoundsHungLinuxOpener(t *testing.T) {
	outer, cancel := context.WithTimeout(t.Context(), 7*time.Second)
	defer cancel()
	files, _ := linuxTestFiles(t)
	open := setupOwnerLoginBrowser("linux", func(ctx context.Context, name string, args ...string) error { <-ctx.Done(); return ctx.Err() }, files)
	start := time.Now()
	err := open(outer, "https://login.invalid/one-use-private")
	if err != nil || time.Since(start) > 6*time.Second {
		t.Fatalf("a launched browser timeout failed sign-in: %v", err)
	}
}
