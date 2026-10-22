package main

import (
	"context"
	"github.com/Derek-X-Wang/wefty/internal/serviceplan"
	"github.com/Derek-X-Wang/wefty/internal/systemduser"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestSetupLinuxFixedToolLayoutsAndMissing(t *testing.T) {
	for _, layout := range []string{"usr", "bin", "missing"} {
		t.Run(layout, func(t *testing.T) {
			f, e, _, s := newLinuxFleetFixture(t, "join")
			files, mirror := linuxTestFiles(t)
			f.host.linuxFiles = files
			for _, tool := range []string{"systemctl", "loginctl", "xdg-open"} {
				from := filepath.Join(mirror, "usr", "bin", tool)
				if layout != "usr" {
					if err := os.Remove(from); err != nil {
						t.Fatal(err)
					}
				}
				if layout == "bin" {
					to := filepath.Join(mirror, "bin", tool)
					if err := os.MkdirAll(filepath.Dir(to), 0755); err != nil {
						t.Fatal(err)
					}
					if err := os.WriteFile(to, []byte("fake"), 0755); err != nil {
						t.Fatal(err)
					}
				}
			}
			want := "/usr/bin/"
			if layout == "bin" {
				want = "/bin/"
			}
			f.host.systemctl = func(ctx context.Context, name string, args ...string) ([]byte, error) {
				if layout == "missing" {
					t.Error("executed a missing systemctl")
				}
				if name != want+"systemctl" {
					t.Errorf("wrong fixed systemctl: %s", name)
				}
				return s.run(ctx, "/usr/bin/systemctl", args...)
			}
			f.host.doctorCommand = func(_ context.Context, name string, args ...string) ([]byte, error) {
				if name != want+"loginctl" {
					t.Errorf("wrong fixed loginctl: %s", name)
				}
				return []byte("Linger=yes"), nil
			}
			f.host.ownerLogin = nil
			f.host.browserCommand = func(_ context.Context, name string, args ...string) error {
				if name != want+"xdg-open" {
					t.Errorf("wrong fixed opener: %s", name)
				}
				return nil
			}
			r := linuxSetupRequest(t, "join")
			r.withPerson = true
			_, err := runSetup(t.Context(), f.host, r)
			if layout == "missing" {
				if err == nil || !strings.Contains(err.Error(), "systemctl not found at /usr/bin or /bin") || strings.Contains(err.Error(), "242") {
					t.Fatalf("missing tool misdiagnosed: %v", err)
				}
				if len(e.roles) != 0 {
					t.Fatal("used keys with a missing tool")
				}
			} else if err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestSetupLinuxTrustedDropInsAndFragmentAliasesRecover(t *testing.T) {
	for _, aliasKind := range []string{"linked-home", "stow-systemd"} {
		t.Run(aliasKind, func(t *testing.T) {
			f, e, _, s := newLinuxFleetFixture(t, "join")
			tools := filepath.Join(f.home, "tools")
			if err := os.MkdirAll(tools, 0700); err != nil {
				t.Fatal(err)
			}
			for _, tool := range []string{"go", "git", "node", "gh", "claude", "codex"} {
				if err := os.WriteFile(filepath.Join(tools, tool), []byte("#!/bin/sh\nexit 0\n"), 0700); err != nil {
					t.Fatal(err)
				}
			}
			f.loginPATH = tools

			files, mirror := linuxTestFiles(t)
			f.host.linuxFiles = files
			drop := "/usr/lib/systemd/user/service.d/10-timeout-abort.conf"
			path := filepath.Join(mirror, strings.TrimPrefix(drop, "/"))
			if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(path, []byte("[Service]\nTimeoutStopFailureMode=abort\n"), 0644); err != nil {
				t.Fatal(err)
			}
			alias := filepath.Join(t.TempDir(), "home")
			if err := os.Symlink(f.home, alias); err != nil {
				t.Fatal(err)
			}
			if aliasKind == "linked-home" {
				f.host.home = alias
			} else {
				target := filepath.Join(f.home, "stow", "systemd")
				if err := os.MkdirAll(target, 0700); err != nil {
					t.Fatal(err)
				}
				if err := os.MkdirAll(filepath.Join(f.home, ".config"), 0700); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(target, filepath.Join(f.home, ".config", "systemd")); err != nil {
					t.Fatal(err)
				}
			}
			f.host.systemctl = func(ctx context.Context, name string, args ...string) ([]byte, error) {
				b, err := s.run(ctx, name, args...)
				if len(args) > 2 && args[1] == "show" && s.known[args[2]] {
					b = []byte(strings.ReplaceAll(string(b), "DropInPaths=", "DropInPaths="+drop))
					b = []byte(strings.ReplaceAll(string(b), "FragmentPath="+f.home, "FragmentPath="+alias))
				}
				return b, err
			}
			r := linuxSetupRequest(t, "join")
			for attempt := 0; attempt < 2; attempt++ {
				report, err := runSetup(t.Context(), f.host, r)
				if err != nil {
					t.Fatalf("attempt %d: %v, %+v", attempt, err, report.Readiness)
				}
				for _, check := range report.Readiness {
					if check.Status != "VERIFIED" {
						t.Fatal(check)
					}
				}
			}
			if len(e.roles) != 2 {
				t.Fatal("rerun consumed keys", e.roles)
			}
			paths, _ := resolveFleetMaintenancePaths(f.home, f.host)
			doctor, diagnosis := runFleetDoctor(t.Context(), f.host, globalOptions{}, paths, serviceplan.Join)
			if diagnosis != nil || !doctor.Healthy {
				t.Fatalf("informational policy changed doctor health: %+v %v", doctor, diagnosis)
			}
			seen := 0
			for _, finding := range doctor.Findings {
				if strings.HasSuffix(finding.Check, ":system-drop-ins") {
					seen++
					if finding.Severity != "ok" || !strings.Contains(finding.Detail, drop) {
						t.Fatal(finding)
					}
				}
			}
			if seen != 2 {
				t.Fatal("doctor omitted informational system policy", seen)
			}
		})
	}
}
func TestSetupLinuxPreflightRefusesUnloadedUnitDropIn(t *testing.T) {
	for _, location := range []string{"home", "root-unit", "root-control", "user-control", "user-data", "runtime"} {
		t.Run(location, func(t *testing.T) {
			f, e, _, _ := newLinuxFleetFixture(t, "join")
			files, mirror := linuxTestFiles(t)
			f.host.linuxFiles = files
			folder := filepath.Join(f.home, ".config", "systemd", "user")
			switch location {
			case "root-unit":
				folder = filepath.Join(mirror, "etc", "systemd", "user")
			case "root-control":
				folder = filepath.Join(mirror, "etc", "systemd", "user.control")
			case "user-data":
				folder = filepath.Join(f.home, ".local", "share", "systemd", "user")
			case "user-control":
				folder = filepath.Join(f.home, ".config", "systemd", "user.control")
			case "runtime":
				f.host.xdgRuntimeDir = t.TempDir()
				folder = filepath.Join(f.host.xdgRuntimeDir, "systemd", "user")
			}
			drop := filepath.Join(folder, "dev.wefty.node-agent.service.d", "override.conf")
			if err := os.MkdirAll(filepath.Dir(drop), 0700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(drop, []byte("[Service]\nExecStart=\n"), 0600); err != nil {
				t.Fatal(err)
			}
			_, err := runSetup(t.Context(), f.host, linuxSetupRequest(t, "join"))
			if err == nil || !strings.Contains(err.Error(), "drop-ins") {
				t.Fatalf("unloaded override was missed: %v", err)
			}
			if len(e.roles) != 0 {
				t.Fatal("used keys before finding override", e.roles)
			}
			if _, err := os.Stat(filepath.Join(f.home, ".config", "wefty", fleetSetupRecordName)); !os.IsNotExist(err) {
				t.Fatal("staged before override refusal")
			}
		})
	}
}

func TestSetupLinuxFixedToolTrust(t *testing.T) {
	for _, kind := range []string{"root-symlink", "unsafe-file", "unsafe-symlink", "unsafe-owner", "unsafe-parent"} {
		t.Run(kind, func(t *testing.T) {
			files, mirror := linuxTestFiles(t)
			path := filepath.Join(mirror, "usr", "bin", "systemctl")
			if kind == "root-symlink" || kind == "unsafe-symlink" {
				target := filepath.Join(mirror, "bin", "systemctl")
				if err := os.MkdirAll(filepath.Dir(target), 0755); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(target, []byte("fake target"), 0755); err != nil {
					t.Fatal(err)
				}
				if kind == "unsafe-symlink" {
					if err := os.Chmod(target, 0777); err != nil {
						t.Fatal(err)
					}
				}
				if err := os.Remove(path); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink("../../bin/systemctl", path); err != nil {
					t.Fatal(err)
				}
			}
			if kind == "unsafe-file" {
				if err := os.Chmod(path, 0777); err != nil {
					t.Fatal(err)
				}
			}
			if kind == "unsafe-parent" {
				if err := os.Chmod(filepath.Dir(path), 0777); err != nil {
					t.Fatal(err)
				}
			}
			if kind == "unsafe-owner" {
				lstat, stat := files.Lstat, files.Stat
				files.Lstat = func(p string) (fs.FileInfo, error) {
					if p == "/usr/bin/systemctl" {
						return os.Lstat(path)
					}
					return lstat(p)
				}
				files.Stat = func(p string) (fs.FileInfo, error) {
					if p == "/usr/bin/systemctl" {
						return os.Stat(path)
					}
					return stat(p)
				}
			}
			tool, err := systemduser.ResolveTool("systemctl", files)
			if kind == "root-symlink" {
				if err != nil || tool != "/usr/bin/systemctl" {
					t.Fatalf("safe symlink rejected: %s %v", tool, err)
				}
			} else if err == nil {
				t.Fatal("unsafe tool accepted", tool)
			}
		})
	}
}

func TestSetupLinuxPreflightIgnoresNonDropInEntries(t *testing.T) {
	f, _, _, _ := newLinuxFleetFixture(t, "join")
	folder := filepath.Join(f.home, ".config", "systemd", "user", "dev.wefty.node-agent.service.d")
	if err := os.MkdirAll(filepath.Join(folder, "directory.conf"), 0700); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{".backup.conf", "pending.conf.tmp"} {
		if err := os.WriteFile(filepath.Join(folder, name), []byte("unused"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := runSetup(t.Context(), f.host, linuxSetupRequest(t, "join")); err != nil {
		t.Fatalf("non-drop-in entries blocked setup: %v", err)
	}
}
