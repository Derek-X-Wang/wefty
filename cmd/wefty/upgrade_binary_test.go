package main

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Derek-X-Wang/wefty/l1"
	"github.com/Derek-X-Wang/wefty/l3"
)

func TestUpgradeRealBinaryHelpAndUsage(t *testing.T) {
	binary := buildWefty(t)
	for _, args := range [][]string{{"upgrade", "--help"}, {"--json", "upgrade", "--help"}, {"--fabric=tsnet", "--profile=/missing-profile", "upgrade", "--help"}} {
		code, stdout, stderr := runPolishBinary(t, binary, args...)
		if code != 0 || len(stderr) != 0 || (!bytes.Contains(stdout, []byte("-yes")) || !bytes.Contains(stdout, []byte("-drain-timeout"))) {
			t.Fatalf("help opens machine state or misses --yes: %d %s %s", code, stdout, stderr)
		}
	}
	for _, args := range [][]string{{"upgrade", "--from=/tmp"}, {"upgrade", "extra"}, {"upgrade", "--yes=nope"}, {"upgrade", "--drain-timeout=0"}, {"upgrade", "--drain-timeout=-1m"}, {"upgrade", "--drain-timeout=bad"}} {
		code, _, stderr := runPolishBinary(t, binary, args...)
		if code != exitUsage || len(stderr) == 0 {
			t.Fatalf("usage exit: %v %d %s", args, code, stderr)
		}
	}
}

func TestUpgradeRealBinariesReportCommitAndPreflightCopies(t *testing.T) {
	if testing.Short() {
		t.Skip("builds four real binaries")
	}
	destination := t.TempDir()
	commit := strings.Repeat("a", 40)
	build := exec.Command("go", "build", "-ldflags=-X main.version="+commit, "-o", destination+string(filepath.Separator), ".", "../wefty-l1", "../wefty-l3", "../wefty-agent")
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build versioned binaries: %v %s", err, output)
	}
	home := t.TempDir()
	for _, name := range []string{"wefty", "wefty-l1", "wefty-l3", "wefty-agent"} {
		t.Run(name, func(t *testing.T) {
			command := exec.Command(filepath.Join(destination, name), "--version")
			command.Env = []string{"HOME=" + home, "PATH=/usr/bin:/bin", "WEFTY_PROFILE=/missing-profile"}
			output, err := command.CombinedOutput()
			if err != nil || strings.TrimSpace(string(output)) != commit {
				t.Fatalf("version touched installation or profile: %s %v", output, err)
			}
		})
	}
	// Setup asks these real binaries for their version through the default
	// runner and records it; status shows it.
	t.Run("setup-records-version", func(t *testing.T) {
		f := newSetupFixture(t)
		report, err := runSetup(t.Context(), f.host, setupRequest{from: destination})
		if err != nil {
			t.Fatal(err)
		}
		if receipt := readSetupReceipt(t, f.home); receipt.InstalledVersion != commit {
			t.Fatalf("setup receipt installed_version = %q, want %q", receipt.InstalledVersion, commit)
		}
		if notes := strings.Join(report.Notes, "\n"); strings.Contains(notes, "installed version") {
			t.Fatalf("setup notes = %v", report.Notes)
		}
		assertStatusShowsVersion(t, f.home, report.NodeID, commit)
		for _, name := range setupBinaries {
			command := exec.CommandContext(t.Context(), filepath.Join(destination, name), "--profile-preflight", "--plain", "--profile="+report.Profile)
			command.Env = []string{"HOME=" + home, "PATH=/usr/bin:/bin", "WEFTY_PROFILE=/missing-profile"}
			output, err := command.CombinedOutput()
			if err != nil || strings.TrimSpace(string(output)) != "profile-readable plain" {
				t.Fatalf("%s plain profile preflight: %s %v", name, output, err)
			}
		}
	})
	for _, name := range []string{"wefty-l1", "wefty-l3"} {
		t.Run(name+"-preflight", func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "copy.sqlite")
			if name == "wefty-l1" {
				var store *l1.Store
				var err error
				withProductionSQLiteSync(func() { store, err = l1.OpenStore(path, l1.StoreOptions{}) })
				if err != nil {
					t.Fatal(err)
				}
				store.Close()
			} else {
				store, err := l3.OpenStore(path, l3.StoreOptions{})
				if err != nil {
					t.Fatal(err)
				}
				store.Close()
			}
			command := exec.Command(filepath.Join(destination, name), "--state-preflight", "--db="+path)
			command.Env = []string{"HOME=" + home, "PATH=/usr/bin:/bin", "WEFTY_PROFILE=/missing-profile"}
			output, err := command.CombinedOutput()
			if err != nil {
				t.Fatalf("preflight attempted to start services: %s %v", output, err)
			}
			malformed := filepath.Join(t.TempDir(), "bad.sqlite")
			if err := os.WriteFile(malformed, []byte("invalid db"), 0o600); err != nil {
				t.Fatal(err)
			}
			command = exec.Command(filepath.Join(destination, name), "--state-preflight", "--db="+malformed)
			command.Env = []string{"HOME=" + home, "PATH=/usr/bin:/bin"}
			_, err = command.CombinedOutput()
			if exit, ok := err.(*exec.ExitError); !ok || exit.ExitCode() != 1 {
				t.Fatalf("preflight failure exit: %v", err)
			}
		})
	}
}
