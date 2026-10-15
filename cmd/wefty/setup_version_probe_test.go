package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// assertSetupWithoutVersion runs setup with one broken binary and checks that
// it completes, records no version and explains why in a note.
func assertSetupWithoutVersion(t *testing.T, f *setupFixture, note string) {
	t.Helper()
	report, err := runSetup(t.Context(), f.host, setupRequest{})
	if err != nil {
		t.Fatalf("setup did not complete: %v", err)
	}
	if receipt := readSetupReceipt(t, f.home); receipt.InstalledVersion != "" {
		t.Fatalf("setup recorded installed_version %q", receipt.InstalledVersion)
	}
	notes := strings.Join(report.Notes, "\n")
	if !strings.Contains(notes, note) || !strings.Contains(notes, "setup recorded no installed version") {
		t.Fatalf("setup notes = %v", report.Notes)
	}
}

// A broken binary that prints without end must not fill memory: the probe
// stops it once its answer is longer than any version. This stand-in floods
// 16 MiB, then leaves a marker; a probe that reads everything lets it finish.
func TestSetupVersionProbeStopsABinaryThatFloodsOutput(t *testing.T) {
	f := newSetupFixture(t)
	marker := filepath.Join(t.TempDir(), "flood-finished")
	script := "#!/bin/sh\nline=" + strings.Repeat("x", 1023) + "\ni=0\nwhile [ $i -lt 16384 ]; do echo \"$line\"; i=$((i+1)); done\n: > '" + marker + "'\n"
	if err := os.WriteFile(filepath.Join(f.source, "wefty-l3"), []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	assertSetupWithoutVersion(t, f, "wefty-l3 --version printed more than 256 bytes")
	if _, err := os.Stat(marker); err == nil {
		t.Fatal("the version probe read the whole 16 MiB flood instead of stopping the binary at its output limit")
	}
}

// A binary that never answers is stopped at the probe budget, and setup goes
// on without a version.
func TestSetupVersionProbeStopsABinaryThatHangs(t *testing.T) {
	f := newSetupFixture(t)
	f.host.versionProbeBudget = 300 * time.Millisecond
	if err := os.WriteFile(filepath.Join(f.source, "wefty-agent"), []byte("#!/bin/sh\nexec sleep 30\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	assertSetupWithoutVersion(t, f, "wefty-agent --version did not answer within 300ms")
	if elapsed := time.Since(started); elapsed > 10*time.Second {
		t.Fatalf("setup waited %s for a hanging binary", elapsed)
	}
}
