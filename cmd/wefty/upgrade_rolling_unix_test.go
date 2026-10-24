//go:build unix

package main

import (
	"errors"
	"io"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/internal/serviceplan"
	"github.com/Derek-X-Wang/wefty/internal/upgradecompat"
)

func TestUpgradeRollingRealBinaryRefusal(t *testing.T) {
	var f *upgradeFixture
	if runtime.GOOS == "linux" {
		// A real Linux fleet install records LinuxLogRoot in both the
		// setup receipt and fleet record. No systemd call precedes this gate.
		f, _ = newLinuxUpgradeFixture(t, serviceplan.Join)
	} else {
		f = newFleetUpgradeFixture(t, serviceplan.Join)
	}
	code, output := runWeftyWithEnv(t, buildWefty(t), 10*time.Second,
		[]string{"HOME=" + f.home, "WEFTY_PROFILE=" + filepath.Join(f.home, setupFolders.config, "profile.json")},
		"--json", "upgrade", "--rolling")
	if code != exitConflict || !strings.Contains(output, "unsupported_installation") || !strings.Contains(output, `"code": "conflict"`) {
		t.Fatalf("real binary refused rolling: exit=%d output=%s", code, output)
	}
}

func TestUpgradeRollingLinuxFleetRefusal(t *testing.T) {
	f, systemd := newLinuxUpgradeFixture(t, serviceplan.Join)
	f.operations.requestRolling = true
	_, err := runUpgrade(t.Context(), f.host, false, io.Discard)
	var refused *upgradeRollingRefusedError
	if !errors.As(err, &refused) || refused.Reason != upgradecompat.UnsupportedInstallation || commandExitCode(err) != exitConflict {
		t.Fatal("wrong Linux fleet rolling refusal", err)
	}
	if len(systemd.calls) != 0 {
		t.Fatal("rolling refusal accessed systemd", systemd.calls)
	}
}
