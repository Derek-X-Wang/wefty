package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/internal/agentinstall"
	"github.com/Derek-X-Wang/wefty/internal/connprofile"
	"github.com/Derek-X-Wang/wefty/internal/upgradecompat"
	"github.com/Derek-X-Wang/wefty/l1"
)

func TestUpgradeEnrolledPlainActivation(t *testing.T) {
	for _, jsonMode := range []bool{false, true} {
		t.Run(map[bool]string{false: "human", true: "json"}[jsonMode], func(t *testing.T) {
			f := newRollingFixture(t)
			f.operations.requestRolling = false
			var stdout, stderr bytes.Buffer
			mustRolling(t, executeUpgradeWithHost(t.Context(), f.host, globalOptions{jsonOutput: jsonMode}, false, &stdout, &stderr))
			c, err := f.install.Read()
			mustRolling(t, err)
			if !f.promoted || c.Primary == f.primary.ID || c.Generations[f.primary.ID].Role != "retiring" {
				t.Fatal("plain upgrade did not activate a generation", c)
			}
			if jsonMode {
				var got map[string]json.RawMessage
				mustRolling(t, json.Unmarshal(stdout.Bytes(), &got))
				for _, key := range []string{"version", "lease_renewal_ns", "setup", "running_work", "already_at", "waiting_nonrestartable", "stopping_restartable", "restart_pending"} {
					if _, ok := got[key]; !ok {
						t.Errorf("missing rolling report field %s", key)
					}
				}
				if stderr.Len() != 0 {
					t.Fatal("JSON wrote human progress", stderr.String())
				}
			} else if !strings.Contains(stdout.String(), "Installed version: "+f.commit) {
				t.Fatal(stdout.String())
			}
		})
	}
}

func TestUpgradeEnrolledPlainKeepsLiveRetirees(t *testing.T) {
	f := newRollingFixture(t)
	resident := f.claim(t, "resident", contract.JobClassOneShot, false)
	mustRolling(t, plainUpgrade(t, f))
	c, err := f.install.Read()
	mustRolling(t, err)
	previous := c.Primary
	mustRolling(t, os.WriteFile(filepath.Join(f.paths.bin, "wefty"), []byte("force another activation"), 0700))
	mustRolling(t, plainUpgrade(t, f))
	c, err = f.install.Read()
	mustRolling(t, err)
	if c.Primary == previous || c.Generations[previous].Role != "retiring" || c.Generations[f.primary.ID].Role != "retiring" {
		t.Fatal("overlap lost", c)
	}
	job, err := f.job(t.Context(), resident.Job.JobID)
	mustRolling(t, err)
	if len(job.Attempts) != 1 || job.CurrentAttemptID != resident.Lease.AttemptID || job.Attempts[0].State != contract.AttemptRunning {
		t.Fatal("resident changed", job)
	}
}

func plainUpgrade(t *testing.T, f *rollingFixture) error {
	t.Helper()
	f.operations.requestRolling = false
	_, err := runUpgrade(t.Context(), f.host, false, io.Discard)
	return err
}

func TestUpgradeEnrolledPlainRecovery(t *testing.T) {
	for _, phase := range []string{"candidate_ready", "promotion_committed", "admission_settling"} {
		t.Run(phase, func(t *testing.T) {
			f := newRollingFixture(t)
			enableRollingRecoveryFixture(t, f)
			f.operations.rollingStep = func(got string) error {
				if got == phase {
					return errors.New("interrupted " + phase)
				}
				return nil
			}
			err := plainUpgrade(t, f)
			if err == nil || !strings.Contains(err.Error(), "interrupted "+phase) {
				t.Fatal("did not reach interruption", err)
			}
			prior, err := readUpgradeRolling(f.paths)
			mustRolling(t, err)
			f.operations.rollingStep = nil
			report, err := runUpgrade(t.Context(), f.host, false, io.Discard)
			mustRolling(t, err)
			if report.RecoveredOperation != prior.OperationID {
				t.Fatal("did not recover exact operation", report)
			}
		})
	}
}

func TestUpgradeEnrolledPlainCompatibilityRefusal(t *testing.T) {
	f := newRollingFixture(t)
	read := f.operations.generations
	f.operations.generations = func(ctx context.Context, p connprofile.Profile) (l1.NodeGenerationInventory, error) {
		in, err := read(ctx, p)
		for i := range in.Generations {
			in.Generations[i].RetirementProtocolVersions = nil
		}
		return in, err
	}
	f.launchd.commands = nil
	err := plainUpgrade(t, f)
	var refused *upgradeRollingRefusedError
	if !errors.As(err, &refused) || refused.Reason != upgradecompat.LegacyPrimary || commandExitCode(err) != exitConflict || len(f.launchd.mutating()) != 0 {
		t.Fatal("compatibility gate bypassed", err)
	}
}

// No L1 or real launchd is opened by these setup and binary tests.
func enrolledSetupFixture(t *testing.T) (*setupFixture, setupPaths) {
	t.Helper()
	f := newConversionFixture(t)
	store := addConversionGeneration(t, f)
	paths, err := resolveSetupPaths(f.home)
	mustRolling(t, err)
	catalog, err := store.CheckMaintenance(nil)
	mustRolling(t, err)
	if catalog == nil {
		t.Fatal("expected a verified quiet enrolled catalog")
	}
	return f.setupFixture, paths
}

func TestSetupEnrolledPlainRefusal(t *testing.T) {
	f, _ := enrolledSetupFixture(t)
	f.launchd.commands = nil
	before := homeSnapshot(t, f.home)
	_, err := runSetup(t.Context(), f.host, setupRequest{})
	if commandExitCode(err) != exitConflict {
		t.Fatal("want typed conflict", err)
	}
	for _, hint := range []string{"enrolled", "wefty upgrade", "wefty setup --remove", "--role join --convert-plain"} {
		if !strings.Contains(err.Error(), hint) {
			t.Errorf("missing %s: %v", hint, err)
		}
	}
	if len(f.launchd.commands) != 0 {
		t.Fatal("refusal reached launchd")
	}
	if !reflect.DeepEqual(before, homeSnapshot(t, f.home)) {
		t.Fatal("refusal changed files")
	}
}

func TestSetupEnrolledPlainRealBinaryExit(t *testing.T) {
	if runtime.GOOS != "darwin" {
		t.Skip("plain setup is Mac-only")
	}
	f, paths := enrolledSetupFixture(t)
	binary := buildWefty(t)
	for _, jsonMode := range []bool{false, true} {
		args := []string{"setup"}
		if jsonMode {
			args = append([]string{"--json"}, args...)
		}
		code, output := runWeftyWithEnv(t, binary, 15*time.Second, []string{"HOME=" + f.home, "WEFTY_PROFILE=" + paths.profile}, args...)
		if code != exitConflict || !strings.Contains(output, "wefty upgrade") {
			t.Fatalf("exit=%d output=%s", code, output)
		}
		if jsonMode && !json.Valid([]byte(output)) {
			t.Fatal("invalid JSON", output)
		}
	}
}

func TestUpgradeEnrolledPendingMaintenanceRealBinaryExit(t *testing.T) {
	if runtime.GOOS != "darwin" {
		t.Skip("plain upgrade is Mac-only")
	}
	f, paths := enrolledSetupFixture(t)
	mustRolling(t, os.WriteFile(filepath.Join(paths.data, agentinstall.MaintenanceFile), []byte("pending intent"), 0600))
	binary := buildWefty(t)
	for _, rolling := range []bool{false, true} {
		args := []string{"--json", "upgrade"}
		if rolling {
			args = append(args, "--rolling")
		}
		code, output := runWeftyWithEnv(t, binary, 15*time.Second, []string{"HOME=" + f.home, "WEFTY_PROFILE=" + paths.profile}, args...)
		if code != exitConflict || !strings.Contains(output, "pending generation maintenance") || !strings.Contains(output, "wefty setup --remove") || !strings.Contains(output, `"code": "conflict"`) {
			t.Fatalf("exit=%d output=%s", code, output)
		}
	}
}

func TestSetupEnrolledUnfinishedOperation(t *testing.T) {
	f, paths := enrolledSetupFixture(t)
	profile, err := connprofile.Read(paths.profile)
	mustRolling(t, err)
	receipt := &upgradeRollingReceipt{Version: 1, NodeID: profile.Identities.Node, OperationID: "unfinished-1233"}
	mustRolling(t, writeUpgradeRolling(paths, receipt, "candidate_loading"))
	before := homeSnapshot(t, f.home)
	f.launchd.commands = nil
	_, err = runSetup(t.Context(), f.host, setupRequest{})
	if commandExitCode(err) != exitConflict || !strings.Contains(err.Error(), "unfinished operation unfinished-1233 at candidate_loading") {
		t.Fatal("missing operation refusal", err)
	}
	if len(f.launchd.commands) != 0 || !reflect.DeepEqual(before, homeSnapshot(t, f.home)) {
		t.Fatal("refusal changed installation")
	}
}
