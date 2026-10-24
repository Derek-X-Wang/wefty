package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/Derek-X-Wang/wefty/agent"
	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/internal/agentinstall"
	"github.com/Derek-X-Wang/wefty/internal/launchagent"
	"github.com/Derek-X-Wang/wefty/l1"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLegacyRecoveryRemovesOriginalAutostartAfterUnloadInterruption(t *testing.T) {
	f := newRollingFixtureWithLegacy(t, true)
	enableRollingRecoveryFixture(t, f)
	hit := false
	f.operations.rollingStep = func(phase string) error {
		if phase == "legacy_unloaded_before_plist_removal" {
			hit = true
			return errors.New("interrupt after unload")
		}
		return nil
	}
	_, err := runUpgrade(t.Context(), f.host, false, io.Discard)
	if err == nil || !hit {
		t.Fatal("missing exact unload interruption", err)
	}
	legacy := filepath.Join(f.paths.launchAgents, setupNodeAgentLabel+".plist")
	if _, err = os.Stat(legacy); err != nil {
		t.Fatal("test missed retained legacy plist", err)
	}
	f.operations.rollingStep = nil
	_, err = runUpgrade(t.Context(), f.host, false, io.Discard)
	mustRolling(t, err)
	if _, err = os.Lstat(legacy); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("legacy autostart survived completed recovery", err)
	}
	c, err := f.install.Read()
	mustRolling(t, err)
	for _, g := range c.Generations {
		if g.ID != c.Primary {
			t.Fatal("unexpected extra generation")
		}
		if _, err = os.Stat(g.Plist); err != nil {
			t.Fatal("primary autostart missing", err)
		}
	}
}
func TestLegacyRecoveryResumesPartialInitialization(t *testing.T) {
	for _, phase := range []string{"intent_recorded", "root_created", "generation_directory_created", "executable_staging_created", "executable_staging_synced", "executable_published_before_sync", "executable_staging_removed", "executable_directory_synced", "executable_created", "catalog_staging_created", "catalog_staging_synced", "catalog_published_before_sync", "catalog_staging_removed", "catalog_published"} {
		t.Run(phase, func(t *testing.T) {
			f := newRollingFixtureWithLegacy(t, true)
			enableRollingRecoveryFixture(t, f)
			hit := false
			f.install.InitializationStep = func(at string) error {
				if at == phase {
					hit = true
					return errors.New("interrupt init")
				}
				return nil
			}
			_, err := runUpgrade(t.Context(), f.host, false, io.Discard)
			if err == nil || !hit {
				t.Fatal("missing exact initialization interruption", err)
			}
			f.install.InitializationStep = nil
			_, err = runUpgrade(t.Context(), f.host, false, io.Discard)
			mustRolling(t, err)
			c, err := f.install.Read()
			mustRolling(t, err)
			if len(c.Generations) != 1 {
				t.Fatal("duplicate initialization")
			}
		})
	}
}

func testRollbackRefusalAllowsFurtherForwardRollingUpgrade(t *testing.T, phase string) {
	f := newRollingFixture(t)
	enableRollingRecoveryFixture(t, f)
	f.claim(t, "kept", contract.JobClassOneShot, false)
	service := f.claim(t, "service", contract.JobClassService, false)
	f.operations.rollingStep = func(phase string) error {
		if phase == "retirement_acknowledged" {
			return errors.New("interrupt")
		}
		return nil
	}
	_, err := runUpgrade(t.Context(), f.host, false, io.Discard)
	if err == nil {
		t.Fatal("missing interruption")
	}
	f.operations.rollingStep = nil
	c, err := f.install.Read()
	mustRolling(t, err)
	before := c.Primary
	manager := f.install.Manager.Run
	f.install.Manager.Run = func(ctx context.Context, name string, args ...string) ([]byte, error) {
		if args[0] == "print" {
			return []byte("state = waiting\npid = 0\n"), nil
		}
		return manager(ctx, name, args...)
	}
	_, err = runUpgrade(t.Context(), f.host, false, io.Discard)
	var refused *destructiveRollingRollbackError
	if !errors.As(err, &refused) {
		t.Fatal("missing typed rollback refusal", err)
	}
	f.install.Manager.Run = manager
	// Run inventory through the installed agent's private adapter and real L1
	// transport. The CLI still has only its normal operator principal.
	exposeRollingL1WithPromotionReply(t, f, nil)
	control := f.operations.generationControl
	inventoryCalls := 0
	f.operations.generationControl = func(ctx context.Context, s agentinstall.Store, r agentinstall.ControlRequest) (json.RawMessage, error) {
		if r.Action == "generation_inventory" {
			inventoryCalls++
			return requestGenerationControl(ctx, s, r)
		}
		return control(ctx, s, r)
	}
	hit := false
	f.operations.rollingStep = func(at string) error {
		if at == phase {
			hit = true
			return errors.New("interrupt successor before drain handoff")
		}
		return nil
	}
	_, err = runUpgrade(t.Context(), f.host, false, io.Discard)
	if err == nil || !hit || inventoryCalls != 1 {
		t.Fatalf("successor missed real inventory/interruption: calls=%d hit=%t err=%v", inventoryCalls, hit, err)
	}
	interrupted, err := readUpgradeRolling(f.paths)
	mustRolling(t, err)
	if interrupted.ForwardFromOperation == "" {
		t.Fatal("successor lost predecessor")
	}
	f.operations.rollingStep = nil
	_, err = runUpgrade(t.Context(), f.host, false, io.Discard)
	mustRolling(t, err)
	settled, err := readUpgradeRolling(f.paths)
	mustRolling(t, err)
	if settled.Phase != "rolled_back" {
		t.Fatal("early successor did not settle", settled.Phase)
	}
	_, err = runUpgrade(t.Context(), f.host, false, io.Discard)
	mustRolling(t, err)
	c, err = f.install.Read()
	mustRolling(t, err)
	if c.Primary == before {
		t.Fatal("rollback refusal intercepted the further forward upgrade")
	}
	job, err := f.job(t.Context(), service.Job.JobID)
	mustRolling(t, err)
	if job.LifetimeRestartCount > 1 {
		t.Fatal("forward recovery repeated service restart")
	}
	node, err := f.h.store.GetNode(t.Context(), f.node.NodeID)
	mustRolling(t, err)
	if !node.ClaimsEnabled {
		t.Fatal("forward recovery failed to restore original admission")
	}
	receipt, err := readUpgradeRolling(f.paths)
	mustRolling(t, err)
	if receipt.Phase != "complete" {
		t.Fatal("forward repair did not converge", receipt.Phase)
	}

}

func testLegacyRecoveryLoadedStopped(t *testing.T, missing bool) {
	f := newRollingFixtureWithLegacy(t, true)
	enableRollingRecoveryFixture(t, f)
	hit := false
	f.operations.rollingStep = func(phase string) error {
		if phase == "legacy_unloaded_before_plist_removal" {
			hit = true
			return errors.New("interrupt after unload")
		}
		return nil
	}
	_, err := runUpgrade(t.Context(), f.host, false, io.Discard)
	if err == nil || !hit {
		t.Fatal("missing exact unload interruption", err)
	}
	legacy := filepath.Join(f.paths.launchAgents, setupNodeAgentLabel+".plist")
	if _, err = os.Stat(legacy); err != nil {
		t.Fatal("test missed retained legacy plist", err)
	}
	f.launchd.loaded[setupNodeAgentLabel] = true
	if missing {
		mustRolling(t, os.Remove(legacy))
	}
	run := f.install.Manager.Run
	f.install.Manager.Run = func(ctx context.Context, name string, args ...string) ([]byte, error) {
		if args[0] == "print" && strings.HasSuffix(args[len(args)-1], setupNodeAgentLabel) && f.launchd.loaded[setupNodeAgentLabel] {
			return []byte("state = waiting\npid = 0\n"), nil
		}
		return run(ctx, name, args...)
	}
	f.operations.rollingStep = nil
	_, err = runUpgrade(t.Context(), f.host, false, io.Discard)
	mustRolling(t, err)
	if _, err = os.Lstat(legacy); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("legacy autostart survived completed recovery", err)
	}
	if f.launchd.loaded[setupNodeAgentLabel] {
		t.Fatal("legacy definition remains loaded")
	}
	c, err := f.install.Read()
	mustRolling(t, err)
	for _, g := range c.Generations {
		if g.ID != c.Primary {
			t.Fatal("unexpected extra generation")
		}
		if _, err = os.Stat(g.Plist); err != nil {
			t.Fatal("primary autostart missing", err)
		}
	}
}
func TestLegacyRecoveryLoadedStoppedDefinition(t *testing.T) {
	for _, missing := range []bool{false, true} {
		t.Run(fmt.Sprint(missing), func(t *testing.T) { testLegacyRecoveryLoadedStopped(t, missing) })
	}
}

func TestRollbackRefusalAllowsFurtherForwardRollingUpgrade(t *testing.T) {
	for _, phase := range []string{"prepared", "admission_closing"} {
		t.Run(phase, func(t *testing.T) { testRollbackRefusalAllowsFurtherForwardRollingUpgrade(t, phase) })
	}
}

func TestForwardDrainHandoffRefusesChangedEvidence(t *testing.T) {
	base := upgradeDrainReceipt{Version: 1, Home: "home", NodeID: "node", Revision: 3, Reason: "wefty upgrade repair", PreviousReason: "operator", RestoreClaims: true, OldBoot: "boot", ServiceQuiescence: &agent.UpgradeServiceQuiescence{OperationID: "predecessor", NodeID: "node", AgentGenerationID: "old-primary", BootSessionID: "boot", IntentRevision: 3, IntentReason: "wefty upgrade repair"}}
	cases := []struct {
		name   string
		change func(*upgradeDrainReceipt, *rollingUpgrade)
	}{
		{"operation", func(d *upgradeDrainReceipt, r *rollingUpgrade) { d.ServiceQuiescence.OperationID = "foreign" }},
		{"primary", func(d *upgradeDrainReceipt, r *rollingUpgrade) { d.ServiceQuiescence.AgentGenerationID = "another" }},
		{"boot", func(d *upgradeDrainReceipt, r *rollingUpgrade) { d.OldBoot = "another" }},
		{"revision", func(d *upgradeDrainReceipt, r *rollingUpgrade) { d.Revision++ }},
		{"reason", func(d *upgradeDrainReceipt, r *rollingUpgrade) { d.Reason = "changed" }},
		{"queue", func(d *upgradeDrainReceipt, r *rollingUpgrade) {
			d.Services = []l1.NodeActiveAttempt{{JobID: "pending"}}
		}},
		{"phase", func(d *upgradeDrainReceipt, r *rollingUpgrade) { r.receipt.Phase = "services_quiescing" }},
		{"predecessor", func(d *upgradeDrainReceipt, r *rollingUpgrade) { r.receipt.ForwardFromOperation = "foreign" }},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			payload, err := json.Marshal(base)
			mustRolling(t, err)
			var d upgradeDrainReceipt
			mustRolling(t, json.Unmarshal(payload, &d))
			r := rollingUpgrade{receipt: upgradeRollingReceipt{OperationID: "successor", Phase: "prepared", ForwardFromOperation: "predecessor", ForwardAdmission: &base}}
			tt.change(&d, &r)
			_, err = r.reconcileForwardDrain(&d)
			var refusal *apiResponseError
			if !errors.As(err, &refusal) || refusal.APIError.Code != contract.ErrorConflict {
				t.Fatal("changed handoff accepted or misclassified", err)
			}
		})
	}
}

func TestLegacyRecoveryRelaunchBeforeUnloadIsTypedRefusal(t *testing.T) {
	f := newRollingFixtureWithLegacy(t, true)
	path := filepath.Join(f.paths.launchAgents, setupNodeAgentLabel+".plist")
	payload, err := os.ReadFile(path)
	mustRolling(t, err)
	prints := 0
	run := f.install.Manager.Run
	f.install.Manager.Run = func(ctx context.Context, name string, args ...string) ([]byte, error) {
		if args[0] == "print" && strings.HasSuffix(args[len(args)-1], setupNodeAgentLabel) {
			prints++
			if prints == 1 {
				return []byte("state = waiting\npid = 0\n"), nil
			}
			return []byte("state = running\npid = 42\n"), nil
		}
		if args[0] == "bootout" {
			t.Fatal("restarted legacy process booted out")
		}
		return run(ctx, name, args...)
	}
	r := rollingUpgrade{paths: f.paths, store: f.install, receipt: upgradeRollingReceipt{LegacyPlistDigest: payloadDigest(payload)}}
	err = r.removeLegacyAutostart(t.Context())
	var refused *apiResponseError
	if !errors.As(err, &refused) || refused.APIError.Code != contract.ErrorConflict {
		t.Fatal("legacy relaunch not a typed refusal", err)
	}
	if _, err = os.Stat(path); err != nil {
		t.Fatal("live legacy plist removed", err)
	}
}

func testLegacyRecoveryFencesLoadAfterUnloadedObservation(t *testing.T, missing bool) {
	f := newRollingFixtureWithLegacy(t, true)
	path := filepath.Join(f.paths.launchAgents, setupNodeAgentLabel+".plist")
	payload, err := os.ReadFile(path)
	mustRolling(t, err)
	if missing {
		mustRolling(t, os.Remove(path))
	}
	unloaded := false
	disabled := false
	loaded := false
	run := f.install.Manager.Run
	f.install.Manager.Run = func(ctx context.Context, name string, args ...string) ([]byte, error) {
		if strings.HasSuffix(args[len(args)-1], setupNodeAgentLabel) {
			switch args[0] {
			case "disable":
				disabled = true
				return nil, nil
			case "print":
				if !unloaded {
					unloaded = true
					loaded = true
					return nil, launchagent.ErrServiceNotFound
				}
				if loaded {
					return []byte("state = waiting\npid = 0\n"), nil
				}
				return nil, launchagent.ErrServiceNotFound
			case "bootout":
				if !disabled {
					t.Fatal("unload without fence")
				}
				loaded = false
				return nil, nil
			}
		}
		return run(ctx, name, args...)
	}
	r := rollingUpgrade{paths: f.paths, store: f.install, receipt: upgradeRollingReceipt{LegacyPlistDigest: payloadDigest(payload)}}
	mustRolling(t, r.removeLegacyAutostart(t.Context()))
	if loaded || !disabled {
		t.Fatal("late-loaded legacy definition survived removal without restart fence")
	}
	if _, err = os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("legacy plist retained", err)
	}
}

func TestLegacyRecoveryRestartFenceThroughActivation(t *testing.T) {
	f := newRollingFixtureWithLegacy(t, true)
	enableRollingRecoveryFixture(t, f)
	f.operations.rollingStep = func(at string) error {
		if at == "legacy_unloaded_before_plist_removal" {
			return errors.New("interrupt original unload")
		}
		return nil
	}
	if _, err := runUpgrade(t.Context(), f.host, false, io.Discard); err == nil {
		t.Fatal("missing unload interruption")
	}
	f.operations.rollingStep = nil
	legacy := filepath.Join(f.paths.launchAgents, setupNodeAgentLabel+".plist")
	f.launchd.loaded[setupNodeAgentLabel] = true
	disabled, running, attempted := false, false, false
	prints, bootstraps := 0, 0
	run := f.install.Manager.Run
	f.install.Manager.Run = func(ctx context.Context, name string, args ...string) ([]byte, error) {
		if strings.HasSuffix(args[len(args)-1], setupNodeAgentLabel) {
			switch args[0] {
			case "enable":
				t.Fatal("legacy restart fence reopened")
			case "disable":
				disabled = true
				return nil, nil
			case "print":
				prints++
				if !f.launchd.loaded[setupNodeAgentLabel] {
					return nil, launchagent.ErrServiceNotFound
				}
				result := []byte("state = waiting\npid = 0\n")
				if running {
					result = []byte("state = running\npid = 42\n")
				}
				// The coordinator and removal preflight are observations 1/2;
				// observation 3 is the unload helper's final process check.
				if prints == 3 {
					attempted = true
					if !disabled {
						running = true
					}
				}
				return result, nil
			case "bootout":
				if running {
					t.Fatal("late-relaunched legacy process booted out")
				}
				f.launchd.loaded[setupNodeAgentLabel] = false
				return nil, nil
			}
		}
		if args[0] == "bootstrap" && strings.Contains(args[len(args)-1], setupNodeAgentLabel+".g-") {
			bootstraps++
			if !disabled || f.launchd.loaded[setupNodeAgentLabel] {
				t.Fatal("generation loaded before fenced legacy unload")
			}
			if _, err := os.Lstat(legacy); !errors.Is(err, os.ErrNotExist) {
				t.Fatal("generation loaded before positive legacy plist removal", err)
			}
		}
		return run(ctx, name, args...)
	}
	_, err := runUpgrade(t.Context(), f.host, false, io.Discard)
	mustRolling(t, err)
	if !attempted || !disabled || bootstraps != 1 {
		t.Fatalf("missing fenced activation: attempt=%t disabled=%t bootstrap=%d", attempted, disabled, bootstraps)
	}
	// Initialization quarantine lives outside generation storage and cannot block
	// a later compatible rolling activation or grant it execution authority.
	c, err := f.install.Read()
	mustRolling(t, err)
	primary := c.Primary
	_, err = runUpgrade(t.Context(), f.host, false, io.Discard)
	mustRolling(t, err)
	c, err = f.install.Read()
	mustRolling(t, err)
	if c.Primary == primary {
		t.Fatal("retained initialization custody blocked further rolling upgrade")
	}
}

func TestLegacyRecoveryFencesLoadAfterUnloadedObservation(t *testing.T) {
	for _, missing := range []bool{false, true} {
		t.Run(fmt.Sprint(missing), func(t *testing.T) { testLegacyRecoveryFencesLoadAfterUnloadedObservation(t, missing) })
	}
}

func TestLegacyRecoveryRollbackReopensAutostartAfterRunningRefusal(t *testing.T) {
	f := newRollingFixtureWithLegacy(t, true)
	enableRollingRecoveryFixture(t, f)
	f.operations.rollingStep = func(at string) error {
		if at == "legacy_unloaded_before_plist_removal" {
			return errors.New("interrupt original unload")
		}
		return nil
	}
	if _, err := runUpgrade(t.Context(), f.host, false, io.Discard); err == nil {
		t.Fatal("missing unload interruption")
	}
	f.operations.rollingStep = nil
	f.launchd.loaded[setupNodeAgentLabel] = true
	disabled, running, failEnable := false, false, true
	prints, enables := 0, 0
	enableFault := errors.New("launchctl enable unavailable")
	run := f.install.Manager.Run
	f.install.Manager.Run = func(ctx context.Context, name string, args ...string) ([]byte, error) {
		if strings.HasSuffix(args[len(args)-1], setupNodeAgentLabel) {
			switch args[0] {
			case "disable":
				disabled = true
				running = true // Relaunch between preflight and the final fenced observation.
				return nil, nil
			case "enable":
				enables++
				if failEnable {
					return nil, enableFault
				}
				disabled = false
				return nil, nil
			case "print":
				prints++
				if running {
					return []byte("state = running\npid = 42\n"), nil
				}
				return []byte("state = waiting\npid = 0\n"), nil
			case "bootout":
				t.Fatal("running legacy process booted out")
			}
		}
		if args[0] == "bootstrap" {
			t.Fatal("generation bootstrapped during retained-legacy rollback")
		}
		return run(ctx, name, args...)
	}
	_, err := runUpgrade(t.Context(), f.host, false, io.Discard)
	var refusal *apiResponseError
	if !errors.As(err, &refusal) || refusal.APIError.Code != contract.ErrorConflict || !disabled || prints < 3 {
		t.Fatalf("missing fenced running refusal: err=%v disabled=%t prints=%d", err, disabled, prints)
	}
	_, err = runUpgrade(t.Context(), f.host, false, io.Discard)
	if !errors.Is(err, enableFault) {
		t.Fatalf("enable fault must fence rollback completion: %v", err)
	}
	receipt, err := readUpgradeRolling(f.paths)
	mustRolling(t, err)
	if receipt.Phase == "rolled_back" || receipt.RecoveryState == "rolled_back" {
		t.Fatal("rollback completed before enable persisted")
	}
	failEnable = false
	interruption := errors.New("interrupt after durable enable before final rollback")
	f.operations.rollingStep = func(at string) error {
		if at == "legacy_stopping" {
			return interruption
		}
		return nil
	}
	if _, err = runUpgrade(t.Context(), f.host, false, io.Discard); !errors.Is(err, interruption) {
		t.Fatal("missing rollback completion interruption", err)
	}
	if disabled {
		t.Fatal("enable was not durable before rollback completion")
	}
	f.operations.rollingStep = nil
	report, err := runUpgrade(t.Context(), f.host, false, io.Discard)
	mustRolling(t, err)
	receipt, err = readUpgradeRolling(f.paths)
	mustRolling(t, err)
	if disabled || enables != 3 || receipt.Phase != "rolled_back" || report.RecoveryState != "rolled_back" {
		t.Fatalf("rollback autostart: disabled=%t enables=%d phase=%s state=%s", disabled, enables, receipt.Phase, report.RecoveryState)
	}
	if _, err = os.Stat(filepath.Join(f.paths.launchAgents, setupNodeAgentLabel+".plist")); err != nil {
		t.Fatal("legacy definition lost", err)
	}
	if _, err = f.install.Read(); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("rollback acquired generation ownership", err)
	}
}
