package main

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/agent"
	limarunner "github.com/Derek-X-Wang/wefty/runner/lima"
	"github.com/Derek-X-Wang/wefty/runner/ocicontrol"
)

func TestPerUserBootstrapUsesSameProcessAndNativeDialogOnce(t *testing.T) {
	root := t.TempDir()
	c := limarunner.UserInstallConfig{Instance: "wefty-proof", LimaHome: filepath.Join(root, "lima"), MountRoot: filepath.Join(root, "mount"), IntentFile: filepath.Join(root, "intent.json"), SetupState: filepath.Join(root, "setup.json"), Sizing: limarunner.Sizing{Memory: "4GiB", CPUs: 4, Disk: "32GiB"}, Assets: limarunner.UserInstallAssets{HelperVersion: "test", ProbeDigest: "sha256:test"}, ProofDialog: true}
	if _, err := limarunner.InitializeOCIIntent(c.IntentFile, time.Date(2026, 10, 23, 12, 0, 0, 0, time.UTC)); err != nil {
		t.Fatal(err)
	}
	payload, _ := json.Marshal(c)
	path := filepath.Join(root, "config.json")
	if err := os.WriteFile(path, payload, 0600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("LIMA_HOME", c.LimaHome)
	prepares, dialogs := 0, 0
	prepare := func(context.Context, limarunner.UserInstallConfig, string, string) error { prepares++; return nil }
	dialog := agent.SeatApprovalDialogFunc(func(context.Context, agent.SeatApprovalPrompt) (agent.SeatApprovalChoice, error) {
		dialogs++
		return agent.SeatApprovalApprove, nil
	})
	for i := range 2 {
		if i == 1 {
			state, err := ocicontrol.ReadSetupState(c.SetupState)
			if err != nil {
				t.Fatal(err)
			}
			state.VMMemory = "6GiB"
			if err := ocicontrol.WriteSetupState(c.SetupState, state); err != nil {
				t.Fatal(err)
			}
		}
		if allowed, err := prepareInstalledUserOCI(t.Context(), path, "node", "boot", "test", prepare, dialog); err != nil || !allowed {
			t.Fatalf("allowed=%t err=%v", allowed, err)
		}
	}
	saved, err := ocicontrol.ReadSetupState(c.SetupState)
	if err != nil {
		t.Fatal(err)
	}
	if saved.VMMemory != "6GiB" {
		t.Fatal("node startup replaced the owner's saved setup-oci state")
	}
	if prepares != 2 || dialogs != 1 {
		t.Fatalf("bootstrap=%d dialog=%d", prepares, dialogs)
	}
	var receipt struct {
		Approved bool `json:"approved"`
		PID      int  `json:"pid"`
	}
	b, err := os.ReadFile(filepath.Join(root, "proof-dialog.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(b, &receipt); err != nil {
		t.Fatal(err)
	}
	if !receipt.Approved || receipt.PID != os.Getpid() {
		t.Fatalf("receipt=%+v", receipt)
	}
}

func TestPerUserProofRefusalIsDurableWithoutRestartDialog(t *testing.T) {
	for _, choice := range []agent.SeatApprovalChoice{agent.SeatApprovalDeny, agent.SeatApprovalNoAnswer} {
		t.Run(string(choice), func(t *testing.T) {
			root := t.TempDir()
			c := limarunner.UserInstallConfig{Instance: "wefty-proof", LimaHome: filepath.Join(root, "lima"), MountRoot: filepath.Join(root, "mount"), IntentFile: filepath.Join(root, "intent.json"), SetupState: filepath.Join(root, "setup.json"), Sizing: limarunner.Sizing{Memory: "4GiB", CPUs: 4, Disk: "32GiB"}, Assets: limarunner.UserInstallAssets{HelperVersion: "test", ProbeDigest: "sha256:test"}, ProofDialog: true}
			if _, err := limarunner.InitializeOCIIntent(c.IntentFile, time.Unix(1, 0)); err != nil {
				t.Fatal(err)
			}
			payload, _ := json.Marshal(c)
			path := filepath.Join(root, "config.json")
			if err := os.WriteFile(path, payload, 0600); err != nil {
				t.Fatal(err)
			}
			t.Setenv("LIMA_HOME", c.LimaHome)
			dialogs, prepares := 0, 0
			prepare := func(context.Context, limarunner.UserInstallConfig, string, string) error { prepares++; return nil }
			dialog := agent.SeatApprovalDialogFunc(func(context.Context, agent.SeatApprovalPrompt) (agent.SeatApprovalChoice, error) {
				dialogs++
				return choice, nil
			})
			for range 2 {
				if allowed, err := prepareInstalledUserOCI(t.Context(), path, "node", "boot", "test", prepare, dialog); err != nil || allowed {
					t.Fatalf("refusal must keep the process-only node agent alive: %v", err)
				}
			}
			var receipt struct {
				Approved bool                     `json:"approved"`
				Choice   agent.SeatApprovalChoice `json:"choice"`
			}
			payload, err := os.ReadFile(filepath.Join(root, "proof-dialog.json"))
			if err != nil {
				t.Fatal(err)
			}
			if err := json.Unmarshal(payload, &receipt); err != nil {
				t.Fatal(err)
			}
			if receipt.Approved || receipt.Choice != choice || dialogs != 1 || prepares != 1 {
				t.Fatalf("receipt=%+v dialogs=%d prepares=%d", receipt, dialogs, prepares)
			}
		})
	}
}
