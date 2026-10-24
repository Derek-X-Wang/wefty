package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"testing/synctest"
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
		if allowed, err := prepareInstalledUserOCI(t.Context(), path, "node", "boot", "test", prepare, dialog, func(context.Context, limarunner.UserInstallConfig) error { return nil }); err != nil || !allowed {
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
			dialogs, prepares, stops := 0, 0, 0
			stopProof := func(context.Context, limarunner.UserInstallConfig) error { stops++; return nil }
			prepare := func(context.Context, limarunner.UserInstallConfig, string, string) error { prepares++; return nil }
			dialog := agent.SeatApprovalDialogFunc(func(context.Context, agent.SeatApprovalPrompt) (agent.SeatApprovalChoice, error) {
				dialogs++
				return choice, nil
			})
			for range 2 {
				if allowed, err := prepareInstalledUserOCI(t.Context(), path, "node", "boot", "test", prepare, dialog, stopProof); err != nil || allowed {
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
			if receipt.Approved || receipt.Choice != choice || dialogs != 1 || prepares != 1 || stops != 1 {
				t.Fatalf("receipt=%+v dialogs=%d prepares=%d stops=%d", receipt, dialogs, prepares, stops)
			}
		})
	}
}

func TestPerUserProofDialogGetsFreshDeadlineAfterSlowBootstrap(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		path := perUserProofTestConfig(t)
		ctx, cancel := context.WithTimeout(t.Context(), 15*time.Minute)
		defer cancel()
		prepare := func(context.Context, limarunner.UserInstallConfig, string, string) error {
			time.Sleep(14*time.Minute + 59*time.Second) // Virtual clock, no wall-clock wait.
			return nil
		}
		dialog := agent.SeatApprovalDialogFunc(func(ctx context.Context, prompt agent.SeatApprovalPrompt) (agent.SeatApprovalChoice, error) {
			deadline, bounded := ctx.Deadline()
			if !bounded || !deadline.Equal(time.Now().Add(5*time.Minute)) || prompt.Wait != 5*time.Minute {
				t.Fatalf("dialog inherited bootstrap budget: remaining=%v wait=%v", time.Until(deadline), prompt.Wait)
			}
			return agent.SeatApprovalApprove, nil
		})
		if allowed, err := prepareInstalledUserOCI(ctx, path, "node", "boot", "test", prepare, dialog, func(context.Context, limarunner.UserInstallConfig) error { return nil }); err != nil || !allowed {
			t.Fatalf("allowed=%t err=%v", allowed, err)
		}
	})
}

func perUserProofTestConfig(t *testing.T) string {
	t.Helper()
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
	return path
}

func TestPerUserProofRefusalStopFailureKeepsNodeProcessUp(t *testing.T) {
	path := perUserProofTestConfig(t)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	prepare := func(context.Context, limarunner.UserInstallConfig, string, string) error { return nil }
	dialog := agent.SeatApprovalDialogFunc(func(context.Context, agent.SeatApprovalPrompt) (agent.SeatApprovalChoice, error) {
		cancel() // The completed bootstrap's context cannot cancel cleanup.
		return agent.SeatApprovalDeny, nil
	})
	failure := errors.New("injected stop failure")
	stops := 0
	stopProof := func(ctx context.Context, c limarunner.UserInstallConfig) error {
		stops++
		if ctx.Err() != nil || c.Instance != "wefty-proof" {
			t.Fatalf("invalid refusal stop: ctx=%v instance=%s", ctx.Err(), c.Instance)
		}
		var receipt proofDialogReceipt
		if err := limarunner.ReadUserInstallJSON(filepath.Join(filepath.Dir(c.SetupState), "proof-dialog.json"), &receipt); err != nil || receipt.Approved {
			t.Fatalf("stop preceded durable refusal: %+v %v", receipt, err)
		}
		return failure
	}
	var logs bytes.Buffer
	previous := log.Writer()
	log.SetOutput(&logs)
	defer log.SetOutput(previous)
	if allowed, err := prepareInstalledUserOCI(ctx, path, "node", "boot", "test", prepare, dialog, stopProof); err != nil || allowed {
		t.Fatalf("refusal exited node process: allowed=%t err=%v", allowed, err)
	}
	if stops != 1 || !strings.Contains(logs.String(), "proof host VM graceful stop failed: injected stop failure") {
		t.Fatalf("stop calls=%d logs=%s", stops, &logs)
	}
}
