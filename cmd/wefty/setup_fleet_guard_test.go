package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/Derek-X-Wang/wefty/internal/serviceplan"
)

func TestSetupAndUpgradeRefuseRecordedFleetRole(t *testing.T) {
	for _, role := range []serviceplan.Role{serviceplan.Join, serviceplan.ControlPlane, serviceplan.Role("future-role")} {
		for _, command := range []string{"setup", "upgrade"} {
			t.Run(string(role)+"/"+command, func(t *testing.T) {
				fixture := newSetupFixture(t)
				fixture.setup(t, setupRequest{})
				paths, err := resolveSetupPaths(fixture.home)
				if err != nil {
					t.Fatal(err)
				}
				receiptPath := filepath.Join(paths.config, setupManifestName)
				payload, err := os.ReadFile(receiptPath)
				if err != nil {
					t.Fatal(err)
				}
				var receipt setupManifest
				if err := json.Unmarshal(payload, &receipt); err != nil {
					t.Fatal(err)
				}
				receipt.Role = role
				payload, err = json.Marshal(receipt)
				if err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(receiptPath, payload, 0o600); err != nil {
					t.Fatal(err)
				}
				sentinel := filepath.Join(paths.data, ".upgrade-preserve")
				if err := os.Mkdir(sentinel, 0o700); err != nil {
					t.Fatal(err)
				}
				before := installedUpgradeFiles(t, &upgradeFixture{setupFixture: fixture})
				fixture.launchd.commands = nil
				reachedPreparation := false
				fixture.host.upgrade = &upgradeHost{command: func(context.Context, string, []string, string, ...string) ([]byte, error) {
					reachedPreparation = true
					return nil, errors.New("unexpected upgrade preparation")
				}}
				if command == "setup" {
					_, err = runSetup(t.Context(), fixture.host, setupRequest{})
				} else {
					_, err = runUpgrade(t.Context(), fixture.host, false, io.Discard)
				}
				if err == nil || !strings.Contains(err.Error(), "recorded fleet role") || !strings.Contains(err.Error(), command) {
					t.Errorf("%s did not clearly refuse fleet role: %v", command, err)
				}
				if reachedPreparation || len(fixture.launchd.commands) != 0 {
					t.Errorf("fleet refusal reached preparation or service manager: preparation=%v calls=%v", reachedPreparation, fixture.launchd.commands)
				}
				if !reflect.DeepEqual(before, installedUpgradeFiles(t, &upgradeFixture{setupFixture: fixture})) {
					t.Error("fleet refusal changed installation files")
				}
				if _, err := os.Stat(sentinel); err != nil {
					t.Errorf("fleet refusal removed upgrade data: %v", err)
				}
			})
		}
	}
}
