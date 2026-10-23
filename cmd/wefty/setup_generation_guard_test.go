package main

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Exercise production entry points, not an independent allowlist simulation.
// The stale .upgrade directory deliberately contains a retained-binary sentinel.
func TestGenerationMaintenanceRefusesBeforeFilesOrServicesChange(t *testing.T) {
	for _, command := range []string{"setup", "conversion", "remove", "purge", "upgrade"} {
		t.Run(command, func(t *testing.T) {
			f := newSetupFixture(t)
			paths, issues := resolveMaintenancePaths(f.home)
			if len(issues) > 0 {
				t.Fatal(issues)
			}
			for _, folder := range []string{filepath.Join(paths.data, "agent-generations"), filepath.Join(paths.data, ".upgrade-retained")} {
				if err := os.MkdirAll(folder, 0700); err != nil {
					t.Fatal(err)
				}
			}
			sentinel := filepath.Join(paths.data, ".upgrade-retained", "wefty-agent")
			if err := os.WriteFile(sentinel, []byte("retained executable"), 0500); err != nil {
				t.Fatal(err)
			}
			called := false
			f.host.launchctl = func(context.Context, string, ...string) ([]byte, error) { called = true; return nil, nil }
			var err error
			switch command {
			case "setup":
				_, err = runSetup(t.Context(), f.host, setupRequest{})
			case "conversion":
				_, err = runSetup(t.Context(), f.host, setupRequest{role: "join", convertPlain: true})
			case "remove", "purge":
				_, err = runSetupRemove(t.Context(), f.host, command == "purge")
			case "upgrade":
				_, err = runUpgrade(t.Context(), f.host, false, nil)
			}
			if err == nil || !strings.Contains(err.Error(), "retiring generations may still hold work") {
				t.Fatalf("expected clear refusal: %v", err)
			}
			if called {
				t.Fatal("maintenance reached launchctl")
			}
			payload, err := os.ReadFile(sentinel)
			if err != nil || string(payload) != "retained executable" {
				t.Fatal("stale staging cleanup removed retained executable", err)
			}
		})
	}
}
