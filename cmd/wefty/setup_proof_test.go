package main

import (
	"bytes"
	"context"
	"howett.net/plist"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestProofSetupRendersIsolatedLoginServicesAndConverges(t *testing.T) {
	fixture := newSetupFixture(t)
	daily := filepath.Join(fixture.home, "Library", "LaunchAgents", setupNodeAgentLabel+".plist")
	if err := os.MkdirAll(filepath.Dir(daily), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(daily, []byte("daily fixture"), 0644); err != nil {
		t.Fatal(err)
	}
	fixture.launchd.loaded[setupNodeAgentLabel] = true
	var output, stderr bytes.Buffer
	run := func(args ...string) error {
		return executeSetupUsingHost(context.Background(), globalOptions{jsonOutput: true}, args, &output, &stderr, func() (setupHost, error) { return fixture.host, nil })
	}
	if err := run("--proof"); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"l1", "l3", "node-agent"} {
		b, err := os.ReadFile(filepath.Join(fixture.home, "Library", "LaunchAgents", "dev.wefty-proof."+name+".plist"))
		if err != nil {
			t.Fatal(err)
		}
		var definition struct{ EnvironmentVariables map[string]string }
		if _, err := plist.Unmarshal(b, &definition); err != nil {
			t.Fatal(err)
		}
		if definition.EnvironmentVariables["HOME"] != filepath.Join(fixture.home, proofInstallRelative, "data", "home") || definition.EnvironmentVariables["TMPDIR"] != filepath.Join(fixture.home, proofInstallRelative, "data", "tmp") {
			t.Fatal("proof service can write default files outside its own state")
		}
		if strings.Contains(string(b), "dev.wefty.") || !strings.Contains(string(b), "wefty-proof") {
			t.Fatalf("not isolated: %s", b)
		}
	}
	fixture.launchd.commands = nil
	if err := run("--proof"); err != nil {
		t.Fatal(err)
	}
	if len(fixture.launchd.mutating()) != 0 {
		t.Fatalf("second setup mutated launchd: %v", fixture.launchd.mutating())
	}
	if err := run("--proof", "--remove", "--purge"); err != nil {
		t.Fatal(err)
	}
	if len(fixture.launchd.loaded) != 1 || !fixture.launchd.loaded[setupNodeAgentLabel] {
		t.Fatalf("daily service changed: %v", fixture.launchd.loaded)
	}
	if b, err := os.ReadFile(daily); err != nil || string(b) != "daily fixture" {
		t.Fatalf("daily plist changed: %q %v", b, err)
	}
	if _, err := os.Stat(filepath.Join(fixture.home, proofInstallRelative)); !os.IsNotExist(err) {
		t.Fatalf("proof purge left files: %v", err)
	}
	if _, err := os.Stat(filepath.Join(fixture.home, ".config", "wefty")); !os.IsNotExist(err) {
		t.Fatalf("touched daily config: %v", err)
	}
}
