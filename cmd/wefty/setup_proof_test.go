package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
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

func TestProofOCISetupCommandReceiptAndPurgeStopBoundary(t *testing.T) {
	f := newSetupFixture(t)
	config := setupOCIAssetsFixture(t, f)
	var stdout, stderr bytes.Buffer
	run := func(args ...string) error {
		return executeSetupUsingHost(t.Context(), globalOptions{jsonOutput: true}, args, &stdout, &stderr, func() (setupHost, error) { return f.host, nil })
	}
	if err := run("--proof", "--oci-config", config); err != nil {
		t.Fatal(err)
	}
	proofRoot := filepath.Join(f.home, proofInstallRelative)
	payload, err := os.ReadFile(filepath.Join(f.home, "Library", "LaunchAgents", "dev.wefty-proof.node-agent.plist"))
	if err != nil {
		t.Fatal(err)
	}
	var definition struct {
		EnvironmentVariables map[string]string
		ProgramArguments     []string
	}
	if _, err := plist.Unmarshal(payload, &definition); err != nil {
		t.Fatal(err)
	}
	if definition.EnvironmentVariables["LIMA_HOME"] != filepath.Join(proofRoot, "data", "lima") || !strings.Contains(strings.Join(definition.ProgramArguments, " "), "--oci-lima-instance=wefty-proof") {
		t.Fatalf("not proof OCI: %+v", definition)
	}
	var receipt map[string]any
	payload, err = os.ReadFile(filepath.Join(proofRoot, "config", setupManifestName))
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(payload, &receipt); err != nil {
		t.Fatal(err)
	}
	if receipt["proof_root"] != proofRoot {
		t.Fatalf("proof receipt lacks exact removal root: %s", payload)
	}
	stopFailure := errors.New("injected stop failed")
	calls := 0
	f.host.stopHostVM = func(_ context.Context, limaHome, instance string) error {
		calls++
		if limaHome != filepath.Join(proofRoot, "data", "lima") || instance != "wefty-proof" {
			t.Fatalf("wrong stop target %s %s", limaHome, instance)
		}
		if !strings.Contains(stderr.String(), proofRoot) {
			t.Fatal("removal did not print exact root before stop/deletion")
		}
		if calls == 1 {
			return stopFailure
		}
		return nil
	}
	if err := run("--proof", "--remove", "--purge"); !errors.Is(err, stopFailure) {
		t.Fatalf("stop error=%v", err)
	}
	for _, file := range []string{filepath.Join(proofRoot, "config", setupManifestName), filepath.Join(f.home, "Library", "LaunchAgents", "dev.wefty-proof.node-agent.plist")} {
		if _, err := os.Stat(file); err != nil {
			t.Fatalf("failed stop removed %s: %v", file, err)
		}
	}
	if err := run("--proof", "--remove", "--purge"); err != nil {
		t.Fatal(err)
	}
	if calls != 2 {
		t.Fatalf("stop calls=%d", calls)
	}
	if _, err := os.Stat(proofRoot); !os.IsNotExist(err) {
		t.Fatalf("purge root still exists: %v", err)
	}
}

func TestProofSetupRefusesSymlinkedLimaPaths(t *testing.T) {
	for _, suffix := range []string{"data/lima", "data/lima/wefty-proof"} {
		t.Run(suffix, func(t *testing.T) {
			f := newSetupFixture(t)
			config := setupOCIAssetsFixture(t, f)
			link := filepath.Join(f.home, proofInstallRelative, suffix)
			if err := os.MkdirAll(filepath.Dir(link), 0700); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(t.TempDir(), link); err != nil {
				t.Fatal(err)
			}
			var stdout, stderr bytes.Buffer
			err := executeSetupUsingHost(t.Context(), globalOptions{}, []string{"--proof", "--oci-config", config}, &stdout, &stderr, func() (setupHost, error) { return f.host, nil })
			if err == nil {
				t.Fatal("setup accepted symlinked proof Lima state")
			}
			if len(f.launchd.mutating()) != 0 {
				t.Fatal("symlink refusal changed services")
			}
		})
	}
}
