package main

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Derek-X-Wang/wefty/internal/connprofile"
)

// The CLI reads the connection profile under the shared precedence: flags,
// then environment, then the profile, then built-in defaults.
func TestCLIReadsTheProfileUnderFlagsAndEnvironment(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	for _, name := range []string{connprofile.EnvironmentVariable, l1AddressEnv, l3AddressEnv,
		"WEFTY_DEV_PLAIN_FABRIC_ID", "WEFTY_DEV_PLAIN_USER_ID", "WEFTY_DEV_PLAIN_DEVICE_ID"} {
		t.Setenv(name, "")
	}
	resolve := func(t *testing.T, args ...string) (globalOptions, error) {
		t.Helper()
		options, _, err := parseGlobalOptions(append(args, "status"), io.Discard)
		if err != nil {
			t.Fatal(err)
		}
		return options, applyConnectionProfile(&options)
	}

	// No profile installed: the built-in defaults, exactly as before setup.
	options, err := resolve(t)
	if err != nil || options.l1Address != "wefty://control-plane" || options.plainIdentity != "wefty-cli" || options.plainUserID != "" {
		t.Fatalf("without a profile: %+v err=%v", options, err)
	}

	path, err := connprofile.DefaultPath(home)
	if err != nil {
		t.Fatal(err)
	}
	if err := connprofile.Write(path, connprofile.Profile{
		Version: connprofile.Version, Fabric: "plain", PlainFabricID: "plain-cli-test",
		L1Address: "127.0.0.1:47101", L3Address: "127.0.0.1:47102",
		Identities: connprofile.Identities{CLI: "profile-cli", PersonUserID: "owner", PersonDeviceID: "mac-1"},
	}); err != nil {
		t.Fatal(err)
	}
	options, err = resolve(t)
	if err != nil || options.l1Address != "127.0.0.1:47101" || options.l3Address != "127.0.0.1:47102" ||
		options.plainIdentity != "profile-cli" || options.plainUserID != "owner" || options.plainDeviceID != "mac-1" ||
		options.plainFabricID != "plain-cli-test" || options.fabricMode != "plain" {
		t.Fatalf("from the profile: %+v err=%v", options, err)
	}

	// Flags win, including an explicit empty --l3= for an L1-only cluster.
	options, err = resolve(t, "--l1=127.0.0.1:1", "--l3=", "--plain-user-id=flag-person")
	if err != nil || options.l1Address != "127.0.0.1:1" || options.l3Address != "" || options.plainUserID != "flag-person" {
		t.Fatalf("flags over the profile: %+v err=%v", options, err)
	}

	// The environment wins over the profile.
	t.Setenv(l1AddressEnv, "127.0.0.1:2")
	t.Setenv("WEFTY_DEV_PLAIN_USER_ID", "env-person")
	options, err = resolve(t)
	if err != nil || options.l1Address != "127.0.0.1:2" || options.plainUserID != "env-person" || options.l3Address != "127.0.0.1:47102" {
		t.Fatalf("environment over the profile: %+v err=%v", options, err)
	}

	// A named profile must exist; a guess at defaults would hide the mistake.
	if _, err := resolve(t, "--profile="+filepath.Join(home, "missing.json")); err == nil {
		t.Fatal("a missing named profile was ignored")
	}
}

// A broken profile stops cluster commands with its path, and never the
// authoring commands a job runs: they do not reach the cluster.
func TestBrokenProfileStopsClusterCommandsButNotAuthoring(t *testing.T) {
	broken := filepath.Join(t.TempDir(), connprofile.FileName)
	if err := os.WriteFile(broken, []byte("{"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv(connprofile.EnvironmentVariable, broken)
	err := run(context.Background(), []string{"status"}, io.Discard, io.Discard)
	if err == nil || !strings.Contains(err.Error(), broken) {
		t.Fatalf("status with a broken profile = %v", err)
	}
	directory := t.TempDir()
	if err := run(context.Background(), []string{"workflow", "init", "starter", "--dir", directory}, io.Discard, io.Discard); err != nil {
		t.Fatalf("workflow init read the broken profile: %v", err)
	}
}
