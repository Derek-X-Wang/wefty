package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/agent"
	"github.com/Derek-X-Wang/wefty/internal/connprofile"
	"github.com/Derek-X-Wang/wefty/l1"
)

func readSetupReceipt(t *testing.T, home string) setupManifest {
	t.Helper()
	paths, err := resolveSetupPaths(home)
	if err != nil {
		t.Fatal(err)
	}
	payload, err := os.ReadFile(filepath.Join(paths.config, setupManifestName))
	if err != nil {
		t.Fatal(err)
	}
	var receipt setupManifest
	if err := json.Unmarshal(payload, &receipt); err != nil {
		t.Fatal(err)
	}
	return receipt
}

// assertStatusShowsVersion reads the receipt through status's public text and
// JSON output, with L1 answering.
func assertStatusShowsVersion(t *testing.T, home, nodeID, version string) {
	t.Helper()
	paths, err := resolveSetupPaths(home)
	if err != nil {
		t.Fatal(err)
	}
	h := newStatusHarness(t)
	h.nodes = l1.NodeList{Nodes: []l1.Node{readyNode(nodeID)}}
	clients := h.clients()
	clients.nodeConfigPath = paths.nodeConfig
	for _, jsonMode := range []bool{false, true} {
		var out bytes.Buffer
		if err := executeStatus(t.Context(), clients, jsonMode, nil, &out, io.Discard); err != nil {
			t.Fatal(err)
		}
		want := "installed version: " + version + "\n"
		if jsonMode {
			var status clusterStatus
			if err := json.Unmarshal(out.Bytes(), &status); err != nil || status.InstalledVersion != version {
				t.Fatalf("status JSON installed_version = %q, want %q (%v)", status.InstalledVersion, version, err)
			}
		} else if !strings.Contains(out.String(), want) {
			t.Fatalf("status text lacks %q:\n%s", want, out.String())
		}
	}
}

// writeSourceBinaries replaces the four binaries setup installs with stand-ins
// whose bytes are exactly what the fixture's fake upgrade build writes.
func writeSourceBinaries(t *testing.T, f *upgradeFixture, version string) {
	t.Helper()
	for _, name := range setupBinaries {
		if err := os.WriteFile(filepath.Join(f.source, name), []byte("#!/bin/sh\necho "+version+"\n"), 0o755); err != nil {
			t.Fatal(err)
		}
	}
}

// writeStandIn writes a binary whose --version prints reported, or fails when
// reported is empty. It keeps the fixture's v1 marker: the fixture's launchctl
// checks that marker to prove the old node agent is still installed when the
// run ledger restarts.
func writeStandIn(t *testing.T, f *upgradeFixture, name, reported string) {
	t.Helper()
	script := "#!/bin/sh\necho " + name + " v1\nexit 3\n"
	if reported != "" {
		script = "#!/bin/sh\nif [ \"$1\" = --version ]; then echo " + reported + "; exit 0; fi\necho " + name + " v1\n"
	}
	if err := os.WriteFile(filepath.Join(f.source, name), []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
}

// setupAgain runs a plain setup on the upgrade fixture's install, with the
// plain fake launchd: the fixture's own launchctl models an upgrade.
func setupAgain(t *testing.T, f *upgradeFixture) setupReport {
	t.Helper()
	upgradeLaunchctl := f.host.launchctl
	f.host.launchctl = f.launchd.run
	defer func() { f.host.launchctl = upgradeLaunchctl }()
	return f.setup(t, setupRequest{})
}

func TestSetupRecordsTheCommitItsBinariesReportAndSameCommitUpgradeStopsNothing(t *testing.T) {
	f := newUpgradeFixture(t)
	writeSourceBinaries(t, f, f.commit)
	report := setupAgain(t, f)
	receipt := readSetupReceipt(t, f.home)
	if receipt.InstalledVersion != f.commit {
		t.Fatalf("setup receipt installed_version = %q, want the commit the binaries report %q", receipt.InstalledVersion, f.commit)
	}
	if receipt.UpgradeSource {
		t.Fatal("setup claimed the upgrade source clone it never made")
	}
	if len(report.Notes) != 1 || !strings.Contains(report.Notes[0], "login-shell PATH") {
		t.Fatalf("setup notes = %v", report.Notes)
	}
	assertStatusShowsVersion(t, f.home, report.NodeID, f.commit)

	before := installedUpgradeFiles(t, f)
	f.events, f.launchd.commands = nil, nil
	var stdout, stderr bytes.Buffer
	if err := executeUpgradeWithHost(t.Context(), f.host, globalOptions{}, false, &stdout, &stderr); err != nil {
		t.Fatalf("same-commit upgrade: %v\n%s", err, stderr.String())
	}
	if stderr.String() != "already at "+f.commit+"\n" || stdout.Len() != 0 {
		t.Fatalf("same-commit upgrade output: stdout=%q stderr=%q", stdout.String(), stderr.String())
	}
	if mutating := f.launchd.mutating(); len(mutating) != 0 {
		t.Fatalf("same-commit upgrade changed launchd state: %v", mutating)
	}
	for _, event := range f.events {
		if strings.HasPrefix(event, "restart:") || strings.HasPrefix(event, "preflight:") {
			t.Fatalf("same-commit upgrade went past the version check: %v", f.events)
		}
	}
	for _, label := range []string{setupL1Label, setupL3Label, setupNodeAgentLabel} {
		if !f.launchd.loaded[label] {
			t.Fatalf("same-commit upgrade stopped %s", label)
		}
	}
	if !reflect.DeepEqual(before, installedUpgradeFiles(t, f)) {
		t.Fatal("same-commit upgrade changed installed files")
	}
	assertNoUpgradeStage(t, f)
}

func TestSetupVersionWithoutACommitNeverMakesUpgradeSkip(t *testing.T) {
	for _, kind := range []string{"dev", "disagree", "unreadable"} {
		t.Run(kind, func(t *testing.T) {
			f := newUpgradeFixture(t)
			wantVersion, wantNote := "", ""
			for _, name := range setupBinaries {
				writeStandIn(t, f, name, f.commit)
			}
			switch kind {
			case "dev":
				// A development build reports `dev`: recorded as is, never current.
				for _, name := range setupBinaries {
					writeStandIn(t, f, name, "dev")
				}
				wantVersion = "dev"
			case "disagree":
				writeStandIn(t, f, "wefty-l3", "dev")
				wantNote = "wefty-l3 dev"
			case "unreadable":
				writeStandIn(t, f, "wefty-agent", "")
				wantNote = "could not read the version of"
			}
			report := setupAgain(t, f)
			if receipt := readSetupReceipt(t, f.home); receipt.InstalledVersion != wantVersion {
				t.Fatalf("setup receipt installed_version = %q, want %q", receipt.InstalledVersion, wantVersion)
			}
			notes := strings.Join(report.Notes, "\n")
			if wantNote == "" && strings.Contains(notes, "installed version") || wantNote != "" && (!strings.Contains(notes, wantNote) || !strings.Contains(notes, "setup recorded no installed version")) {
				t.Fatalf("setup notes = %v", report.Notes)
			}
			if wantVersion != "" {
				assertStatusShowsVersion(t, f.home, report.NodeID, wantVersion)
			}
			f.events, f.launchd.commands = nil, nil
			var out bytes.Buffer
			upgrade, err := runUpgrade(t.Context(), f.host, true, &out)
			if err != nil || upgrade.AlreadyAt || strings.Contains(out.String(), "already at") {
				t.Fatalf("upgrade over a %s install did not activate: %+v %v %s", kind, upgrade, err, out.String())
			}
			if !slices.Contains(f.events, "restart:"+setupNodeAgentLabel) {
				t.Fatalf("upgrade over a %s install skipped activation: %v", kind, f.events)
			}
			if receipt := readSetupReceipt(t, f.home); receipt.InstalledVersion != f.commit {
				t.Fatalf("upgrade receipt installed_version = %q, want %q", receipt.InstalledVersion, f.commit)
			}
		})
	}
}

// The renewal window holds the two restarts and one full heartbeat interval
// of the node agent setup installs, and stays well under the attempt lease.
func TestUpgradeRenewalWindowIsDerivedFromTheInstalledHeartbeat(t *testing.T) {
	if upgradeHeartbeatInterval != agent.DefaultHeartbeatInterval {
		t.Fatalf("heartbeat interval %s is not the node agent's %s", upgradeHeartbeatInterval, agent.DefaultHeartbeatInterval)
	}
	if upgradeRenewalWindow != upgradeHeartbeatInterval+upgradeReloadAllowance || upgradeReloadAllowance <= 0 {
		t.Fatalf("window %s is not one heartbeat %s plus the reload allowance %s", upgradeRenewalWindow, upgradeHeartbeatInterval, upgradeReloadAllowance)
	}
	if upgradeRenewalWindow > l1.DefaultLeaseDuration-5*time.Second {
		t.Fatalf("window %s is not well under the %s lease", upgradeRenewalWindow, l1.DefaultLeaseDuration)
	}
	// The derivation holds only while the installed node agent runs with the
	// default interval.
	f := newSetupFixture(t)
	f.setup(t, setupRequest{})
	if plist := f.plist(t, setupNodeAgentLabel); strings.Contains(plist, "--heartbeat-interval") {
		t.Fatalf("setup's node agent overrides the heartbeat interval:\n%s", plist)
	}
}

// A renewal can come one full heartbeat after slow restarts. It passes, and
// the renewal line is printed once. One that never comes still fails loudly.
func TestUpgradeRenewalOneHeartbeatAfterSlowRestartsPasses(t *testing.T) {
	const reload = 6 * time.Second
	for _, arrives := range []bool{true, false} {
		t.Run(map[bool]string{true: "late", false: "never"}[arrives], func(t *testing.T) {
			f := newUpgradeFixture(t)
			var restartedAt time.Time
			launchctl := f.host.launchctl
			f.host.launchctl = func(ctx context.Context, name string, args ...string) ([]byte, error) {
				output, err := launchctl(ctx, name, args...)
				if args[0] == "bootstrap" {
					switch strings.TrimSuffix(filepath.Base(args[2]), ".plist") {
					case setupL1Label:
						f.now = f.now.Add(reload)
					case setupL3Label:
						restartedAt = f.now
					}
				}
				return output, err
			}
			observe := f.operations.observe
			f.operations.observe = func(ctx context.Context, profile connprofile.Profile) (l1.Node, error) {
				node, err := observe(ctx, profile)
				if f.restarted && !f.replaced {
					// The agent's last heartbeat came just before the restarts
					// finished; the next is one interval later.
					node.LastHeartbeatAt = restartedAt.Add(-time.Millisecond)
					if arrives && !f.now.Before(restartedAt.Add(agent.DefaultHeartbeatInterval)) {
						node.LastHeartbeatAt = f.now
					}
				}
				return node, err
			}
			var output bytes.Buffer
			err := executeUpgradeWithHost(t.Context(), f.host, globalOptions{}, true, &output, &output)
			if !arrives {
				if err == nil || !strings.Contains(err.Error(), "activation failed") || !strings.Contains(err.Error(), "no successful node lease renewal within "+upgradeRenewalWindow.String()) {
					t.Fatalf("missing renewal did not fail loudly: %v", err)
				}
				if strings.Contains(output.String(), "Successful node lease renewal") || f.replaced {
					t.Fatalf("missing renewal reported success or replaced the node agent: %s", output.String())
				}
				return
			}
			if err != nil {
				t.Fatalf("renewal one heartbeat after %s restarts failed: %v\n%s", reload, err, output.String())
			}
			if count := strings.Count(output.String(), "Successful node lease renewal"); count != 1 {
				t.Fatalf("renewal line printed %d times:\n%s", count, output.String())
			}
			want := reload + 100*time.Millisecond + agent.DefaultHeartbeatInterval
			if !strings.Contains(output.String(), "Successful node lease renewal after control-plane restart: "+want.String()+" (window "+upgradeRenewalWindow.String()+").") {
				t.Fatalf("renewal line does not report %s:\n%s", want, output.String())
			}
			if !strings.Contains(output.String(), "Installed version: "+f.commit+"\n") || !f.replaced {
				t.Fatalf("upgrade summary:\n%s", output.String())
			}
		})
	}
}

func TestUpgradePrintsTheRenewalLineOnce(t *testing.T) {
	f := newUpgradeFixture(t)
	var output bytes.Buffer
	if err := executeUpgradeWithHost(t.Context(), f.host, globalOptions{}, true, &output, &output); err != nil {
		t.Fatal(err)
	}
	if count := strings.Count(output.String(), "Successful node lease renewal"); count != 1 {
		t.Fatalf("renewal line printed %d times:\n%s", count, output.String())
	}
	if !strings.Contains(output.String(), "Installed version: "+f.commit+"\nNode agent replaced") {
		t.Fatalf("upgrade summary:\n%s", output.String())
	}
}
