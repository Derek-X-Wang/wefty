package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/agent"
	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/fabric/plain"
	"github.com/Derek-X-Wang/wefty/internal/agentinstall"
	"github.com/Derek-X-Wang/wefty/internal/connprofile"
	"github.com/Derek-X-Wang/wefty/internal/launchagent"
	"github.com/Derek-X-Wang/wefty/l1"
)

func newQuietRemovalFixture(t *testing.T) *rollingFixture {
	t.Helper()
	f := newRollingFixture(t)
	f.operations.generationControl = requestGenerationControl
	delete(f.launchd.loaded, setupNodeAgentLabel)
	if err := os.Remove(filepath.Join(f.paths.launchAgents, setupNodeAgentLabel+".plist")); err != nil && !os.IsNotExist(err) {
		t.Fatal(err)
	}
	f.host.launchctl = func(ctx context.Context, name string, args ...string) ([]byte, error) {
		label := strings.TrimSuffix(filepath.Base(args[len(args)-1]), ".plist")
		if args[0] == "disable" {
			f.launchd.commands = append(f.launchd.commands, append([]string{name}, args...))
			f.launchd.disabled[label] = true
			return nil, nil
		}
		if args[0] == "bootout" && label == f.primary.Label {
			if h := f.lifetimes[f.primary.ID]; h != nil {
				h.Close()
				delete(f.lifetimes, f.primary.ID)
			}
		}
		return f.launchd.run(ctx, name, args...)
	}
	f.install.Manager = launchagent.Manager{Run: f.host.launchctl, UID: 501}
	exposeRollingL1(t, f)
	return f
}

func TestQuietCatalogRemovalAndSameIDLegacyReinstall(t *testing.T) {
	f := newQuietRemovalFixture(t)
	report, err := runSetupRemove(t.Context(), f.host, false)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = os.Stat(f.install.Root()); !os.IsNotExist(err) {
		t.Fatalf("active catalog remains: %v", err)
	}
	if len(report.Retained) == 0 {
		t.Fatal("retained generation evidence was not reported")
	}
	if _, err = runSetup(t.Context(), f.host, setupRequest{nodeID: f.node.NodeID}); err != nil {
		t.Fatalf("same-ID plain setup: %v", err)
	}
	registration := f.node.NodeRegistration
	registration.AgentGenerationID = ""
	registration.BootSessionID = "reinstalled-legacy"
	registration.Capabilities = map[string]bool{"kind:process": true}
	if _, err = f.h.store.RegisterNode(t.Context(), fabric.Identity{NodeID: f.node.NodeID}, registration, l1.DefaultNodePolicy(), true); err != nil {
		t.Fatalf("legacy reinstall stranded: %v", err)
	}
}

func addConversionGeneration(t *testing.T, f *conversionFixture) agentinstall.Store {
	t.Helper()
	paths, _ := resolveSetupPaths(f.home)
	baseRun := f.host.launchctl
	f.host.launchctl = func(ctx context.Context, name string, args ...string) ([]byte, error) {
		if args[0] == "disable" {
			label := filepath.Base(args[len(args)-1])
			f.launchd.disabled[label] = true
			f.launchd.commands = append(f.launchd.commands, append([]string{name}, args...))
			return nil, nil
		}
		return baseRun(ctx, name, args...)
	}
	store := agentinstall.Store{Data: paths.data, LaunchAgents: paths.launchAgents, Manager: launchagent.Manager{Run: f.host.launchctl, UID: 501}}
	g, err := store.Initialize(f.node.NodeID, filepath.Join(paths.bin, "wefty-agent"), "v1", rollingTestBoot, []int{1})
	if err != nil {
		t.Fatal(err)
	}
	base := launchagent.Agent{Label: setupNodeAgentLabel, Program: filepath.Join(paths.bin, "wefty-agent"), WorkingDirectory: paths.data, LogPath: filepath.Join(paths.logs, "node-agent.log"), Arguments: []string{"--profile=" + paths.profile}}
	if _, err = store.LoadCandidate(t.Context(), g.ID, base); err != nil {
		t.Fatal(err)
	}
	c, err := store.Read()
	if err != nil {
		t.Fatal(err)
	}
	g = c.Generations[g.ID]
	g.Process = &agentinstall.Binding{PID: 4242, ProcessStartID: "fixture-start", BootSessionID: "generation-boot", SystemBootID: rollingTestBoot, ReadyAt: f.now}
	c.Generations[g.ID] = g
	b, _ := json.Marshal(c)
	if err = os.WriteFile(filepath.Join(store.Root(), "catalog.json"), b, 0600); err != nil {
		t.Fatal(err)
	}
	f.node.AgentGenerationID, f.node.BootSessionID = g.ID, g.Process.BootSessionID
	f.host.upgrade.generationStore = func(setupPaths) agentinstall.Store { return store }
	delete(f.launchd.loaded, setupNodeAgentLabel)
	if err = os.Remove(filepath.Join(paths.launchAgents, setupNodeAgentLabel+".plist")); err != nil {
		t.Fatal(err)
	}
	return store
}

func TestQuietCatalogConversionKeepsPrimaryUntilHistory(t *testing.T) {
	f := newConversionFixture(t)
	store := addConversionGeneration(t, f)
	paths, _ := resolveSetupPaths(f.home)
	request := f.request(t)
	request.afterFleetStep = func(step string) error {
		if step == "convert:quiescent" {
			if _, err := store.Read(); err != nil {
				t.Fatalf("catalog removed before history: %v", err)
			}
			c, _ := store.Read()
			g := c.Generations[c.Primary]
			if _, err := os.Stat(g.Plist); err != nil {
				t.Fatal("primary plist removed before history", err)
			}
			if _, err := os.Stat(g.Executable); err != nil {
				t.Fatal("primary executable removed before history", err)
			}
		}
		return nil
	}
	report, err := runSetup(t.Context(), f.host, request)
	if err != nil {
		t.Fatal(err)
	}
	if report.History == nil || !report.History.ReadOnly {
		t.Fatal("conversion did not commit history")
	}
	if _, err = os.Stat(store.Root()); !os.IsNotExist(err) {
		t.Fatal("active generation catalog remains", err)
	}
	profile, err := connprofile.Read(paths.profile)
	if err != nil {
		t.Fatal(err)
	}
	if profile.Fabric == "plain" || profile.L1Address == "127.0.0.1:47101" {
		t.Fatal("join still targets local plain L1")
	}
	plist, err := os.ReadFile(filepath.Join(paths.launchAgents, setupNodeAgentLabel+".plist"))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(plist), "--agent-generation-") {
		t.Fatal("join retained generation layout")
	}
}

func TestQuietCatalogConversionRestoresRecordedGenerationBeforeHistory(t *testing.T) {
	for _, boundary := range []string{"convert:stop", "convert:quiescent", "convert:inventory"} {
		t.Run(boundary, func(t *testing.T) {
			f := newConversionFixture(t)
			store := addConversionGeneration(t, f)
			c, err := store.Read()
			if err != nil {
				t.Fatal(err)
			}
			request := f.request(t)
			reached := false
			request.afterFleetStep = func(step string) error {
				if step == boundary {
					reached = true
					return errors.New("injected pre-history failure")
				}
				return nil
			}
			if _, err = runSetup(t.Context(), f.host, request); err == nil {
				t.Fatal("missing interruption")
			}
			if !reached {
				t.Fatal("injected pre-history boundary was not exercised")
			}
			if !f.launchd.loaded[c.Generations[c.Primary].Label] || f.launchd.stopped[c.Generations[c.Primary].Label] {
				t.Fatal("generation primary not restored")
			}
			if f.launchd.loaded[setupNodeAgentLabel] {
				t.Fatal("legacy agent restored against generation L1")
			}
			if _, err = store.Read(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestQuietCatalogMaintenanceRefusesWithoutMutation(t *testing.T) {
	for _, command := range []string{"remove", "convert"} {
		for _, state := range []string{"retiring", "staged", "pending", "interrupted", "reboot_reconciled", "unreadable", "bad-receipt"} {
			t.Run(command+"/"+state, func(t *testing.T) {
				f := newConversionFixture(t)
				store := addConversionGeneration(t, f)
				paths, _ := resolveSetupPaths(f.home)
				c, err := store.Read()
				if err != nil {
					t.Fatal(err)
				}
				want, reason := 5, ""
				switch state {
				case "retiring", "staged":
					g := c.Generations[c.Primary]
					g.ID = strings.Repeat("a", 32)
					g.Label = "dev.wefty.node-agent.g-" + g.ID
					g.Executable = filepath.Join(store.Root(), g.ID, "wefty-agent")
					g.Plist = filepath.Join(paths.launchAgents, g.Label+".plist")
					g.Role = ""
					if state == "retiring" {
						g.Role = "retiring"
						reason = "wait for 1 retiring generations"
					} else {
						reason = "staged"
					}
					c.Generations[g.ID] = g
				case "pending":
					c.PendingReceipt = strings.Repeat("b", 32)
					reason = "pending"
				case "interrupted", "reboot_reconciled":
					phase := "promotion_requested"
					if state == "reboot_reconciled" {
						phase = state
					}
					r := upgradeRollingReceipt{Version: 1, OperationID: "interrupted", NodeID: c.NodeID, Phase: phase}
					if err = writeUpgradeRolling(paths, &r, phase); err != nil {
						t.Fatal(err)
					}
					reason = "wefty upgrade --rolling"
				case "unreadable":
					want = 1
				case "bad-receipt":
					want = 1
					if err = os.WriteFile(filepath.Join(paths.config, upgradeRollingFile), []byte("{"), 0600); err != nil {
						t.Fatal(err)
					}
				}
				b, _ := json.Marshal(c)
				if state == "unreadable" {
					b = []byte("{")
				}
				if err = os.WriteFile(filepath.Join(store.Root(), "catalog.json"), b, 0600); err != nil {
					t.Fatal(err)
				}
				before := quietMaintenanceFiles(t, f.home)
				f.launchd.commands = nil
				if command == "remove" {
					_, err = runSetupRemove(t.Context(), f.host, false)
				} else {
					_, err = runSetup(t.Context(), f.host, f.request(t))
				}
				if err == nil || commandExitCode(err) != want || reason != "" && !strings.Contains(err.Error(), reason) {
					t.Fatalf("exit=%d err=%v want=%d reason=%s", commandExitCode(err), err, want, reason)
				}
				if want == 5 {
					var typed *apiResponseError
					if !errors.As(err, &typed) {
						t.Fatalf("untyped refusal: %T", err)
					}
				}
				if !reflect.DeepEqual(before, quietMaintenanceFiles(t, f.home)) {
					t.Fatal("refusal/fault mutated disk")
				}
				if len(f.launchd.commands) != 0 {
					t.Fatal("refusal/fault reached launchd", f.launchd.commands)
				}
			})
		}
	}
}
func quietMaintenanceFiles(t *testing.T, home string) map[string]string {
	t.Helper()
	files := map[string]string{}
	err := filepath.WalkDir(home, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() {
			b, e := os.ReadFile(path)
			if e != nil {
				return e
			}
			files[path] = string(b)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return files
}

func TestQuietCatalogConversionForwardRetryAfterHistory(t *testing.T) {
	for _, boundary := range []string{"conversion:history_durable", "conversion:catalog_retired"} {
		t.Run(boundary, func(t *testing.T) {
			f := newConversionFixture(t)
			store := addConversionGeneration(t, f)
			f.host.maintenanceStep = func(step string) error {
				if step == boundary {
					panic("simulated kill")
				}
				return nil
			}
			func() {
				defer func() {
					if recover() == nil {
						t.Fatal("kill seam not reached")
					}
				}()
				_, _ = runSetup(t.Context(), f.host, f.request(t))
			}()
			paths, _ := resolveSetupPaths(f.home)
			r, e := readPlainConversion(paths)
			if e != nil || r == nil || !conversionForward(r) {
				t.Fatal("history boundary not durable", e)
			}
			f.host.maintenanceStep = nil
			if _, e = runSetup(t.Context(), f.host, f.request(t)); e != nil {
				t.Fatal(e)
			}
			if _, e = os.Stat(store.Root()); !os.IsNotExist(e) {
				t.Fatal("catalog remains", e)
			}
		})
	}
}

func TestQuietCatalogRemovalRealBinary(t *testing.T) {
	binary := buildPlainInstallCLI(t)
	f := newRollingFixture(t)
	exposeRollingL1(t, f)
	// The control-only fixture carries no payload. Its lifetime fixture is
	// released before fake bootout; real installed shutdown releases it itself.
	for id, h := range f.lifetimes {
		h.Close()
		delete(f.lifetimes, id)
	}
	if err := os.Remove(filepath.Join(f.paths.launchAgents, setupNodeAgentLabel+".plist")); err != nil && !os.IsNotExist(err) {
		t.Fatal(err)
	}
	state := t.TempDir()
	for _, label := range []string{setupL1Label, setupL3Label, f.primary.Label} {
		if err := os.WriteFile(filepath.Join(state, label), nil, 0600); err != nil {
			t.Fatal(err)
		}
	}
	runner := filepath.Join(t.TempDir(), "launchctl")
	script := `#!/bin/sh
label=${2##*/}
case "$1" in
print) if [ ! -f "$TEST_STATE/$label" ]; then echo 'Could not find service'; exit 113; fi; echo 'state = running'; echo 'pid = 4242' ;;
bootout) /bin/rm -f "$TEST_STATE/$label" ;;
disable|enable) ;;
*) exit 99 ;;
esac
`
	if err := os.WriteFile(runner, []byte(script), 0700); err != nil {
		t.Fatal(err)
	}
	command := exec.CommandContext(t.Context(), binary, "--json", "setup", "--remove")
	command.Env = []string{"HOME=" + f.home, "PATH=/usr/bin:/bin", "USER=owner", "TEST_LAUNCHCTL=" + runner, "TEST_STATE=" + state, "TEST_LOGIN_PATH=/usr/bin:/bin"}
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("real binary removal: %v\n%s", err, output)
	}
	var report setupRemoveReport
	if err = json.Unmarshal(output, &report); err != nil {
		t.Fatal(err, string(output))
	}
	if len(report.Retained) == 0 {
		t.Fatal("real binary omitted retained evidence")
	}
	if _, err = os.Stat(f.install.Root()); !os.IsNotExist(err) {
		t.Fatal("real binary left catalog", err)
	}
	if _, err = os.Stat(filepath.Join(state, f.primary.Label)); !os.IsNotExist(err) {
		t.Fatal("real binary left primary loaded", err)
	}
}

func TestQuietCatalogRemovalInterruptionConverges(t *testing.T) {
	for _, boundary := range []string{"maintenance:intent", "maintenance:admission_intent", "maintenance:admission_closed", "maintenance:de_enrollment_intent", "maintenance:de_enrolled", "maintenance:unloaded", "maintenance:catalog_retired"} {
		t.Run(boundary, func(t *testing.T) {
			f := newQuietRemovalFixture(t)
			f.host.maintenanceStep = func(step string) error {
				if step == boundary {
					panic("simulated kill")
				}
				return nil
			}
			func() {
				defer func() {
					if recover() == nil {
						t.Fatal("kill seam not reached")
					}
				}()
				_, _ = runSetupRemove(t.Context(), f.host, false)
			}()
			f.host.maintenanceStep = nil
			if _, err := runSetupRemove(t.Context(), f.host, false); err != nil {
				t.Fatal(err)
			}
			if _, err := os.Stat(f.install.Root()); !os.IsNotExist(err) {
				t.Fatal("catalog remains", err)
			}
			n, err := f.h.store.GetNode(t.Context(), f.node.NodeID)
			if err != nil {
				t.Fatal(err)
			}
			if n.AgentGenerationID != "" {
				t.Fatal("L1 remains enrolled")
			}
		})
	}
}

func TestQuietCatalogJoinRegistersWithFleetAuthority(t *testing.T) {
	f := newConversionFixture(t)
	store := addConversionGeneration(t, f)
	paths, _ := resolveSetupPaths(f.home)
	for _, name := range plainHistoryEntries {
		if strings.HasPrefix(name, "l1.sqlite") {
			if err := os.Remove(filepath.Join(paths.data, name)); err != nil && !os.IsNotExist(err) {
				t.Fatal(err)
			}
		}
	}
	local, err := l1.OpenStore(filepath.Join(paths.data, "l1.sqlite"), l1.StoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	registration := contract.NodeRegistration{NodeID: f.node.NodeID, AgentGenerationID: f.node.AgentGenerationID, BootSessionID: f.node.BootSessionID, OS: "darwin", Architecture: "arm64", AgentVersion: "v1", Capabilities: map[string]bool{"kind:process": true, "upgrade:retirement:v1": true}, CapabilityRevision: 1, CapabilityObservedAt: time.Now()}
	if _, err = local.RegisterNode(t.Context(), fabric.Identity{NodeID: f.node.NodeID}, registration, l1.DefaultNodePolicy(), true); err != nil {
		t.Fatal(err)
	}
	if err = local.Close(); err != nil {
		t.Fatal(err)
	}
	network := plain.NewNetwork()
	control := network.NewFabric(fabric.Identity{NodeID: "fleet-control"})
	fleet, err := l1.OpenStore(filepath.Join(t.TempDir(), "fleet.sqlite"), l1.StoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer fleet.Close()
	server, err := l1.NewServer(control, fleet, l1.ServerConfig{})
	if err != nil {
		t.Fatal(err)
	}
	listener, err := control.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := serveTestServer(ctx, func() error { return server.Serve(ctx, listener) })
	defer func() {
		cancel()
		if err := <-done; err != nil {
			t.Error(err)
		}
	}()
	request := f.request(t)
	request.joinCode = joinCodeForTest(t, "opaque-l1", "opaque-l3", "")
	report, err := runSetup(t.Context(), f.host, request)
	if err != nil {
		t.Fatal(err)
	}
	// Inspect a disposable copy, never open the retained read-only authority.
	b, err := os.ReadFile(filepath.Join(report.History.Directory, "l1.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	copyPath := filepath.Join(t.TempDir(), "l1.sqlite")
	if err = os.WriteFile(copyPath, b, 0600); err != nil {
		t.Fatal(err)
	}
	history, err := l1.OpenStore(copyPath, l1.StoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer history.Close()
	old, err := history.NodeGenerations(t.Context(), f.node.NodeID, f.node.NodeID)
	if err != nil {
		t.Fatal(err)
	}
	if len(old.Generations) != 1 || old.Generations[0].Role != l1.NodeGenerationPrimary {
		t.Fatal("local history was de-enrolled or modified")
	}
	profile, err := connprofile.Read(paths.profile)
	if err != nil {
		t.Fatal(err)
	}
	if profile.L1Address == "127.0.0.1:47101" {
		t.Fatal("join targets old authority")
	}
	nodeClient, err := agent.NewClient(quietFleetPeerFabric{Fabric: network.NewFabric(fabric.Identity{NodeID: "fleet-joined-node", Tags: []string{l1.DefaultAgentPrincipalTag}}), peer: profile.L1Address, endpoint: listener.Addr().String()}, profile.L1Address)
	if err != nil {
		t.Fatal(err)
	}
	defer nodeClient.Close()
	registration.NodeID, registration.AgentGenerationID, registration.BootSessionID = profile.Identities.Node, "", "fleet-boot"
	registration.Capabilities = map[string]bool{"kind:process": true}
	if _, err = nodeClient.Register(t.Context(), registration); err != nil {
		t.Fatalf("join registration reached local generation fence: %v", err)
	}
	if _, err = os.Stat(store.Root()); !os.IsNotExist(err) {
		t.Fatal("join retained active generation layout", err)
	}
}

// Model fleet peer-ID resolution while keeping real HTTP, agent registration
// and L1 authority. The selected opaque peer must be exactly the join profile's.
type quietFleetPeerFabric struct {
	fabric.Fabric
	peer, endpoint string
}

func (f quietFleetPeerFabric) Dial(ctx context.Context, network, address string) (net.Conn, error) {
	if address != f.peer {
		return nil, fmt.Errorf("unexpected fleet peer %q", address)
	}
	return f.Fabric.Dial(ctx, network, f.endpoint)
}

func TestQuietCatalogRemovalRestoresAfterInterruptedRollback(t *testing.T) {
	f := newQuietRemovalFixture(t)
	control := f.operations.generationControl
	inject := true
	f.operations.generationControl = func(ctx context.Context, store agentinstall.Store, r agentinstall.ControlRequest) (json.RawMessage, error) {
		if r.Action == "de_enroll_generation" && inject {
			inject = false
			return nil, &generationOperationFault{Message: "injected answered operation failure"}
		}
		return control(ctx, store, r)
	}
	setClaims := f.operations.setClaims
	failRestore := true
	f.operations.setClaims = func(ctx context.Context, p connprofile.Profile, r l1.NodeIntentRequest) (l1.Node, error) {
		if r.ClaimsEnabled && failRestore {
			failRestore = false
			return l1.Node{}, errors.New("injected restoration CAS failure")
		}
		return setClaims(ctx, p, r)
	}
	if _, err := runSetupRemove(t.Context(), f.host, false); err == nil || commandExitCode(err) != 1 {
		t.Fatalf("missing restoration fault: %v", err)
	}
	m, err := f.install.ReadMaintenance()
	if err != nil || m == nil || !m.Restoring || !m.ServiceResumed {
		t.Fatalf("resume success was not durable: %+v %v", m, err)
	}
	if !f.launchd.loaded[f.primary.Label] {
		t.Fatal("pre-de-enrollment failure stopped primary")
	}
	if _, err = runSetupRemove(t.Context(), f.host, false); err != nil {
		t.Fatal("retry stranded restored operation", err)
	}
}

func TestQuietCatalogConversionRejectsChangedPrimaryBeforeHistory(t *testing.T) {
	f := newConversionFixture(t)
	store := addConversionGeneration(t, f)
	c, err := store.Read()
	if err != nil {
		t.Fatal(err)
	}
	primary := c.Generations[c.Primary]
	if err = os.WriteFile(primary.Plist, []byte("owner-changed primary definition"), 0644); err != nil {
		t.Fatal(err)
	}
	before := quietMaintenanceFiles(t, f.home)
	f.launchd.commands = nil
	if _, err = runSetup(t.Context(), f.host, f.request(t)); err == nil {
		t.Fatal("changed primary definition accepted")
	}
	if !reflect.DeepEqual(before, quietMaintenanceFiles(t, f.home)) {
		t.Fatal("changed primary definition was detected after conversion had already mutated the installation")
	}
	if len(f.launchd.mutating()) != 0 {
		t.Fatal("changed primary definition was detected after launchd mutation")
	}
	if !f.node.ClaimsEnabled {
		t.Fatal("changed primary definition closed admission")
	}
}

func TestQuietCatalogProofRemovalRefusesBeforeMutation(t *testing.T) {
	f := newConversionFixture(t)
	paths, err := resolveProofSetupPaths(f.home)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.MkdirAll(paths.data, 0700); err != nil {
		t.Fatal(err)
	}
	store := agentinstall.Store{Data: paths.data, LaunchAgents: paths.launchAgents}
	if _, err = store.Initialize(f.node.NodeID, filepath.Join(f.home, setupFolders.bin, "wefty-agent"), "v1", rollingTestBoot, []int{1}); err != nil {
		t.Fatal(err)
	}
	c, err := store.Read()
	if err != nil {
		t.Fatal(err)
	}
	c.PendingReceipt = strings.Repeat("b", 32)
	payload, _ := json.Marshal(c)
	if err = os.WriteFile(filepath.Join(store.Root(), "catalog.json"), payload, 0600); err != nil {
		t.Fatal(err)
	}
	before := quietMaintenanceFiles(t, f.home)
	f.launchd.commands = nil
	_, err = removeProofSetup(t.Context(), f.host, true)
	var refusal *apiResponseError
	if commandExitCode(err) != 5 || !errors.As(err, &refusal) || !strings.Contains(err.Error(), "pending") {
		t.Fatalf("proof gate did not preserve typed generation refusal: %v", err)
	}
	if !reflect.DeepEqual(before, quietMaintenanceFiles(t, f.home)) || len(f.launchd.commands) != 0 {
		t.Fatal("proof refusal mutated files or launchd")
	}
}
