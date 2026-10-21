package main

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/fabric/plain"
	"github.com/Derek-X-Wang/wefty/internal/connprofile"
	"github.com/Derek-X-Wang/wefty/internal/launchagent"
	"github.com/Derek-X-Wang/wefty/l1"
)

// Build the real CLI entry point with its machine adapters replaced. The
// doctor platform selection also uses the simulated Mac on Linux test hosts. The injected runner is a subprocess fake, never real launchctl.
// There are no test environment hooks in the shipped binary.
func buildPlainInstallCLI(t *testing.T) string {
	return buildPlainInstallCLIForPlatform(t, "darwin")
}

func buildPlainInstallCLIForPlatform(t *testing.T, goos string) string {
	t.Helper()
	dir := t.TempDir()
	source, err := os.ReadFile("setup_command.go")
	if err != nil {
		t.Fatal(err)
	}
	modified, err := plainInstallOverlay(string(source))
	if err != nil {
		t.Fatal(err) // Fail before writing an overlay, building or running a binary.
	}
	nodeSource, err := os.ReadFile("local_node_command.go")
	if err != nil {
		t.Fatal(err)
	}
	nodeModified, err := plainDoctorOverlayForPlatform(string(nodeSource), goos)
	if err != nil {
		t.Fatal(err)
	}
	nodeReplacement := filepath.Join(dir, "local_node_command.go")
	if err := os.WriteFile(nodeReplacement, []byte(nodeModified), 0o600); err != nil {
		t.Fatal(err)
	}
	replacement := filepath.Join(dir, "setup_command.go")
	if err := os.WriteFile(replacement, []byte(modified), 0o600); err != nil {
		t.Fatal(err)
	}
	absolute, _ := filepath.Abs("setup_command.go")
	nodeAbsolute, _ := filepath.Abs("local_node_command.go")
	overlay, _ := json.Marshal(map[string]any{"Replace": map[string]string{absolute: replacement, nodeAbsolute: nodeReplacement}})
	overlayPath := filepath.Join(dir, "overlay.json")
	if err := os.WriteFile(overlayPath, overlay, 0o600); err != nil {
		t.Fatal(err)
	}
	binary := filepath.Join(dir, "wefty")
	// This child inherits the outer golock, caches and compiler limits.
	if output, err := exec.Command("go", "build", "-overlay", overlayPath, "-o", binary, ".").CombinedOutput(); err != nil {
		t.Fatalf("build injected CLI: %v\n%s", err, output)
	}
	return binary
}

// Every machine adapter must occur exactly once. Source drift must never
// produce a test binary with even one live machine adapter left in place.
func plainInstallOverlay(source string) (string, error) {
	replacements := []struct{ pattern, replacement string }{
		{regexp.QuoteMeta("goos: runtime.GOOS, euid: os.Geteuid(), uid: os.Getuid()"), `goos: func() string { _ = runtime.GOOS; return "darwin" }(), euid: func() int { if os.Getenv("TEST_ROOT") == "1" { return 0 }; return 501 }(), uid: 501`},
		{`launchctl:\s+launchagent.ExecRunner`, `launchctl: func(ctx context.Context, _ string, args ...string) ([]byte, error) {
 command := exec.CommandContext(ctx, os.Getenv("TEST_LAUNCHCTL"), args...)
 output, err := command.CombinedOutput()
 if err != nil && strings.Contains(string(output), "Could not find service") { return output, launchagent.ErrServiceNotFound }
 return output, err
 }`},
		{regexp.QuoteMeta("loginShell: runLoginShell"), `loginShell: func(context.Context, string, ...string) ([]byte, error) { return []byte(setupPATHBegin+"\n"+os.Getenv("TEST_LOGIN_PATH")+"\n"+setupPATHEnd+"\n"), nil }`},
		// Each unload check starts the fake launchctl, which can take well
		// over 100ms on a loaded CI runner; only the stuck case waits it out.
		{regexp.QuoteMeta("l1Address: defaultSetupL1Address, l3Address: defaultSetupL3Address,"), "l1Address: defaultSetupL1Address, l3Address: defaultSetupL3Address, unloadBound: 2*time.Second,"},
	}
	for _, replacement := range replacements {
		var err error
		source, err = replacePlainAdapter(source, replacement.pattern, replacement.replacement)
		if err != nil {
			return "", err
		}
	}
	return source, nil
}

func replacePlainAdapter(source, expression, replacement string) (string, error) {
	pattern := regexp.MustCompile(expression)
	if count := len(pattern.FindAllStringIndex(source, -1)); count != 1 {
		return "", fmt.Errorf("unsafe plain install overlay: %s matched %d times, want exactly once", expression, count)
	}
	return pattern.ReplaceAllStringFunc(source, func(string) string { return replacement }), nil
}

func plainDoctorOverlay(source string) (string, error) {
	return plainDoctorOverlayForPlatform(source, "darwin")
}
func plainDoctorOverlayForPlatform(source, goos string) (string, error) {
	pattern := regexp.MustCompile(regexp.QuoteMeta("plainDoctorSelected(runtime.GOOS, options.nodeConfigPath)"))
	if count := len(pattern.FindAllStringIndex(source, -1)); count != 2 {
		return "", fmt.Errorf("unsafe doctor platform overlay: matched %d selectors, want exactly two", count)
	}
	replacement := fmt.Sprintf(`plainDoctorSelected(func() string { _ = runtime.GOOS; return %q }(), options.nodeConfigPath)`, goos)
	return pattern.ReplaceAllStringFunc(source, func(string) string { return replacement }), nil
}

func TestPlainInstallOverlayRejectsSourceDrift(t *testing.T) {
	for _, adapter := range []struct {
		file    string
		overlay func(string) (string, error)
		needles []string
	}{
		{"setup_command.go", plainInstallOverlay, []string{"goos: runtime.GOOS, euid: os.Geteuid(), uid: os.Getuid()", "launchagent.ExecRunner", "loginShell: runLoginShell", "l1Address: defaultSetupL1Address, l3Address: defaultSetupL3Address,"}},
		{"local_node_command.go", plainDoctorOverlay, []string{"plainDoctorSelected(runtime.GOOS, options.nodeConfigPath)"}},
	} {
		payload, err := os.ReadFile(adapter.file)
		if err != nil {
			t.Fatal(err)
		}
		source := string(payload)
		if _, err := adapter.overlay(source); err != nil {
			t.Fatal(err)
		}
		for _, needle := range adapter.needles {
			for _, duplicate := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/duplicate=%t", needle, duplicate), func(t *testing.T) {
					changed := strings.Replace(source, needle, "source-drift", 1)
					if duplicate {
						// Duplicate the full adapter line, including the launchctl field.
						for _, line := range strings.Split(source, "\n") {
							if strings.Contains(line, needle) {
								changed = source + "\n" + line
								break
							}
						}
					}
					if modified, err := adapter.overlay(changed); err == nil || modified != "" {
						t.Fatalf("unsafe overlay accepted: %v", err)
					}
				})
			}
		}
	}
}

type plainBinaryFixture struct {
	setup                        *setupFixture
	binary, state, runner, tools string
	environment                  []string
	stopL1                       func()
}

func newPlainBinaryFixture(t *testing.T, binary string) *plainBinaryFixture {
	t.Helper()
	f := newSetupFixture(t)
	tools := t.TempDir()
	for _, name := range []string{"go", "git", "node", "gh", "claude", "codex"} {
		if err := os.WriteFile(filepath.Join(tools, name), []byte("#!/bin/sh\nexit 0\n"), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	f.loginPATH = tools
	state := t.TempDir()
	runner := filepath.Join(t.TempDir(), "launchctl")
	script := `#!/bin/sh
label=${2##*/}
case "$1" in
print)
 if [ -f "$TEST_STATE/print-error" ]; then echo 'Operation not permitted'; exit 5; fi
 if [ ! -f "$TEST_STATE/$label" ]; then echo 'Could not find service'; exit 113; fi
 if [ -f "$TEST_STATE/stopped-$label" ]; then echo 'state = not running'; else echo 'state = running'; fi ;;
bootout)
 if [ -n "$TEST_REPLACE_FILE" ]; then printf custom > "$TEST_REPLACE_FILE"; fi
 if [ -f "$TEST_STATE/bootout-error" ]; then echo 'Boot-out failed'; exit 5; fi
 if [ ! -f "$TEST_STATE/stuck" ]; then /bin/rm -f "$TEST_STATE/$label"; fi ;;
enable) ;;
bootstrap) label=${3##*/}; label=${label%.plist}; : > "$TEST_STATE/$label" ;;
kickstart) /bin/rm -f "$TEST_STATE/stopped-$label" ;;
*) exit 99 ;;
esac
`
	if err := os.WriteFile(runner, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	for _, label := range []string{setupL1Label, setupL3Label, setupNodeAgentLabel} {
		if err := os.WriteFile(filepath.Join(state, label), nil, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	return &plainBinaryFixture{setup: f, binary: binary, state: state, runner: runner, tools: tools,
		environment: []string{"HOME=" + f.home, "PATH=/usr/bin:/bin", "USER=owner", "TEST_LAUNCHCTL=" + runner, "TEST_STATE=" + state, "TEST_LOGIN_PATH=" + tools}}
}
func (f *plainBinaryFixture) run(t *testing.T, args ...string) (int, string, string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, f.binary, args...)
	command.Env = f.environment
	var out, errOut strings.Builder
	command.Stdout, command.Stderr = &out, &errOut
	err := command.Run()
	code := 0
	if err != nil {
		if exit, ok := err.(*exec.ExitError); ok {
			code = exit.ExitCode()
		} else {
			t.Fatalf("CLI: %v", err)
		}
	}
	return code, out.String(), errOut.String()
}
func TestPlainDoctorRealBinary(t *testing.T) {
	binary := buildPlainInstallCLI(t)
	// Every row asserts the published finding and exit, not merely a failure.
	cases := []struct {
		name, check, severity, detail string
		damage                        func(*testing.T, *plainBinaryFixture)
	}{
		{name: "healthy"},
		{name: "existing mixed-case node", damage: func(t *testing.T, f *plainBinaryFixture) {
			path := filepath.Join(f.setup.home, ".config/wefty/node.json")
			mustWrite(t, path, `{"version":1,"node_id":"Test-Mac"}`)
			profile := f.setup.profile(t)
			profile.Identities.Node = "Test-Mac"
			profile.NodeAdmission[0].NodeID = "Test-Mac"
			if err := connprofile.Write(filepath.Join(f.setup.home, ".config/wefty/profile.json"), profile); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "services down", check: setupNodeAgentLabel, severity: "error", detail: "not loaded", damage: func(t *testing.T, f *plainBinaryFixture) {
			for _, label := range []string{setupL1Label, setupL3Label, setupNodeAgentLabel} {
				mustRemove(t, filepath.Join(f.state, label))
			}
			f.stopL1()
		}},
		{name: "config linked outside home", check: "path:" + setupFolders.config, severity: "error", detail: "outside the home", damage: func(t *testing.T, f *plainBinaryFixture) {
			config := filepath.Join(f.setup.home, setupFolders.config)
			outside := filepath.Join(f.state, "outside-config")
			if err := os.Rename(config, outside); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(outside, config); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "unreadable receipt", check: "connection-profile", severity: "error", detail: "unexpected EOF", damage: func(t *testing.T, f *plainBinaryFixture) {
			// Keep an independent local finding which the receipt must not hide.
			mustWrite(t, filepath.Join(f.setup.home, setupFolders.config, "profile.json"), "{")
			if err := os.Chmod(filepath.Join(f.setup.home, setupFolders.config, setupManifestName), 0000); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "bad profile", check: "connection-profile", severity: "error", damage: func(t *testing.T, f *plainBinaryFixture) {
			mustWrite(t, filepath.Join(f.setup.home, ".config/wefty/profile.json"), "{")
		}},
		{name: "missing profile", check: "connection-profile", severity: "error", damage: func(t *testing.T, f *plainBinaryFixture) {
			mustRemove(t, filepath.Join(f.setup.home, ".config/wefty/profile.json"))
		}},
		{name: "bad node config", check: "node-config", severity: "error", detail: "invalid JSON", damage: func(t *testing.T, f *plainBinaryFixture) {
			mustWrite(t, filepath.Join(f.setup.home, ".config/wefty/node.json"), "{")
		}},
		{name: "truncated node config", check: "node-config", severity: "error", detail: "invalid JSON", damage: func(t *testing.T, f *plainBinaryFixture) {
			mustWrite(t, filepath.Join(f.setup.home, ".config/wefty/node.json"), `{"version":1,"node_id":"test-mac"`)
		}},
		{name: "invalid node config version", check: "node-config", severity: "error", detail: "invalid version", damage: func(t *testing.T, f *plainBinaryFixture) {
			mustWrite(t, filepath.Join(f.setup.home, ".config/wefty/node.json"), `{"version":2,"node_id":"test-mac"}`)
		}},
		{name: "invalid node identity", check: "node-config", severity: "error", damage: func(t *testing.T, f *plainBinaryFixture) {
			mustWrite(t, filepath.Join(f.setup.home, ".config/wefty/node.json"), `{"version":1,"node_id":" bad "}`)
		}},
		{name: "missing node config", check: "node-config", severity: "error", damage: func(t *testing.T, f *plainBinaryFixture) {
			mustRemove(t, filepath.Join(f.setup.home, ".config/wefty/node.json"))
		}},
		{name: "wrong node", check: "node-config", severity: "error", damage: func(t *testing.T, f *plainBinaryFixture) {
			mustWrite(t, filepath.Join(f.setup.home, ".config/wefty/node.json"), `{"version":1,"node_id":"other-node"}`)
		}},
		{name: "stale claude skill", check: "skill:.claude/skills/wefty/SKILL.md", severity: "error", damage: func(t *testing.T, f *plainBinaryFixture) {
			mustWrite(t, filepath.Join(f.setup.home, ".claude/skills/wefty/SKILL.md"), "stale")
		}},
		{name: "stale codex skill", check: "skill:.agents/skills/wefty/SKILL.md", severity: "error", damage: func(t *testing.T, f *plainBinaryFixture) {
			mustWrite(t, filepath.Join(f.setup.home, ".agents/skills/wefty/SKILL.md"), "stale")
		}},
		{name: "recorded PATH missing", check: "node-agent-PATH", severity: "error", damage: func(t *testing.T, f *plainBinaryFixture) {
			mustRemove(t, launchagent.Path(f.setup.home, setupNodeAgentLabel))
		}},
		{name: "L1 unreachable", check: "local-node", severity: "warning", detail: "NOT-RUN: L1 is unavailable", damage: func(t *testing.T, f *plainBinaryFixture) {
			f.environment = append(f.environment, "WEFTY_L1_ADDR=127.0.0.1:1")
		}},
		{name: "L1 unauthorized", check: "local-node", severity: "error", detail: "L1 refused this identity: HTTP 401 (unauthorized)"},
		{name: "L1 forbidden", check: "local-node", severity: "error", detail: "L1 refused this identity: HTTP 403 (forbidden)"},
		{name: "L1 node unknown", check: "local-node", severity: "error", detail: "not known to L1"},
		{name: "L1 tag missing", check: "local-node", severity: "error", detail: "reserved routing tag"},
		{name: "launchctl unknown", check: setupL1Label, severity: "error", detail: "cannot tell whether", damage: func(t *testing.T, f *plainBinaryFixture) { mustWrite(t, filepath.Join(f.state, "print-error"), "") }},
	}
	for _, label := range []string{setupL1Label, setupL3Label, setupNodeAgentLabel} {
		cases = append(cases, struct {
			name, check, severity, detail string
			damage                        func(*testing.T, *plainBinaryFixture)
		}{"unloaded " + label, label, "error", "not loaded", func(t *testing.T, f *plainBinaryFixture) { mustRemove(t, filepath.Join(f.state, label)) }}, struct {
			name, check, severity, detail string
			damage                        func(*testing.T, *plainBinaryFixture)
		}{"stopped " + label, label, "error", "not running", func(t *testing.T, f *plainBinaryFixture) { mustWrite(t, filepath.Join(f.state, "stopped-"+label), "") }})
	}
	for _, tool := range []string{"go", "git", "node", "gh", "claude", "codex"} {
		severity := "warning"
		if tool == "git" {
			severity = "error"
		}
		cases = append(cases, struct {
			name, check, severity, detail string
			damage                        func(*testing.T, *plainBinaryFixture)
		}{"missing " + tool, "tool:" + tool, severity, "missing", func(t *testing.T, f *plainBinaryFixture) { mustRemove(t, filepath.Join(f.tools, tool)) }})
	}
	for _, folder := range []string{setupFolders.bin, setupFolders.config, setupFolders.data, setupFolders.logs, setupFolders.launchAgents, filepath.Join(setupFolders.data, "node-agent/log-spool"), filepath.Join(setupFolders.data, "node-agent/managed")} {
		cases = append(cases, struct {
			name, check, severity, detail string
			damage                        func(*testing.T, *plainBinaryFixture)
		}{"unwritable " + folder, "path:" + folder, "error", "write", func(t *testing.T, f *plainBinaryFixture) {
			path := filepath.Join(f.setup.home, folder)
			if err := os.Chmod(path, 0o500); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { os.Chmod(path, 0o700) })
		}})
	}

	for _, folder := range []string{setupFolders.bin, setupFolders.config, setupFolders.data, setupFolders.logs, setupFolders.launchAgents, filepath.Join(setupFolders.data, "node-agent/log-spool"), filepath.Join(setupFolders.data, "node-agent/managed"), setupFolders.skills[0], setupFolders.skills[1]} {
		cases = append(cases, struct {
			name, check, severity, detail string
			damage                        func(*testing.T, *plainBinaryFixture)
		}{"missing folder " + folder, "path:" + folder, "error", "", func(t *testing.T, f *plainBinaryFixture) {
			if err := os.RemoveAll(filepath.Join(f.setup.home, folder)); err != nil {
				t.Fatal(err)
			}
		}})
	}
	cases = append(cases, struct {
		name, check, severity, detail string
		damage                        func(*testing.T, *plainBinaryFixture)
	}{"git not executable", "tool:git", "error", "missing", func(t *testing.T, f *plainBinaryFixture) {
		if err := os.Chmod(filepath.Join(f.tools, "git"), 0o600); err != nil {
			t.Fatal(err)
		}
	}})
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			f := newPlainBinaryFixture(t, binary)
			nodeID := "test-mac"
			if test.name == "existing mixed-case node" {
				nodeID = "Test-Mac"
			}

			const fabricID = "plain-doctor-real-l1"
			profilePath := filepath.Join(f.setup.home, ".config/wefty/profile.json")
			if err := connprofile.Write(profilePath, connprofile.Profile{Version: connprofile.Version, Fabric: "plain", PlainFabricID: fabricID}); err != nil {
				t.Fatal(err)
			}
			network, err := plain.NewNetworkWithID(fabricID)
			if err != nil {
				t.Fatal(err)
			}
			participant := network.NewFabric(fabric.Identity{NodeID: "doctor-test-l1"})
			store, err := l1.OpenStore(filepath.Join(t.TempDir(), "l1.sqlite"), l1.StoreOptions{})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { store.Close() })
			policy := l1.DefaultNodePolicy("wefty:node:test-mac")
			if test.name == "L1 tag missing" {
				policy = l1.DefaultNodePolicy("other")
			}
			if test.name != "L1 node unknown" {
				if _, err := store.RegisterNode(t.Context(), fabric.Identity{NodeID: nodeID, FabricID: fabricID, Tags: []string{l1.DefaultAgentPrincipalTag}}, contract.NodeRegistration{
					NodeID: nodeID, BootSessionID: "doctor-test-boot", OS: "darwin", Architecture: "arm64", AgentVersion: "test", Capabilities: map[string]bool{"kind:process": true},
				}, policy, true); err != nil {
					t.Fatal(err)
				}
			}
			controlPlane, err := l1.NewServer(participant, store, l1.ServerConfig{NodePolicies: map[string]l1.NodePolicy{nodeID: policy}})
			if err != nil {
				t.Fatal(err)
			}
			handler := controlPlane.Handler()
			if test.name == "L1 unauthorized" || test.name == "L1 forbidden" {
				handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					code, status := "unauthorized", 401
					if test.name == "L1 forbidden" {
						code, status = "forbidden", 403
					}
					w.Header().Set("Content-Type", "application/json")
					w.WriteHeader(status)
					fmt.Fprintf(w, `{"error":{"code":%q,"message":"identity refused"}}`, code)
				})
			}
			server := httptest.NewUnstartedServer(handler)

			listener, err := participant.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			server.Listener.Close()
			server.Listener = listener
			server.Start()
			f.stopL1 = server.Close
			defer server.Close()
			f.setup.host.l1Address = strings.TrimPrefix(server.URL, "http://")
			f.setup.setup(t, setupRequest{nodeID: "test-mac"})
			if test.damage != nil {
				test.damage(t, f)
			}
			code, out, errOut := f.run(t, "--json", "node", "doctor")
			var report struct {
				Healthy  bool                                            `json:"healthy"`
				Findings []struct{ Check, Severity, Detail, Fix string } `json:"findings"`
			}
			if err := json.Unmarshal([]byte(out), &report); err != nil {
				t.Fatalf("not a doctor report: exit=%d stdout=%s stderr=%s: %v", code, out, errOut, err)
			}
			if test.check == "" {
				textCode, text, textErr := f.run(t, "node", "doctor")
				if textCode != 0 || strings.Contains(text, "Fix:") || textErr != "" {
					t.Fatalf("healthy text: %d %s %s", textCode, text, textErr)
				}
				if code != 0 || !report.Healthy {
					t.Fatalf("healthy exit=%d report=%s stderr=%s", code, out, errOut)
				}
				return
			}
			if code != 12 || report.Healthy || errOut != "" {
				t.Fatalf("broken exit=%d report=%s stderr=%s", code, out, errOut)
			}
			if test.name == "config linked outside home" || test.name == "unreadable receipt" {
				textCode, text, textErr := f.run(t, "node", "doctor")
				if textCode != 12 {
					t.Fatalf("damaged plain text exit=%d stdout=%s stderr=%s", textCode, text, textErr)
				}
				if textErr != "wefty: plain-process node doctor found issues; follow the fix hints\n" {
					t.Fatalf("plain doctor stderr changed: %s", textErr)
				}
				text = strings.ReplaceAll(text, f.setup.home, "<HOME>")
				text = strings.ReplaceAll(text, f.state, "<STATE>")
				name := "plain-doctor-" + strings.ReplaceAll(test.name, " ", "-") + "-base.txt"
				assertPlainBaseBytes(t, name, text)
			}
			if test.name == "services down" {
				for _, label := range []string{setupL1Label, setupL3Label, setupNodeAgentLabel} {
					found := false
					for _, finding := range report.Findings {
						if finding.Check == label && finding.Severity == "error" && finding.Detail == "not loaded" {
							found = true
						}
					}
					if !found {
						t.Fatalf("services down omitted %s: %s", label, out)
					}
				}
				if len(report.Findings) < 20 {
					t.Fatalf("services down prevented local checks: %s", out)
				}
			}
			for _, finding := range report.Findings {
				if finding.Check == test.check && finding.Severity == test.severity && strings.Contains(finding.Detail, test.detail) && finding.Fix != "" {
					return
				}
			}
			t.Fatalf("missing %s %s finding with fix: %s", test.check, test.severity, out)
		})
	}
}
func mustWrite(t *testing.T, path, text string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(text), 0o600); err != nil {
		t.Fatal(err)
	}
}
func mustRemove(t *testing.T, path string) {
	t.Helper()
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
}

func TestSetupRemoveRealBinary(t *testing.T) {
	binary := buildPlainInstallCLI(t)
	for _, purge := range []bool{false, true} {
		t.Run(fmt.Sprintf("purge=%t", purge), func(t *testing.T) {
			f := newPlainBinaryFixture(t, binary)
			// Install from fixture binaries; remove does not need their source later.
			f.setup.setup(t, setupRequest{})
			if err := os.RemoveAll(f.setup.source); err != nil {
				t.Fatal(err)
			}
			paths, _ := resolveSetupPaths(f.setup.home)
			for _, folder := range []string{paths.data, paths.logs} {
				mustWrite(t, filepath.Join(folder, "keep.txt"), "retained")
			}
			mustWrite(t, filepath.Join(paths.bin, "unrelated"), "keep")
			mustWrite(t, filepath.Join(paths.launchAgents, "unrelated.plist"), "keep")
			args := []string{"--json", "setup", "--remove"}
			if purge {
				args = append(args, "--purge")
			}
			for attempt := 0; attempt < 2; attempt++ {
				code, out, errOut := f.run(t, args...)
				wantCode := 0
				if purge {
					wantCode = 12
				}
				if code != wantCode || errOut != "" {
					t.Fatalf("remove attempt %d: exit=%d %s %s", attempt, code, out, errOut)
				}
				for _, path := range []string{paths.profile, paths.nodeConfig, filepath.Join(paths.bin, "wefty"), filepath.Join(paths.bin, "wefty-l1"), filepath.Join(paths.bin, "wefty-l3"), filepath.Join(paths.bin, "wefty-agent"), launchagent.Path(f.setup.home, setupL1Label), launchagent.Path(f.setup.home, setupL3Label), launchagent.Path(f.setup.home, setupNodeAgentLabel), filepath.Join(f.setup.home, ".claude/skills/wefty"), filepath.Join(f.setup.home, ".agents/skills/wefty")} {
					if _, err := os.Lstat(path); !os.IsNotExist(err) {
						t.Fatalf("remove left %s: %v", path, err)
					}
				}
				for _, label := range []string{setupL1Label, setupL3Label, setupNodeAgentLabel} {
					if _, err := os.Stat(filepath.Join(f.state, label)); !os.IsNotExist(err) {
						t.Fatalf("still loaded %s", label)
					}
				}
				for _, folder := range []string{paths.data, paths.logs} {
					_, err := os.Stat(filepath.Join(folder, "keep.txt"))
					if err != nil {
						t.Fatalf("purge=%t data %s: %v", purge, folder, err)
					}
				}
				for _, path := range []string{filepath.Join(paths.bin, "unrelated"), filepath.Join(paths.launchAgents, "unrelated.plist")} {
					if _, err := os.Stat(path); err != nil {
						t.Fatalf("removed unrelated file: %v", err)
					}
				}
			}
		})
	}
}

func TestSetupRemovePreservesUnownedFilesAndRetries(t *testing.T) {
	binary := buildPlainInstallCLI(t)
	for _, damage := range []string{"binary", "skill", "skill extra", "skill empty folder", "profile", "node config", "binary symlink", "skill symlink", "unload failure", "stuck unload", "inspection failure", "root", "escape", "purge redirected", "changed during unload", "plist changed", "plist missing", "bad manifest", "manifest outside allowlist", "legacy"} {
		t.Run(damage, func(t *testing.T) {
			f := newPlainBinaryFixture(t, binary)
			f.setup.setup(t, setupRequest{})
			paths, _ := resolveSetupPaths(f.setup.home)
			target := ""
			wantError := ""
			purge := false
			switch damage {
			case "binary":
				target = filepath.Join(paths.bin, "wefty-l1")
			case "skill":
				target = filepath.Join(f.setup.home, ".agents/skills/wefty/SKILL.md")
			case "skill extra":
				target = filepath.Join(f.setup.home, ".claude/skills/wefty/custom.txt")
			case "skill empty folder":
				target = filepath.Join(f.setup.home, ".claude/skills/wefty/custom-empty")
				if err := os.Mkdir(target, 0o755); err != nil {
					t.Fatal(err)
				}
			case "profile":
				target = paths.profile
			case "node config":
				target = paths.nodeConfig
			case "binary symlink":
				target = filepath.Join(paths.bin, "wefty-l1")
			case "skill symlink":
				target = filepath.Join(f.setup.home, ".agents/skills/wefty/SKILL.md")
			case "unload failure":
				mustWrite(t, filepath.Join(f.state, "bootout-error"), "")
				wantError = "unload LaunchAgent"
			case "stuck unload":
				mustWrite(t, filepath.Join(f.state, "stuck"), "")
				wantError = "still loaded"
			case "inspection failure":
				mustWrite(t, filepath.Join(f.state, "print-error"), "")
				wantError = "cannot tell whether"
			case "root":
				f.environment = append(f.environment, "TEST_ROOT=1")
				wantError = "must not run as root"
			case "escape":
				mustRemove(t, filepath.Join(paths.bin, "wefty"))
				if err := os.RemoveAll(paths.bin); err != nil {
					t.Fatal(err)
				}
				outside := t.TempDir()
				if err := os.Symlink(outside, paths.bin); err != nil {
					t.Fatal(err)
				}
				target = filepath.Join(outside, "keep")
				wantError = "outside the home folder"
			case "purge redirected":
				if err := os.RemoveAll(paths.logs); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(paths.bin, paths.logs); err != nil {
					t.Fatal(err)
				}
				target = filepath.Join(paths.bin, "keep")
				wantError = "purge requires standard"
				purge = true
			case "plist changed":
				mustWrite(t, launchagent.Path(f.setup.home, setupL1Label), "custom")
				wantError = "setup-owned definition cannot be verified"
			case "plist missing":
				mustRemove(t, launchagent.Path(f.setup.home, setupL1Label))
				wantError = "setup-owned definition cannot be verified"
			case "bad manifest":
				mustWrite(t, filepath.Join(paths.config, "setup-files.json"), "{")
				wantError = "invalid setup file digests"
			case "manifest outside allowlist":
				target = filepath.Join(paths.bin, "unrelated")
				manifestPath := filepath.Join(paths.config, "setup-files.json")
				payload, _ := os.ReadFile(manifestPath)
				var manifest map[string]any
				_ = json.Unmarshal(payload, &manifest)
				if manifest == nil {
					manifest = map[string]any{"version": 1, "files": map[string]any{}}
				}
				manifest["files"].(map[string]any)[filepath.Join(setupFolders.bin, "unrelated")] = fmt.Sprintf("sha256:%x", sha256.Sum256([]byte("custom")))
				payload, _ = json.Marshal(manifest)
				mustWrite(t, manifestPath, string(payload))
			case "changed during unload":
				target = filepath.Join(paths.bin, "wefty-l1")
				f.environment = append(f.environment, "TEST_REPLACE_FILE="+target)
			case "legacy":
				_ = os.Remove(filepath.Join(paths.config, "setup-files.json"))
			}
			if target != "" && damage != "changed during unload" && damage != "skill empty folder" {
				if strings.HasSuffix(damage, "symlink") {
					mustRemove(t, target)
					outside := filepath.Join(t.TempDir(), "keep")
					mustWrite(t, outside, "custom")
					if err := os.Symlink(outside, target); err != nil {
						t.Fatal(err)
					}
				} else {
					mustWrite(t, target, "custom")
				}
			}
			args := []string{"--json", "setup", "--remove"}
			if purge {
				args = append(args, "--purge")
			}
			for attempt := 0; attempt < 2; attempt++ {
				code, out, errOut := f.run(t, args...)
				if wantError != "" {
					if code == 0 || !strings.Contains(errOut, wantError) {
						t.Fatalf("expected %q refusal: exit=%d %s %s", wantError, code, out, errOut)
					}
					if damage == "unload failure" || damage == "stuck unload" || damage == "inspection failure" || damage == "root" || damage == "plist changed" || damage == "plist missing" || damage == "bad manifest" {
						if _, err := os.Stat(paths.profile); err != nil {
							t.Fatalf("refusal deleted profile: %v", err)
						}
					}
				} else {
					wantCode := 12
					if damage == "manifest outside allowlist" || damage == "profile" || damage == "node config" {
						wantCode = 0
					}
					if code != wantCode || errOut != "" {
						t.Fatalf("remove exit=%d %s %s", code, out, errOut)
					}
					if damage != "legacy" && damage != "manifest outside allowlist" && damage != "skill empty folder" && damage != "profile" && damage != "node config" && !strings.Contains(out, "content is not the file setup installed") {
						t.Fatalf("no ownership warning: %s", out)
					}
					if damage == "legacy" {
						if _, err := os.Stat(filepath.Join(paths.bin, "wefty-l1")); !os.IsNotExist(err) {
							t.Fatalf("legacy service binary remains: %v", err)
						}
					}
				}
				if damage == "skill empty folder" {
					info, err := os.Stat(target)
					if err != nil || !info.IsDir() || !strings.Contains(out, "not installed by setup") {
						t.Fatalf("unknown empty skill folder changed or unreported: %v %s", err, out)
					}
				}
				if damage == "profile" || damage == "node config" {
					if _, err := os.Lstat(target); !os.IsNotExist(err) {
						t.Fatalf("edited metadata remains: %v", err)
					}
				} else if target != "" && damage != "skill empty folder" {
					payload, err := os.ReadFile(target)
					if err != nil || string(payload) != "custom" {
						t.Fatalf("unowned file changed: %s %v", payload, err)
					}
				}
			}
			if damage == "unload failure" {
				mustRemove(t, filepath.Join(f.state, "bootout-error"))
				if code, out, errOut := f.run(t, args...); code != 0 {
					t.Fatalf("retry: %d %s %s", code, out, errOut)
				}
			}
		})
	}
}

func TestSetupRemovalFlagsRealBinary(t *testing.T) {
	binary := buildPlainInstallCLI(t)
	for _, args := range [][]string{{"setup", "--purge"}, {"setup", "--remove", "--from=/unused"}, {"setup", "--remove", "--node-id=other"}} {
		t.Run(strings.Join(args, " "), func(t *testing.T) {
			f := newPlainBinaryFixture(t, binary)
			code, _, errOut := f.run(t, args...)
			if code != exitUsage || !strings.Contains(errOut, "--purge requires --remove") {
				t.Fatalf("flags: exit=%d stderr=%s", code, errOut)
			}
		})
	}
}
