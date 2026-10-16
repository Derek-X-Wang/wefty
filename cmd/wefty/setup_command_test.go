package main

import (
	"bytes"
	"context"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/agent"
	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/internal/connprofile"
	"github.com/Derek-X-Wang/wefty/internal/launchagent"
	"github.com/Derek-X-Wang/wefty/runner/ocicontrol"
	processrunner "github.com/Derek-X-Wang/wefty/runner/process"
)

// These tests run `wefty setup` against a temporary home with an injected
// service-manager runner and an injected login shell: they assert the files
// it writes and the launchctl commands it issues, and no test reaches the
// real launchd.

const setupTestUID = 501

// fakeLaunchd answers launchctl the way launchd does for the owner's GUI
// domain: print fails for a label that is not loaded, and reports whether a
// loaded one is running. A service can be loaded but stopped, as after a
// clean exit, and a verb can be made to fail once.
type fakeLaunchd struct {
	loaded   map[string]bool
	stopped  map[string]bool
	failOnce map[string]error
	commands [][]string
}

func (launchd *fakeLaunchd) run(_ context.Context, name string, args ...string) ([]byte, error) {
	launchd.commands = append(launchd.commands, append([]string{name}, args...))
	if name != launchagent.Launchctl || len(args) == 0 {
		return nil, errors.New("unexpected command")
	}
	if err, fail := launchd.failOnce[args[0]]; fail {
		delete(launchd.failOnce, args[0])
		return nil, err
	}
	label := func(target string) string { return target[strings.LastIndex(target, "/")+1:] }
	switch args[0] {
	case "print":
		service := label(args[1])
		if !launchd.loaded[service] {
			return nil, fmt.Errorf("%w: Could not find service %q in domain for user gui: 501", launchagent.ErrServiceNotFound, service)
		}
		if launchd.stopped[service] {
			return []byte(args[1] + " = {\n\tstate = not running\n\tlast exit code = 0\n}\n"), nil
		}
		return []byte(args[1] + " = {\n\tstate = running\n\tpid = 4242\n}\n"), nil
	case "bootout":
		delete(launchd.loaded, label(args[1]))
		delete(launchd.stopped, label(args[1]))
	case "bootstrap":
		service := strings.TrimSuffix(filepath.Base(args[2]), ".plist")
		launchd.loaded[service], launchd.stopped[service] = true, false
	case "kickstart":
		delete(launchd.stopped, label(args[1]))
	case "enable":
	default:
		return nil, errors.New("unexpected launchctl verb")
	}
	return nil, nil
}

// mutating returns every command other than the read-only print.
func (launchd *fakeLaunchd) mutating() [][]string {
	var mutating [][]string
	for _, command := range launchd.commands {
		if command[1] != "print" {
			mutating = append(mutating, command)
		}
	}
	return mutating
}

type setupFixture struct {
	home, source string
	launchd      *fakeLaunchd
	loginPATH    string
	shellCalls   [][]string
	host         setupHost
}

func newSetupFixture(t *testing.T) *setupFixture {
	t.Helper()
	home, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	fixture := &setupFixture{
		home: home, source: t.TempDir(),
		launchd:   &fakeLaunchd{loaded: make(map[string]bool), stopped: make(map[string]bool), failOnce: make(map[string]error)},
		loginPATH: "/opt/homebrew/bin:/usr/bin:/bin",
	}
	for _, name := range setupBinaries {
		fixture.writeBinary(t, name, "v1")
	}
	fixture.host = setupHost{
		goos: "darwin", euid: setupTestUID, uid: setupTestUID, home: fixture.home,
		username: "owner", hostname: "Owners-MacBook-Pro.local", shell: "/bin/zsh",
		executable: filepath.Join(fixture.source, "wefty"),
		launchctl:  fixture.launchd.run,
		loginShell: func(_ context.Context, shell string, args ...string) ([]byte, error) {
			fixture.shellCalls = append(fixture.shellCalls, append([]string{shell}, args...))
			// An interactive login shell may print anything around the answer.
			return []byte("Last login: today\nwelcome\n" + setupPATHBegin + "\n" + fixture.loginPATH + "\n" + setupPATHEnd + "\n"), nil
		},
		random:    strings.NewReader(strings.Repeat("\x5a", 4096)),
		l1Address: "127.0.0.1:47101", l3Address: "127.0.0.1:47102",
	}
	return fixture
}

// writeBinary writes a stand-in that answers `--version` the way the real
// binaries do, so setup records version for the four of them.
func (fixture *setupFixture) writeBinary(t *testing.T, name, version string) {
	t.Helper()
	script := "#!/bin/sh\nif [ \"$1\" = --version ]; then echo " + version + "; exit 0; fi\necho " + name + " " + version + "\n"
	if err := os.WriteFile(filepath.Join(fixture.source, name), []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
}

func (fixture *setupFixture) setup(t *testing.T, request setupRequest) setupReport {
	t.Helper()
	fixture.launchd.commands = nil
	report, err := runSetup(t.Context(), fixture.host, request)
	if err != nil {
		t.Fatalf("wefty setup: %v", err)
	}
	return report
}

func (fixture *setupFixture) plist(t *testing.T, label string) string {
	t.Helper()
	payload, err := os.ReadFile(launchagent.Path(fixture.home, label))
	if err != nil {
		t.Fatal(err)
	}
	return string(payload)
}

func (fixture *setupFixture) profile(t *testing.T) connprofile.Profile {
	t.Helper()
	path, err := connprofile.DefaultPath(fixture.home)
	if err != nil {
		t.Fatal(err)
	}
	profile, err := connprofile.Read(path)
	if err != nil {
		t.Fatal(err)
	}
	return profile
}

func serviceActions(report setupReport) map[string]launchagent.Action {
	actions := make(map[string]launchagent.Action)
	for _, service := range report.Services {
		actions[service.Label] = service.Action
	}
	return actions
}

func TestSetupInstallsTheDailyStackIntoACleanHome(t *testing.T) {
	fixture := newSetupFixture(t)
	report := fixture.setup(t, setupRequest{})

	if !regexp.MustCompile(`^owners-macbook-pro-[0-9a-f]{8}$`).MatchString(report.NodeID) {
		t.Fatalf("node ID = %q, want one named after the host with a random suffix", report.NodeID)
	}
	nodeID := report.NodeID

	// The connection profile: stable loopback ports, the endpoints, the plain
	// Fabric identities and L1's node admission policy.
	profile := fixture.profile(t)
	if profile.Fabric != "plain" || !strings.HasPrefix(profile.PlainFabricID, "plain-") ||
		profile.L1Address != "127.0.0.1:47101" || profile.L3Address != "127.0.0.1:47102" ||
		!profile.AllowPlainPersonIdentities {
		t.Fatalf("profile = %+v", profile)
	}
	if profile.Identities != (connprofile.Identities{CLI: "wefty-cli", PersonUserID: "owner", PersonDeviceID: nodeID, Node: nodeID}) {
		t.Fatalf("profile identities = %+v", profile.Identities)
	}
	if len(profile.NodeAdmission) != 1 || profile.NodeAdmission[0].NodeID != nodeID ||
		!slices.Equal(profile.NodeAdmission[0].Tags, []string{contract.StableNodeTagPrefix + nodeID}) ||
		profile.NodeAdmission[0].MaxOneshotSlots <= 0 || profile.NodeAdmission[0].MaxServiceSlots <= 0 {
		t.Fatalf("node admission = %+v", profile.NodeAdmission)
	}

	// The node configuration records the stable node ID with no OCI control
	// socket, and the #887 reader accepts it.
	nodeConfig := filepath.Join(fixture.home, ".config", "wefty", "node.json")
	config, err := ocicontrol.ReadInstalledConfig(nodeConfig)
	if err != nil || config.NodeID != nodeID || config.ControlSocket != "" {
		t.Fatalf("installed node configuration = %+v err=%v", config, err)
	}

	// Binaries in ~/.local/bin, byte for byte.
	for _, name := range setupBinaries {
		installed, err := os.ReadFile(filepath.Join(fixture.home, ".local", "bin", name))
		if err != nil {
			t.Fatal(err)
		}
		source, _ := os.ReadFile(filepath.Join(fixture.source, name))
		info, _ := os.Stat(filepath.Join(fixture.home, ".local", "bin", name))
		if !bytes.Equal(installed, source) || info.Mode().Perm()&0o100 == 0 {
			t.Fatalf("installed %s = %q mode %v", name, installed, info.Mode())
		}
	}
	// Fresh data and log roots in the standard paths; no database is adopted.
	for _, directory := range []string{
		filepath.Join(fixture.home, "Library", "Application Support", "wefty"),
		filepath.Join(fixture.home, "Library", "Logs", "wefty"),
	} {
		if info, err := os.Stat(directory); err != nil || !info.IsDir() {
			t.Fatalf("%s: %v", directory, err)
		}
	}

	// Three LaunchAgents, each a well-formed plist that starts at login,
	// restarts after a crash, and reads the profile.
	data := filepath.Join(fixture.home, "Library", "Application Support", "wefty")
	profilePath := filepath.Join(fixture.home, ".config", "wefty", "profile.json")
	for label, wants := range map[string][]string{
		setupL1Label: {"<string>" + filepath.Join(fixture.home, ".local", "bin", "wefty-l1") + "</string>",
			"<string>--profile=" + profilePath + "</string>", "<string>--db=" + filepath.Join(data, "l1.sqlite") + "</string>",
			"<string>" + filepath.Join(fixture.home, "Library", "Logs", "wefty", "l1.log") + "</string>"},
		setupL3Label: {"<string>" + filepath.Join(fixture.home, ".local", "bin", "wefty-l3") + "</string>",
			"<string>--profile=" + profilePath + "</string>", "<string>--db=" + filepath.Join(data, "l3.sqlite") + "</string>"},
		setupNodeAgentLabel: {"<string>" + filepath.Join(fixture.home, ".local", "bin", "wefty-agent") + "</string>",
			"<string>--profile=" + profilePath + "</string>", "<key>ExitTimeOut</key>"},
	} {
		plist := fixture.plist(t, label)
		decoder := xml.NewDecoder(strings.NewReader(plist))
		for {
			if _, err := decoder.Token(); err == io.EOF {
				break
			} else if err != nil {
				t.Fatalf("%s is not well-formed XML: %v\n%s", label, err, plist)
			}
		}
		for _, want := range append(wants, "<key>Label</key>\n  <string>"+label+"</string>",
			"<key>RunAtLoad</key>\n  <true/>", "<key>SuccessfulExit</key>\n    <false/>") {
			if !strings.Contains(plist, want) {
				t.Fatalf("%s plist lacks %q:\n%s", label, want, plist)
			}
		}
		if strings.Contains(plist, "UserName") || strings.Contains(strings.ToLower(plist), "sudo") {
			t.Fatalf("%s plist is not a per-user LaunchAgent:\n%s", label, plist)
		}
		// Where macOS's own property-list parser exists, it must accept the
		// file too; it only reads it.
		if _, err := os.Stat("/usr/bin/plutil"); err == nil {
			if output, err := exec.Command("/usr/bin/plutil", "-lint", launchagent.Path(fixture.home, label)).CombinedOutput(); err != nil {
				t.Fatalf("plutil rejects %s: %v\n%s", label, err, output)
			}
		}
	}

	// The node agent's environment holds the captured login-shell PATH; the
	// bin folder is added because the owner's PATH lacked it.
	wantPATH := fixture.loginPATH + ":" + filepath.Join(fixture.home, ".local", "bin")
	if plist := fixture.plist(t, setupNodeAgentLabel); !strings.Contains(plist, "<key>PATH</key>\n    <string>"+wantPATH+"</string>") {
		t.Fatalf("node agent PATH is not the login-shell PATH %q:\n%s", wantPATH, plist)
	}
	if plist := fixture.plist(t, setupL1Label); strings.Contains(plist, fixture.loginPATH) {
		t.Fatalf("L1 received the login-shell PATH, which only the node agent's jobs need:\n%s", plist)
	}
	if len(fixture.shellCalls) != 1 || !slices.Equal(fixture.shellCalls[0][:4], []string{"/bin/zsh", "-l", "-i", "-c"}) {
		t.Fatalf("login shell calls = %v", fixture.shellCalls)
	}
	if len(report.Notes) != 1 || !strings.Contains(report.Notes[0], "not on your login-shell PATH") {
		t.Fatalf("notes = %v", report.Notes)
	}

	// The commands: only launchctl, only the owner's GUI domain, never sudo.
	var want [][]string
	for _, label := range []string{setupL1Label, setupL3Label, setupNodeAgentLabel} {
		want = append(want,
			[]string{launchagent.Launchctl, "print", "gui/501/" + label},
			[]string{launchagent.Launchctl, "enable", "gui/501/" + label},
			[]string{launchagent.Launchctl, "bootstrap", "gui/501", launchagent.Path(fixture.home, label)},
		)
	}
	if !slices.EqualFunc(fixture.launchd.commands, want, slices.Equal[[]string]) {
		t.Fatalf("launchctl commands = %v\nwant %v", fixture.launchd.commands, want)
	}
	for label, action := range serviceActions(report) {
		if action != launchagent.Loaded {
			t.Fatalf("%s action = %s", label, action)
		}
	}

	// The skill is copied for Claude Code and Codex.
	skill, err := os.ReadFile("../../skills/wefty/SKILL.md")
	if err != nil {
		t.Fatal(err)
	}
	for _, target := range []string{".claude", ".agents"} {
		copied, err := os.ReadFile(filepath.Join(fixture.home, target, "skills", "wefty", "SKILL.md"))
		if err != nil || !bytes.Equal(copied, skill) {
			t.Fatalf("%s skill copy differs: %v", target, err)
		}
	}
}

func TestSetupSecondRunChangesNothingAndReloadsOnlyWhatChanged(t *testing.T) {
	fixture := newSetupFixture(t)
	first := fixture.setup(t, setupRequest{})
	profileBefore, _ := os.ReadFile(filepath.Join(fixture.home, ".config", "wefty", "profile.json"))

	second := fixture.setup(t, setupRequest{})
	if len(second.Changes) != 0 || len(fixture.launchd.mutating()) != 0 || second.NodeID != first.NodeID {
		t.Fatalf("an unchanged rerun changed %v and issued %v", second.Changes, fixture.launchd.mutating())
	}
	for label, action := range serviceActions(second) {
		if action != launchagent.Unchanged {
			t.Fatalf("%s action on an unchanged rerun = %s", label, action)
		}
	}
	if profileAfter, _ := os.ReadFile(filepath.Join(fixture.home, ".config", "wefty", "profile.json")); !bytes.Equal(profileBefore, profileAfter) {
		t.Fatal("an unchanged rerun rewrote the profile")
	}

	reloadsOnly := func(t *testing.T, report setupReport, label string) {
		t.Helper()
		want := [][]string{
			{launchagent.Launchctl, "bootout", "gui/501/" + label},
			{launchagent.Launchctl, "enable", "gui/501/" + label},
			{launchagent.Launchctl, "bootstrap", "gui/501", launchagent.Path(fixture.home, label)},
		}
		if got := fixture.launchd.mutating(); !slices.EqualFunc(got, want, slices.Equal[[]string]) {
			t.Fatalf("launchctl commands = %v, want only the %s reload %v", got, label, want)
		}
		for service, action := range serviceActions(report) {
			if (service == label) != (action == launchagent.Reloaded) || service != label && action != launchagent.Unchanged {
				t.Fatalf("%s action = %s; only %s should reload", service, action, label)
			}
		}
	}

	// A changed run-ledger binary reloads only the run ledger.
	fixture.writeBinary(t, "wefty-l3", "v2")
	report := fixture.setup(t, setupRequest{})
	reloadsOnly(t, report, setupL3Label)
	if !slices.Contains(report.Changes, "installed "+filepath.Join(fixture.home, ".local", "bin", "wefty-l3")) || len(report.Changes) != 1 {
		t.Fatalf("changes = %v", report.Changes)
	}

	// The owner's edit to L1's node admission is kept, and reloads only L1.
	profile := fixture.profile(t)
	profile.NodeAdmission[0].MaxOneshotSlots = 8
	if err := connprofile.Write(filepath.Join(fixture.home, ".config", "wefty", "profile.json"), profile); err != nil {
		t.Fatal(err)
	}
	report = fixture.setup(t, setupRequest{})
	reloadsOnly(t, report, setupL1Label)
	if got := fixture.profile(t).NodeAdmission[0].MaxOneshotSlots; got != 8 || len(report.Changes) != 0 {
		t.Fatalf("setup replaced the owner's slot policy (%d) or reported changes %v", got, report.Changes)
	}

	// Setup preserves a local load setting and reloads only its node agent.
	for _, limit := range []float64{18, 0} {
		profile = fixture.profile(t)
		profile.OneshotLoadLimit = &limit
		if err := connprofile.Write(filepath.Join(fixture.home, ".config", "wefty", "profile.json"), profile); err != nil {
			t.Fatal(err)
		}
		report = fixture.setup(t, setupRequest{})
		reloadsOnly(t, report, setupNodeAgentLabel)
		got := fixture.profile(t).OneshotLoadLimit
		if got == nil || *got != limit || len(report.Changes) != 0 {
			t.Fatalf("setup replaced the load setting %v; changes=%v", got, report.Changes)
		}
	}

	// A changed login-shell PATH reloads only the node agent.
	fixture.loginPATH = "/opt/homebrew/bin:/usr/local/go/bin:/usr/bin:/bin"
	reloadsOnly(t, fixture.setup(t, setupRequest{}), setupNodeAgentLabel)

	// A service that is not loaded is loaded again, without a bootout.
	delete(fixture.launchd.loaded, setupL3Label)
	report = fixture.setup(t, setupRequest{})
	if got := fixture.launchd.mutating(); len(got) != 2 || got[1][1] != "bootstrap" || !strings.HasSuffix(got[1][3], setupL3Label+".plist") {
		t.Fatalf("repair commands = %v", got)
	}
	if actions := serviceActions(report); actions[setupL3Label] != launchagent.Loaded || actions[setupL1Label] != launchagent.Unchanged {
		t.Fatalf("repair actions = %v", actions)
	}

	// A changed skill copy is refreshed and nothing else moves.
	skillCopy := filepath.Join(fixture.home, ".claude", "skills", "wefty", "SKILL.md")
	if err := os.WriteFile(skillCopy, []byte("stale"), 0o644); err != nil {
		t.Fatal(err)
	}
	report = fixture.setup(t, setupRequest{})
	skill, _ := os.ReadFile("../../skills/wefty/SKILL.md")
	if copied, _ := os.ReadFile(skillCopy); !bytes.Equal(copied, skill) || len(report.Changes) != 1 || len(fixture.launchd.mutating()) != 0 {
		t.Fatalf("skill refresh: changes=%v commands=%v", report.Changes, fixture.launchd.mutating())
	}
}

func TestSetupRefusesRootAndNeverNeedsSudo(t *testing.T) {
	fixture := newSetupFixture(t)
	fixture.host.euid = 0
	_, err := runSetup(t.Context(), fixture.host, setupRequest{})
	if err == nil || !strings.Contains(err.Error(), "must not run as root") {
		t.Fatalf("setup as root = %v", err)
	}
	entries, readErr := os.ReadDir(fixture.home)
	if readErr != nil || len(entries) != 0 || len(fixture.launchd.commands) != 0 || len(fixture.shellCalls) != 0 {
		t.Fatalf("setup as root touched the home (%v, %v) or ran %v %v", entries, readErr, fixture.launchd.commands, fixture.shellCalls)
	}

	fixture.host.euid = setupTestUID
	fixture.setup(t, setupRequest{})
	fixture.writeBinary(t, "wefty-agent", "v2")
	fixture.setup(t, setupRequest{})
	for _, command := range fixture.launchd.commands {
		if command[0] != launchagent.Launchctl || !strings.HasPrefix(command[2], "gui/501") {
			t.Fatalf("setup issued %v outside the owner's GUI domain", command)
		}
	}
}

func TestSetupKeepsTheNodeIDAndLeavesAnOCINodeAlone(t *testing.T) {
	fixture := newSetupFixture(t)
	first := fixture.setup(t, setupRequest{nodeID: "derek-mac"})
	if first.NodeID != "derek-mac" {
		t.Fatalf("node ID = %q", first.NodeID)
	}
	// Kept across runs, and a different explicit ID is refused rather than
	// silently re-identifying this machine.
	if again := fixture.setup(t, setupRequest{}); again.NodeID != "derek-mac" {
		t.Fatalf("rerun node ID = %q", again.NodeID)
	}
	if _, err := runSetup(t.Context(), fixture.host, setupRequest{nodeID: "other-mac"}); err == nil || !strings.Contains(err.Error(), `already node "derek-mac"`) {
		t.Fatalf("changing the node ID = %v", err)
	}

	// An OCI node installation owns node.json on this machine: setup refuses
	// and leaves it exactly as it was.
	oci := newSetupFixture(t)
	nodeConfig := filepath.Join(oci.home, ".config", "wefty", "node.json")
	ociConfig := ocicontrol.InstalledConfig{Version: ocicontrol.InstalledConfigVersion, ControlSocket: "/tmp/wefty-control.sock", NodeID: "oci-node"}
	if err := ocicontrol.WriteInstalledConfig(nodeConfig, ociConfig); err != nil {
		t.Fatal(err)
	}
	before, _ := os.ReadFile(nodeConfig)
	if _, err := runSetup(t.Context(), oci.host, setupRequest{}); err == nil || !strings.Contains(err.Error(), "OCI node installation") {
		t.Fatalf("setup over an OCI node = %v", err)
	}
	if after, _ := os.ReadFile(nodeConfig); !bytes.Equal(before, after) || len(oci.launchd.commands) != 0 {
		t.Fatalf("setup changed the OCI node configuration or ran %v", oci.launchd.commands)
	}
	if _, err := os.Stat(filepath.Join(oci.home, ".config", "wefty", "profile.json")); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("a refused setup wrote the profile: %v", err)
	}
}

func TestSetupRefusesASkillLinkAndAMissingBinary(t *testing.T) {
	fixture := newSetupFixture(t)
	if err := os.MkdirAll(filepath.Join(fixture.home, ".claude", "skills"), 0o755); err != nil {
		t.Fatal(err)
	}
	checkout := t.TempDir()
	if err := os.Symlink(checkout, filepath.Join(fixture.home, ".claude", "skills", "wefty")); err != nil {
		t.Fatal(err)
	}
	if _, err := runSetup(t.Context(), fixture.host, setupRequest{}); err == nil || !strings.Contains(err.Error(), "symbolic link") {
		t.Fatalf("setup over a skill link = %v", err)
	}
	if entries, _ := os.ReadDir(checkout); len(entries) != 0 {
		t.Fatalf("setup wrote through the skill link: %v", entries)
	}
	if entries, _ := os.ReadDir(fixture.home); len(entries) != 1 || len(fixture.launchd.commands) != 0 {
		t.Fatalf("a refused setup still wrote %v or ran %v", entries, fixture.launchd.commands)
	}

	missing := newSetupFixture(t)
	if err := os.Remove(filepath.Join(missing.source, "wefty-l3")); err != nil {
		t.Fatal(err)
	}
	if _, err := runSetup(t.Context(), missing.host, setupRequest{}); err == nil || !strings.Contains(err.Error(), "build all four") {
		t.Fatalf("setup without wefty-l3 = %v", err)
	}
	if entries, _ := os.ReadDir(missing.home); len(entries) != 0 {
		t.Fatalf("setup without its binaries wrote %v", entries)
	}
}

func TestSetupJSONReportFromTheCommand(t *testing.T) {
	fixture := newSetupFixture(t)
	report := fixture.setup(t, setupRequest{})
	var human bytes.Buffer
	if err := writeSetupReport(&human, report); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(human.String(), "This machine is node "+report.NodeID) || !strings.Contains(human.String(), "wefty status") {
		t.Fatalf("human report:\n%s", human.String())
	}
	payload, err := json.Marshal(report)
	if err != nil {
		t.Fatal(err)
	}
	var decoded map[string]any
	if err := json.Unmarshal(payload, &decoded); err != nil || decoded["node_id"] != report.NodeID || decoded["services"] == nil {
		t.Fatalf("JSON report = %s", payload)
	}
}

// homeSnapshot lists every path under root with its type and contents, so a
// refused run can be shown to have changed nothing.
func homeSnapshot(t *testing.T, root string) map[string]string {
	t.Helper()
	snapshot := make(map[string]string)
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		value := info.Mode().String()
		if info.Mode().IsRegular() {
			payload, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			value += " " + string(payload)
		} else if info.Mode()&os.ModeSymlink != 0 {
			target, _ := os.Readlink(path)
			value += " -> " + target
		}
		snapshot[path] = value
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return snapshot
}

// No write may leave the home folder: a destination's ancestor that is a
// link to a folder outside home is refused before anything changes, while a
// link that stays inside home is followed and the real path is installed.
func TestSetupWritesOnlyInsideTheHomeThroughLinkedAncestors(t *testing.T) {
	for _, linked := range []string{".local", ".config", filepath.Join("Library", "LaunchAgents"), ".agents"} {
		t.Run("outside/"+linked, func(t *testing.T) {
			fixture := newSetupFixture(t)
			outside, err := filepath.EvalSymlinks(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			if err := os.MkdirAll(filepath.Dir(filepath.Join(fixture.home, linked)), 0o755); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(outside, filepath.Join(fixture.home, linked)); err != nil {
				t.Fatal(err)
			}
			before := homeSnapshot(t, fixture.home)
			_, err = runSetup(t.Context(), fixture.host, setupRequest{})
			if err == nil || !strings.Contains(err.Error(), "outside the home folder") {
				t.Fatalf("setup through %s linked outside home = %v", linked, err)
			}
			if entries, _ := os.ReadDir(outside); len(entries) != 0 {
				t.Fatalf("setup wrote outside the home folder: %v", entries)
			}
			if after := homeSnapshot(t, fixture.home); !reflect.DeepEqual(before, after) || len(fixture.launchd.commands) != 0 {
				t.Fatalf("a refused setup changed the home or ran %v", fixture.launchd.commands)
			}
		})
	}
	t.Run("inside", func(t *testing.T) {
		fixture := newSetupFixture(t)
		dotfiles := filepath.Join(fixture.home, "dotfiles", "config")
		if err := os.MkdirAll(dotfiles, 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(dotfiles, filepath.Join(fixture.home, ".config")); err != nil {
			t.Fatal(err)
		}
		report := fixture.setup(t, setupRequest{})
		real := filepath.Join(dotfiles, "wefty", "profile.json")
		if report.Profile != real {
			t.Fatalf("profile path = %s, want the real in-home path %s", report.Profile, real)
		}
		if _, err := connprofile.Read(real); err != nil {
			t.Fatal(err)
		}
		if plist := fixture.plist(t, setupL1Label); !strings.Contains(plist, "--profile="+real) {
			t.Fatalf("L1 plist does not name the real profile path:\n%s", plist)
		}
	})
}

// A reload whose bootout fails must not become a later "unchanged": the
// retry still sees the difference and reloads the service.
func TestSetupRetriesAReloadWhoseBootoutFailed(t *testing.T) {
	fixture := newSetupFixture(t)
	fixture.setup(t, setupRequest{})
	fixture.writeBinary(t, "wefty-l1", "v2")
	fixture.launchd.failOnce["bootout"] = errors.New("Boot-out failed: 5: Input/output error")
	fixture.launchd.commands = nil
	if _, err := runSetup(t.Context(), fixture.host, setupRequest{}); err == nil || !strings.Contains(err.Error(), "unload LaunchAgent "+setupL1Label) {
		t.Fatalf("setup with a failing bootout = %v", err)
	}
	report := fixture.setup(t, setupRequest{})
	if serviceActions(report)[setupL1Label] != launchagent.Reloaded {
		t.Fatalf("retry actions = %v, commands %v", serviceActions(report), fixture.launchd.mutating())
	}
	if got := fixture.launchd.mutating(); len(got) != 3 || got[0][1] != "bootout" || got[2][1] != "bootstrap" {
		t.Fatalf("retry commands = %v", got)
	}
}

// KeepAlive restarts a service only after a crash, so a clean exit leaves it
// loaded but stopped; setup starts it without reloading it.
func TestSetupStartsALoadedButStoppedService(t *testing.T) {
	fixture := newSetupFixture(t)
	fixture.setup(t, setupRequest{})
	fixture.launchd.stopped[setupNodeAgentLabel] = true
	report := fixture.setup(t, setupRequest{})
	want := [][]string{{launchagent.Launchctl, "kickstart", "gui/501/" + setupNodeAgentLabel}}
	if got := fixture.launchd.mutating(); !slices.EqualFunc(got, want, slices.Equal[[]string]) {
		t.Fatalf("commands for a stopped service = %v, want %v", got, want)
	}
	if actions := serviceActions(report); actions[setupNodeAgentLabel] != launchagent.Started || actions[setupL1Label] != launchagent.Unchanged {
		t.Fatalf("actions = %v", actions)
	}
	if report := fixture.setup(t, setupRequest{}); len(fixture.launchd.mutating()) != 0 || serviceActions(report)[setupNodeAgentLabel] != launchagent.Unchanged {
		t.Fatalf("a running service was touched again: %v", fixture.launchd.mutating())
	}
}

// launchd SIGKILLs the node agent ExitTimeOut seconds after SIGTERM. The
// installed agent cancels resident attempts once its shutdown budget has
// passed, and ExitTimeOut must leave room for that orderly cancellation.
func TestSetupNodeAgentShutdownFitsLaunchdExitTimeout(t *testing.T) {
	fixture := newSetupFixture(t)
	fixture.setup(t, setupRequest{})
	plist := fixture.plist(t, setupNodeAgentLabel)
	if !strings.Contains(plist, "<string>--shutdown-budget="+setupNodeAgentShutdownBudget.String()+"</string>") ||
		!strings.Contains(plist, "<key>ExitTimeOut</key>\n  <integer>"+strconv.Itoa(setupNodeAgentExitTimeout)+"</integer>") {
		t.Fatalf("node agent plist lacks the shutdown budget or its ExitTimeOut:\n%s", plist)
	}
	// Every serial phase of an orderly cancellation, from the real constants:
	// termination grace, reap, the final log flush and the request in flight
	// when it ends, completion and the result upload. A phase that grows past
	// ExitTimeOut fails here.
	cancellation := processrunner.DefaultTerminationGraceTime + processrunner.DefaultProcessReapTimeout +
		agent.DefaultFinalizationTimeout + 3*agent.DefaultOperationTimeout
	if bound := agent.ForcedShutdownBound(agent.ShutdownTimeouts{}); bound < cancellation {
		t.Fatalf("the agent's forced-shutdown bound %s omits a phase of %s", bound, cancellation)
	}
	if time.Duration(setupNodeAgentExitTimeout)*time.Second < setupNodeAgentShutdownBudget+cancellation+time.Second {
		t.Fatalf("ExitTimeOut %ds does not cover the %s budget plus %s of orderly cancellation with slack",
			setupNodeAgentExitTimeout, setupNodeAgentShutdownBudget, cancellation)
	}
	for _, label := range []string{setupL1Label, setupL3Label} {
		if strings.Contains(fixture.plist(t, label), "shutdown-budget") {
			t.Fatalf("%s got the node agent's shutdown budget", label)
		}
	}
}

// A destination of the wrong type is refused before the first write: no
// file and no service changes.
func TestSetupRefusesAWrongDestinationTypeBeforeAnyChange(t *testing.T) {
	for name, prepare := range map[string]func(t *testing.T, home string){
		"skill folder is a file": func(t *testing.T, home string) {
			if err := os.MkdirAll(filepath.Join(home, ".agents", "skills"), 0o755); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(home, ".agents", "skills", "wefty"), []byte("not a folder"), 0o644); err != nil {
				t.Fatal(err)
			}
		},
		"binary is a folder": func(t *testing.T, home string) {
			if err := os.MkdirAll(filepath.Join(home, ".local", "bin", "wefty-l3"), 0o755); err != nil {
				t.Fatal(err)
			}
		},
	} {
		t.Run(name, func(t *testing.T) {
			fixture := newSetupFixture(t)
			prepare(t, fixture.home)
			before := homeSnapshot(t, fixture.home)
			if _, err := runSetup(t.Context(), fixture.host, setupRequest{}); err == nil || !strings.Contains(err.Error(), "move it aside") {
				t.Fatalf("setup = %v", err)
			}
			if after := homeSnapshot(t, fixture.home); !reflect.DeepEqual(before, after) || len(fixture.launchd.commands) != 0 {
				t.Fatalf("a refused setup changed the home or ran %v", fixture.launchd.commands)
			}
		})
	}
}

// Matching bytes are not enough: a binary that lost its executable bit, or a
// link standing in for it, is repaired.
func TestSetupRepairsTheInstalledFileTypeAndMode(t *testing.T) {
	fixture := newSetupFixture(t)
	fixture.setup(t, setupRequest{})
	binary := filepath.Join(fixture.home, ".local", "bin", "wefty-agent")
	if err := os.Chmod(binary, 0o644); err != nil {
		t.Fatal(err)
	}
	cli := filepath.Join(fixture.home, ".local", "bin", "wefty")
	elsewhere := filepath.Join(fixture.home, "copy-of-wefty")
	if err := os.Rename(cli, elsewhere); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(elsewhere, cli); err != nil {
		t.Fatal(err)
	}
	report := fixture.setup(t, setupRequest{})
	for _, path := range []string{binary, cli} {
		info, err := os.Lstat(path)
		if err != nil || !info.Mode().IsRegular() || info.Mode().Perm() != 0o755 {
			t.Fatalf("%s after a rerun: %v %v", path, info.Mode(), err)
		}
		if !slices.Contains(report.Changes, "installed "+path) {
			t.Fatalf("changes %v do not report %s", report.Changes, path)
		}
	}
	// The bytes did not change, so no running service needs a reload.
	if len(fixture.launchd.mutating()) != 0 {
		t.Fatalf("a mode repair reloaded services: %v", fixture.launchd.mutating())
	}
}

// A print that fails other than with launchd's "no such service" leaves the
// service's state unknown. With a changed binary, setup must neither publish
// the new definition under the old process nor call it unchanged later: it
// stops, and the next run reloads.
func TestSetupStopsWhenLaunchdCannotBeAsked(t *testing.T) {
	fixture := newSetupFixture(t)
	fixture.setup(t, setupRequest{})
	before := fixture.plist(t, setupL3Label)
	fixture.writeBinary(t, "wefty-l3", "v2")
	failingPrints := 0
	run := fixture.host.launchctl
	fixture.host.launchctl = func(ctx context.Context, name string, args ...string) ([]byte, error) {
		if len(args) > 1 && args[0] == "print" && strings.HasSuffix(args[1], setupL3Label) && failingPrints == 0 {
			failingPrints++
			fixture.launchd.commands = append(fixture.launchd.commands, append([]string{name}, args...))
			return nil, errors.New("launchctl print: Operation not permitted")
		}
		return run(ctx, name, args...)
	}
	fixture.launchd.commands = nil
	if _, err := runSetup(t.Context(), fixture.host, setupRequest{}); err == nil || !strings.Contains(err.Error(), "cannot tell whether LaunchAgent "+setupL3Label) {
		t.Fatalf("setup with an unanswerable print = %v", err)
	}
	if got := fixture.launchd.mutating(); len(got) != 0 {
		t.Fatalf("setup acted on an unknown service state: %v", got)
	}
	if after := fixture.plist(t, setupL3Label); after != before {
		t.Fatal("setup published a new plist while the old service's state was unknown")
	}
	report := fixture.setup(t, setupRequest{})
	if serviceActions(report)[setupL3Label] != launchagent.Reloaded {
		t.Fatalf("the next run did not reload: %v %v", serviceActions(report), fixture.launchd.mutating())
	}
}
