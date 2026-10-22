package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/internal/connprofile"
	"github.com/Derek-X-Wang/wefty/internal/serviceplan"
)

func TestSetupLinuxFleetAccepted(t *testing.T) {
	f, _, _ := newFleetFixture(t)
	f.host.goos = "linux"
	f.host.euid = os.Geteuid()
	f.host.linuxFiles, _ = linuxTestFiles(t)
	f.host.systemctl = func(_ context.Context, _ string, args ...string) ([]byte, error) {
		if len(args) == 1 && args[0] == "--version" {
			return []byte("systemd 252\n+PAM\n"), nil
		}
		return []byte("FragmentPath=\nDropInPaths=\nLoadState=not-found\nActiveState=inactive\nSubState=dead\nMainPID=0\nUnitFileState=\nNeedDaemonReload=no\n"), nil
	}
	// Interrupt after the durable stage: reaching it proves Linux uses fleet setup.
	stop := "stage-record"
	_, err := runSetup(t.Context(), f.host, setupRequest{role: "control-plane", nodeID: "studio-0123abcd", afterFleetStep: func(step string) error {
		if step == stop {
			return errLinuxStageReached
		}
		return nil
	}})
	if err != errLinuxStageReached {
		t.Fatalf("Linux fleet setup did not reach durable stage: %v", err)
	}
}

var errLinuxStageReached = errors.New("injected stage interruption")

func TestSetupLinuxUpgradeRefusal(t *testing.T) {
	f := newSetupFixture(t)
	f.host.goos = "linux"
	_, err := runUpgrade(t.Context(), f.host, false, nil)
	if err == nil || !strings.Contains(err.Error(), "plain upgrade is Mac-only; on Linux use wefty setup --role control-plane or --role join --join-code CODE") {
		t.Fatalf("Linux upgrade advice: %v", err)
	}
}

type fakeFleetSystemd struct {
	running, known, enabled map[string]bool
	calls                   [][]string
	failStart               string
	beforeStart             func(string) error
	unitFolder              string
}

func (s *fakeFleetSystemd) run(_ context.Context, name string, args ...string) ([]byte, error) {
	if len(args) == 1 && args[0] == "--version" {
		s.calls = append(s.calls, []string{name, "--version", ""})
		return []byte("systemd 252 (test)\n+PAM\n"), nil
	}
	s.calls = append(s.calls, append([]string{name}, args...))
	if name != "/usr/bin/systemctl" || len(args) < 2 || args[0] != "--user" {
		return nil, fmt.Errorf("unexpected systemd command: %s %v", name, args)
	}
	unit := ""
	if len(args) > 2 {
		unit = args[2]
	}
	switch args[1] {
	case "show":
		load, active, sub, pid, enabled := "not-found", "inactive", "dead", 0, ""
		if s.known[unit] {
			load, enabled = "loaded", "disabled"
		}
		if s.enabled[unit] {
			enabled = "enabled"
		}
		if s.running[unit] {
			active, sub, pid = "active", "running", 4242
		}
		fragment := ""
		if s.known[unit] {
			fragment = filepath.Join(s.unitFolder, unit)
		}
		output := fmt.Sprintf("LoadState=%s\nActiveState=%s\nSubState=%s\nMainPID=%d\nUnitFileState=%s\nNeedDaemonReload=no\n", load, active, sub, pid, enabled)
		if strings.Contains(strings.Join(args, " "), "FragmentPath") {
			output += "FragmentPath=" + fragment + "\nDropInPaths=\n"
		}
		return []byte(output), nil

	case "reset-failed":
	case "daemon-reload":
	case "enable":
		s.known[unit], s.enabled[unit] = true, true
	case "start":
		if s.failStart == unit {
			s.failStart = ""
			return nil, errors.New("injected service start failure")
		}
		if s.beforeStart != nil {
			if err := s.beforeStart(unit); err != nil {
				return nil, err
			}
		}
		s.running[unit] = true
	case "stop":
		s.running[unit] = false
	case "disable":
		s.enabled[unit] = false
	default:
		return nil, fmt.Errorf("unexpected systemctl verb %s", args[1])
	}
	return nil, nil
}
func (s *fakeFleetSystemd) mutations() [][]string {
	var calls [][]string
	for _, c := range s.calls {
		if c[1] != "--version" && c[2] != "show" {
			calls = append(calls, c)
		}
	}
	return calls
}
func newLinuxFleetFixture(t *testing.T, role string) (*setupFixture, *fakeSetupEnroller, *bytes.Buffer, *fakeFleetSystemd) {
	t.Helper()
	var f *setupFixture
	var e *fakeSetupEnroller
	var out *bytes.Buffer
	if role == "join" {
		f, e, out = newJoinFixture(t)
	} else {
		f, e, out = newFleetFixture(t)
	}
	f.host.goos = "linux"
	f.host.euid = os.Geteuid()
	f.host.linuxFiles, _ = linuxTestFiles(t)
	f.host.readinessBudget = 10 * time.Millisecond
	f.host.launchctl = func(context.Context, string, ...string) ([]byte, error) {
		t.Error("Linux used launchctl")
		return nil, errors.New("unexpected launchctl")
	}
	f.host.doctorCommand = func(_ context.Context, name string, args ...string) ([]byte, error) {
		if name != "/usr/bin/loginctl" || !slices.Equal(args, []string{"show-user", "owner", "-p", "Linger"}) {
			t.Fatalf("unexpected host probe %s %v", name, args)
		}
		return []byte("Linger=yes\n"), nil
	}
	s := &fakeFleetSystemd{known: map[string]bool{}, running: map[string]bool{}, enabled: map[string]bool{}}
	s.unitFolder = filepath.Join(f.home, ".config", "systemd", "user")
	s.beforeStart = func(unit string) error {
		paths, _ := resolvePlatformMaintenancePaths(f.home, "linux", f.host.xdgStateHome)
		payload, err := os.ReadFile(filepath.Join(paths.config, setupManifestName))
		if err != nil {
			return fmt.Errorf("started before receipt: %w", err)
		}
		var receipt setupManifest
		if json.Unmarshal(payload, &receipt) != nil || string(receipt.Role) != role {
			return errors.New("started before complete role receipt")
		}
		profile, err := connprofile.Read(paths.profile)
		if err != nil {
			return err
		}
		if err := serviceplan.ValidateProfile(serviceplan.Role(role), profile); err != nil {
			return err
		}
		service := strings.TrimSuffix(strings.TrimPrefix(unit, "dev.wefty."), ".service")
		body := []byte(`{"ready_at":"2026-10-20T12:00:00Z","address":"opaque-listener","pid":4242,"node_id":"studio-0123abcd","boot_session_id":"boot-fixture"}`)
		if service == "client-service" {
			body = []byte(`{"ready_at":"2026-10-20T12:00:00Z","address":"fixture-socket","pid":4242,"node_id":"opaque-client"}`)
		}
		return os.WriteFile(filepath.Join(paths.data, "setup-ready", service+".json"), body, 0600)
	}
	f.host.systemctl = s.run
	return f, e, out, s
}
func linuxSetupRequest(t *testing.T, role string) setupRequest {
	r := setupRequest{role: role, nodeID: "studio-0123abcd"}
	if role == "join" {
		r.joinCode = joinCodeForTest(t, "opaque-l1", "opaque-l3", "")
	}
	return r
}
func TestSetupLinuxFleetPublishesOrderedUnitsAndResumesWithoutKeys(t *testing.T) {
	for _, role := range []string{"control-plane", "join"} {
		t.Run(role, func(t *testing.T) {
			f, e, prompts, s := newLinuxFleetFixture(t, role)
			if role == "join" {
				for _, name := range []string{"wefty-l1", "wefty-l3"} {
					if err := os.Remove(filepath.Join(f.source, name)); err != nil {
						t.Fatal(err)
					}
				}
			}
			report, err := runSetup(t.Context(), f.host, linuxSetupRequest(t, role))
			if err != nil {
				t.Fatal(err)
			}
			want := []string{"node-agent", "client"}
			services := []string{"node-agent", "client-service"}
			if role == "control-plane" {
				want = append([]string{"l1", "l3"}, want...)
				services = append([]string{"l1", "l3"}, services...)
			}
			if !slices.Equal(e.roles, want) {
				t.Fatalf("enrolment order: %v", e.roles)
			}
			var started []string
			for _, c := range s.calls {
				if c[2] == "start" {
					started = append(started, c[3])
				}
			}
			for i, service := range services {
				unit := "dev.wefty." + service + ".service"
				if started[i] != unit || report.Services[i].Label != unit || report.Services[i].Plist != "" {
					t.Fatal("wrong service order or report", report.Services, started)
				}
				b, err := os.ReadFile(report.Services[i].Unit)
				if err != nil {
					t.Fatal(err)
				}
				if !strings.HasPrefix(report.Services[i].Unit, filepath.Join(f.home, ".config", "systemd", "user")+"/") {
					t.Fatal("wrong unit path")
				}
				if i > 0 && !strings.Contains(string(b), "After="+started[i-1]+"\n") {
					t.Fatalf("missing dependency order: %s", b)
				}
				if service == "node-agent" && !strings.Contains(string(b), "WEFTY_LAUNCH_UNIT="+unit) {
					t.Fatal("wrong node unit identity")
				}
			}
			for _, check := range report.Readiness {
				if check.Status != "VERIFIED" {
					t.Fatalf("readiness: %+v", report.Readiness)
				}
			}
			if report.DataDir != filepath.Join(f.home, ".local", "share", "wefty") || report.LogDir != filepath.Join(f.home, ".local", "state", "wefty") {
				t.Fatal("wrong Linux paths")
			}
			if role == "join" {
				for _, name := range []string{"wefty-l1", "wefty-l3"} {
					if _, err := os.Stat(filepath.Join(report.BinDir, name)); !os.IsNotExist(err) {
						t.Fatal("joining Linux install included control-plane binary", name, err)
					}
				}
			}
			before := snapshotLinuxHome(t, f.home)
			s.calls = nil
			prompts.Reset()
			f.host.enrolmentInput = strings.NewReader("")
			again, err := runSetup(t.Context(), f.host, setupRequest{})
			if err != nil {
				t.Fatal(err)
			}
			if len(s.mutations()) > 0 || prompts.Len() > 0 || len(e.roles) != len(want) || len(again.Changes) > 0 || !reflect.DeepEqual(before, snapshotLinuxHome(t, f.home)) {
				t.Fatal("rerun mutated installation or consumed keys", s.calls)
			}
			assertJoinArtifactsPrivate(t, f.home, prompts.String(), report, "secret-l1", "secret-l3", "secret-node", "secret-client", "https://login.invalid/one-use")
		})
	}
}
func snapshotLinuxHome(t *testing.T, home string) map[string]string {
	t.Helper()
	files := map[string]string{}
	if err := filepath.WalkDir(home, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		b, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		files[path] = payloadDigest(b)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return files
}
func TestSetupLinuxFleetResumesEveryDurableStep(t *testing.T) {
	for _, role := range []string{"control-plane", "join"} {
		steps := []string{"stage-record", "stage-profile", "enrolled:node-agent", "enrolled:client", "profile", "receipt", "committed"}
		if role == "control-plane" {
			steps = append(steps, "enrolled:l1", "enrolled:l3")
		} else {
			steps = append(steps, "peer-verification")
		}
		for _, step := range steps {
			t.Run(role+"/"+step, func(t *testing.T) {
				f, e, prompts, s := newLinuxFleetFixture(t, role)
				r := linuxSetupRequest(t, role)
				r.afterFleetStep = func(got string) error {
					if got == step {
						return errors.New("injected durable interruption")
					}
					return nil
				}
				_, err := runSetup(t.Context(), f.host, r)
				if err == nil || !strings.Contains(err.Error(), "injected") {
					t.Fatal("missing interruption", err)
				}
				if len(s.mutations()) > 0 {
					t.Fatal("service mutation before publication", s.calls)
				}
				if step == "profile" {
					p, err := connprofile.Read(filepath.Join(f.home, ".config", "wefty", "profile.json"))
					if err != nil || serviceplan.ValidateProfile(serviceplan.Role(role), p) != nil {
						t.Fatal("partial profile published", err)
					}
					if _, err := os.Stat(filepath.Join(f.home, ".config", "wefty", setupManifestName)); !os.IsNotExist(err) {
						t.Fatal("receipt published before profile callback")
					}
				}
				all := []string{"secret-node", "secret-client"}
				if role == "control-plane" {
					all = append([]string{"secret-l1", "secret-l3"}, all...)
				}
				f.host.enrolmentInput = strings.NewReader(strings.Join(all[len(e.roles):], "\n") + "\n")
				r.afterFleetStep = nil
				report, err := runSetup(t.Context(), f.host, r)
				if err != nil {
					t.Fatal(err)
				}
				if len(e.roles) != len(all) {
					t.Fatalf("re-enrolled: %v", e.roles)
				}
				assertJoinArtifactsPrivate(t, f.home, prompts.String(), report, all...)
			})
		}
	}
}
func TestSetupLinuxFleetResumesEachActivationFailure(t *testing.T) {
	for _, role := range []string{"control-plane", "join"} {
		services := []string{"node-agent", "client-service"}
		if role == "control-plane" {
			services = append([]string{"l1", "l3"}, services...)
		}
		for _, service := range services {
			t.Run(role+"/"+service, func(t *testing.T) {
				f, e, _, s := newLinuxFleetFixture(t, role)
				s.failStart = "dev.wefty." + service + ".service"
				report, err := runSetup(t.Context(), f.host, linuxSetupRequest(t, role))
				if err == nil || len(report.Readiness) == 0 {
					t.Fatal("activation failure did not retain readiness", err)
				}
				keys := len(e.roles)
				f.host.enrolmentInput = strings.NewReader("")
				report, err = runSetup(t.Context(), f.host, setupRequest{})
				if err != nil {
					t.Fatal(err)
				}
				if len(e.roles) != keys {
					t.Fatal("consumed keys after publication")
				}
				for _, check := range report.Readiness {
					if check.Status != "VERIFIED" {
						t.Fatal(report.Readiness)
					}
				}
			})
		}
	}
}

func TestSetupLinuxRefusalsLeaveHomeUntouched(t *testing.T) {
	for _, r := range []setupRequest{{}, {role: "join", convertPlain: true}, {role: "join", convertPlain: true, abandonConversion: true}} {
		t.Run(fmt.Sprint(r.convertPlain, r.abandonConversion), func(t *testing.T) {
			f, _, _, s := newLinuxFleetFixture(t, "join")
			before := snapshotLinuxHome(t, f.home)
			_, err := runSetup(t.Context(), f.host, r)
			if err == nil || !strings.Contains(err.Error(), "Mac-only") {
				t.Fatal("missing Linux refusal", err)
			}
			if !reflect.DeepEqual(before, snapshotLinuxHome(t, f.home)) || len(s.calls) > 0 {
				t.Fatal("refusal mutated or inspected services")
			}
		})
	}
	for _, stage := range []bool{false, true} {
		t.Run(fmt.Sprint("removal/", stage), func(t *testing.T) {
			f, _, _, s := newLinuxFleetFixture(t, "join")
			r := linuxSetupRequest(t, "join")
			if stage {
				r.afterFleetStep = func(step string) error {
					if step == "stage-record" {
						return errLinuxStageReached
					}
					return nil
				}
			}
			_, err := runSetup(t.Context(), f.host, r)
			if !stage && err != nil {
				t.Fatal(err)
			}
			before := snapshotLinuxHome(t, f.home)
			s.calls = nil
			_, err = runSetupRemove(t.Context(), f.host, false)
			if err == nil || !strings.Contains(err.Error(), "removal") {
				t.Fatal("Linux fleet removal accepted", err)
			}
			if !reflect.DeepEqual(before, snapshotLinuxHome(t, f.home)) || len(s.calls) > 0 {
				t.Fatal("removal changed fleet")
			}
		})
	}
}
func TestSetupLinuxPersonWarnsAndUsesBrowserWithoutKey(t *testing.T) {
	f, e, prompts, _ := newLinuxFleetFixture(t, "join")
	var urls []string
	f.host.ownerLogin = func(_ context.Context, url string) error {
		if !strings.Contains(prompts.String(), "--with-person is meant for the owner's laptop") {
			t.Error("warning was not printed before browser opened")
		}
		urls = append(urls, url)
		return nil
	}
	r := linuxSetupRequest(t, "join")
	r.withPerson = true
	report, err := runSetup(t.Context(), f.host, r)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(e.roles, []string{"node-agent", "client", "person"}) || len(urls) != 1 || strings.Count(prompts.String(), "Single-use join key") != 2 {
		t.Fatal("person flow changed", e.roles)
	}
	if !strings.Contains(strings.Join(report.Notes, " "), "--with-person is meant for the owner's laptop") {
		t.Fatal("missing Linux person warning")
	}
	assertJoinArtifactsPrivate(t, f.home, prompts.String(), report, append([]string{"secret-node", "secret-client"}, urls...)...)
}
func TestSetupLinuxPathsAndPrivateSocket(t *testing.T) {
	f, _, _, _ := newLinuxFleetFixture(t, "join")
	t.Setenv("XDG_CONFIG_HOME", filepath.Join(f.home, "ignored-config"))
	t.Setenv("XDG_DATA_HOME", filepath.Join(f.home, "ignored-data"))
	t.Setenv("XDG_RUNTIME_DIR", filepath.Join(f.home, "ignored-runtime"))
	f.host.xdgStateHome = filepath.Join(f.home, "state elsewhere")
	report, err := runSetup(t.Context(), f.host, linuxSetupRequest(t, "join"))
	if err != nil {
		t.Fatal(err)
	}
	if report.Profile != filepath.Join(f.home, ".config", "wefty", "profile.json") || report.LogDir != filepath.Join(f.host.xdgStateHome, "wefty") {
		t.Fatal("XDG paths", report)
	}
	paths, issues := resolvePlatformMaintenancePaths(f.home, "linux", f.host.xdgStateHome)
	if len(issues) > 0 {
		t.Fatal(issues)
	}
	plan, err := setupRolePlan(paths, serviceplan.Join)
	if err != nil {
		t.Fatal(err)
	}
	socket := filepath.Join(f.home, ".config", "wefty", "client", "service.sock")
	if !slices.Contains(plan.Services[1].Arguments, "--socket="+socket) || len(socket) > 100 {
		t.Fatal("socket policy changed")
	}
	// Shared-folder and long socket paths are refused by the shared plan tests.
	for _, state := range []string{"relative", filepath.Join(f.home, "..", "outside"), strings.Repeat("x", 200)} {
		_, issues := resolvePlatformMaintenancePaths(f.home, "linux", state)
		if len(issues) == 0 {
			t.Fatal("invalid state root accepted", state)
		}
	}
}
func TestSetupLinuxParsesBootTimeFailClosed(t *testing.T) {
	for _, payload := range []string{"cpu 0\nbtime 123456\n", "btime 0\n", "btime unknown\n", "btime 1\nbtime 2\n", "cpu 0\n"} {
		boot, err := parseLinuxSetupBootTime(payload)
		if payload == "cpu 0\nbtime 123456\n" {
			if err != nil || boot.Unix() != 123456 {
				t.Fatal(boot, err)
			}
		} else if err == nil {
			t.Fatal("invalid boot time accepted", payload)
		}
	}
}

func TestSetupLinuxDigestChangeReloadsOnlyOwningUnit(t *testing.T) {
	for _, role := range []string{"control-plane", "join"} {
		t.Run(role, func(t *testing.T) {
			f, _, _, _ := newLinuxFleetFixture(t, role)
			if _, err := runSetup(t.Context(), f.host, linuxSetupRequest(t, role)); err != nil {
				t.Fatal(err)
			}
			paths, _ := resolvePlatformMaintenancePaths(f.home, "linux", "")
			p := f.profile(t)
			manager := fleetServiceManager{host: f.host}
			render := func() map[string]string {
				services, err := setupRoleServices(paths, p, map[string]string{"wefty": "cli-digest", "wefty-agent": "agent-digest", "wefty-l1": "l1-digest", "wefty-l3": "l3-digest"}, f.loginPATH, serviceplan.Role(role))
				if err != nil {
					t.Fatal(err)
				}
				result := map[string]string{}
				previous := ""
				for _, service := range services {
					b, err := manager.render(service, previous)
					if err != nil {
						t.Fatal(err)
					}
					result[service.Label] = string(b)
					previous = service.Label
				}
				return result
			}
			before := render()
			part := p.Participants["client"]
			part.NetworkName = "wefty-new-readable-name"
			p.Participants["client"] = part
			after := render()
			for label, b := range before {
				changed := after[label] != b
				if changed != (label == "dev.wefty.client-service") {
					t.Fatalf("unexpected input digest effect: %s changed=%v", label, changed)
				}
			}
		})
	}
}
