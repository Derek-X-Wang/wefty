package launchagent

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/internal/durable"
)

type testPlist struct {
	agent   Agent
	folder  *os.Root
	path    string
	payload []byte
}

func newTestPlist(t *testing.T) testPlist {
	t.Helper()
	home := t.TempDir()
	agent := Agent{
		Label: "dev.wefty.test", Program: "/usr/local/bin/wefty-test", Arguments: []string{"--profile=/p.json"},
		Environment: map[string]string{"PATH": "/usr/bin:/bin", "HOME": home}, WorkingDirectory: home,
		LogPath: filepath.Join(home, "test.log"), InputDigest: "sha256:abc",
	}
	payload, err := Render(agent)
	if err != nil {
		t.Fatal(err)
	}
	path := Path(home, agent.Label)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	folder, err := os.OpenRoot(filepath.Dir(path))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { folder.Close() })
	return testPlist{agent: agent, folder: folder, path: path, payload: payload}
}

func (plist testPlist) converge(t *testing.T, manager Manager) (Action, error) {
	t.Helper()
	return manager.Converge(t.Context(), plist.folder, plist.path, plist.agent.Label, plist.payload)
}

// assertOnly checks the LaunchAgents folder holds exactly want at the plist,
// and no staged leftover.
func (plist testPlist) assertOnly(t *testing.T, want string) {
	t.Helper()
	if current, err := os.ReadFile(plist.path); err != nil || string(current) != want {
		t.Fatalf("plist = %q (%v), want %q", current, err, want)
	}
	entries, err := os.ReadDir(filepath.Dir(plist.path))
	if err != nil || len(entries) != 1 {
		t.Fatalf("LaunchAgents folder = %v (%v), want only the plist", entries, err)
	}
}

// fakeLaunchd is one service in launchd's owner domain. print reports it,
// or launchd's confirmed "no such service".
type fakeLaunchd struct {
	loaded, running bool
	// printErr, when set, is how print fails while it is set.
	printErr    error
	bootoutErr  error
	verbs       []string
	staysLoaded bool
}

func (launchd *fakeLaunchd) run(_ context.Context, _ string, args ...string) ([]byte, error) {
	launchd.verbs = append(launchd.verbs, args[0])
	switch args[0] {
	case "print":
		if launchd.printErr != nil {
			return nil, launchd.printErr
		}
		if !launchd.loaded {
			return nil, fmt.Errorf("%w: Could not find service", ErrServiceNotFound)
		}
		if !launchd.running {
			return []byte("gui/501/dev.wefty.test = {\n\tstate = not running\n}\n"), nil
		}
		return []byte("gui/501/dev.wefty.test = {\n\tstate = running\n\tpid = 42\n}\n"), nil
	case "bootout":
		if launchd.bootoutErr != nil {
			return nil, launchd.bootoutErr
		}
		launchd.loaded = launchd.staysLoaded
	case "bootstrap":
		launchd.loaded, launchd.running = true, true
	case "kickstart":
		launchd.running = true
	}
	return nil, nil
}

func (launchd *fakeLaunchd) manager() Manager {
	return Manager{UID: 501, Run: launchd.run, PollInterval: time.Millisecond, UnloadBound: 50 * time.Millisecond}
}

// bootout can return while launchd still lists the service; bootstrapping
// then would fail, so Converge waits until launchd confirms it has forgotten it.
func TestConvergeWaitsForUnloadBeforeBootstrap(t *testing.T) {
	plist := newTestPlist(t)
	if err := os.WriteFile(plist.path, []byte("old"), 0o644); err != nil {
		t.Fatal(err)
	}
	launchd := &fakeLaunchd{loaded: true, running: true}
	printsAfterBootout := 0
	manager := launchd.manager()
	manager.UnloadBound = 5 * time.Second
	manager.Run = func(ctx context.Context, name string, args ...string) ([]byte, error) {
		if args[0] == "print" && launchd.verbs != nil && strings.Contains(strings.Join(launchd.verbs, ","), "bootout") {
			if printsAfterBootout++; printsAfterBootout < 3 {
				launchd.verbs = append(launchd.verbs, "print")
				return []byte("state = running\n"), nil
			}
		}
		return launchd.run(ctx, name, args...)
	}
	if action, err := plist.converge(t, manager); err != nil || action != Reloaded {
		t.Fatalf("converge = %s, %v", action, err)
	}
	if got := strings.Join(launchd.verbs, ","); got != "print,bootout,print,print,print,enable,bootstrap" {
		t.Fatalf("launchctl verbs = %s", got)
	}
	plist.assertOnly(t, string(plist.payload))
}

func TestConvergeFailsWhenUnloadExceedsItsBound(t *testing.T) {
	plist := newTestPlist(t)
	if err := os.WriteFile(plist.path, []byte("old"), 0o644); err != nil {
		t.Fatal(err)
	}
	launchd := &fakeLaunchd{loaded: true, running: true, staysLoaded: true}
	if _, err := plist.converge(t, launchd.manager()); err == nil || !strings.Contains(err.Error(), "still loaded") || strings.Contains(strings.Join(launchd.verbs, ","), "bootstrap") {
		t.Fatalf("converge with a stuck unload = %v, verbs %v", err, launchd.verbs)
	}
	plist.assertOnly(t, "old")
}

// A failed bootout leaves the old plist, so the next run still sees the
// difference and reloads, rather than reporting an old process as current.
func TestConvergePublishesOnlyAfterUnloading(t *testing.T) {
	plist := newTestPlist(t)
	if err := os.WriteFile(plist.path, []byte("old"), 0o644); err != nil {
		t.Fatal(err)
	}
	launchd := &fakeLaunchd{loaded: true, running: true, bootoutErr: errors.New("Boot-out failed: 5: Input/output error")}
	if _, err := plist.converge(t, launchd.manager()); err == nil {
		t.Fatal("a failed bootout was reported as success")
	}
	plist.assertOnly(t, "old")
	launchd.bootoutErr, launchd.verbs = nil, nil
	if action, err := plist.converge(t, launchd.manager()); err != nil || action != Reloaded ||
		strings.Join(launchd.verbs, ",") != "print,bootout,print,enable,bootstrap" {
		t.Fatalf("retry = %s %v, verbs %v", action, err, launchd.verbs)
	}
	plist.assertOnly(t, string(plist.payload))
}

// Only launchd's confirmed "no such service" means not loaded. A print that
// fails any other way leaves the state unknown, and nothing is changed: a
// guess could publish a new definition under a process still running the
// old one, and a later run would call that process current.
func TestConvergeNeedsAConfirmedAnswerFromLaunchd(t *testing.T) {
	plist := newTestPlist(t)
	if err := os.WriteFile(plist.path, []byte("old"), 0o644); err != nil {
		t.Fatal(err)
	}
	launchd := &fakeLaunchd{loaded: true, running: true, printErr: errors.New("launchctl: Operation not permitted while System Integrity Protection is engaged")}
	if _, err := plist.converge(t, launchd.manager()); err == nil || !strings.Contains(err.Error(), "cannot tell whether") {
		t.Fatalf("converge with a failing print = %v", err)
	}
	if strings.Join(launchd.verbs, ",") != "print" {
		t.Fatalf("launchctl verbs after an unknown state = %v", launchd.verbs)
	}
	plist.assertOnly(t, "old")

	// Waiting for an unload needs the same confirmation.
	launchd.printErr, launchd.verbs = nil, nil
	manager := launchd.manager()
	manager.Run = func(ctx context.Context, name string, args ...string) ([]byte, error) {
		if args[0] == "print" && strings.Contains(strings.Join(launchd.verbs, ","), "bootout") {
			launchd.verbs = append(launchd.verbs, "print")
			return nil, errors.New("launchctl: connection interrupted")
		}
		return launchd.run(ctx, name, args...)
	}
	if _, err := plist.converge(t, manager); err == nil || !strings.Contains(err.Error(), "not confirmed unloaded") || strings.Contains(strings.Join(launchd.verbs, ","), "bootstrap") {
		t.Fatalf("converge whose unload cannot be confirmed = %v, verbs %v", err, launchd.verbs)
	}
	plist.assertOnly(t, "old")
}

// If publishing fails after the unload -- before or after the rename -- the
// previous definition is put back and loaded, and the next run reloads.
func TestConvergeRestoresThePreviousDefinitionWhenPublishFails(t *testing.T) {
	for _, renamed := range []bool{false, true} {
		t.Run(fmt.Sprintf("renamed=%t", renamed), func(t *testing.T) {
			plist := newTestPlist(t)
			if err := os.WriteFile(plist.path, []byte("old"), 0o644); err != nil {
				t.Fatal(err)
			}
			launchd := &fakeLaunchd{loaded: true, running: true}
			manager := launchd.manager()
			manager.publish = func(dir *os.Root, staging, name string) error {
				if renamed {
					if err := dir.Rename(staging, name); err != nil {
						return err
					}
					return errors.New("sync LaunchAgents folder: input/output error")
				}
				return errors.New("rename: input/output error")
			}
			_, err := plist.converge(t, manager)
			if err == nil || !strings.Contains(err.Error(), "previous definition was restored") {
				t.Fatalf("converge with a failing publish = %v", err)
			}
			if got := strings.Join(launchd.verbs, ","); got != "print,bootout,print,enable,bootstrap" || !launchd.loaded {
				t.Fatalf("the previous definition was not loaded again: verbs %s loaded=%t", got, launchd.loaded)
			}
			plist.assertOnly(t, "old")
			launchd.verbs = nil
			if action, err := plist.converge(t, launchd.manager()); err != nil || action != Reloaded {
				t.Fatalf("retry = %s %v", action, err)
			}
			plist.assertOnly(t, string(plist.payload))
		})
	}
}

// The new definition is prepared and synced before anything is unloaded, so
// a failure to stage it leaves the running service alone.
func TestConvergeStagesBeforeUnloading(t *testing.T) {
	plist := newTestPlist(t)
	if err := os.WriteFile(plist.path, []byte("old"), 0o644); err != nil {
		t.Fatal(err)
	}
	launchd := &fakeLaunchd{loaded: true, running: true}
	manager := launchd.manager()
	manager.stage = func(*os.Root, string, []byte, os.FileMode) error { return errors.New("no space left on device") }
	if _, err := plist.converge(t, manager); err == nil || !strings.Contains(err.Error(), "stage LaunchAgent") {
		t.Fatalf("converge with a failing stage = %v", err)
	}
	if strings.Join(launchd.verbs, ",") != "print" || !launchd.loaded {
		t.Fatalf("a failed stage touched the service: %v", launchd.verbs)
	}
	plist.assertOnly(t, "old")
	var stagedAtBootout bool
	manager = launchd.manager()
	manager.Run = func(ctx context.Context, name string, args ...string) ([]byte, error) {
		if args[0] == "bootout" {
			_, err := os.Stat(filepath.Join(filepath.Dir(plist.path), stagingName(filepath.Base(plist.path))))
			stagedAtBootout = err == nil
		}
		return launchd.run(ctx, name, args...)
	}
	if _, err := plist.converge(t, manager); err != nil || !stagedAtBootout {
		t.Fatalf("converge = %v, staged before bootout = %t", err, stagedAtBootout)
	}
	plist.assertOnly(t, string(plist.payload))
	if !durable.Holds(plist.folder, filepath.Base(plist.path), plist.payload, 0o644) {
		t.Fatal("published plist does not hold the payload with mode 0644")
	}
}

// KeepAlive restarts a service only after a crash, so a clean exit leaves it
// loaded but stopped. Loaded is not running.
func TestConvergeStartsALoadedButStoppedService(t *testing.T) {
	plist := newTestPlist(t)
	if err := os.WriteFile(plist.path, plist.payload, 0o644); err != nil {
		t.Fatal(err)
	}
	launchd := &fakeLaunchd{loaded: true}
	if action, err := plist.converge(t, launchd.manager()); err != nil || action != Started || strings.Join(launchd.verbs, ",") != "print,kickstart" {
		t.Fatalf("converge on a stopped service = %s %v, %v", action, err, launchd.verbs)
	}
	launchd.verbs = nil
	if action, err := plist.converge(t, launchd.manager()); err != nil || action != Unchanged || len(launchd.verbs) != 1 {
		t.Fatalf("converge on a running service = %s %v, %v", action, err, launchd.verbs)
	}
}

func TestRenderRefusesValuesThatWouldBreakThePlist(t *testing.T) {
	for name, edit := range map[string]func(*Agent){
		"relative program": func(agent *Agent) { agent.Program = "wefty" },
		"newline argument": func(agent *Agent) { agent.Arguments = []string{"a\nb"} },
		"comment digest":   func(agent *Agent) { agent.InputDigest = "sha256:--" },
		"label with slash": func(agent *Agent) { agent.Label = "dev/wefty" },
	} {
		agent := newTestPlist(t).agent
		edit(&agent)
		if _, err := Render(agent); err == nil {
			t.Errorf("%s was rendered", name)
		}
	}
}

// launchctl's "could not find service" exit is the only failure that means
// not loaded; a refusal or a broken connection is not.
func TestOnlyLaunchctlsNotFoundExitMeansNotLoaded(t *testing.T) {
	exit := func(code int) error {
		return exec.Command("/bin/sh", "-c", fmt.Sprintf("exit %d", code)).Run()
	}
	if err := classifyLaunchctlFailure(exit(113), "Bad request."); !errors.Is(err, ErrServiceNotFound) {
		t.Fatalf("exit 113 = %v", err)
	}
	if err := classifyLaunchctlFailure(exit(1), `Could not find service "dev.wefty.l1" in domain for user gui: 501`); !errors.Is(err, ErrServiceNotFound) {
		t.Fatalf("the not-found message = %v", err)
	}
	for _, err := range []error{
		classifyLaunchctlFailure(exit(1), "Operation not permitted"),
		classifyLaunchctlFailure(exit(5), "Input/output error"),
		classifyLaunchctlFailure(context.DeadlineExceeded, ""),
	} {
		if errors.Is(err, ErrServiceNotFound) {
			t.Fatalf("%v was read as not loaded", err)
		}
	}
}

// A loaded service is replaced only while its previous definition can be put
// back. With the plist missing or unreadable, Converge refuses before bootout,
// so the service keeps running.
func TestConvergeKeepsALoadedServiceWhoseDefinitionCannotBeRead(t *testing.T) {
	for _, damage := range []string{"missing", "unreadable"} {
		t.Run(damage, func(t *testing.T) {
			plist := newTestPlist(t)
			switch damage {
			case "unreadable":
				if os.Geteuid() == 0 {
					t.Skip("root reads a file whatever its mode")
				}
				if err := os.WriteFile(plist.path, []byte("old"), 0o000); err != nil {
					t.Fatal(err)
				}
			}
			launchd := &fakeLaunchd{loaded: true, running: true}
			if _, err := plist.converge(t, launchd.manager()); err == nil || !strings.Contains(err.Error(), "cannot be read") {
				t.Fatalf("converge over an unreadable definition = %v", err)
			}
			if got := strings.Join(launchd.verbs, ","); got != "print" || !launchd.loaded {
				t.Fatalf("launchctl verbs = %s, loaded=%t; the running service must be left alone", got, launchd.loaded)
			}
			if entries, _ := os.ReadDir(filepath.Dir(plist.path)); len(entries) > 1 {
				t.Fatalf("a refused converge left %v", entries)
			}
		})
	}
}
