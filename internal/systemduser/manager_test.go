package systemduser

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"
)

type fakeManager struct {
	loaded, running, enabled, dirty bool
	pid                             int
	calls                           []string
	inspectErr                      error
	output                          string
	refuseStop                      bool
	fragment                        string
}

func (f *fakeManager) run(_ context.Context, name string, args ...string) ([]byte, error) {
	if name != Systemctl || len(args) < 2 || args[0] != "--user" {
		return nil, errors.New("not systemctl --user")
	}
	f.calls = append(f.calls, strings.Join(args, " "))
	switch args[1] {
	case "show":
		if f.inspectErr != nil {
			return nil, f.inspectErr
		}
		if f.output != "" {
			return []byte(f.output), nil
		}
		load, active, sub, enabled, pid := "not-found", "inactive", "dead", "", 0
		if f.loaded {
			load = "loaded"
			enabled = "disabled"
			if f.enabled {
				enabled = "enabled"
			}
		}
		if f.running {
			active, sub, pid = "active", "running", 42
		}
		dirty := "no"
		if f.dirty {
			dirty = "yes"
		}
		fragment := f.fragment
		if f.loaded && fragment == "" {
			fragment = "/home/owner/.config/systemd/user/" + args[2]
		}
		if !f.loaded {
			fragment = ""
		}
		output := fmt.Sprintf("LoadState=%s\nActiveState=%s\nSubState=%s\nMainPID=%d\nUnitFileState=%s\nNeedDaemonReload=%s\n", load, active, sub, pid, enabled, dirty)
		if strings.Contains(strings.Join(args, " "), "FragmentPath") {
			output += "FragmentPath=" + fragment + "\nDropInPaths=\n"
		}
		return []byte(output), nil

	case "stop":
		if !f.refuseStop {
			f.running = false
		}
	case "disable":
		f.enabled = false
	case "daemon-reload":
		f.loaded = true
		f.dirty = false
	case "enable":
		f.enabled = true
	case "start":
		f.running = true
	default:
		return nil, fmt.Errorf("unexpected %s", args[1])
	}
	return nil, nil
}
func (f *fakeManager) mutations() []string {
	var result []string
	for _, call := range f.calls {
		if !strings.HasPrefix(call, "--user show ") {
			result = append(result, call)
		}
	}
	return result
}
func managerFixture(t *testing.T) (*os.Root, string) {
	t.Helper()
	dir := t.TempDir()
	root, err := os.OpenRoot(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = root.Close() })
	return root, filepath.Join(dir, "dev.wefty.l1.service")
}
func TestManagerConvergesWithoutUnneededRestarts(t *testing.T) {
	root, path := managerFixture(t)
	f := &fakeManager{}
	f.fragment = path
	m := Manager{Run: f.run}
	action, err := m.Converge(t.Context(), root, path, "dev.wefty.l1.service", []byte("first"))
	if err != nil || action != Loaded {
		t.Fatalf("first activation: %s %v", action, err)
	}
	if !slices.Equal(f.mutations(), []string{"--user daemon-reload", "--user enable dev.wefty.l1.service", "--user start dev.wefty.l1.service"}) {
		t.Fatal(f.calls)
	}
	f.calls = nil
	action, err = m.Converge(t.Context(), root, path, "dev.wefty.l1.service", []byte("first"))
	if err != nil || action != Unchanged || len(f.mutations()) != 0 {
		t.Fatalf("unchanged: %s %v %v", action, err, f.calls)
	}
	f.calls = nil
	action, err = m.Converge(t.Context(), root, path, "dev.wefty.l1.service", []byte("second"))
	if err != nil || action != Reloaded {
		t.Fatalf("changed: %s %v", action, err)
	}
	if !slices.Equal(f.mutations(), []string{"--user stop dev.wefty.l1.service", "--user daemon-reload", "--user enable dev.wefty.l1.service", "--user start dev.wefty.l1.service"}) {
		t.Fatal(f.calls)
	}
	f.running = false
	f.calls = nil
	action, err = m.Converge(t.Context(), root, path, "dev.wefty.l1.service", []byte("second"))
	if err != nil || action != Started || !slices.Equal(f.mutations(), []string{"--user enable dev.wefty.l1.service", "--user start dev.wefty.l1.service"}) {
		t.Fatalf("stopped: %s %v %v", action, err, f.calls)
	}
}
func TestManagerInspectionFailureAndUnknownOutputDoNotMutate(t *testing.T) {
	for _, f := range []*fakeManager{{inspectErr: errors.New("bus unavailable")}, {output: "" + "LoadState=mystery\n"}, {output: "LoadState=not-found\nActiveState=active\nSubState=running\nMainPID=42\nUnitFileState=\nNeedDaemonReload=no\nFragmentPath=\nDropInPaths=\n"}} {
		root, path := managerFixture(t)
		m := Manager{Run: f.run}
		if _, err := m.Converge(t.Context(), root, path, "dev.wefty.l1.service", []byte("first")); err == nil {
			t.Fatal("unknown accepted")
		}
		if len(f.mutations()) != 0 {
			t.Fatal(f.calls)
		}
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Fatal("published on unknown state")
		}
	}
}
func TestManagerConfirmsStopBeforePublicationOrDisable(t *testing.T) {
	root, path := managerFixture(t)
	if err := root.WriteFile(filepath.Base(path), []byte("old"), 0644); err != nil {
		t.Fatal(err)
	}
	f := &fakeManager{loaded: true, running: true, enabled: true, refuseStop: true}
	f.fragment = path
	m := Manager{Run: f.run, UnitDirectory: filepath.Dir(path), StopBound: time.Millisecond, PollInterval: time.Microsecond}
	if _, err := m.Converge(t.Context(), root, path, "dev.wefty.l1.service", []byte("new")); err == nil {
		t.Fatal("unconfirmed stop accepted")
	}
	b, _ := os.ReadFile(path)
	if string(b) != "old" {
		t.Fatal("published before stop")
	}
	f.calls = nil
	if err := m.Unload(t.Context(), "dev.wefty.l1.service"); err == nil {
		t.Fatal("unconfirmed unload accepted")
	}
	if slices.Contains(f.mutations(), "--user disable dev.wefty.l1.service") {
		t.Fatal("disabled before confirmed stop")
	}
	f.refuseStop = false
	f.calls = nil
	if err := m.Unload(t.Context(), "dev.wefty.l1.service"); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(f.mutations(), []string{"--user stop dev.wefty.l1.service", "--user disable dev.wefty.l1.service"}) {
		t.Fatal(f.calls)
	}
}

func TestManagerReloadsDirtyDefinitionWithoutRestartingOtherServices(t *testing.T) {
	root, path := managerFixture(t)
	if err := root.WriteFile(filepath.Base(path), []byte("current"), 0644); err != nil {
		t.Fatal(err)
	}
	f := &fakeManager{loaded: true, running: true, enabled: true, dirty: true}
	f.fragment = path
	action, err := (Manager{Run: f.run}).Converge(t.Context(), root, path, "dev.wefty.l1.service", []byte("current"))
	if err != nil || action != Reloaded {
		t.Fatalf("dirty cached definition: %s %v", action, err)
	}
	if !slices.Equal(f.mutations(), []string{"--user stop dev.wefty.l1.service", "--user daemon-reload", "--user enable dev.wefty.l1.service", "--user start dev.wefty.l1.service"}) {
		t.Fatal(f.calls)
	}
}
func TestManagerPublicationFailureRestoresPreviousDefinition(t *testing.T) {
	root, path := managerFixture(t)
	if err := root.WriteFile(filepath.Base(path), []byte("previous"), 0644); err != nil {
		t.Fatal(err)
	}
	f := &fakeManager{loaded: true, running: true, enabled: true}
	f.fragment = path
	m := Manager{Run: f.run, publish: func(dir *os.Root, staging, name string) error {
		// Model a directory-sync error after rename, as well as pre-rename errors.
		if err := dir.Rename(staging, name); err != nil {
			return err
		}
		return errors.New("injected publish failure")
	}}
	_, err := m.Converge(t.Context(), root, path, "dev.wefty.l1.service", []byte("replacement"))
	if err == nil {
		t.Fatal("publish failure hidden")
	}
	b, readErr := os.ReadFile(path)
	if readErr != nil || string(b) != "previous" || !f.running {
		t.Fatal("previous service not restored", string(b), readErr)
	}
}
func TestManagerStageAndStopFailuresRetainDefinition(t *testing.T) {
	for _, at := range []string{"stage", "stop", "inspect-after-stop"} {
		t.Run(at, func(t *testing.T) {
			root, path := managerFixture(t)
			if err := root.WriteFile(filepath.Base(path), []byte("old"), 0644); err != nil {
				t.Fatal(err)
			}
			f := &fakeManager{loaded: true, running: true, enabled: true}
			f.fragment = path
			m := Manager{Run: func(ctx context.Context, name string, args ...string) ([]byte, error) {
				if args[1] == "stop" && at == "stop" {
					return nil, errors.New("injected stop failure")
				}
				b, err := f.run(ctx, name, args...)
				if args[1] == "stop" && at == "inspect-after-stop" {
					f.inspectErr = errors.New("injected bus failure")
				}
				return b, err
			}}
			if at == "stage" {
				m.stage = func(*os.Root, string, []byte, os.FileMode) error { return errors.New("injected stage failure") }
			}
			_, err := m.Converge(t.Context(), root, path, "dev.wefty.l1.service", []byte("new"))
			if err == nil {
				t.Fatal("failure hidden")
			}
			b, _ := os.ReadFile(path)
			if string(b) != "old" {
				t.Fatal("changed definition on failure")
			}
			for _, c := range f.mutations() {
				if strings.Contains(c, "daemon-reload") || strings.Contains(c, "start ") {
					t.Fatal("activated despite failure", f.calls)
				}
			}
		})
	}
}

func TestManagerStopBudgetIncludesTheStopCommand(t *testing.T) {
	root, path := managerFixture(t)
	if err := root.WriteFile(filepath.Base(path), []byte("current"), 0644); err != nil {
		t.Fatal(err)
	}
	f := &fakeManager{loaded: true, running: true, enabled: true}
	bounded := false
	f.fragment = path
	m := Manager{UnitDirectory: filepath.Dir(path), StopBound: time.Millisecond, Run: func(ctx context.Context, name string, args ...string) ([]byte, error) {
		if args[1] == "stop" {
			_, bounded = ctx.Deadline()
			return nil, errors.New("stop interrupted")
		}
		return f.run(ctx, name, args...)
	}}
	if err := m.Unload(t.Context(), "dev.wefty.l1.service"); err == nil {
		t.Fatal("stop error lost")
	}
	if !bounded {
		t.Fatal("stop command has no deadline")
	}
}

func TestManagerRequestsUnsetPropertiesForConfirmedAbsence(t *testing.T) {
	f := &fakeManager{}
	m := Manager{Run: func(ctx context.Context, name string, args ...string) ([]byte, error) {
		b, err := f.run(ctx, name, args...)
		// systemctl omits unset properties unless show is given --all.
		if !slices.Contains(args, "--all") {
			b = []byte(strings.ReplaceAll(string(b), "UnitFileState=\n", ""))
		}
		return b, err
	}}
	state, err := m.State(t.Context(), "dev.wefty.l1.service")
	if err != nil || state.Loaded {
		t.Fatalf("confirmed absence unavailable: %+v %v", state, err)
	}
}

func TestManagerFailedStopCanPublishAndRerun(t *testing.T) {
	root, path := managerFixture(t)
	if err := root.WriteFile(filepath.Base(path), []byte("old"), 0644); err != nil {
		t.Fatal(err)
	}
	f := &fakeManager{loaded: true, running: true, enabled: true}
	f.fragment = path
	failed := false
	resets := 0
	m := Manager{StopBound: 5 * time.Millisecond, PollInterval: time.Microsecond, Run: func(ctx context.Context, name string, args ...string) ([]byte, error) {
		if args[1] == "reset-failed" {
			resets++
			failed = false
			return nil, nil
		}
		if args[1] == "start" && failed {
			return nil, errors.New("failure was not reset before restart")
		}
		b, err := f.run(ctx, name, args...)
		if args[1] == "stop" {
			failed = true
		}
		if args[1] == "show" && failed {
			b = []byte(strings.ReplaceAll(strings.ReplaceAll(string(b), "ActiveState=inactive", "ActiveState=failed"), "SubState=dead", "SubState=failed"))
		}
		return b, err
	}, publish: func(dir *os.Root, staging, name string) error {
		if failed {
			return errors.New("published before reset-failed")
		}
		return dir.Rename(staging, name)
	}}
	if _, err := m.Converge(t.Context(), root, path, filepath.Base(path), []byte("new")); err != nil {
		t.Fatal(err)
	}
	if resets != 1 {
		t.Fatalf("resets=%d", resets)
	}
	f.calls = nil
	if action, err := m.Converge(t.Context(), root, path, filepath.Base(path), []byte("new")); err != nil || action != Unchanged || len(f.mutations()) != 0 {
		t.Fatalf("unchanged rerun: %s %v %v", action, err, f.calls)
	}
	// A failed service with matching bytes must also be recoverable on rerun.
	f.running = false
	failed = true
	if _, err := m.Converge(t.Context(), root, path, filepath.Base(path), []byte("new")); err != nil {
		t.Fatal(err)
	}
	if resets != 2 {
		t.Fatalf("rerun resets=%d", resets)
	}
}

func TestManagerRefusesForeignFragmentsAndDropIns(t *testing.T) {
	for _, kind := range []string{"foreign", "drop-in"} {
		t.Run(kind, func(t *testing.T) {
			root, path := managerFixture(t)
			if err := root.WriteFile(filepath.Base(path), []byte("old"), 0644); err != nil {
				t.Fatal(err)
			}
			f := &fakeManager{loaded: true, running: true, enabled: true}
			f.fragment = path
			m := Manager{Run: func(ctx context.Context, name string, args ...string) ([]byte, error) {
				b, err := f.run(ctx, name, args...)
				if args[1] == "show" {
					fragment, drop := path, ""
					if kind == "foreign" {
						fragment = "/etc/systemd/user/" + filepath.Base(path)
					} else {
						drop = path + ".d/override.conf"
					}
					if strings.Contains(strings.Join(args, " "), "FragmentPath") {
						b = []byte(strings.ReplaceAll(strings.ReplaceAll(string(b), "FragmentPath="+path, "FragmentPath="+fragment), "DropInPaths=", "DropInPaths="+drop))
					}
				}
				return b, err
			}}
			if _, err := m.Converge(t.Context(), root, path, filepath.Base(path), []byte("new")); err == nil || !strings.Contains(err.Error(), "foreign") {
				t.Fatalf("foreign definition accepted or unclear advice: %v", err)
			}
			if len(f.mutations()) != 0 {
				t.Fatal(f.calls)
			}
		})
	}
}

func TestExecRunnerKeepsStderrOutOfParsedOutput(t *testing.T) {
	path := filepath.Join(t.TempDir(), "fake-systemctl")
	if err := os.WriteFile(path, []byte("#!/bin/sh\nprintf 'LoadState=not-found\\n'\nprintf 'warning from stderr\\n' >&2\n"), 0700); err != nil {
		t.Fatal(err)
	}
	output, err := ExecRunner(t.Context(), path, "--user", "show")
	if err != nil || string(output) != "LoadState=not-found\n" {
		t.Fatalf("stderr entered parsed output: %q %v", output, err)
	}
	if err := os.WriteFile(path, []byte("#!/bin/sh\nprintf 'diagnostic from stderr\\n' >&2\nexit 1\n"), 0700); err != nil {
		t.Fatal(err)
	}
	_, err = ExecRunner(t.Context(), path, "--user", "show")
	var exit *exec.ExitError
	if !errors.As(err, &exit) || string(exit.Stderr) != "diagnostic from stderr\n" || strings.Contains(err.Error(), "diagnostic from stderr") {
		t.Fatalf("stderr missing from structured error or exposed in report: %v", err)
	}
}

func TestManagerUsesAbsoluteSystemctl(t *testing.T) {
	f := &fakeManager{}
	command := ""
	m := Manager{Run: func(ctx context.Context, name string, args ...string) ([]byte, error) {
		command = name
		return f.run(ctx, name, args...)
	}}
	if _, err := m.State(t.Context(), "dev.wefty.l1.service"); err != nil {
		t.Fatal(err)
	}
	if command != "/usr/bin/systemctl" {
		t.Fatalf("PATH lookup for manager tool: %s", command)
	}
}
