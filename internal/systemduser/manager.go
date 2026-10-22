package systemduser

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/Derek-X-Wang/wefty/internal/durable"
)

const Systemctl = "/usr/bin/systemctl"

type Runner func(context.Context, string, ...string) ([]byte, error)

// ExecRunner does not repeat command output in errors. A diagnostic must not
// copy arbitrary backend output into a setup log or machine-readable report.
// Output keeps stderr separate, available via errors.As to *exec.ExitError
// for a caller that needs a private diagnostic; only stdout is parsed.
func ExecRunner(ctx context.Context, name string, args ...string) ([]byte, error) {
	command := exec.CommandContext(ctx, name, args...)
	command.WaitDelay = time.Second
	output, err := command.Output()
	if err != nil {
		return output, fmt.Errorf("%s failed: %w", name, err)
	}
	return output, nil
}

type Action string

const (
	Unchanged Action = "unchanged"
	Started   Action = "started"
	Loaded    Action = "loaded"
	Reloaded  Action = "reloaded"
)

type State struct {
	Loaded, Running, Enabled, NeedsReload, Stopped, Failed bool
	FragmentPath                                           string
	DropIns                                                []string
	PID                                                    int
}
type Manager struct {
	Run                     Runner
	UnitDirectory           string
	Home, RuntimeDir        string
	Files                   FileSystem
	ToolPath                string
	StopBound, PollInterval time.Duration
	stage                   func(*os.Root, string, []byte, os.FileMode) error
	publish                 func(*os.Root, string, string) error
}

func (m Manager) command(ctx context.Context, args ...string) ([]byte, error) {
	if m.Run == nil {
		return nil, errors.New("systemd user manager has no command runner")
	}
	tool := m.ToolPath
	if tool == "" {
		tool = Systemctl
	}
	return m.Run(ctx, tool, append([]string{"--user"}, args...)...)
}

// State uses machine-readable properties, not status's human output or exit
// code. Only an explicit, coherent not-found answer is absence. A dead bus,
// masked/broken definition, unknown property or unknown state is an error.
func (m Manager) State(ctx context.Context, name string) (State, error) {
	if !unitName.MatchString(name) {
		return State{}, errors.New("invalid fleet user unit name")
	}
	b, err := m.command(ctx, "show", name, "--property=LoadState,ActiveState,SubState,MainPID,UnitFileState,NeedDaemonReload,FragmentPath,DropInPaths", "--all", "--no-pager")
	if err != nil {
		return State{}, fmt.Errorf("cannot inspect user unit %s: %w", name, err)
	}
	properties := map[string]string{}
	for _, line := range strings.Split(strings.TrimSpace(string(b)), "\n") {
		k, v, ok := strings.Cut(line, "=")
		if !ok {
			return State{}, errors.New("unrecognized systemctl show output")
		}
		if _, duplicate := properties[k]; duplicate {
			return State{}, errors.New("duplicate systemctl property")
		}
		switch k {
		case "LoadState", "ActiveState", "SubState", "MainPID", "UnitFileState", "NeedDaemonReload", "FragmentPath", "DropInPaths":
		default:
			return State{}, errors.New("unknown systemctl property")
		}
		properties[k] = v
	}
	if len(properties) != 8 {
		return State{}, errors.New("incomplete systemctl show output")
	}
	pid, err := strconv.Atoi(properties["MainPID"])
	if err != nil || pid < 0 {
		return State{}, errors.New("invalid systemctl PID")
	}
	s := State{PID: pid, FragmentPath: properties["FragmentPath"]}
	switch properties["NeedDaemonReload"] {
	case "yes":
		s.NeedsReload = true
	case "no":
	default:
		return State{}, errors.New("unknown systemctl reload state")
	}
	switch properties["LoadState"] {
	case "not-found":
	case "loaded":
		s.Loaded = true
	default:
		return State{}, errors.New("unknown or unusable user unit load state")
	}
	if (s.Loaded && (!filepath.IsAbs(s.FragmentPath) || !filepath.IsAbs(m.UnitDirectory) || !m.sameFragment(s.FragmentPath, filepath.Join(m.UnitDirectory, name)))) || (!s.Loaded && s.FragmentPath != "") {
		return State{}, foreignDefinition
	}
	s.DropIns, err = dropInWords(properties["DropInPaths"])
	if err != nil {
		return State{}, err
	}
	if err := m.checkDropIns(name, s.DropIns); err != nil {
		return State{}, err
	}

	switch properties["UnitFileState"] {
	case "enabled":
		s.Enabled = true
	case "disabled", "static", "indirect", "enabled-runtime", "linked", "linked-runtime", "":
	default:
		return State{}, errors.New("unknown or unusable user unit enable state")
	}
	active, sub := properties["ActiveState"], properties["SubState"]
	switch {
	case active == "active" && sub == "running" && pid > 0:
		s.Running = true
	case active == "inactive" && sub == "dead" && pid == 0:
		s.Stopped = true
	case active == "failed" && sub == "failed" && pid == 0:
		s.Failed = true
	case active == "activating" && (sub == "start" || sub == "start-pre" || sub == "start-post" || sub == "auto-restart" || sub == "auto-restart-queued"):
	case active == "deactivating" && (sub == "stop" || sub == "stop-sigterm" || sub == "stop-sigkill" || sub == "stop-post" || sub == "final-sigterm" || sub == "final-sigkill"):
	default:
		return State{}, errors.New("unknown or inconsistent user unit process state")
	}
	if !s.Loaded && (!s.Stopped || properties["UnitFileState"] != "" || s.NeedsReload) {
		return State{}, errors.New("inconsistent absent user unit state")
	}
	return s, nil
}

func (m Manager) start(ctx context.Context, name string, reload bool) error {
	if reload {
		if _, err := m.command(ctx, "daemon-reload"); err != nil {
			return fmt.Errorf("reload user manager: %w", err)
		}
	}
	if _, err := m.command(ctx, "enable", name); err != nil {
		return fmt.Errorf("enable user unit %s: %w", name, err)
	}
	if _, err := m.command(ctx, "start", name); err != nil {
		return fmt.Errorf("start user unit %s: %w", name, err)
	}
	return nil
}

// Converge stages changed bytes before stopping the old process, confirms a
// stop before publication, reloads the user manager and enables/starts exactly
// this entry. Matching enabled/running bytes do not cause a restart or reload.
func (m Manager) Converge(ctx context.Context, folder *os.Root, path, name string, payload []byte) (Action, error) {
	if !unitName.MatchString(name) || !filepath.IsAbs(path) || filepath.Base(path) != name {
		return "", errors.New("user unit path does not match its name")
	}
	m.UnitDirectory = filepath.Dir(path)
	state, err := m.State(ctx, name)
	if err != nil {
		return "", err
	}
	current := durable.Holds(folder, name, payload, 0644)
	if current && state.Running && !state.NeedsReload {
		if !state.Enabled {
			if _, err := m.command(ctx, "enable", name); err != nil {
				return "", err
			}
		}
		return Unchanged, nil
	}
	if current && state.Loaded && !state.NeedsReload {
		if state.Failed {
			if _, err := m.command(ctx, "reset-failed", name); err != nil {
				return "", fmt.Errorf("reset failed user unit: %w", err)
			}
		}
		if err := m.start(ctx, name, false); err != nil {
			return "", err
		}
		return Started, nil
	}
	stage, publish := m.stage, m.publish
	if stage == nil {
		stage = durable.Stage
	}
	if publish == nil {
		publish = durable.Publish
	}
	staging := "." + name + ".staged"
	var previous []byte
	if state.Loaded {
		info, statErr := folder.Lstat(name)
		if statErr != nil || !info.Mode().IsRegular() {
			return "", errors.New("loaded user unit has no restorable regular definition; stop it before repairing setup")
		}
		previous, err = folder.ReadFile(name)
		if err != nil {
			return "", errors.New("cannot read loaded user unit definition for recovery")
		}
	}
	if !current {
		if err := stage(folder, staging, payload, 0644); err != nil {
			return "", err
		}
		defer func() { _ = folder.Remove(staging) }()
	}
	if state.Loaded {
		if err := m.stop(ctx, name); err != nil {
			return "", err
		}
	}
	if !current {
		if err := publish(folder, staging, name); err != nil {
			if state.Loaded {
				restoreErr := durable.WriteFileMode(folder, name, previous, 0644)
				if restoreErr == nil {
					restoreErr = m.start(ctx, name, true)
				}
				return "", errors.Join(fmt.Errorf("publish user unit: %w", err), restoreErr)
			}
			return "", err
		}
	}
	if err := m.start(ctx, name, true); err != nil {
		return "", err
	}
	if state.Loaded {
		return Reloaded, nil
	}
	return Loaded, nil
}
func (m Manager) stop(ctx context.Context, name string) error {
	bound := m.StopBound
	if bound <= 0 {
		bound = (DefaultStopTimeoutSeconds + 15) * time.Second
	}
	interval := m.PollInterval
	if interval <= 0 {
		interval = 100 * time.Millisecond
	}
	wait, cancel := context.WithTimeout(ctx, bound)
	defer cancel()
	if _, err := m.command(wait, "stop", name); err != nil {
		return fmt.Errorf("stop user unit %s: %w", name, err)
	}
	for {
		state, err := m.State(wait, name)
		if err != nil {
			return fmt.Errorf("user unit stop is unverified: %w", err)
		}
		if state.Stopped || state.Failed {
			if state.Failed {
				if _, err := m.command(wait, "reset-failed", name); err != nil {
					return fmt.Errorf("reset failed user unit: %w", err)
				}
			}
			return nil
		}
		timer := time.NewTimer(interval)
		select {
		case <-wait.Done():
			timer.Stop()
			return fmt.Errorf("user unit %s did not confirm stopped: %w", name, wait.Err())
		case <-timer.C:
		}
	}
}

// Unload stops and confirms the process is gone, then disables login startup.
// It retains the definition. Deleting retained files is a caller's policy.
func (m Manager) Unload(ctx context.Context, name string) error {
	state, err := m.State(ctx, name)
	if err != nil || !state.Loaded {
		return err
	}
	if err := m.stop(ctx, name); err != nil {
		return err
	}
	_, err = m.command(ctx, "disable", name)
	return err
}

func (m Manager) sameFragment(reported, published string) bool {
	expected, err := m.Files.lstat(published)
	if err != nil || !expected.Mode().IsRegular() {
		return false
	}
	actual, err := m.Files.stat(reported)
	return err == nil && actual.Mode().IsRegular() && os.SameFile(expected, actual)
}
