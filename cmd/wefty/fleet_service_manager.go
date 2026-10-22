package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/Derek-X-Wang/wefty/internal/launchagent"
	"github.com/Derek-X-Wang/wefty/internal/serviceplan"
	"github.com/Derek-X-Wang/wefty/internal/systemduser"
)

// Fleet keeps the existing setup descriptors and reports at the Mac boundary.
// Only this adapter translates them to systemd's distinct unit/state types.
type fleetServiceManager struct{ host setupHost }

func (m fleetServiceManager) name(label string) string {
	if m.host.goos == "linux" {
		return label + ".service"
	}
	return label
}
func (m fleetServiceManager) file(label string) string {
	if m.host.goos == "linux" {
		return m.name(label)
	}
	return label + ".plist"
}
func (m fleetServiceManager) description() string {
	if m.host.goos == "linux" {
		return "systemd user unit"
	}
	return "LaunchAgent"
}
func (m fleetServiceManager) render(service launchagent.Agent, previous string) ([]byte, error) {
	if m.host.goos != "linux" {
		return launchagent.Render(service)
	}
	environment := make(map[string]string, len(service.Environment))
	for k, v := range service.Environment {
		environment[k] = v
	}
	if _, ok := environment["WEFTY_LAUNCH_UNIT"]; ok {
		environment["WEFTY_LAUNCH_UNIT"] = m.name(service.Label)
	}
	u := systemduser.Unit{Name: m.name(service.Label), Program: service.Program, Arguments: service.Arguments, Environment: environment, WorkingDirectory: service.WorkingDirectory, LogPath: service.LogPath, InputDigest: service.InputDigest, TimeoutStopSeconds: service.ExitTimeOut}
	if previous != "" {
		u.After = []string{m.name(previous)}
	}
	return systemduser.Render(u)
}
func (m fleetServiceManager) linuxManager(paths setupPaths) (systemduser.Manager, error) {
	tool, err := systemduser.ResolveTool("systemctl", m.host.linuxFiles)
	if err != nil {
		return systemduser.Manager{}, err
	}
	return systemduser.Manager{Run: m.host.systemctl, ToolPath: tool, UnitDirectory: paths.launchAgents, Home: paths.home, RuntimeDir: m.host.xdgRuntimeDir, Files: m.host.linuxFiles}, nil
}
func (m fleetServiceManager) state(ctx context.Context, label string) (launchagent.State, error) {
	state, _, err := m.stateWithNotes(ctx, label)
	return state, err
}
func (m fleetServiceManager) stateWithNotes(ctx context.Context, label string) (launchagent.State, []string, error) {
	if m.host.goos != "linux" {
		state, err := (launchagent.Manager{Run: m.host.launchctl, UID: m.host.uid}).State(ctx, label)
		return state, nil, err
	}
	home, err := filepath.EvalSymlinks(m.host.home)
	if err != nil {
		return launchagent.State{}, nil, err
	}
	paths, issues := resolveFleetMaintenancePaths(home, m.host)
	if len(issues) > 0 {
		return launchagent.State{}, nil, fmt.Errorf("cannot verify unit paths: %w", issues[0].err)
	}
	manager, err := m.linuxManager(paths)
	if err != nil {
		return launchagent.State{}, nil, err
	}
	state, err := manager.State(ctx, m.name(label))
	return launchagent.State{Loaded: state.Loaded, Running: state.Running, PID: state.PID}, state.DropIns, err
}
func (m fleetServiceManager) converge(ctx context.Context, folder *os.Root, paths setupPaths, service launchagent.Agent, payload []byte) (launchagent.Action, error) {
	bound := m.host.unloadBound
	if bound == 0 && service.ExitTimeOut > 0 {
		bound = time.Duration(service.ExitTimeOut+15) * time.Second
	}
	path := filepath.Join(paths.launchAgents, m.file(service.Label))
	if m.host.goos != "linux" {
		return (launchagent.Manager{Run: m.host.launchctl, UID: m.host.uid, UnloadBound: bound}).Converge(ctx, folder, path, service.Label, payload)
	}
	if bound == 0 {
		// Allow systemd its complete stop timeout plus a confirmation margin.
		bound = (systemduser.DefaultStopTimeoutSeconds + 15) * time.Second
	}
	manager, err := m.linuxManager(paths)
	if err != nil {
		return "", err
	}
	manager.StopBound = bound
	action, err := manager.Converge(ctx, folder, path, m.name(service.Label), payload)
	return launchagent.Action(action), err
}
func (m fleetServiceManager) report(paths setupPaths, service launchagent.Agent, action launchagent.Action) setupServiceReport {
	r := setupServiceReport{Label: m.name(service.Label), Log: service.LogPath, Action: action}
	path := filepath.Join(paths.launchAgents, m.file(service.Label))
	if m.host.goos == "linux" {
		r.Unit = path
	} else {
		r.Plist = path
	}
	return r
}
func (m fleetServiceManager) recordedPATH(payload []byte) (string, error) {
	if m.host.goos == "linux" {
		return systemduser.RecordedPATH(payload)
	}
	return recordedNodePATH(payload)
}
func (m fleetServiceManager) readinessDetail(detail string) string {
	if m.host.goos == "linux" {
		return strings.ReplaceAll(detail, "LaunchAgent", "systemd user unit")
	}
	return detail
}

// unload retains the definition so rollback can restore it before restart.
func (m fleetServiceManager) unload(ctx context.Context, paths setupPaths, label string) error {
	if m.host.goos != "linux" {
		return (launchagent.Manager{Run: m.host.launchctl, UID: m.host.uid, UnloadBound: m.host.unloadBound}).Unload(ctx, label)
	}
	manager, err := m.linuxManager(paths)
	if err != nil {
		return err
	}
	manager.StopBound = m.host.unloadBound
	return manager.Unload(ctx, m.name(label))
}

// Restored files may have the same timestamp as the cached definition. Recovery
// must not depend on NeedDaemonReload noticing the replacement.
func (m fleetServiceManager) reload(ctx context.Context, paths setupPaths) error {
	if m.host.goos != "linux" {
		return nil
	}
	manager, err := m.linuxManager(paths)
	if err != nil {
		return err
	}
	return manager.Reload(ctx)
}

// The caller verifies the current PID/boot and advertised capability first.
func (m fleetServiceManager) heartbeat(ctx context.Context, paths setupPaths, label string) error {
	if m.host.goos != "linux" {
		_, err := m.host.launchctl(ctx, launchagent.Launchctl, "kill", "SIGUSR1", fmt.Sprintf("gui/%d/%s", m.host.uid, label))
		return err
	}
	manager, err := m.linuxManager(paths)
	if err != nil {
		return err
	}
	_, err = manager.Run(ctx, manager.ToolPath, "--user", "kill", "--kill-who=main", "--signal=SIGUSR1", m.name(label))
	return err
}

// Upgrade uses the same trusted tools, home-confined paths, log ownership and
// override policy as Linux setup, after drain recovery and before preparation.
func (m fleetServiceManager) preflightUpgrade(ctx context.Context, paths setupPaths, role serviceplan.Role) error {
	if m.host.goos != "linux" {
		return nil
	}
	payload, err := os.ReadFile(filepath.Join(paths.launchAgents, m.file(setupNodeAgentLabel)))
	if err != nil {
		return err
	}
	loginPATH, err := m.recordedPATH(payload)
	if err != nil {
		return err
	}
	if err := preflightLinuxFleet(ctx, m.host, paths, nil, loginPATH, role); err != nil {
		return err
	}
	plan, err := setupRolePlan(paths, role)
	if err != nil {
		return err
	}
	manager, err := m.linuxManager(paths)
	if err != nil {
		return err
	}
	for _, service := range plan.Services {
		unit := m.name("dev.wefty." + service.Name)
		probe, cancel := context.WithTimeout(ctx, statusProbeBudget)
		_, err := manager.State(probe, unit)
		cancel()
		if err != nil {
			return fmt.Errorf("cannot verify Linux fleet unit %s; the user service manager is unreachable from this shell or the unit definition is not the one setup wrote; restore manager access and setup's original units before retrying upgrade: %w", unit, err)
		}
	}
	return nil
}
