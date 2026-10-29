// Package launchagent renders macOS per-user LaunchAgents and loads them into
// the owner's GUI login domain. It needs no privilege: every file is under the
// owner's home, and every launchctl call targets gui/<uid>.
//
// The interim OCI bootstrap's system LaunchDaemon (runner/lima) is a different
// installer with a different trust model, and this package does not touch it.
package launchagent

import (
	"bytes"
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/Derek-X-Wang/wefty/internal/durable"
)

// Launchctl is the only service-manager executable this package runs.
const Launchctl = "/bin/launchctl"

// Runner runs one command and returns its output. Tests inject a fake, so no
// test ever reaches the real launchd.
type Runner func(ctx context.Context, name string, args ...string) ([]byte, error)

// ErrServiceNotFound is launchd's confirmed answer that it has no such
// service. Only this answer means "not loaded"; any other failure to inspect a
// service leaves its state unknown.
var ErrServiceNotFound = errors.New("launchd has no such service")

// launchctlServiceNotFound is the exit status `launchctl print` uses for a
// service launchd does not have ("Could not find service").
const launchctlServiceNotFound = 113

// ExecRunner runs the real command. launchctl's "could not find service"
// answer is returned as ErrServiceNotFound.
func ExecRunner(ctx context.Context, name string, args ...string) ([]byte, error) {
	command := exec.CommandContext(ctx, name, args...)
	command.WaitDelay = time.Second
	output, err := command.CombinedOutput()
	if err != nil {
		detail := strings.TrimSpace(string(output))
		if name == Launchctl {
			err = classifyLaunchctlFailure(err, detail)
		}
		return output, fmt.Errorf("%s %s: %w: %s", name, strings.Join(args, " "), err, detail)
	}
	return output, nil
}

// classifyLaunchctlFailure marks launchctl's confirmed "no such service"
// answer, and nothing else, as ErrServiceNotFound.
func classifyLaunchctlFailure(err error, detail string) error {
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) &&
		(exitErr.ExitCode() == launchctlServiceNotFound || strings.Contains(detail, "Could not find service")) {
		return fmt.Errorf("%w: %w", ErrServiceNotFound, err)
	}
	return err
}

// Agent is one login service. It holds no secrets: every value is visible to
// anyone who can read the owner's LaunchAgents folder.
type Agent struct {
	// SuppressAutostart is used only for recorded retiring generations.
	SuppressAutostart bool
	Label             string
	Program           string
	Arguments         []string
	Environment       map[string]string
	WorkingDirectory  string
	// LogPath receives both standard output and standard error.
	LogPath string
	// ExitTimeOut is how long launchd waits after SIGTERM before SIGKILL; zero
	// keeps launchd's default.
	ExitTimeOut int
	// ProcessType is launchd's resource class; empty keeps launchd's default.
	ProcessType string
	// InputDigest fingerprints everything the service reads at start that is
	// not in the plist itself: its binary and its connection profile
	// settings. It is recorded in the plist so a change to either changes the
	// file, and a changed file is what triggers a reload.
	InputDigest string
}

// Path is where the agent's plist lives for the owner whose home is home.
func Path(home, label string) string {
	return filepath.Join(home, "Library", "LaunchAgents", label+".plist")
}

func (agent Agent) validate() error {
	if agent.Label == "" || strings.ContainsAny(agent.Label, "/ \r\n") {
		return errors.New("LaunchAgent label is invalid")
	}
	for name, path := range map[string]string{
		"program": agent.Program, "working directory": agent.WorkingDirectory, "log": agent.LogPath,
	} {
		if !filepath.IsAbs(path) || strings.ContainsAny(path, "\r\n") {
			return fmt.Errorf("LaunchAgent %s %s path must be absolute", agent.Label, name)
		}
	}
	for _, value := range append(slices.Clone(agent.Arguments), agent.InputDigest) {
		if strings.ContainsAny(value, "\r\n") {
			return fmt.Errorf("LaunchAgent %s values must be single-line", agent.Label)
		}
	}
	for name, value := range agent.Environment {
		if name == "" || strings.ContainsAny(name, "=\r\n") || strings.ContainsAny(value, "\r\n") {
			return fmt.Errorf("LaunchAgent %s environment must be single-line", agent.Label)
		}
	}
	if strings.Contains(agent.InputDigest, "--") {
		return fmt.Errorf("LaunchAgent %s input digest is invalid", agent.Label)
	}
	return nil
}

// Render returns the exact plist setup installs. It starts at login, restarts
// after a crash, and never restarts a clean exit.
func Render(agent Agent) ([]byte, error) {
	if err := agent.validate(); err != nil {
		return nil, err
	}
	var output bytes.Buffer
	output.WriteString("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n")
	output.WriteString("<!DOCTYPE plist PUBLIC \"-//Apple//DTD PLIST 1.0//EN\" \"http://www.apple.com/DTDs/PropertyList-1.0.dtd\">\n")
	output.WriteString("<!-- Written by wefty setup. Run `wefty setup` again instead of editing this file. -->\n")
	if agent.InputDigest != "" {
		output.WriteString("<!-- wefty-input-digest: " + agent.InputDigest + " -->\n")
	}
	output.WriteString("<plist version=\"1.0\">\n<dict>\n")
	writeString(&output, "Label", agent.Label)
	output.WriteString("  <key>ProgramArguments</key>\n  <array>\n")
	writeArrayString(&output, agent.Program)
	for _, argument := range agent.Arguments {
		writeArrayString(&output, argument)
	}
	output.WriteString("  </array>\n")
	writeString(&output, "WorkingDirectory", agent.WorkingDirectory)
	if len(agent.Environment) > 0 {
		output.WriteString("  <key>EnvironmentVariables</key>\n  <dict>\n")
		names := make([]string, 0, len(agent.Environment))
		for name := range agent.Environment {
			names = append(names, name)
		}
		slices.Sort(names)
		for _, name := range names {
			writeIndentedString(&output, "    ", name, agent.Environment[name])
		}
		output.WriteString("  </dict>\n")
	}
	writeString(&output, "StandardOutPath", agent.LogPath)
	writeString(&output, "StandardErrorPath", agent.LogPath)
	if agent.SuppressAutostart {
		output.WriteString("  <key>RunAtLoad</key>\n  <false/>\n  <key>KeepAlive</key>\n  <false/>\n")
	} else {
		output.WriteString("  <key>RunAtLoad</key>\n  <true/>\n")
		output.WriteString("  <key>KeepAlive</key>\n  <dict>\n")
		output.WriteString("    <key>SuccessfulExit</key>\n    <false/>\n")
		output.WriteString("  </dict>\n")
	}
	output.WriteString("  <key>ThrottleInterval</key>\n  <integer>10</integer>\n")
	if agent.ExitTimeOut > 0 {
		output.WriteString("  <key>ExitTimeOut</key>\n  <integer>" + strconv.Itoa(agent.ExitTimeOut) + "</integer>\n")
	}
	if agent.ProcessType != "" {
		writeString(&output, "ProcessType", agent.ProcessType)
	}
	output.WriteString("</dict>\n</plist>\n")
	return output.Bytes(), nil
}

func writeString(output *bytes.Buffer, name, value string) {
	writeIndentedString(output, "  ", name, value)
}

func writeIndentedString(output *bytes.Buffer, indent, name, value string) {
	output.WriteString(indent + "<key>")
	_ = xml.EscapeText(output, []byte(name))
	output.WriteString("</key>\n" + indent + "<string>")
	_ = xml.EscapeText(output, []byte(value))
	output.WriteString("</string>\n")
}

func writeArrayString(output *bytes.Buffer, value string) {
	output.WriteString("    <string>")
	_ = xml.EscapeText(output, []byte(value))
	output.WriteString("</string>\n")
}

// Action is what Converge did to one login service.
type Action string

const (
	// Unchanged means the plist matched and the service was already running.
	Unchanged Action = "unchanged"
	// Started means the plist matched and the service was loaded but not
	// running, for example after a clean exit, so it was started.
	Started Action = "started"
	// Loaded means the service was not loaded and now is.
	Loaded Action = "loaded"
	// Reloaded means a loaded service was replaced because its plist changed.
	Reloaded Action = "reloaded"
)

// State is what launchd reports about one service in the owner's domain.
type State struct {
	Loaded  bool
	Running bool
	PID     int
}

// Manager loads plists into one owner's GUI login domain.
type Manager struct {
	Run Runner
	UID int
	// UnloadBound limits the wait for launchd to forget a service after
	// bootout. Zero means 30 seconds.
	UnloadBound time.Duration
	// PollInterval is how often the unload wait asks launchd. Zero means 100ms.
	PollInterval time.Duration
	// stage and publish are test seams over durable.Stage and durable.Publish.
	stage   func(dir *os.Root, staging string, payload []byte, mode os.FileMode) error
	publish func(dir *os.Root, staging, name string) error
}

func (manager Manager) domain() string { return "gui/" + strconv.Itoa(manager.UID) }

func (manager Manager) target(label string) string { return manager.domain() + "/" + label }

// State asks launchd whether the service is loaded and whether its process
// is running. A loaded service can be stopped: KeepAlive restarts it only
// after a crash, so a clean exit leaves it loaded and idle. Only launchd's
// confirmed "no such service" means not loaded; any other failure is
// returned, because acting on a guess could replace a definition under a
// process that keeps running the old one.
func (manager Manager) State(ctx context.Context, label string) (State, error) {
	if manager.Run == nil {
		return State{}, errors.New("LaunchAgent manager has no command runner")
	}
	output, err := manager.Run(ctx, Launchctl, "print", manager.target(label))
	if errors.Is(err, ErrServiceNotFound) {
		return State{}, nil
	}
	if err != nil {
		return State{}, fmt.Errorf("cannot tell whether LaunchAgent %s is loaded: %w", label, err)
	}
	state := State{Loaded: true, Running: printedRunning(output)}
	for _, line := range strings.Split(string(output), "\n") {
		if value, ok := strings.CutPrefix(strings.TrimSpace(line), "pid = "); ok {
			state.PID, _ = strconv.Atoi(strings.TrimSpace(value))
			break
		}
	}
	return state, nil
}

// printedRunning reads the service's own state line, the first one in
// `launchctl print` output; nested sections come after it.
func printedRunning(output []byte) bool {
	for _, line := range strings.Split(string(output), "\n") {
		key, value, found := strings.Cut(strings.TrimSpace(line), " = ")
		if found && key == "state" {
			return value == "running"
		}
	}
	return false
}

// stagingName keeps a prepared plist hidden and without the .plist suffix,
// so launchd never mistakes it for a definition.
func stagingName(name string) string { return "." + name + ".staged" }

// Converge makes launchd run exactly payload for label. folder is the
// owner's LaunchAgents folder, opened so no write can leave it, and path is
// the plist's absolute name for launchctl. It writes and reloads only when
// the plist differs (bytes, type or mode), loads a service that is not
// loaded, and starts one that is loaded but stopped, so a second run with
// nothing to do issues no reload.
//
// A changed definition is staged and synced before the running service is
// unloaded, and published only after the unload. If publishing fails, the
// previous definition is put back and loaded again, so a failure never leaves
// a service that was running unloaded.
func (manager Manager) Converge(ctx context.Context, folder *os.Root, path, label string, payload []byte) (Action, error) {
	return manager.converge(ctx, folder, path, label, payload, false)
}

// LoadGeneration allows a new generation label without replacing any loaded definition.
func (manager Manager) LoadGeneration(ctx context.Context, folder *os.Root, path, label string, payload []byte) (Action, error) {
	if !generationLabel(label) {
		return "", errors.New("not a generation label")
	}
	return manager.converge(ctx, folder, path, label, payload, true)
}
func generationLabel(label string) bool { return strings.HasPrefix(label, "dev.wefty.node-agent.g-") }
func (manager Manager) converge(ctx context.Context, folder *os.Root, path, label string, payload []byte, generationStart bool) (Action, error) {
	if manager.Run == nil {
		return "", errors.New("LaunchAgent manager has no command runner")
	}
	if filepath.Base(path) != label+".plist" {
		return "", fmt.Errorf("LaunchAgent %s plist path %s does not match its label", label, path)
	}
	stage, publish := manager.stage, manager.publish
	if stage == nil {
		stage = durable.Stage
	}
	if publish == nil {
		publish = durable.Publish
	}
	name, staging := filepath.Base(path), stagingName(filepath.Base(path))
	current := durable.Holds(folder, name, payload, 0o644)
	state, err := manager.State(ctx, label)
	if err != nil {
		return "", err
	}
	// Setup must restore autostart even when an existing legacy definition needs
	// no bootstrap. Migration rollback can leave a persistent disabled override.
	if current && state.Loaded && label == "dev.wefty.node-agent" {
		disabled, err := manager.startDisabled(ctx, label)
		if err != nil {
			return "", err
		}
		if disabled {
			if err := manager.RestoreLegacyStart(ctx, label); err != nil {
				return "", err
			}
		}
	}
	if current && state.Running {
		return Unchanged, nil
	}
	if generationLabel(label) && (state.Loaded || !generationStart) {
		return "", errors.New("retained generation cannot be reloaded or restarted by Converge")
	}
	if current && state.Loaded {
		if _, err := manager.Run(ctx, Launchctl, "kickstart", manager.target(label)); err != nil {
			return "", fmt.Errorf("start LaunchAgent %s: %w", label, err)
		}
		return Started, nil
	}
	// A loaded service is replaced only while its previous definition can be
	// put back. Without one, a later publish failure would leave it unloaded
	// with nothing to reload, so refuse while it is still running.
	var previous []byte
	if state.Loaded {
		if previous, err = folder.ReadFile(name); err != nil {
			return "", fmt.Errorf("LaunchAgent %s is loaded but its definition %s cannot be read, so it could not be restored if replacing it failed; "+
				"unload it with `launchctl bootout %s` and run setup again: %w", label, path, manager.target(label), err)
		}
	}
	if !current {
		if err := stage(folder, staging, payload, 0o644); err != nil {
			return "", fmt.Errorf("stage LaunchAgent %s: %w", label, err)
		}
	}
	if !state.Loaded {
		if !current {
			if err := publish(folder, staging, name); err != nil {
				_ = folder.Remove(staging)
				return "", fmt.Errorf("write LaunchAgent %s: %w", label, err)
			}
		}
		if err := manager.load(ctx, path, label); err != nil {
			return "", err
		}
		return Loaded, nil
	}
	// Loaded, with a changed definition; previous is the one to put back.
	if _, err := manager.Run(ctx, Launchctl, "bootout", manager.target(label)); err != nil {
		_ = folder.Remove(staging)
		return "", fmt.Errorf("unload LaunchAgent %s: %w", label, err)
	}
	if err := manager.waitUnloaded(ctx, label); err != nil {
		_ = folder.Remove(staging)
		return "", err
	}
	if err := publish(folder, staging, name); err != nil {
		_ = folder.Remove(staging)
		publishErr := fmt.Errorf("publish LaunchAgent %s: %w", label, err)
		if restoreErr := manager.restore(ctx, folder, path, label, previous); restoreErr != nil {
			return "", errors.Join(publishErr, restoreErr)
		}
		return "", fmt.Errorf("%w; the previous definition was restored and loaded", publishErr)
	}
	if err := manager.load(ctx, path, label); err != nil {
		return "", err
	}
	return Reloaded, nil
}

// load bootstraps the plist at path. A service the owner disabled cannot be
// bootstrapped; setup's job is to make it run, so it clears that override.
// Enabling only a disabled label avoids creating persistent override entries
// for fresh generation labels.
func (manager Manager) load(ctx context.Context, path, label string) error {
	disabled, err := manager.startDisabled(ctx, label)
	if err != nil {
		return err
	}
	if disabled {
		if _, err := manager.Run(ctx, Launchctl, "enable", manager.target(label)); err != nil {
			return fmt.Errorf("enable LaunchAgent %s: %w", label, err)
		}
	}
	if _, err := manager.Run(ctx, Launchctl, "bootstrap", manager.domain(), path); err != nil {
		return fmt.Errorf("load LaunchAgent %s: %w", label, err)
	}
	return nil
}

// restore puts the previous definition back, whether or not the failed
// publish had already replaced it, and loads it again.
func (manager Manager) restore(ctx context.Context, folder *os.Root, path, label string, previous []byte) error {
	if !durable.Holds(folder, filepath.Base(path), previous, 0o644) {
		if err := durable.WriteFileMode(folder, filepath.Base(path), previous, 0o644); err != nil {
			return fmt.Errorf("restore the previous LaunchAgent %s: %w", label, err)
		}
	}
	if err := manager.load(ctx, path, label); err != nil {
		return fmt.Errorf("reload the previous LaunchAgent %s: %w", label, err)
	}
	return nil
}

// waitUnloaded waits for launchd to confirm it has forgotten label: bootout
// can return while the service is still being torn down, and bootstrapping
// then fails. An inspection failure is not that confirmation. launchd's
// confirmation counts even when it arrives at the bound; past the bound, the
// error reports launchd's last answer, and "still loaded" only after launchd
// said so.
func (manager Manager) waitUnloaded(ctx context.Context, label string) error {
	bound := manager.UnloadBound
	if bound <= 0 {
		bound = 30 * time.Second
	}
	interval := manager.PollInterval
	if interval <= 0 {
		interval = 100 * time.Millisecond
	}
	waitContext, cancel := context.WithTimeout(ctx, bound)
	defer cancel()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	// lastErr is why the latest check did not confirm the unload, and nil
	// after launchd answered "loaded". A check the bound cut off gave no
	// answer, so its failure counts only when no check came before it.
	var lastErr error
	for checks := 0; ; checks++ {
		state, err := manager.State(waitContext, label)
		if err == nil && !state.Loaded {
			return nil
		}
		if err == nil || waitContext.Err() == nil || checks == 0 {
			lastErr = err
		}
		select {
		case <-waitContext.Done():
			if lastErr != nil {
				return fmt.Errorf("LaunchAgent %s was not confirmed unloaded %s after bootout: %w", label, bound, lastErr)
			}
			return fmt.Errorf("LaunchAgent %s was still loaded %s after bootout", label, bound)
		case <-ticker.C:
		}
	}
}

// Unload waits for launchd to confirm absence before callers delete files.
// A failed inspection or bootout leaves the install intact for a retry.
func (manager Manager) Unload(ctx context.Context, label string) error {
	if generationLabel(label) {
		return errors.New("retained generation requires finished-generation cleanup")
	}
	return manager.unload(ctx, label, false)
}

// RunningProcessError is a positive stopped-process validation refusal.
// Service-manager observations and I/O failures retain their original errors.
type RunningProcessError struct{ Label string }

func (e *RunningProcessError) Error() string {
	return fmt.Sprintf("LaunchAgent %s process still running", e.Label)
}

// UnloadStoppedLegacy disables legacy restart before its final stopped-process
// check, then waits for launchd to confirm unload. The override stays disabled
// through forward conversion and completion, including an absent plist. A caller
// that instead retains legacy ownership must restore its start override.
func (manager Manager) UnloadStoppedLegacy(ctx context.Context, label string) error {
	if generationLabel(label) {
		return errors.New("retained generation requires finished-generation cleanup")
	}
	if manager.Run == nil {
		return errors.New("LaunchAgent manager has no command runner")
	}
	// This override is retained through plist removal and across crashes. The
	// retired legacy label must never autostart beside a generation primary.
	if _, err := manager.Run(ctx, Launchctl, "disable", manager.target(label)); err != nil {
		return fmt.Errorf("fence legacy LaunchAgent %s restart: %w", label, err)
	}
	return manager.unload(ctx, label, true)
}

// startDisabled reads the persisted override without changing an already
// enabled service. Unknown output is an operational decode fault, never proof
// that autostart is enabled.
func (manager Manager) startDisabled(ctx context.Context, targetLabel string) (bool, error) {
	payload, err := manager.Run(ctx, Launchctl, "print-disabled", manager.domain())
	if err != nil {
		return false, fmt.Errorf("inspect LaunchAgent %s autostart: %w", targetLabel, err)
	}
	lines := strings.Split(strings.TrimSpace(string(payload)), "\n")
	// Other launchctl sections may surround the top-level override block.
	start, depth := 0, 0
	for start < len(lines) {
		line := strings.TrimSpace(lines[start])
		if depth == 0 && line == "disabled services = {" {
			break
		}
		if strings.HasSuffix(line, "{") {
			depth++
		} else if line == "}" && depth > 0 {
			depth--
		}
		start++
	}
	end := start + 1
	for end < len(lines) && strings.TrimSpace(lines[end]) != "}" {
		end++
	}
	if start == len(lines) || end == len(lines) {
		return false, errors.New("decode launchctl disabled services: malformed domain record")
	}
	seen := make(map[string]bool)
	disabled := false
	for _, line := range lines[start+1 : end] {
		if strings.TrimSpace(line) == "" {
			continue
		}
		key, value, ok := strings.Cut(strings.TrimSpace(line), "=>")
		label, err := strconv.Unquote(strings.TrimSpace(key))
		if !ok || err != nil || label == "" || seen[label] {
			return false, errors.New("decode launchctl disabled services: malformed or duplicate entry")
		}
		seen[label] = true
		var flag bool
		switch strings.TrimSpace(value) {
		case "true", "disabled":
			flag = true
		case "false", "enabled":
			flag = false
		default:
			return false, errors.New("decode launchctl disabled services: unknown override")
		}
		if label == targetLabel {
			disabled = flag
		}
	}
	return disabled, nil
}

// RestoreLegacyStart clears the durable migration restart fence only after the
// caller establishes that legacy ownership is retained. Repeating it is safe
// after a crash before the caller records rollback completion.
func (manager Manager) RestoreLegacyStart(ctx context.Context, label string) error {
	if label != "dev.wefty.node-agent" {
		return errors.New("not the legacy node-agent label")
	}
	if manager.Run == nil {
		return errors.New("LaunchAgent manager has no command runner")
	}
	if _, err := manager.Run(ctx, Launchctl, "enable", manager.target(label)); err != nil {
		return fmt.Errorf("restore legacy LaunchAgent %s autostart: %w", label, err)
	}
	return nil
}

// UnloadFinishedGeneration refuses a running generation even if a caller has a completion receipt.
func (manager Manager) UnloadFinishedGeneration(ctx context.Context, label string) error {
	if !generationLabel(label) {
		return errors.New("not a generation label")
	}
	return manager.unload(ctx, label, true)
}

// SuppressGenerationStart persists a launchd disabled override without bootout.
func (manager Manager) SuppressGenerationStart(ctx context.Context, label string) error {
	if manager.Run == nil {
		return errors.New("LaunchAgent manager has no command runner")
	}
	if !generationLabel(label) {
		return errors.New("not a generation label")
	}
	_, err := manager.Run(ctx, Launchctl, "disable", manager.target(label))
	return err
}
func (manager Manager) unload(ctx context.Context, label string, finished bool) error {
	state, err := manager.State(ctx, label)
	if err != nil || !state.Loaded {
		return err
	}
	if finished && state.Running {
		return &RunningProcessError{Label: label}
	}
	if _, err := manager.Run(ctx, Launchctl, "bootout", manager.target(label)); err != nil {
		return fmt.Errorf("unload LaunchAgent %s: %w", label, err)
	}
	return manager.waitUnloaded(ctx, label)
}

// UnloadMaintenancePrimary is used only after generation-aware maintenance has
// recorded the exact quiet primary. Graceful bootout retains legacy semantics.
func (manager Manager) UnloadMaintenancePrimary(ctx context.Context, label string) error {
	if !generationLabel(label) {
		return errors.New("not a generation primary label")
	}
	if err := manager.SuppressGenerationStart(ctx, label); err != nil {
		return err
	}
	return manager.unload(ctx, label, false)
}

// RestoreMaintenancePrimary reloads the exact retained definition. No new
// generation is allocated and no different binary or catalog is published.
func (manager Manager) RestoreMaintenancePrimary(ctx context.Context, folder *os.Root, path, label string, payload []byte) error {
	if !generationLabel(label) {
		return errors.New("not a generation primary label")
	}
	state, err := manager.State(ctx, label)
	if err != nil {
		return err
	}
	if _, err = manager.Run(ctx, Launchctl, "enable", manager.target(label)); err != nil {
		return err
	}
	if state.Running {
		return nil
	}
	if state.Loaded {
		if err = manager.unload(ctx, label, true); err != nil {
			return err
		}
	}
	_, err = manager.LoadGeneration(ctx, folder, path, label, payload)
	return err
}
