package main

import (
	"archive/tar"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"time"

	"github.com/Derek-X-Wang/wefty/agent"
	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/internal/agentinstall"
	"github.com/Derek-X-Wang/wefty/internal/connprofile"
	"github.com/Derek-X-Wang/wefty/internal/fabricconfig"
	"github.com/Derek-X-Wang/wefty/internal/launchagent"
	"github.com/Derek-X-Wang/wefty/internal/upgradestate"
	"github.com/Derek-X-Wang/wefty/l1"
	"github.com/Derek-X-Wang/wefty/runner/ocicontrol"
)

const upgradeRemote = "https://github.com/Derek-X-Wang/wefty.git"
const upgradeSourceName = "upgrade-source.git"

// The renewal window starts just before L1 restarts. Only a node heartbeat
// newer than both restarts counts as renewal evidence, so the window holds the
// two restarts and then one full heartbeat interval. The installed node agent
// heartbeats every agent.DefaultHeartbeatInterval: setup's plist passes no
// --heartbeat-interval and the connection profile carries none. The window
// stays well under the 30 s lease (l1.DefaultLeaseDuration); a test holds it.
const (
	upgradeHeartbeatInterval = agent.DefaultHeartbeatInterval
	// upgradeReloadAllowance covers restarting L1 and the run ledger and
	// observing the node afterwards.
	upgradeReloadAllowance = 8 * time.Second
	upgradeRenewalWindow   = upgradeReloadAllowance + upgradeHeartbeatInterval
	// upgradeReplacementBudget bounds the replacement node agent's new boot
	// session and first heartbeat.
	upgradeReplacementBudget = 20 * time.Second
)

// These machine operations extend setupHost, rather than adding public flags.
// Tests use a local bare repository, a fake build, runner and clock.
type upgradeHost struct {
	writeDrain       func(context.Context, string, *upgradeDrainReceipt) error
	remote           string
	build            func(context.Context, string, string, string, string) error
	command          func(context.Context, string, []string, string, ...string) ([]byte, error)
	observe          func(context.Context, connprofile.Profile) (l1.Node, error)
	setClaims        func(context.Context, connprofile.Profile, l1.NodeIntentRequest) (l1.Node, error)
	readNode         func(context.Context, connprofile.Profile) (l1.Node, error)
	readJob          func(context.Context, connprofile.Profile, string) (l1.Job, error)
	restartService   func(context.Context, connprofile.Profile, string, string) (l1.Job, error)
	recoverService   func(context.Context, connprofile.Profile, string, l1.ServiceRestartRequest) (l1.Job, error)
	interruptAttempt func(context.Context, connprofile.Profile, string, l1.InterruptRestartableAttemptRequest) (l1.Job, error)
	now              func() time.Time
	wait             func(context.Context, time.Duration) error
}

type upgradeDrainCounts struct {
	WaitingNonrestartable int `json:"waiting_nonrestartable"`
	StoppingRestartable   int `json:"stopping_restartable"`
	RestartPending        int `json:"restart_pending"`
}

type upgradeReport struct {
	upgradeDrainCounts
	VersionWarnings   []statusReason         `json:"version_warnings,omitempty"`
	Version           string                 `json:"version"`
	LeaseRenewal      time.Duration          `json:"lease_renewal_ns"`
	Setup             setupReport            `json:"setup"`
	RunningWork       []l1.NodeActiveAttempt `json:"running_work"`
	AlreadyAt         bool                   `json:"already_at"`
	RestartedServices []string               `json:"restarted_services,omitempty"`
	ClaimsDisabled    bool                   `json:"claims_remain_disabled,omitempty"`
}

func executeUpgrade(ctx context.Context, options globalOptions, args []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("upgrade", flag.ContinueOnError)
	flags.SetOutput(flagErrorOutput(stderr, options.jsonOutput))
	yes := flags.Bool("yes", false, "replace the node agent even when work is running; one-shot attempts end")
	drainTimeout := flags.Duration("drain-timeout", upgradeDrainTimeout, "wait at most this long for one-shot attempts to finish")
	if err := parseCommandFlags(flags, args, false); err != nil {
		return flagUsageError(err)
	}
	if flags.NArg() != 0 {
		return usageError("usage: wefty upgrade [--yes] [--drain-timeout DURATION]")
	}
	if *drainTimeout < upgradeDrainMinimum {
		return usageError("--drain-timeout must be at least 10s")
	}
	host, err := realSetupHost()
	if err != nil {
		return err
	}
	return executeUpgradeWithHost(ctx, host, options, *yes, stdout, stderr, *drainTimeout)
}

func executeUpgradeWithHost(ctx context.Context, host setupHost, options globalOptions, yes bool, stdout, stderr io.Writer, drainTimeout ...time.Duration) error {
	warnings := stderr
	if options.jsonOutput {
		warnings = io.Discard
	}
	report, err := runUpgrade(ctx, host, yes, warnings, drainTimeout...)
	if options.jsonOutput {
		if writeErr := writeJSON(stdout, report); writeErr != nil {
			return errors.Join(err, writeErr)
		}
		return err
	}
	if err != nil {
		return err
	}
	if report.AlreadyAt {
		return nil
	}
	nodeMessage := "Node agent replaced; services come back."
	if report.ClaimsDisabled {
		nodeMessage = "Node agent replaced; claims remain disabled; service restart requests wait for the operator to enable claims."
	}
	for _, service := range report.Setup.Services {
		if service.Label == (fleetServiceManager{host: host}).name(setupNodeAgentLabel) && service.Action == launchagent.Unchanged {
			nodeMessage = "Node agent unchanged."
		}
	}
	// The renewal line was already printed when it was observed, so a later
	// node agent failure still shows it. The summary does not repeat it.
	_, err = fmt.Fprintf(stdout, "Installed version: %s\n%s\nwaiting_nonrestartable=%d stopping_restartable=%d restart_pending=%d\n", report.Version, nodeMessage, report.WaitingNonrestartable, report.StoppingRestartable, report.RestartPending)
	return err
}

func upgradeOperations(host setupHost) upgradeHost {
	operations := upgradeHost{}
	if host.upgrade != nil {
		operations = *host.upgrade
	}
	if operations.remote == "" {
		operations.remote = upgradeRemote
	}
	if operations.command == nil {
		operations.command = runUpgradeCommand
	}
	if operations.build == nil {
		operations.build = func(ctx context.Context, source, destination, commit, loginPATH string) error {
			goBinary, err := findUpgradeTool(loginPATH, "go")
			if err != nil {
				return err
			}
			_, err = operations.command(ctx, source, upgradeBuildEnvironment(loginPATH), goBinary, "build", "-trimpath", "-ldflags=-X main.version="+commit, "-o", destination+string(filepath.Separator), "./cmd/wefty", "./cmd/wefty-l1", "./cmd/wefty-l3", "./cmd/wefty-agent")
			return err
		}
	}
	if operations.observe == nil {
		operations.observe = observeUpgradeNode
	}
	if operations.setClaims == nil {
		operations.setClaims = setUpgradeClaims
	}
	if operations.readNode == nil {
		operations.readNode = readUpgradeIntent
	}
	if operations.readJob == nil {
		operations.readJob = observeUpgradeJob
	}
	if operations.restartService == nil {
		operations.restartService = restartUpgradeService
	}
	if operations.recoverService == nil {
		operations.recoverService = recoverUpgradeService
	}
	if operations.interruptAttempt == nil {
		operations.interruptAttempt = interruptUpgradeAttempt
	}
	if operations.now == nil {
		operations.now = time.Now
	}
	if operations.wait == nil {
		operations.wait = waitForContext
	}
	return operations
}

func upgradeEnvironment(loginPATH string) []string {
	environment := slices.DeleteFunc(slices.Clone(os.Environ()), func(value string) bool { return strings.HasPrefix(value, "PATH=") })
	return append(environment, "PATH="+loginPATH)
}

func upgradeBuildEnvironment(loginPATH string) []string {
	environment := slices.DeleteFunc(upgradeEnvironment(loginPATH), func(value string) bool {
		key, _, _ := strings.Cut(value, "=")
		return slices.Contains([]string{"GOOS", "GOARCH", "GOFLAGS", "CGO_ENABLED"}, key)
	})
	return append(environment, "GOOS="+runtime.GOOS, "GOARCH="+runtime.GOARCH, "GOFLAGS=", "CGO_ENABLED=0")
}

func findUpgradeTool(loginPATH, name string) (string, error) {
	for _, directory := range filepath.SplitList(loginPATH) {
		if !filepath.IsAbs(directory) {
			continue
		}
		path := filepath.Join(directory, name)
		if info, err := os.Stat(path); err == nil && info.Mode().IsRegular() && info.Mode().Perm()&0o111 != 0 {
			return path, nil
		}
	}
	return "", fmt.Errorf("%s is missing from the node agent's recorded login-shell PATH; run wefty setup after installing it", name)
}

func runUpgradeCommand(ctx context.Context, directory string, environment []string, name string, args ...string) ([]byte, error) {
	command := exec.CommandContext(ctx, name, args...)
	command.Dir, command.Env, command.WaitDelay = directory, environment, time.Second
	output, err := command.Output()
	if err != nil {
		var exit *exec.ExitError
		if errors.As(err, &exit) {
			return nil, fmt.Errorf("%s failed: %w: %s", filepath.Base(name), err, strings.TrimSpace(string(exit.Stderr)))
		}
		return nil, err
	}
	return output, nil
}

func runUpgrade(ctx context.Context, host setupHost, yes bool, warnings io.Writer, drainTimeout ...time.Duration) (result upgradeReport, resultErr error) {
	if host.euid == 0 {
		return upgradeReport{}, errors.New("wefty upgrade must run as the owner, never as root")
	}
	if host.goos != "darwin" && host.goos != "linux" {
		return upgradeReport{}, fmt.Errorf("wefty upgrade supports macOS and Linux fleet installs; this machine runs %s", host.goos)
	}
	if !filepath.IsAbs(host.home) {
		return upgradeReport{}, errors.New("wefty upgrade needs an absolute home folder")
	}
	home, err := filepath.EvalSymlinks(host.home)
	if err != nil {
		return upgradeReport{}, err
	}
	host.home = home
	paths, issues := resolveFleetMaintenancePaths(home, host)
	if len(issues) > 0 {
		return upgradeReport{}, issues[0].err
	}
	if err := agentinstall.RefuseMaintenance(paths.data, "upgrade"); err != nil {
		return upgradeReport{}, err
	}
	role, err := selectUpgradeRole(paths)
	if err != nil {
		return upgradeReport{}, err
	}
	if role != "" {
		return runFleetUpgrade(ctx, host, paths, role, yes, warnings, drainTimeout...)
	}
	if host.goos == "linux" {
		return upgradeReport{}, errors.New("plain upgrade is Mac-only; on Linux use wefty setup --role control-plane or --role join --join-code CODE")
	}
	if err := refuseRecordedFleetRole(paths, "upgrade"); err != nil {
		return upgradeReport{}, err
	}
	unlock, err := lockSetup(paths, true)
	if err != nil {
		return result, err
	}
	defer unlock()
	if err := agentinstall.RefuseMaintenance(paths.data, "upgrade"); err != nil {
		return result, err
	}
	entries, err := os.ReadDir(paths.data)
	if err != nil {
		return result, err
	}
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), ".upgrade-") {
			if err := os.RemoveAll(filepath.Join(paths.data, entry.Name())); err != nil {
				return result, err
			}
		}
	}
	profile, err := connprofile.Read(paths.profile)
	if err != nil {
		return upgradeReport{}, fmt.Errorf("upgrade requires an installed stack; run wefty setup first: %w", err)
	}
	config, err := ocicontrol.ReadInstalledConfig(paths.nodeConfig)
	if err != nil {
		return upgradeReport{}, err
	}
	if profile.Fabric != "plain" || config.ControlSocket != "" || config.NodeID != profile.Identities.Node {
		return upgradeReport{}, errors.New("upgrade requires setup's plain-process node and matching connection profile")
	}
	operations := upgradeOperations(host)
	defer func() {
		resultErr = upgradeDrainRunHint(ctx, operations, profile, resultErr)
	}()
	if err := recoverUpgradeDrain(ctx, operations, paths, profile, warnings); err != nil {
		return result, err
	}
	plist, err := os.ReadFile(filepath.Join(paths.launchAgents, setupNodeAgentLabel+".plist"))
	if err != nil {
		return upgradeReport{}, err
	}
	loginPATH, err := recordedNodePATH(plist)
	if err != nil {
		return upgradeReport{}, err
	}
	var drain *upgradeDrainReceipt
	defer func() {
		if drain != nil {
			restoreErr := finishUpgradeDrain(ctx, operations, paths, profile, drain, false, warnings)
			resultErr = upgradeDrainResult(resultErr, restoreErr, !drain.claimsDisabled)
			result.RestartedServices = slices.Clone(drain.restarted)
			result.ClaimsDisabled = drain.claimsDisabled
			result.upgradeDrainCounts = drain.counts
		}
	}()
	// Refuse linked work folders, even links to another folder inside home.
	for _, path := range []string{paths.data, filepath.Join(paths.data, upgradeSourceName)} {
		if info, err := os.Lstat(path); err == nil && (!info.IsDir() || info.Mode()&os.ModeSymlink != 0) {
			return upgradeReport{}, fmt.Errorf("upgrade source path %s must be a real folder", path)
		} else if err != nil && !errors.Is(err, fs.ErrNotExist) {
			return upgradeReport{}, err
		}
	}
	stage, err := os.MkdirTemp(paths.data, ".upgrade-")
	if err != nil {
		return upgradeReport{}, err
	}
	defer func() {
		if err := os.RemoveAll(stage); err != nil {
			resultErr = errors.Join(resultErr, fmt.Errorf("remove upgrade staging folder %s: %w", stage, err))
		}
	}()
	source, destination := filepath.Join(stage, "source"), filepath.Join(stage, "binaries")
	for _, folder := range []string{source, destination} {
		if err := os.Mkdir(folder, 0o700); err != nil {
			return upgradeReport{}, err
		}
	}
	preparation, cancelPreparation := context.WithTimeout(ctx, 15*time.Minute)
	defer cancelPreparation()
	commit, err := fetchUpgradeSource(preparation, operations, paths, source, loginPATH, warnings)
	if err != nil {
		return upgradeReport{}, fmt.Errorf("upgrade fetch failed; installed stack unchanged: %w", err)
	}
	if err := operations.build(preparation, source, destination, commit, loginPATH); err != nil {
		return upgradeReport{}, fmt.Errorf("upgrade build failed; installed stack unchanged: %w", err)
	}
	for _, name := range setupBinaries {
		if _, err := executableDigest(filepath.Join(destination, name)); err != nil {
			return upgradeReport{}, err
		}
		output, err := operations.command(preparation, stage, upgradeEnvironment(loginPATH), filepath.Join(destination, name), "--version")
		if err != nil || strings.TrimSpace(string(output)) != commit {
			return upgradeReport{}, fmt.Errorf("upgrade version check failed for %s: expected %s, got %q (%v); installed stack unchanged", name, commit, strings.TrimSpace(string(output)), err)
		}
	}
	if upgradeAlreadyInstalled(paths, destination, commit) {
		_, err := fmt.Fprintln(warnings, "already at "+commit)
		return upgradeReport{Version: commit, AlreadyAt: true}, err
	}
	live := []string{filepath.Join(paths.data, "l1.sqlite"), filepath.Join(paths.data, "l3.sqlite")}
	for index, path := range live {
		temporary := filepath.Join(stage, fmt.Sprintf("preflight-%d.sqlite", index))
		if err := upgradestate.Snapshot(preparation, path, temporary); err != nil {
			return upgradeReport{}, fmt.Errorf("upgrade state snapshot failed; installed stack unchanged: %w", err)
		}
		name := []string{"wefty-l1", "wefty-l3"}[index]
		if _, err := operations.command(preparation, stage, upgradeEnvironment(loginPATH), filepath.Join(destination, name), "--state-preflight", "--db="+temporary); err != nil {
			return upgradeReport{}, fmt.Errorf("upgrade state preflight failed for %s; installed stack unchanged: %w", name, err)
		}
	}
	skillRoot, err := os.OpenRoot(filepath.Join(source, "skills"))
	if err != nil {
		return upgradeReport{}, fmt.Errorf("upgrade skill source: %w", err)
	}
	defer skillRoot.Close()
	baseline, err := operations.observe(ctx, profile)
	if err != nil {
		return result, upgradeObservationError(ctx, host, err)
	}
	result = upgradeReport{Version: commit, RunningWork: slices.Clone(baseline.ActiveAttempts)}
	var before []string
	var saved []setupFile
	activated := false
	nodeStageBegan := false
	written := map[string]string{}
	report := result
	var started, controlsRestarted time.Time
	var controlCtx context.Context
	var cancelControls context.CancelFunc
	defer func() {
		if cancelControls != nil {
			cancelControls()
		}
	}()
	request := setupRequest{from: destination, installedVersion: commit, skillSource: skillRoot.FS(), deferNodeBinary: true, deferSkills: true, lockHeld: true}
	request.wroteFile = func(name string, payload []byte) {
		if name != filepath.Join(paths.configRel, setupManifestName) {
			written[name] = payloadDigest(payload)
		}
	}
	request.beforeWrites = func() error {
		// Setup has validated all destinations and captured the login shell.
		// Only now close admission; build/preflight failures never drain.
		current, err := operations.observe(ctx, profile)
		if err != nil {
			return upgradeObservationError(ctx, host, err)
		}
		for _, attempt := range current.ActiveAttempts {
			recordUpgradeWork(&report.RunningWork, attempt)
		}
		bound := upgradeDrainTimeout
		if len(drainTimeout) > 0 {
			bound = drainTimeout[0]
		}
		var deadline time.Time
		current, deadline, err = waitUpgradeGap(ctx, operations, profile, current, yes, bound, warnings, &report.RunningWork, &report.upgradeDrainCounts)
		if err != nil {
			return err
		}
		drain, err = beginUpgradeDrain(ctx, operations, paths, profile, current, bound, deadline)
		if err != nil {
			return err
		}
		current, err = waitUpgradeDrain(ctx, operations, profile, drain, yes, bound, warnings, &report.RunningWork)
		if err != nil {
			return err
		}
		baseline = current
		for _, path := range live {
			value, err := upgradestate.Fingerprint(ctx, path)
			if err != nil {
				return err
			}
			before = append(before, value)
		}
		saved, err = saveUpgradeInstall(paths, request.skillSource)
		if err != nil {
			return err
		}
		activated = true
		return nil
	}
	request.serviceContext = func(index int) context.Context {
		if index == 0 {
			started = operations.now()
			controlCtx, cancelControls = context.WithTimeout(ctx, upgradeRenewalWindow)
		}
		if index < 2 {
			return controlCtx
		}
		nodeStageBegan = true
		return ctx
	}
	request.beforeService = func(index int) error {
		if index != 2 {
			return nil
		}
		current, err := operations.observe(ctx, profile)
		if err != nil {
			return err
		}
		for _, attempt := range current.ActiveAttempts {
			recordUpgradeWork(&report.RunningWork, attempt)
		}
		if err := checkUpgradeReplacement(current, drain, yes); err != nil {
			return err
		}
		drain.OldBoot = current.BootSessionID
		for _, attempt := range current.ActiveAttempts {
			if attempt.Class == contract.JobClassService {
				drain.Services = append(drain.Services, attempt)
			} else if yes {
				if _, err := fmt.Fprintf(warnings, "Ending one-shot job %s attempt %s (--yes).\n", attempt.JobID, attempt.AttemptID); err != nil {
					return err
				}
				if !slices.ContainsFunc(report.RunningWork, func(old l1.NodeActiveAttempt) bool { return old.AttemptID == attempt.AttemptID }) {
					report.RunningWork = append(report.RunningWork, attempt)
				}
			}
		}
		return writeUpgradeDrain(paths.config, drain)
	}
	request.afterService = func(index int, action launchagent.Action) error {
		if index == 1 {
			controlsRestarted = operations.now()
			elapsed, err := waitUpgradeRenewal(controlCtx, operations, profile, baseline, started, controlsRestarted)
			report.LeaseRenewal = elapsed
			if err != nil {
				return err
			}
			if _, err := fmt.Fprintf(warnings, "Successful node lease renewal after control-plane restart: %s (window %s).\n", elapsed, upgradeRenewalWindow); err != nil {
				return err
			}
		}
		if index == 2 {
			// Bootstrapping is not health. Require the replacement node agent to
			// register a new boot and heartbeat; the CLI also probes the run ledger.
			if err := waitUpgradeReplacement(ctx, operations, profile, baseline.BootSessionID, commit, action); err != nil {
				return err
			}
			return nil
		}
		return nil
	}
	report.Setup, err = runSetup(ctx, host, request)
	for _, note := range report.Setup.Notes {
		if _, writeErr := fmt.Fprintln(warnings, "Warning:", note); writeErr != nil {
			err = errors.Join(err, writeErr)
		}
	}
	if err == nil {
		return report, nil
	}
	if !activated {
		return report, err
	}
	recovery, cancelRecovery := context.WithTimeout(context.WithoutCancel(ctx), time.Duration(setupNodeAgentExitTimeout+90)*time.Second)
	defer cancelRecovery()
	recoveryErr := recoverUpgrade(recovery, host, paths, live, before, saved, nodeStageBegan, written)
	return report, fmt.Errorf("upgrade activation failed: %w; %v", err, recoveryErr)
}

func upgradeAlreadyInstalled(paths setupPaths, staged, commit string) bool {
	payload, err := os.ReadFile(filepath.Join(paths.config, setupManifestName))
	var receipt setupManifest
	if err != nil || json.Unmarshal(payload, &receipt) != nil || receipt.Version != 1 || receipt.InstalledVersion != commit {
		return false
	}
	for _, name := range setupBinaries {
		installed, err := executableDigest(filepath.Join(paths.bin, name))
		if err != nil || installed != receipt.Files[filepath.Join(paths.binRel, name)] {
			return false
		}
		built, err := executableDigest(filepath.Join(staged, name))
		if err != nil || built != installed {
			return false
		}
	}
	return true
}

func upgradeObservationError(ctx context.Context, host setupHost, cause error, fleet ...bool) error {
	if len(fleet) > 0 && fleet[0] {
		return fmt.Errorf("cannot list running work; check the local client service and fleet L1; repair the client service or start L1 on its control plane: %w", cause)
	}
	manager := launchagent.Manager{Run: host.launchctl, UID: host.uid}
	_, err := manager.Run(ctx, launchagent.Launchctl, "print", fmt.Sprintf("gui/%d/%s", host.uid, setupL1Label))
	if errors.Is(err, launchagent.ErrServiceNotFound) {
		return fmt.Errorf("L1 is down; run ~/.local/bin/wefty setup to start it: %w", cause)
	}
	return fmt.Errorf("cannot list running work; check L1; if L1 is down, run ~/.local/bin/wefty setup to start it: %w", cause)
}

func fetchUpgradeSource(ctx context.Context, operations upgradeHost, paths setupPaths, source, loginPATH string, warnings ...io.Writer) (string, error) {
	git, err := findUpgradeTool(loginPATH, "git")
	if err != nil {
		return "", err
	}
	clone := filepath.Join(paths.data, upgradeSourceName)
	environment := upgradeEnvironment(loginPATH)
	run := func(args ...string) ([]byte, error) {
		return operations.command(ctx, paths.data, environment, git, append([]string{"-c", "core.hooksPath=/dev/null"}, args...)...)
	}
	info, statErr := os.Lstat(clone)
	if statErr == nil {
		if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
			return "", fmt.Errorf("upgrade source path %s must be a real folder", clone)
		}
		output, err := run("--git-dir="+clone, "rev-parse", "--is-bare-repository")
		valid := err == nil && strings.TrimSpace(string(output)) == "true"
		if valid {
			_, err = run("--git-dir="+clone, "rev-parse", "--verify", "refs/heads/main^{commit}")
			valid = err == nil
		}
		if !valid {
			payload, readErr := os.ReadFile(filepath.Join(paths.config, setupManifestName))
			var receipt setupManifest
			if readErr != nil || json.Unmarshal(payload, &receipt) != nil || receipt.Version != 1 || !receipt.UpgradeSource {
				return "", fmt.Errorf("refusing to delete invalid source clone %s: setup receipt lacks upgrade_source ownership evidence; inspect the folder and move it aside before rerunning upgrade", clone)
			}
			for _, writer := range warnings {
				if _, err := fmt.Fprintf(writer, "Removing invalid wefty-owned source clone %s; re-cloning.\n", clone); err != nil {
					return "", err
				}
			}
			if err := os.RemoveAll(clone); err != nil {
				return "", err
			}
			statErr = fs.ErrNotExist
		}
	}
	if errors.Is(statErr, fs.ErrNotExist) {
		if _, err := run("clone", "--bare", "--single-branch", "--branch", "main", operations.remote, clone); err != nil {
			return "", err
		}
	} else if statErr != nil {
		return "", statErr
	}
	if _, err := run("--git-dir="+clone, "fetch", "--no-tags", operations.remote, "+refs/heads/main:refs/heads/main"); err != nil {
		return "", err
	}
	output, err := run("--git-dir="+clone, "rev-parse", "refs/heads/main^{commit}")
	if err != nil {
		return "", err
	}
	commit := strings.TrimSpace(string(output))
	if len(commit) != 40 && len(commit) != 64 {
		return "", errors.New("source did not report a full commit")
	}
	for _, ch := range commit {
		if !(ch >= '0' && ch <= '9' || ch >= 'a' && ch <= 'f') {
			return "", errors.New("source reported an invalid commit")
		}
	}
	archive, err := run("--git-dir="+clone, "archive", "--format=tar", commit)
	if err != nil {
		return "", err
	}
	root, err := os.OpenRoot(source)
	if err != nil {
		return "", err
	}
	defer root.Close()
	reader := tar.NewReader(bytes.NewReader(archive))
	for {
		entry, err := reader.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return "", err
		}
		name := filepath.FromSlash(strings.TrimSuffix(entry.Name, "/"))
		if !filepath.IsLocal(name) {
			return "", fmt.Errorf("unsafe source path %q", name)
		}
		switch entry.Typeflag {
		case tar.TypeXGlobalHeader:
			continue // git archive records its commit in a global PAX header.
		case tar.TypeDir:
			err = root.MkdirAll(name, 0o700)
		case tar.TypeReg:
			if err = root.MkdirAll(filepath.Dir(name), 0o700); err != nil {
				return "", err
			}
			var file *os.File
			file, err = root.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_EXCL, os.FileMode(entry.Mode)&0o777)
			if err == nil {
				_, err = io.Copy(file, reader)
				err = errors.Join(err, file.Close())
			}
		default:
			return "", fmt.Errorf("upgrade source contains unsupported linked or special entry %s", name)
		}
		if err != nil {
			return "", err
		}
	}
	return commit, nil
}

func observeUpgradeNode(ctx context.Context, profile connprofile.Profile) (l1.Node, error) {
	participant, closeFabric, err := fabricconfig.Open(ctx, fabricconfig.Config{Mode: "plain", PlainFabricID: profile.PlainFabricID, Identity: fabric.Identity{NodeID: profile.Identities.CLI, Tags: []string{l1.DefaultClientPrincipalTag}}})
	if closeFabric != nil {
		defer closeFabric()
	}
	if err != nil {
		return l1.Node{}, err
	}
	clients, err := newAPIClients(participant, profile.L1Address, profile.L3Address)
	if err != nil {
		return l1.Node{}, err
	}
	defer clients.close()
	probe, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	var node l1.Node
	if err := clients.l1.do(probe, http.MethodGet, "/v1/nodes/"+url.PathEscape(profile.Identities.Node), nil, nil, &node, http.StatusOK); err != nil {
		return node, err
	}
	var runs any
	if err := clients.l3.do(probe, http.MethodGet, "/v1/runs?limit=1", nil, nil, &runs, http.StatusOK); err != nil {
		return node, err
	}
	return node, nil
}

// Cancellation alone caps an attempt lease. Read its job through the existing
// public client instead of adding a persisted renewal timestamp to L1.
func observeUpgradeJob(ctx context.Context, profile connprofile.Profile, jobID string) (l1.Job, error) {
	participant, closeFabric, err := fabricconfig.Open(ctx, fabricconfig.Config{Mode: "plain", PlainFabricID: profile.PlainFabricID, Identity: fabric.Identity{NodeID: profile.Identities.CLI, Tags: []string{l1.DefaultClientPrincipalTag}}})
	if closeFabric != nil {
		defer closeFabric()
	}
	if err != nil {
		return l1.Job{}, err
	}
	clients, err := newAPIClients(participant, profile.L1Address, profile.L3Address)
	if err != nil {
		return l1.Job{}, err
	}
	defer clients.close()
	probe, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	var job l1.Job
	err = clients.l1.do(probe, http.MethodGet, "/v1/jobs/"+url.PathEscape(jobID), nil, nil, &job, http.StatusOK)
	return job, err
}

func waitUpgradeRenewal(ctx context.Context, operations upgradeHost, profile connprofile.Profile, baseline l1.Node, started, restarted time.Time) (time.Duration, error) {
	var lastErr error
	for operations.now().Sub(started) < upgradeRenewalWindow && ctx.Err() == nil {
		node, err := operations.observe(ctx, profile)
		if err == nil && node.BootSessionID == baseline.BootSessionID && node.AuthorityGeneration == baseline.AuthorityGeneration && node.LastHeartbeatAt.After(restarted) {
			renewed := true
			for _, old := range baseline.ActiveAttempts {
				for _, current := range node.ActiveAttempts {
					if current.AttemptID == old.AttemptID && (!current.LeaseExpiresAt.After(old.LeaseExpiresAt) || !current.LeaseExpiresAt.Add(-l1.DefaultLeaseDuration).After(restarted)) {
						job, readErr := observeUpgradeJob(ctx, profile, old.JobID)
						if readErr != nil || job.Outcome != contract.JobOutcomeCanceled {
							renewed = false
						}
						if readErr != nil {
							lastErr = readErr
						}
					}
				}
			}
			elapsed := operations.now().Sub(started)
			if renewed && elapsed < upgradeRenewalWindow && ctx.Err() == nil {
				return elapsed, nil
			}
		}
		if err != nil {
			lastErr = err
		}
		if err := operations.wait(ctx, 100*time.Millisecond); err != nil {
			lastErr = err
			break
		}
	}
	return operations.now().Sub(started), fmt.Errorf("no successful node lease renewal within %s after restarting L1 and the run ledger (last observation: %v)", upgradeRenewalWindow, lastErr)
}

func waitUpgradeReplacement(ctx context.Context, operations upgradeHost, profile connprofile.Profile, oldBoot, commit string, action launchagent.Action) error {
	if action == launchagent.Unchanged {
		return nil
	}
	budget, cancel := context.WithTimeout(ctx, upgradeReplacementBudget)
	defer cancel()
	started := operations.now()
	for operations.now().Sub(started) < upgradeReplacementBudget && budget.Err() == nil {
		node, err := operations.observe(budget, profile)
		if err == nil && node.BootSessionID != "" && node.BootSessionID != oldBoot && node.AgentVersion == commit && node.State == contract.NodeAlive && node.LastHeartbeatAt.After(started) {
			return nil
		}
		if err := operations.wait(budget, 100*time.Millisecond); err != nil {
			break
		}
	}
	return fmt.Errorf("replacement node agent did not become healthy within %s", upgradeReplacementBudget)
}

// Save installation files only. No database copy is ever kept for rollback.
func saveUpgradeInstall(paths setupPaths, skillSource fs.FS) ([]setupFile, error) {
	names := []string{filepath.Join(paths.configRel, connprofile.FileName), filepath.Join(paths.configRel, "node.json"), filepath.Join(paths.configRel, setupManifestName)}
	for _, name := range setupBinaries {
		names = append(names, filepath.Join(paths.binRel, name))
	}
	for _, label := range []string{setupL1Label, setupL3Label, setupNodeAgentLabel} {
		names = append(names, filepath.Join(paths.launchAgentsRel, label+".plist"))
	}
	for _, source := range []fs.FS{nil, skillSource} {
		files, err := setupSkillFilesFrom(paths, source)
		if err != nil {
			return nil, err
		}
		for _, file := range files {
			names = append(names, filepath.Join(file.folder, file.name))
		}
	}
	root, err := os.OpenRoot(paths.home)
	if err != nil {
		return nil, err
	}
	defer root.Close()
	var saved []setupFile
	slices.Sort(names)
	for index, name := range names {
		if index > 0 && name == names[index-1] {
			continue
		}
		info, err := root.Lstat(name)
		if errors.Is(err, fs.ErrNotExist) {
			saved = append(saved, setupFile{folder: filepath.Dir(name), name: filepath.Base(name)})
			continue
		}
		if err != nil {
			return nil, err
		}
		if !info.Mode().IsRegular() {
			return nil, fmt.Errorf("upgrade refuses non-regular installed file %s", name)
		}
		payload, err := root.ReadFile(name)
		if err != nil {
			// Setup repairs an unreadable plain receipt; it is not a rollback
			// authority. Every other installed file still requires a snapshot.
			if name == filepath.Join(paths.configRel, setupManifestName) && canRepairPlainSetupReceipt(paths) {
				continue
			}
			return nil, err
		}
		saved = append(saved, setupFile{folder: filepath.Dir(name), name: filepath.Base(name), payload: payload, mode: info.Mode().Perm()})
	}
	return saved, nil
}

func recoverUpgrade(ctx context.Context, host setupHost, paths setupPaths, live, before []string, saved []setupFile, nodeStageBegan bool, written map[string]string) error {
	manager := launchagent.Manager{Run: host.launchctl, UID: host.uid, UnloadBound: time.Duration(setupNodeAgentExitTimeout+15) * time.Second}
	// Quiesce database writers. Preserve the old node agent unless replacement began.
	var stopErrors []error
	labels := []string{setupL3Label, setupL1Label}
	if nodeStageBegan {
		labels = append([]string{setupNodeAgentLabel}, labels...)
	}
	for _, label := range labels {
		if err := manager.Unload(ctx, label); err != nil {
			stopErrors = append(stopErrors, err)
		}
	}
	if len(stopErrors) != 0 {
		return fmt.Errorf("could not stop the stack; rollback refused: %w; receipt refresh: %v; run the newly installed ~/.local/bin/wefty setup to bring the new version up", errors.Join(stopErrors...), refreshPartialUpgradeReceipt(paths, written))
	}
	for index, path := range live {
		after, err := upgradestate.Fingerprint(ctx, path)
		if err != nil || after != before[index] {
			receiptErr := refreshPartialUpgradeReceipt(paths, written)
			return fmt.Errorf("L1 and the run ledger stopped; live database format changed or could not be read; old binaries were not restored; databases were not restored (fingerprint error: %v; receipt: %v); run the newly installed ~/.local/bin/wefty setup to bring the new version up", err, receiptErr)
		}
	}
	root, err := os.OpenRoot(paths.home)
	if err != nil {
		return err
	}
	defer root.Close()
	for _, file := range saved {
		if file.mode == 0 {
			if err := root.Remove(filepath.Join(file.folder, file.name)); err != nil && !errors.Is(err, fs.ErrNotExist) {
				return err
			}
			continue
		}
		if _, err := convergeHomeFile(root, file); err != nil {
			return fmt.Errorf("binary rollback failed; stack stopped: %w", err)
		}
	}
	folder, err := root.OpenRoot(paths.launchAgentsRel)
	if err != nil {
		return err
	}
	defer folder.Close()
	for _, label := range []string{setupL1Label, setupL3Label, setupNodeAgentLabel} {
		payload, err := folder.ReadFile(label + ".plist")
		if err != nil {
			return err
		}
		if _, err := manager.Converge(ctx, folder, filepath.Join(paths.launchAgents, label+".plist"), label, payload); err != nil {
			return fmt.Errorf("old binary restart failed: %w", err)
		}
	}
	return errors.New("schema fingerprints unchanged; old installation files restored and services reloaded; live databases preserved")
}

func refreshPartialUpgradeReceipt(paths setupPaths, written map[string]string) error {
	root, err := os.OpenRoot(paths.home)
	if err != nil {
		return err
	}
	defer root.Close()
	payload, err := root.ReadFile(filepath.Join(paths.configRel, setupManifestName))
	if err != nil {
		return err
	}
	var manifest setupManifest
	if err := json.Unmarshal(payload, &manifest); err != nil {
		return err
	}
	manifest.InstalledVersion = ""
	manifest.UpgradeSource = true
	if manifest.Files == nil {
		manifest.Files = map[string]string{}
	}
	for name, digest := range written {
		manifest.Files[name] = digest
	}
	payload, err = json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return err
	}
	_, err = convergeHomeFile(root, setupFile{folder: paths.configRel, name: setupManifestName, payload: append(payload, '\n'), mode: 0o600})
	return err
}

func installedStackVersion(nodeConfigPath string) string {
	if nodeConfigPath == "" {
		nodeConfigPath = defaultNodeConfigPath()
	}
	if nodeConfigPath == "" {
		return ""
	}
	path := filepath.Join(filepath.Dir(nodeConfigPath), setupManifestName)
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() || info.Size() > 1<<20 {
		return ""
	}
	payload, err := os.ReadFile(path)
	if err != nil {
		return ""
	}
	var manifest setupManifest
	if json.Unmarshal(payload, &manifest) != nil || manifest.Version != 1 {
		return ""
	}
	return manifest.InstalledVersion
}
