//go:build darwin

package lima

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

type launchDaemonInstaller struct {
	run  commandRunner
	glob func(string) ([]string, error)
	stat func(string) (os.FileInfo, error)
}

func InstallLaunchDaemon(ctx context.Context, config LaunchDaemonConfig) error {
	return (launchDaemonInstaller{run: runCommand, glob: filepath.Glob}).install(ctx, config)
}

func (installer launchDaemonInstaller) install(ctx context.Context, config LaunchDaemonConfig) error {
	if err := ValidateLaunchDaemonInstall(config); err != nil {
		return err
	}
	payload, _ := RenderLaunchDaemon(config)
	if installer.run == nil {
		installer.run = runCommand
	}
	if installer.stat == nil {
		installer.stat = os.Stat
	}
	if installer.glob == nil {
		installer.glob = filepath.Glob
	}
	patterns := []string{
		"/Library/LaunchDaemons/io.lima-vm.daemon.*.plist",
		"/Library/LaunchAgents/io.lima-vm.autostart.*.plist",
		filepath.Join(config.Home, "Library/LaunchAgents/io.lima-vm.autostart.*.plist"),
	}
	for _, pattern := range patterns {
		autostart, err := installer.glob(pattern)
		if err != nil {
			return fmt.Errorf("inspect Lima autostart units: %w", err)
		}
		if len(autostart) != 0 {
			return errors.New("Lima autostart unit is installed; disable it before installing dev.wefty.agent")
		}
	}
	systemDomain, err := installer.run(ctx, "/bin/launchctl", "print", "system")
	if err != nil {
		return errors.New("cannot inspect the launchd system domain for Lima autostart")
	}
	if containsLimaAutostart(systemDomain) {
		return errors.New("Lima autostart unit is loaded; disable it before installing dev.wefty.agent")
	}
	uid, err := installer.run(ctx, "/usr/bin/id", "-u", config.OperatorUser)
	if err != nil || strings.TrimSpace(string(uid)) == "" {
		return errors.New("cannot resolve the LaunchDaemon operator UID")
	}
	for _, domain := range []string{"user/" + strings.TrimSpace(string(uid)), "gui/" + strings.TrimSpace(string(uid))} {
		loaded, printErr := installer.run(ctx, "/bin/launchctl", "print", domain)
		if printErr == nil && containsLimaAutostart(loaded) {
			return errors.New("Lima login autostart unit is loaded; disable it before installing dev.wefty.agent")
		}
	}
	temporary, err := os.CreateTemp("", "dev.wefty.agent-*.plist")
	if err != nil {
		return fmt.Errorf("stage LaunchDaemon: %w", err)
	}
	temporaryPath := temporary.Name()
	defer os.Remove(temporaryPath)
	if err := temporary.Chmod(0o600); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("secure staged LaunchDaemon: %w", err)
	}
	if _, err := temporary.Write(payload); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("write staged LaunchDaemon: %w", err)
	}
	if err := temporary.Close(); err != nil {
		return fmt.Errorf("close staged LaunchDaemon: %w", err)
	}
	if _, err := installer.run(ctx, "/usr/bin/sudo", "/usr/bin/install", "-o", "root", "-g", "wheel", "-m", "0644", temporaryPath, LaunchDaemonPath); err != nil {
		return fmt.Errorf("install LaunchDaemon: %w", err)
	}
	label := "system/" + LaunchDaemonLabel
	if _, printErr := installer.run(ctx, "/bin/launchctl", "print", label); printErr == nil {
		if _, err := installer.run(ctx, "/usr/bin/sudo", "/bin/launchctl", "bootout", label); err != nil {
			return fmt.Errorf("unload prior LaunchDaemon: %w", err)
		}
	}
	if _, err := installer.run(ctx, "/usr/bin/sudo", "/bin/launchctl", "bootstrap", "system", LaunchDaemonPath); err != nil {
		return fmt.Errorf("load LaunchDaemon: %w", err)
	}
	if _, err := installer.run(ctx, "/usr/bin/sudo", "/bin/launchctl", "kickstart", "-k", label); err != nil {
		return fmt.Errorf("start LaunchDaemon: %w", err)
	}
	return nil
}

func containsLimaAutostart(payload []byte) bool {
	text := string(payload)
	return strings.Contains(text, "io.lima-vm.daemon.") || strings.Contains(text, "io.lima-vm.autostart.")
}

func RemoveLaunchDaemon(ctx context.Context) (LaunchDaemonRemovalEvidence, error) {
	return (launchDaemonInstaller{run: runCommand, glob: filepath.Glob}).remove(ctx)
}

func (installer launchDaemonInstaller) remove(ctx context.Context) (LaunchDaemonRemovalEvidence, error) {
	if installer.run == nil {
		installer.run = runCommand
	}
	if installer.stat == nil {
		installer.stat = os.Stat
	}
	label := "system/" + LaunchDaemonLabel
	evidence := LaunchDaemonRemovalEvidence{Label: LaunchDaemonLabel, Unloaded: true}
	if _, err := installer.run(ctx, "/bin/launchctl", "print", label); err == nil {
		if _, err := installer.run(ctx, "/usr/bin/sudo", "/bin/launchctl", "bootout", label); err != nil {
			return evidence, fmt.Errorf("unload LaunchDaemon: %w", err)
		}
	}
	if _, err := installer.run(ctx, "/usr/bin/sudo", "/bin/rm", "-f", LaunchDaemonPath); err != nil {
		return evidence, fmt.Errorf("remove LaunchDaemon: %w", err)
	}
	_, loadedErr := installer.run(ctx, "/bin/launchctl", "print", label)
	evidence.Unloaded = loadedErr != nil
	_, statErr := installer.stat(LaunchDaemonPath)
	evidence.PlistAbsent = errors.Is(statErr, os.ErrNotExist)
	if !evidence.Unloaded || !evidence.PlistAbsent {
		return evidence, errors.New("LaunchDaemon removal verification failed")
	}
	return evidence, nil
}
