package linuxunit

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/Derek-X-Wang/wefty/runner/ocicontrol"
)

type ConfigurePaths struct {
	UnitDirectory string
	NodeConfig    string
	ControlSocket string
	chown         func(string, int, int) error
}

type ConfigureReceipt struct {
	GroupCreated   bool       `json:"group_created"`
	UserAdded      bool       `json:"user_added"`
	Commands       [][]string `json:"systemctl_commands"`
	SystemdVersion int        `json:"systemd_version"`
	RestartPolicy  string     `json:"helper_restart_policy"`
}

type CommandRunner interface {
	Run(context.Context, string, ...string) ([]byte, error)
}

type ExecRunner struct{}

func (ExecRunner) Run(ctx context.Context, command string, arguments ...string) ([]byte, error) {
	return exec.CommandContext(ctx, command, arguments...).CombinedOutput()
}

func Configure(ctx context.Context, config Config, paths ConfigurePaths, runner CommandRunner) (ConfigureReceipt, error) {
	if runner == nil || !filepath.IsAbs(paths.UnitDirectory) || !filepath.IsAbs(paths.NodeConfig) || !filepath.IsAbs(paths.ControlSocket) {
		return ConfigureReceipt{}, fmt.Errorf("Linux OCI setup requires absolute install paths and a command runner")
	}
	versionOutput, err := runner.Run(ctx, "systemctl", "--version")
	if err != nil {
		return ConfigureReceipt{}, fmt.Errorf("inspect systemd version: %w", err)
	}
	fields := strings.Fields(string(versionOutput))
	if len(fields) < 2 || fields[0] != "systemd" {
		return ConfigureReceipt{}, fmt.Errorf("inspect systemd version: unexpected output")
	}
	config.SystemdVersion, err = strconv.Atoi(fields[1])
	if err != nil {
		return ConfigureReceipt{}, fmt.Errorf("inspect systemd version: %w", err)
	}
	units, err := Render(config)
	if err != nil {
		return ConfigureReceipt{}, err
	}
	receipt := ConfigureReceipt{SystemdVersion: config.SystemdVersion, RestartPolicy: "legacy_fixed_1s"}
	if config.SystemdVersion >= 254 {
		receipt.RestartPolicy = "geometric_capped_1s"
	}
	if paths.chown == nil {
		paths.chown = os.Chown
	}
	if _, err := runner.Run(ctx, "getent", "group", HelperGroup); err != nil {
		if output, createErr := runner.Run(ctx, "groupadd", "--system", HelperGroup); createErr != nil {
			return receipt, fmt.Errorf("create Linux OCI helper group: %s: %w", strings.TrimSpace(string(output)), createErr)
		}
		receipt.GroupCreated = true
	}
	groups, err := runner.Run(ctx, "id", "-nG", config.OperatorUser)
	if err != nil {
		return receipt, fmt.Errorf("inspect Linux agent group membership: %w", err)
	}
	if !containsWord(string(groups), HelperGroup) {
		if output, addErr := runner.Run(ctx, "usermod", "-a", "-G", HelperGroup, config.OperatorUser); addErr != nil {
			return receipt, fmt.Errorf("add Linux agent to OCI helper group: %s: %w", strings.TrimSpace(string(output)), addErr)
		}
		receipt.UserAdded = true
	}
	for name, payload := range map[string][]byte{AgentUnit: units.Agent, HelperSocketUnit: units.HelperSocket, HelperServiceUnit: units.HelperService} {
		if err := writeUnit(filepath.Join(paths.UnitDirectory, name), payload); err != nil {
			return receipt, err
		}
	}
	if err := ocicontrol.WriteInstalledConfig(paths.NodeConfig, ocicontrol.InstalledConfig{Version: ocicontrol.InstalledConfigVersion, ControlSocket: paths.ControlSocket}); err != nil {
		return receipt, err
	}
	if err := paths.chown(filepath.Dir(paths.NodeConfig), config.OperatorUID, config.OperatorGID); err != nil {
		return receipt, err
	}
	if err := paths.chown(filepath.Dir(filepath.Dir(paths.NodeConfig)), config.OperatorUID, config.OperatorGID); err != nil {
		return receipt, err
	}
	if err := paths.chown(paths.NodeConfig, config.OperatorUID, config.OperatorGID); err != nil {
		return receipt, err
	}
	receipt.Commands = ReconcileCommands(receipt.UserAdded)
	return receipt, nil
}

func containsWord(value, word string) bool {
	for _, candidate := range strings.Fields(value) {
		if candidate == word {
			return true
		}
	}
	return false
}

func writeUnit(path string, payload []byte) error {
	temporary, err := os.CreateTemp(filepath.Dir(path), ".wefty-unit-*")
	if err != nil {
		return err
	}
	temporaryPath := temporary.Name()
	defer func() { _ = os.Remove(temporaryPath) }()
	if err := temporary.Chmod(0o644); err != nil {
		_ = temporary.Close()
		return err
	}
	if _, err := temporary.Write(payload); err != nil {
		_ = temporary.Close()
		return err
	}
	if err := temporary.Sync(); err != nil {
		_ = temporary.Close()
		return err
	}
	if err := temporary.Close(); err != nil {
		return err
	}
	return os.Rename(temporaryPath, path)
}
