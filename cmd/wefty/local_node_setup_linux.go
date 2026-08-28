//go:build linux

package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/user"
	"strconv"
	"strings"
	"time"

	"github.com/Derek-X-Wang/wefty/runner/lima"
	"github.com/Derek-X-Wang/wefty/runner/linuxunit"
	"github.com/Derek-X-Wang/wefty/runner/ocicontrol"
)

func maybeExecutePrivilegedLinuxSetup(ctx context.Context, options globalOptions, args []string, stdout, stderr io.Writer) (bool, error) {
	if args[0] != "setup-oci" {
		return false, nil
	}
	if os.Geteuid() != 0 {
		return true, errors.New("Linux OCI setup must run with privilege; see " + ocicontrol.RunbookPath)
	}
	flags := flag.NewFlagSet("node setup-oci", flag.ContinueOnError)
	flags.SetOutput(stderr)
	agentPath := flags.String("agent-path", "/usr/local/libexec/wefty-agent", "installed Linux wefty-agent executable")
	operatorUser := flags.String("operator-user", os.Getenv("SUDO_USER"), "unprivileged agent user")
	workingDirectory := flags.String("working-directory", "/var/lib/wefty", "agent working directory")
	unitDirectory := flags.String("unit-directory", "/etc/systemd/system", "systemd unit directory")
	nodeID := flags.String("node-id", "wefty-linux", "stable node ID")
	helperChecksum := flags.String("helper-checksum", "", "installed agent sha256 checksum")
	probeReference := flags.String("probe-reference", "", "pinned probe image reference")
	probeDigest := flags.String("probe-digest", "", "pinned probe top-level digest")
	probeArchive := flags.String("probe-archive", "", "absolute probe OCI archive")
	installManifest := flags.String("install-manifest", "", "release OCI install manifest (defaults next to the wefty binary)")
	allowedMountRoot := flags.String("allowed-mount-root", "/srv/wefty", "absolute operator mount root")
	intentPath := flags.String("intent-file", "/var/lib/wefty/oci-intent.json", "durable OCI intent")
	setupStatePath := flags.String("setup-state", "/var/lib/wefty/oci-setup.json", "durable setup convergence state")
	applyRestart := flags.Bool("apply-restart", false, "print the required agent restart after a restart-required change")
	recreate := flags.Bool("recreate", false, "authorize a recreate-required change with zero live attempts")
	defaults, err := lima.HostDefaultSizing()
	if err != nil {
		return true, err
	}
	sizing := lima.BindSizingFlags(flags, defaults)
	if err := flags.Parse(args[1:]); err != nil {
		return true, err
	}
	if flags.NArg() != 0 {
		return true, usageError("wefty node setup-oci does not accept positional arguments")
	}
	missing := func(name string) (bool, error) {
		_, err := fmt.Fprintf(stdout, "OCI setup not applied: missing %s; see %s\n", name, ocicontrol.RunbookPath)
		return true, err
	}
	if *helperChecksum == "" || *probeReference == "" || *probeDigest == "" || *probeArchive == "" {
		manifestPath := *installManifest
		if manifestPath == "" {
			executable, executableErr := os.Executable()
			if executableErr != nil {
				return missing("install_manifest")
			}
			manifestPath, err = ocicontrol.DefaultOCIInstallManifestPath(executable)
			if err != nil {
				return missing("install_manifest")
			}
		}
		manifest, manifestErr := ocicontrol.ReadOCIInstallManifest(manifestPath)
		if manifestErr != nil {
			return missing("install_manifest")
		}
		if *helperChecksum == "" {
			*helperChecksum = manifest.HelperChecksum
		}
		if *probeReference == "" {
			*probeReference = manifest.ProbeReference
		}
		if *probeDigest == "" {
			*probeDigest = manifest.ProbeDigest
		}
		if *probeArchive == "" {
			*probeArchive = manifest.ProbeArchivePath
		}
	}
	if *operatorUser == "" || *helperChecksum == "" || *probeReference == "" || *probeDigest == "" || *probeArchive == "" {
		return missing("operator_or_probe_configuration")
	}
	for name, path := range map[string]string{"agent": *agentPath, "probe_archive": *probeArchive} {
		info, statErr := os.Stat(path)
		if statErr != nil || !info.Mode().IsRegular() {
			return missing(name)
		}
	}
	executablePaths := make(map[string]string)
	for _, executable := range []string{"containerd", "runc", "systemctl", "groupadd", "usermod", "getent", "id"} {
		resolvedPath, err := exec.LookPath(executable)
		if err != nil {
			return missing(executable)
		}
		executablePaths[executable] = resolvedPath
	}
	operator, err := user.Lookup(*operatorUser)
	if err != nil {
		return true, err
	}
	uid, uidErr := strconv.Atoi(operator.Uid)
	gid, gidErr := strconv.Atoi(operator.Gid)
	group, groupErr := user.LookupGroupId(operator.Gid)
	if uidErr != nil || gidErr != nil || groupErr != nil || uid == 0 || gid == 0 {
		return true, errors.New("Linux OCI operator must have a numeric non-root UID and GID")
	}
	nodeConfig, err := ocicontrol.DefaultInstalledConfigPath(operator.HomeDir)
	if err != nil {
		return true, err
	}
	resolved, err := sizing.Sizing()
	if err != nil {
		return true, err
	}
	desired := ocicontrol.SetupState{VMMemory: resolved.Memory, VMCPUs: resolved.CPUs, VMDisk: resolved.Disk, VMType: "native", HostMountRoot: *allowedMountRoot, ProbeDigest: *probeDigest}
	class := ocicontrol.ConvergenceLiveSafe
	if current, readErr := ocicontrol.ReadSetupState(*setupStatePath); readErr == nil {
		class = ocicontrol.ClassifyConvergence(current, desired)
	} else if !errors.Is(readErr, os.ErrNotExist) {
		return true, readErr
	}
	controlSocket := "/run/wefty-agent/control.sock"
	agentArguments := []string{
		"--node-id=" + *nodeID, "--oci-helper-socket=" + linuxunit.HelperSocketPath,
		"--oci-helper-checksum=" + *helperChecksum, "--oci-probe-image=" + *probeReference,
		"--oci-probe-digest=" + *probeDigest, "--oci-probe-archive=" + *probeArchive,
		"--oci-intent-file=" + *intentPath, "--oci-setup-state=" + *setupStatePath,
		"--oci-control-socket=" + controlSocket,
	}
	receipt, err := linuxunit.Configure(ctx, linuxunit.Config{
		AgentPath: *agentPath, AgentArguments: agentArguments, OperatorUser: operator.Username, OperatorGroup: group.Name,
		OperatorUID: uid, OperatorGID: gid, WorkingDirectory: *workingDirectory,
		AllowedMountRoots: []string{*allowedMountRoot}, ContainerdAddress: "/run/containerd/containerd.sock",
		ContainerdStateRoot: "/run/containerd", RuntimeRoot: "/var/lib/wefty/oci", RuncExecutable: executablePaths["runc"],
	}, linuxunit.ConfigurePaths{UnitDirectory: *unitDirectory, NodeConfig: nodeConfig, ControlSocket: controlSocket}, linuxunit.ExecRunner{})
	if err != nil {
		return true, err
	}
	if err := ocicontrol.WriteSetupState(ocicontrol.DesiredSetupStatePath(*setupStatePath), desired); err != nil {
		return true, err
	}
	if err := ocicontrol.AuthorizeConvergence(class, *applyRestart, *recreate, 0); err != nil {
		if options.jsonOutput {
			var controlErr *ocicontrol.ControlError
			reasonCode := "setup_convergence_required"
			if errors.As(err, &controlErr) {
				reasonCode = string(controlErr.Code)
			}
			return true, writeJSON(stdout, map[string]any{"configured": true, "convergence": class, "reason_code": reasonCode, "systemctl_commands": receipt.Commands})
		}
		_, printErr := fmt.Fprintf(stdout, "OCI setup configured; convergence=%s reason=%v; rerun with the required convergence flag\n", class, err)
		return true, printErr
	}
	if err := ocicontrol.WriteSetupState(*setupStatePath, desired); err != nil {
		return true, err
	}
	if _, err := lima.InitializeOCIIntent(*intentPath, time.Now()); err != nil {
		return true, err
	}
	if (class == ocicontrol.ConvergenceRestartRequired && *applyRestart) || (class == ocicontrol.ConvergenceRecreateRequired && *recreate) {
		receipt.Commands[len(receipt.Commands)-1] = []string{"systemctl", "restart", linuxunit.AgentUnit}
	}
	if options.jsonOutput {
		return true, writeJSON(stdout, map[string]any{"configured": true, "convergence": class, "systemctl_commands": receipt.Commands})
	}
	for _, command := range receipt.Commands {
		if _, err := fmt.Fprintln(stdout, strings.Join(command, " ")); err != nil {
			return true, err
		}
	}
	_, err = fmt.Fprintf(stdout, "OCI setup configured (%s); run the commands above to converge services\n", class)
	return true, err
}
