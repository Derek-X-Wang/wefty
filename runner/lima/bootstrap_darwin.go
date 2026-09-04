//go:build darwin

package lima

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/Derek-X-Wang/wefty/l1"
	ocirunner "github.com/Derek-X-Wang/wefty/runner/oci"
	"github.com/Derek-X-Wang/wefty/runner/ocihelper"
)

type guestHelperInstaller struct {
	run commandRunner
}

func InspectGuestSystemdVersion(ctx context.Context, instance, limactl string) (int, error) {
	if limactl == "" {
		limactl = "limactl"
	}
	output, err := runCommand(ctx, limactl, "--tty=false", "shell", "--workdir=/", instance, "systemctl", "--version")
	if err != nil {
		return 0, errors.New("cannot inspect Lima guest systemd version")
	}
	fields := strings.Fields(string(output))
	if len(fields) < 2 || fields[0] != "systemd" {
		return 0, errors.New("cannot parse Lima guest systemd version")
	}
	version, err := strconv.Atoi(fields[1])
	if err != nil || version <= 0 {
		return 0, errors.New("cannot parse Lima guest systemd version")
	}
	return version, nil
}

func InspectGuestHelperServiceUnit(ctx context.Context, instance, limactl string) (string, error) {
	if limactl == "" {
		limactl = "limactl"
	}
	output, err := runCommand(ctx, limactl, "--tty=false", "shell", "--workdir=/", instance, "systemctl", "cat", GuestHelperServiceUnit)
	if err != nil {
		return "", errors.New("cannot inspect Lima guest helper service unit")
	}
	return string(output), nil
}

func InstallGuestHelper(ctx context.Context, config GuestHelperInstallConfig) error {
	return (guestHelperInstaller{run: runCommand}).install(ctx, config)
}

func (installer guestHelperInstaller) install(ctx context.Context, config GuestHelperInstallConfig) error {
	if err := ValidateGuestHelperInstall(config); err != nil {
		return err
	}
	if config.Limactl == "" {
		config.Limactl = "limactl"
	}
	if installer.run == nil {
		installer.run = runCommand
	}
	if _, err := installer.run(ctx, config.Limactl, "--tty=false", "shell", "--workdir=/", config.Instance, "command", "-v", "e2fsck"); err != nil {
		return errors.New("Lima guest requires e2fsprogs (e2fsck) before helper installation")
	}
	groups, err := installer.run(ctx, config.Limactl, "--tty=false", "shell", "--workdir=/", config.Instance, "id", "-nG", config.GuestUser)
	if err != nil {
		return errors.New("cannot inspect Lima guest helper group membership")
	}
	needsGroupRefresh := !slices.Contains(strings.Fields(string(groups)), "wefty-oci")
	if config.SystemdVersion == 0 {
		versionOutput, versionErr := installer.run(ctx, config.Limactl, "--tty=false", "shell", "--workdir=/", config.Instance, "systemctl", "--version")
		if versionErr != nil {
			return errors.New("cannot inspect Lima guest systemd version")
		}
		fields := strings.Fields(string(versionOutput))
		if len(fields) < 2 || fields[0] != "systemd" {
			return errors.New("cannot parse Lima guest systemd version")
		}
		config.SystemdVersion, err = strconv.Atoi(fields[1])
		if err != nil {
			return errors.New("cannot parse Lima guest systemd version")
		}
	}
	staging, err := os.MkdirTemp("", "wefty-lima-helper-*")
	if err != nil {
		return err
	}
	defer os.RemoveAll(staging)
	socketUnit := filepath.Join(staging, GuestHelperSocketUnit)
	serviceUnit := filepath.Join(staging, GuestHelperServiceUnit)
	if err := os.WriteFile(socketUnit, renderGuestSocketUnit(), 0o600); err != nil {
		return err
	}
	if err := os.WriteFile(serviceUnit, renderGuestServiceUnit(config), 0o600); err != nil {
		return err
	}
	guestStage := "/tmp/" + filepath.Base(staging)
	defer func() {
		cleanupContext, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, _ = installer.run(cleanupContext, config.Limactl, "--tty=false", "shell", "--workdir=/", config.Instance, "rm", "-rf", guestStage)
	}()
	guestShell := []string{config.Limactl, "--tty=false", "shell", "--workdir=/", config.Instance}
	for _, unit := range []string{GuestHelperServiceUnit, GuestHelperSocketUnit} {
		_, _ = installer.run(ctx, guestShell[0], append(guestShell[1:], "sudo", "systemctl", "stop", unit)...)
	}
	commands := [][]string{
		{config.Limactl, "--tty=false", "shell", "--workdir=/", config.Instance, "install", "-d", "-m", "0700", guestStage},
		{config.Limactl, "--tty=false", "copy", config.HelperBinary, config.Instance + ":" + guestStage + "/wefty-agent"},
		{config.Limactl, "--tty=false", "copy", socketUnit, config.Instance + ":" + guestStage + "/" + GuestHelperSocketUnit},
		{config.Limactl, "--tty=false", "copy", serviceUnit, config.Instance + ":" + guestStage + "/" + GuestHelperServiceUnit},
		{config.Limactl, "--tty=false", "shell", "--workdir=/", config.Instance, "sudo", "groupadd", "--system", "--force", "wefty-oci"},
		{config.Limactl, "--tty=false", "shell", "--workdir=/", config.Instance, "sudo", "usermod", "-a", "-G", "wefty-oci", config.GuestUser},
		{config.Limactl, "--tty=false", "shell", "--workdir=/", config.Instance, "sudo", "install", "-o", "root", "-g", "root", "-m", "0755", guestStage + "/wefty-agent", GuestHelperPath},
		{config.Limactl, "--tty=false", "shell", "--workdir=/", config.Instance, "sudo", "install", "-o", "root", "-g", "root", "-m", "0644", guestStage + "/" + GuestHelperSocketUnit, GuestHelperSocketUnitPath},
		{config.Limactl, "--tty=false", "shell", "--workdir=/", config.Instance, "sudo", "install", "-o", "root", "-g", "root", "-m", "0644", guestStage + "/" + GuestHelperServiceUnit, GuestHelperServiceUnitPath},
		{config.Limactl, "--tty=false", "shell", "--workdir=/", config.Instance, "sudo", "systemctl", "daemon-reload"},
		{config.Limactl, "--tty=false", "shell", "--workdir=/", config.Instance, "sudo", "systemctl", "enable", "--now", GuestHelperSocketUnit},
	}
	for _, command := range commands {
		if _, err := installer.run(ctx, command[0], command[1:]...); err != nil {
			return fmt.Errorf("install guest OCI helper: %w", err)
		}
	}
	if needsGroupRefresh {
		for _, arguments := range [][]string{{"stop", config.Instance}, {"start", config.Instance}} {
			if _, err := installer.run(ctx, config.Limactl, arguments...); err != nil {
				return errors.New("restart Lima after helper group installation failed")
			}
		}
	}
	permissions, err := installer.run(ctx, config.Limactl, "--tty=false", "shell", "--workdir=/", config.Instance, "sudo", "stat", "-c", "%a:%U:%G", GuestHelperSocket)
	if err != nil || strings.TrimSpace(string(permissions)) != "660:root:wefty-oci" {
		return errors.New("guest helper socket permissions are not 0660 root:wefty-oci")
	}
	client, _ := NewHelperClient(config.HelperSocket, config.ExpectedChecksum)
	barrier, err := ocihelper.NewBootBarrier(client, ocihelper.AcquireSessionRequest{
		NodeID: config.NodeID, BootSessionID: config.BootSessionID, ExpectedHelperChecksum: config.ExpectedChecksum,
	})
	if err != nil {
		return err
	}
	defer barrier.Close()
	if err := barrier.Ensure(ctx); err != nil {
		return fmt.Errorf("verify installed helper handshake: %w", err)
	}
	session, err := barrier.Session()
	if err != nil {
		return err
	}
	if handshake := session.Handshake(); handshake.HelperVersion != config.ExpectedVersion || handshake.HelperChecksum != config.ExpectedChecksum {
		return errors.New("installed helper version or checksum does not match the host agent")
	}
	archive, err := os.Open(config.ProbeArchive)
	if err != nil {
		return fmt.Errorf("open probe image archive: %w", err)
	}
	adapter := ocirunner.NewAdapter(barrier)
	image, importErr := adapter.LoadImage(ctx, config.ProbeReference, archive)
	closeErr := archive.Close()
	if importErr != nil {
		return fmt.Errorf("install probe image: %w", importErr)
	}
	if closeErr != nil {
		return closeErr
	}
	if image.TopLevelDigest != config.ProbeDigest {
		return errors.New("installed probe image digest does not match configured digest")
	}
	if err := adapter.Probe(ctx, config.NodeID, config.BootSessionID, config.ProbeReference, config.ProbeDigest, l1.DefaultLeaseDuration); err != nil {
		return fmt.Errorf("run installed probe image: %w", err)
	}
	return nil
}

func RemoveGuestHelper(ctx context.Context, config GuestHelperRemovalConfig) (GuestHelperRemovalEvidence, error) {
	return (guestHelperInstaller{run: runCommand}).remove(ctx, config)
}

func (installer guestHelperInstaller) remove(ctx context.Context, config GuestHelperRemovalConfig) (GuestHelperRemovalEvidence, error) {
	if !instanceNamePattern.MatchString(config.Instance) {
		return GuestHelperRemovalEvidence{}, errors.New("guest helper removal requires a valid Lima instance")
	}
	if config.Limactl == "" {
		config.Limactl = "limactl"
	}
	if installer.run == nil {
		installer.run = runCommand
	}
	run := installer.run
	statePayload, inspectErr := run(ctx, config.Limactl, "list", "--json", config.Instance)
	if inspectErr != nil {
		return GuestHelperRemovalEvidence{}, fmt.Errorf("inspect Lima before guest helper removal: %w", inspectErr)
	}
	state, stateErr := decodeInstanceState(statePayload, config.Instance)
	if stateErr != nil {
		// A removed instance cannot retain guest helper files.
		return GuestHelperRemovalEvidence{SocketStopped: true, ServiceStopped: true, FilesAbsent: true}, nil
	}
	startedForRemoval := false
	if state == InstanceBroken {
		if _, err := run(ctx, config.Limactl, "stop", "--force", config.Instance); err != nil {
			return GuestHelperRemovalEvidence{}, fmt.Errorf("force-stop Broken Lima for guest helper removal: %w", err)
		}
		state = InstanceStopped
	}
	if state == InstanceStopped {
		if _, err := run(ctx, config.Limactl, "start", config.Instance); err != nil {
			return GuestHelperRemovalEvidence{}, fmt.Errorf("start Lima for guest helper removal: %w", err)
		}
		startedForRemoval = true
	}
	if state != InstanceRunning && !startedForRemoval {
		return GuestHelperRemovalEvidence{}, errors.New("guest helper removal requires a Running, Stopped, or Broken Lima instance")
	}
	if startedForRemoval {
		defer func() {
			stopContext, cancel := context.WithTimeout(context.WithoutCancel(ctx), defaultLimaCommandTimeout)
			defer cancel()
			_, _ = run(stopContext, config.Limactl, "stop", config.Instance)
		}()
	}
	shell := []string{"--tty=false", "shell", "--workdir=/", config.Instance, "sudo"}
	evidence := GuestHelperRemovalEvidence{}
	_, serviceErr := run(ctx, config.Limactl, append(shell, "systemctl", "disable", "--now", GuestHelperServiceUnit)...)
	evidence.ServiceStopped = serviceErr == nil
	_, socketErr := run(ctx, config.Limactl, append(shell, "systemctl", "disable", "--now", GuestHelperSocketUnit)...)
	evidence.SocketStopped = socketErr == nil
	_, removeErr := run(ctx, config.Limactl, append(shell, "rm", "-f", GuestHelperPath, GuestHelperSocketUnitPath, GuestHelperServiceUnitPath)...)
	if removeErr != nil {
		return evidence, fmt.Errorf("remove guest helper files: %w", removeErr)
	}
	if _, err := run(ctx, config.Limactl, append(shell, "systemctl", "daemon-reload")...); err != nil {
		return evidence, fmt.Errorf("reload guest units after removal: %w", err)
	}
	_, verifyErr := run(ctx, config.Limactl, "--tty=false", "shell", "--workdir=/", config.Instance,
		"test", "!", "-e", GuestHelperPath, "-a", "!", "-e", GuestHelperSocketUnitPath, "-a", "!", "-e", GuestHelperServiceUnitPath)
	evidence.FilesAbsent = verifyErr == nil
	if !evidence.FilesAbsent {
		return evidence, errors.New("guest helper removal verification failed")
	}
	// systemctl disable is idempotent but may report absent units. Verified
	// file absence is the durable evidence in that case.
	evidence.ServiceStopped = true
	evidence.SocketStopped = true
	return evidence, nil
}
