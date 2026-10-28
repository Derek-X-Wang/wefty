package lima

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/Derek-X-Wang/wefty/internal/durable"
)

// UserInstallAssets names immutable local inputs. No Mac privilege is needed;
// only the matching Linux helper runs as root inside the guest.
type UserInstallAssets struct {
	LinuxHelper    string `json:"linux_helper"`
	HelperVersion  string `json:"helper_version"`
	HelperChecksum string `json:"helper_checksum"`
	ProbeArchive   string `json:"probe_archive"`
	ProbeReference string `json:"probe_reference"`
	ProbeDigest    string `json:"probe_digest"`
}

type UserInstallConfig struct {
	Assets                 UserInstallAssets `json:"assets"`
	Instance               string            `json:"instance"`
	LimaHome               string            `json:"lima_home"`
	MountRoot              string            `json:"mount_root"`
	IntentFile             string            `json:"intent_file"`
	SetupState             string            `json:"setup_state"`
	TemplatePath           string            `json:"template_path"`
	Sizing                 Sizing            `json:"sizing"`
	HostVMIdleWait         time.Duration     `json:"host_vm_idle_wait,omitempty"`
	HostVMMinimumFreeBytes uint64            `json:"host_vm_minimum_free_bytes,omitempty"`
	ProofDialog            bool              `json:"proof_dialog,omitempty"`
}

func ReadUserInstallJSON(path string, value any) error {
	b, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	d := json.NewDecoder(bytes.NewReader(b))
	d.DisallowUnknownFields()
	if err := d.Decode(value); err != nil {
		return err
	}
	if d.Decode(&struct{}{}) != io.EOF {
		return errors.New("OCI install JSON must contain one object")
	}
	return nil
}

func (c UserInstallConfig) Validate() error {
	if c.HostVMIdleWait < 0 {
		return errors.New("host VM idle wait must be positive")
	}
	if !instanceNamePattern.MatchString(c.Instance) {
		return errors.New("invalid per-user host VM instance")
	}
	if c.ProofDialog && c.Instance != "wefty-proof" {
		return errors.New("proof dialog requires wefty-proof")
	}
	for _, p := range []string{c.LimaHome, c.MountRoot, c.IntentFile, c.SetupState, c.TemplatePath} {
		if !filepath.IsAbs(p) || filepath.Clean(p) != p {
			return errors.New("per-user OCI paths must be absolute and clean")
		}
	}
	if err := c.Sizing.Validate(); err != nil {
		return err
	}
	capacity, reserve, err := DefaultMacComputerCapacity(c.Sizing)
	if err != nil {
		return err
	}
	socket, err := HelperSocketPath(c.LimaHome, c.Instance)
	if err != nil {
		return err
	}
	return ValidateGuestHelperInstall(GuestHelperInstallConfig{Instance: c.Instance, GuestUser: "operator", GuestUID: 501,
		HelperBinary: c.Assets.LinuxHelper, ExpectedVersion: c.Assets.HelperVersion, ExpectedChecksum: c.Assets.HelperChecksum,
		HostMountRoot: c.MountRoot, HelperSocket: socket, ProbeArchive: c.Assets.ProbeArchive, ProbeReference: c.Assets.ProbeReference, ProbeDigest: c.Assets.ProbeDigest,
		NodeID: "setup", BootSessionID: "setup", MemoryCapacityBytes: capacity, MemoryReserveBytes: reserve})
}

type userInstallOps struct {
	run          commandRunner
	ensure       func(context.Context) error
	install      func(context.Context, GuestHelperInstallConfig) error
	hostStartEnd func() time.Time
}

// PrepareUserInstall runs in the installed node agent, before its normal OCI
// boot barrier. setup itself only writes files and loads the login service.
func PrepareUserInstall(ctx context.Context, c UserInstallConfig, nodeID, bootID string) error {

	var startedAt time.Time
	run := func(ctx context.Context, name string, args ...string) ([]byte, error) {
		output, err := runUserInstallCommand(ctx, name, args...)
		if err == nil && name == "limactl" && len(args) > 0 && args[0] == "start" {
			startedAt = time.Now().UTC()
		}
		return output, err
	}
	supervisor, err := NewSupervisor(SupervisorConfig{Instance: c.Instance, Intent: FileIntentSource{Path: c.IntentFile}, run: run,
		Demand: DemandSourceFunc(func(context.Context) ([]DemandItem, error) {
			return []DemandItem{{Kind: "load-image", ID: "bootstrap"}}, nil
		}),
		CheckFreeSpace: func() error { return CheckHostVMFreeSpace(c.LimaHome, c.HostVMMinimumFreeBytes) },
	})
	if err != nil {
		return err
	}
	return prepareUserInstall(ctx, c, nodeID, bootID, userInstallOps{run: run, ensure: supervisor.Ensure, install: InstallGuestHelper, hostStartEnd: func() time.Time { return startedAt }})
}

func prepareUserInstall(ctx context.Context, c UserInstallConfig, nodeID, bootID string, ops userInstallOps) error {
	if os.Getenv("LIMA_HOME") != c.LimaHome {
		return errors.New("per-user host VM LIMA_HOME does not match configuration")
	}
	if err := c.Validate(); err != nil {
		return err
	}
	intent, err := (FileIntentSource{Path: c.IntentFile}).ReadIntent(ctx)
	if err != nil {
		return err
	}
	if !intent.Enabled {
		return nil
	}
	if err := os.MkdirAll(c.LimaHome, 0700); err != nil {
		return err
	}
	// LIMA_HOME is set by the rendered LaunchAgent, never by a host-wide change.
	payload, err := ops.run(ctx, "limactl", "list", "--json")
	if err != nil {
		return fmt.Errorf("inspect per-user host VM: %w", err)
	}
	_, err = decodeInstanceState(payload, c.Instance)
	if err != nil {
		var absent *instanceAbsentError
		if !errors.As(err, &absent) {
			return err
		}
		if _, err = ops.run(ctx, "limactl", "create", "--tty=false", "--name="+c.Instance, c.TemplatePath); err != nil {
			return fmt.Errorf("create per-user host VM: %w", err)
		}
	}
	if err := ops.ensure(ctx); err != nil {
		return err
	}
	hostStarted := time.Now().UTC()
	if ops.hostStartEnd != nil {
		hostStarted = ops.hostStartEnd()
	}
	guest := func(args ...string) ([]byte, error) {
		return ops.run(ctx, "limactl", append([]string{"--tty=false", "shell", "--workdir=/", c.Instance}, args...)...)
	}
	name, err := guest("id", "-un")
	if err != nil {
		return err
	}
	uidBytes, err := guest("id", "-u")
	if err != nil {
		return err
	}
	uid, err := strconv.ParseUint(strings.TrimSpace(string(uidBytes)), 10, 32)
	if err != nil || uid == 0 {
		return errors.New("guest login UID must be positive")
	}
	capacity, reserve, _ := DefaultMacComputerCapacity(c.Sizing)
	socket, _ := HelperSocketPath(c.LimaHome, c.Instance)
	helper := GuestHelperInstallConfig{MinimumFreeBytes: c.HostVMMinimumFreeBytes, Instance: c.Instance, GuestUser: strings.TrimSpace(string(name)), GuestUID: uint32(uid),
		HelperBinary: c.Assets.LinuxHelper, ExpectedVersion: c.Assets.HelperVersion, ExpectedChecksum: c.Assets.HelperChecksum,
		HostMountRoot: c.MountRoot, HelperSocket: socket, ProbeArchive: c.Assets.ProbeArchive, ProbeReference: c.Assets.ProbeReference, ProbeDigest: c.Assets.ProbeDigest,
		NodeID: nodeID, BootSessionID: bootID, MemoryCapacityBytes: capacity, MemoryReserveBytes: reserve}
	if err := ops.install(ctx, helper); err != nil {
		return err
	}
	if c.Instance == "wefty-proof" {
		var start *time.Time
		if !hostStarted.IsZero() {
			start = &hostStarted
		}
		receipt, err := json.Marshal(struct {
			PID             int        `json:"pid"`
			HostStarted     *time.Time `json:"host_started"`
			HelperInstalled time.Time  `json:"helper_installed"`
		}{os.Getpid(), start, time.Now().UTC()})
		if err != nil {
			return err
		}
		root, err := os.OpenRoot(filepath.Dir(c.SetupState))
		if err != nil {
			return err
		}
		defer root.Close()
		if err := durable.WriteFile(root, "user-install-timing.json", append(receipt, '\n'), 0600); err != nil {
			return err
		}
	}
	return nil
}

// Successful Lima warnings belong on stderr, never in the JSON inventory.
func runUserInstallCommand(ctx context.Context, name string, args ...string) ([]byte, error) {
	command := exec.CommandContext(ctx, name, args...)
	command.WaitDelay = time.Second
	return command.Output()
}
