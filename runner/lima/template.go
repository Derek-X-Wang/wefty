// Package lima contains the macOS-local mechanics for carrying the OCI helper
// contract across a Lima VM. The VM remains plumbing for one Wefty node.
package lima

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strconv"
	"strings"
)

const (
	DefaultInstanceName    = "wefty-oci"
	GuestAllowedMountRoot  = "/mnt/wefty-host"
	GuestHelperSocket      = "/run/wefty/oci-helper.sock"
	HostHelperSocketName   = "wefty-oci-helper.sock"
	DefaultDisk            = "32GiB"
	VMMemoryFlagName       = "vm-memory"
	VMCPUsFlagName         = "vm-cpus"
	VMDiskFlagName         = "vm-disk"
	maximumDefaultMemory   = int64(4 << 30)
	defaultComputerCeiling = int64(4 << 30)
)

var limaSizePattern = regexp.MustCompile(`^[1-9][0-9]*(?:[KMGT]iB|[kMGT]?B)$`)

// Sizing is resolved once by setup and serialized explicitly into the Lima
// template. It is never recomputed while the VM is running.
type Sizing struct {
	Memory string
	CPUs   int
	Disk   string
}

func (sizing Sizing) Validate() error {
	if !limaSizePattern.MatchString(sizing.Memory) || !limaSizePattern.MatchString(sizing.Disk) || sizing.CPUs <= 0 {
		return errors.New("Lima sizing requires positive CPUs and explicit memory/disk byte quantities")
	}
	return nil
}

// ParseByteQuantity converts the explicit setup quantity grammar into bytes.
// Admission receives this persisted setup result; the helper never derives its
// own ceiling from a runtime memory observation.
func ParseByteQuantity(value string) (int64, error) {
	if !limaSizePattern.MatchString(value) {
		return 0, errors.New("invalid explicit byte quantity")
	}
	units := []struct {
		suffix     string
		multiplier int64
	}{
		{suffix: "TiB", multiplier: 1 << 40},
		{suffix: "GiB", multiplier: 1 << 30},
		{suffix: "MiB", multiplier: 1 << 20},
		{suffix: "KiB", multiplier: 1 << 10},
		{suffix: "TB", multiplier: 1_000_000_000_000},
		{suffix: "GB", multiplier: 1_000_000_000},
		{suffix: "MB", multiplier: 1_000_000},
		{suffix: "kB", multiplier: 1_000},
		{suffix: "B", multiplier: 1},
	}
	for _, unit := range units {
		if !strings.HasSuffix(value, unit.suffix) {
			continue
		}
		count, err := strconv.ParseInt(strings.TrimSuffix(value, unit.suffix), 10, 64)
		if err != nil || count > (1<<63-1)/unit.multiplier {
			return 0, errors.New("explicit byte quantity overflows int64")
		}
		return count * unit.multiplier, nil
	}
	return 0, errors.New("invalid explicit byte quantity")
}

// SizingFlags provides the exact setup-facing flags consumed by the
// node-local setup/convergence command.
type SizingFlags struct {
	memory string
	cpus   int
	disk   string
}

func BindSizingFlags(flags *flag.FlagSet, defaults Sizing) *SizingFlags {
	values := &SizingFlags{}
	flags.StringVar(&values.memory, VMMemoryFlagName, defaults.Memory, "fixed Lima VM memory reservation")
	flags.IntVar(&values.cpus, VMCPUsFlagName, defaults.CPUs, "fixed Lima VM virtual CPU count")
	flags.StringVar(&values.disk, VMDiskFlagName, defaults.Disk, "fixed Lima VM disk size")
	return values
}

func (values *SizingFlags) Sizing() (Sizing, error) {
	if values == nil {
		return Sizing{}, errors.New("Lima sizing flags are unavailable")
	}
	sizing := Sizing{Memory: values.memory, CPUs: values.cpus, Disk: values.disk}
	if err := sizing.Validate(); err != nil {
		return Sizing{}, err
	}
	return sizing, nil
}

// DefaultSizing applies the owner-ratified setup defaults: one quarter of
// host RAM capped at 4 GiB, four CPUs capped at half the logical cores, and a
// 32 GiB disk. Callers persist the returned values as explicit setup flags.
func DefaultSizing(hostMemoryBytes int64, logicalCPUs int) (Sizing, error) {
	if hostMemoryBytes <= 0 || logicalCPUs <= 0 {
		return Sizing{}, errors.New("host memory and logical CPU count must be positive")
	}
	memory := hostMemoryBytes / 4
	if memory > maximumDefaultMemory {
		memory = maximumDefaultMemory
	}
	cpus := logicalCPUs / 2
	if cpus < 1 {
		cpus = 1
	}
	if cpus > 4 {
		cpus = 4
	}
	return Sizing{Memory: fmt.Sprintf("%dMiB", memory>>20), CPUs: cpus, Disk: DefaultDisk}, nil
}

func HostDefaultSizing() (Sizing, error) {
	memory, err := hostPhysicalMemoryBytes()
	if err != nil {
		return Sizing{}, err
	}
	return DefaultSizing(memory, runtime.NumCPU())
}

// DefaultMacComputerCapacity materializes the shipped Computer admission
// configuration from the setup-converged Lima sizing.
func DefaultMacComputerCapacity(sizing Sizing) (capacityBytes, reserveBytes int64, err error) {
	if err := sizing.Validate(); err != nil {
		return 0, 0, err
	}
	vmMemoryBytes, err := ParseByteQuantity(sizing.Memory)
	if err != nil {
		return 0, 0, err
	}
	return defaultComputerCeiling, vmMemoryBytes / 4, nil
}

// HostPhysicalMemoryBytes exposes the setup-time node sizing fact. Runtime
// admission never derives its ceiling from this value.
func HostPhysicalMemoryBytes() (int64, error) {
	return hostPhysicalMemoryBytes()
}

// TemplateConfig is the complete Ticket #145 Lima transport configuration.
// Helper installation and boot supervision remain owned by later tickets.
type TemplateConfig struct {
	Sizing               Sizing
	HostAllowedMountRoot string
}

func (config TemplateConfig) validate() error {
	if runtime.GOOS == "windows" {
		return errors.New("Lima templates are unavailable on Windows")
	}
	if err := config.Sizing.Validate(); err != nil {
		return err
	}
	if !filepath.IsAbs(config.HostAllowedMountRoot) || filepath.Clean(config.HostAllowedMountRoot) == string(filepath.Separator) {
		return errors.New("Lima allowed mount root must be an absolute non-root path")
	}
	return nil
}

// RenderTemplate returns a Lima 2.2 template with rootful containerd,
// overlayfs, one explicit writable operator mount root, the narrow helper
// socket forward, and all dynamic TCP/UDP forwarding disabled.
func RenderTemplate(config TemplateConfig) ([]byte, error) {
	if err := config.validate(); err != nil {
		return nil, err
	}
	quote := func(value string) string {
		encoded, _ := json.Marshal(value)
		return string(encoded)
	}
	template := fmt.Sprintf(`vmType: vz
arch: default
cpus: %d
memory: %s
disk: %s
base:
  - template:_images/ubuntu-24.04
mountType: virtiofs
mounts:
  - location: %s
    mountPoint: %s
    writable: true
containerd:
  system: true
  user: false
hostResolver:
  enabled: true
provision:
  - mode: system
    script: |
      #!/bin/sh
      set -eu
      modprobe overlay
      install -d -m 0755 /etc/modules-load.d
      printf 'overlay\n' > /etc/modules-load.d/wefty-overlay.conf
      ctr --address /run/containerd/containerd.sock plugins ls | awk '$1 == "io.containerd.snapshotter.v1" && $2 == "overlayfs" && $4 == "ok" { found=1 } END { exit !found }'
portForwards:
  - guestSocket: %s
    hostSocket: "{{.Dir}}/sock/%s"
  - guestIP: "0.0.0.0"
    guestIPMustBeZero: false
    proto: any
    ignore: true
`, config.Sizing.CPUs, quote(config.Sizing.Memory), quote(config.Sizing.Disk),
		quote(filepath.Clean(config.HostAllowedMountRoot)), quote(GuestAllowedMountRoot), quote(GuestHelperSocket), HostHelperSocketName)
	return []byte(template), nil
}

func WriteTemplate(path string, config TemplateConfig) error {
	if !filepath.IsAbs(path) {
		return errors.New("Lima template path must be absolute")
	}
	payload, err := RenderTemplate(config)
	if err != nil {
		return err
	}
	temporary, err := os.CreateTemp(filepath.Dir(path), ".wefty-lima-template-*")
	if err != nil {
		return err
	}
	temporaryPath := temporary.Name()
	defer func() { _ = os.Remove(temporaryPath) }()
	if err := temporary.Chmod(0o600); err != nil {
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
	if err := os.Rename(temporaryPath, path); err != nil {
		return err
	}
	return syncDirectory(filepath.Dir(path))
}

// HelperSocketPath is the operator-owned 0700 instance socket directory used
// by the host agent. The template never forwards containerd itself.
func HelperSocketPath(limaHome, instanceName string) (string, error) {
	if !filepath.IsAbs(limaHome) || strings.TrimSpace(instanceName) == "" || instanceName == "." || instanceName == ".." || filepath.Base(instanceName) != instanceName {
		return "", errors.New("Lima home must be absolute and instance name must be one path component")
	}
	return filepath.Join(limaHome, instanceName, "sock", HostHelperSocketName), nil
}
