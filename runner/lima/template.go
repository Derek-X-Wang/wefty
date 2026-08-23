// Package lima contains the macOS-local mechanics for carrying the OCI helper
// contract across a Lima VM. The VM remains plumbing for one Wefty node.
package lima

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
)

const (
	DefaultInstanceName   = "wefty-oci"
	GuestAllowedMountRoot = "/mnt/wefty-host"
	GuestHelperSocket     = "/run/wefty/oci-helper.sock"
	HostHelperSocketName  = "wefty-oci-helper.sock"
	DefaultDisk           = "32GiB"
	VMMemoryFlagName      = "vm-memory"
	VMCPUsFlagName        = "vm-cpus"
	VMDiskFlagName        = "vm-disk"
	maximumDefaultMemory  = int64(4 << 30)
)

var limaSizePattern = regexp.MustCompile(`^[1-9][0-9]*(?:[KMGT]iB|[kMGT]?B)$`)

// Sizing is resolved once by setup and serialized explicitly into the Lima
// template. It is never recomputed while the VM is running.
type Sizing struct {
	Memory string
	CPUs   int
	Disk   string
}

// SizingFlags provides the exact setup-facing flags without implementing the
// setup/convergence command owned by Ticket #153.
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
	if !limaSizePattern.MatchString(sizing.Memory) || !limaSizePattern.MatchString(sizing.Disk) || sizing.CPUs <= 0 {
		return Sizing{}, errors.New("Lima sizing flags require positive CPUs and explicit memory/disk byte quantities")
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
	if !limaSizePattern.MatchString(config.Sizing.Memory) || !limaSizePattern.MatchString(config.Sizing.Disk) {
		return errors.New("Lima memory and disk sizes must be explicit IEC/SI byte quantities")
	}
	if config.Sizing.CPUs <= 0 {
		return errors.New("Lima CPU count must be positive")
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

// HelperSocketPath is the operator-owned 0700 instance socket directory used
// by the host agent. The template never forwards containerd itself.
func HelperSocketPath(limaHome, instanceName string) (string, error) {
	if !filepath.IsAbs(limaHome) || strings.TrimSpace(instanceName) == "" || instanceName == "." || instanceName == ".." || filepath.Base(instanceName) != instanceName {
		return "", errors.New("Lima home must be absolute and instance name must be one path component")
	}
	return filepath.Join(limaHome, instanceName, "sock", HostHelperSocketName), nil
}
