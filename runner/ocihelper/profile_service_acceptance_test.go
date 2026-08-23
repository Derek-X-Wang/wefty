//go:build service_acceptance && (darwin || linux)

package ocihelper

import (
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"testing"

	specs "github.com/opencontainers/runtime-spec/specs-go"
)

const linuxENOSYS = uint(38)

func TestServiceAcceptancePinnedRuntimeProfiles(t *testing.T) {
	for _, test := range runtimeSpecGoldenCases() {
		t.Run(test.name, func(t *testing.T) {
			payload, err := os.ReadFile(filepath.Join("testdata", "containerd-v2.3.4", test.golden))
			if err != nil {
				t.Fatal(err)
			}
			var profile specs.Spec
			if err := json.Unmarshal(payload, &profile); err != nil {
				t.Fatal(err)
			}
			if profile.Process == nil || profile.Process.User.UID != test.expectedUID || profile.Process.User.GID != test.expectedGID ||
				!slices.Equal(profile.Process.User.AdditionalGids, test.expectedGIDs) {
				t.Fatalf("named image user and supplemental groups were not resolved: %#v", profile.Process)
			}
			capabilities := profile.Process.Capabilities
			if capabilities == nil || len(capabilities.Bounding) != len(isolationCapabilities) ||
				len(capabilities.Permitted) != len(isolationCapabilities) || len(capabilities.Effective) != len(isolationCapabilities) ||
				capabilities.Inheritable == nil || len(capabilities.Inheritable) != 0 ||
				capabilities.Ambient == nil || len(capabilities.Ambient) != 0 {
				t.Fatalf("golden capability sets do not match wefty-v1: %#v", capabilities)
			}
			if slices.ContainsFunc(profile.Linux.Namespaces, func(namespace specs.LinuxNamespace) bool {
				return namespace.Type == specs.NetworkNamespace || namespace.Type == specs.UserNamespace
			}) {
				t.Fatalf("golden contains an unsupported namespace: %#v", profile.Linux.Namespaces)
			}
			architecture := specs.ArchX86_64
			if test.architecture == "arm64" {
				architecture = specs.ArchAARCH64
			}
			if profile.Linux.Seccomp == nil || !slices.Contains(profile.Linux.Seccomp.Architectures, architecture) ||
				!hasSeccompErrno(profile.Linux.Seccomp, "clone3", linuxENOSYS) {
				t.Fatalf("golden seccomp profile does not match the guest architecture: %#v", profile.Linux.Seccomp)
			}
			assertMount(t, profile.Mounts, "/etc/resolv.conf", "/run/wefty/fixtures/resolv.conf", true)
			assertMount(t, profile.Mounts, "/etc/hosts", "/run/wefty/fixtures/hosts", true)
			assertExactMount(t, profile.Mounts, specs.Mount{Destination: "/sys/fs/cgroup", Type: "cgroup", Source: "cgroup", Options: []string{"nosuid", "noexec", "nodev", "relatime", "ro"}})
			if test.unlimited && (profile.Linux.Resources.Memory != nil || profile.Linux.Resources.CPU != nil) {
				t.Fatalf("unlimited golden serialized kernel limits: %#v", profile.Linux.Resources)
			}
			if test.service {
				assertMount(t, profile.Mounts, "/wefty/service", "/run/wefty/fixtures/service", false)
				if slices.ContainsFunc(profile.Mounts, func(mount specs.Mount) bool {
					return mount.Source == "/run/wefty/fixtures/log-segments"
				}) {
					t.Fatal("helper log segments escaped into the payload mount table")
				}
			}
			if test.golden == "wefty-v1-linux-amd64.json" && profile.Process.ApparmorProfile != "wefty-default" {
				t.Fatalf("opportunistic AppArmor profile = %q", profile.Process.ApparmorProfile)
			}
		})
	}
}

func hasSeccompErrno(profile *specs.LinuxSeccomp, syscallName string, errno uint) bool {
	for _, rule := range profile.Syscalls {
		if rule.ErrnoRet != nil && *rule.ErrnoRet == errno && slices.Contains(rule.Names, syscallName) {
			return true
		}
	}
	return false
}
