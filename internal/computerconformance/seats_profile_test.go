//go:build linux || darwin

package computerconformance_test

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/Derek-X-Wang/wefty/runner/ocihelper"
	imagev1 "github.com/opencontainers/image-spec/specs-go/v1"
)

type seatProfilePaths struct{ rootfs, managed, service, control, kit, resolver, hosts string }

func seatProfileInput(id, reference, digest string, config imagev1.ImageConfig, paths seatProfilePaths, kernel string) ocihelper.RuntimeSpecInput {
	return ocihelper.RuntimeSpecInput{
		ContainerID: id, CgroupPath: "/" + id, RootfsPath: paths.rootfs, ManagedRoot: paths.managed,
		Image: ocihelper.ImageRuntimeConfig{User: config.User, Environment: config.Env, Entrypoint: config.Entrypoint, Command: config.Cmd, WorkingDirectory: config.WorkingDir},
		Workload: ocihelper.WorkloadInput{ImageReference: reference, ImageDigest: digest, Computer: true,
			Limits:         ocihelper.WorkloadLimits{MemoryBytes: 1 << 30},
			ManagedVolumes: []ocihelper.ManagedVolumeDescriptor{{Kind: ocihelper.ManagedVolumeComputerDisk, ComputerStorage: &ocihelper.ComputerStorageReference{ComputerID: "seat-conformance", StorageID: "seat-conformance", StorageGeneration: 1, IntentRevision: 1, DiskBytes: 2 << 30}}},
			OperatorMounts: []ocihelper.OperatorMount{{NodePath: paths.kit, ContainerPath: "/opt/seat-install-kit", ReadOnly: true}}},
		Guest:        ocihelper.GuestKernelFacts{Architecture: runtime.GOARCH, KernelRelease: kernel},
		ResolverPath: paths.resolver, HostsPath: paths.hosts, ComputerControlSource: paths.control,
		ManagedVolumeSources: map[ocihelper.ManagedVolumeKind]string{ocihelper.ManagedVolumeComputerDisk: paths.service},
		AllowedMountRoots:    []string{filepath.Dir(paths.kit)}, OperatorMountSources: []string{paths.kit},
	}
}

func TestSeatLaneSetupPassesRealProfileValidationForFreshAndRestartedStorage(t *testing.T) {
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	paths := seatProfilePaths{rootfs: filepath.Join(root, "rootfs"), managed: filepath.Join(root, "managed"), kit: filepath.Join(root, "kit")}
	paths.service = filepath.Join(paths.managed, "service")
	paths.resolver, paths.hosts = filepath.Join(paths.managed, "resolv.conf"), filepath.Join(paths.managed, "hosts")
	for _, directory := range []string{filepath.Join(paths.rootfs, "etc"), filepath.Join(paths.service, "etc"), paths.kit} {
		if err := os.MkdirAll(directory, 0755); err != nil {
			t.Fatal(err)
		}
	}
	for _, file := range []string{paths.resolver, paths.hosts, filepath.Join(paths.service, "etc", "machine-id")} {
		if err := os.WriteFile(file, []byte("fixture\n"), 0444); err != nil {
			t.Fatal(err)
		}
	}
	for _, attempt := range []string{"fresh", "restart", "negative"} {
		paths.control = filepath.Join(paths.managed, "control-"+attempt)
		if err := os.Mkdir(paths.control, 0755); err != nil {
			t.Fatal(err)
		}
		input := seatProfileInput("seat-"+attempt, "example@sha256:"+strings.Repeat("a", 64), "sha256:"+strings.Repeat("a", 64), imagev1.ImageConfig{Entrypoint: []string{"/usr/bin/tini", "-g", "--", "/usr/local/bin/wefty-seat-supervisor"}, WorkingDir: "/"}, paths, "fixture-kernel")
		document, err := ocihelper.BuildRuntimeSpec(context.Background(), input)
		if runtime.GOOS != "linux" {
			// All real source, workload and image-user validation precedes this
			// Linux-only step. This proves setup, never guest seccomp or runtime.
			if err == nil || !strings.Contains(err.Error(), "seccomp profile construction must run inside the Linux guest") {
				t.Fatalf("%s profile setup rejected before Linux seccomp: %v", attempt, err)
			}
			continue
		}
		if err != nil {
			t.Fatalf("%s profile: %v", attempt, err)
		}
		if err := document.RevalidateMounts(); err != nil {
			t.Fatal(err)
		}
		if err := document.Close(); err != nil {
			t.Fatal(err)
		}
	}
}
