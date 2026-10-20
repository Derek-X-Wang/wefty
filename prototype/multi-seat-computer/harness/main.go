// PROTOTYPE harness (wefty #1028). Throwaway.
//
// Builds the REAL wefty-v1 Computer runtime spec with ocihelper.BuildRuntimeSpec
// and runs it with plain containerd, the way containerd_engine_linux.go does,
// minus everything that is not the profile (leases, ownership records, binary
// logger, host bridge, NAT/DNS network). Must run as root inside the Linux VM.
//
//	harness up   IMAGE_REF   build spec, create container+task, bring lo up, start
//	harness down             kill task, delete task+container (service dir stays)
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"time"

	"github.com/Derek-X-Wang/wefty/runner/ocihelper"
	containerd "github.com/containerd/containerd/v2/client"
	"github.com/containerd/containerd/v2/core/content"
	"github.com/containerd/containerd/v2/core/mount"
	"github.com/containerd/containerd/v2/core/snapshots"
	"github.com/containerd/containerd/v2/pkg/cio"
	"github.com/containerd/containerd/v2/pkg/namespaces"
	"github.com/containerd/containerd/v2/pkg/oci"
	"github.com/containerd/containerd/v2/core/containers"
	"github.com/opencontainers/image-spec/identity"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
)

const (
	root        = "/var/lib/proto-oci" // ManagedRoot
	containerID = "proto-seats"
	cgroupID    = "proto-seats"
	snapshotID  = "proto-seats-snap"
)

func must(err error, what string) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "harness: %s: %v\n", what, err)
		os.Exit(1)
	}
}

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: harness up IMAGE | down")
		os.Exit(2)
	}
	client, err := containerd.New("/run/containerd/containerd.sock", containerd.WithDefaultNamespace(ocihelper.ContainerdNamespace))
	must(err, "connect containerd")
	defer client.Close()
	ctx := namespaces.WithNamespace(context.Background(), ocihelper.ContainerdNamespace)
	switch os.Args[1] {
	case "up":
		up(ctx, client, os.Args[2])
	case "down":
		down(ctx, client)
	default:
		must(fmt.Errorf("unknown command %q", os.Args[1]), "usage")
	}
}

func up(ctx context.Context, client *containerd.Client, ref string) {
	t0 := time.Now()
	image, err := client.GetImage(ctx, ref)
	must(err, "get image")
	digest := image.Target().Digest.String()
	must(image.Unpack(ctx, ocihelper.DefaultSnapshotter), "unpack image")
	diffs, err := image.RootFS(ctx)
	must(err, "image rootfs")
	parent := identity.ChainID(diffs)
	snapshotter := client.SnapshotService(ocihelper.DefaultSnapshotter)
	mounts, err := snapshotter.Prepare(ctx, snapshotID, parent.String(),
		snapshots.WithLabels(map[string]string{"containerd.io/gc.root": time.Now().UTC().Format(time.RFC3339)}))
	must(err, "prepare snapshot")

	// Managed-root layout the profile validators expect.
	service := filepath.Join(root, "service")
	control := filepath.Join(root, "control")
	for _, d := range []string{service, filepath.Join(service, "etc"), control} {
		must(os.MkdirAll(d, 0o755), "mkdir "+d)
	}
	machineID := filepath.Join(service, "etc", "machine-id")
	if _, err := os.Stat(machineID); err != nil {
		must(os.WriteFile(machineID, []byte("0123456789abcdef0123456789abcdef\n"), 0o444), "machine-id")
	}
	resolver, hosts := filepath.Join(root, "resolv.conf"), filepath.Join(root, "hosts")
	must(os.WriteFile(resolver, []byte("nameserver 127.0.0.53\n"), 0o444), "resolv.conf")
	must(os.WriteFile(hosts, []byte("127.0.0.1 localhost\n"), 0o444), "hosts")

	cfgDesc, err := image.Config(ctx)
	must(err, "image config descriptor")
	blob, err := readBlob(ctx, client, cfgDesc)
	must(err, "read image config")
	var cfg ocispec.Image
	must(json.Unmarshal(blob, &cfg), "parse image config")

	var spec []byte
	var document *ocihelper.RuntimeSpecDocument
	err = mount.WithTempMount(ctx, mounts, func(rootfs string) error {
		var e error
		document, e = ocihelper.BuildRuntimeSpec(ctx, ocihelper.RuntimeSpecInput{
			ContainerID: containerID, CgroupPath: "/" + cgroupID, RootfsPath: rootfs,
			Image: ocihelper.ImageRuntimeConfig{User: cfg.Config.User, Environment: cfg.Config.Env,
				Entrypoint: cfg.Config.Entrypoint, Command: cfg.Config.Cmd, WorkingDirectory: cfg.Config.WorkingDir},
			Workload: ocihelper.WorkloadInput{
				ImageReference: ref, ImageDigest: digest, Computer: true,
				ManagedVolumes: []ocihelper.ManagedVolumeDescriptor{{Kind: ocihelper.ManagedVolumeComputerDisk,
					ComputerStorage: &ocihelper.ComputerStorageReference{ComputerID: "proto", StorageID: "proto", StorageGeneration: 1, IntentRevision: 1, DiskBytes: 1 << 30}}},
				Limits: ocihelper.WorkloadLimits{MemoryBytes: 2 << 30},
			},
			Guest:        ocihelper.GuestKernelFacts{Architecture: runtime.GOARCH, KernelRelease: kernelRelease()},
			ResolverPath: resolver, HostsPath: hosts, ManagedRoot: root,
			ManagedVolumeSources:  map[ocihelper.ManagedVolumeKind]string{ocihelper.ManagedVolumeComputerDisk: service},
			ComputerControlSource: control,
		})
		return e
	})
	must(err, "BuildRuntimeSpec")
	defer document.Close()
	any, err := document.ContainerdSpec()
	must(err, "ContainerdSpec")
	spec = any.GetValue()
	must(os.WriteFile(filepath.Join(root, "spec.json"), spec, 0o644), "write spec.json")

	container, err := client.NewContainer(ctx, containerID,
		containerd.WithImage(image), containerd.WithRuntime(ocihelper.DefaultRuntimeHandler, nil),
		containerd.WithSnapshotter(ocihelper.DefaultSnapshotter), containerd.WithSnapshot(snapshotID),
		func(_ context.Context, _ *containerd.Client, record *containers.Container) error {
			record.Spec = any
			return nil
		})
	must(err, "create container")
	task, err := container.NewTask(ctx, cio.LogFile("/var/log/proto-container.log"))
	must(err, "create task")
	pid := int(task.Pid())
	// runc creates the private netns with loopback DOWN; the real engine brings it
	// up in prepareComputerNetwork. Do the minimum here.
	out, err := exec.Command("nsenter", "-t", fmt.Sprint(pid), "-n", "ip", "link", "set", "lo", "up").CombinedOutput()
	must(err, "lo up: "+string(out))
	must(document.RevalidateMounts(), "revalidate mounts")
	must(task.Start(ctx), "start task")
	fmt.Printf("{\"pid\":%d,\"digest\":%q,\"setup_ms\":%d}\n", pid, digest, time.Since(t0).Milliseconds())
}

func down(ctx context.Context, client *containerd.Client) {
	container, err := client.LoadContainer(ctx, containerID)
	must(err, "load container")
	if task, err := container.Task(ctx, nil); err == nil {
		ch, _ := task.Wait(ctx)
		_ = task.Kill(ctx, syscall.SIGKILL, containerd.WithKillAll)
		select {
		case <-ch:
		case <-time.After(20 * time.Second):
			fmt.Fprintln(os.Stderr, "harness: task did not exit in 20s")
		}
		_, _ = task.Delete(ctx)
	}
	must(container.Delete(ctx, containerd.WithSnapshotCleanup), "delete container")
	fmt.Println("{\"down\":true}")
}

func readBlob(ctx context.Context, client *containerd.Client, desc ocispec.Descriptor) ([]byte, error) {
	return content.ReadBlob(ctx, client.ContentStore(), desc)
}

func kernelRelease() string {
	var u syscall.Utsname
	if syscall.Uname(&u) != nil {
		return "unknown"
	}
	var b strings.Builder
	for _, c := range u.Release {
		if c == 0 {
			break
		}
		b.WriteByte(byte(c))
	}
	return b.String()
}

var _ = oci.WithDefaultSpec
