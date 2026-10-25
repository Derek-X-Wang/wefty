//go:build linux && computer_seat_lane

package ocihelper

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/internal/seatcopy"
	containerd "github.com/containerd/containerd/v2/client"
	"github.com/containerd/containerd/v2/core/containers"
	"github.com/containerd/containerd/v2/core/content"
	"github.com/containerd/containerd/v2/core/mount"
	"github.com/containerd/containerd/v2/core/snapshots"
	"github.com/containerd/containerd/v2/pkg/cio"
	"github.com/containerd/containerd/v2/pkg/namespaces"
	"github.com/opencontainers/image-spec/identity"
	imagev1 "github.com/opencontainers/image-spec/specs-go/v1"
)

// The task fixture uses the unmodified production profile. All Seat directives,
// ready decisions and cleanup decisions go through the real helper engine;
// task creation/termination is fixture plumbing, not a claim about Run admission.
func TestHelperComputerSeatCopy(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Fatal("computer_seat_lane requires root")
	}
	imageRef, address := os.Getenv("WEFTY_SEAT_IMAGE"), os.Getenv("WEFTY_SEAT_CONTAINERD")
	if imageRef == "" || address == "" {
		t.Fatal("Seat lane provisioning missing")
	}
	ctx, cancel := context.WithTimeout(namespaces.WithNamespace(t.Context(), ContainerdNamespace), 4*time.Minute)
	defer cancel()
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	client, err := containerd.New(address, containerd.WithDefaultNamespace(ContainerdNamespace))
	must(err)
	defer client.Close()
	image, err := client.GetImage(ctx, imageRef)
	must(err)
	must(image.Unpack(ctx, DefaultSnapshotter))
	root := t.TempDir()
	managed := filepath.Join(root, "managed")
	service := filepath.Join(managed, "service")
	control := filepath.Join(managed, "control")
	for _, p := range []string{filepath.Join(service, "etc"), control} {
		must(os.MkdirAll(p, 0755))
	}
	must(os.WriteFile(filepath.Join(service, "etc/machine-id"), []byte("0123456789abcdef0123456789abcdef\n"), 0444))
	resolver, hosts := filepath.Join(managed, "resolv.conf"), filepath.Join(managed, "hosts")
	must(os.WriteFile(resolver, []byte("nameserver 127.0.0.53\n"), 0444))
	must(os.WriteFile(hosts, []byte("127.0.0.1 localhost\n"), 0444))
	id := "helper-seat-copy-" + filepath.Base(root)
	diffs, err := image.RootFS(ctx)
	must(err)
	snapshotter := client.SnapshotService(DefaultSnapshotter)
	// Without a GC root, containerd may collect the unreferenced active
	// snapshot before NewContainer refers to it.
	mounts, err := snapshotter.Prepare(ctx, id, identity.ChainID(diffs).String(), snapshots.WithLabels(map[string]string{"containerd.io/gc.root": time.Now().UTC().Format(time.RFC3339)}))
	must(err)
	defer func() {
		cleanupCtx, cancel := context.WithTimeout(namespaces.WithNamespace(context.Background(), ContainerdNamespace), 15*time.Second)
		defer cancel()
		if err := snapshotter.Remove(cleanupCtx, id); err != nil && !strings.Contains(err.Error(), "not found") {
			t.Error(err)
		}
	}()
	desc, err := image.Config(ctx)
	must(err)
	blob, err := content.ReadBlob(ctx, client.ContentStore(), desc)
	must(err)
	var config imagev1.Image
	must(json.Unmarshal(blob, &config))
	release, err := os.ReadFile("/proc/sys/kernel/osrelease")
	must(err)
	var document *RuntimeSpecDocument
	must(mount.WithTempMount(ctx, mounts, func(rootfs string) error {
		var err error
		document, err = BuildRuntimeSpec(ctx, RuntimeSpecInput{ContainerID: id, CgroupPath: "/" + id, RootfsPath: rootfs, ManagedRoot: managed,
			Image:    ImageRuntimeConfig{User: config.Config.User, Environment: config.Config.Env, Entrypoint: config.Config.Entrypoint, Command: config.Config.Cmd, WorkingDirectory: config.Config.WorkingDir},
			Workload: WorkloadInput{ImageReference: imageRef, ImageDigest: image.Target().Digest.String(), Computer: true, Limits: WorkloadLimits{MemoryBytes: 1 << 30}, ManagedVolumes: []ManagedVolumeDescriptor{{Kind: ManagedVolumeComputerDisk, ComputerStorage: &ComputerStorageReference{ComputerID: "helper-seats", StorageID: "helper-seats", StorageGeneration: 1, IntentRevision: 1, DiskBytes: 2 << 30}}}},
			Guest:    GuestKernelFacts{Architecture: runtime.GOARCH, KernelRelease: strings.TrimSpace(string(release))}, ResolverPath: resolver, HostsPath: hosts, ComputerControlSource: control, ManagedVolumeSources: map[ManagedVolumeKind]string{ManagedVolumeComputerDisk: service}})
		return err
	}))
	defer document.Close()
	spec, err := document.ContainerdSpec()
	must(err)
	container, err := client.NewContainer(ctx, id, containerd.WithImage(image), containerd.WithRuntime(DefaultRuntimeHandler, nil), containerd.WithSnapshotter(DefaultSnapshotter), containerd.WithSnapshot(id), func(_ context.Context, _ *containerd.Client, record *containers.Container) error {
		record.Spec = spec
		return nil
	})
	must(err)
	defer func() {
		cleanupCtx, cancel := context.WithTimeout(namespaces.WithNamespace(context.Background(), ContainerdNamespace), 15*time.Second)
		defer cancel()
		if err := container.Delete(cleanupCtx); err != nil {
			t.Error(err)
		}
	}()
	task, err := container.NewTask(ctx, cio.LogFile(filepath.Join(root, "computer.log")))
	must(err)
	defer func() {
		cleanupCtx, cancel := context.WithTimeout(namespaces.WithNamespace(context.Background(), ContainerdNamespace), 15*time.Second)
		defer cancel()
		wait, err := task.Wait(cleanupCtx)
		if err == nil {
			_ = task.Kill(cleanupCtx, syscall.SIGKILL, containerd.WithKillAll)
			select {
			case <-wait:
			case <-cleanupCtx.Done():
				t.Error(cleanupCtx.Err())
			}
		}
		if _, err := task.Delete(cleanupCtx); err != nil {
			t.Error(err)
		}
	}()
	namespace, err := pinTaskNetworkNamespace(task.Pid())
	must(err)
	defer namespace.close()
	out, err := exec.CommandContext(ctx, "nsenter", "-t", fmt.Sprint(task.Pid()), "-n", "ip", "link", "set", "lo", "up").CombinedOutput()
	if err != nil {
		t.Fatalf("loopback: %v %s", err, out)
	}
	must(document.RevalidateMounts())
	must(task.Start(ctx))
	authority := testAuthority()
	authority.Class = "service"
	attempt := &containerdAttempt{authority: authority, task: task, networkNamespace: namespace, controlDirectory: control}
	engine := &ContainerdEngine{config: NativeEngineConfig{Clock: systemClock{}}, attempts: map[string]*containerdAttempt{authority.key(): attempt}}
	waitForSeat := func(generation uint64, state string) {
		t.Helper()
		deadline := time.NewTimer(65 * time.Second)
		defer deadline.Stop()
		ticker := time.NewTicker(200 * time.Millisecond)
		defer ticker.Stop()
		var last ObserveComputerSeatsResponse
		var lastErr error
		for {
			last, lastErr = engine.ObserveComputerSeats(ctx, ObserveComputerSeatsRequest{Authority: authority, Generation: generation})
			if lastErr == nil && len(last.Seats) == 1 && last.Seats[0].State == state {
				if state == "cleaned" && !last.Seats[0].CleanupComplete {
					t.Fatal("cleaned without positive evidence")
				}
				return
			}
			if lastErr == nil && len(last.Seats) == 1 && last.Seats[0].State == "failed" {
				t.Fatalf("Seat failed: %+v", last)
			}
			select {
			case <-deadline.C:
				t.Fatalf("Seat never %s: %+v %v", state, last, lastErr)
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			case <-ticker.C:
			}
		}
	}
	applied, err := engine.ApplyComputerSeats(ctx, ApplyComputerSeatsRequest{authority, []ComputerSeat{{1, "seat1", 7102, 7103}}})
	must(err)
	waitForSeat(applied.Generation, "ready")
	source := filepath.Join(root, ".env")
	must(os.WriteFile(source, []byte("ignored secret"), 0600))
	input, writer := io.Pipe()
	packed := make(chan error, 1)
	go func() { _, err := seatcopy.Pack(writer, source); writer.CloseWithError(err); packed <- err }()
	request := CopyComputerSeatRequest{Authority: authority, Generation: applied.Generation, Place: 1, Direction: "in", Path: ".env"}
	size, err := engine.CopyComputerSeat(ctx, request, input, nil)
	input.Close()
	must(err)
	must(<-packed)
	if size != 14 {
		t.Fatalf("copied size %d", size)
	}
	info, err := os.Stat(filepath.Join(service, "seats", "1", ".env"))
	must(err)
	if info.Sys().(*syscall.Stat_t).Uid != 2001 {
		t.Fatal("copy did not run as the Seat user")
	}
	copyOut := func(path, destination string) {
		t.Helper()
		request.Direction = "out"
		request.Path = path
		reader, writer := io.Pipe()
		done := make(chan error, 1)
		go func() {
			_, err := engine.CopyComputerSeat(ctx, request, nil, func(data []byte) error { _, e := writer.Write(data); return e })
			writer.CloseWithError(err)
			done <- err
		}()
		_, err := seatcopy.Receive(reader, destination)
		reader.Close()
		must(err)
		must(<-done)
	}
	result := filepath.Join(root, "result")
	copyOut(".env", result)
	got, err := os.ReadFile(result)
	must(err)
	if string(got) != "ignored secret" {
		t.Fatalf("round trip=%q", got)
	}
	must(os.MkdirAll(filepath.Join(service, "shared"), 0755))
	must(os.WriteFile(filepath.Join(service, "shared", "readable"), []byte("shared"), 0644))
	copyOut("/wefty/service/shared/readable", filepath.Join(root, "shared-result"))
	must(os.WriteFile(filepath.Join(service, "root-only"), []byte("root secret"), 0600))
	request.Path = "/wefty/service/root-only"
	if _, err = engine.CopyComputerSeat(ctx, request, nil, func([]byte) error { return nil }); err == nil {
		t.Fatal("copy-out borrowed root permission")
	}
	if _, err = engine.ApplyComputerSeats(ctx, ApplyComputerSeatsRequest{authority, nil}); err != nil {
		t.Fatal(err)
	}
	if _, err = engine.CopyComputerSeat(ctx, request, nil, func([]byte) error { return nil }); err == nil {
		t.Fatal("removed Seat accepted a copy")
	}
	t.Log("Seat copy round trip, ignored file, relative/shared absolute paths, UID and permission/end refusals verified")
}
