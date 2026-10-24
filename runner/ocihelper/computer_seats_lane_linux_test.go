//go:build linux && computer_seat_lane

package ocihelper

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

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
func TestHelperComputerSeatsIM5(t *testing.T) {
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
	id := "helper-seats-" + filepath.Base(root)
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
	t.Cleanup(func() {
		if output := os.Getenv("WEFTY_HELPER_SEAT_EVIDENCE"); output != "" {
			b, err := os.ReadFile(filepath.Join(root, "computer.log"))
			if err != nil {
				t.Error(err)
				return
			}
			if err := os.WriteFile(output+".computer.log", b, 0600); err != nil {
				t.Error(err)
			}
		}
	})
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
	proc := fmt.Sprintf("/proc/%d", task.Pid())
	data, err := os.ReadFile(filepath.Join(proc, "cgroup"))
	must(err)
	cgroup := ""
	for _, line := range strings.Split(string(data), "\n") {
		if strings.HasPrefix(line, "0::") {
			cgroup = filepath.Join("/sys/fs/cgroup", strings.TrimPrefix(line, "0::"))
		}
	}
	if cgroup == "" {
		t.Fatal("cgroup v2 required")
	}
	memory := func() int64 {
		t.Helper()
		b, err := os.ReadFile(filepath.Join(cgroup, "memory.current"))
		must(err)
		v, err := strconv.ParseInt(strings.TrimSpace(string(b)), 10, 64)
		must(err)
		return v
	}
	charge := func() int64 {
		t.Helper()
		b, err := os.ReadFile(filepath.Join(cgroup, "memory.stat"))
		must(err)
		v, err := seatMemoryCharge(b)
		must(err)
		return v
	}
	// Wait for root-supervisor startup before sampling the idle Computer. Use
	// the minimum of five samples so a transient boot allocation cannot hide a leak.
	boot := filepath.Join(proc, "root/run/wefty-seat-supervisor.json")
	for i := 0; ; i++ {
		if _, err := os.Stat(boot); err == nil {
			break
		}
		if i == 649 {
			t.Fatal("supervisor did not initialize")
		}
		time.Sleep(100 * time.Millisecond)
	}
	baseline := charge()
	baselineCurrent := memory()
	for i := 0; i < 4; i++ {
		time.Sleep(200 * time.Millisecond)
		if sample := charge(); sample < baseline {
			baseline = sample
		}
	}
	applied, err := engine.ApplyComputerSeats(ctx, ApplyComputerSeatsRequest{authority, []ComputerSeat{{1, "seat1", 7102, 7103}}})
	must(err)
	waitForSeat(applied.Generation, "ready")
	seat := ComputerSeat{1, "seat1", 7102, 7103}
	if err := confirmComputerSeatAbsent(proc, seat); err == nil {
		t.Fatal("absence check accepted a live ready Seat")
	}
	run := func(args ...string) {
		t.Helper()
		argv := []string{"--address", address, "--namespace", ContainerdNamespace, "tasks", "exec", "--exec-id", fmt.Sprintf("helper-proof-%d", time.Now().UnixNano()), "--user", "2001:2001", id}
		out, err := exec.CommandContext(ctx, "ctr", append(argv, args...)...).CombinedOutput()
		if err != nil {
			t.Fatalf("fixture command: %v %s", err, out)
		}
	}
	run("sh", "-c", "echo retained > /wefty/service/shared/keep")
	// The root-owned sticky folder survives Seat cleanup, but its Seat-owned
	// contents must not. Split the 128 MiB charge between top-level and nested
	// files so either leak exceeds the recovery tolerance below.
	scratchDir := filepath.Join(proc, "root/dev/shm/wefty-seat-im5-shared")
	must(os.Mkdir(scratchDir, 0777))
	must(os.Chmod(scratchDir, os.ModeSticky|0777))
	must(os.WriteFile(filepath.Join(scratchDir, "keep"), []byte("other uid\n"), 0600))
	// These Seat-owned tmpfs files remain charged after their writer dies. Only
	// the image's uid-based scratch cleanup can release their 128 MiB of shmem.
	// The detached writer also exercises cleanup beyond the desktop process group.
	run("python3", "-c", `import os,time
if os.fork(): os._exit(0)
os.setsid()
if os.fork(): os._exit(0)
for path in ('/dev/shm/wefty-seat-im5-1', '/dev/shm/wefty-seat-im5-shared/nested'):
    with open(path,'wb') as f:
        chunk=b'x'*(1024*1024)
        for _ in range(64): f.write(chunk)
        f.flush()
        os.fsync(f.fileno())
open('/wefty/service/seats/1/home/allocated','w').write('ready')
time.sleep(180)`)
	allocation := filepath.Join(proc, "root/wefty/service/seats/1/home/allocated")
	for i := 0; i < 100; i++ {
		if _, err := os.Stat(allocation); err == nil {
			break
		}
		if i == 99 {
			t.Fatal("tmpfs fixture never allocated")
		}
		time.Sleep(100 * time.Millisecond)
	}
	scratch := filepath.Join(proc, "root/dev/shm/wefty-seat-im5-1")
	nestedScratch := filepath.Join(scratchDir, "nested")
	for _, path := range []string{scratch, nestedScratch} {
		info, err := os.Stat(path)
		must(err)
		if info.Size() != 64<<20 {
			t.Fatalf("tmpfs fixture size mismatch: %s", filepath.Base(path))
		}
	}
	chargeBefore := charge()
	before := memory()
	removed, err := engine.ApplyComputerSeats(ctx, ApplyComputerSeatsRequest{authority, nil})
	must(err)
	if removed.Generation <= applied.Generation {
		t.Fatal("directive generation did not advance")
	}
	waitForSeat(removed.Generation, "cleaned")
	// Allow 32 MiB of anon/shmem accounting and root-supervisor variation.
	// This is well below the 128 MiB tmpfs fixture, so deleting the desktop
	// alone cannot pass. File cache is excluded. Five consecutive 200 ms
	// samples must return to the pre-Seat baseline plus that tolerance.
	recovery := seatMemoryRecovery{baseline: baseline, tolerance: 32 << 20, required: 5}
	chargeAfter := charge()
	recovered := recovery.observe(chargeAfter)
	for i := 0; i < 100 && !recovered; i++ {
		time.Sleep(200 * time.Millisecond)
		chargeAfter = charge()
		recovered = recovery.observe(chargeAfter)
	}
	after := memory()
	if !recovered {
		t.Fatalf("Seat anon+shmem did not return: baseline=%d before=%d after=%d tolerance=%d consecutive=%d", baseline, chargeBefore, chargeAfter, recovery.tolerance, recovery.consecutive)
	}
	for _, path := range []string{scratch, nestedScratch} {
		if _, err := os.Lstat(path); !os.IsNotExist(err) {
			t.Fatalf("Seat tmpfs fixture %s remains or cannot be inspected: %v", filepath.Base(path), err)
		}
	}
	foreign, err := os.ReadFile(filepath.Join(scratchDir, "keep"))
	must(err)
	if string(foreign) != "other uid\n" {
		t.Fatal("another uid's scratch file lost")
	}
	b, err := os.ReadFile(filepath.Join(service, "shared/keep"))
	must(err)
	if string(b) != "retained\n" {
		t.Fatal("shared file lost")
	}
	must(confirmComputerSeatAbsent(proc, ComputerSeat{1, "seat1", 7102, 7103}))
	if output := os.Getenv("WEFTY_HELPER_SEAT_EVIDENCE"); output != "" {
		payload, err := json.MarshalIndent(map[string]any{"version": 1, "acceptance": "IM-5", "platform": "linux/" + runtime.GOARCH, "image": imageRef, "add_generation": applied.Generation, "remove_generation": removed.Generation, "memory_before_bytes": before, "memory_after_bytes": after, "memory_baseline_bytes": baselineCurrent, "anon_shmem_baseline_bytes": baseline, "anon_shmem_before_bytes": chargeBefore, "anon_shmem_after_bytes": chargeAfter, "anon_shmem_tolerance_bytes": recovery.tolerance, "consecutive_baseline_samples": recovery.consecutive, "shared_survives": true, "cleanup_complete": true}, "", "  ")
		must(err)
		must(os.WriteFile(output, payload, 0600))
	}
	t.Logf("IM-5 helper: generation=%d cleanup confirmed; memory.current before=%d after=%d; anon+shmem baseline=%d after=%d consecutive=%d; top-level and nested tmpfs absent; other uid scratch and shared retained", removed.Generation, before, after, baseline, chargeAfter, recovery.consecutive)
}
