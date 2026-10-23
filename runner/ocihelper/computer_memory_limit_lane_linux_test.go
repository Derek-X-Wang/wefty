//go:build linux && computer_seat_lane

package ocihelper

import (
	"context"
	"encoding/json"
	"errors"
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
func TestHelperComputerMemoryRM5(t *testing.T) {
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
	id := "helper-memory-" + filepath.Base(root)
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
			Workload: WorkloadInput{ImageReference: imageRef, ImageDigest: image.Target().Digest.String(), Computer: true, Limits: WorkloadLimits{MemoryBytes: 1 << 30}, ManagedVolumes: []ManagedVolumeDescriptor{{Kind: ManagedVolumeComputerDisk, ComputerStorage: &ComputerStorageReference{ComputerID: "helper-memory", StorageID: "helper-memory", StorageGeneration: 1, IntentRevision: 1, DiskBytes: 2 << 30}}}},
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
		if output := os.Getenv("WEFTY_HELPER_MEMORY_EVIDENCE"); output != "" {
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
	attempt := &containerdAttempt{authority: authority, resources: ResourceIdentity{CgroupID: id}, task: task, networkNamespace: namespace, controlDirectory: control}
	engine := &ContainerdEngine{config: NativeEngineConfig{Clock: systemClock{}, MemoryCapacityBytes: 3 << 30}, attempts: map[string]*containerdAttempt{authority.key(): attempt}, capacityReservations: map[string]*capacityReservation{authority.JobID: {memoryBytes: 1 << 30, tmpfsCeilingBytes: profileReceipt(WorkloadInput{Computer: true, Limits: WorkloadLimits{MemoryBytes: 1 << 30}}).ComputerTmpfsCeilingBytes, attempts: map[string]struct{}{authority.key(): {}}}}}

	proveComputerMemoryRM5(t, ctx, engine, authority, task, id, address)
}

func proveComputerMemoryRM5(t *testing.T, ctx context.Context, engine *ContainerdEngine, authority AttemptAuthority, task containerd.Task, id, address string) {
	t.Helper()
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	proc := fmt.Sprintf("/proc/%d", task.Pid())
	cgroup := filepath.Join("/sys/fs/cgroup", id)
	readLimit := func() int64 {
		t.Helper()
		b, err := os.ReadFile(filepath.Join(cgroup, "memory.max"))
		must(err)
		n, err := strconv.ParseInt(strings.TrimSpace(string(b)), 10, 64)
		must(err)
		return n
	}
	checkThawed := func() {
		t.Helper()
		requested, err := os.ReadFile(filepath.Join(cgroup, "cgroup.freeze"))
		must(err)
		events, err := os.ReadFile(filepath.Join(cgroup, "cgroup.events"))
		must(err)
		frozen, err := computerMemoryFrozen(events)
		must(err)
		if strings.TrimSpace(string(requested)) != "0" || frozen {
			t.Fatalf("memory operation left Computer frozen: requested=%s events=%s", requested, events)
		}
	}
	resize := func(target int64, generation uint64) (SetComputerMemoryLimitResponse, error) {
		t.Helper()
		request := SetComputerMemoryLimitRequest{authority, target, generation}
		result, err := engine.SetComputerMemoryLimit(ctx, request)
		checkThawed()
		return result, err
	}
	check := func(r SetComputerMemoryLimitResponse, want int64) {
		t.Helper()
		_, _, _, err := verifyComputerCgroupMemoryPolicy("/sys/fs/cgroup", id, want)
		must(err)
		if r.MemoryLimitBytes != want || r.ReservedMemoryBytes != want || readLimit() != want || engine.capacityReservations[authority.JobID].memoryBytes != want {
			t.Fatalf("limit/reservation mismatch: %+v actual=%d", r, readLimit())
		}
	}
	wait := func(g uint64, place int, state string) {
		t.Helper()
		deadline := time.NewTimer(65 * time.Second)
		defer deadline.Stop()
		tick := time.NewTicker(200 * time.Millisecond)
		defer tick.Stop()
		for {
			r, err := engine.ObserveComputerSeats(ctx, ObserveComputerSeatsRequest{Authority: authority, Generation: g})
			must(err)
			for _, s := range r.Seats {
				if s.Place == place && s.State == state {
					if state == "cleaned" && !s.CleanupComplete {
						t.Fatal("missing cleanup evidence")
					}
					return
				}
				if s.State == "failed" {
					t.Fatalf("Seat failed: %+v", r)
				}
			}
			select {
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			case <-deadline.C:
				t.Fatalf("Seat %d never %s", place, state)
			case <-tick.C:
			}
		}
	}
	one := ComputerSeat{1, "seat1", 7102, 7103}
	two := ComputerSeat{2, "seat2", 7104, 7105}
	first, err := engine.ApplyComputerSeats(ctx, ApplyComputerSeatsRequest{authority, []ComputerSeat{one}})
	must(err)
	wait(first.Generation, 1, "ready")
	grown, err := resize(2<<30, first.Generation)
	must(err)
	check(grown, 2<<30)
	if grown.FreezeRequests != 0 {
		t.Fatalf("grow froze Computer: %+v", grown)
	}
	added, err := engine.ApplyComputerSeats(ctx, ApplyComputerSeatsRequest{authority, []ComputerSeat{one, two}})
	must(err)
	wait(added.Generation, 2, "ready")
	removed, err := engine.ApplyComputerSeats(ctx, ApplyComputerSeatsRequest{authority, []ComputerSeat{one}})
	must(err)
	// An image answer alone is insufficient: no helper observation has yet made
	// the positive absence decision for the omitted Seat.
	_, err = resize(1<<30, removed.Generation)
	var refusal *SeatOperationRefusalError
	if !errors.As(err, &refusal) || refusal.Reason != SeatRefusalCleanupNotConfirmed {
		t.Fatalf("shrank before cleanup evidence: %v", err)
	}
	wait(removed.Generation, 2, "cleaned")
	run := func(args ...string) {
		t.Helper()
		argv := []string{"--address", address, "--namespace", ContainerdNamespace, "tasks", "exec", "--exec-id", fmt.Sprintf("memory-proof-%d", time.Now().UnixNano()), "--user", "2001:2001", id}
		out, err := exec.CommandContext(ctx, "ctr", append(argv, args...)...).CombinedOutput()
		if err != nil {
			t.Fatalf("memory fixture: %v %s", err, out)
		}
	}
	run("python3", "-c", `import os,time
if os.fork(): os._exit(0)
os.setsid()
if os.fork(): os._exit(0)
buf=bytearray(1152*1024*1024)
for n in range(0,len(buf),4096): buf[n]=1
open('/wefty/service/seats/1/home/memory-ready','w').write('ready')
while not os.path.exists('/wefty/service/seats/1/home/memory-release'): time.sleep(.1)
`)
	for i := 0; ; i++ {
		if _, err := os.Stat(filepath.Join(proc, "root/wefty/service/seats/1/home/memory-ready")); err == nil {
			break
		}
		if i == 199 {
			t.Fatal("remaining Seat did not allocate memory")
		}
		time.Sleep(100 * time.Millisecond)
	}
	held, err := resize(1<<30, removed.Generation)
	must(err)
	check(held, 2<<30)
	if held.FreezeRequests != 0 {
		t.Fatalf("held shrink froze Computer: %+v", held)
	}
	if held.Reason != "memory_in_use" || held.HeldMemoryBytes != 1<<30 || held.MemoryCurrentBytes <= 1<<30 {
		t.Fatalf("usage failed to hold room: %+v", held)
	}
	// The held GiB is unavailable to another Computer too.
	engine.capacityMu.Lock()
	available, err := engine.availableComputerMemoryLocked()
	engine.capacityMu.Unlock()
	must(err)
	if available != 1<<30 {
		t.Fatalf("held room missing from admission: %d", available)
	}
	run("sh", "-c", "touch /wefty/service/seats/1/home/memory-release")
	var shrunk SetComputerMemoryLimitResponse
	for i := 0; ; i++ {
		shrunk, err = resize(1<<30, removed.Generation)
		must(err)
		if shrunk.Reason == "" {
			break
		}
		if i == 99 {
			t.Fatalf("released use still holds room: %+v", shrunk)
		}
		time.Sleep(200 * time.Millisecond)
	}
	check(shrunk, 1<<30)
	// 960 MiB remains above the fixed tmpfs floor. This must reach cleanup
	// proof validation, rather than being hidden by the lower floor refusal.
	_, err = resize(960<<20, removed.Generation)
	if !errors.As(err, &refusal) || refusal.Reason != SeatRefusalCleanupNotConfirmed {
		t.Fatalf("used cleanup proof was reused: %v", err)
	}
	// At creation all six memory-backed mounts were bounded by half the cap.
	// The live operation preserves these ceilings and refuses a lower floor.
	_, err = resize(512<<20, removed.Generation)
	if !errors.As(err, &refusal) || refusal.Reason != SeatRefusalTmpfsFloor {
		t.Fatalf("tmpfs floor not enforced: %v", err)
	}
	payload, err := json.MarshalIndent(map[string]any{"version": 1, "acceptance": "RM-5", "platform": "linux/" + runtime.GOARCH, "grow": grown, "held": held, "shrink": shrunk, "cleanup_complete": true, "held_room_visible": true, "tmpfs_floor_refused": true, "cleanup_reuse_refused": true, "freezer_checked": true, "image": os.Getenv("WEFTY_SEAT_IMAGE")}, "", "  ")
	must(err)
	if output := os.Getenv("WEFTY_HELPER_MEMORY_EVIDENCE"); output != "" {
		must(os.WriteFile(output, payload, 0600))
	}
	t.Logf("RM-5: %s", payload)
}
