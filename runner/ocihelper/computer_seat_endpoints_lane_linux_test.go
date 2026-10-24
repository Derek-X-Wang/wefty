//go:build linux && computer_seat_lane

package ocihelper

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
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
	"golang.org/x/sys/unix"
)

// This fixture uses the production profile without adding mounts, privileges,
// namespaces or processes to its OCI spec. Both Computers stay live during proof.
func withSeatEndpointComputer(t *testing.T, imageRef, address string, prove func(context.Context, *ContainerdEngine, AttemptAuthority, containerd.Task, string)) {
	withSeatEndpointComputerNetwork(t, imageRef, address, "", prove)
}

func withSeatEndpointComputerNetwork(t *testing.T, imageRef, address, network string, prove func(context.Context, *ContainerdEngine, AttemptAuthority, containerd.Task, string)) {
	t.Helper()
	ctx, cancel := context.WithTimeout(namespaces.WithNamespace(t.Context(), ContainerdNamespace), 5*time.Minute)
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
	if network == ComputerNetworkNone {
		must(prepareComputerNoneNetworkFiles(managed))
	}
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
			Workload: WorkloadInput{ImageReference: imageRef, ImageDigest: image.Target().Digest.String(), Computer: true, ComputerNetwork: network, Limits: WorkloadLimits{MemoryBytes: 1 << 30}, ManagedVolumes: []ManagedVolumeDescriptor{{Kind: ManagedVolumeComputerDisk, ComputerStorage: &ComputerStorageReference{ComputerID: id, StorageID: id, StorageGeneration: 1, IntentRevision: 1, DiskBytes: 2 << 30}}}},
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
			payload, err := os.ReadFile(filepath.Join(root, "computer.log"))
			if err != nil {
				t.Error(err)
				return
			}
			if err := os.WriteFile(output+"."+id+".im4.computer.log", payload, 0600); err != nil {
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
	if network == ComputerNetworkNone {
		// Use the production selection and setup path. There is deliberately no
		// iptables executable or reachable upstream resolver in this fixture.
		e := &ContainerdEngine{}
		attachment, err := e.prepareComputerWorkloadNetwork(ctx, namespace, 7102, WorkloadInput{Computer: true, ComputerNetwork: network})
		must(err)
		defer attachment.close()
		if attachment.hostLink != "" || attachment.dns != nil || attachment.probe != nil {
			t.Fatal("none installed an egress attachment")
		}
	} else {
		out, err := exec.CommandContext(ctx, "nsenter", "-t", fmt.Sprint(task.Pid()), "-n", "ip", "link", "set", "lo", "up").CombinedOutput()
		if err != nil {
			t.Fatalf("loopback: %v %s", err, out)
		}
	}
	must(document.RevalidateMounts())
	must(task.Start(ctx))
	authority := testAuthority()
	authority.Class = "service"
	authority.AttemptID = id
	authority.JobID = id
	attempt := &containerdAttempt{authority: authority, task: task, networkNamespace: namespace, controlDirectory: control, networkMode: network}
	engine := &ContainerdEngine{config: NativeEngineConfig{Clock: systemClock{}}, attempts: map[string]*containerdAttempt{authority.key(): attempt}}

	defer func() {
		attempt.seatsMu.Lock()
		defer attempt.seatsMu.Unlock()
		if attempt.seats != nil {
			for _, entry := range attempt.seats.entries {
				entry.closeEndpointPins()
			}
		}
	}()
	prove(ctx, engine, authority, task, id)
}

func TestHelperSeatEndpointIM4(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Fatal("computer_seat_lane requires root")
	}
	image, address := os.Getenv("WEFTY_SEAT_IMAGE"), os.Getenv("WEFTY_SEAT_CONTAINERD")
	if image == "" || address == "" {
		t.Fatal("Seat lane provisioning missing")
	}
	withSeatEndpointComputer(t, image, address, func(ctx context.Context, first *ContainerdEngine, a AttemptAuthority, task containerd.Task, id string) {
		ready := func(e *ContainerdEngine, a AttemptAuthority, seats []ComputerSeat) ObserveComputerSeatsResponse {
			t.Helper()
			applied, err := e.ApplyComputerSeats(ctx, ApplyComputerSeatsRequest{a, seats})
			if err != nil {
				t.Fatal(err)
			}
			deadline := time.NewTimer(65 * time.Second)
			defer deadline.Stop()
			ticker := time.NewTicker(200 * time.Millisecond)
			defer ticker.Stop()
			for {
				result, err := e.ObserveComputerSeats(ctx, ObserveComputerSeatsRequest{Authority: a, Generation: applied.Generation})
				all := err == nil && len(result.Seats) == len(seats)
				for _, seat := range result.Seats {
					all = all && seat.State == "ready" && len(seat.Endpoints) == 2
					if seat.State == "failed" {
						t.Fatalf("Seat failed: %+v", result)
					}
				}
				if all {
					return result
				}
				select {
				case <-deadline.C:
					t.Fatalf("Seat endpoints never ready: %+v %v", result, err)
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				case <-ticker.C:
				}
			}
		}
		result := ready(first, a, []ComputerSeat{{1, "seat1", 7102, 7103}, {2, "seat2", 7104, 7105}})
		dial := func(e *ContainerdEngine, r DialSeatEndpointRequest) error {
			return probeComputerSeatGreeting(ctx, func(ctx context.Context) (net.Conn, error) {
				client, helper := net.Pipe()
				go func() { defer helper.Close(); _ = e.DialSeatEndpoint(ctx, r, helper) }()
				var marker [1]byte
				_ = client.SetReadDeadline(time.Now().Add(3 * time.Second))
				if _, err := io.ReadFull(client, marker[:]); err != nil {
					client.Close()
					return nil, err
				}
				if marker[0] != attemptPortBackendReady {
					client.Close()
					return nil, fmt.Errorf("wrong backend marker")
				}
				_ = client.SetDeadline(time.Time{})
				return client, nil
			})
		}
		for _, observation := range result.Seats {
			for _, name := range observation.Endpoints {
				if err := dial(first, DialSeatEndpointRequest{a, result.Generation, observation.Place, name}); err != nil {
					t.Fatalf("own endpoint greeting: %v", err)
				}
			}
		}
		// The refusal is observed before any stream-ready byte can be published.
		refuse := func(e *ContainerdEngine, r DialSeatEndpointRequest) {
			t.Helper()
			stream := &seatRefusalStream{}
			if err := e.DialSeatEndpoint(ctx, r, stream); err == nil || stream.wrote {
				t.Fatalf("unauthorized endpoint reached: %+v err=%v wrote=%v", r, err, stream.wrote)
			}
		}
		view := result.Seats[0].Endpoints["view"]
		refuse(first, DialSeatEndpointRequest{a, result.Generation, 2, view})
		stale := a
		stale.FencingToken = "old"
		refuse(first, DialSeatEndpointRequest{stale, result.Generation, 1, view})
		run := func(user string, args ...string) {
			t.Helper()
			argv := []string{"--address", address, "--namespace", ContainerdNamespace, "tasks", "exec", "--exec-id", fmt.Sprintf("endpoint-proof-%d", time.Now().UnixNano()), "--user", user, id}
			if out, err := exec.CommandContext(ctx, "ctr", append(argv, args...)...).CombinedOutput(); err != nil {
				t.Fatalf("confinement: %v %s", err, out)
			}
		}
		for _, pair := range [][2]int{{1, 2}, {2, 1}} {
			run(fmt.Sprintf("%d:%d", 2000+pair[0], 2000+pair[0]), "python3", "-c", fmt.Sprintf(`import socket
for name in ('view.ws','control.ws','view.sock','control.sock'):
 s=socket.socket(socket.AF_UNIX);s.settimeout(.2)
 try:s.connect('/wefty/service/seats/%d/run/' + name)
 except PermissionError:pass
 else:raise AssertionError('other Seat endpoint reachable: '+name)
 finally:s.close()
`, pair[1]))
			run(fmt.Sprintf("%d:%d", 2000+pair[0], 2000+pair[0]), "sh", "-c", fmt.Sprintf("if DISPLAY=:%d XAUTHORITY=/wefty/service/seats/%d/.Xauthority xdpyinfo >/dev/null 2>&1; then exit 1; fi", pair[1], pair[0]))
		}
		withSeatEndpointComputer(t, image, address, func(_ context.Context, second *ContainerdEngine, b AttemptAuthority, _ containerd.Task, peerID string) {
			neighbour := ready(second, b, []ComputerSeat{{1, "seat1", 7102, 7103}, {3, "seat3", 7106, 7107}})
			name := neighbour.Seats[0].Endpoints["view"]
			// Both roots have the same Seat, uid, path and directive generation.
			// Turn only B green, so wrong-root observation or dial cannot pass.
			probeHelperSeatFrame(t, ctx, first, DialSeatEndpointRequest{a, result.Generation, 1, view}, "frame", "ff0000")
			probeHelperSeatFrame(t, ctx, second, DialSeatEndpointRequest{b, neighbour.Generation, 1, neighbour.Seats[0].Endpoints["control"]}, "input", "00ff00")
			probeHelperSeatFrame(t, ctx, second, DialSeatEndpointRequest{b, neighbour.Generation, 1, name}, "frame", "00ff00")
			probeHelperSeatFrame(t, ctx, first, DialSeatEndpointRequest{a, result.Generation, 1, view}, "frame", "ff0000")
			peerLocal := func() {
				argv := []string{"--address", address, "--namespace", ContainerdNamespace, "tasks", "exec", "--exec-id", fmt.Sprintf("peer-live-%d", time.Now().UnixNano()), "--user", "2001:2001", peerID, "python3", "-c", `import socket
for path in ('/wefty/service/seats/1/run/view.ws','/wefty/service/seats/1/run/control.ws','/tmp/.X11-unix/X3','\0/tmp/.X11-unix/X3'):
 with socket.socket(socket.AF_UNIX) as s:
  s.settimeout(.2);s.connect(path)
`}
				if out, err := exec.CommandContext(ctx, "ctr", argv...).CombinedOutput(); err != nil {
					t.Fatalf("peer-local sockets not live: %v %s", err, out)
				}
			}
			peerLocal()
			if err := dial(second, DialSeatEndpointRequest{b, neighbour.Generation, 1, name}); err != nil {
				t.Fatalf("neighbour not live: %v", err)
			}
			refuse(first, DialSeatEndpointRequest{a, result.Generation, 1, name})
			refuse(second, DialSeatEndpointRequest{b, neighbour.Generation, 1, view})
			// The same-Seat frames above prove root selection. A distinct
			// live X display additionally tests namespace socket refusal.
			run("2001:2001", "python3", "-c", `import socket
for path in ('/tmp/.X11-unix/X3','\0/tmp/.X11-unix/X3'):
 s=socket.socket(socket.AF_UNIX);s.settimeout(.2)
 try:s.connect(path)
 except OSError:pass
 else:raise AssertionError('neighbour endpoint reachable: '+repr(path))
 finally:s.close()
`)
			if err := dial(second, DialSeatEndpointRequest{b, neighbour.Generation, 1, name}); err != nil {
				t.Fatalf("neighbour withdrew during crossover proof: %v", err)
			}
			peerLocal()
			probeHelperSeatFrame(t, ctx, second, DialSeatEndpointRequest{b, neighbour.Generation, 1, name}, "frame", "00ff00")
			probeHelperSeatFrame(t, ctx, first, DialSeatEndpointRequest{a, result.Generation, 1, view}, "frame", "ff0000")
			if err := dial(first, DialSeatEndpointRequest{a, result.Generation, 1, view}); err != nil {
				t.Fatalf("source Computer withdrew during crossover proof: %v", err)
			}
		})
	})
}

type seatRefusalStream struct{ wrote bool }

func (s *seatRefusalStream) Write(p []byte) (int, error) { s.wrote = true; return len(p), nil }
func (s *seatRefusalStream) Read(p []byte) (int, error)  { return 0, io.EOF }
func (s *seatRefusalStream) Close() error                { return nil }

// Run the real RFB framebuffer probe through DialSeatEndpoint, not a direct
// task-root connection. A wrong-root dial must show the other colour or fail.
func probeHelperSeatFrame(t *testing.T, ctx context.Context, engine *ContainerdEngine, request DialSeatEndpointRequest, mode, colour string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(ctx, 25*time.Second)
	defer cancel()
	pair, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_STREAM|unix.SOCK_CLOEXEC, 0)
	if err != nil {
		t.Fatal(err)
	}
	clientFile, helperFile := os.NewFile(uintptr(pair[0]), "Seat frame client"), os.NewFile(uintptr(pair[1]), "Seat frame helper")
	defer clientFile.Close()
	defer helperFile.Close()
	helper, err := net.FileConn(helperFile)
	if err != nil {
		t.Fatal(err)
	}
	helperFile.Close()
	go func() { defer helper.Close(); _ = engine.DialSeatEndpoint(ctx, request, helper) }()
	setup, err := net.FileConn(clientFile)
	if err != nil {
		t.Fatal(err)
	}
	_ = setup.SetReadDeadline(time.Now().Add(3 * time.Second))
	var marker [1]byte
	_, err = io.ReadFull(setup, marker[:])
	setup.Close()
	if err != nil || marker[0] != attemptPortBackendReady {
		t.Fatalf("Seat frame backend not ready: marker=%v err=%v", marker, err)
	}
	_, source, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("frame probe source unavailable")
	}
	script := filepath.Join(filepath.Dir(source), "../../examples/computer/seat-probe.py")
	command := exec.CommandContext(ctx, "python3", script, mode, "fd:3", colour)
	command.ExtraFiles = []*os.File{clientFile}
	var output bytes.Buffer
	command.Stdout = &output
	command.Stderr = &output
	err = command.Start()
	clientFile.Close()
	if err == nil {
		err = command.Wait()
	}
	t.Logf("Computer %s Seat %d %s expected %s: %s", request.Authority.AttemptID, request.Place, mode, colour, output.String())
	if err != nil {
		t.Fatalf("helper selected the wrong Computer screen or failed: %v %s", err, output.String())
	}
}
