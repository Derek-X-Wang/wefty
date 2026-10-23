//go:build linux && computer_seat_lane

package ocihelper

import (
	"bytes"
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
func TestHelperComputerSeatExecIM3(t *testing.T) {
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
	id := "helper-seat-exec-" + filepath.Base(root)
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
		if output := os.Getenv("WEFTY_HELPER_SEAT_EXEC_EVIDENCE"); output != "" {
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
			last, lastErr = engine.ObserveComputerSeats(ctx, ObserveComputerSeatsRequest{authority, generation})
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
	request := ExecComputerSeatRequest{authority, applied.Generation, 1, nil, time.Minute}
	run := func(args ...string) ([]byte, []byte, *SeatCommandExit) {
		t.Helper()
		req := request
		req.Args = args
		var stdout, stderr bytes.Buffer
		var exit *SeatCommandExit
		must(engine.ExecComputerSeat(ctx, req, func(e SeatCommandEvent) error {
			if e.Exit != nil {
				exit = e.Exit
			} else if e.Stream == "stdout" {
				stdout.Write(e.Data)
			} else {
				stderr.Write(e.Data)
			}
			return nil
		}))
		if exit == nil {
			t.Fatal("missing exit")
		}
		return stdout.Bytes(), stderr.Bytes(), exit
	}
	stdout, stderr, exit := run("python3", "-I", "-S", "-c", `import os,json,subprocess,sys
subprocess.run(['xdpyinfo'],check=True,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
security={k:v.strip() for k,v in (line.split(':',1) for line in open('/proc/self/status')) if k in ('CapEff','CapBnd','CapAmb','NoNewPrivs')}
print(json.dumps({'security':security,'uid':os.getuid(),'gid':os.getgid(),'groups':os.getgroups(),'cwd':os.getcwd(),'home':os.environ['HOME'],'display':os.environ['DISPLAY'],'xauthority':os.environ['XAUTHORITY'],'env':dict(os.environ)}))
sys.stderr.write('seat-stderr')
sys.exit(23)`)
	var identityProof struct {
		UID, GID                       int
		Groups                         []int
		Cwd, Home, Display, Xauthority string
		Env                            map[string]string
		Security                       map[string]string
	}
	must(json.Unmarshal(stdout, &identityProof))
	if identityProof.UID != 2001 || identityProof.GID != 2001 || len(identityProof.Groups) != 0 || identityProof.Cwd != "/wefty/service/seats/1" || identityProof.Home != "/wefty/service/seats/1/home" || identityProof.Display != ":1" || identityProof.Xauthority != "/wefty/service/seats/1/.Xauthority" || string(stderr) != "seat-stderr" || exit.Code == nil || *exit.Code != 23 {
		t.Fatalf("IM-3: %+v stderr=%s exit=%+v", identityProof, stderr, exit)
	}
	for _, field := range []string{"CapEff", "CapBnd", "CapAmb"} {
		value, present := identityProof.Security[field]
		if field == "CapAmb" && !present {
			continue
		}
		parsed, err := strconv.ParseUint(value, 16, 64)
		if !present || err != nil || parsed != 0 {
			t.Fatalf("Seat retained %s=%q: %v", field, value, err)
		}
	}
	if identityProof.Security["NoNewPrivs"] != "1" {
		t.Fatalf("Seat exec permits new privileges: %+v", identityProof.Security)
	}
	commandConfig, err := attempt.seats.command(request)
	must(err)
	if len(identityProof.Env) != len(commandConfig.Environment) {
		t.Fatalf("environment inherited extra keys: %+v", identityProof.Env)
	}
	for _, pair := range commandConfig.Environment {
		key, value, _ := strings.Cut(pair, "=")
		if identityProof.Env[key] != value {
			t.Fatalf("wrong env %s", key)
		}
	}
	_, _, signalExit := run("python3", "-I", "-S", "-c", "import os,signal;os.kill(os.getpid(),signal.SIGTERM)")
	if signalExit.Signal != 15 || signalExit.Code != nil {
		t.Fatalf("signal lost: %+v", signalExit)
	}
	// Deliberately keep inherited output descriptors: only the wrapper owns the
	// containerd FIFOs, so a detached program cannot hold the command open.
	_, _, backgroundExit := run("python3", "-I", "-S", "-c", `import os,time
if os.fork(): os._exit(0)
os.setsid()
if os.fork(): os._exit(0)
open('run/background.pid','w').write(str(os.getpid()))
while True: time.sleep(1)`)
	if backgroundExit.Code == nil || *backgroundExit.Code != 0 {
		t.Fatalf("background command: %+v", backgroundExit)
	}
	seatRoot := filepath.Join(fmt.Sprintf("/proc/%d/root", task.Pid()), "wefty/service/seats/1")
	fileCtx, fileCancel := context.WithTimeout(ctx, 10*time.Second)
	defer fileCancel()
	waitSeatCommandFile(t, fileCtx, filepath.Join(seatRoot, "run/background.pid"))
	// Start two commands in the same Seat. Cancel one process tree while the
	// other command and the detached program continue.
	runLong := func(name string) (context.CancelFunc, <-chan error) {
		t.Helper()
		commandCtx, stop := context.WithCancel(ctx)
		req := request
		req.Args = []string{"python3", "-I", "-S", "-c", `import os,subprocess,sys,time
child=subprocess.Popen(['sleep','180'])
open('run/'+sys.argv[1]+'.pids','w').write(str(os.getpid())+' '+str(child.pid))
while True: time.sleep(1)`, name}
		done := make(chan error, 1)
		go func() { done <- engine.ExecComputerSeat(commandCtx, req, func(SeatCommandEvent) error { return nil }) }()
		waitSeatCommandFile(t, fileCtx, filepath.Join(seatRoot, "run/"+name+".pids"))
		return stop, done
	}
	stopKeeper, keeperDone := runLong("keeper")
	defer stopKeeper()
	stopCommand, commandDone := runLong("cancelled")
	defer stopCommand()
	cancelledPIDs, err := os.ReadFile(filepath.Join(seatRoot, "run/cancelled.pids"))
	must(err)
	keeperPIDs, err := os.ReadFile(filepath.Join(seatRoot, "run/keeper.pids"))
	must(err)
	stopCommand()
	select {
	case err := <-commandDone:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("cancel result: %v", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("cancel did not stop command")
	}
	// The checking command sees the same PID namespace as the command. A
	// zombie is stopped; any live member of the cancelled tree fails the check.
	_, _, checkExit := run("python3", "-I", "-S", "-c", `import os,sys
for pid in sys.argv[1].split():
 try:
  state=open('/proc/'+pid+'/stat').read().rsplit(')',1)[1].split()[0]
  assert state=='Z', (pid,state)
 except FileNotFoundError: pass
for pid in sys.argv[2].split()+[open('run/background.pid').read()]:
 os.kill(int(pid),0)
 state=open('/proc/'+pid+'/stat').read().rsplit(')',1)[1].split()[0]
 assert state!='Z', (pid,state)`, string(cancelledPIDs), string(keeperPIDs))
	if checkExit.Code == nil || *checkExit.Code != 0 {
		t.Fatalf("cancel escaped its command: %+v", checkExit)
	}
	stopKeeper()
	mustCancel := <-keeperDone
	if !errors.Is(mustCancel, context.Canceled) {
		t.Fatalf("keeper cancel: %v", mustCancel)
	}
	// A plain '&' writer keeps its inherited stdout/stderr after its parent
	// exits. It must survive ten seconds and remain live until Seat cleanup.
	_, _, printingExit := run("/bin/sh", "-c", `python3 -u -c 'import os,time
open("run/printing.pid","w").write(str(os.getpid()))
started=None
while True:
 print("background output",flush=True)
 if started is None and os.path.exists("run/printing-command-ended"):
  started=time.monotonic()
 if started is not None and time.monotonic()-started>=10:
  open("run/printing-survived","w").write("yes")
 time.sleep(.05)' &`)
	if printingExit.Code == nil || *printingExit.Code != 0 {
		t.Fatalf("plain background start: %+v", printingExit)
	}
	// Start the ten-second proof only after the command stream has ended.
	must(os.WriteFile(filepath.Join(seatRoot, "run/printing-command-ended"), []byte("yes"), 0644))
	printingCtx, stopPrintingWait := context.WithTimeout(ctx, 20*time.Second)
	defer stopPrintingWait()
	waitSeatCommandFile(t, printingCtx, filepath.Join(seatRoot, "run/printing-survived"))
	_, _, printingCheck := run("python3", "-I", "-S", "-c", `pid=open('run/printing.pid').read()
state=open('/proc/'+pid+'/stat').read().rsplit(')',1)[1].split()[0]
assert state!='Z',state`)
	if printingCheck.Code == nil || *printingCheck.Code != 0 {
		t.Fatalf("printing background program stopped: %+v", printingCheck)
	}
	// The background program outlived both its command and another command's
	// cancel. Part 1 cleanup must now prove there are no processes of this uid.
	removed, err := engine.ApplyComputerSeats(ctx, ApplyComputerSeatsRequest{authority, nil})
	must(err)
	waitForSeat(removed.Generation, "cleaned")
	must(confirmComputerSeatAbsent(fmt.Sprintf("/proc/%d", task.Pid()), ComputerSeat{1, "seat1", 7102, 7103}))
	if output := os.Getenv("WEFTY_HELPER_SEAT_EXEC_EVIDENCE"); output != "" {
		payload, err := json.MarshalIndent(map[string]any{"version": 1, "acceptance": "IM-3", "secondary": "CF-3", "platform": "linux/" + runtime.GOARCH, "image": imageRef, "uid": identityProof.UID, "gid": identityProof.GID, "supplementary_groups": identityProof.Groups, "cwd": identityProof.Cwd, "home": identityProof.Home, "display": identityProof.Display, "x_cookie_works": true, "environment_only_seat": true, "exit_code": 23, "signal": 15, "capabilities_absent": true, "no_new_privileges": true, "plain_background_prints_after_10s": true, "cancel_stopped_process_tree": true, "other_command_survived": true, "double_fork_survived_command": true, "double_fork_survived_other_cancel": true, "cleanup_complete": true}, "", "  ")
		must(err)
		must(os.WriteFile(output, payload, 0600))
	}
	t.Log("IM-3 verified: Seat uid, folder, home, display/cookie, isolated environment, output and exit/signal; cancel confined; CF-3 double-fork survives command and stops at Seat removal")
}
