//go:build linux && computer_seat_lane

package computerconformance_test

import (
	"bytes"
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

	"github.com/Derek-X-Wang/wefty/internal/computerconformance"
	"github.com/Derek-X-Wang/wefty/runner/ocihelper"
	containerd "github.com/containerd/containerd/v2/client"
	"github.com/containerd/containerd/v2/core/containers"
	"github.com/containerd/containerd/v2/core/content"
	"github.com/containerd/containerd/v2/core/mount"
	"github.com/containerd/containerd/v2/core/snapshots"
	"github.com/containerd/containerd/v2/pkg/cio"
	"github.com/containerd/containerd/v2/pkg/namespaces"
	"github.com/opencontainers/image-spec/identity"
	imagev1 "github.com/opencontainers/image-spec/specs-go/v1"
	specs "github.com/opencontainers/runtime-spec/specs-go"
)

// This lane uses BuildRuntimeSpec without modifying its returned OCI spec.
// It proves image duties, including P7 confinement; helper operations have their own lane test.
func TestDefaultComputerSeatsAtProductionTimings(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Fatal("real Computer profile requires root on the Linux CI runner")
	}
	image := requiredSeatEnv(t, "WEFTY_SEAT_IMAGE")
	negative := requiredSeatEnv(t, "WEFTY_SEAT_NEGATIVE_IMAGE")
	output := requiredSeatEnv(t, "WEFTY_SEAT_EVIDENCE")
	kit := requiredSeatEnv(t, "WEFTY_SEAT_INSTALL_KIT")
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	client, err := containerd.New(requiredSeatEnv(t, "WEFTY_SEAT_CONTAINERD"), containerd.WithDefaultNamespace(ocihelper.ContainerdNamespace))
	seatMust(t, err)
	t.Cleanup(func() {
		if err := client.Close(); err != nil {
			t.Error(err)
		}
	})
	ctx = namespaces.WithNamespace(ctx, ocihelper.ContainerdNamespace)
	evidence := computerconformance.SeatEvidence{Version: 1, Image: image, Platform: "linux/" + runtime.GOARCH, Observations: map[string]bool{}, MemoryLimitBytes: 1 << 30}
	defer func() {
		payload, err := json.MarshalIndent(evidence, "", "  ")
		seatMust(t, err)
		seatMust(t, os.WriteFile(output, payload, 0600))
	}()
	root := t.TempDir()
	service := filepath.Join(root, "managed", "service")
	h := startSeatComputer(t, ctx, client, image, root, service, kit)
	runtimeErr := computerconformance.CheckSeatCommandRuntime(func(args ...string) error { return h.tryExec("2001:2001", args...) })
	evidence.Observations["seat_command_python"] = runtimeErr == nil
	seatMust(t, runtimeErr)
	passwd := h.exec("0:0", "cat", "/etc/passwd")
	evidence.Observations["passwd_entries"] = computerconformance.CheckSeatUsers([]byte(passwd)) == nil
	evidence.Observations["profile_unmodified"] = h.profileUnmodified
	evidence.Observations["readonly_root"] = h.exec("0:0", "sh", "-c", "if touch /rootfs-must-be-readonly 2>/dev/null; then exit 1; fi; echo readonly") == "readonly"
	evidence.TmpfsCeilingBytes = h.tmpfsBytes
	evidence.Observations["tools_present"] = h.exec("2001:2001", "sh", "-c", "for x in git curl python3 node npm; do \"$x\" --version >/dev/null || exit; done; command -v chromium >/dev/null; echo present") == "present"
	// No Seats until the first directive; a new Seat added after 60 seconds
	// must get a fresh budget rather than inherit the Computer boot deadline.
	h.directive(1, 1)
	h.ready(1)
	h.probe("frame", 1, "ff0000")
	evidence.Observations["own_display"] = h.exec("2001:2001", "sh", "-c", seatEnv(1)+"xdpyinfo | grep '1280x800 pixels'") != ""
	h.exec("2001:2001", "sh", "-c", seatEnv(1)+"echo keep > /wefty/service/shared/keep")
	h.exec("2001:2001", "sh", "-c", seatEnv(1)+"cp -R /opt/seat-install-kit \"$HOME/test-kit\"; du -sb \"$HOME/test-kit\" | cut -f1")
	size := h.exec("2001:2001", "sh", "-c", seatEnv(1)+"du -sb \"$HOME/test-kit\" | cut -f1")
	evidence.InstallBytes, err = strconv.ParseInt(strings.TrimSpace(size), 10, 64)
	seatMust(t, err)
	// Prove the installed browser works under the unchanged Computer profile.
	h.exec("2001:2001", "sh", "-c", seatEnv(1)+`cd "$HOME/test-kit"; PLAYWRIGHT_BROWSERS_PATH="$HOME/test-kit/browsers" node -e 'const {chromium}=require("playwright"); (async()=>{let b=await chromium.launch({headless:true,args:["--no-sandbox","--disable-dev-shm-usage"]}); let p=await b.newPage(); await p.setContent("<title>Seat install works</title>"); if(await p.title()!=="Seat install works")process.exit(1); await b.close()})().catch(e=>{console.error(e);process.exit(1)})'`)
	// Real elapsed time is intentional here; state timing is separately covered
	// with an injected clock in test_seat_supervisor.py.
	timer := time.NewTimer(61 * time.Second)
	select {
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	case <-timer.C:
	}
	h.directive(2, 1, 2)
	h.ready(2)
	h.probe("frame", 2, "0000aa")
	evidence.Observations["second_browser"] = true
	evidence.Observations["deadline_per_seat"] = true

	h.exec("2001:2001", "sh", "-c", seatEnv(1)+"echo live > \"$HOME/restart-marker\"")
	for _, component := range []string{"browser", "edges"} {
		path := "/wefty/service/seats/1/run/" + component + ".pid"
		original := h.exec("0:0", "cat", path)
		h.exec("0:0", "kill", "-KILL", original)
		h.wait(component+" restarted", func() bool { return h.exec("0:0", "cat", path) != original })
		lastProbe := "probe not completed"
		h.wait(component+" screen recovered after component exit", func() bool {
			out, err := h.probeCommand("frame", 1, "ff0000").CombinedOutput()
			lastProbe = fmt.Sprintf("error=%v output=%s", err, out)
			return err == nil
		}, func() string { return lastProbe })
		h.ready(1)
		if h.exec("2001:2001", "cat", "/wefty/service/seats/1/home/restart-marker") != "live" {
			t.Fatal("transient component exit ended the Seat")
		}
		h.probe("frame", 2, "0000aa")
	}
	h.probe("view-input", 1, "ff0000")
	evidence.Observations["view_ignores_input"] = true
	h.probe("input", 1, "00ff00")
	h.probe("frame", 2, "0000aa")
	evidence.Observations["own_screen_input"] = true
	evidence.Observations["other_files_refused"] = h.exec("2002:2002", "sh", "-c", "if cat /wefty/service/seats/1/.Xauthority 2>/dev/null; then exit 1; fi; echo refused") == "refused"
	evidence.Observations["other_display_refused"] = h.exec("2002:2002", "sh", "-c", seatEnv(2)+"if DISPLAY=:1 xdpyinfo >/dev/null 2>&1; then exit 1; fi; echo refused") == "refused"
	for _, pair := range [][2]int{{1, 2}, {2, 1}} {
		h.exec(fmt.Sprintf("%d:%d", 2000+pair[0], 2000+pair[0]), "python3", "-c", fmt.Sprintf(`import socket
for name in ('view.ws','control.ws','view.sock','control.sock'):
 s=socket.socket(socket.AF_UNIX);s.settimeout(.2)
 try:s.connect('/wefty/service/seats/%d/run/'+name)
 except PermissionError:pass
 else:raise AssertionError('other Seat endpoint reachable: '+name)
 finally:s.close()
for port in (7102,7103,7104,7105):
 s=socket.socket();s.settimeout(.2)
 try:s.connect(('127.0.0.1',port))
 except OSError:pass
 else:raise AssertionError('legacy Seat TCP endpoint still reachable')
 finally:s.close()
`, pair[1]))
	}
	evidence.Observations["other_endpoints_refused"] = true
	// Both Computers use Seat 1 and its identical socket names. Different
	// colours prove their roots stay separate; display 3 adds an abstract-socket refusal.
	peerRoot := t.TempDir()
	peer := startSeatComputer(t, ctx, client, image, peerRoot, filepath.Join(peerRoot, "managed/service"), kit)
	peer.directive(1, 1, 3)
	peer.ready(1)
	peer.ready(3)
	h.probe("frame", 1, "00ff00")
	peer.probe("frame", 1, "ff0000")
	script, err := os.ReadFile("../../examples/computer/seat-probe.py")
	seatMust(t, err)
	// Same uid and socket spelling in both Computers must show their own colour.
	localFrames := func() {
		t.Log(h.exec("2001:2001", "python3", "-c", string(script), "frame", "/wefty/service/seats/1/run/view.ws", "00ff00"))
		t.Log(peer.exec("2001:2001", "python3", "-c", string(script), "frame", "/wefty/service/seats/1/run/view.ws", "ff0000"))
	}
	localFrames()
	peerLocal := func() {
		peer.exec("2001:2001", "python3", "-c", `import socket
for path in ('/wefty/service/seats/1/run/view.ws','/wefty/service/seats/1/run/control.ws','/tmp/.X11-unix/X3','\0/tmp/.X11-unix/X3'):
 with socket.socket(socket.AF_UNIX) as s:
  s.settimeout(.2);s.connect(path)
`)
	}
	peerLocal()
	h.exec("2001:2001", "python3", "-c", `import socket
for path in ('/tmp/.X11-unix/X3','\0/tmp/.X11-unix/X3'):
 s=socket.socket(socket.AF_UNIX);s.settimeout(.2)
 try:s.connect(path)
 except OSError:pass
 else:raise AssertionError('neighbour Computer reachable: '+repr(path))
 finally:s.close()
`)
	peer.probe("frame", 1, "ff0000")
	h.probe("frame", 1, "00ff00")
	localFrames()
	peerLocal()
	peer.stop()
	evidence.Observations["cross_computer_endpoints_refused"] = true
	for _, place := range []int{1, 2} {
		h.driver(place, false)
	}
	h.driver(2, true)
	h.wait("per-Seat driver consumed", func() bool {
		return strings.Contains(h.exec("0:0", "cat", "/wefty/service/seats/2/run/driver-state.json"), `"human_driving":true`) && strings.Contains(h.exec("0:0", "cat", "/wefty/service/seats/1/run/driver-state.json"), `"human_driving":false`)
	})
	evidence.Observations["driver_per_seat"] = true
	ipc := h.exec("2002:2002", "python3", "-c", `import ctypes,json,os
libc=ctypes.CDLL(None,use_errno=True)
objects={}
objects['shm']=libc.shmget(0,32<<20,0o1600)
objects['sem']=libc.semget(0,1,0o1600)
objects['msg']=libc.msgget(0,0o1600)
assert all(value>=0 for value in objects.values()),ctypes.get_errno()
libc.shmat.restype=ctypes.c_void_p
address=libc.shmat(objects['shm'],None,0)
assert address!=ctypes.c_void_p(-1).value
ctypes.memset(address,1,32<<20)
libc.shmdt.argtypes=[ctypes.c_void_p]
assert libc.shmdt(address)==0
class Attr(ctypes.Structure):
 _fields_=[('flags',ctypes.c_long),('maxmsg',ctypes.c_long),('msgsize',ctypes.c_long),('curmsgs',ctypes.c_long),('pad',ctypes.c_long*4)]
attr=Attr();attr.maxmsg=1;attr.msgsize=128
queue=libc.mq_open(b'/wefty-seat2-conformance',os.O_CREAT|os.O_EXCL|os.O_RDWR,0o600,ctypes.byref(attr))
assert queue>=0,ctypes.get_errno()
assert libc.mq_close(queue)==0
print(json.dumps(objects))`)
	var ipcObjects map[string]int
	seatMust(t, json.Unmarshal([]byte(ipc), &ipcObjects))
	// Detached grandchildren are not confined to the shell's process group.
	h.exec("2002:2002", "python3", "-c", `import os,time; p=os.fork();
if p==0:
 os.setsid(); p=os.fork()
 if p==0:
  open('/wefty/service/seats/2/detached-pid','w').write(str(os.getpid())); time.sleep(300)
 os._exit(0)
os.waitpid(p,0)`)
	h.wait("double-forked Seat program", func() bool { return h.tryExec("0:0", "test", "-s", "/wefty/service/seats/2/detached-pid") == nil })
	before := h.memory()
	for i := 0; i < 2; i++ {
		before = min(before, h.memory())
	}
	evidence.MemoryBeforeRemovalBytes = before
	h.directive(3, 1)
	h.wait("generation-bound cleanup proof", func() bool {
		b, err := readSeatStatusAnswer(h.status, "2.json")
		if err != nil {
			return false
		}
		var answer struct {
			Generation      int
			State           string
			CleanupComplete bool `json:"cleanup_complete"`
		}
		seatMust(t, json.Unmarshal(b, &answer))
		return answer.Generation == 3 && answer.State == "removed" && answer.CleanupComplete
	})
	h.wait("Seat folder removed", func() bool { return h.tryExec("0:0", "test", "!", "-e", "/wefty/service/seats/2") == nil })
	evidence.Observations["removed_folder_absent"] = true
	evidence.Observations["removed_processes_absent"] = h.exec("0:0", "sh", "-c", `if ps -eo uid=,stat= | awk '$1==2002 && $2 !~ /^Z/ {found=1} END {exit !found}'; then exit 1; fi; echo absent`) == "absent"
	command, err := seatEndpointAbsenceCommand(ctx, fmt.Sprintf("/proc/%d/root", h.pid), fmt.Sprintf("/proc/%d/net/unix", h.pid), 2)
	seatMust(t, err)
	absence, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("removed Seat endpoints: %v %s", err, absence)
	}
	evidence.Observations["removed_endpoints_absent"] = strings.TrimSpace(string(absence)) == "absent"

	h.exec("0:0", "test", "!", "-e", "/tmp/.X11-unix/X2")
	h.exec("0:0", "test", "!", "-e", "/tmp/.X2-lock")
	h.exec("0:0", "test", "!", "-e", "/dev/mqueue/wefty-seat2-conformance")
	for kind, id := range ipcObjects {
		h.exec("0:0", "python3", "-c", fmt.Sprintf(`from pathlib import Path
rows=Path('/proc/sysvipc/%s').read_text().splitlines()
index=rows[0].split().index('%s')
assert all(int(row.split()[index])!=%d for row in rows[1:])`, kind, map[string]string{"shm": "shmid", "sem": "semid", "msg": "msqid"}[kind], id))
	}
	evidence.Observations["shared_survives"] = h.exec("0:0", "cat", "/wefty/service/shared/keep") == "keep"
	stable := 0
	var after int64
	h.wait("Seat memory returned by at least 64 MiB", func() bool {
		sample := h.memory()
		if computerconformance.MemoryReclaimed(before, sample) {
			stable++
			after = max(after, sample)
		} else {
			stable = 0
			after = 0
		}
		return stable >= 3
	})
	evidence.MemoryAfterRemovalBytes = after
	evidence.Observations["memory_returns"] = true
	h.probe("frame", 1, "00ff00")
	h.stop()
	// A new attempt gets fresh control and status folders, the same Storage.
	h = startSeatComputer(t, ctx, client, image, filepath.Join(root, "restart"), service, kit)
	h.wait("restart Seat folders cleared", func() bool {
		return h.exec("0:0", "sh", "-c", "find /wefty/service/seats -mindepth 1 -maxdepth 1 | wc -l") == "0"
	})
	evidence.Observations["restart_folders_absent"] = true
	evidence.Observations["restart_processes_absent"] = h.exec("0:0", "sh", "-c", `if ps -eo uid= | awk '$1>=2001 && $1<=2004 {found=1} END {exit !found}'; then exit 1; fi; echo absent`) == "absent"
	entries, err := h.status.ReadDir(-1)
	seatMust(t, err)
	evidence.Observations["restart_answers_absent"] = len(entries) == 0
	if h.exec("0:0", "cat", "/wefty/service/shared/keep") != "keep" {
		t.Fatal("restart lost shared file")
	}
	h.stop()
	// Actual negative image under the same profile, not a numeric-uid mock.
	h = startSeatComputer(t, ctx, client, negative, filepath.Join(root, "negative"), service, kit)
	h.directive(1, 4)
	h.wait("missing passwd refusal", func() bool {
		b, err := readSeatStatusAnswer(h.status, "4.json")
		if err != nil {
			return false
		}
		var v struct{ State, Reason string }
		if json.Unmarshal(b, &v) != nil {
			return false
		}
		return v.State == "failed" && v.Reason == "passwd_entry_missing"
	})
	if h.tryExec("0:0", "test", "!", "-e", "/wefty/service/seats/4") != nil {
		t.Fatal("missing passwd left a Seat folder")
	}
	evidence.Observations["missing_passwd_refused"] = true
	h.stop()
	report := computerconformance.CheckSeats(evidence)
	if report.Status != computerconformance.StatusPass {
		t.Fatalf("Seat conformance: %+v", report)
	}
}

type seatComputer struct {
	t                   *testing.T
	ctx                 context.Context
	client              *containerd.Client
	container           containerd.Container
	task                containerd.Task
	id, control, cgroup string
	logPath             string
	status              *os.File
	pid                 uint32
	tmpfsBytes          int64
	stopped             bool
	profileUnmodified   bool
}

func startSeatComputer(t *testing.T, ctx context.Context, client *containerd.Client, reference, root, service, kit string) *seatComputer {
	t.Helper()
	h := &seatComputer{t: t, ctx: ctx, client: client, id: "seat-" + filepath.Base(t.TempDir()), control: filepath.Join(root, "managed", "control")}
	logPath := filepath.Join(root, "computer.log")
	h.logPath = logPath
	// Registered before stop: LIFO cleanup stops and flushes the Computer first,
	// then retains logs before TempDir cleanup, including after a failed wait.
	t.Cleanup(func() {
		data, err := os.ReadFile(logPath)
		if os.IsNotExist(err) {
			return
		}
		seatMust(t, err)
		evidenceDir := filepath.Dir(requiredSeatEnv(t, "WEFTY_SEAT_EVIDENCE"))
		seatMust(t, os.WriteFile(filepath.Join(evidenceDir, runtime.GOARCH+"-"+h.id+"-computer.log"), data, 0600))
	})
	managed := filepath.Join(root, "managed")
	for _, dir := range []string{h.control, filepath.Join(service, "etc")} {
		seatMust(t, os.MkdirAll(dir, 0755))
	}
	// Storage can be supplied from the first attempt outside the new managed
	// root; use one stable managed root for all attempts.
	storageRoot := filepath.Dir(service)
	if storageRoot != managed {
		h.control = filepath.Join(storageRoot, "control-"+h.id)
		seatMust(t, os.MkdirAll(h.control, 0755))
		managed = storageRoot
	}
	seatMust(t, os.WriteFile(filepath.Join(service, "etc", "machine-id"), []byte("0123456789abcdef0123456789abcdef\n"), 0444))
	resolver, hosts := filepath.Join(managed, "resolv.conf"), filepath.Join(managed, "hosts")
	seatMust(t, os.WriteFile(resolver, []byte("nameserver 127.0.0.53\n"), 0444))
	seatMust(t, os.WriteFile(hosts, []byte("127.0.0.1 localhost\n"), 0444))
	image, err := client.GetImage(ctx, reference)
	seatMust(t, err)
	seatMust(t, image.Unpack(ctx, ocihelper.DefaultSnapshotter))
	diffs, err := image.RootFS(ctx)
	seatMust(t, err)
	snapshotter := client.SnapshotService(ocihelper.DefaultSnapshotter)
	mounts, err := snapshotter.Prepare(ctx, h.id, identity.ChainID(diffs).String(), snapshots.WithLabels(map[string]string{"containerd.io/gc.root": time.Now().UTC().Format(time.RFC3339)}))
	seatMust(t, err)
	t.Cleanup(func() {
		if h.container == nil {
			if err := snapshotter.Remove(namespaces.WithNamespace(context.Background(), ocihelper.ContainerdNamespace), h.id); err != nil {
				t.Log(err)
			}
		}
	})
	desc, err := image.Config(ctx)
	seatMust(t, err)
	blob, err := content.ReadBlob(ctx, client.ContentStore(), desc)
	seatMust(t, err)
	var config imagev1.Image
	seatMust(t, json.Unmarshal(blob, &config))
	var document *ocihelper.RuntimeSpecDocument
	seatMust(t, mount.WithTempMount(ctx, mounts, func(rootfs string) error {
		var err error
		document, err = ocihelper.BuildRuntimeSpec(ctx, seatProfileInput(h.id, reference, image.Target().Digest.String(), config.Config,
			seatProfilePaths{rootfs: rootfs, managed: managed, service: service, control: h.control,
				kit: kit, resolver: resolver, hosts: hosts}, seatKernelRelease()))
		return err
	}))
	defer document.Close()
	spec, err := document.ContainerdSpec()
	seatMust(t, err)
	var decoded specs.Spec
	seatMust(t, json.Unmarshal(spec.Value, &decoded))
	if !decoded.Root.Readonly || !decoded.Process.NoNewPrivileges || decoded.Process.User.UID != 0 {
		t.Fatal("Computer profile/root supervisor mismatch")
	}
	for _, m := range decoded.Mounts {
		if m.Type == "tmpfs" {
			for _, option := range m.Options {
				if strings.HasPrefix(option, "size=") {
					var kb int64
					if _, err := fmt.Sscanf(option, "size=%dk", &kb); err == nil {
						h.tmpfsBytes += kb * 1024
					}
				}
			}
		}
	}
	// Keep exact profile evidence separate from the retained Computer logs.
	if evidenceDir := os.Getenv("WEFTY_SEAT_PROFILE_RECEIPT"); evidenceDir != "" {
		profile, err := document.ProfileReceipt()
		seatMust(t, err)
		data, err := json.MarshalIndent(profile, "", "  ")
		seatMust(t, err)
		seatMust(t, os.WriteFile(evidenceDir, data, 0600))
	}
	h.container, err = client.NewContainer(ctx, h.id, containerd.WithImage(image), containerd.WithRuntime(ocihelper.DefaultRuntimeHandler, nil), containerd.WithSnapshotter(ocihelper.DefaultSnapshotter), containerd.WithSnapshot(h.id), func(_ context.Context, _ *containerd.Client, record *containers.Container) error {
		record.Spec = spec
		return nil
	})
	seatMust(t, err)
	t.Cleanup(h.stop)
	info, err := h.container.Info(ctx)
	seatMust(t, err)
	h.profileUnmodified = bytes.Equal(info.Spec.GetValue(), document.JSON())
	if !h.profileUnmodified {
		t.Fatal("containerd changed the generated Computer profile")
	}
	h.task, err = h.container.NewTask(ctx, cio.LogFile(logPath))
	seatMust(t, err)
	h.pid = h.task.Pid()
	mountinfo, err := os.ReadFile(fmt.Sprintf("/proc/%d/mountinfo", h.pid))
	seatMust(t, err)
	h.status, err = openValidatedSeatStatusDirectory(fmt.Sprintf("/proc/%d/root", h.pid), mountinfo)
	seatMust(t, err)
	t.Cleanup(func() { seatMust(t, h.status.Close()) })
	out, err := exec.CommandContext(ctx, "nsenter", "-t", fmt.Sprint(h.pid), "-n", "ip", "link", "set", "lo", "up").CombinedOutput()
	if err != nil {
		t.Fatalf("loopback: %v %s", err, out)
	}
	seatMust(t, document.RevalidateMounts())
	seatMust(t, h.task.Start(ctx))
	b, err := os.ReadFile(fmt.Sprintf("/proc/%d/cgroup", h.pid))
	seatMust(t, err)
	for _, line := range strings.Split(string(b), "\n") {
		if strings.HasPrefix(line, "0::") {
			h.cgroup = filepath.Join("/sys/fs/cgroup", strings.TrimPrefix(line, "0::"))
		}
	}
	if h.cgroup == "" {
		t.Fatal("cgroup v2 is required")
	}
	for file, want := range map[string]string{"memory.max": "1073741824", "memory.swap.max": "0", "memory.oom.group": "1"} {
		value, err := os.ReadFile(filepath.Join(h.cgroup, file))
		seatMust(t, err)
		if strings.TrimSpace(string(value)) != want {
			t.Fatalf("%s=%s want %s", file, value, want)
		}
	}
	h.wait("supervisor boot", func() bool { return h.tryExec("0:0", "test", "-f", "/run/wefty-seat-supervisor.json") == nil })
	return h
}

func (h *seatComputer) stop() {
	if h.stopped {
		return
	}
	h.stopped = true
	ctx, cancel := context.WithTimeout(namespaces.WithNamespace(context.Background(), ocihelper.ContainerdNamespace), 30*time.Second)
	defer cancel()
	if h.task != nil {
		wait, err := h.task.Wait(ctx)
		if err != nil {
			h.t.Error(err)
		}
		if err := h.task.Kill(ctx, syscall.SIGKILL, containerd.WithKillAll); err != nil {
			h.t.Error(err)
		}
		if wait != nil {
			select {
			case <-wait:
			case <-ctx.Done():
				h.t.Error(ctx.Err())
			}
		}
		if _, err := h.task.Delete(ctx); err != nil {
			h.t.Error(err)
		}
	}
	if h.container != nil {
		if err := h.container.Delete(ctx, containerd.WithSnapshotCleanup); err != nil {
			h.t.Error(err)
		}
	}
}
func (h *seatComputer) command(user string, args ...string) *exec.Cmd {
	argv := []string{"--address", requiredSeatEnv(h.t, "WEFTY_SEAT_CONTAINERD"), "--namespace", ocihelper.ContainerdNamespace, "tasks", "exec", "--exec-id", fmt.Sprintf("probe-%d", time.Now().UnixNano()), "--user", user, h.id}
	argv = append(argv, args...)
	return exec.CommandContext(h.ctx, "ctr", argv...)
}
func (h *seatComputer) exec(user string, args ...string) string {
	h.t.Helper()
	out, err := h.command(user, args...).CombinedOutput()
	if err != nil {
		h.t.Fatalf("Seat command %q: %v: %s", args, err, out)
	}
	return strings.TrimSpace(string(out))
}
func (h *seatComputer) tryExec(user string, args ...string) error {
	return h.command(user, args...).Run()
}
func (h *seatComputer) wait(description string, predicate func() bool, details ...func() string) {
	h.t.Helper()
	failure := func(reason string) {
		for _, detail := range details {
			reason += "; last probe: " + detail()
		}
		h.t.Fatal(reason)
	}
	deadline := time.NewTimer(65 * time.Second)
	defer deadline.Stop()
	for {
		if predicate() {
			return
		}
		select {
		case <-h.ctx.Done():
			failure(fmt.Sprintf("%s: %v", description, h.ctx.Err()))
		case <-deadline.C:
			failure("deadline: " + description)
		case <-time.After(100 * time.Millisecond):
		}
	}
}
func (h *seatComputer) directive(generation int, places ...int) {
	h.t.Helper()
	seats := []map[string]any{}
	for _, place := range places {
		seats = append(seats, map[string]any{"place": place, "user": fmt.Sprintf("seat%d", place), "view_port": 7100 + place*2, "control_port": 7101 + place*2, "view_socket": fmt.Sprintf("/wefty/service/seats/%d/run/view.ws", place), "control_socket": fmt.Sprintf("/wefty/service/seats/%d/run/control.ws", place)})
	}
	payload, err := json.Marshal(map[string]any{"version": 1, "generation": generation, "seats": seats})
	seatMust(h.t, err)
	seatAtomic(h.t, filepath.Join(h.control, "seats.json"), payload)
	h.t.Logf("Computer %s directive generation=%d wanted_places=%v", h.id, generation, places)
}
func (h *seatComputer) ready(place int) {
	h.t.Helper()
	lastStatus := "not yet read"
	diagnostics := func() string { return waitSeatReadinessDiagnostics(h.logPath, lastStatus, time.Now, time.Sleep) }
	h.wait(fmt.Sprintf("Computer %s Seat %d ready", h.id, place), func() bool {
		h.t.Helper()
		b, err := readSeatStatusAnswer(h.status, fmt.Sprintf("%d.json", place))
		if err != nil {
			lastStatus = fmt.Sprintf("read failed: %v", err)
			return false
		}
		lastStatus = string(b)
		var v struct{ State, Reason string }
		seatMust(h.t, json.Unmarshal(b, &v))
		if v.State == "failed" {
			h.t.Fatalf("Computer %s Seat %d failed: %s; %s", h.id, place, v.Reason, diagnostics())
		}
		return v.State == "ready"
	}, diagnostics)
}
func (h *seatComputer) driver(place int, human bool) {
	path := filepath.Join(h.control, "seats", fmt.Sprint(place), "driver.json")
	seatMust(h.t, os.MkdirAll(filepath.Dir(path), 0755))
	seatAtomic(h.t, path, []byte(fmt.Sprintf(`{"version":1,"human_driving":%t}`, human)))
}
func (h *seatComputer) probeCommand(mode string, place int, colour string) *exec.Cmd {
	command, err := seatScreenProbeCommand(h.ctx, h.pid, place, mode, colour)
	seatMust(h.t, err)
	return command
}
func (h *seatComputer) probe(mode string, place int, colour string) {
	h.t.Helper()
	out, err := h.probeCommand(mode, place, colour).CombinedOutput()
	h.t.Logf("Seat %d screen probe %s: %s", place, mode, out)
	if err != nil {
		h.t.Fatalf("screen probe %s: %v %s", mode, err, out)
	}
}

func (h *seatComputer) memory() int64 {
	b, err := os.ReadFile(filepath.Join(h.cgroup, "memory.current"))
	seatMust(h.t, err)
	v, err := strconv.ParseInt(strings.TrimSpace(string(b)), 10, 64)
	seatMust(h.t, err)
	return v
}
func seatEnv(place int) string {
	return fmt.Sprintf("export SEAT_ID=%d HOME=/wefty/service/seats/%d/home DISPLAY=:%d XAUTHORITY=/wefty/service/seats/%d/.Xauthority XDG_RUNTIME_DIR=/wefty/service/seats/%d/run TMPDIR=/wefty/service/seats/%d/tmp; ", place, place, place, place, place, place)
}
func seatAtomic(t *testing.T, path string, payload []byte) {
	t.Helper()
	seatMust(t, os.WriteFile(path+".new", payload, 0444))
	seatMust(t, os.Rename(path+".new", path))
}
func seatMust(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}
func requiredSeatEnv(t *testing.T, key string) string {
	t.Helper()
	value := os.Getenv(key)
	if value == "" {
		t.Fatalf("required Linux Seat acceptance input %s is missing", key)
	}
	return value
}
func seatKernelRelease() string {
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
