package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	containerd "github.com/containerd/containerd/v2/client"
	"github.com/containerd/containerd/v2/core/containers"
	"github.com/containerd/containerd/v2/core/content"
	"github.com/containerd/containerd/v2/core/images"
	imagearchive "github.com/containerd/containerd/v2/core/images/archive"
	"github.com/containerd/containerd/v2/core/leases"
	"github.com/containerd/containerd/v2/pkg/cio"
	"github.com/containerd/containerd/v2/pkg/namespaces"
	"github.com/containerd/containerd/v2/pkg/oci"
	"github.com/containerd/errdefs"
	"github.com/containerd/platforms"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	specs "github.com/opencontainers/runtime-spec/specs-go"
)

const (
	imageRef  = "docker.io/library/alpine:3.22"
	namespace = "wefty-proto"
)

type config struct {
	socket      string
	hostShared  string
	guestShared string
	out         string
	label       string
}

type verdict struct {
	Operation string
	Status    string
	Timing    time.Duration
	Notes     string
}

type report struct {
	Title string
	Rows  []verdict
}

type durableState struct {
	ContainerID string `json:"container_id"`
	SnapshotKey string `json:"snapshot_key"`
	Snapshotter string `json:"snapshotter"`
	GuestStdout string `json:"guest_stdout"`
	GuestStderr string `json:"guest_stderr"`
	HostStdout  string `json:"host_stdout"`
	HostStderr  string `json:"host_stderr"`
	StartedAt   string `json:"started_at"`
}

func main() {
	if len(os.Args) < 2 {
		fatalf("usage: %s run|client-restart-prepare|client-restart-resume|vm-restart-prepare|vm-restart-resume|first-container|rss-hold [flags]", os.Args[0])
	}
	command := os.Args[1]
	cfg := parseFlags(command, os.Args[2:])
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	defer cancel()

	var err error
	switch command {
	case "run":
		err = runSuite(ctx, cfg)
	case "client-restart-prepare", "vm-restart-prepare":
		err = prepareSurvivor(ctx, cfg, command)
	case "client-restart-resume", "vm-restart-resume":
		err = resumeSurvivor(ctx, cfg, command)
	case "first-container":
		err = firstContainer(ctx, cfg)
	case "rss-hold":
		err = rssHold(ctx, cfg)
	default:
		err = fmt.Errorf("unknown command %q", command)
	}
	if err != nil {
		fatalf("%s: %v", command, err)
	}
}

func parseFlags(command string, args []string) config {
	fs := flag.NewFlagSet(command, flag.ExitOnError)
	home, _ := os.UserHomeDir()
	cwd, _ := os.Getwd()
	defaultShared := filepath.Join(cwd, "shared")
	if filepath.Base(cwd) != "lima-containerd" {
		defaultShared = filepath.Join(cwd, "prototype", "lima-containerd", "shared")
	}
	cfg := config{}
	fs.StringVar(&cfg.socket, "socket", filepath.Join(home, ".lima", "wefty-proto", "sock", "containerd.sock"), "forwarded containerd Unix socket")
	fs.StringVar(&cfg.hostShared, "host-shared", defaultShared, "host path mounted into Lima")
	fs.StringVar(&cfg.guestShared, "guest-shared", "/mnt/wefty-proto", "corresponding guest mount path")
	fs.StringVar(&cfg.out, "out", filepath.Join(filepath.Dir(defaultShared), "out"), "host evidence directory")
	fs.StringVar(&cfg.label, "label", "baseline", "report/run label")
	_ = fs.Parse(args)
	for _, p := range []string{cfg.hostShared, cfg.out} {
		if err := os.MkdirAll(p, 0o755); err != nil {
			fatalf("create %s: %v", p, err)
		}
	}
	return cfg
}

func newClient(cfg config) (*containerd.Client, context.Context, error) {
	matcher := platforms.OnlyStrict(ocispec.Platform{OS: "linux", Architecture: "arm64"})
	c, err := containerd.New(cfg.socket, containerd.WithDefaultPlatform(matcher), containerd.WithTimeout(10*time.Second))
	if err != nil {
		return nil, nil, err
	}
	return c, namespaces.WithNamespace(context.Background(), namespace), nil
}

func runSuite(parent context.Context, cfg config) error {
	c, baseCtx, err := newClient(cfg)
	if err != nil {
		return err
	}
	defer c.Close()
	ctx, cancel := context.WithCancel(baseCtx)
	defer cancel()

	r := report{Title: "containerd over Lima forwarded socket — " + cfg.label}
	started := time.Now()
	version, err := c.Version(ctx)
	r.add("connect + version over forwarded socket", started, err == nil, fmt.Sprintf("socket=%s; version=%s; revision=%s; error=%v", cfg.socket, version.Version, version.Revision, err))
	if err != nil {
		return r.write(cfg, "run")
	}

	leaseID := "wefty-proto-" + safeID(cfg.label) + "-" + fmt.Sprint(time.Now().UnixNano())
	leaseCtx, release, err := c.WithLease(ctx, leases.WithID(leaseID), leases.WithExpiration(2*time.Hour))
	if err != nil {
		return err
	}
	ctx = leaseCtx
	defer func() { _ = release(namespaces.WithNamespace(context.Background(), namespace)) }()

	started = time.Now()
	img, err := c.Pull(ctx, imageRef, containerd.WithPlatform("linux/arm64"))
	resolvedPlatform, platformErr := imagePlatform(ctx, img)
	r.add("pull multi-arch image (linux/arm64 selection)", started, err == nil && platformErr == nil && resolvedPlatform == "linux/arm64", fmt.Sprintf("ref=%s; target=%s; selected config platform=%s; lease=%s; errors pull=%v platform=%v", imageRef, targetDigest(img), resolvedPlatform, leaseID, err, platformErr))
	if err != nil {
		return r.write(cfg, "run")
	}

	started = time.Now()
	importedNames, importErr := exportImport(ctx, c, img, cfg)
	r.add("tar export to host + import under translated refs", started, importErr == nil, fmt.Sprintf("tar=%s; imported=%s; error=%v", filepath.Join(cfg.out, cfg.label+"-alpine.tar"), strings.Join(importedNames, ","), importErr))

	started = time.Now()
	err = img.Unpack(ctx, "overlayfs")
	r.add("unpack image", started, err == nil, fmt.Sprintf("snapshotter=overlayfs (explicit because macOS client-side default resolution selected unavailable erofs); error=%v", err))
	if err != nil {
		return r.write(cfg, "run")
	}

	r.Rows = append(r.Rows, lifecycleCapture(ctx, c, img, cfg))
	r.Rows = append(r.Rows, standardImageConfigFailure(ctx, c, img, cfg))
	r.Rows = append(r.Rows, defaultFIFOFailure(ctx, c, img, cfg))
	r.Rows = append(r.Rows, signalPropagation(ctx, c, img, cfg))
	r.Rows = append(r.Rows, bindAndHeavyWrite(ctx, c, img, cfg))
	r.Rows = append(r.Rows, tcpEndpoint(ctx, c, img, cfg))
	r.Rows = append(r.Rows, cleanupVerification(ctx, c, img, cfg))

	return r.write(cfg, "run")
}

func standardImageConfigFailure(ctx context.Context, c *containerd.Client, img containerd.Image, cfg config) verdict {
	started := time.Now()
	id := uniqueID(cfg.label, "standard-image-config")
	snapshot := id + "-snap"
	_, err := c.NewContainer(ctx, id,
		containerd.WithImage(img),
		containerd.WithRuntime("io.containerd.runc.v2", nil),
		containerd.WithSnapshotter("overlayfs"),
		containerd.WithNewSnapshot(snapshot, img),
		containerd.WithNewSpec(
			oci.WithDefaultSpecForPlatform("linux/arm64"),
			oci.WithImageConfig(img),
			oci.WithProcessArgs("/bin/true"),
		),
	)
	expected := err != nil && strings.Contains(err.Error(), "/var/lib/containerd/") && strings.Contains(err.Error(), "no such file or directory")
	status := "FAIL"
	if err == nil {
		status = "PASS"
	}
	notes := fmt.Sprintf("standard oci.WithImageConfig result=%v; expected cross-boundary failure observed=%t; cause: WithImageConfig calls WithAdditionalGIDs, whose Darwin path tries to open daemon snapshot paths locally; harness continues with explicit remote-safe root-user spec", err, expected)
	return verdict{Operation: "standard oci.WithImageConfig spec generation across boundary", Status: status, Timing: time.Since(started), Notes: notes}
}

func exportImport(ctx context.Context, c *containerd.Client, img containerd.Image, cfg config) ([]string, error) {
	tarPath := filepath.Join(cfg.out, cfg.label+"-alpine.tar")
	f, err := os.Create(tarPath)
	if err != nil {
		return nil, err
	}
	err = c.Export(ctx, f, imagearchive.WithImage(c.ImageService(), img.Name()), imagearchive.WithPlatform(platforms.OnlyStrict(ocispec.Platform{OS: "linux", Architecture: "arm64"})))
	closeErr := f.Close()
	if err != nil {
		return nil, err
	}
	if closeErr != nil {
		return nil, closeErr
	}
	f, err = os.Open(tarPath)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	imported, err := c.Import(ctx, f, containerd.WithImageRefTranslator(imagearchive.AddRefPrefix("wefty-proto-import/"+safeID(cfg.label))))
	if err != nil {
		return nil, err
	}
	names := make([]string, 0, len(imported))
	for _, rec := range imported {
		newRef := "wefty-proto.local/imported:" + safeID(cfg.label)
		created, createErr := c.ImageService().Create(ctx, images.Image{Name: newRef, Target: rec.Target})
		if createErr != nil && !errdefs.IsAlreadyExists(createErr) {
			return names, createErr
		}
		if createErr == nil {
			rec = created
		}
		names = append(names, newRef)
		if _, err := c.ImageService().Get(ctx, newRef); err != nil {
			return names, err
		}
		break
	}
	return names, nil
}

func lifecycleCapture(ctx context.Context, c *containerd.Client, img containerd.Image, cfg config) verdict {
	started := time.Now()
	id := uniqueID(cfg.label, "capture")
	stdoutGuest, stdoutHost := logPaths(cfg, id, "stdout")
	stderrGuest, stderrHost := logPaths(cfg, id, "stderr")
	creator, err := splitLogCreator(stdoutGuest, stderrGuest)
	if err != nil {
		return failVerdict("create/start/wait + independent shim-side stdout/stderr + non-zero exit", started, err)
	}
	ctr, task, info, err := createTask(ctx, c, img, id, []string{"/bin/sh", "-c", "printf 'stdout-marker\\n'; printf 'stderr-marker\\n' >&2; exit 37"}, creator)
	if err != nil {
		return failVerdict("create/start/wait + independent shim-side stdout/stderr + non-zero exit", started, err)
	}
	waitC, err := task.Wait(ctx) // Deliberately before Start.
	if err == nil {
		err = task.Start(ctx)
	}
	var code uint32
	if err == nil {
		status := <-waitC
		code, _, err = status.Result()
	}
	stdout, stdoutErr := readEventually(stdoutHost)
	stderr, stderrErr := readEventually(stderrHost)
	cleanupErr := cleanupTaskContainer(ctx, task, ctr)
	ok := err == nil && code == 37 && stdout == "stdout-marker\n" && stderr == "stderr-marker\n" && stdoutErr == nil && stderrErr == nil && cleanupErr == nil
	notes := fmt.Sprintf("Wait called before Start; exit=%d; stdout=%q; stderr=%q; stdoutErr=%v; stderrErr=%v; cleanup=%v; snapshot=%s/%s", code, stdout, stderr, stdoutErr, stderrErr, cleanupErr, info.Snapshotter, info.SnapshotKey)
	return verdictFor("create/start/wait + independent shim-side stdout/stderr + non-zero exit", started, ok, notes)
}

func defaultFIFOFailure(ctx context.Context, c *containerd.Client, img containerd.Image, cfg config) verdict {
	started := time.Now()
	id := uniqueID(cfg.label, "fifo")
	fifoRoot := filepath.Join(cfg.out, "host-only-fifos")
	_ = os.MkdirAll(fifoRoot, 0o755)
	var stdout, stderr bytes.Buffer
	creator := cio.NewCreator(cio.WithStreams(nil, &stdout, &stderr), cio.WithFIFODir(fifoRoot))
	shortCtx, cancel := context.WithTimeout(ctx, 8*time.Second)
	defer cancel()
	ctr, task, _, err := createTask(shortCtx, c, img, id, []string{"/bin/echo", "fifo-should-not-work"}, creator)
	mode := ""
	if err != nil {
		mode = err.Error()
	} else {
		waitC, waitErr := task.Wait(shortCtx)
		startErr := waitErr
		if startErr == nil {
			startErr = task.Start(shortCtx)
		}
		if startErr == nil {
			select {
			case st := <-waitC:
				mode = fmt.Sprintf("unexpected success exit=%d stdout=%q stderr=%q", st.ExitCode(), stdout.String(), stderr.String())
			case <-shortCtx.Done():
				mode = "hung until timeout: " + shortCtx.Err().Error()
			}
		} else {
			mode = startErr.Error()
		}
	}
	cleanupCtx, cleanupCancel := context.WithTimeout(namespaces.WithNamespace(context.Background(), namespace), 15*time.Second)
	defer cleanupCancel()
	if task != nil {
		_, _ = task.Delete(cleanupCtx, containerd.WithProcessKill)
	}
	if ctr != nil {
		_ = ctr.Delete(cleanupCtx, containerd.WithSnapshotCleanup)
	}
	expectedFailure := err != nil || strings.Contains(mode, "timeout") || strings.Contains(mode, "deadline") || strings.Contains(mode, "no such file")
	status := "FAIL"
	if !expectedFailure {
		status = "PASS"
	}
	return verdict{Operation: "default cio.NewCreator FIFO stdio across VM boundary (expected failure)", Status: status, Timing: time.Since(started), Notes: "host-only FIFO root=" + fifoRoot + "; observed=" + mode}
}

func signalPropagation(ctx context.Context, c *containerd.Client, img containerd.Image, cfg config) verdict {
	started := time.Now()
	id := uniqueID(cfg.label, "signals")
	stdoutGuest, _ := logPaths(cfg, id, "stdout")
	stderrGuest, stderrHost := logPaths(cfg, id, "stderr")
	creator, err := splitLogCreator(stdoutGuest, stderrGuest)
	if err != nil {
		return failVerdict("SIGTERM then SIGKILL propagation", started, err)
	}
	ctr, task, _, err := createTask(ctx, c, img, id, []string{"/bin/sh", "-c", "trap 'echo saw-sigterm >&2' TERM; while :; do :; done"}, creator)
	if err != nil {
		return failVerdict("SIGTERM then SIGKILL propagation", started, err)
	}
	waitC, err := task.Wait(ctx)
	if err == nil {
		err = task.Start(ctx)
	}
	if err == nil {
		time.Sleep(300 * time.Millisecond)
		err = task.Kill(ctx, syscall.SIGTERM)
	}
	time.Sleep(300 * time.Millisecond)
	statusAfterTerm, statusErr := task.Status(ctx)
	if err == nil {
		err = task.Kill(ctx, syscall.SIGKILL)
	}
	var code uint32
	if err == nil {
		status := <-waitC
		code, _, err = status.Result()
	}
	stderr, stderrErr := readEventually(stderrHost)
	cleanupErr := cleanupTaskContainer(ctx, task, ctr)
	ok := err == nil && statusErr == nil && statusAfterTerm.Status == containerd.Running && code == 137 && strings.Contains(stderr, "saw-sigterm") && stderrErr == nil && cleanupErr == nil
	return verdictFor("SIGTERM then SIGKILL propagation", started, ok, fmt.Sprintf("after SIGTERM=%s; marker=%q; final exit=%d (128+SIGKILL); errors status=%v run=%v log=%v cleanup=%v", statusAfterTerm.Status, stderr, code, statusErr, err, stderrErr, cleanupErr))
}

func bindAndHeavyWrite(ctx context.Context, c *containerd.Client, img containerd.Image, cfg config) verdict {
	started := time.Now()
	id := uniqueID(cfg.label, "bind")
	hostDir := filepath.Join(cfg.hostShared, "bind-"+safeID(cfg.label))
	guestDir := filepath.Join(cfg.guestShared, "bind-"+safeID(cfg.label))
	_ = os.RemoveAll(hostDir)
	if err := os.MkdirAll(hostDir, 0o755); err != nil {
		return failVerdict("writable bind mount + 2000 files + git 500-file commit", started, err)
	}
	stdoutGuest, _ := logPaths(cfg, id, "stdout")
	stderrGuest, stderrHost := logPaths(cfg, id, "stderr")
	creator, _ := splitLogCreator(stdoutGuest, stderrGuest)
	command := `set -eu
echo container-wrote-this > /host/from-container.txt
mkdir -p /host/small /host/repo
i=1; while [ "$i" -le 2000 ]; do printf x > "/host/small/f-$i"; i=$((i+1)); done
apk add --no-cache git >/dev/null
cd /host/repo
git init -q
git config user.email prototype@wefty.invalid
git config user.name wefty-prototype
i=1; while [ "$i" -le 500 ]; do printf 'file %s\n' "$i" > "f-$i"; i=$((i+1)); done
git add .
git commit -qm initial
printf 'heavy-write-ok\n'`
	mount := specs.Mount{Destination: "/host", Type: "bind", Source: guestDir, Options: []string{"rbind", "rw"}}
	guestDNS := specs.Mount{Destination: "/etc/resolv.conf", Type: "bind", Source: "/etc/resolv.conf", Options: []string{"rbind", "ro"}}
	ctr, task, _, err := createTask(ctx, c, img, id, []string{"/bin/sh", "-c", command}, creator, oci.WithMounts([]specs.Mount{mount, guestDNS}), oci.WithHostNamespace(specs.NetworkNamespace))
	if err != nil {
		return failVerdict("writable bind mount + 2000 files + git 500-file commit", started, err)
	}
	waitC, err := task.Wait(ctx)
	if err == nil {
		err = task.Start(ctx)
	}
	var code uint32
	if err == nil {
		status := <-waitC
		code, _, err = status.Result()
	}
	marker, markerErr := os.ReadFile(filepath.Join(hostDir, "from-container.txt"))
	files, countErr := filepath.Glob(filepath.Join(hostDir, "small", "f-*"))
	_, commitErr := os.Stat(filepath.Join(hostDir, "repo", ".git", "HEAD"))
	stderr, _ := readEventually(stderrHost)
	cleanupErr := cleanupTaskContainer(ctx, task, ctr)
	permissionFault := strings.Contains(strings.ToLower(stderr), "operation not permitted") || strings.Contains(strings.ToLower(stderr), "input/output error")
	ok := err == nil && code == 0 && string(marker) == "container-wrote-this\n" && markerErr == nil && countErr == nil && len(files) == 2000 && commitErr == nil && !permissionFault && cleanupErr == nil
	notes := fmt.Sprintf("exit=%d; host marker=%q; small files=%d; git HEAD exists=%t; EPERM/EIO=%t; stderr=%q; errors run=%v marker=%v glob=%v git=%v cleanup=%v", code, string(marker), len(files), commitErr == nil, permissionFault, truncate(stderr, 500), err, markerErr, countErr, commitErr, cleanupErr)
	return verdictFor("writable bind mount + 2000 files + git 500-file commit", started, ok, notes)
}

func tcpEndpoint(ctx context.Context, c *containerd.Client, img containerd.Image, cfg config) verdict {
	started := time.Now()
	id := uniqueID(cfg.label, "tcp")
	port := 18080
	if cfg.label != "baseline" {
		port = 18081 + int(time.Now().UnixNano()%1000)
	}
	stdoutGuest, _ := logPaths(cfg, id, "stdout")
	stderrGuest, _ := logPaths(cfg, id, "stderr")
	creator, _ := splitLogCreator(stdoutGuest, stderrGuest)
	cmd := fmt.Sprintf(`while :; do printf 'HTTP/1.1 200 OK\r\nContent-Length: 13\r\nConnection: close\r\n\r\nwefty-tcp-ok\n' | nc -l -p %d; done`, port)
	ctr, task, _, err := createTask(ctx, c, img, id, []string{"/bin/sh", "-c", cmd}, creator, oci.WithHostNamespace(specs.NetworkNamespace))
	if err != nil {
		return failVerdict("container TCP endpoint reached from macOS host", started, err)
	}
	waitC, err := task.Wait(ctx)
	if err == nil {
		err = task.Start(ctx)
	}
	url := fmt.Sprintf("http://127.0.0.1:%d/", port)
	body, reachErr := httpEventually(ctx, url, 20*time.Second)
	if err == nil {
		err = task.Kill(ctx, syscall.SIGKILL)
	}
	var code uint32
	if err == nil {
		status := <-waitC
		code, _, err = status.Result()
	}
	cleanupErr := cleanupTaskContainer(ctx, task, ctr)
	ok := reachErr == nil && strings.TrimSpace(body) == "wefty-tcp-ok" && err == nil && code == 137 && cleanupErr == nil
	return verdictFor("container TCP endpoint reached from macOS host", started, ok, fmt.Sprintf("path=Lima dynamic guest-loopback to macOS 127.0.0.1:%d; response=%q; exit after kill=%d; errors reach=%v task=%v cleanup=%v", port, body, code, reachErr, err, cleanupErr))
}

func cleanupVerification(ctx context.Context, c *containerd.Client, img containerd.Image, cfg config) verdict {
	started := time.Now()
	id := uniqueID(cfg.label, "cleanup")
	stdoutGuest, _ := logPaths(cfg, id, "stdout")
	stderrGuest, _ := logPaths(cfg, id, "stderr")
	creator, _ := splitLogCreator(stdoutGuest, stderrGuest)
	ctr, task, info, err := createTask(ctx, c, img, id, []string{"/bin/sh", "-c", "while :; do sleep 1; done"}, creator)
	if err != nil {
		return failVerdict("kill/delete and verify task/container/snapshot absence", started, err)
	}
	waitC, err := task.Wait(ctx)
	if err == nil {
		err = task.Start(ctx)
	}
	if err == nil {
		err = task.Kill(ctx, syscall.SIGKILL)
	}
	if err == nil {
		<-waitC
	}
	if err == nil {
		_, err = task.Delete(ctx)
	}
	taskGone := false
	if err == nil {
		_, taskErr := ctr.Task(ctx, nil)
		taskGone = errdefs.IsNotFound(taskErr)
	}
	if err == nil {
		err = ctr.Delete(ctx, containerd.WithSnapshotCleanup)
	}
	_, containerErr := c.LoadContainer(ctx, id)
	containerGone := errdefs.IsNotFound(containerErr)
	_, snapshotErr := c.SnapshotService(info.Snapshotter).Stat(ctx, info.SnapshotKey)
	snapshotGone := errdefs.IsNotFound(snapshotErr)
	ok := err == nil && taskGone && containerGone && snapshotGone
	return verdictFor("kill/delete and verify task/container/snapshot absence", started, ok, fmt.Sprintf("taskGone=%t containerGone=%t snapshotGone=%t snapshot=%s/%s lease is caller-owned and released at suite end; errors operation=%v container=%v snapshot=%v", taskGone, containerGone, snapshotGone, info.Snapshotter, info.SnapshotKey, err, containerErr, snapshotErr))
}

func prepareSurvivor(ctx context.Context, cfg config, command string) error {
	c, baseCtx, err := newClient(cfg)
	if err != nil {
		return err
	}
	defer c.Close()
	ctx, cancel := context.WithTimeout(baseCtx, 5*time.Minute)
	defer cancel()
	img, err := c.Pull(ctx, imageRef, containerd.WithPlatform("linux/arm64"))
	if err != nil {
		return err
	}
	if err := img.Unpack(ctx, "overlayfs"); err != nil {
		return err
	}
	kind := strings.TrimSuffix(command, "-prepare")
	id := uniqueID(cfg.label, kind)
	stdoutGuest, stdoutHost := logPaths(cfg, id, "stdout")
	stderrGuest, stderrHost := logPaths(cfg, id, "stderr")
	creator, err := splitLogCreator(stdoutGuest, stderrGuest)
	if err != nil {
		return err
	}
	ctr, task, info, err := createTask(ctx, c, img, id, []string{"/bin/sh", "-c", "trap 'echo survivor-term >&2; exit 143' TERM; echo survivor-started; while :; do sleep 1; done"}, creator)
	if err != nil {
		return err
	}
	if _, err := task.Wait(ctx); err != nil { // Establish before Start in this process.
		return err
	}
	if err := task.Start(ctx); err != nil {
		return err
	}
	time.Sleep(500 * time.Millisecond)
	state := durableState{ContainerID: id, SnapshotKey: info.SnapshotKey, Snapshotter: info.Snapshotter, GuestStdout: stdoutGuest, GuestStderr: stderrGuest, HostStdout: stdoutHost, HostStderr: stderrHost, StartedAt: time.Now().UTC().Format(time.RFC3339Nano)}
	statePath := statePath(cfg, kind)
	data, _ := json.MarshalIndent(state, "", "  ")
	if err := os.WriteFile(statePath, append(data, '\n'), 0o644); err != nil {
		return err
	}
	fmt.Printf("started %s pid=%d; state=%s; exiting without task cleanup\n", id, task.Pid(), statePath)
	_ = ctr
	return nil
}

func resumeSurvivor(ctx context.Context, cfg config, command string) error {
	kind := strings.TrimSuffix(command, "-resume")
	state, err := readState(statePath(cfg, kind))
	if err != nil {
		return err
	}
	c, baseCtx, err := newClient(cfg)
	if err != nil {
		return err
	}
	defer c.Close()
	ctx, cancel := context.WithTimeout(baseCtx, 3*time.Minute)
	defer cancel()
	r := report{Title: kind + " survival and recovery"}
	started := time.Now()
	ctr, loadErr := c.LoadContainer(ctx, state.ContainerID)
	if loadErr != nil {
		r.add(kind+": reload container by ID", started, false, loadErr.Error())
		return r.write(cfg, kind)
	}
	task, taskErr := ctr.Task(ctx, nil)
	if kind == "client-restart" {
		if taskErr != nil {
			r.add("client restart: task survives client process death and reattaches", started, false, taskErr.Error())
			return r.write(cfg, kind)
		}
		status, statusErr := task.Status(ctx)
		waitC, waitErr := task.Wait(ctx)
		killErr := waitErr
		if killErr == nil {
			killErr = task.Kill(ctx, syscall.SIGTERM)
		}
		var code uint32
		if killErr == nil {
			exit := <-waitC
			code, _, killErr = exit.Result()
		}
		stderr, stderrErr := readEventually(state.HostStderr)
		cleanupErr := cleanupTaskContainer(ctx, task, ctr)
		ok := statusErr == nil && status.Status == containerd.Running && killErr == nil && code == 143 && strings.Contains(stderr, "survivor-term") && stderrErr == nil && cleanupErr == nil
		r.add("client restart: task survives client process death and reattaches", started, ok, fmt.Sprintf("pre-kill status=%s; Wait called in new process before Kill; exit=%d; stderr=%q; errors status=%v kill=%v log=%v cleanup=%v", status.Status, code, stderr, statusErr, killErr, stderrErr, cleanupErr))
	} else {
		staleTask := taskErr == nil
		statusText := "task absent"
		if taskErr == nil {
			status, statusErr := task.Status(ctx)
			statusText = fmt.Sprintf("task status=%s error=%v", status.Status, statusErr)
			_, _ = task.Delete(ctx, containerd.WithProcessKill)
		}
		cleanupErr := ctr.Delete(ctx, containerd.WithSnapshotCleanup)
		_, containerErr := c.LoadContainer(ctx, state.ContainerID)
		_, snapshotErr := c.SnapshotService(state.Snapshotter).Stat(ctx, state.SnapshotKey)
		containerGone := errdefs.IsNotFound(containerErr)
		snapshotGone := errdefs.IsNotFound(snapshotErr)
		ok := !staleTask && cleanupErr == nil && containerGone && snapshotGone
		r.add("VM restart: task death, stale metadata, and cleanup", started, ok, fmt.Sprintf("container metadata survived=true; %s; task lookup error=%v; cleanup=%v; containerGone=%t; snapshotGone=%t", statusText, taskErr, cleanupErr, containerGone, snapshotGone))
	}
	return r.write(cfg, kind)
}

func firstContainer(ctx context.Context, cfg config) error {
	started := time.Now()
	c, baseCtx, err := newClient(cfg)
	if err != nil {
		return err
	}
	defer c.Close()
	ctx, cancel := context.WithTimeout(baseCtx, 5*time.Minute)
	defer cancel()
	img, err := c.Pull(ctx, imageRef, containerd.WithPlatform("linux/arm64"))
	if err != nil {
		return err
	}
	if err := img.Unpack(ctx, "overlayfs"); err != nil {
		return err
	}
	id := uniqueID(cfg.label, "first")
	stdoutGuest, _ := logPaths(cfg, id, "stdout")
	stderrGuest, _ := logPaths(cfg, id, "stderr")
	creator, _ := splitLogCreator(stdoutGuest, stderrGuest)
	ctr, task, _, err := createTask(ctx, c, img, id, []string{"/bin/true"}, creator)
	if err != nil {
		return err
	}
	waitC, err := task.Wait(ctx)
	if err == nil {
		err = task.Start(ctx)
	}
	if err == nil {
		<-waitC
	}
	cleanupErr := cleanupTaskContainer(ctx, task, ctr)
	if err != nil {
		return err
	}
	if cleanupErr != nil {
		return cleanupErr
	}
	fmt.Printf("first-container-ready=%s\n", time.Since(started).Round(time.Millisecond))
	return nil
}

func rssHold(ctx context.Context, cfg config) error {
	c, baseCtx, err := newClient(cfg)
	if err != nil {
		return err
	}
	defer c.Close()
	ctx, cancel := context.WithTimeout(baseCtx, 2*time.Minute)
	defer cancel()
	img, err := c.Pull(ctx, imageRef, containerd.WithPlatform("linux/arm64"))
	if err != nil {
		return err
	}
	if err := img.Unpack(ctx, "overlayfs"); err != nil {
		return err
	}
	type live struct {
		ctr  containerd.Container
		task containerd.Task
		wait <-chan containerd.ExitStatus
	}
	liveTasks := make([]live, 0, 5)
	for i := 0; i < 5; i++ {
		id := uniqueID(cfg.label, fmt.Sprintf("rss-%d", i))
		stdoutGuest, _ := logPaths(cfg, id, "stdout")
		stderrGuest, _ := logPaths(cfg, id, "stderr")
		creator, _ := splitLogCreator(stdoutGuest, stderrGuest)
		ctr, task, _, err := createTask(ctx, c, img, id, []string{"/bin/sh", "-c", "while :; do sleep 1; done"}, creator)
		if err != nil {
			return err
		}
		waitC, err := task.Wait(ctx)
		if err != nil {
			return err
		}
		if err := task.Start(ctx); err != nil {
			return err
		}
		liveTasks = append(liveTasks, live{ctr, task, waitC})
	}
	fmt.Println("five-tasks-running; holding for 30s for host RSS measurement")
	time.Sleep(30 * time.Second)
	for _, item := range liveTasks {
		_ = item.task.Kill(ctx, syscall.SIGKILL)
		<-item.wait
		_ = cleanupTaskContainer(ctx, item.task, item.ctr)
	}
	return nil
}

func createTask(ctx context.Context, c *containerd.Client, img containerd.Image, id string, args []string, ioCreator cio.Creator, extra ...oci.SpecOpts) (containerd.Container, containerd.Task, containers.Container, error) {
	snapshot := id + "-snap"
	// Do not use oci.WithImageConfig here. On Darwin it calls
	// WithAdditionalGIDs, which opens daemon-local snapshot paths in the client.
	// Every prototype command supplies explicit args and uses the default root
	// user/environment, so this is a deliberately narrow remote-safe spec.
	specOpts := []oci.SpecOpts{oci.WithDefaultSpecForPlatform("linux/arm64"), oci.WithProcessArgs(args...)}
	specOpts = append(specOpts, extra...)
	ctr, err := c.NewContainer(ctx, id, containerd.WithImage(img), containerd.WithRuntime("io.containerd.runc.v2", nil), containerd.WithSnapshotter("overlayfs"), containerd.WithNewSnapshot(snapshot, img), containerd.WithNewSpec(specOpts...))
	if err != nil {
		return nil, nil, containers.Container{}, err
	}
	info, infoErr := ctr.Info(ctx)
	if infoErr != nil {
		_ = ctr.Delete(ctx, containerd.WithSnapshotCleanup)
		return nil, nil, containers.Container{}, infoErr
	}
	task, err := ctr.NewTask(ctx, ioCreator)
	if err != nil {
		return ctr, nil, info, err
	}
	return ctr, task, info, nil
}

func cleanupTaskContainer(ctx context.Context, task containerd.Task, ctr containerd.Container) error {
	var errs []error
	if task != nil {
		if _, err := task.Delete(ctx); err != nil && !errdefs.IsNotFound(err) {
			errs = append(errs, err)
		}
	}
	if ctr != nil {
		if err := ctr.Delete(ctx, containerd.WithSnapshotCleanup); err != nil && !errdefs.IsNotFound(err) {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

type staticIO struct{ config cio.Config }

func (s *staticIO) Config() cio.Config { return s.config }
func (s *staticIO) Cancel()            {}
func (s *staticIO) Wait()              {}
func (s *staticIO) Close() error       { return nil }

func splitLogCreator(stdout, stderr string) (cio.Creator, error) {
	if !filepath.IsAbs(stdout) || !filepath.IsAbs(stderr) {
		return nil, fmt.Errorf("log paths must be absolute: stdout=%s stderr=%s", stdout, stderr)
	}
	u := &url.URL{Scheme: "binary-v2", Path: "/usr/local/bin/wefty-log-split"}
	query := u.Query()
	query.Set("stdout", stdout)
	query.Set("stderr", stderr)
	u.RawQuery = query.Encode()
	uri := u.String()
	return func(string) (cio.IO, error) {
		return &staticIO{config: cio.Config{Stdout: uri, Stderr: uri}}, nil
	}, nil
}

func logPaths(cfg config, id, stream string) (guest, host string) {
	name := id + "-" + stream + ".log"
	return filepath.Join(cfg.guestShared, "logs", name), filepath.Join(cfg.hostShared, "logs", name)
}

func readEventually(path string) (string, error) {
	var data []byte
	var err error
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		data, err = os.ReadFile(path)
		if err == nil {
			return string(data), nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	return string(data), err
}

func httpEventually(ctx context.Context, endpoint string, timeout time.Duration) (string, error) {
	deadline := time.Now().Add(timeout)
	var last error
	for time.Now().Before(deadline) {
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
		resp, err := http.DefaultClient.Do(req)
		if err == nil {
			body, readErr := io.ReadAll(resp.Body)
			_ = resp.Body.Close()
			if readErr == nil && resp.StatusCode == http.StatusOK {
				return string(body), nil
			}
			last = readErr
		} else {
			last = err
		}
		time.Sleep(200 * time.Millisecond)
	}
	return "", last
}

func statePath(cfg config, kind string) string { return filepath.Join(cfg.out, kind+"-state.json") }

func readState(path string) (durableState, error) {
	var state durableState
	data, err := os.ReadFile(path)
	if err == nil {
		err = json.Unmarshal(data, &state)
	}
	return state, err
}

func (r *report) add(operation string, started time.Time, ok bool, notes string) {
	r.Rows = append(r.Rows, verdictFor(operation, started, ok, notes))
}

func verdictFor(operation string, started time.Time, ok bool, notes string) verdict {
	status := "FAIL"
	if ok {
		status = "PASS"
	}
	return verdict{Operation: operation, Status: status, Timing: time.Since(started), Notes: notes}
}

func failVerdict(operation string, started time.Time, err error) verdict {
	return verdictFor(operation, started, false, "error="+err.Error())
}

func (r report) write(cfg config, kind string) error {
	path := filepath.Join(cfg.out, safeID(cfg.label)+"-"+kind+"-verdict.md")
	var b strings.Builder
	fmt.Fprintf(&b, "# %s\n\n", r.Title)
	b.WriteString("| Operation | Verdict | Timing | Notes |\n|---|---|---:|---|\n")
	for _, row := range r.Rows {
		fmt.Fprintf(&b, "| %s | %s | %s | %s |\n", md(row.Operation), row.Status, row.Timing.Round(time.Millisecond), md(row.Notes))
	}
	if err := os.WriteFile(path, []byte(b.String()), 0o644); err != nil {
		return err
	}
	fmt.Print(b.String())
	fmt.Printf("report=%s\n", path)
	return nil
}

func targetDigest(img containerd.Image) string {
	if img == nil {
		return ""
	}
	return img.Target().Digest.String()
}

func imagePlatform(ctx context.Context, img containerd.Image) (string, error) {
	if img == nil {
		return "", errors.New("nil image")
	}
	desc, err := img.Config(ctx)
	if err != nil {
		return "", err
	}
	data, err := content.ReadBlob(ctx, img.ContentStore(), desc)
	if err != nil {
		return "", err
	}
	var cfg ocispec.Image
	if err := json.Unmarshal(data, &cfg); err != nil {
		return "", err
	}
	return cfg.OS + "/" + cfg.Architecture, nil
}

func uniqueID(label, suffix string) string {
	return "wefty-" + safeID(label) + "-" + suffix + "-" + fmt.Sprint(time.Now().UnixNano())
}

func safeID(s string) string {
	s = strings.ToLower(s)
	var b strings.Builder
	for _, r := range s {
		if r >= 'a' && r <= 'z' || r >= '0' && r <= '9' || r == '-' {
			b.WriteRune(r)
		} else {
			b.WriteByte('-')
		}
	}
	return strings.Trim(b.String(), "-")
}

func md(s string) string {
	s = strings.ReplaceAll(s, "|", "\\|")
	s = strings.ReplaceAll(s, "\n", "<br>")
	return s
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}

func fatalf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}
