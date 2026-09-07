//go:build service_acceptance_realtiming && (darwin || linux)

package serviceacceptance

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/l1"
	"github.com/Derek-X-Wang/wefty/runner/ocihelper"
	"github.com/coder/websocket"
	containerd "github.com/containerd/containerd/v2/client"
	"github.com/containerd/containerd/v2/pkg/cio"
	"github.com/containerd/containerd/v2/pkg/namespaces"
)

// The root-only containerd socket is never exposed to the test runner. This
// narrow child owns the SDK Process and exits before the normal test TestMain.
func init() {
	if os.Getenv("WEFTY_COMPUTER_RELAY_SUPERVISOR") == "1" {
		if err := runComputerRelaySupervisor(os.Stdin, os.Stdout); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		os.Exit(0)
	}
}

const computerVethRelayPython = `
import ipaddress, json, select, socket, sys, threading
address, view, control = sys.argv[1], int(sys.argv[2]), int(sys.argv[3])
if ipaddress.ip_address(address).is_unspecified or view == control:
    raise ValueError("exact address and distinct ports required")
listeners = []
limit = threading.BoundedSemaphore(4)
def forward(front, port):
    back = None
    try:
        front.settimeout(5)
        back = socket.create_connection(("127.0.0.1", port), timeout=5)
        active = [front, back]
        while active:
            readable, _, _ = select.select(active, [], [], 5)
            if not readable:
                break
            for source in readable:
                destination = back if source is front else front
                data = source.recv(16384)
                if data:
                    destination.sendall(data)
                else:
                    active.remove(source)
                    destination.shutdown(socket.SHUT_WR)
    except OSError:
        pass
    finally:
        front.close()
        if back is not None:
            back.close()
        limit.release()
for port in (view, control):
    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.bind((address, port))
    listener.listen(4)
    listeners.append(listener)
print("relay-ready", flush=True)
while True:
    readable, _, _ = select.select(listeners, [], [])
    for listener in readable:
        front, _ = listener.accept()
        if not limit.acquire(blocking=False):
            front.close()
            continue
        threading.Thread(target=forward, args=(front, listener.getsockname()[1]), daemon=True).start()
`

type computerRelayConfig struct {
	Socket, ContainerID, JobID, AttemptID, NamespaceInode, Address, ExecID string
	ViewPort, ControlPort                                                  int
}
type computerRelaySnapshot struct {
	NamespaceInode    string `json:"namespace_inode"`
	ViewInode         string `json:"view_inode"`
	ControlInode      string `json:"control_inode"`
	ViewOwner         string `json:"view_owner"`
	ControlOwner      string `json:"control_owner"`
	RelayViewInode    string `json:"relay_view_inode"`
	RelayControlInode string `json:"relay_control_inode"`
}
type computerRelayEvent struct {
	RelayPID                                uint32              `json:"relay_pid"`
	Event                                   string              `json:"event"`
	Config                                  computerRelayConfig `json:"config"`
	Before, During, After                   computerRelaySnapshot
	ExitConfirmed, Deleted, ListenersAbsent bool
	Error                                   string `json:"error,omitempty"`
}

func computerRelaySocketSnapshot(pid uint32, config computerRelayConfig) (computerRelaySnapshot, error) {
	var result computerRelaySnapshot
	info, err := os.Stat(fmt.Sprintf("/proc/%d/ns/net", pid))
	if err != nil {
		return result, err
	}
	result.NamespaceInode = strconv.FormatUint(info.Sys().(*syscall.Stat_t).Ino, 10)
	if result.NamespaceInode != config.NamespaceInode {
		return result, errors.New("target network namespace changed")
	}
	payload, err := os.ReadFile(fmt.Sprintf("/proc/%d/net/tcp", pid))
	if err != nil {
		return result, err
	}
	ip := net.ParseIP(config.Address).To4()
	if ip == nil {
		return result, errors.New("target is not IPv4")
	}
	address := fmt.Sprintf("%02X%02X%02X%02X", ip[3], ip[2], ip[1], ip[0])
	for _, line := range strings.Split(string(payload), "\n") {
		fields := strings.Fields(line)
		if len(fields) < 10 || fields[3] != "0A" {
			continue
		}
		switch fields[1] {
		case fmt.Sprintf("0100007F:%04X", config.ViewPort):
			result.ViewInode = fields[9]
		case fmt.Sprintf("0100007F:%04X", config.ControlPort):
			result.ControlInode = fields[9]
		case fmt.Sprintf("%s:%04X", address, config.ViewPort):
			result.RelayViewInode = fields[9]
		case fmt.Sprintf("%s:%04X", address, config.ControlPort):
			result.RelayControlInode = fields[9]
		}
	}
	if result.ViewInode == "" || result.ControlInode == "" {
		return result, errors.New("real loopback endpoint is not listening")
	}
	targetGroup, err := os.ReadFile(fmt.Sprintf("/proc/%d/cgroup", pid))
	if err != nil {
		return result, err
	}
	entries, err := os.ReadDir("/proc")
	if err != nil {
		return result, err
	}
	for _, entry := range entries {
		if _, err := strconv.Atoi(entry.Name()); err != nil {
			continue
		}
		group, err := os.ReadFile(filepath.Join("/proc", entry.Name(), "cgroup"))
		if err != nil || string(group) != string(targetGroup) {
			continue
		}
		fds, _ := os.ReadDir(filepath.Join("/proc", entry.Name(), "fd"))
		for _, fd := range fds {
			link, _ := os.Readlink(filepath.Join("/proc", entry.Name(), "fd", fd.Name()))
			if link == "socket:["+result.ViewInode+"]" {
				result.ViewOwner = entry.Name()
			}
			if link == "socket:["+result.ControlInode+"]" {
				result.ControlOwner = entry.Name()
			}
		}
	}
	if result.ViewOwner == "" || result.ControlOwner == "" {
		return result, errors.New("loopback endpoint owner is outside target cgroup")
	}
	return result, nil
}
func sameComputerRelayBackend(a, b computerRelaySnapshot) bool {
	return a.NamespaceInode == b.NamespaceInode && a.ViewInode == b.ViewInode && a.ControlInode == b.ControlInode && a.ViewOwner == b.ViewOwner && a.ControlOwner == b.ControlOwner
}

type relayManagedProcess interface {
	Status(context.Context) (containerd.Status, error)
	Kill(context.Context, syscall.Signal, ...containerd.KillOpts) error
	Delete(context.Context, ...containerd.ProcessDeleteOpts) (*containerd.ExitStatus, error)
}

func stopComputerRelayProcess(ctx context.Context, process relayManagedProcess, exited <-chan containerd.ExitStatus) (bool, bool, error) {
	status, err := process.Status(ctx)
	if err != nil {
		return false, false, err
	}
	if status.Status == containerd.Created {
		_, err = process.Delete(ctx)
		return false, err == nil, err // Created-but-never-started is not an exit proof.
	}
	if status.Status != containerd.Stopped {
		if err := process.Kill(ctx, syscall.SIGKILL); err != nil {
			return false, false, err
		}
	}
	select {
	case exit, ok := <-exited:
		if !ok {
			return false, false, errors.New("relay exit observation channel closed without evidence")
		}
		if _, _, err := exit.Result(); err != nil {
			return false, false, err
		}
	case <-ctx.Done():
		return false, false, ctx.Err()
	}
	if _, err := process.Delete(ctx); err != nil {
		return true, false, err
	}
	return true, true, nil
}

func runComputerRelaySupervisor(input io.Reader, output io.Writer) (resultErr error) {
	reader := bufio.NewReader(input)
	var config computerRelayConfig
	line, err := reader.ReadBytes('\n')
	if err != nil {
		return err
	}
	if err = json.Unmarshal(line, &config); err != nil {
		return err
	}
	if os.Geteuid() != 0 || !strings.HasPrefix(config.Address, "198.18.") && !strings.HasPrefix(config.Address, "198.19.") || config.ExecID == "" || config.AttemptID == "" {
		return errors.New("invalid relay authority")
	}
	ctx, cancel := context.WithCancel(namespaces.WithNamespace(context.Background(), ocihelper.ContainerdNamespace))
	defer cancel()
	setupCtx, setupCancel := context.WithTimeout(ctx, 10*time.Second)
	defer setupCancel()
	client, err := containerd.New(config.Socket)
	if err != nil {
		return err
	}
	defer client.Close()
	container, err := client.LoadContainer(setupCtx, config.ContainerID)
	if err != nil {
		return err
	}
	labels, err := container.Labels(setupCtx)
	if err != nil {
		return err
	}
	if labels["io.wefty/job_id"] != config.JobID || labels["io.wefty/attempt_id"] != config.AttemptID {
		return errors.New("container labels do not match target attempt")
	}
	task, err := container.Task(setupCtx, nil)
	if err != nil {
		return err
	}
	pid := task.Pid()
	before, err := computerRelaySocketSnapshot(pid, config)
	if err != nil {
		return err
	}
	if before.RelayViewInode != "" || before.RelayControlInode != "" {
		return errors.New("veth proof ports already bound")
	}
	spec, err := container.Spec(setupCtx)
	if err != nil {
		return err
	}
	if spec.Process == nil {
		return errors.New("target process specification missing")
	}
	processSpec := *spec.Process
	processSpec.Args = []string{"/usr/bin/python3", "-c", computerVethRelayPython, config.Address, fmt.Sprint(config.ViewPort), fmt.Sprint(config.ControlPort)}
	processSpec.Terminal = false
	stdoutRead, stdoutWrite := io.Pipe()
	defer stdoutRead.Close()
	defer stdoutWrite.Close()
	process, execErr := task.Exec(setupCtx, config.ExecID, &processSpec, cio.NewCreator(cio.WithStreams(nil, stdoutWrite, os.Stderr)))
	if execErr != nil {
		// An uncertain Exec response never authorizes replay. Recover only the
		// exact exec ID so an allocated process can still be deleted.
		recoveryCtx, recoveryCancel := context.WithTimeout(ctx, 10*time.Second)
		process, err = task.LoadProcess(recoveryCtx, config.ExecID, nil)
		recoveryCancel()
		if err != nil {
			return errors.Join(execErr, fmt.Errorf("recover exact relay exec %s: %w", config.ExecID, err))
		}
	}
	var exited <-chan containerd.ExitStatus
	encoder := json.NewEncoder(output)
	defer func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(ctx, 10*time.Second)
		defer cleanupCancel()
		event := computerRelayEvent{Event: "stopped", Config: config, Before: before}
		if exited == nil {
			exited, err = process.Wait(cleanupCtx)
			resultErr = errors.Join(resultErr, err)
		}
		event.ExitConfirmed, event.Deleted, err = stopComputerRelayProcess(cleanupCtx, process, exited)
		resultErr = errors.Join(resultErr, err)
		after, snapshotErr := computerRelaySocketSnapshot(pid, config)
		event.After = after
		event.ListenersAbsent = snapshotErr == nil && after.RelayViewInode == "" && after.RelayControlInode == ""
		if snapshotErr != nil || !event.ListenersAbsent || !sameComputerRelayBackend(before, after) {
			resultErr = errors.Join(resultErr, errors.New("relay cleanup did not preserve backend identity and prove veth listener absence"), snapshotErr)
		}
		if resultErr != nil {
			event.Error = resultErr.Error()
		}
		if err := encoder.Encode(event); err != nil {
			resultErr = errors.Join(resultErr, err)
		}
	}()
	if execErr != nil {
		return execErr
	}
	exited, err = process.Wait(ctx)
	if err != nil {
		return err
	} // Observe before Start.
	ready := make(chan string, 1)
	// Drain readiness before Start: an uncertain Start response can make the
	// SDK wait for its I/O copier, which must not block on our pipe reader.
	go func() { line, _ := bufio.NewReader(stdoutRead).ReadString('\n'); ready <- strings.TrimSpace(line) }()
	if err = process.Start(setupCtx); err != nil {
		return err
	}
	select {
	case line := <-ready:
		if line != "relay-ready" {
			return fmt.Errorf("relay readiness failed: %q", line)
		}
	case <-setupCtx.Done():
		return setupCtx.Err()
	}
	if inode, inodeErr := networkNamespaceInodeForRelay(process.Pid()); inodeErr != nil || inode != config.NamespaceInode {
		return errors.Join(errors.New("relay process namespace differs from target"), inodeErr)
	}
	during, err := computerRelaySocketSnapshot(pid, config)
	if err != nil {
		return err
	}
	if !sameComputerRelayBackend(before, during) || during.RelayViewInode == "" || during.RelayControlInode == "" || !computerRelayOwnsSockets(process.Pid(), during) {
		return errors.New("relay changed target backend or did not bind both veth ports")
	}
	if err = encoder.Encode(computerRelayEvent{Event: "ready", RelayPID: process.Pid(), Config: config, Before: before, During: during}); err != nil {
		return err
	}
	_, err = reader.ReadString('\n') // Parent stop or EOF both trigger owned cleanup.
	if err != nil && !errors.Is(err, io.EOF) {
		return err
	}
	return nil
}

func startComputerVethRelay(t *testing.T, computer l1.Computer, endpoints liveComputerEndpointEnvironment) (computerRelayEvent, func() computerRelayEvent) {
	t.Helper()
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	config := computerRelayConfig{Socket: requiredComputerRealtimeEnvironment(t, "WEFTY_OCI_CONTAINERD_ADDRESS"), ContainerID: liveComputerContainerID(t, computer.CurrentJobID), JobID: computer.CurrentJobID, AttemptID: computer.CurrentJob.CurrentAttemptID, NamespaceInode: endpoints.NamespaceInode, Address: endpoints.Address, ViewPort: endpoints.ViewPort, ControlPort: endpoints.ControlPort, ExecID: fmt.Sprintf("crossover-relay-%d", time.Now().UnixNano())}
	command := exec.Command("sudo", "env", "WEFTY_COMPUTER_RELAY_SUPERVISOR=1", executable)
	stdin, err := command.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	stdout, err := command.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	command.Stderr = os.Stderr
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	events := make(chan computerRelayEvent, 2)
	waited := make(chan error, 1)
	go func() {
		defer func() { close(events); waited <- command.Wait() }()
		decoder := json.NewDecoder(stdout)
		for {
			var event computerRelayEvent
			if decoder.Decode(&event) != nil {
				return
			}
			events <- event
		}
	}()
	var once sync.Once
	var stopped computerRelayEvent
	stop := func() computerRelayEvent {
		once.Do(func() {
			_ = stdin.Close()
			timer := time.NewTimer(15 * time.Second)
			defer timer.Stop()
			eventsOpen := true
			for stopped.Event != "stopped" && eventsOpen {
				select {
				case event, ok := <-events:
					if !ok {
						t.Errorf("relay supervisor exited without cleanup receipt")
						eventsOpen = false
						continue
					}
					stopped = event
				case <-timer.C:
					t.Errorf("relay supervisor cleanup unconfirmed, pid=%d exec=%s", command.Process.Pid, config.ExecID)
					return
				}
			}
			select {
			case err := <-waited:
				if err != nil {
					t.Errorf("relay supervisor exit: %v", err)
				}
			case <-timer.C:
				t.Errorf("relay supervisor exit unconfirmed, pid=%d", command.Process.Pid)
			}
			if stopped.Error != "" || !stopped.ExitConfirmed || !stopped.Deleted || !stopped.ListenersAbsent {
				t.Errorf("relay cleanup failed: %+v", stopped)
			}
		})
		return stopped
	}
	t.Cleanup(func() { stop() })
	if err := json.NewEncoder(stdin).Encode(config); err != nil {
		t.Fatal(err)
	}
	select {
	case event, ok := <-events:
		if event.Event == "stopped" {
			stopped = event
		}
		if !ok || event.Event != "ready" || event.Error != "" {
			t.Fatalf("relay did not become ready: %+v", event)
		}
		return event, stop
	case <-time.After(15 * time.Second):
		t.Fatal("relay readiness unconfirmed")
	}
	return computerRelayEvent{}, stop
}

// Keep this interface assertion close to the narrow lifecycle seam.
var _ relayManagedProcess = (containerd.Process)(nil)

func computerRelayBackendIdentity(snapshot computerRelaySnapshot) string {
	return strings.Join([]string{snapshot.NamespaceInode, snapshot.ViewInode, snapshot.ControlInode, snapshot.ViewOwner, snapshot.ControlOwner}, ":")
}

func computerRelayOwnsSockets(pid uint32, snapshot computerRelaySnapshot) bool {
	fds, err := os.ReadDir(fmt.Sprintf("/proc/%d/fd", pid))
	if err != nil {
		return false
	}
	view, control := false, false
	for _, fd := range fds {
		link, _ := os.Readlink(fmt.Sprintf("/proc/%d/fd/%s", pid, fd.Name()))
		view = view || link == "socket:["+snapshot.RelayViewInode+"]"
		control = control || link == "socket:["+snapshot.RelayControlInode+"]"
	}
	return view && control
}

type fakeRelayProcess struct {
	status          containerd.ProcessStatus
	killed, deleted bool
	deleteErr       error
	killErr         error
}

func (p *fakeRelayProcess) Status(context.Context) (containerd.Status, error) {
	return containerd.Status{Status: p.status}, nil
}
func (p *fakeRelayProcess) Kill(context.Context, syscall.Signal, ...containerd.KillOpts) error {
	p.killed = true
	return p.killErr
}
func (p *fakeRelayProcess) Delete(context.Context, ...containerd.ProcessDeleteOpts) (*containerd.ExitStatus, error) {
	p.deleted = true
	return nil, p.deleteErr
}
func TestRelayCleanupRequiresObservedExitAndDeletion(t *testing.T) {
	for _, name := range []string{"confirmed", "wait error", "closed wait", "delete error", "kill error", "created"} {
		t.Run(name, func(t *testing.T) {
			process := &fakeRelayProcess{status: containerd.Running}
			exited := make(chan containerd.ExitStatus, 1)
			var waitErr error
			if name == "wait error" {
				waitErr = errors.New("lost exit observation")
			}
			if name == "delete error" {
				process.deleteErr = errors.New("delete failed")
			}
			if name == "kill error" {
				process.killErr = errors.New("kill failed")
			}
			if name == "created" {
				process.status = containerd.Created
			}
			if name == "closed wait" {
				close(exited)
			} else {
				exited <- *containerd.NewExitStatus(137, time.Now(), waitErr)
			}
			confirmed, deleted, err := stopComputerRelayProcess(t.Context(), process, exited)
			switch name {
			case "confirmed":
				if err != nil || !confirmed || !deleted || !process.killed {
					t.Fatalf("cleanup: %t %t %v", confirmed, deleted, err)
				}
			case "created":
				if err != nil || confirmed || !deleted || process.killed {
					t.Fatal("unstarted process claimed exit or was signaled")
				}
			case "delete error":
				if err == nil || !confirmed || deleted {
					t.Fatal("Delete failure accepted")
				}
			default:
				if err == nil || confirmed || deleted || process.deleted {
					t.Fatal("missing exit evidence allowed Delete or successful cleanup")
				}
			}
		})
	}
}

func TestVethRelayLivenessRequiresRealRFBBackends(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("distinct Linux loopback addresses exercise the relay without host network changes")
	}
	var servers []*http.Server
	var ports []int
	for range 2 {
		listener, err := net.Listen("tcp4", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		ports = append(ports, listener.Addr().(*net.TCPAddr).Port)
		server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			connection, err := websocket.Accept(w, r, &websocket.AcceptOptions{Subprotocols: []string{"binary"}})
			if err != nil {
				return
			}
			stream := websocket.NetConn(r.Context(), connection, websocket.MessageBinary)
			defer stream.Close()
			if _, err = stream.Write([]byte("RFB 003.008\n")); err != nil {
				return
			}
			if _, err = io.ReadFull(stream, make([]byte, 12)); err != nil {
				return
			}
			if _, err = stream.Write([]byte{1, 1}); err != nil {
				return
			}
			if _, err = io.ReadFull(stream, make([]byte, 1)); err != nil {
				return
			}
			if _, err = stream.Write(make([]byte, 4)); err != nil {
				return
			}
			if _, err = io.ReadFull(stream, make([]byte, 1)); err != nil {
				return
			}
			init := make([]byte, 24)
			init[1], init[3] = 32, 32
			_, _ = stream.Write(init)
			_, _ = io.Copy(io.Discard, stream)
		})}
		servers = append(servers, server)
		go server.Serve(listener)
		t.Cleanup(func() { server.Close() })
	}
	relay := exec.Command("python3", "-c", computerVethRelayPython, "127.0.0.2", fmt.Sprint(ports[0]), fmt.Sprint(ports[1]))
	stdout, err := relay.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	relay.Stderr = os.Stderr
	if err = relay.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = relay.Process.Kill(); _ = relay.Wait() })
	ready := make(chan string, 1)
	go func() { line, _ := bufio.NewReader(stdout).ReadString('\n'); ready <- strings.TrimSpace(line) }()
	select {
	case line := <-ready:
		if line != "relay-ready" {
			t.Fatalf("relay startup: %q", line)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("relay startup timed out")
	}
	probe := func() screenCrossoverReceipt {
		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		defer cancel()
		output, err := exec.CommandContext(ctx, "python3", "-c", liveComputerScreenCrossoverPython, "wayland", "target", "target", fmt.Sprint(ports[0]), fmt.Sprint(ports[1]), "127.0.0.2", "1", "::1", "1", "127.0.0.2").CombinedOutput()
		if err != nil {
			t.Fatalf("real RFB probe did not complete: %v\n%s", err, output)
		}
		var receipt screenCrossoverReceipt
		if err = json.Unmarshal(output, &receipt); err != nil {
			t.Fatal(err)
		}
		return receipt
	}
	live := probe()
	if live.ViewRead.Outcome != "read_succeeded" || live.ControlInject.Outcome != "inject_succeeded" {
		t.Fatalf("real RFB relay liveness failed: %+v", live)
	}
	for _, server := range servers {
		server.Close()
	}
	unavailable := probe()
	if unavailable.ViewRead.Outcome == "read_succeeded" || unavailable.ControlInject.Outcome == "inject_succeeded" {
		t.Fatalf("relay synthesized RFB liveness without backends: %+v", unavailable)
	}
}

func TestRFBProbeFailsClosedOnHandshakeEOF(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("probe reads Linux namespace authority")
	}
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	go func() {
		for {
			connection, err := listener.Accept()
			if err != nil {
				return
			}
			go func() {
				defer connection.Close()
				reader := bufio.NewReader(connection)
				for {
					line, err := reader.ReadString('\n')
					if err != nil || line == "\r\n" {
						return
					}
				}
			}()
		}
	}()
	port := fmt.Sprint(listener.Addr().(*net.TCPAddr).Port)
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	output, err := exec.CommandContext(ctx, "python3", "-c", liveComputerScreenCrossoverPython, "wayland", "target", "target", port, port, "127.0.0.1", "1", "::1", "1", "127.0.0.1").CombinedOutput()
	if err != nil {
		t.Fatalf("EOF did not produce a bounded failed-liveness receipt: %v %s", err, output)
	}
	var receipt screenCrossoverReceipt
	if err = json.Unmarshal(output, &receipt); err != nil {
		t.Fatal(err)
	}
	if receipt.ViewRead.Outcome != "protocol_error" || receipt.ControlInject.Outcome != "protocol_error" {
		t.Fatalf("EOF accepted: %+v", receipt)
	}
}

func networkNamespaceInodeForRelay(pid uint32) (string, error) {
	info, err := os.Stat(fmt.Sprintf("/proc/%d/ns/net", pid))
	if err != nil {
		return "", err
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return "", errors.New("namespace stat authority unavailable")
	}
	return strconv.FormatUint(uint64(stat.Ino), 10), nil
}
