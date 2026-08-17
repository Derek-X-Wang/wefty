//go:build service_acceptance && (darwin || linux)

package serviceacceptance

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/fabric/plain"
	"github.com/Derek-X-Wang/wefty/l1"
)

var (
	agentBinaryPath       string
	controlPlanePath      string
	echoServiceBinaryPath string
)

func TestMain(main *testing.M) {
	directory, err := os.MkdirTemp("", "wefty-service-acceptance-")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	agentBinaryPath = filepath.Join(directory, "wefty-agent")
	controlPlanePath = filepath.Join(directory, "wefty-l1")
	echoServiceBinaryPath = filepath.Join(directory, "wefty-echo-service")
	for _, build := range []struct {
		name, output, pkg string
	}{
		{name: "agent", output: agentBinaryPath, pkg: "./cmd/wefty-agent"},
		{name: "control plane", output: controlPlanePath, pkg: "./cmd/wefty-l1"},
		{name: "echo service", output: echoServiceBinaryPath, pkg: "./cmd/wefty-echo-service"},
	} {
		command := exec.Command("go", "build", "-o", build.output, build.pkg)
		command.Dir = repositoryRoot()
		if output, buildErr := command.CombinedOutput(); buildErr != nil {
			fmt.Fprintf(os.Stderr, "build %s: %v\n%s", build.name, buildErr, output)
			_ = os.RemoveAll(directory)
			os.Exit(1)
		}
	}
	code := main.Run()
	if err := os.RemoveAll(directory); err != nil && code == 0 {
		fmt.Fprintln(os.Stderr, err)
		code = 1
	}
	os.Exit(code)
}

type acceptanceHarness struct {
	client *http.Client
	agent  *managedProcess
}

func newAcceptanceHarness(t *testing.T) *acceptanceHarness {
	t.Helper()
	directory := t.TempDir()
	readyFile := filepath.Join(directory, "l1-ready.json")
	controlPlane := newManagedProcess(t, controlPlanePath,
		"--fabric=plain",
		"--listen=127.0.0.1:0",
		"--db="+filepath.Join(directory, "l1.sqlite"),
		"--lease-duration=1s",
		"--node-tags=acceptance-node=service-acceptance",
		"--ready-file="+readyFile,
	)
	controlPlane.start(t)
	address := waitForReadyAddress(t, readyFile, controlPlane, 10*time.Second)

	agentProcess := newManagedProcess(t, agentBinaryPath,
		"--fabric=plain",
		"--control-plane="+address,
		"--node-id=acceptance-node",
		"--plain-identity=acceptance-agent",
		"--log-spool-dir="+filepath.Join(directory, "agent-spool"),
		"--heartbeat-interval=250ms",
		"--claim-interval=10ms",
		"--renewal-interval=100ms",
	)
	agentProcess.start(t)

	clientFabric := plain.NewNetwork().NewFabric(fabric.Identity{
		NodeID: "acceptance-client",
		Tags:   []string{l1.DefaultClientPrincipalTag},
	})
	client := &http.Client{Transport: &http.Transport{
		DialContext: func(ctx context.Context, network, _ string) (net.Conn, error) {
			return clientFabric.Dial(ctx, network, address)
		},
	}}
	t.Cleanup(client.CloseIdleConnections)
	return &acceptanceHarness{client: client, agent: agentProcess}
}

func (h *acceptanceHarness) submitEchoService(t *testing.T, port int) l1.Job {
	t.Helper()
	workingDirectory := t.TempDir()
	spec := contract.JobSpec{
		SchemaVersion: contract.SchemaVersionV1,
		DispatchKey:   "service-acceptance-" + strconv.FormatInt(time.Now().UnixNano(), 10),
		Kind:          "process",
		// The lane submits a genuine service-class spec: #59 made class
		// required and exempted services from handoff_directory, so a spec
		// carrying one would prove the wrong path ran (spec §4.4).
		Class:         contract.JobClassService,
		PublishedPort: &port,
		Restart:       contract.RestartAlways,
		RoutingTags:   []string{"service-acceptance"},
		Execution: contract.ExecutionSpec{
			Executable:       contract.ExecutableSpec{Path: echoServiceBinaryPath},
			Argv:             []string{echoServiceBinaryPath},
			Env:              map[string]string{servicePortEnvironment: strconv.Itoa(port)},
			WorkingDirectory: workingDirectory,
		},
		Limits: &contract.JobLimits{
			MaxRuntimeSeconds:  30,
			IdleTimeoutSeconds: 30,
		},
	}
	var job l1.Job
	status, body := h.doJSON(t, http.MethodPost, "/v1/jobs", spec, &job)
	if status != http.StatusCreated {
		t.Fatalf("submit echo service status = %d body=%s", status, body)
	}
	return job
}

func (h *acceptanceHarness) waitForJobState(t *testing.T, jobID string, state contract.JobState, timeout time.Duration) l1.Job {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if h.agent.exited() {
			t.Fatalf("agent exited while waiting for job %q: %v\n%s", state, h.agent.waitError(), h.agent.outputString())
		}
		var job l1.Job
		status, body := h.doJSON(t, http.MethodGet, "/v1/jobs/"+jobID, nil, &job)
		if status != http.StatusOK {
			t.Fatalf("get job status = %d body=%s", status, body)
		}
		if job.State == state {
			return job
		}
		if job.State == contract.JobFailed {
			t.Fatalf("job failed while waiting for %q\n%s", state, h.agent.outputString())
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for job %q state %q\n%s", jobID, state, h.agent.outputString())
	return l1.Job{}
}

func (h *acceptanceHarness) doJSON(t *testing.T, method, path string, input, output any) (int, []byte) {
	t.Helper()
	var body io.Reader
	if input != nil {
		payload, err := json.Marshal(input)
		if err != nil {
			t.Fatal(err)
		}
		body = bytes.NewReader(payload)
	}
	request, err := http.NewRequest(method, "http://control-plane.invalid"+path, body)
	if err != nil {
		t.Fatal(err)
	}
	if input != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	response, err := h.client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	payload, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	if output != nil && response.StatusCode >= 200 && response.StatusCode < 300 {
		if err := json.Unmarshal(payload, output); err != nil {
			t.Fatal(err)
		}
	}
	return response.StatusCode, payload
}

type readyMetadata struct {
	Address string `json:"address"`
}

func waitForReadyAddress(t *testing.T, path string, process *managedProcess, timeout time.Duration) string {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if process.exited() {
			t.Fatalf("control plane exited before ready: %v\n%s", process.waitError(), process.outputString())
		}
		payload, err := os.ReadFile(path)
		if err == nil {
			var metadata readyMetadata
			if err := json.Unmarshal(payload, &metadata); err != nil {
				t.Fatal(err)
			}
			if metadata.Address == "" {
				t.Fatal("ready file contains an empty address")
			}
			return metadata.Address
		}
		if !os.IsNotExist(err) {
			t.Fatal(err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for control-plane ready file\n%s", process.outputString())
	return ""
}

type managedProcess struct {
	command *exec.Cmd
	output  lockedBuffer
	done    chan struct{}
	once    sync.Once
	errMu   sync.Mutex
	err     error
}

func newManagedProcess(t *testing.T, executable string, arguments ...string) *managedProcess {
	t.Helper()
	process := &managedProcess{command: exec.Command(executable, arguments...), done: make(chan struct{})}
	process.command.Stdout = &process.output
	process.command.Stderr = &process.output
	t.Cleanup(func() { process.stop(t) })
	return process
}

func (process *managedProcess) start(t *testing.T) {
	t.Helper()
	if err := process.command.Start(); err != nil {
		t.Fatalf("start %s: %v", process.command.Path, err)
	}
	go func() {
		err := process.command.Wait()
		process.errMu.Lock()
		process.err = err
		process.errMu.Unlock()
		close(process.done)
	}()
}

func (process *managedProcess) stop(t *testing.T) {
	t.Helper()
	process.once.Do(func() {
		if process.command.Process == nil || process.exited() {
			return
		}
		if err := process.command.Process.Signal(syscall.SIGTERM); err != nil {
			_ = process.command.Process.Kill()
		}
		select {
		case <-process.done:
			return
		case <-time.After(250 * time.Millisecond):
		}
		// The agent treats the first TERM as a graceful drain and the second as
		// cancellation. This guarantees a failed acceptance does not orphan the
		// payload it was exercising.
		_ = process.command.Process.Signal(syscall.SIGTERM)
		select {
		case <-process.done:
		case <-time.After(5 * time.Second):
			_ = process.command.Process.Kill()
			<-process.done
			t.Errorf("force-killed %s after shutdown timeout\n%s", process.command.Path, process.outputString())
		}
	})
}

func (process *managedProcess) kill(t *testing.T) {
	t.Helper()
	if process.command.Process == nil {
		t.Fatal("cannot kill a process that was not started")
	}
	if err := process.command.Process.Kill(); err != nil && !process.exited() {
		t.Fatalf("kill %s: %v", process.command.Path, err)
	}
	select {
	case <-process.done:
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for killed process %s", process.command.Path)
	}
}

func (process *managedProcess) exited() bool {
	select {
	case <-process.done:
		return true
	default:
		return false
	}
}

func (process *managedProcess) waitError() error {
	process.errMu.Lock()
	defer process.errMu.Unlock()
	return process.err
}

func (process *managedProcess) outputString() string { return process.output.String() }

type lockedBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (buffer *lockedBuffer) Write(payload []byte) (int, error) {
	buffer.mu.Lock()
	defer buffer.mu.Unlock()
	return buffer.b.Write(payload)
}

func (buffer *lockedBuffer) String() string {
	buffer.mu.Lock()
	defer buffer.mu.Unlock()
	return buffer.b.String()
}

func repositoryRoot() string {
	_, filename, _, ok := runtime.Caller(0)
	if !ok {
		panic("locate acceptance harness source")
	}
	return filepath.Dir(filepath.Dir(filename))
}
