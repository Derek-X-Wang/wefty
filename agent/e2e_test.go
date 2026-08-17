//go:build darwin || linux

package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/fabric/plain"
	"github.com/Derek-X-Wang/wefty/l1"
)

func TestAgentProcessEndToEndPlainFabric(t *testing.T) {
	tests := []struct {
		name            string
		leaseDuration   time.Duration
		renewalInterval time.Duration
		arguments       []string
		wantOutput      string
	}{
		{
			name:            "claim execute and complete",
			leaseDuration:   3 * time.Second,
			renewalInterval: 500 * time.Millisecond,
			arguments:       []string{"processhelper", "stdout", "e2e-complete"},
			wantOutput:      "e2e-complete\n",
		},
		{
			// The process (3s) outlives the original lease (1s) three times
			// over, so success proves renewal. Margins are wide because this
			// runs real processes over HTTP on shared CI runners; sub-second
			// leases flake under scheduler pauses.
			name:            "renew short lease during long process",
			leaseDuration:   1 * time.Second,
			renewalInterval: 100 * time.Millisecond,
			arguments:       []string{"processhelper", "sleep", "3000"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			runProcessE2E(t, test.leaseDuration, test.renewalInterval, test.arguments, test.wantOutput)
		})
	}
}

func TestAgentUploadsLongPartialAndInvalidUTF8Output(t *testing.T) {
	directory := t.TempDir()
	readyFile := filepath.Join(directory, "ready.json")
	server := startManagedProcess(t, controlPlanePath,
		"--fabric=plain",
		"--listen=127.0.0.1:0",
		"--db="+filepath.Join(directory, "l1.sqlite"),
		"--node-tags=stable-node=linux",
		"--ready-file="+readyFile,
	)
	server.command.Stdout = &lockedBuffer{}
	server.command.Stderr = server.command.Stdout
	server.start(t)
	address := waitForReadyAddress(t, readyFile, server, 10*time.Second)

	node := startManagedProcess(t, agentBinaryPath,
		"--fabric=plain",
		"--control-plane="+address,
		"--node-id=stable-node",
		"--log-spool-dir="+directory,
		"--plain-identity=fabric-node",
		"--heartbeat-interval=5s",
		"--claim-interval=10ms",
	)
	node.command.Stdout = &lockedBuffer{}
	node.command.Stderr = node.command.Stdout
	node.start(t)

	clientFabric := plain.NewNetwork().NewFabric(fabric.Identity{NodeID: "raw-client", Tags: []string{l1.DefaultClientPrincipalTag}})
	client := newHTTPClient(clientFabric, address)
	defer client.CloseIdleConnections()
	workingDirectory := t.TempDir()
	job := submitE2EJob(t, client, contract.JobSpec{
		SchemaVersion: contract.SchemaVersionV1,
		DispatchKey:   "e2e-raw-" + fmt.Sprint(time.Now().UnixNano()),
		Kind:          "process",
		Class:         contract.JobClassOneShot,
		RoutingTags:   []string{"linux"},
		Execution: contract.ExecutionSpec{
			Executable:       contract.ExecutableSpec{Path: agentHelperPath},
			Argv:             []string{"processhelper", "raw-output"},
			WorkingDirectory: workingDirectory,
			HandoffDirectory: workingDirectory,
		},
	})
	waitForE2EJobState(t, client, job.JobID, contract.JobSucceeded, 10*time.Second)
	events, _ := pollAllE2ELogs(t, client, job.JobID, "")
	want := append(bytes.Repeat([]byte{'x'}, 70*1024), []byte("partial-without-newline")...)
	want = append(want, 0xff, 0xfe, 0x00, '\n')
	if got := joinAgentLogStream(events, contract.LogStdout); !bytes.Equal(got, want) {
		t.Fatalf("uploaded stdout differs: got %d bytes, want %d", len(got), len(want))
	}
	node.stop(t)
	server.stop(t)
}

func TestAgentLogPollTailsRunningProcess(t *testing.T) {
	directory := t.TempDir()
	readyFile := filepath.Join(directory, "ready.json")
	server := startManagedProcess(t, controlPlanePath,
		"--fabric=plain",
		"--listen=127.0.0.1:0",
		"--db="+filepath.Join(directory, "l1.sqlite"),
		"--node-tags=stable-node=linux",
		"--ready-file="+readyFile,
	)
	server.command.Stdout = &lockedBuffer{}
	server.command.Stderr = server.command.Stdout
	server.start(t)
	address := waitForReadyAddress(t, readyFile, server, 10*time.Second)

	node := startManagedProcess(t, agentBinaryPath,
		"--fabric=plain",
		"--control-plane="+address,
		"--node-id=stable-node",
		"--log-spool-dir="+directory,
		"--plain-identity=fabric-node",
		"--heartbeat-interval=5s",
		"--claim-interval=10ms",
	)
	node.command.Stdout = &lockedBuffer{}
	node.command.Stderr = node.command.Stdout
	node.start(t)

	clientFabric := plain.NewNetwork().NewFabric(fabric.Identity{NodeID: "tail-client", Tags: []string{l1.DefaultClientPrincipalTag}})
	client := newHTTPClient(clientFabric, address)
	defer client.CloseIdleConnections()
	workingDirectory := t.TempDir()
	job := submitE2EJob(t, client, contract.JobSpec{
		SchemaVersion: contract.SchemaVersionV1,
		DispatchKey:   "e2e-tail-" + fmt.Sprint(time.Now().UnixNano()),
		Kind:          "process",
		Class:         contract.JobClassOneShot,
		RoutingTags:   []string{"linux"},
		Execution: contract.ExecutionSpec{
			Executable:       contract.ExecutableSpec{Path: agentHelperPath},
			Argv:             []string{"processhelper", "paced-output", "800"},
			WorkingDirectory: workingDirectory,
			HandoffDirectory: workingDirectory,
		},
	})

	deadline := time.Now().Add(5 * time.Second)
	var firstPage l1.LogPage
	for time.Now().Before(deadline) {
		firstPage = pollE2ELogs(t, client, job.JobID, "", 1)
		if len(firstPage.Events) > 0 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if len(firstPage.Events) != 1 || string(firstPage.Events[0].Bytes) != "first\n" {
		t.Fatalf("live first page = %#v", firstPage)
	}
	running := getE2EJob(t, client, job.JobID)
	if running.State != contract.JobRunning {
		t.Fatalf("job state while first output is tail-able = %q, want running", running.State)
	}

	waitForE2EJobState(t, client, job.JobID, contract.JobSucceeded, 10*time.Second)
	rest, _ := pollAllE2ELogs(t, client, job.JobID, firstPage.NextCursor)
	all := append(append([]contract.LogEvent(nil), firstPage.Events...), rest...)
	for index, event := range all {
		if event.Stream != contract.LogStdout || event.Sequence != uint64(index) {
			t.Fatalf("event %d = %#v", index, event)
		}
	}
	if got := string(joinAgentLogStream(all, contract.LogStdout)); got != "first\nsecond\n" {
		t.Fatalf("tailed stdout = %q", got)
	}
	node.stop(t)
	server.stop(t)
}

func runProcessE2E(t *testing.T, leaseDuration, renewalInterval time.Duration, arguments []string, wantOutput string) {
	t.Helper()
	directory := t.TempDir()
	readyFile := filepath.Join(directory, "ready.json")
	serverLogs := &lockedBuffer{}
	server := startManagedProcess(t, controlPlanePath,
		"--fabric=plain",
		"--listen=127.0.0.1:0",
		"--db="+filepath.Join(directory, "l1.sqlite"),
		"--lease-duration="+leaseDuration.String(),
		"--node-tags=stable-node=linux",
		"--ready-file="+readyFile,
	)
	server.command.Stdout = serverLogs
	server.command.Stderr = serverLogs
	server.start(t)
	address := waitForReadyAddress(t, readyFile, server, 10*time.Second)

	agentLogs := &lockedBuffer{}
	node := startManagedProcess(t, agentBinaryPath,
		"--fabric=plain",
		"--control-plane="+address,
		"--node-id=stable-node",
		"--log-spool-dir="+directory,
		"--plain-identity=fabric-node",
		"--heartbeat-interval=5s",
		"--claim-interval=10ms",
		"--renewal-interval="+renewalInterval.String(),
	)
	node.command.Stdout = agentLogs
	node.command.Stderr = agentLogs
	node.start(t)

	clientFabric := plain.NewNetwork().NewFabric(fabric.Identity{NodeID: "e2e-client", Tags: []string{l1.DefaultClientPrincipalTag}})
	client := newHTTPClient(clientFabric, address)
	defer client.CloseIdleConnections()
	workingDirectory := t.TempDir()
	job := submitE2EJob(t, client, contract.JobSpec{
		SchemaVersion: contract.SchemaVersionV1,
		DispatchKey:   "e2e-" + fmt.Sprint(time.Now().UnixNano()),
		Kind:          "process",
		Class:         contract.JobClassOneShot,
		RoutingTags:   []string{"linux"},
		Execution: contract.ExecutionSpec{
			Executable:       contract.ExecutableSpec{Path: agentHelperPath},
			Argv:             arguments,
			WorkingDirectory: workingDirectory,
			HandoffDirectory: workingDirectory,
		},
	})
	waitForE2EJobState(t, client, job.JobID, contract.JobSucceeded, 10*time.Second)
	node.stop(t)
	server.stop(t)
	if wantOutput != "" && !bytes.Contains(agentLogs.Bytes(), []byte(wantOutput)) {
		t.Fatalf("agent output does not contain %q:\n%s", wantOutput, agentLogs.Bytes())
	}
}

func submitE2EJob(t *testing.T, client *http.Client, spec contract.JobSpec) l1.Job {
	t.Helper()
	payload, err := json.Marshal(spec)
	if err != nil {
		t.Fatal(err)
	}
	request, err := http.NewRequest(http.MethodPost, "http://control-plane.invalid/v1/jobs", bytes.NewReader(payload))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusCreated {
		t.Fatalf("submit status = %d body=%s", response.StatusCode, body)
	}
	var job l1.Job
	if err := json.Unmarshal(body, &job); err != nil {
		t.Fatal(err)
	}
	return job
}

func waitForE2EJobState(t *testing.T, client *http.Client, jobID string, state contract.JobState, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		job := getE2EJob(t, client, jobID)
		if job.State == state {
			return
		}
		if job.State == contract.JobFailed {
			t.Fatalf("job failed while waiting for %q", state)
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for job %q state %q", jobID, state)
}

func getE2EJob(t *testing.T, client *http.Client, jobID string) l1.Job {
	t.Helper()
	request, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "http://control-plane.invalid/v1/jobs/"+jobID, nil)
	if err != nil {
		t.Fatal(err)
	}
	response, err := client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusOK {
		t.Fatalf("get job status = %d body=%s", response.StatusCode, body)
	}
	var job l1.Job
	if err := json.Unmarshal(body, &job); err != nil {
		t.Fatal(err)
	}
	return job
}

func pollE2ELogs(t *testing.T, client *http.Client, jobID, cursor string, limit int) l1.LogPage {
	t.Helper()
	path := fmt.Sprintf("http://control-plane.invalid/v1/jobs/%s/logs?limit=%d", jobID, limit)
	if cursor != "" {
		path += "&cursor=" + cursor
	}
	response, err := client.Get(path)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusOK {
		t.Fatalf("poll logs status = %d body=%s", response.StatusCode, body)
	}
	var page l1.LogPage
	if err := json.Unmarshal(body, &page); err != nil {
		t.Fatal(err)
	}
	return page
}

func pollAllE2ELogs(t *testing.T, client *http.Client, jobID, cursor string) ([]contract.LogEvent, string) {
	t.Helper()
	var events []contract.LogEvent
	for {
		page := pollE2ELogs(t, client, jobID, cursor, 1000)
		events = append(events, page.Events...)
		cursor = page.NextCursor
		if len(page.Events) < 1000 {
			return events, cursor
		}
	}
}

func joinAgentLogStream(events []contract.LogEvent, stream contract.LogStream) []byte {
	var joined []byte
	for _, event := range events {
		if event.Stream == stream {
			joined = append(joined, event.Bytes...)
		}
	}
	return joined
}

type readyMetadata struct {
	Address string `json:"address"`
}

func waitForReadyAddress(t *testing.T, path string, process *managedProcess, timeout time.Duration) string {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if process.exited() {
			t.Fatalf("control plane exited before ready: %v", process.waitError())
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
	t.Fatal("timed out waiting for control-plane ready file")
	return ""
}

type managedProcess struct {
	command *exec.Cmd
	done    chan error
	once    sync.Once
	errMu   sync.Mutex
	err     error
}

func startManagedProcess(t *testing.T, executable string, arguments ...string) *managedProcess {
	t.Helper()
	process := &managedProcess{command: exec.Command(executable, arguments...), done: make(chan error, 1)}
	t.Cleanup(func() { process.stop(t) })
	return process
}

func (process *managedProcess) start(t *testing.T) {
	t.Helper()
	if err := process.command.Start(); err != nil {
		t.Fatal(err)
	}
	go func() {
		err := process.command.Wait()
		process.errMu.Lock()
		process.err = err
		process.errMu.Unlock()
		process.done <- err
		close(process.done)
	}()
}

func (process *managedProcess) stop(t *testing.T) {
	t.Helper()
	process.once.Do(func() {
		if process.command.Process == nil || process.exited() {
			return
		}
		if err := process.command.Process.Signal(os.Interrupt); err != nil {
			_ = process.command.Process.Kill()
		}
		select {
		case <-process.done:
		case <-time.After(5 * time.Second):
			_ = process.command.Process.Kill()
			<-process.done
		}
	})
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

type lockedBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (buffer *lockedBuffer) Write(payload []byte) (int, error) {
	buffer.mu.Lock()
	defer buffer.mu.Unlock()
	return buffer.b.Write(payload)
}

func (buffer *lockedBuffer) Bytes() []byte {
	buffer.mu.Lock()
	defer buffer.mu.Unlock()
	return bytes.Clone(buffer.b.Bytes())
}
