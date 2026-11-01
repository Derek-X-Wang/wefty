package main

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
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/fabric/local"
	"github.com/Derek-X-Wang/wefty/fabric/participantstate"
	"github.com/Derek-X-Wang/wefty/fabric/plain"
	"github.com/Derek-X-Wang/wefty/internal/connprofile"
	"github.com/Derek-X-Wang/wefty/internal/testwait"
	"github.com/Derek-X-Wang/wefty/l1"
)

func TestClientServiceRequiredFromRealBinary(t *testing.T) {
	binary := buildWefty(t)
	home := t.TempDir()
	profile := filepath.Join(home, "profile.json")
	state := savedChoiceState(t, "client")
	if err := connprofile.Write(profile, connprofile.Profile{Version: 1, Fabric: "plain", L1Address: unusedAddress(t), Participants: map[string]connprofile.Participant{
		"client": {Name: "wefty://client/test", StateDirectory: state, NodeID: "client"},
	}}); err != nil {
		t.Fatal(err)
	}
	release, err := participantstate.Acquire(state, "wefty://client/test")
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	for _, mode := range []string{"plain", "tsnet"} {
		saved, err := connprofile.Read(profile)
		if err != nil {
			t.Fatal(err)
		}
		saved.Fabric = mode
		if err := connprofile.Write(profile, saved); err != nil {
			t.Fatal(err)
		}
		for _, command := range [][]string{{"nodes", "list"}, {"status"}} {
			args := append([]string{"--profile=" + profile, "--json"}, command...)
			code, output := runWeftyWithEnv(t, binary, 10*time.Second, []string{"HOME=" + home, "WEFTY_CLIENT_SOCKET=" + clientServiceSocketForTest(t)}, args...)
			var response contract.ErrorResponse
			if json.Unmarshal([]byte(output), &response) != nil || code != 13 || response.Error.Code != "fabric_unreachable" || response.Error.Retryable || !strings.Contains(response.Error.Message, "owner") || !strings.Contains(response.Error.Message, "client service") {
				t.Fatalf("%s %v missing service: exit=%d output=%s", mode, command, code, output)
			}
		}
	}

}

func TestParticipantLockFileIsNotSavedState(t *testing.T) {
	state := t.TempDir()
	if err := os.WriteFile(filepath.Join(state, ".participant.lock"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if hasSavedParticipantState(state) {
		t.Fatal("lock file alone counted as saved participant state")
	}
}

type clientServiceProcess struct {
	command *exec.Cmd
	done    chan struct{}
	err     error
}

func clientServiceSocketForTest(t *testing.T) string {
	t.Helper()
	directory, err := os.MkdirTemp("/tmp", "wc-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(directory) })
	return filepath.Join(directory, "s")
}
func startClientServiceForTest(t *testing.T, binary, profile, socket string) *clientServiceProcess {
	t.Helper()
	logfile, err := os.CreateTemp(t.TempDir(), "service-log-")
	if err != nil {
		t.Fatal(err)
	}
	command := exec.Command(binary, "--profile="+profile, "client-service", "--socket="+socket)
	command.Stdout, command.Stderr = logfile, logfile
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	process := &clientServiceProcess{command: command, done: make(chan struct{})}
	go func() { process.err = command.Wait(); close(process.done) }()
	t.Cleanup(func() {
		select {
		case <-process.done:
		default:
			_ = command.Process.Signal(syscall.SIGTERM)
			select {
			case <-process.done:
			case <-time.After(5 * time.Second):
				_ = command.Process.Kill()
				<-process.done
			}
		}
		_ = logfile.Close()
	})
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		select {
		case <-process.done:
			data, _ := os.ReadFile(logfile.Name())
			t.Fatalf("client-service exited: %v %s", process.err, data)
		default:
		}
		if info, err := os.Stat(socket); err == nil && info.Mode()&os.ModeSocket != 0 {
			// Confirm that this process published a usable socket, including after a
			// killed predecessor left the same path behind.
			d, _ := local.New(socket, local.Client, "machine-client")
			if d != nil {
				ctx, cancel := context.WithTimeout(t.Context(), time.Second)
				err = d.Check(ctx)
				cancel()
				if err == nil {
					return process
				}
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	data, _ := os.ReadFile(logfile.Name())
	t.Fatalf("client-service did not become ready: %s", data)
	return nil
}
func writeClientServiceProfile(t *testing.T, address string) (string, connprofile.Profile) {
	t.Helper()
	t.Setenv("HOME", t.TempDir())
	profile := connprofile.Profile{Version: 1, Fabric: "plain", PlainFabricID: "plain-client-service-test", L1Address: address, Participants: map[string]connprofile.Participant{
		"client": {Name: "wefty://client/test", NodeID: "machine-client", StateDirectory: savedChoiceState(t, "client")},
		"person": {Name: "wefty://person/test", NodeID: "person-device", UserID: "owner", StateDirectory: savedChoiceState(t, "person")},
	}}
	path := filepath.Join(t.TempDir(), "profile.json")
	if err := connprofile.Write(path, profile); err != nil {
		t.Fatal(err)
	}
	return path, profile
}
func TestClientServiceConcurrentRealBinaryAndBuzz(t *testing.T) {
	network, _ := plain.NewNetworkWithID("plain-client-service-test")
	receiver := network.NewFabric(fabric.Identity{NodeID: "l1"})
	listener, err := receiver.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var requests atomic.Int32
	failures := make(chan string, 64)
	server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		identity, err := receiver.WhoIs(r.Context(), r.RemoteAddr)
		if err != nil || identity.NodeID != "machine-client" || identity.UserID != "" || len(identity.Tags) != 1 || identity.Tags[0] != "tag:wefty-client" {
			failures <- fmt.Sprintf("identity=%+v err=%v", identity, err)
		}
		requests.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"nodes":[],"next_cursor":""}`)
	})}
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() { _ = server.Close() })
	profile, _ := writeClientServiceProfile(t, listener.Addr().String())
	socket := clientServiceSocketForTest(t)
	t.Setenv("WEFTY_CLIENT_SOCKET", socket)
	binary := buildWefty(t)
	startClientServiceForTest(t, binary, profile, socket)
	start := make(chan struct{})
	var group sync.WaitGroup
	for range 20 {
		group.Add(1)
		go func() {
			defer group.Done()
			<-start
			code, output := runWeftyWithEnv(t, binary, 20*time.Second, nil, "--profile="+profile, "--json", "nodes", "list")
			if code != 0 {
				failures <- fmt.Sprintf("CLI exit=%d: %s", code, output)
			}
		}()
	}
	group.Add(1)
	go func() {
		defer group.Done()
		<-start
		// A Buzz-style consumer uses only fabric/local and ordinary HTTP. It has
		// no state folder, embedded backend, credential, or CLI implementation.
		d, err := local.New(socket, local.Client, "machine-client")
		if err != nil {
			failures <- err.Error()
			return
		}
		transport := &http.Transport{DialContext: func(ctx context.Context, network, _ string) (net.Conn, error) {
			return d.Dial(ctx, network, listener.Addr().String())
		}}
		defer transport.CloseIdleConnections()
		request, _ := http.NewRequestWithContext(t.Context(), "GET", "http://wefty.invalid/v1/nodes", nil)
		response, err := (&http.Client{Transport: transport, Timeout: 10 * time.Second}).Do(request)
		if err != nil {
			failures <- err.Error()
			return
		}
		defer response.Body.Close()
		body, err := io.ReadAll(response.Body)
		if err != nil || response.StatusCode != 200 || !strings.Contains(string(body), "nodes") {
			failures <- fmt.Sprintf("Buzz response=%s err=%v", body, err)
		}
	}()
	close(start)
	group.Wait()
	close(failures)
	for failure := range failures {
		t.Error(failure)
	}
	if requests.Load() != 21 {
		t.Fatalf("remote requests=%d, want 21", requests.Load())
	}
}
func TestClientServiceSecondOwnerAndSIGKILLRecovery(t *testing.T) {
	binary := buildWefty(t)
	profile, saved := writeClientServiceProfile(t, unusedAddress(t))
	socket := clientServiceSocketForTest(t)
	t.Setenv("WEFTY_CLIENT_SOCKET", socket)
	first := startClientServiceForTest(t, binary, profile, socket)
	for _, role := range []string{"client", "person"} {
		release, err := participantstate.Acquire(saved.Participants[role].StateDirectory, saved.Participants[role].Name)
		if err == nil {
			_ = release()
			t.Fatalf("service did not own %s", role)
		}
	}
	code, output := runWeftyWithEnv(t, binary, 5*time.Second, nil, "--profile="+profile, "--json", "client-service", "--socket="+clientServiceSocketForTest(t))
	var response contract.ErrorResponse
	if json.Unmarshal([]byte(output), &response) != nil || code != 3 || response.Error.Code != "participant_in_use" || !strings.Contains(response.Error.Message, "wefty://client/test") || strings.Contains(output, saved.Participants["client"].StateDirectory) {
		t.Fatalf("second owner exit=%d output=%s", code, output)
	}
	if err := first.command.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	<-first.done
	startClientServiceForTest(t, binary, profile, socket)
}

func TestClientServiceLogFollowAndCtrlCFromRealBinary(t *testing.T) {
	network := plain.NewNetwork()
	receiver := network.NewFabric(fabric.Identity{NodeID: "l3"})
	listener, err := receiver.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	blocked, cancelled := make(chan struct{}), make(chan struct{})
	server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path == "/v1/runs/stream/logs" {
			if r.URL.Query().Get("cursor") == "" {
				_ = json.NewEncoder(w).Encode(l1.LogPage{Events: []contract.LogEvent{{AttemptID: "attempt", Stream: contract.LogStdout, Sequence: 1, Bytes: []byte("streamed event\n")}}, NextCursor: "1"})
				return
			}
			close(blocked)
			<-r.Context().Done()
			close(cancelled)
			return
		}
		_ = json.NewEncoder(w).Encode(contract.RunRecord{Status: contract.RunRunning})
	})}
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() { _ = server.Close() })
	path, profile := writeClientServiceProfile(t, listener.Addr().String())
	profile.L3Address = listener.Addr().String()
	if err := connprofile.Write(path, profile); err != nil {
		t.Fatal(err)
	}
	socket := clientServiceSocketForTest(t)
	t.Setenv("WEFTY_CLIENT_SOCKET", socket)
	binary := buildWefty(t)
	startClientServiceForTest(t, binary, path, socket)
	command := exec.Command(binary, "--profile="+path, "logs", "stream", "--follow", "--poll-interval=10ms")
	output, err := command.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	diagnostic, err := os.CreateTemp(t.TempDir(), "follow-stderr")
	if err != nil {
		t.Fatal(err)
	}
	defer diagnostic.Close()
	command.Stderr = diagnostic
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- command.Wait() }()
	t.Cleanup(func() { _ = command.Process.Kill() })
	line := make(chan string, 1)
	go func() { text, _ := bufio.NewReader(output).ReadString('\n'); line <- text }()
	select {
	case text := <-line:
		if text != "streamed event\n" {
			t.Fatalf("stream=%q", text)
		}
	case <-time.After(testwait.Bound):
		t.Fatal("follow did not stream")
	}
	select {
	case <-blocked:
	case <-time.After(testwait.Bound):
		t.Fatal("follow did not keep reading")
	}
	if err := command.Process.Signal(os.Interrupt); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		var exit *exec.ExitError
		if !errors.As(err, &exit) || exit.ExitCode() != 1 {
			t.Fatalf("interrupted follow=%v", err)
		}
	case <-time.After(testwait.Bound):
		t.Fatal("Ctrl-C did not stop follow")
	}
	select {
	case <-cancelled:
	case <-time.After(testwait.Bound):
		t.Fatal("caller cancellation did not close remote stream")
	}
}

func TestClientServiceStatusDeadlineRetainsFailedCheckVerdict(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	path := participantContextProfile(t)
	socket := clientServiceSocketForTest(t)
	t.Setenv("WEFTY_CLIENT_SOCKET", socket)
	server, err := local.NewServer(socket, map[string]local.Participant{local.Client: {NodeID: "saved", Check: func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() }}})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- server.Serve(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Error(err)
			}
		case <-time.After(testwait.Bound):
			t.Error("service did not stop")
		}
	})
	deadline := time.Now().Add(time.Second)
	for {
		if _, err := os.Stat(socket); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("service did not publish socket")
		}
		time.Sleep(5 * time.Millisecond)
	}
	code, output := runWeftyWithEnv(t, buildWefty(t), 5*time.Second, nil, "--profile="+path, "--json", "status", "--timeout=40ms")
	var status clusterStatus
	if json.Unmarshal([]byte(output), &status) != nil || code != 12 || len(status.Reasons) != 1 || status.Reasons[0].Code != "participant_check_failed" {
		t.Fatalf("deadline exit=%d output=%s", code, output)
	}
}
