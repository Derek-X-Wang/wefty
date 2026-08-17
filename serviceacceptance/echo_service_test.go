//go:build service_acceptance

package serviceacceptance

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"testing"
	"time"
)

const servicePortEnvironment = "WEFTY_SERVICE_PORT"

func TestEchoService(t *testing.T) {
	root := repositoryRoot(t)
	binary := filepath.Join(t.TempDir(), "wefty-echo-service")
	build := exec.Command("go", "build", "-o", binary, "./cmd/wefty-echo-service")
	build.Dir = root
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build echo service: %v\n%s", err, output)
	}

	port := reservePort(t)
	var processOutput bytes.Buffer
	command := exec.Command(binary)
	for _, entry := range os.Environ() {
		if !strings.HasPrefix(entry, servicePortEnvironment+"=") {
			command.Env = append(command.Env, entry)
		}
	}
	command.Env = append(command.Env, fmt.Sprintf("%s=%d", servicePortEnvironment, port))
	command.Stdout = &processOutput
	command.Stderr = &processOutput
	if err := command.Start(); err != nil {
		t.Fatalf("start echo service: %v", err)
	}
	waited := make(chan error, 1)
	go func() { waited <- command.Wait() }()
	processExited := false
	t.Cleanup(func() {
		if processExited {
			return
		}
		_ = command.Process.Kill()
		<-waited
	})

	baseURL := fmt.Sprintf("http://127.0.0.1:%d", port)
	client := &http.Client{Timeout: 5 * time.Second}
	health := waitForHealth(t, client, baseURL, command, waited, &processExited, &processOutput)
	if health.PID != command.Process.Pid {
		t.Fatalf("health PID = %d, process PID = %d", health.PID, command.Process.Pid)
	}

	assertEcho(t, client, baseURL, []byte("echo acceptance"))
	assertGracefulShutdown(t, baseURL, command, waited, &processExited, &processOutput)
}

func repositoryRoot(t *testing.T) string {
	t.Helper()
	_, filename, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locate acceptance test source")
	}
	return filepath.Dir(filepath.Dir(filename))
}

func reservePort(t *testing.T) int {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve backend port: %v", err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	if err := listener.Close(); err != nil {
		t.Fatalf("release backend port reservation: %v", err)
	}
	return port
}

func waitForHealth(
	t *testing.T,
	client *http.Client,
	baseURL string,
	command *exec.Cmd,
	waited <-chan error,
	processExited *bool,
	processOutput *bytes.Buffer,
) struct{ PID int } {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		select {
		case err := <-waited:
			*processExited = true
			t.Fatalf("echo service exited before healthy: %v\n%s", err, processOutput.String())
		default:
		}
		response, err := client.Get(baseURL + "/healthz")
		if err == nil {
			var health struct{ PID int }
			decodeErr := json.NewDecoder(response.Body).Decode(&health)
			closeErr := response.Body.Close()
			if response.StatusCode == http.StatusOK && decodeErr == nil && closeErr == nil {
				return health
			}
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("echo service did not become healthy on injected port; PID %d\n%s", command.Process.Pid, processOutput.String())
	return struct{ PID int }{}
}

func assertEcho(t *testing.T, client *http.Client, baseURL string, payload []byte) {
	t.Helper()
	response, err := client.Post(baseURL+"/echo", "application/octet-stream", bytes.NewReader(payload))
	if err != nil {
		t.Fatalf("POST /echo: %v", err)
	}
	defer response.Body.Close()
	actual, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatalf("read /echo response: %v", err)
	}
	if response.StatusCode != http.StatusOK {
		t.Fatalf("POST /echo status = %d, want %d", response.StatusCode, http.StatusOK)
	}
	if !bytes.Equal(actual, payload) {
		t.Fatalf("POST /echo body = %q, want %q", actual, payload)
	}
}

func assertGracefulShutdown(
	t *testing.T,
	baseURL string,
	command *exec.Cmd,
	waited <-chan error,
	processExited *bool,
	processOutput *bytes.Buffer,
) {
	t.Helper()
	connection, err := net.DialTimeout("tcp", strings.TrimPrefix(baseURL, "http://"), 5*time.Second)
	if err != nil {
		t.Fatalf("connect streaming echo request: %v", err)
	}
	defer connection.Close()
	if err := connection.SetDeadline(time.Now().Add(10 * time.Second)); err != nil {
		t.Fatalf("set streaming echo deadline: %v", err)
	}

	first := []byte("before-term|")
	second := []byte("after-term")
	if _, err := fmt.Fprintf(
		connection,
		"POST /echo HTTP/1.1\r\nHost: acceptance\r\nContent-Length: %d\r\nConnection: close\r\n\r\n",
		len(first)+len(second),
	); err != nil {
		t.Fatalf("write streaming echo headers: %v", err)
	}
	if _, err := connection.Write(first); err != nil {
		t.Fatalf("write first streaming echo segment: %v", err)
	}
	response, err := http.ReadResponse(bufio.NewReader(connection), &http.Request{Method: http.MethodPost})
	if err != nil {
		t.Fatalf("read streaming echo response headers: %v", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("streaming POST /echo status = %d, want %d", response.StatusCode, http.StatusOK)
	}
	actualFirst := make([]byte, len(first))
	if _, err := io.ReadFull(response.Body, actualFirst); err != nil {
		t.Fatalf("read first streaming echo segment: %v", err)
	}
	if !bytes.Equal(actualFirst, first) {
		t.Fatalf("first streaming echo segment = %q, want %q", actualFirst, first)
	}

	if err := command.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatalf("send SIGTERM: %v", err)
	}
	select {
	case err := <-waited:
		*processExited = true
		t.Fatalf("echo service exited before its in-flight request completed: %v\n%s", err, processOutput.String())
	case <-time.After(250 * time.Millisecond):
	}

	if _, err := connection.Write(second); err != nil {
		t.Fatalf("write second streaming echo segment after SIGTERM: %v", err)
	}
	actualSecond, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatalf("read streaming echo response after SIGTERM: %v", err)
	}
	if !bytes.Equal(actualSecond, second) {
		t.Fatalf("second streaming echo segment = %q, want %q", actualSecond, second)
	}

	select {
	case err := <-waited:
		*processExited = true
		if err != nil {
			t.Fatalf("echo service shutdown: %v\n%s", err, processOutput.String())
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("echo service did not exit after graceful shutdown\n%s", processOutput.String())
	}
}
