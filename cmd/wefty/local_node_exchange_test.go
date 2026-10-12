package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/internal/httpexchange"
	"github.com/Derek-X-Wang/wefty/runner/ocicontrol"
)

func localExchangeRoot(t *testing.T) string {
	t.Helper()
	root, err := os.MkdirTemp("", "ws")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	return root
}

func localExchangeConfig(t *testing.T, root, socket string) string {
	t.Helper()
	path := filepath.Join(root, "node.json")
	payload, err := json.Marshal(ocicontrol.InstalledConfig{Version: ocicontrol.InstalledConfigVersion, ControlSocket: socket})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, payload, 0600); err != nil {
		t.Fatal(err)
	}
	return path
}

func serveLocalExchange(t *testing.T, socket string, handler http.Handler) {
	t.Helper()
	listener, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewUnstartedServer(handler)
	_ = server.Listener.Close()
	server.Listener = listener
	server.Start()
	t.Cleanup(server.Close)
}

// Each failure travels through a real Unix socket and the command dispatcher,
// then through the same exit/stderr decision consumed by main.
func TestNodeLocalFailedExchange(t *testing.T) {
	for _, scenario := range []string{"no-file", "no-listener", "permission-denied", "headers-timeout", "stalled-body", "truncated-body", "truncated-valid-body", "truncated-refusal", "bare-5xx", "bounded-5xx", "oversized-5xx", "empty-envelope-5xx", "bare-4xx", "malformed-body", "trailing-malformed-body", "cancel-dial", "cancel-headers", "cancel-body", "cancel-refusal-body"} {
		t.Run(scenario, func(t *testing.T) {
			root := localExchangeRoot(t)
			socket := filepath.Join(root, "s")
			permissionDir := filepath.Join(root, "denied")
			if scenario == "permission-denied" {
				if err := os.Mkdir(permissionDir, 0700); err != nil {
					t.Fatal(err)
				}
				socket = filepath.Join(permissionDir, "s")
			}
			config := localExchangeConfig(t, root, socket)
			ctx, cancel := context.WithTimeout(t.Context(), 200*time.Millisecond)
			defer cancel()
			wantPhase := httpexchange.Request
			switch scenario {
			case "no-file", "cancel-dial":
				if scenario == "cancel-dial" {
					cancel()
				}
			case "no-listener":
				listener, err := net.ListenUnix("unix", &net.UnixAddr{Name: socket, Net: "unix"})
				if err != nil {
					t.Fatal(err)
				}
				listener.SetUnlinkOnClose(false)
				if err := listener.Close(); err != nil {
					t.Fatal(err)
				}
			default:
				serveLocalExchange(t, socket, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if scenario == "headers-timeout" || scenario == "cancel-headers" {
						if scenario == "cancel-headers" {
							cancel()
						}
						<-r.Context().Done()
						return
					}
					status := http.StatusOK
					if strings.Contains(scenario, "5xx") || scenario == "truncated-refusal" || scenario == "cancel-refusal-body" {
						status = http.StatusServiceUnavailable
					}
					if scenario == "bare-4xx" {
						status = http.StatusBadRequest
					}
					if strings.HasPrefix(scenario, "truncated-") {
						w.Header().Set("Content-Length", "300")
					}
					w.WriteHeader(status)
					payload := "{"
					switch scenario {
					case "truncated-valid-body":
						payload = `{}`
					case "truncated-refusal":
						payload = `{"error":{"code":"internal","message":"refused","retryable":true}}`
					case "bare-5xx", "bare-4xx":
						payload = "agent\nnot ready\x00\t世界"
					case "bounded-5xx":
						payload = strings.Repeat("x", 4095) + "世界\x00TAIL"
					case "oversized-5xx":
						payload = strings.Repeat("x", (1<<20)+10)
					case "empty-envelope-5xx":
						payload = `{}`
					case "trailing-malformed-body":
						payload = `{}broken`
					}
					_, _ = io.WriteString(w, payload)
					if scenario == "stalled-body" || scenario == "cancel-body" || scenario == "cancel-refusal-body" {
						w.(http.Flusher).Flush()
						if strings.HasPrefix(scenario, "cancel-") {
							// Give the client time to receive headers; assert the recorded body
							// phase below so a cancellation before the body cannot pass this row.
							time.AfterFunc(50*time.Millisecond, cancel)
						}
						<-r.Context().Done()
					}
				}))
				if scenario == "permission-denied" {
					if os.Geteuid() == 0 {
						t.Skip("root bypasses Unix directory permission checks")
					}
					if err := os.Chmod(permissionDir, 0000); err != nil {
						t.Fatal(err)
					}
					t.Cleanup(func() { _ = os.Chmod(permissionDir, 0700) })
				}
			}
			wantExit, wantCode, wantRetry := 13, contract.ErrorUnavailable, true
			if scenario == "permission-denied" || scenario == "bare-4xx" || strings.Contains(scenario, "malformed-body") || strings.HasPrefix(scenario, "cancel-") {
				wantExit, wantCode, wantRetry = 1, contract.ErrorInternal, false
			}
			switch scenario {
			case "stalled-body", "truncated-body", "truncated-valid-body", "truncated-refusal", "cancel-body", "cancel-refusal-body":
				wantPhase = httpexchange.Body
			case "bare-5xx", "bounded-5xx", "oversized-5xx", "empty-envelope-5xx":
				wantPhase = httpexchange.BareStatus
			case "bare-4xx", "malformed-body", "trailing-malformed-body":
				wantPhase = httpexchange.Decode
			}
			var stdout, stderr bytes.Buffer
			err := run(ctx, []string{"--fabric=invalid-must-not-open", "--node-config=" + config, "--json", "node", "doctor"}, &stdout, &stderr)
			assertExchangeReport(t, err, wantExit, wantCode, wantRetry)
			if stdout.Len() != 0 || stderr.Len() != 0 {
				t.Fatalf("command wrote output before reporting: stdout=%s stderr=%s", &stdout, &stderr)
			}
			var failure *httpexchange.Failure
			if !errors.As(err, &failure) || failure.Phase != wantPhase {
				t.Fatalf("phase=%v want=%v err=%v", failure, wantPhase, err)
			}
			if scenario == "no-file" || scenario == "no-listener" {
				errno := syscall.ENOENT
				if scenario == "no-listener" {
					errno = syscall.ECONNREFUSED
				}
				if !errors.Is(err, errno) || !strings.Contains(err.Error(), "check that the node agent is running") {
					t.Fatalf("missing errno or agent guidance: %v", err)
				}
			}
			if scenario == "permission-denied" && !errors.Is(err, syscall.EACCES) {
				t.Fatalf("permission cause lost: %v", err)
			}
			if strings.HasPrefix(scenario, "truncated-") && !errors.Is(err, io.ErrUnexpectedEOF) {
				t.Fatalf("body I/O cause lost: %v", err)
			}
			if strings.HasPrefix(scenario, "cancel-") && !errors.Is(err, context.Canceled) {
				t.Fatalf("cancellation cause lost: %v", err)
			}
			if scenario == "bounded-5xx" || scenario == "oversized-5xx" {
				if strings.Count(err.Error(), "x") > 4096 || strings.Contains(err.Error(), "TAIL") {
					t.Fatalf("unbounded reason: %v", err)
				}
			}
			if scenario == "bare-5xx" && !strings.Contains(err.Error(), "agentnot ready\t世界") {
				t.Fatalf("bare reason lost: %v", err)
			}
		})
	}
}

func TestNodeLocalMissingAgentCommandFamily(t *testing.T) {
	root := localExchangeRoot(t)
	config := localExchangeConfig(t, root, filepath.Join(root, "s"))
	archive := filepath.Join(root, "image.tar")
	if err := os.WriteFile(archive, []byte("archive"), 0600); err != nil {
		t.Fatal(err)
	}
	commands := [][]string{{"doctor"}, {"oci", "start"}, {"oci", "stop"}, {"oci", "removals"}, {"load-image", archive}}
	if runtime.GOOS == "darwin" {
		commands = append(commands, []string{"setup-oci"})
	}
	for _, command := range commands {
		t.Run(strings.Join(command[:1], "-"), func(t *testing.T) {
			args := append([]string{"--node-config=" + config, "--json", "node"}, command...)
			err := run(t.Context(), args, io.Discard, io.Discard)
			assertExchangeReport(t, err, 13, contract.ErrorUnavailable, true)
		})
	}
}

func TestNodeLocalStructuredRefusalsUnchanged(t *testing.T) {
	for _, row := range []struct {
		code  contract.ErrorCode
		retry bool
		exit  int
	}{
		{contract.ErrorInternal, true, 1}, {contract.ErrorInternal, false, 1}, {contract.ErrorUnavailable, true, 13},
		{contract.ErrorInvalidRequest, false, 2}, {contract.ErrorUnauthorized, false, 3}, {contract.ErrorNotFound, false, 4}, {contract.ErrorConflict, false, 5}, {"future_refusal", true, 1},
	} {
		t.Run(string(row.code)+"-"+strconv.Itoa(row.exit), func(t *testing.T) {
			root := localExchangeRoot(t)
			socket := filepath.Join(root, "s")
			config := localExchangeConfig(t, root, socket)
			envelope := contract.ErrorResponse{Error: contract.APIError{Code: row.code, Message: "structured refusal", Retryable: row.retry, RequestID: "node-request", Details: map[string]any{"reason": "unchanged"}}}
			serveLocalExchange(t, socket, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(503)
				_ = json.NewEncoder(w).Encode(envelope)
			}))
			err := run(t.Context(), []string{"--node-config=" + config, "--json", "node", "doctor"}, io.Discard, io.Discard)
			var stderr bytes.Buffer
			writeCommandError(&stderr, err, true)
			var got contract.ErrorResponse
			if decode := json.Unmarshal(stderr.Bytes(), &got); decode != nil {
				t.Fatal(decode)
			}
			if commandExitCode(err) != row.exit || got.Error.Code != row.code || got.Error.Retryable != row.retry || got.Error.Message != envelope.Error.Message || got.Error.RequestID != envelope.Error.RequestID || got.Error.Details["reason"] != "unchanged" {
				t.Fatalf("exit=%d err=%v stderr=%s", commandExitCode(err), err, &stderr)
			}
			var failure *httpexchange.Failure
			if errors.As(err, &failure) {
				t.Fatalf("structured refusal marked as exchange failure: %v", err)
			}
		})
	}
}

func TestNodeLocalAgentNotRunningFromRealBinary(t *testing.T) {
	binary := buildWefty(t)
	for _, scenario := range []string{"no-file", "no-listener"} {
		t.Run(scenario, func(t *testing.T) {
			root := localExchangeRoot(t)
			socket := filepath.Join(root, "s")
			if scenario == "no-listener" {
				listener, err := net.ListenUnix("unix", &net.UnixAddr{Name: socket, Net: "unix"})
				if err != nil {
					t.Fatal(err)
				}
				listener.SetUnlinkOnClose(false)
				if err := listener.Close(); err != nil {
					t.Fatal(err)
				}
			}
			config := localExchangeConfig(t, root, socket)
			exit, output := runWefty(t, binary, 5*time.Second, "--node-config="+config, "--json", "node", "doctor")
			var response contract.ErrorResponse
			if err := json.Unmarshal([]byte(output), &response); err != nil {
				t.Fatalf("output=%s decode=%v", output, err)
			}
			if exit != 13 || response.Error.Code != contract.ErrorUnavailable || !response.Error.Retryable || !strings.Contains(response.Error.Message, "check that the node agent is running") {
				t.Fatalf("exit=%d output=%s", exit, output)
			}
		})
	}
}

func TestNodeLocalErrnoAndCancellationClassification(t *testing.T) {
	for _, errno := range []syscall.Errno{syscall.EACCES, syscall.EPERM, syscall.ENOENT, syscall.ECONNREFUSED} {
		for _, phase := range []httpexchange.Phase{httpexchange.Request, httpexchange.Body, httpexchange.Decode, httpexchange.BareStatus} {
			cause := &net.OpError{Op: "dial", Net: "unix", Err: &os.SyscallError{Syscall: "connect", Err: errno}}
			failure := &httpexchange.Failure{Phase: phase, Cause: cause}
			wantExit, wantCode, wantRetry := 13, contract.ErrorUnavailable, true
			if phase == httpexchange.Decode || (phase == httpexchange.Request && (errno == syscall.EACCES || errno == syscall.EPERM)) {
				wantExit, wantCode, wantRetry = 1, contract.ErrorInternal, false
			}
			assertExchangeReport(t, classifyFailedExchange(t.Context(), phase, failure), wantExit, wantCode, wantRetry)
			ctx, cancel := context.WithCancel(t.Context())
			cancel()
			err := classifyFailedExchange(ctx, phase, failure)
			assertExchangeReport(t, err, 1, contract.ErrorInternal, false)
			if !errors.Is(err, context.Canceled) || !errors.Is(err, errno) {
				t.Fatalf("causes lost: %v", err)
			}
		}
	}
}

// A successful intent read must not hide a transport failure in the following
// start/stop mutation. This exercises the second RPC in both command paths.
func TestNodeLocalMutationFailedExchange(t *testing.T) {
	for _, verb := range []string{"start", "stop"} {
		t.Run(verb, func(t *testing.T) {
			root := localExchangeRoot(t)
			socket := filepath.Join(root, "s")
			config := localExchangeConfig(t, root, socket)
			var mutationSeen atomic.Bool
			serveLocalExchange(t, socket, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/v1/intent" && r.Method == http.MethodGet {
					_, _ = io.WriteString(w, `{"revision":7}`)
					return
				}
				if r.URL.Path != "/v1/oci/"+verb || r.Method != http.MethodPost {
					t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
				}
				mutationSeen.Store(true)
				w.WriteHeader(http.StatusServiceUnavailable)
				_, _ = io.WriteString(w, "agent unavailable during mutation")
			}))
			err := run(t.Context(), []string{"--node-config=" + config, "--json", "node", "oci", verb}, io.Discard, io.Discard)
			assertExchangeReport(t, err, 13, contract.ErrorUnavailable, true)
			if !mutationSeen.Load() {
				t.Fatal("mutation was not called")
			}
		})
	}
}

func TestNodeLocalInvalidConfigurationStaysLocal(t *testing.T) {
	for _, socket := range []string{"relative.sock", "/"} {
		if _, err := ocicontrol.NewClient(socket); err == nil {
			t.Fatalf("accepted socket %q", socket)
		} else {
			assertExchangeReport(t, err, 1, contract.ErrorInternal, false)
		}
		root := localExchangeRoot(t)
		config := localExchangeConfig(t, root, socket)
		err := run(t.Context(), []string{"--node-config=" + config, "--json", "node", "doctor"}, io.Discard, io.Discard)
		assertExchangeReport(t, err, 1, contract.ErrorInternal, false)
	}
}
