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
	for _, scenario := range []string{"no-file", "no-listener", "overlong-path", "not-directory", "peer-uid-refusal", "permission-denied", "headers-timeout", "stalled-body", "truncated-body", "truncated-valid-body", "truncated-refusal", "bare-5xx", "bounded-5xx", "oversized-5xx", "empty-envelope-5xx", "bare-4xx", "malformed-body", "trailing-malformed-body", "cancel-dial", "cancel-headers", "cancel-body", "cancel-refusal-body"} {
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
			if scenario == "overlong-path" {
				socket = filepath.Join(root, strings.Repeat("s", 128))
			}
			if scenario == "not-directory" {
				parent := filepath.Join(root, "regular-file")
				if err := os.WriteFile(parent, []byte("not a directory"), 0600); err != nil {
					t.Fatal(err)
				}
				socket = filepath.Join(parent, "s")
			}
			config := localExchangeConfig(t, root, socket)
			// Only the rows whose server waits for the client deadline need a short
			// one. Every other row ends on its own failure or its own cancel, so a
			// starved runner must not let the deadline pre-empt the asserted phase (#1412).
			timeout := 10 * time.Second
			if scenario == "headers-timeout" || scenario == "stalled-body" {
				timeout = 2 * time.Second
			}
			ctx, cancel := context.WithTimeout(t.Context(), timeout)
			defer cancel()
			wantPhase := httpexchange.Request
			switch scenario {
			case "no-file", "overlong-path", "not-directory", "cancel-dial":
				if scenario == "cancel-dial" {
					cancel()
				}
			case "peer-uid-refusal":
				server, err := ocicontrol.NewServer(socket, ocicontrol.ServiceFuncs{}, uint32(os.Geteuid())+1)
				if err != nil {
					t.Fatal(err)
				}
				serverCtx, stop := context.WithCancel(t.Context())
				done := make(chan error, 1)
				go func() { done <- server.Serve(serverCtx) }()
				t.Cleanup(func() {
					stop()
					if err := <-done; err != nil {
						t.Errorf("stop control server: %v", err)
					}
				})
				deadline := time.Now().Add(3 * time.Second)
				for {
					if info, err := os.Stat(socket); err == nil && info.Mode()&os.ModeSocket != 0 {
						break
					}
					if time.Now().After(deadline) {
						t.Fatal("control server did not become ready")
					}
					time.Sleep(10 * time.Millisecond)
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
			if scenario == "overlong-path" || scenario == "not-directory" || scenario == "peer-uid-refusal" || scenario == "headers-timeout" || scenario == "permission-denied" || scenario == "bare-4xx" || strings.Contains(scenario, "malformed-body") || strings.HasPrefix(scenario, "cancel-") {
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
			if scenario != "no-file" && scenario != "no-listener" && strings.Contains(err.Error(), "node agent") {
				t.Fatalf("unexpected missing-agent hint: %v", err)
			}
			if scenario == "overlong-path" && !errors.Is(err, syscall.EINVAL) {
				t.Fatalf("overlong-path cause lost: %v", err)
			}
			if scenario == "not-directory" && !errors.Is(err, syscall.ENOTDIR) {
				t.Fatalf("not-directory cause lost: %v", err)
			}
			// The agent closes a refused peer's connection; the client sees
			// that as EOF, a reset, (when the close beats the request
			// write) a broken pipe or, on macOS, a socket that is no longer
			// connected, or net/http's unexported idle-close error when the
			// close lands before the transport reads.
			if scenario == "peer-uid-refusal" && !errors.Is(err, io.EOF) && !errors.Is(err, syscall.ECONNRESET) && !errors.Is(err, syscall.EPIPE) && !errors.Is(err, syscall.ENOTCONN) &&
				!strings.Contains(err.Error(), "http: server closed idle connection") {
				t.Fatalf("peer refusal did not close the connection: %v", err)
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
	commands := [][]string{{"doctor"}, {"oci", "always-on"}, {"oci", "on-demand"}, {"oci", "off"}, {"oci", "removals"}, {"load-image", archive}}
	if runtime.GOOS == "darwin" {
		commands = append(commands, []string{"setup-oci"})
	}
	for _, command := range commands {
		name := strings.Join(command, "-")
		if command[0] == "load-image" {
			name = "load-image"
		}
		t.Run(name, func(t *testing.T) {
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
		{contract.ErrorInvalidRequest, false, 2}, {contract.ErrorUnauthorized, false, 3}, {contract.ErrorForbidden, false, 3}, {contract.ErrorNotFound, false, 4}, {contract.ErrorConflict, false, 5}, {"future_refusal", true, 1},
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
	for _, errno := range []syscall.Errno{syscall.EACCES, syscall.EPERM, syscall.EINVAL, syscall.ENOTDIR, syscall.ETIMEDOUT, syscall.ENOENT, syscall.ECONNREFUSED} {
		for _, phase := range []httpexchange.Phase{httpexchange.Request, httpexchange.Body, httpexchange.Decode, httpexchange.BareStatus} {
			t.Run(errno.Error()+"/phase-"+strconv.Itoa(int(phase)), func(t *testing.T) {
				cause := &net.OpError{Op: "dial", Net: "unix", Err: &os.SyscallError{Syscall: "connect", Err: errno}}
				failure := &httpexchange.Failure{Phase: phase, Cause: cause}
				wantExit, wantCode, wantRetry := 13, contract.ErrorUnavailable, true
				if phase == httpexchange.Decode || (phase == httpexchange.Request && (errno != syscall.ENOENT && errno != syscall.ECONNREFUSED)) {
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
			})
		}
	}
}

// Request errors need the adapter's network even when there is no dial errno.
func TestNodeLocalRequestFailureClassification(t *testing.T) {
	for _, row := range []struct {
		name  string
		cause error
	}{
		{"eof", io.EOF},
		{"connection-reset", &net.OpError{Op: "read", Net: "unix", Err: syscall.ECONNRESET}},
		{"dial-timeout", &net.OpError{Op: "dial", Net: "unix", Err: context.DeadlineExceeded}},
		{"request-deadline", context.DeadlineExceeded},
	} {
		t.Run(row.name, func(t *testing.T) {
			failure := &httpexchange.Failure{Phase: httpexchange.Request, Network: "unix", Cause: row.cause}
			err := classifyRecordedExchangeError(t.Context(), failure)
			assertExchangeReport(t, err, 1, contract.ErrorInternal, false)
			if !errors.Is(err, row.cause) || strings.Contains(err.Error(), "node agent") {
				t.Fatalf("cause lost or unexpected missing-agent hint: %v", err)
			}
		})
	}
}

// A successful intent read must not hide a transport failure in the following
// position mutation. This exercises the second RPC in all three command paths.
func TestNodeLocalMutationFailedExchange(t *testing.T) {
	for _, verb := range []string{"always-on", "on-demand", "off"} {
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
				route := "start"
				if verb == "off" {
					route = "stop"
				}
				if r.URL.Path != "/v1/oci/"+route || r.Method != http.MethodPost {
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

func TestNodeLocalRemovedIntentVerbsNameReplacement(t *testing.T) {
	for _, verb := range []string{"start", "stop"} {
		err := executeLocalOCIIntent(t.Context(), nil, false, []string{verb}, io.Discard)
		if commandExitCode(err) != 2 {
			t.Fatalf("removed verb %q exit=%d", verb, commandExitCode(err))
		}
		for _, word := range []string{"off", "on-demand", "always-on"} {
			if !strings.Contains(err.Error(), word) {
				t.Fatalf("usage lacks %s: %v", word, err)
			}
		}
	}
}

func TestNodeLocalProcessCallerRefusalFromRealBinary(t *testing.T) {
	binary := buildWefty(t)
	root := localExchangeRoot(t)
	socket := filepath.Join(root, "s")
	config := localExchangeConfig(t, root, socket)
	server, err := ocicontrol.NewNodeServer(socket, ocicontrol.ServiceFuncs{})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- server.Serve(ctx) }()
	t.Cleanup(func() {
		cancel()
		if err := <-done; err != nil {
			t.Error(err)
		}
	})
	deadline := time.Now().Add(5 * time.Second)
	for {
		if info, err := os.Stat(socket); err == nil && info.Mode()&os.ModeSocket != 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("control fixture did not start")
		}
		time.Sleep(10 * time.Millisecond)
	}
	code, output := runWefty(t, binary, 30*time.Second, "--json", "--node-config="+config, "node", "oci", "always-on")
	var refusal contract.ErrorResponse
	if err := json.Unmarshal([]byte(output), &refusal); err != nil || code != exitUnauthorized || refusal.Error.Code != contract.ErrorForbidden || !strings.Contains(refusal.Error.Message, "owner's own shell") {
		t.Fatalf("caller refusal exit=%d output=%s decode=%v", code, output, err)
	}
}
