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
	"net/url"
	"strings"
	"testing"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/coder/websocket"
)

// Exercise each production adapter over real HTTP, then its process reporting
// boundary. No expected exit or JSON field comes from the classifier.
func TestHTTPFailedExchangeAdapters(t *testing.T) {
	for _, adapter := range []string{"L1", "L3", "take", "release", "view", "registry-token", "registry-head"} {
		for _, failure := range []string{"dial-failure", "header-timeout", "stalled-body", "truncated-body", "truncated-valid-body", "bare-5xx", "bounded-5xx", "malformed-body", "cancel-dial", "cancel-headers", "cancel-body"} {
			// A HEAD has no response body on the wire. Body coverage belongs to
			// the registry token GET; HEAD's malformed case is an invalid digest.
			if adapter == "registry-head" && (failure == "stalled-body" || failure == "truncated-body" || failure == "truncated-valid-body" || failure == "cancel-body") {
				continue
			}
			t.Run(adapter+"/"+failure, func(t *testing.T) {
				budget := 150 * time.Millisecond
				if strings.HasPrefix(failure, "cancel-") {
					budget = time.Second
				}
				ctx, cancel := context.WithTimeout(t.Context(), budget)
				defer cancel()
				arrived := make(chan struct{}, 1)
				notify := func() {
					select {
					case arrived <- struct{}{}:
					default:
					}
				}
				handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if failure == "header-timeout" || failure == "cancel-headers" {
						notify()
						<-r.Context().Done()
						return
					}
					status := http.StatusOK
					if (failure == "bare-5xx" || failure == "bounded-5xx") || (adapter == "view" && failure != "malformed-body") {
						status = http.StatusServiceUnavailable
					}
					if adapter == "view" && failure == "malformed-body" {
						status = http.StatusBadRequest
					}
					if failure == "truncated-body" || failure == "truncated-valid-body" {
						w.Header().Set("Content-Length", "200")
					}
					w.WriteHeader(status)
					if failure == "bounded-5xx" {
						_, _ = w.Write([]byte(strings.Repeat("x", 4095) + "世界\x00TAIL"))
						return
					}
					if failure == "truncated-valid-body" {
						payload := `{"token":"public-token"}`
						if adapter == "view" {
							payload = `{"error":{"code":"tenure_unavailable","message":"partial refusal","retryable":true}}`
						}
						_, _ = w.Write([]byte(payload))
						return
					}
					if failure == "bare-5xx" {
						_, _ = w.Write([]byte("registry or display\nnot ready\x00\t世界"))
						return
					}
					_, _ = w.Write([]byte("{"))
					if failure == "stalled-body" || failure == "cancel-body" {
						w.(http.Flusher).Flush()
						notify()
						<-r.Context().Done()
					}
				})
				var server *httptest.Server
				if strings.HasPrefix(adapter, "registry-") {
					server = httptest.NewTLSServer(handler)
				} else {
					server = httptest.NewServer(handler)
				}
				defer server.Close()
				address, _ := url.Parse(server.URL)
				dialRelease := make(chan struct{})
				defer close(dialRelease)
				dial := func(ctx context.Context, network, target string) (net.Conn, error) {
					if failure == "cancel-dial" {
						notify()
						select {
						case <-ctx.Done():
							return nil, ctx.Err()
						case <-dialRelease:
							return nil, context.Canceled
						}
					}
					return (&net.Dialer{}).DialContext(ctx, network, target)
				}
				if strings.HasPrefix(failure, "cancel-") {
					go func() {
						select {
						case <-arrived:
							// Let flushed headers reach the body reader before
							// cancellation. The server remains blocked throughout.
							if failure == "cancel-body" {
								time.Sleep(20 * time.Millisecond)
							}
							cancel()
						case <-ctx.Done():
						}
					}()
				}
				if failure == "dial-failure" {
					server.Close()
				}
				var err error
				participant := exchangeTestFabric{dial: dial}
				switch adapter {
				case "L1", "L3":
					client := newAPIClient(adapter, strings.ToLower(adapter), participant, address.Host)
					defer client.client.CloseIdleConnections()
					var body map[string]any
					_, err = client.doWithResponse(ctx, http.MethodGet, "/test", nil, nil, &body, http.StatusOK)
				case "take", "release":
					clients := &apiClients{fabric: participant}
					_, err = clients.performComputerTakeoverAction(ctx, "ws://"+address.Host+contract.ComputerDisplayWebSocketPath, "test-token", adapter)
				case "view":
					_, err = openTakeoverViewWithPolicyRetry(ctx, participant, "ws://"+address.Host+contract.ComputerDisplayWebSocketPath, 1)
				default:
					client := server.Client()
					transport := client.Transport.(*http.Transport).Clone()
					transport.DialContext = dial
					client.Transport = transport
					defer client.CloseIdleConnections()
					resolver := newRegistryResolver(client)
					if adapter == "registry-head" {
						_, err = resolver.ResolveDigest(ctx, address.Host+"/example/service:latest")
					} else {
						_, err = resolver.publicBearerToken(ctx, `Bearer realm="`+server.URL+`/token"`)
					}
				}
				wantExit, wantCode, wantRetry := 13, contract.ErrorUnavailable, true
				if failure == "malformed-body" || strings.HasPrefix(failure, "cancel-") {
					wantExit, wantCode, wantRetry = 1, contract.ErrorInternal, false
				}
				assertExchangeReport(t, err, wantExit, wantCode, wantRetry)
				if strings.HasPrefix(failure, "cancel-") && !errors.Is(err, context.Canceled) {
					t.Fatalf("cancellation cause lost: %v", err)
				}

				if failure == "cancel-body" && !strings.Contains(err.Error(), "read ") {
					t.Fatalf("did not cancel a body read: %v", err)
				}
				if (failure == "truncated-body" || failure == "truncated-valid-body") && !errors.Is(err, io.ErrUnexpectedEOF) {
					t.Fatalf("body I/O cause lost: %v", err)
				}
				if (failure == "header-timeout" || failure == "stalled-body") && !errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("transport deadline cause lost: %v", err)
				}
				if failure == "bounded-5xx" {
					message := err.Error()
					if !utf8.ValidString(message) || strings.Contains(message, "TAIL") || strings.Count(message, "x") > 4096 {
						t.Fatalf("unbounded reason: %q", message)
					}
					for _, r := range message {
						if !unicode.IsPrint(r) && r != '\t' {
							t.Fatalf("non-printable reason: %q", message)
						}
					}
					if adapter != "registry-head" && !strings.Contains(message, strings.Repeat("x", 100)) {
						t.Fatalf("reason lost: %q", message)
					}
				}
				if failure == "bare-5xx" && adapter != "registry-head" && !strings.Contains(err.Error(), "registry or displaynot ready\t世界") {
					t.Fatalf("sanitized reason lost: %v", err)
				}
			})
		}
	}
}

type exchangeTestFabric struct {
	directDialFabric
	dial func(context.Context, string, string) (net.Conn, error)
}

func (f exchangeTestFabric) Dial(ctx context.Context, network, address string) (net.Conn, error) {
	return f.dial(ctx, network, address)
}

func assertExchangeReport(t *testing.T, err error, exit int, code contract.ErrorCode, retryable bool) {
	t.Helper()
	if err == nil {
		t.Fatal("exchange unexpectedly succeeded")
	}
	var stderr bytes.Buffer
	writeCommandError(&stderr, err, true)
	var response contract.ErrorResponse
	if decode := json.Unmarshal(stderr.Bytes(), &response); decode != nil {
		t.Fatalf("stderr=%s decode=%v", stderr.String(), decode)
	}
	if commandExitCode(err) != exit || response.Error.Code != code || response.Error.Retryable != retryable || response.Error.Message != err.Error() || response.Error.RequestID != "" || len(response.Error.Details) != 0 {
		t.Fatalf("exit=%d err=%v stderr=%s; want exit=%d code=%s retryable=%t", commandExitCode(err), err, stderr.String(), exit, code, retryable)
	}
}

func TestHTTPFailedExchangeNormalViewEnd(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set(contract.ComputerControlTokenHeader, "session-token")
		conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{Subprotocols: []string{contract.ComputerDisplayWebSocketSubprotocol}})
		if err != nil {
			return
		}
		defer conn.CloseNow()
		_ = conn.Write(r.Context(), websocket.MessageBinary, []byte("RFB 003.008\n"))
		_ = conn.Close(websocket.StatusNormalClosure, "done")
	}))
	defer server.Close()
	session, err := openTakeoverViewWithPolicyRetry(t.Context(), directDialFabric{}, "ws"+strings.TrimPrefix(server.URL, "http")+contract.ComputerDisplayWebSocketPath, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	assertExchangeReport(t, session.Wait(t.Context()), 1, contract.ErrorInternal, false)
}
