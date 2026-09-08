package l3

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
)

type clientTestFabric struct {
	fabric.Fabric
	dial func(context.Context, string, string) (net.Conn, error)
}

func (f clientTestFabric) Dial(ctx context.Context, n, a string) (net.Conn, error) {
	return f.dial(ctx, n, a)
}

func TestL1ClientErrorBoundary(t *testing.T) {
	tests := []struct {
		name           string
		status         int
		body           string
		missing, retry bool
	}{
		{"authority", 404, `{"error":{"code":"not_found","message":"absent","retryable":false,"details":{"safe":"detail"},"request_id":"req"}}`, true, false},
		{"oversized malformed", 404, `{"error":{"code":"not_found","message":"absent","retryable":false}}` + strings.Repeat(" ", 2<<20) + "invalid suffix", false, false},
		{"html", 404, `<html>not found</html>`, false, false},
		{"empty", 404, ``, false, false},
		{"invalid", 404, `{"error":`, false, false},
		{"partial", 404, `{"error":{"code":"not_found"}}`, false, false},
		{"null message", 404, `{"error":{"code":"not_found","message":null,"retryable":false}}`, false, false},
		{"attempt", 404, `{"error":{"code":"attempt_not_found","message":"absent","retryable":false}}`, false, false},
		{"wrong status", 503, `{"error":{"code":"not_found","message":"absent","retryable":false}}`, false, false},
		{"auth", 401, `{"error":{"code":"unauthorized","message":"auth","retryable":false}}`, false, false},
		{"malformed 429", 429, `overloaded`, false, true},
		{"malformed 503", 503, `down`, false, true},
		{"reserved 501", 501, `{"error":{"code":"not_implemented","message":"reserved","retryable":false}}`, false, false},
		{"capacity 409", 409, `{"error":{"code":"capacity_exhausted","message":"full","retryable":true}}`, false, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &L1Client{operationTimeout: time.Second, client: &http.Client{Transport: recoveryRoundTripper(func(r *http.Request) (*http.Response, error) {
				return &http.Response{StatusCode: tt.status, Body: io.NopCloser(strings.NewReader(tt.body)), Header: make(http.Header)}, nil
			})}}
			_, err := c.GetJob(context.Background(), "job-1")
			if err == nil {
				t.Fatal("expected error")
			}
			err = fmt.Errorf("wrapped: %w", err)
			if isMissingL1Job(err, "job-1") != tt.missing || isMissingL1Job(err, "other") {
				t.Fatalf("wrong absence boundary: %v", err)
			}
			var protocol *Error
			if !errors.As(err, &protocol) || protocol.Retryable != tt.retry {
				t.Fatalf("wrong retryability: %#v", protocol)
			}
			if tt.missing {
				if protocol.RequestID != "req" || protocol.Details["safe"] != "detail" {
					t.Fatalf("lost envelope: %#v", protocol)
				}
				var remote *l1ResponseError
				if !errors.As(err, &remote) || remote.status != 404 || remote.method != "GET" || remote.path != "/v1/jobs/job-1" {
					t.Fatalf("lost origin: %#v", remote)
				}
				_, err = c.SubmitJob(context.Background(), contract.JobSpec{})
				if isMissingL1Job(err, "job-1") {
					t.Fatal("submit absence marked as GetJob")
				}
				_, err = c.GetJobLogs(context.Background(), "job-1", "", 1)
				if isMissingL1Job(err, "job-1") {
					t.Fatal("logs absence marked as GetJob")
				}
			}
		})
	}
}

func TestL1ClientBoundedPhases(t *testing.T) {
	for _, phase := range []string{"dial", "headers", "body", "error body", "caller deadline", "caller cancel"} {
		t.Run(phase, func(t *testing.T) {
			entered, cleaned := make(chan struct{}), make(chan struct{})
			var calls atomic.Int32
			f := clientTestFabric{dial: func(ctx context.Context, n, a string) (net.Conn, error) {
				calls.Add(1)
				if n != "tcp" || a != "logical-l1:8123" {
					t.Errorf("Fabric route=%s %s", n, a)
				}
				if phase == "dial" || phase == "caller deadline" || phase == "caller cancel" {
					close(entered)
					<-ctx.Done()
					close(cleaned)
					return nil, ctx.Err()
				}
				client, server := net.Pipe()
				go func() {
					defer close(cleaned)
					defer server.Close()
					reader := bufio.NewReader(server)
					request, err := http.ReadRequest(reader)
					if err != nil {
						t.Error(err)
						return
					}
					request.Body.Close()
					close(entered)
					if phase == "body" || phase == "error body" {
						status := "200 OK"
						if phase == "error body" {
							status = "503 Unavailable"
						}
						if _, err = fmt.Fprintf(server, "HTTP/1.1 %s\r\nContent-Length: 100\r\n\r\n{", status); err != nil {
							return
						}
					}
					// Transport cancellation must close the active connection.
					_, _ = io.Copy(io.Discard, reader)
				}()
				return client, nil
			}}
			operation, dial, header := 200*time.Millisecond, 200*time.Millisecond, 200*time.Millisecond
			switch phase {
			case "dial":
				dial = 40 * time.Millisecond
			case "headers":
				header = 40 * time.Millisecond
			case "body", "error body":
				operation = 40 * time.Millisecond
			}
			c, err := newL1Client(f, "logical-l1:8123", operation, dial, header)
			if err != nil {
				t.Fatal(err)
			}
			defer c.CloseIdleConnections()
			parent, cancel := context.WithCancel(context.Background())
			defer cancel()
			ctx := parent
			if phase == "caller deadline" {
				var end context.CancelFunc
				ctx, end = context.WithTimeout(parent, 30*time.Millisecond)
				defer end()
			}
			done := make(chan error, 1)
			go func() { _, err := c.GetJob(ctx, "job-1"); done <- err }()
			select {
			case <-entered:
			case <-time.After(time.Second):
				t.Fatal("phase not entered")
			}
			if phase == "caller cancel" {
				cancel()
			}
			select {
			case err := <-done:
				if err == nil || !retryableDispatchError(err) {
					t.Fatalf("expected retryable failure: %v", err)
				}
				if phase == "caller cancel" && !errors.Is(err, context.Canceled) {
					t.Fatalf("cancel lost: %v", err)
				}
			case <-time.After(150 * time.Millisecond):
				t.Fatal("phase deadline did not terminate request before the longer operation bound")
			}
			select {
			case <-cleaned:
			case <-time.After(100 * time.Millisecond):
				t.Fatal("dial/connection outlived request")
			}
			if calls.Load() != 1 {
				t.Fatalf("retried %d calls", calls.Load())
			}
			if phase != "caller cancel" && parent.Err() != nil {
				t.Fatal("canceled parent")
			}
		})
	}
}

type closeTrackedBody struct {
	io.Reader
	closed bool
}

func (b *closeTrackedBody) Close() error { b.closed = true; return nil }
func TestL1ClientClosesResponseBodies(t *testing.T) {
	for _, status := range []int{200, 404, 503} {
		body := &closeTrackedBody{Reader: strings.NewReader(`{}`)}
		c := &L1Client{operationTimeout: time.Second, client: &http.Client{Transport: recoveryRoundTripper(func(*http.Request) (*http.Response, error) {
			return &http.Response{StatusCode: status, Body: body}, nil
		})}}
		_, _ = c.GetJob(context.Background(), "job")
		if !body.closed {
			t.Fatalf("body not closed for %d", status)
		}
	}
}

func TestL1ClientRedirectDoesNotInventJobAbsence(t *testing.T) {
	f := clientTestFabric{dial: func(context.Context, string, string) (net.Conn, error) { t.Fatal("unexpected dial"); return nil, nil }}
	c, err := NewL1Client(f, "authority")
	if err != nil {
		t.Fatal(err)
	}
	calls := 0
	c.client.Transport = recoveryRoundTripper(func(r *http.Request) (*http.Response, error) {
		calls++
		if r.URL.Path == "/v1/jobs/job" {
			return &http.Response{StatusCode: 302, Header: http.Header{"Location": []string{"/unknown"}}, Body: io.NopCloser(strings.NewReader("")), Request: r}, nil
		}
		return &http.Response{StatusCode: 404, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(`{"error":{"code":"not_found","message":"missing route","retryable":false}}`)), Request: r}, nil
	})
	_, err = c.GetJob(context.Background(), "job")
	if err == nil || isMissingL1Job(err, "job") {
		t.Fatalf("redirect invented absence: %v", err)
	}
	var remote *l1ResponseError
	if !errors.As(err, &remote) || remote.path != "/unknown" || calls != 2 {
		t.Fatalf("lost redirect origin: %#v calls=%d", remote, calls)
	}
}
