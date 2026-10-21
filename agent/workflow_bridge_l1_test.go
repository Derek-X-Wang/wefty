package agent

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httptrace"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	workloadrunner "github.com/Derek-X-Wang/wefty/runner"
)

type bridgeDialFabric struct {
	fabric.Fabric
	dial func(context.Context, string, string) (net.Conn, error)
}

func (f bridgeDialFabric) Dial(ctx context.Context, network, address string) (net.Conn, error) {
	return f.dial(ctx, network, address)
}

func TestWorkflowBridgeL1DeadReusedConnection(t *testing.T) {
	client, upstream := net.Pipe()
	defer client.Close()
	defer upstream.Close()
	if err := upstream.SetDeadline(time.Now().Add(10 * time.Second)); err != nil {
		t.Fatal(err)
	}
	const payload = `{"dispatch_key":"same-request","parent_job_id":"parent"}`
	scriptDone := make(chan error, 1)
	go func() {
		scriptDone <- func() error {
			defer upstream.Close()
			reader := bufio.NewReader(upstream)
			first, err := http.ReadRequest(reader)
			if err != nil {
				return err
			}
			if first.Method != http.MethodGet || first.URL.Path != "/v1/jobs/parent" {
				return fmt.Errorf("first request = %s %s", first.Method, first.URL.Path)
			}
			if err := first.Body.Close(); err != nil {
				return err
			}
			if _, err := io.WriteString(upstream, "HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\n{}"); err != nil {
				return err
			}
			second, err := http.ReadRequest(reader)
			if err != nil {
				return err
			}
			body, err := io.ReadAll(second.Body)
			if err != nil {
				return err
			}
			if err := second.Body.Close(); err != nil {
				return err
			}
			if second.Method != http.MethodPost || second.URL.Path != "/v1/jobs" || string(body) != payload {
				return fmt.Errorf("second request = %s %s %s", second.Method, second.URL.Path, body)
			}
			if second.Header.Get("Idempotency-Key") != "" || second.Header.Get("X-Idempotency-Key") != "" {
				return errors.New("bridge injected an idempotency header")
			}
			// L1 consumed the full POST, then died before sending a response.
			return nil
		}()
	}()
	var dials atomic.Int32
	participant := bridgeDialFabric{dial: func(_ context.Context, network, address string) (net.Conn, error) {
		if network != "tcp" || address != "wefty://control-plane" {
			return nil, fmt.Errorf("unexpected dial %s %s", network, address)
		}
		if dials.Add(1) != 1 {
			return nil, errors.New("bridge retried the POST")
		}
		return client, nil
	}}
	bridge, err := newWorkflowBridgeWithSurface(t.Context(), participant, "", workloadrunner.WorkflowBridgeBinding{}, workflowBridgeSurfaceRun, true,
		withoutRunLedgerSurface(), withControlPlaneSurface(participant, "wefty://control-plane"))
	if err != nil {
		t.Fatal(err)
	}
	defer bridge.close()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	first := httptest.NewRecorder()
	bridge.server.Handler.ServeHTTP(first, httptest.NewRequestWithContext(ctx, http.MethodGet, "/l1/v1/jobs/parent", nil))
	if first.Code != http.StatusOK || first.Body.String() != "{}" {
		t.Fatalf("keep-alive exchange = %d %s", first.Code, first.Body)
	}
	var reused atomic.Bool
	var connections atomic.Int32
	ctx = httptrace.WithClientTrace(ctx, &httptrace.ClientTrace{GotConn: func(info httptrace.GotConnInfo) {
		connections.Add(1)
		reused.Store(info.Reused)
	}})
	second := httptest.NewRecorder()
	request := httptest.NewRequestWithContext(ctx, http.MethodPost, "/l1/v1/jobs", strings.NewReader(payload))
	request.Header.Set("Authorization", "Bearer attempt-token")
	bridge.server.Handler.ServeHTTP(second, request)
	if err := <-scriptDone; err != nil {
		t.Fatalf("scripted L1: %v", err)
	}
	if !reused.Load() || connections.Load() != 1 || dials.Load() != 1 {
		t.Fatalf("upstream reused=%t, connections=%d, dials=%d; want true, 1, 1", reused.Load(), connections.Load(), dials.Load())
	}
	var typed contract.ErrorResponse
	if err := json.Unmarshal(second.Body.Bytes(), &typed); err != nil {
		t.Fatal(err)
	}
	if second.Code != http.StatusServiceUnavailable || typed.Error.Code != contract.ErrorUnavailable || !typed.Error.Retryable {
		t.Fatalf("dead reused L1 response = %d %s; want 503 unavailable, retryable=true", second.Code, second.Body)
	}
	if typed.Error.Details["reason"] != "l1_upstream_unreachable" {
		t.Fatalf("unavailable details = %#v", typed.Error.Details)
	}
}

func TestWorkflowBridgeL1CallerCancellation(t *testing.T) {
	for _, deadline := range []bool{false, true} {
		t.Run(fmt.Sprintf("deadline-%t", deadline), func(t *testing.T) {
			var ctx context.Context
			var cancel context.CancelFunc
			if deadline {
				ctx, cancel = context.WithDeadline(t.Context(), time.Now().Add(-time.Second))
			} else {
				ctx, cancel = context.WithCancel(t.Context())
			}
			defer cancel()
			participant := bridgeDialFabric{dial: func(ctx context.Context, _, _ string) (net.Conn, error) {
				<-ctx.Done()
				return nil, ctx.Err()
			}}
			bridge, err := newWorkflowBridgeWithSurface(t.Context(), participant, "", workloadrunner.WorkflowBridgeBinding{}, workflowBridgeSurfaceRun, true,
				withoutRunLedgerSurface(), withControlPlaneSurface(participant, "wefty://control-plane"))
			if err != nil {
				t.Fatal(err)
			}
			defer bridge.close()
			cancel()
			response := httptest.NewRecorder()
			bridge.server.Handler.ServeHTTP(response, httptest.NewRequestWithContext(ctx, http.MethodPost, "/l1/v1/jobs", strings.NewReader(`{}`)))
			var typed contract.ErrorResponse
			if err := json.Unmarshal(response.Body.Bytes(), &typed); err != nil {
				t.Fatal(err)
			}
			if response.Code != http.StatusBadGateway || typed.Error.Code != contract.ErrorInternal || typed.Error.Retryable {
				t.Fatalf("caller cancellation = %d %s; want unchanged 502 internal, retryable=false", response.Code, response.Body)
			}
		})
	}
}

func TestWorkflowBridgeL1UpstreamAnswersRemainUnchanged(t *testing.T) {
	const refusal = `{"error":{"code":"instance_key_conflict","message":"","retryable":false}}`
	for _, test := range []struct {
		name     string
		response string
		status   int
		code     contract.ErrorCode
	}{
		{"typed-refusal", fmt.Sprintf("HTTP/1.1 409 Conflict\r\nContent-Length: %d\r\n\r\n%s", len(refusal), refusal), http.StatusConflict, contract.ErrorInstanceKeyConflict},
		{"invalid-upgrade", "HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: unexpected\r\n\r\n", http.StatusBadGateway, contract.ErrorInternal},
	} {
		t.Run(test.name, func(t *testing.T) {
			client, upstream := net.Pipe()
			defer client.Close()
			defer upstream.Close()
			if err := upstream.SetDeadline(time.Now().Add(10 * time.Second)); err != nil {
				t.Fatal(err)
			}
			scriptDone := make(chan error, 1)
			go func() {
				defer upstream.Close()
				request, err := http.ReadRequest(bufio.NewReader(upstream))
				if err == nil {
					_, err = io.Copy(io.Discard, request.Body)
					_ = request.Body.Close()
				}
				if err == nil {
					_, err = io.WriteString(upstream, test.response)
				}
				scriptDone <- err
			}()
			participant := bridgeDialFabric{dial: func(context.Context, string, string) (net.Conn, error) {
				return client, nil
			}}
			bridge, err := newWorkflowBridgeWithSurface(t.Context(), participant, "", workloadrunner.WorkflowBridgeBinding{}, workflowBridgeSurfaceRun, true,
				withoutRunLedgerSurface(), withControlPlaneSurface(participant, "wefty://control-plane"))
			if err != nil {
				t.Fatal(err)
			}
			defer bridge.close()
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			response := httptest.NewRecorder()
			bridge.server.Handler.ServeHTTP(response, httptest.NewRequestWithContext(ctx, http.MethodPost, "/l1/v1/jobs", strings.NewReader(`{}`)))
			if err := <-scriptDone; err != nil {
				t.Fatal(err)
			}
			var typed contract.ErrorResponse
			if err := json.Unmarshal(response.Body.Bytes(), &typed); err != nil {
				t.Fatal(err)
			}
			if response.Code != test.status || typed.Error.Code != test.code || typed.Error.Retryable || len(typed.Error.Details) != 0 {
				t.Fatalf("upstream answer = %d %s; want unchanged %d %s", response.Code, response.Body, test.status, test.code)
			}
		})
	}
}
