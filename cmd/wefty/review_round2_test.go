package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/coder/websocket"
)

func TestReviewCancellationIsNotUnavailable(t *testing.T) {
	for _, operation := range []string{"takeover-open", "takeover-perform", "registry-head", "registry-get"} {
		t.Run(operation, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			cancel()
			var err error
			switch operation {
			case "takeover-open":
				_, err = openTakeoverViewWithPolicyRetry(ctx, directDialFabric{}, "ws://127.0.0.1:1"+contract.ComputerDisplayWebSocketPath, 1)
			case "takeover-perform":
				clients := &apiClients{fabric: directDialFabric{}}
				_, err = clients.performComputerTakeoverAction(ctx, "ws://127.0.0.1:1"+contract.ComputerDisplayWebSocketPath, "test-token", "take")
			default:
				resolver := newRegistryResolver(&http.Client{Transport: reviewTransport{context.Canceled}})
				if operation == "registry-head" {
					_, err = resolver.headManifest(ctx, "https://registry.example.test/v2/test/manifests/latest", "")
				} else {
					_, err = resolver.publicBearerToken(ctx, `Bearer realm="https://registry.example.test/token"`)
				}
			}
			var unavailable *unavailableError
			if !errors.Is(err, context.Canceled) || errors.As(err, &unavailable) || commandExitCode(err) != 1 {
				t.Fatalf("cancellation misclassified: exit=%d err=%v", commandExitCode(err), err)
			}
			var stderr bytes.Buffer
			writeCommandError(&stderr, err, true)
			var envelope contract.ErrorResponse
			if decode := json.Unmarshal(stderr.Bytes(), &envelope); decode != nil || envelope.Error.Code != contract.ErrorInternal || envelope.Error.Retryable {
				t.Fatalf("cancellation envelope=%s decode=%v", stderr.String(), decode)
			}
		})
	}
}

func TestReviewTakeoverSilentAfterUpgradeIsUnavailable(t *testing.T) {
	var upgraded atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set(contract.ComputerControlTokenHeader, "test-token")
		conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{Subprotocols: []string{contract.ComputerDisplayWebSocketSubprotocol}})
		if err != nil {
			return
		}
		defer conn.CloseNow()
		upgraded.Store(true)
		// Hold the upgraded connection open without sending the display banner.
		_, _, _ = conn.Read(r.Context())
	}))
	defer server.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	endpoint := "ws" + strings.TrimPrefix(server.URL, "http") + contract.ComputerDisplayWebSocketPath
	_, err := openTakeoverViewWithPolicyRetry(ctx, directDialFabric{}, endpoint, 1)
	if !upgraded.Load() || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected banner deadline after upgrade: upgraded=%t err=%v", upgraded.Load(), err)
	}
	var stderr bytes.Buffer
	writeCommandError(&stderr, err, true)
	var envelope contract.ErrorResponse
	if decode := json.Unmarshal(stderr.Bytes(), &envelope); decode != nil || commandExitCode(err) != 13 || envelope.Error.Code != "unavailable" || !envelope.Error.Retryable {
		t.Fatalf("exit=%d error=%s decode=%v", commandExitCode(err), stderr.String(), decode)
	}
}
