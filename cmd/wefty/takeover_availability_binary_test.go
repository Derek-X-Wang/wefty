package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/l1"
)

func TestTakeoverFrontDoorNonEnvelope5xxFromRealBinary(t *testing.T) {
	binary := buildWefty(t)
	for _, failure := range []struct {
		status int
		body   string
	}{
		{503, "Computer display is not ready"},
		{503, "Fabric identity could not be verified"},
		{500, "front door failed"},
		{502, `{"error":{}}`},
		{599, "front door unavailable"},
	} {
		for _, action := range []string{"take", "release", "view"} {
			t.Run(fmt.Sprintf("%s/%d/%s", action, failure.status, failure.body), func(t *testing.T) {
				var unauthorized atomic.Bool
				frontDoor := startStubLedger(t, func(w http.ResponseWriter, r *http.Request) {
					wantPath := contract.ComputerDisplayWebSocketPath
					wantMethod := http.MethodGet
					if action != "view" {
						wantMethod = http.MethodPost
						wantPath = contract.ComputerControlTakePath
						if action == "release" {
							wantPath = contract.ComputerControlReleasePath
						}
					}
					if r.URL.Path != wantPath || r.Method != wantMethod {
						t.Errorf("front door request = %s %s, want %s %s", r.Method, r.URL.Path, wantMethod, wantPath)
					}
					if unauthorized.Load() {
						http.Error(w, "identity absent", http.StatusUnauthorized)
						return
					}
					w.WriteHeader(failure.status)
					_, _ = w.Write([]byte(failure.body))
				})
				endpoint := "ws://" + frontDoor + contract.ComputerDisplayWebSocketPath
				address := startStubLedger(t, func(w http.ResponseWriter, r *http.Request) {
					switch r.URL.Path {
					case "/v1/computers/computer-1":
						_ = json.NewEncoder(w).Encode(l1.Computer{ComputerID: "computer-1", Name: "alice"})
					case "/v1/computers/computer-1/takeover":
						_ = json.NewEncoder(w).Encode(l1.ComputerTakeoverAvailability{
							ComputerID: "computer-1", FriendlyName: "alice", DisplayEndpoint: &endpoint, PolicyRevision: 1,
						})
					default:
						t.Errorf("unexpected L1 request path %q", r.URL.Path)
						w.WriteHeader(http.StatusNotFound)
					}
				})
				capabilityFile := filepath.Join(t.TempDir(), "session.json")
				if action != "view" {
					if err := writeTakeoverSessionCapability(capabilityFile, takeoverSessionCapability{Endpoint: endpoint, Token: "session-token"}); err != nil {
						t.Fatal(err)
					}
				}
				args := []string{"--json", "--fabric=plain", "--plain-user-id=operator", "--plain-device-id=test-device", "--l1", address,
					"services", "takeover", action, "computer-1", "--session-token-file", capabilityFile}
				code, stdout, stderr := runPolishBinary(t, binary, args...)
				var envelope contract.ErrorResponse
				if err := json.Unmarshal(stderr, &envelope); err != nil || code != 13 || envelope.Error.Code != contract.ErrorUnavailable ||
					!envelope.Error.Retryable || len(stdout) != 0 || !strings.Contains(envelope.Error.Message, failure.body) {
					t.Errorf("exit=%d stdout=%s stderr=%s decode=%v, want unavailable/retryable/exit 13", code, stdout, stderr, err)
				}

				// Plain-text authentication refusals retain their distinct exit.
				unauthorized.Store(true)
				code, stdout, stderr = runPolishBinary(t, binary, args...)
				if err := json.Unmarshal(stderr, &envelope); err != nil || code != 3 || envelope.Error.Code != contract.ErrorUnauthorized ||
					envelope.Error.Retryable || len(stdout) != 0 {
					t.Fatalf("401 exit=%d stdout=%s stderr=%s decode=%v", code, stdout, stderr, err)
				}

				// Cancellation of the same exchange remains a local failure.
				ctx, cancel := context.WithCancel(t.Context())
				cancel()
				var err error
				if action == "view" {
					_, err = openTakeoverViewWithPolicyRetry(ctx, directDialFabric{}, endpoint, 1)
				} else {
					clients := &apiClients{fabric: directDialFabric{}}
					_, err = clients.performComputerTakeoverAction(ctx, endpoint, "session-token", action)
				}
				var canceledOutput bytes.Buffer
				writeCommandError(&canceledOutput, err, true)
				if decode := json.Unmarshal(canceledOutput.Bytes(), &envelope); decode != nil || !errors.Is(err, context.Canceled) ||
					commandExitCode(err) != 1 || envelope.Error.Code != contract.ErrorInternal || envelope.Error.Retryable {
					t.Fatalf("cancellation exit=%d stderr=%s err=%v decode=%v", commandExitCode(err), canceledOutput.String(), err, decode)
				}
			})
		}
	}
}

func TestTakeoverStructured5xxFromRealBinary(t *testing.T) {
	binary := buildWefty(t)
	for _, action := range []string{"take", "release", "view"} {
		for _, retryable := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/retryable=%t", action, retryable), func(t *testing.T) {
				failure := contract.ComputerControlErrorResponse{Error: contract.APIError{
					Code: contract.ErrorTenureUnavailable, Message: "Controller tenure is unavailable", Retryable: retryable,
					RequestID: "front-door-request", Details: map[string]any{"reason": "backend_unavailable"},
				}, Receipt: &contract.ComputerControlReceipt{ComputerID: "computer-1"}}
				frontDoor := startStubLedger(t, func(w http.ResponseWriter, r *http.Request) {
					w.WriteHeader(http.StatusServiceUnavailable)
					_ = json.NewEncoder(w).Encode(failure)
				})
				endpoint := "ws://" + frontDoor + contract.ComputerDisplayWebSocketPath
				address := startStubLedger(t, func(w http.ResponseWriter, r *http.Request) {
					switch r.URL.Path {
					case "/v1/computers/computer-1":
						_ = json.NewEncoder(w).Encode(l1.Computer{ComputerID: "computer-1", Name: "alice"})
					case "/v1/computers/computer-1/takeover":
						_ = json.NewEncoder(w).Encode(l1.ComputerTakeoverAvailability{
							ComputerID: "computer-1", FriendlyName: "alice", DisplayEndpoint: &endpoint, PolicyRevision: 1,
						})
					default:
						t.Errorf("unexpected L1 request path %q", r.URL.Path)
						w.WriteHeader(http.StatusNotFound)
					}
				})
				capabilityFile := filepath.Join(t.TempDir(), "session.json")
				if action != "view" {
					if err := writeTakeoverSessionCapability(capabilityFile, takeoverSessionCapability{Endpoint: endpoint, Token: "session-token"}); err != nil {
						t.Fatal(err)
					}
				}
				code, stdout, stderr := runPolishBinary(t, binary, "--json", "--fabric=plain", "--plain-user-id=operator",
					"--plain-device-id=test-device", "--l1", address, "services", "takeover", action, "computer-1", "--session-token-file", capabilityFile)
				var envelope contract.ComputerControlErrorResponse
				if err := json.Unmarshal(stderr, &envelope); err != nil || code != 1 || len(stdout) != 0 ||
					envelope.Error.Code != failure.Error.Code || envelope.Error.Retryable != retryable ||
					!strings.Contains(envelope.Error.Message, failure.Error.Message) || envelope.Error.RequestID != failure.Error.RequestID ||
					envelope.Error.Details["reason"] != failure.Error.Details["reason"] || envelope.Receipt == nil || envelope.Receipt.ComputerID != "computer-1" {
					t.Fatalf("exit=%d stdout=%s stderr=%s decode=%v, want preserved structured refusal/exit 1", code, stdout, stderr, err)
				}
			})
		}
	}
}

func TestTakeoverCancellationDuringExchange(t *testing.T) {
	for _, action := range []string{"take", "release", "view"} {
		t.Run(action, func(t *testing.T) {
			entered, release := make(chan struct{}), make(chan struct{})
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				close(entered)
				<-release
			}))
			defer server.Close()
			defer close(release)
			endpoint := "ws" + strings.TrimPrefix(server.URL, "http") + contract.ComputerDisplayWebSocketPath
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			done := make(chan error, 1)
			go func() {
				var err error
				if action == "view" {
					_, err = openTakeoverViewWithPolicyRetry(ctx, directDialFabric{}, endpoint, 1)
				} else {
					clients := &apiClients{fabric: directDialFabric{}}
					_, err = clients.performComputerTakeoverAction(ctx, endpoint, "session-token", action)
				}
				done <- err
			}()
			select {
			case <-entered:
			case err := <-done:
				t.Fatalf("exchange ended before reaching front door: %v", err)
			case <-time.After(5 * time.Second):
				t.Fatal("exchange did not reach front door")
			}
			cancel()
			var err error
			select {
			case err = <-done:
			case <-time.After(5 * time.Second):
				t.Fatal("exchange did not stop on cancellation")
			}
			var stderr bytes.Buffer
			writeCommandError(&stderr, err, true)
			var envelope contract.ErrorResponse
			if decode := json.Unmarshal(stderr.Bytes(), &envelope); decode != nil || !errors.Is(err, context.Canceled) ||
				commandExitCode(err) != 1 || envelope.Error.Code != contract.ErrorInternal || envelope.Error.Retryable {
				t.Fatalf("cancellation exit=%d stderr=%s err=%v decode=%v", commandExitCode(err), stderr.String(), err, decode)
			}
		})
	}
}
