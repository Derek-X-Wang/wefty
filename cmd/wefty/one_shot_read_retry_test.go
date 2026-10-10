package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/l1"
)

// A one-shot read — a GET with no --wait or --follow — quietly retries a
// retryable `unavailable` answer (#773) instead of handing an operator exit
// 13 on the first transient 503. Every check here decides by request count;
// no wall-clock assertion decides an outcome.

// readRetryStub serves one path for every request and counts them all.
func readRetryStub(t *testing.T, answer func(requests int32, r *http.Request, w http.ResponseWriter)) (*apiClients, *atomic.Int32) {
	t.Helper()
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		answer(requests.Add(1), r, w)
	}))
	t.Cleanup(server.Close)
	client := &apiClient{name: "L1", client: server.Client()}
	client.client.Transport = rewriteWaitTestTransport{base: server.Client().Transport, url: server.URL}
	return &apiClients{l1: client, l3: client}, &requests
}

// runOneShotList runs `computers list --json` and returns its exit error,
// stdout, and stderr.
func runOneShotList(t *testing.T, clients *apiClients) (error, *bytes.Buffer, *bytes.Buffer) {
	t.Helper()
	var stdout, stderr bytes.Buffer
	err := execute(t.Context(), clients, true, []string{"computers", "list"}, &stdout, &stderr)
	return err, &stdout, &stderr
}

// TestOneShotReadQuietlyRetriesARetryableAnswer: a `computers list` that
// meets one or two retryable 503s recovers inside the same read and exits 0.
func TestOneShotReadQuietlyRetriesARetryableAnswer(t *testing.T) {
	for _, test := range []struct {
		name     string
		refusals int32
	}{
		{"one retryable 503", 1},
		{"two retryable 503s", 2},
	} {
		t.Run(test.name, func(t *testing.T) {
			clients, requests := readRetryStub(t, func(seen int32, _ *http.Request, w http.ResponseWriter) {
				if seen <= test.refusals {
					writeRetryableUnavailable(w, "read_snapshot_expired")
					return
				}
				_ = json.NewEncoder(w).Encode(l1.ComputerList{Computers: []l1.Computer{{ComputerID: "computer-1"}}})
			})
			err, stdout, stderr := runOneShotList(t, clients)
			var listing l1.ComputerList
			decode := json.Unmarshal(stdout.Bytes(), &listing)
			if err != nil || decode != nil || requests.Load() != test.refusals+1 ||
				len(listing.Computers) != 1 || listing.Computers[0].ComputerID != "computer-1" {
				t.Fatalf("one-shot read after %d retryable 503s: err=%v requests=%d decode=%v stdout=%s stderr=%s",
					test.refusals, err, requests.Load(), decode, stdout.String(), stderr)
			}
		})
	}
}

// TestOneShotReadExhaustsQuietRetries: only retryable 503s are refused three
// times beyond the first request, and the last answer is reported exactly as
// a single unquieted try would be: exit 13, same envelope. The process
// boundary is where the envelope is written, so the check runs the real
// binary against a counting stub.
func TestOneShotReadExhaustsQuietRetries(t *testing.T) {
	binary := buildWefty(t)
	var requests int32
	address := startStubLedger(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		requests++
		writeRetryableUnavailable(w, "read_snapshot_expired")
	})
	code, output := runWefty(t, binary, 30*time.Second, "--json", "--l1", address, "computers", "list")
	var envelope contract.ErrorResponse
	if decode := json.Unmarshal([]byte(output), &envelope); decode != nil ||
		code != exitUnavailable ||
		envelope.Error.Code != contract.ErrorUnavailable || !envelope.Error.Retryable ||
		envelope.Error.Details["reason"] != "read_snapshot_expired" ||
		requests != 1+3 {
		t.Fatalf("exhausted one-shot read: exit=%d requests=%d output=%s decode=%v", code, requests, output, decode)
	}
}

// TestMutationIsNeverQuietlyRetried: a mutation answering a retryable 503 is
// sent once; its semantics are exact-once, not at-least-once.
func TestMutationIsNeverQuietlyRetried(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(context.Context, *apiClients) error
	}{
		{"PUT", func(ctx context.Context, clients *apiClients) error {
			_, err := clients.setServiceDesiredState(ctx, "svc", contract.ServiceDesiredStopped)
			return err
		}},
		{"POST", func(ctx context.Context, clients *apiClients) error {
			_, err := clients.removeService(ctx, "svc")
			return err
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			clients, requests := readRetryStub(t, func(_ int32, _ *http.Request, w http.ResponseWriter) {
				writeRetryableUnavailable(w, "read_snapshot_expired")
			})
			err := test.mutate(t.Context(), clients)
			if !isRetryableL1Answer(err) || requests.Load() != 1 {
				t.Fatalf("mutation answered a retryable 503 %d times: err=%v", requests.Load(), err)
			}
		})
	}
}

// TestOneShotReadNeverRetriesANonRetryableAnswer: any other answer — a
// refusal included — ends the read at once.
func TestOneShotReadNeverRetriesANonRetryableAnswer(t *testing.T) {
	clients, requests := readRetryStub(t, func(_ int32, _ *http.Request, w http.ResponseWriter) {
		writeForbiddenResponse(w)
	})
	err, _, _ := runOneShotList(t, clients)
	if commandExitCode(err) != exitUnauthorized || requests.Load() != 1 {
		t.Fatalf("non-retryable answer: requests=%d exit=%d", requests.Load(), commandExitCode(err))
	}
}

// TestOneShotReadCancelledDuringQuietRetryBackoff: a caller cancellation
// during the backoff returns promptly, despite the read never having to wait
// longer than the backoff it was already inside.
func TestOneShotReadCancelledDuringQuietRetryBackoff(t *testing.T) {
	served := make(chan struct{})
	var once sync.Once
	clients, requests := readRetryStub(t, func(_ int32, _ *http.Request, w http.ResponseWriter) {
		writeRetryableUnavailable(w, "read_snapshot_expired")
		once.Do(func() { close(served) })
	})
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		var stdout, stderr bytes.Buffer
		done <- execute(ctx, clients, true, []string{"computers", "list"}, &stdout, &stderr)
	}()
	<-served
	// The first quiet retry sleeps about 100 ms; leave well inside it.
	time.Sleep(50 * time.Millisecond)
	cancel()
	select {
	case err := <-done:
		if errors.Is(err, context.Canceled) && requests.Load() == 1 {
			return
		}
		t.Fatalf("cancelled during the quiet-retry backoff: requests=%d err=%v", requests.Load(), err)
	case <-time.After(5 * time.Second):
		t.Fatal("one-shot read did not return after cancellation")
	}
}
