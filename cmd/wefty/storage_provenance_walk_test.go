package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/l1"
)

// The provenance walk shares the observation window: retryable answers keep
// polling there too, and exhausting that window is the wait-timeout verdict.
func TestProvenanceWalkToleratesARetryableAnswer(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()

	provenanceStub := func(t *testing.T, retryable func(reads int32) bool) (*apiClients, *atomic.Int32) {
		t.Helper()
		var reads atomic.Int32
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			reads.Add(1)
			if !strings.HasSuffix(r.URL.Path, "/storage-provenance") {
				t.Errorf("unexpected request %s %s", r.Method, r.URL)
				w.WriteHeader(http.StatusNotFound)
				return
			}
			if !retryable(reads.Load()) {
				_ = json.NewEncoder(w).Encode(l1.ComputerStorageProvenance{})
				return
			}
			writeRetryableUnavailable(w, "read_snapshot_expired")
		}))
		t.Cleanup(server.Close)
		client := &apiClient{name: "L1", client: server.Client()}
		client.client.Transport = rewriteWaitTestTransport{base: server.Client().Transport, url: server.URL}
		return &apiClients{l1: client}, &reads
	}

	t.Run("retryable answers mid-wait keep polling", func(t *testing.T) {
		clients, reads := provenanceStub(t, func(reads int32) bool { return reads < 3 })
		output := storageMutationOutput{Observation: &storageWaitObservation{
			waitDeadline: time.Now().Add(2 * time.Second), waitPollInterval: time.Millisecond,
		}}
		if err := attachStorageProvenance(ctx, clients, "computer-1", &output, nil); err != nil {
			t.Fatalf("provenance walk with two retryable answers: %v", err)
		}
		if output.StorageProvenance == nil || reads.Load() != 3 {
			t.Fatalf("provenance reads = %d, want 3 with provenance attached", reads.Load())
		}
	})
	t.Run("one-shot read is unchanged", func(t *testing.T) {
		clients, reads := provenanceStub(t, func(int32) bool { return true })
		output := storageMutationOutput{}
		err := attachStorageProvenance(ctx, clients, "computer-1", &output, nil)
		var refusal *apiResponseError
		if commandExitCode(err) != exitUnavailable || !errors.As(err, &refusal) || reads.Load() != 1 ||
			output.ProvenanceUnavailable != "" {
			t.Fatalf("one-shot provenance read changed: reads=%d exit=%d err=%v note=%q",
				reads.Load(), commandExitCode(err), err, output.ProvenanceUnavailable)
		}
	})
	t.Run("only retryable answers until the window closes", func(t *testing.T) {
		clients, reads := provenanceStub(t, func(int32) bool { return true })
		output := storageMutationOutput{Observation: &storageWaitObservation{
			waitDeadline: time.Now().Add(300 * time.Millisecond), waitPollInterval: time.Millisecond,
		}}
		err := attachStorageProvenance(ctx, clients, "computer-1", &output, nil)
		var timeout *mutationWaitTimeoutError
		if commandExitCode(err) != exitMutationWaitTimeout || !errors.As(err, &timeout) || reads.Load() < 3 {
			t.Fatalf("retryable-only provenance walk: reads=%d exit=%d err=%v", reads.Load(), commandExitCode(err), err)
		}
		if !strings.Contains(err.Error(), "read_snapshot_expired") {
			t.Fatalf("last retryable answer missing from provenance timeout: %v", err)
		}
	})
}
