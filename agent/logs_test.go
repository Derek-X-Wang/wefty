package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/fabric/plain"
	"github.com/Derek-X-Wang/wefty/l1"
)

func TestBatchingLogSinkRetriesTheIdenticalBatch(t *testing.T) {
	network := plain.NewNetwork()
	serverFabric := network.NewFabric(fabric.Identity{NodeID: "control-plane"})
	listener, err := serverFabric.Listen("tcp", "wefty://control-plane")
	if err != nil {
		t.Fatal(err)
	}

	var mu sync.Mutex
	var payloads [][]byte
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		payload, err := io.ReadAll(r.Body)
		if err != nil {
			t.Error(err)
			return
		}
		mu.Lock()
		payloads = append(payloads, bytes.Clone(payload))
		requestNumber := len(payloads)
		mu.Unlock()
		if requestNumber == 1 {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(contract.ErrorResponse{Error: contract.APIError{Code: contract.ErrorInternal, Message: "try again", Retryable: false}})
			return
		}
		var request l1.AppendLogsRequest
		if err := json.Unmarshal(payload, &request); err != nil {
			t.Error(err)
			return
		}
		acknowledged := map[contract.LogStream]uint64{}
		for _, event := range request.Events {
			acknowledged[event.Stream] = event.Sequence
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(l1.AppendLogsResponse{Acknowledged: acknowledged})
	})
	server := &http.Server{Handler: handler}
	served := make(chan error, 1)
	go func() { served <- server.Serve(listener) }()
	t.Cleanup(func() {
		_ = server.Close()
		if err := <-served; err != nil && err != http.ErrServerClosed {
			t.Errorf("serve upload test: %v", err)
		}
	})

	participant := network.NewFabric(fabric.Identity{NodeID: "agent"})
	client, err := NewClient(participant, "wefty://control-plane")
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	claim := l1.Claim{
		Job:   l1.Job{JobID: "job-batch", Spec: contract.JobSpec{Class: contract.JobClassOneShot}},
		Lease: l1.AttemptLease{AttemptID: "attempt-batch", FencingToken: "fence-batch"},
	}
	spool, err := openLogSpool(t.TempDir(), "node-batch", 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	defer spool.Close()
	sink, err := newBatchingLogSink(context.Background(), client, claim, spool, systemClock{}, 3, time.Hour, time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	for sequence := range uint64(3) {
		event := contract.LogEvent{
			AttemptID: claim.Lease.AttemptID,
			Stream:    contract.LogStdout,
			Sequence:  sequence,
			Timestamp: time.Date(2026, 8, 9, 12, 0, int(sequence), 0, time.UTC),
			Bytes:     []byte{byte('a' + sequence)},
		}
		if err := sink.WriteOutput(context.Background(), event); err != nil {
			t.Fatal(err)
		}
	}
	if err := sink.Close(); err != nil {
		t.Fatal(err)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(payloads) != 2 {
		t.Fatalf("upload requests = %d, want failed attempt plus one retry", len(payloads))
	}
	if !bytes.Equal(payloads[0], payloads[1]) {
		t.Fatal("retry changed the idempotent upload batch")
	}
	var uploaded l1.AppendLogsRequest
	if err := json.Unmarshal(payloads[1], &uploaded); err != nil {
		t.Fatal(err)
	}
	if len(uploaded.Events) != 3 {
		t.Fatalf("uploaded event count = %d, want one three-event batch", len(uploaded.Events))
	}
}
