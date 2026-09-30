package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/l1"
)

type handlerTransport struct{ handler http.Handler }

func (transport handlerTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	recorder := httptest.NewRecorder()
	transport.handler.ServeHTTP(recorder, request)
	return recorder.Result(), nil
}

// A trimmed job's logs must say they were trimmed (#52): a one-shot run whose
// early output aged out must not read as a run that printed little.
func TestLogsCommandsAnnounceRetentionTruncation(t *testing.T) {
	earliest := time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)
	page := l1.LogPage{
		Events: []contract.LogEvent{{
			AttemptID: "attempt-1", Stream: contract.LogStdout, Sequence: 9, Timestamp: earliest, Bytes: []byte("still here\n"),
		}},
		NextCursor: "cursor-1",
		Truncation: &l1.LogTruncation{
			BoundKind: l1.LogRetentionAge, EvictedEventCount: 9, EvictedByteCount: 4096,
			EvictedThroughOrdinal: 9, EarliestRetainedAt: &earliest, UpdatedAt: earliest,
		},
	}
	serve := http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if !strings.HasSuffix(request.URL.Path, "/logs") {
			http.NotFound(writer, request)
			return
		}
		writer.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(writer).Encode(page)
	})
	client := &http.Client{Transport: handlerTransport{handler: serve}}
	clients := &apiClients{
		l1: &apiClient{name: "L1", flag: "l1", client: client},
		l3: &apiClient{name: "L3", flag: "l3", client: client},
	}
	for _, args := range [][]string{{"logs", "run-1"}, {"services", "logs", "job-1"}} {
		var stdout, stderr bytes.Buffer
		if err := execute(context.Background(), clients, false, args, &stdout, &stderr); err != nil {
			t.Fatalf("%v: %v", args, err)
		}
		if !strings.Contains(stdout.String(), "still here\n") {
			t.Fatalf("%v stdout = %q, want the retained event", args, stdout.String())
		}
		want := "wefty: logs trimmed by L1 retention: 9 earlier events (4096 bytes) deleted, last by the age limit; earliest retained event 2026-09-01T12:00:00Z\n"
		if stderr.String() != want {
			t.Fatalf("%v stderr = %q, want %q", args, stderr.String(), want)
		}
		if strings.Contains(stdout.String(), "trimmed") {
			t.Fatalf("%v put the notice on stdout, which is the job's own output", args)
		}
	}

	var jsonOut bytes.Buffer
	if err := execute(context.Background(), clients, true, []string{"logs", "run-1"}, &jsonOut, &bytes.Buffer{}); err != nil {
		t.Fatal(err)
	}
	var decoded l1.LogPage
	if err := json.Unmarshal(jsonOut.Bytes(), &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.Truncation == nil || decoded.Truncation.BoundKind != l1.LogRetentionAge {
		t.Fatalf("JSON logs page dropped the truncation marker: %s", jsonOut.String())
	}
}
