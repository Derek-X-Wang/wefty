package main

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/l1"
)

func TestSeatLogWorkingCopyUpdateTextAndJSON(t *testing.T) {
	t.Setenv("WEFTY_SEAT_STATE_ROOT", filepath.Join(t.TempDir(), "seats"))
	id := "seat_" + strings.Repeat("a", 32)
	size := int64(999)
	event := l1.SeatLogEvent{SeatID: id, EventID: "copy:sync", Kind: l1.SeatEventCopyIn, CopyID: "sync", Direction: "sync", Path: "work", SizeBytes: &size, Outcome: "completed", NodeOutcome: "completed", Sync: &l1.SeatSyncCounts{FilesWritten: 3, FilesRemoved: 2}}
	page := l1.SeatLogPage{SeatID: id, NextCursor: "copy", Events: []l1.SeatLogEvent{event}}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			io.WriteString(w, `{}`)
			return
		}
		json.NewEncoder(w).Encode(page)
	}))
	defer server.Close()
	clients := &apiClients{l1: statusClient(t, "L1", "l1", server)}
	for _, jsonOutput := range []bool{false, true} {
		var out bytes.Buffer
		if err := executeSeatLogs(t.Context(), clients, jsonOutput, id, "", 200, 0, false, false, false, time.Second, &out, io.Discard); err != nil {
			t.Fatal(err)
		}
		if jsonOutput {
			want, _ := json.MarshalIndent(page, "", "  ")
			if !bytes.Equal(out.Bytes(), append(want, '\n')) {
				t.Fatalf("JSON changed: %s", &out)
			}
			continue
		}
		for _, part := range []string{"working-copy update", "3 written", "2 removed", "999 B"} {
			if !strings.Contains(out.String(), part) {
				t.Fatalf("missing %q: %s", part, &out)
			}
		}
		if strings.Contains(out.String(), "copy_in") {
			t.Fatalf("sync shown as plain copy: %s", &out)
		}
	}
}
