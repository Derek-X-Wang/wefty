package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/l1"
)

func TestSeatLogOneShotAcknowledgementRefusalWarns(t *testing.T) {
	for _, status := range []int{http.StatusForbidden, http.StatusServiceUnavailable} {
		for _, mode := range []string{"text", "json", "tail"} {
			t.Run(fmt.Sprintf("%d-%s", status, mode), func(t *testing.T) {
				t.Setenv("WEFTY_SEAT_STATE_ROOT", t.TempDir()+"/seats")
				id := "seat_" + strings.Repeat("c", 32)
				gets, posts := 0, 0
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if r.Method == http.MethodPost {
						posts++
						w.WriteHeader(status)
						io.WriteString(w, `{"error":{"code":"forbidden","message":"acknowledgement unavailable"}}`)
						return
					}
					gets++
					page := l1.SeatLogPage{SeatID: id, Events: []l1.SeatLogEvent{}, NextCursor: "one"}
					if gets == 1 {
						page.Events = []l1.SeatLogEvent{{SeatID: id, EventID: "owner-note", Kind: l1.SeatEventControlTaken, Note: "Inspect this page"}}
					}
					json.NewEncoder(w).Encode(page)
				}))
				defer server.Close()
				clients := &apiClients{l1: statusClient(t, "L1", "l1", server), wait: func(context.Context, time.Duration) error { return nil }}
				var out, warning bytes.Buffer
				err := executeSeatLogs(t.Context(), clients, mode == "json", id, "", 200, 1, mode == "tail", false, false, time.Second, &out, &warning)
				if err != nil {
					t.Fatalf("printed log returned failure: %v", err)
				}
				if !strings.Contains(out.String(), "Inspect this page") || !strings.Contains(warning.String(), "warning") || !strings.Contains(warning.String(), "acknowledg") || posts != 1 {
					t.Fatalf("output=%q stderr=%q POSTs=%d", out.String(), warning.String(), posts)
				}
			})
		}
	}
}
