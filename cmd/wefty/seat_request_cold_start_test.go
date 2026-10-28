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
)

func TestSeatRequestColdStartReplyOnce(t *testing.T) {
	for _, asJSON := range []bool{false, true} {
		t.Run(map[bool]string{false: "text", true: "json"}[asJSON], func(t *testing.T) {
			id := "seat_" + strings.Repeat("a", 32)
			polls := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				polls++
				state, reason := "starting", ""
				if polls == 4 {
					state, reason = "refused", "Seat start deadline exceeded: lima_start_timeout; run wefty node doctor"
				}
				json.NewEncoder(w).Encode(map[string]any{"seat_id": id, "state": state, "reason": reason, "host_vm_starting": polls < 4})
			}))
			defer server.Close()
			clients := &apiClients{l1: statusClient(t, "L1", "test", server), wait: func(context.Context, time.Duration) error { return nil }}
			var out bytes.Buffer
			err := waitSeatCall(t.Context(), clients, asJSON, id, &out)
			if err == nil || !strings.Contains(err.Error(), "lima_start_timeout") {
				t.Fatalf("refusal=%v", err)
			}
			if strings.Count(out.String(), "starting the host VM, about 1 minute") != 1 {
				t.Fatalf("reply=%q", out.String())
			}
			if asJSON {
				var value map[string]any
				if json.Unmarshal(out.Bytes(), &value) != nil || value["host_vm_starting"] != true {
					t.Fatalf("JSON reply=%s", out.String())
				}
			}
		})
	}
}
