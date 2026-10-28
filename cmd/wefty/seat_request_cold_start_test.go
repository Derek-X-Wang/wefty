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

	"github.com/Derek-X-Wang/wefty/contract"
)

func TestSeatRequestColdStartReplyOnce(t *testing.T) {
	for _, asJSON := range []bool{false, true} {
		for _, succeeds := range []bool{false, true} {
			t.Run(fmt.Sprintf("json=%v/success=%v", asJSON, succeeds), func(t *testing.T) {
				t.Setenv("XDG_STATE_HOME", t.TempDir())
				id := "seat_" + strings.Repeat("a", 32)
				polls := 0
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					polls++
					result := map[string]any{"seat_id": id, "state": "starting", "host_vm_starting": true}
					if polls == 4 {
						result["host_vm_starting"] = false
						if succeeds {
							result["state"] = "live"
							result["credential"] = "private-test-credential"
						} else {
							result["state"] = "refused"
							result["reason"] = "Seat start deadline exceeded: lima_start_timeout; run wefty node doctor"
						}
					}
					json.NewEncoder(w).Encode(result)
				}))
				defer server.Close()
				clients := &apiClients{l1: statusClient(t, "L1", "test", server), wait: func(context.Context, time.Duration) error { return nil }}
				var out, stderr bytes.Buffer
				err := waitSeatCall(t.Context(), clients, asJSON, id, &out)
				if succeeds && err != nil {
					t.Fatal(err)
				}
				if !succeeds && (err == nil || !strings.Contains(err.Error(), "lima_start_timeout")) {
					t.Fatalf("refusal=%v", err)
				}
				const message = "starting the host VM, about 1 minute"
				if !asJSON {
					if strings.Count(out.String(), message) != 1 {
						t.Fatalf("reply=%q", out.String())
					}
					return
				}
				if succeeds {
					decoder := json.NewDecoder(&out)
					var result map[string]any
					if err := decoder.Decode(&result); err != nil || result["host_vm_starting"] != true || result["message"] != message {
						t.Fatalf("result=%v err=%v", result, err)
					}
					var extra any
					if err := decoder.Decode(&extra); err != io.EOF {
						t.Fatalf("extra stdout document=%v err=%v", extra, err)
					}
				} else {
					if out.Len() != 0 {
						t.Fatalf("refusal wrote stdout: %s", out.String())
					}
					writeCommandError(&stderr, err, true)
					var envelope contract.ErrorResponse
					if e := json.Unmarshal(stderr.Bytes(), &envelope); e != nil || envelope.Error.Details["host_vm_starting"] != true || envelope.Error.Details["message"] != message {
						t.Fatalf("refusal=%s err=%v", stderr.String(), e)
					}
				}
			})
		}
	}
}
