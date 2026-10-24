package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/l1"
)

func TestSeatLogFollowAcknowledgesOnlyDeliveredEvents(t *testing.T) {
	t.Setenv("WEFTY_SEAT_STATE_ROOT", t.TempDir()+"/seats")
	id := "seat_" + strings.Repeat("d", 32)
	gets, posts := 0, 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			posts++
			io.WriteString(w, `{}`)
			return
		}
		gets++
		page := l1.SeatLogPage{SeatID: id, Events: []l1.SeatLogEvent{}, NextCursor: "one"}
		if gets == 1 {
			page.Events = []l1.SeatLogEvent{{SeatID: id, EventID: "started", Kind: l1.SeatEventStarted}}
		}
		if gets == 4 {
			page.Ended = true
			page.Reason = "released"
		}
		json.NewEncoder(w).Encode(page)
	}))
	defer server.Close()
	clients := &apiClients{l1: statusClient(t, "L1", "l1", server), wait: func(context.Context, time.Duration) error { return nil }}
	var out bytes.Buffer
	if err := executeSeatLogs(t.Context(), clients, false, id, "", 200, 0, false, true, false, time.Second, &out, io.Discard); err != nil {
		t.Fatal(err)
	}
	if gets != 4 || posts != 1 {
		t.Fatalf("GETs=%d POSTs=%d; empty polls must not acknowledge", gets, posts)
	}
}

func TestSeatLogFollowRetriesPendingAcknowledgement(t *testing.T) {
	t.Setenv("WEFTY_SEAT_STATE_ROOT", t.TempDir()+"/seats")
	id := "seat_" + strings.Repeat("e", 32)
	gets, posts := 0, 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			posts++
			if posts == 1 {
				w.WriteHeader(503)
				io.WriteString(w, `{"error":{"code":"unavailable","retryable":true,"message":"try later"}}`)
				return
			}
			io.WriteString(w, `{}`)
			return
		}
		gets++
		page := l1.SeatLogPage{SeatID: id, Events: []l1.SeatLogEvent{}, NextCursor: "one"}
		if gets == 1 {
			page.Events = []l1.SeatLogEvent{{SeatID: id, EventID: "started", Kind: l1.SeatEventStarted}}
		}
		if gets >= 3 {
			page.Ended = true
			page.Reason = "released"
		}
		json.NewEncoder(w).Encode(page)
	}))
	defer server.Close()
	clients := &apiClients{l1: statusClient(t, "L1", "l1", server), wait: func(context.Context, time.Duration) error { return nil }}
	var out bytes.Buffer
	if err := executeSeatLogs(t.Context(), clients, false, id, "", 200, 0, false, true, false, time.Second, &out, io.Discard); err != nil {
		t.Fatalf("retryable acknowledgement ended follow: %v", err)
	}
	if gets != 3 || posts != 2 {
		t.Fatalf("GETs=%d POSTs=%d; retry pending acknowledgement once, then stop posting on empty polls", gets, posts)
	}
	if strings.Count(out.String(), "started") != 1 || !strings.Contains(out.String(), "ended: released") {
		t.Fatal(out.String())
	}
}

func TestSeatLogOldCredentialFallsBackOnceToCaller(t *testing.T) {
	for _, status := range []int{http.StatusOK, http.StatusForbidden, http.StatusNotFound} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			t.Setenv("WEFTY_SEAT_STATE_ROOT", t.TempDir()+"/seats")
			id := "seat_" + strings.Repeat("f", 32)
			if _, err := storeSeatCredential(id, "old-scrubbed-secret"); err != nil {
				t.Fatal(err)
			}
			gets, posts := 0, 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method == http.MethodPost {
					posts++
					if r.Header.Get(l1.SeatCredentialHeader) != "" {
						t.Error("acknowledgement retried with scrubbed secret")
					}
					io.WriteString(w, `{}`)
					return
				}
				gets++
				if gets == 1 {
					if r.Header.Get(l1.SeatCredentialHeader) == "" {
						t.Error("initial credential missing")
					}
					w.WriteHeader(403)
					io.WriteString(w, `{"error":{"code":"forbidden","message":"invalid Seat credential"}}`)
					return
				}
				if r.Header.Get(l1.SeatCredentialHeader) != "" {
					t.Error("fallback kept credential")
				}
				w.WriteHeader(status)
				if status == 200 {
					json.NewEncoder(w).Encode(l1.SeatLogPage{SeatID: id, Events: []l1.SeatLogEvent{{SeatID: id, EventID: "upgrade-state", Kind: l1.SeatEventEnded, Reason: "released"}}, Ended: true, Reason: "released", NextCursor: "end"})
					return
				}
				code := "forbidden"
				if status == 404 {
					code = "not_found"
				}
				json.NewEncoder(w).Encode(map[string]any{"error": map[string]any{"code": code, "message": "caller refusal"}})
			}))
			defer server.Close()
			clients := &apiClients{l1: statusClient(t, "L1", "l1", server)}
			var out bytes.Buffer
			err := executeSeatLogs(t.Context(), clients, false, id, "", 200, 0, false, false, false, time.Second, &out, io.Discard)
			if gets != 2 {
				t.Fatalf("GETs=%d; want exactly one credential-free retry; error=%v", gets, err)
			}
			if status == 200 {
				if err != nil || posts != 1 {
					t.Fatalf("read failed: %v POSTs=%d", err, posts)
				}
			} else if err == nil || posts != 0 {
				t.Fatalf("fallback widened access: error=%v POSTs=%d", err, posts)
			}
		})
	}
}

func TestSeatLogFollowEndReasonReadableTextAndJSONToken(t *testing.T) {
	t.Setenv("WEFTY_SEAT_STATE_ROOT", t.TempDir()+"/seats")
	id := "seat_" + strings.Repeat("a", 32)
	for _, test := range []struct{ token, text string }{{"computer_stopped", "the owner stopped the Computer"}, {"computer_restarted", "the owner restarted the Computer"}, {"computer_reimaged", "the owner reimaged the Computer"}, {"computer_removed", "the owner removed the Computer"}} {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			json.NewEncoder(w).Encode(l1.SeatLogPage{SeatID: id, Events: []l1.SeatLogEvent{}, Ended: true, Reason: test.token, NextCursor: "end"})
		}))
		clients := &apiClients{l1: statusClient(t, "L1", "l1", server)}
		for _, jsonOutput := range []bool{false, true} {
			var out bytes.Buffer
			if err := executeSeatLogs(t.Context(), clients, jsonOutput, id, "", 200, 0, false, true, false, time.Second, &out, io.Discard); err != nil {
				t.Fatal(err)
			}
			want := test.text
			if jsonOutput {
				want = `"reason":"` + test.token + `"`
			}
			if !strings.Contains(out.String(), want) {
				t.Errorf("reason=%s JSON=%t output=%s", test.token, jsonOutput, out.String())
			}
		}
		server.Close()
	}
}
