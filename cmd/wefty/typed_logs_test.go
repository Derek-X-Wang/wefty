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

func TestTypedLogTargetsRealBinary(t *testing.T) {
	binary := buildWefty(t)
	for _, cmd := range []string{"logs", "cancel"} {
		code, out := runWefty(t, binary, 30*time.Second, "--l1="+unusedAddress(t), "--l3=", cmd, "unknown_123")
		if code != exitUsage || !strings.Contains(out, "unknown ID kind") {
			t.Fatalf("%s: exit=%d %s", cmd, code, out)
		}
	}
	address := startStubLedger(t, func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/jobs/job-log":
			json.NewEncoder(w).Encode(map[string]any{"job_id": "job-log", "spec": map[string]any{"labels": map[string]string{"run_id": "run_log"}}})
		case "/v1/runs/run_log/logs":
			json.NewEncoder(w).Encode(l1.LogPage{Events: nil})
		default:
			t.Errorf("unexpected request %s", r.URL)
			http.NotFound(w, r)
		}
	})
	code, out := runWefty(t, binary, 30*time.Second, "--l1="+address, "--l3="+address, "logs", "job-log", "--json")
	if code != 0 {
		t.Fatalf("Job log: exit=%d %s", code, out)
	}
}

func TestSeatUnreadReply(t *testing.T) {
	t.Setenv("WEFTY_SEAT_STATE_ROOT", t.TempDir()+"/seats")
	id := "seat_" + strings.Repeat("a", 32)
	for _, count := range []int{0, 1, 2} {
		for _, jsonOutput := range []bool{false, true} {
			t.Run(fmt.Sprintf("unread-%d-json-%t", count, jsonOutput), func(t *testing.T) {
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					json.NewEncoder(w).Encode(map[string]any{"seat_id": id, "state": "live", "credential": "secret", "unread_events": count, "room_revision": int64(9007199254740993)})
				}))
				defer server.Close()
				clients := &apiClients{l1: statusClient(t, "L1", "l1", server)}
				var out bytes.Buffer
				if err := waitSeatCall(context.Background(), clients, jsonOutput, id, &out); err != nil {
					t.Fatal(err)
				}
				if count == 0 {
					if strings.Contains(out.String(), "new Seat") {
						t.Fatal(out.String())
					}
				} else {
					noun := "events"
					if count == 1 {
						noun = "event"
					}
					if !strings.Contains(out.String(), fmt.Sprintf("%d new Seat %s: wefty logs %s", count, noun, id)) {
						t.Fatal(out.String())
					}
				}
				if jsonOutput {
					if !strings.Contains(out.String(), `"room_revision": 9007199254740993`) {
						t.Fatalf("reply changed the revision: %s", out.String())
					}
					var v map[string]any
					if err := json.Unmarshal(out.Bytes(), &v); err != nil {
						t.Fatal(err)
					}
				}
			})
		}
	}
}

func TestSeatLogFollowTailCursorAndEndedExitRealBinary(t *testing.T) {
	binary := buildWefty(t)
	id := "seat_" + strings.Repeat("b", 32)
	root := t.TempDir() + "/seats"
	t.Setenv("WEFTY_SEAT_STATE_ROOT", root)
	if _, err := storeSeatCredential(id, "seat-secret"); err != nil {
		t.Fatal(err)
	}
	reads := []string{}
	address := startStubLedger(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get(l1.SeatCredentialHeader) != "seat-secret" {
			t.Error("missing Seat credential")
		}
		if r.Method == http.MethodPost && r.URL.Path == "/v1/seats/"+id+"/logs/read" {
			var read l1.SeatLogRead
			json.NewDecoder(r.Body).Decode(&read)
			reads = append(reads, read.Cursor)
			json.NewEncoder(w).Encode(read)
			return
		}
		if r.URL.Path != "/v1/seats/"+id+"/logs" {
			t.Errorf("unexpected %s", r.URL)
			http.NotFound(w, r)
			return
		}
		page := l1.SeatLogPage{SeatID: id, Events: []l1.SeatLogEvent{}, Ended: true, Reason: "the Computer restarted", NextCursor: "end"}
		switch r.URL.Query().Get("cursor") {
		case "start":
			page.Events = []l1.SeatLogEvent{{SeatID: id, EventID: "control", Kind: l1.SeatEventControlTaken}, {SeatID: id, EventID: "ended", Kind: l1.SeatEventEnded, Reason: page.Reason}}
		case "end":
		default:
			t.Errorf("cursor lost: %s", r.URL)
		}
		json.NewEncoder(w).Encode(page)
	})
	code, out := runWefty(t, binary, 30*time.Second, "--l1="+address, "--l3=", "logs", id, "--follow", "--tail=1", "--cursor=start", "--poll-interval=1ms")
	if code != 0 || strings.Contains(out, "control_taken") || !strings.Contains(out, "ended: the Computer restarted") {
		t.Fatalf("follow: exit=%d %s", code, out)
	}
	if len(reads) == 0 || reads[len(reads)-1] != "end" {
		t.Fatalf("read cursors: %v", reads)
	}
	lines := strings.Split(strings.TrimSpace(out), "\n")
	if !strings.Contains(lines[len(lines)-1], "the Computer restarted") {
		t.Fatal(out)
	}
	// The existing typed Seat refusal must also cross the real-binary boundary.
	refused := startStubLedger(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusConflict)
		io.WriteString(w, `{"error":{"code":"seat_ended","message":"Seat ended: released"}}`)
	})
	code, out = runWefty(t, binary, 30*time.Second, "--l1="+refused, "--l3=", "logs", id)
	if code != exitConflict || !strings.Contains(out, "Seat ended: released") {
		t.Fatalf("typed end: %d %s", code, out)
	}
}

func TestSeatPendingReplyIncludesUnreadHint(t *testing.T) {
	id := "seat_" + strings.Repeat("c", 32)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]any{"seat_id": id, "state": "requested", "unread_events": 1})
	}))
	defer server.Close()
	clients := &apiClients{l1: statusClient(t, "L1", "l1", server), wait: func(context.Context, time.Duration) error { return context.Canceled }}
	var out bytes.Buffer
	err := waitSeatCall(t.Context(), clients, false, id, &out)
	if err == nil || !strings.Contains(err.Error(), "1 new Seat event: wefty logs "+id) {
		t.Fatalf("pending reply: %v", err)
	}
}
