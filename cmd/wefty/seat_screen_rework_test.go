package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/fabric/plain"
	"github.com/Derek-X-Wang/wefty/l1"
	"github.com/coder/websocket"
)

func seatScreenReworkFixture(t *testing.T, failure string) (*apiClients, string, *atomic.Int32) {
	t.Helper()
	id := "seat_" + strings.Repeat("c", 32)
	secret := "rework-seat-secret"
	t.Setenv("WEFTY_SEAT_STATE_ROOT", filepath.Join(t.TempDir(), "seats"))
	if _, err := storeSeatCredential(id, secret); err != nil {
		t.Fatal(err)
	}
	input := new(atomic.Int32)
	address := ""
	address = startStubLedger(t, func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasPrefix(r.URL.Path, "/v1/seats/"+id+"/") || r.Header.Get(l1.SeatCredentialHeader) != secret {
			t.Error("path and credential changed Seat")
			w.WriteHeader(403)
			return
		}
		if strings.HasSuffix(r.URL.Path, "/screen-target") {
			json.NewEncoder(w).Encode(map[string]any{"endpoint": "ws://" + address, "unread_events": 0})
			return
		}
		if failure == "view" && strings.HasSuffix(r.URL.Path, "/view") {
			w.WriteHeader(500)
			json.NewEncoder(w).Encode(contract.ErrorResponse{Error: contract.APIError{Code: contract.ErrorInternal, Message: "view failed", Retryable: true}})
			return
		}
		isInput := strings.HasSuffix(r.URL.Path, "/input")
		if isInput {
			input.Add(1)
		}
		w.Header().Set("Wefty-Seat-Release-Note", base64.RawURLEncoding.EncodeToString([]byte("Same release note.")))
		w.Header().Set("Wefty-Seat-Unread-Events", "0")
		ws, err := websocket.Accept(w, r, &websocket.AcceptOptions{Subprotocols: []string{"binary"}})
		if err != nil {
			return
		}
		defer ws.CloseNow()
		ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
		defer cancel()
		events := 0
		_ = screenFakeRFBRequests(websocket.NetConn(ctx, ws, websocket.MessageBinary), func([]byte) { events++ }, func(width, height int) {
			if isInput && (width != 1 || height != 1) {
				t.Errorf("input synchronization requested %dx%d instead of 1x1", width, height)
			}
			if !isInput && (width != 1280 || height != 800) {
				t.Errorf("screenshot requested %dx%d instead of full screen", width, height)
			}
			if isInput && failure == "sync" {
				if events != 2 {
					t.Errorf("sync failure before click press and release: events=%d", events)
				}
				_ = ws.CloseNow()
			}
		})
	})
	clients, err := newAPIClients(plain.NewNetwork().NewFabric(fabric.Identity{NodeID: "screen-client"}), address, "")
	if err != nil {
		t.Fatal(err)
	}
	return clients, id, input
}

func TestSeatScreenDestinationCheckedBeforeInput(t *testing.T) {
	clients, id, input := seatScreenReworkFixture(t, "")
	path := filepath.Join(t.TempDir(), "exists.png")
	if err := os.WriteFile(path, []byte("keep"), 0600); err != nil {
		t.Fatal(err)
	}
	err := execute(t.Context(), clients, true, []string{"seat", "click", id, "1", "2", "--screenshot", path}, io.Discard, io.Discard)
	if err == nil {
		t.Fatal("existing destination accepted")
	}
	if input.Load() != 0 {
		t.Fatal("input was sent before destination refusal")
	}
	data, _ := os.ReadFile(path)
	if string(data) != "keep" {
		t.Fatal("existing destination changed")
	}
}
func TestSeatScreenScreenshotFailureReportsActionDone(t *testing.T) {
	clients, id, input := seatScreenReworkFixture(t, "view")
	path := filepath.Join(t.TempDir(), "failed.png")
	err := execute(t.Context(), clients, true, []string{"seat", "click", id, "1", "2", "--screenshot", path}, io.Discard, io.Discard)
	var api *apiResponseError
	if err == nil || !strings.Contains(err.Error(), "click done; screenshot failed:") || !errors.As(err, &api) || api.APIError.Details["action_done"] != true || api.APIError.Retryable {
		t.Fatalf("missing completed action: %v", err)
	}
	if input.Load() != 1 {
		t.Fatalf("input calls=%d", input.Load())
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("failed PNG kept: %v", err)
	}
	var rendered bytes.Buffer
	writeCommandError(&rendered, err, true)
	var value contract.ErrorResponse
	if e := json.Unmarshal(rendered.Bytes(), &value); e != nil || value.Error.Details["action_done"] != true {
		t.Fatalf("JSON lost action_done: %s", rendered.String())
	}
}
func TestSeatScreenReleaseNotesUseCallerStderrAndRepeat(t *testing.T) {
	clients, id, _ := seatScreenReworkFixture(t, "")
	var stdout, stderr bytes.Buffer
	for i := 0; i < 2; i++ {
		path := filepath.Join(t.TempDir(), "capture.png")
		if err := execute(t.Context(), clients, true, []string{"seat", "click", id, "1", "2", "--screenshot", path}, &stdout, &stderr); err != nil {
			t.Fatal(err)
		}
	}
	if strings.Contains(stdout.String(), "Same release note.") || strings.Count(stderr.String(), "Same release note.") != 4 {
		t.Fatalf("repeated note went to wrong writer: stdout=%s stderr=%s", stdout.String(), stderr.String())
	}
}
func TestSeatScreenEmptyTextRealBinary(t *testing.T) {
	binary := buildWefty(t)
	code, out := runWefty(t, binary, 30*time.Second, "seat", "type", "seat_"+strings.Repeat("d", 32), "")
	if code != 2 {
		t.Fatalf("empty TEXT exit=%d: %s", code, out)
	}
}

func TestSeatScreenSyncFailureReportsActionDone(t *testing.T) {
	for _, screenshot := range []bool{false, true} {
		t.Run(fmt.Sprintf("screenshot=%t", screenshot), func(t *testing.T) {
			clients, id, input := seatScreenReworkFixture(t, "sync")
			args := []string{"seat", "click", id, "1", "2"}
			path := filepath.Join(t.TempDir(), "failed.png")
			if screenshot {
				args = append(args, "--screenshot", path)
			}
			err := execute(t.Context(), clients, true, args, io.Discard, io.Discard)
			var api *apiResponseError
			if err == nil || !strings.Contains(err.Error(), "click done; input synchronization failed:") || !errors.As(err, &api) || api.APIError.Details["action_done"] != true || api.APIError.Retryable {
				t.Fatalf("missing completed action after sync failure: %v", err)
			}
			if input.Load() != 1 {
				t.Fatalf("input calls=%d", input.Load())
			}
			if _, err := os.Stat(path); !os.IsNotExist(err) {
				t.Fatalf("failed PNG kept: %v", err)
			}
			var rendered bytes.Buffer
			writeCommandError(&rendered, err, true)
			var value contract.ErrorResponse
			if e := json.Unmarshal(rendered.Bytes(), &value); e != nil || value.Error.Details["action_done"] != true || value.Error.Retryable {
				t.Fatalf("JSON lost completed action: %s", rendered.String())
			}
		})
	}
}
