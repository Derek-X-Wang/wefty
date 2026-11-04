package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"image/png"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/fabric/plain"
	"github.com/Derek-X-Wang/wefty/l1"
	"github.com/coder/websocket"
)

func TestSeatScreenTimeoutReopensView(t *testing.T) {
	for _, tc := range []struct {
		name   string
		split  int
		refuse bool
	}{
		{name: "split-0"}, {name: "split-2", split: 2}, {name: "split-18", split: 18}, {name: "split-30", split: 30}, {name: "reopen-failure", refuse: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("WEFTY_SEAT_SCREEN_TIMING", "1")
			t.Setenv("WEFTY_SEAT_STATE_ROOT", filepath.Join(t.TempDir(), "seats"))
			id := "seat_" + strings.Repeat("f", 32)
			if _, err := storeSeatCredential(id, "reopen-secret"); err != nil {
				t.Fatal(err)
			}
			var input, views, events, inputFrames, viewFrames, syncs atomic.Int32
			captureAttempt := make(chan struct{})
			var captureOnce sync.Once
			signalCapture := func() { captureOnce.Do(func() { close(captureAttempt) }) }
			inputClosed := make(chan struct{})
			oldFinished := make(chan error, 1)
			address := ""
			address = startStubLedger(t, func(w http.ResponseWriter, r *http.Request) {
				if r.Header.Get(l1.SeatCredentialHeader) != "reopen-secret" {
					t.Error("missing Seat credential")
					w.WriteHeader(403)
					return
				}
				if strings.HasSuffix(r.URL.Path, "/screen-target") {
					_ = json.NewEncoder(w).Encode(map[string]any{"endpoint": "ws://" + address})
					return
				}
				isView := strings.HasSuffix(r.URL.Path, "/view")
				if isView {
					views.Add(1)
					if tc.refuse {
						w.WriteHeader(http.StatusInternalServerError)
						_ = json.NewEncoder(w).Encode(contract.ErrorResponse{Error: contract.APIError{Code: contract.ErrorInternal, Message: "view reopen failed", Retryable: true}})
						return
					}
				} else {
					input.Add(1)
					defer close(inputClosed)
				}
				w.Header().Set("Wefty-Seat-Release-Note", base64.RawURLEncoding.EncodeToString([]byte("One combined note.")))
				count := "2"
				if isView {
					count = "7"
				}
				w.Header().Set("Wefty-Seat-Unread-Events", count)
				ws, err := websocket.Accept(w, r, &websocket.AcceptOptions{Subprotocols: []string{"binary"}})
				if err != nil {
					return
				}
				defer ws.CloseNow()
				ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
				defer cancel()
				conn := websocket.NetConn(ctx, ws, websocket.MessageBinary)
				_ = screenFakeRFBRespond(conn, func([]byte) {
					if isView {
						t.Error("input repeated on view connection")
					}
					events.Add(1)
				}, func(request []byte) error {
					if isView {
						viewFrames.Add(1)
						signalCapture()
						if request[0] != 0 {
							t.Error("view requested incremental frame")
						}
						return settleUpdate(conn, 1280, 800, true)
					}
					if request[0] == 1 {
						var old bytes.Buffer
						if err := settleUpdate(&old, 1280, 800, false); err != nil {
							return err
						}
						response := old.Bytes()
						if tc.split > 0 {
							if _, err := conn.Write(response[:tc.split]); err != nil {
								return err
							}
						}
						// Deliver the stale reply only after capture is attempted or input
						// is closed. Opening the spare alone must not release it. The old code
						// sends another request on input, then accepts this delayed first byte.
						go func() {
							select {
							case <-captureAttempt:
							case <-inputClosed:
							case <-ctx.Done():
								oldFinished <- ctx.Err()
								return
							}
							_, err := conn.Write(response[tc.split:])
							oldFinished <- err
						}()
						return nil
					}
					if binary.BigEndian.Uint16(request[5:]) == 1 && binary.BigEndian.Uint16(request[7:]) == 1 {
						syncs.Add(1)
						return settleUpdate(conn, 1, 1, false)
					}
					inputFrames.Add(1)
					if events.Load() > 0 {
						signalCapture()
						select {
						case <-oldFinished:
						case <-ctx.Done():
							return ctx.Err()
						}
						// Keep the completion observable for the assertion below.
						oldFinished <- nil
						return settleUpdate(conn, 1280, 800, true)
					}
					return settleUpdate(conn, 1280, 800, false)
				})
			})
			clients, err := newAPIClients(plain.NewNetwork().NewFabric(fabric.Identity{NodeID: "screen-client"}), address, "")
			if err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(t.TempDir(), "capture.png")
			var out, stderr bytes.Buffer
			start := time.Now()
			runErr := execute(t.Context(), clients, true, []string{"seat", "click", id, "1", "2", "--screenshot", path}, &out, &stderr)
			t.Logf("call elapsed=%s", time.Since(start))
			if tc.refuse {
				var api *apiResponseError
				if runErr == nil || !errors.As(runErr, &api) || api.APIError.Details["action_done"] != true || api.APIError.Retryable || !strings.Contains(runErr.Error(), "click done; screenshot failed: view reopen failed") {
					t.Fatalf("reopen lost completed input: %v", runErr)
				}
				if _, err := os.Stat(path); !os.IsNotExist(err) {
					t.Fatalf("failed reopen retained PNG: %v", err)
				}
			} else {
				if runErr != nil {
					t.Fatal(runErr)
				}
				var reply struct {
					After  *bool `json:"frame_after_input"`
					Unread int   `json:"unread_events"`
					Debug  struct {
						Phases map[string]float64 `json:"screen_timing_ms"`
					} `json:"debug"`
				}
				if err := json.Unmarshal(out.Bytes(), &reply); err != nil {
					t.Fatal(err)
				}
				if reply.After == nil || *reply.After || reply.Unread != 7 {
					t.Errorf("wrong reopened reply: %s", out.String())
				}
				timeout, okTimeout := reply.Debug.Phases["frame_update_timeout"]
				reopened, okReopen := reply.Debug.Phases["capture_reopened"]
				syncDone, okSync := reply.Debug.Phases["sync_done"]
				if !okTimeout || !okReopen || !okSync || syncDone < reply.Debug.Phases["input_written"] || syncDone > timeout || timeout > reopened || reopened > reply.Debug.Phases["frame_read"] {
					t.Errorf("missing or unordered reopen timing: %v", reply.Debug.Phases)
				}
				t.Logf("reopen elapsed=%.3fms", reopened-timeout)
				f, err := os.Open(path)
				if err != nil {
					t.Fatal(err)
				}
				defer f.Close()
				frame, err := png.Decode(f)
				if err != nil {
					t.Fatal(err)
				}
				red, green, _, _ := frame.At(0, 0).RGBA()
				if red != 0 || green != 65535 {
					t.Errorf("stale capture: red=%d green=%d", red, green)
				}
				if viewFrames.Load() != 1 {
					t.Errorf("view full requests=%d", viewFrames.Load())
				}
			}
			if input.Load() != 1 || views.Load() != 1 || events.Load() != 2 || inputFrames.Load() != 1 || syncs.Load() != 1 {
				t.Errorf("input=%d views=%d events=%d input full requests=%d Sync=%d", input.Load(), views.Load(), events.Load(), inputFrames.Load(), syncs.Load())
			}
			if strings.Count(stderr.String(), "One combined note.") != 1 {
				t.Errorf("release note repeated: %q", stderr.String())
			}
			select {
			case <-inputClosed:
			case <-time.After(time.Second):
				t.Error("input connection still open")
			}
			select {
			case <-oldFinished:
			case <-time.After(time.Second):
				t.Error("old reader/writer still pending")
			}
		})
	}
}
