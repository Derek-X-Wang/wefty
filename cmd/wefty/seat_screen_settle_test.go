package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"image/png"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/fabric/plain"
	"github.com/coder/websocket"
)

func TestSeatScreenInputScreenshotSettles(t *testing.T) {
	for _, scenario := range []string{"delayed", "delayed-move", "delayed-drag", "delayed-scroll", "delayed-type", "delayed-key", "baseline-failure", "unchanged", "initial-modified", "empty", "empty-then-change", "screenshot", "input-only"} {
		t.Run(scenario, func(t *testing.T) {
			t.Setenv("WEFTY_SEAT_SCREEN_TIMING", "1")
			t.Setenv("WEFTY_SEAT_STATE_ROOT", filepath.Join(t.TempDir(), "seats"))
			id := "seat_" + strings.Repeat("e", 32)
			if _, err := storeSeatCredential(id, "settle-secret"); err != nil {
				t.Fatal(err)
			}
			var incremental, baseline, full, syncs, events atomic.Int32
			address := ""
			address = startStubLedger(t, func(w http.ResponseWriter, r *http.Request) {
				if strings.HasSuffix(r.URL.Path, "/screen-target") {
					_ = json.NewEncoder(w).Encode(map[string]any{"endpoint": "ws://" + address})
					return
				}
				ws, err := websocket.Accept(w, r, &websocket.AcceptOptions{Subprotocols: []string{"binary"}})
				if err != nil {
					return
				}
				defer ws.CloseNow()
				ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
				defer cancel()
				conn := websocket.NetConn(ctx, ws, websocket.MessageBinary)
				changed := false
				_ = screenFakeRFBRespond(conn, func([]byte) { events.Add(1) }, func(request []byte) error {
					width, height := int(binary.BigEndian.Uint16(request[5:])), int(binary.BigEndian.Uint16(request[7:]))
					if request[0] == 1 {
						n := incremental.Add(1)
						if scenario == "initial-modified" && baseline.Load() == 0 {
							return settleUpdate(conn, width, height, false)
						}
						if scenario == "empty" || scenario == "empty-then-change" && n == 1 {
							_, err := conn.Write([]byte{0, 0, 0, 0})
							return err
						}
						if strings.HasPrefix(scenario, "delayed") || scenario == "empty-then-change" {
							time.Sleep(80 * time.Millisecond)
							changed = true
							return settleUpdate(conn, 1, 1, true)
						}
						return nil // Hold the incremental request until the input connection closes.
					}
					if width == 1 && height == 1 {
						if scenario == "input-only" {
							full.Add(1)
						} else {
							syncs.Add(1)
						}
						return settleUpdate(conn, 1, 1, false)
					}
					if events.Load() == 0 {
						if scenario == "baseline-failure" {
							return ws.CloseNow()
						}
						if width == 1280 && height == 800 {
							baseline.Add(1)
						}
					} else {
						full.Add(1)
						if scenario == "input-only" && (width != 1 || height != 1) {
							t.Errorf("input-only request=%dx%d", width, height)
						}
					}
					return settleUpdate(conn, width, height, changed)
				})
			})
			clients, err := newAPIClients(plain.NewNetwork().NewFabric(fabric.Identity{NodeID: "screen-client"}), address, "")
			if err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(t.TempDir(), "capture.png")
			args := []string{"seat", "click", id, "1", "2", "--screenshot", path}
			switch scenario {
			case "delayed-move":
				args = []string{"seat", "move", id, "1", "2", "--screenshot", path}
			case "delayed-drag":
				args = []string{"seat", "drag", id, "1", "2", "3", "4", "--screenshot", path}
			case "delayed-scroll":
				args = []string{"seat", "scroll", id, "1", "2", "--dy", "1", "--screenshot", path}
			case "delayed-type":
				args = []string{"seat", "type", id, "text", "--screenshot", path}
			case "delayed-key":
				args = []string{"seat", "key", id, "Return", "--screenshot", path}
			}
			if scenario == "screenshot" {
				args = []string{"seat", "screenshot", id, "-o", path}
			}
			if scenario == "input-only" {
				args = []string{"seat", "click", id, "1", "2"}
			}
			var out bytes.Buffer
			start := time.Now()
			runErr := execute(t.Context(), clients, true, args, &out, io.Discard)
			if scenario == "baseline-failure" {
				if runErr == nil || events.Load() != 0 || strings.Contains(runErr.Error(), "done;") {
					t.Fatalf("baseline failure acted: err=%v events=%d", runErr, events.Load())
				}
				if _, err := os.Stat(path); !os.IsNotExist(err) {
					t.Fatalf("failed baseline retained PNG: %v", err)
				}
				return
			}
			if runErr != nil {
				t.Fatal(runErr)
			}
			elapsed := time.Since(start)
			t.Logf("call elapsed=%s", elapsed)
			var reply struct {
				After *bool `json:"frame_after_input"`
				Debug struct {
					Phases map[string]float64 `json:"screen_timing_ms"`
				} `json:"debug"`
			}
			if err := json.Unmarshal(out.Bytes(), &reply); err != nil {
				t.Fatal(err)
			}
			combined := scenario != "screenshot" && scenario != "input-only"
			wantChange := strings.HasPrefix(scenario, "delayed") || scenario == "empty-then-change"
			if combined {
				if reply.After == nil || *reply.After != wantChange {
					t.Errorf("frame_after_input=%v want %t: %s", reply.After, wantChange, out.String())
				}
				if baseline.Load() != 1 || full.Load() != 1 || syncs.Load() != 1 {
					t.Errorf("baseline=%d final=%d Sync=%d", baseline.Load(), full.Load(), syncs.Load())
				}
				if base, ok := reply.Debug.Phases["frame_baseline"]; !ok || base > reply.Debug.Phases["input_written"] {
					t.Errorf("missing or unordered baseline: %v", reply.Debug.Phases)
				}
				mark := "frame_update_timeout"
				if wantChange {
					mark = "frame_update_seen"
				}
				wait, ok := reply.Debug.Phases[mark]
				syncDone, hasSync := reply.Debug.Phases["sync_done"]
				if !ok || !hasSync || syncDone < reply.Debug.Phases["input_written"] || wait < syncDone || wait > reply.Debug.Phases["frame_read"] {
					t.Errorf("missing or unordered wait mark: %v", reply.Debug.Phases)
				}
				reopened, hasReopen := reply.Debug.Phases["capture_reopened"]
				if wantChange && hasReopen || !wantChange && (!hasReopen || reopened < wait || reopened > reply.Debug.Phases["frame_read"]) {
					t.Errorf("wrong reopen timing: %v", reply.Debug.Phases)
				}
				if !wantChange && elapsed < 900*time.Millisecond {
					t.Errorf("settle returned early: %s", elapsed)
				}
				if elapsed > 1800*time.Millisecond {
					t.Errorf("settle exceeded limit and margin: %s", elapsed)
				}
				if incremental.Load() == 0 || incremental.Load() > 60 {
					t.Errorf("incremental requests=%d", incremental.Load())
				}
			} else if reply.Debug.Phases["capture_reopened"] != 0 || reply.Debug.Phases["frame_baseline"] != 0 || reply.Debug.Phases["frame_update_seen"] != 0 || reply.Debug.Phases["frame_update_timeout"] != 0 || reply.After != nil || incremental.Load() != 0 || elapsed > 800*time.Millisecond {
				t.Errorf("ordinary call changed: reply=%s incremental=%d elapsed=%s", out.String(), incremental.Load(), elapsed)
			}
			if scenario == "input-only" {
				if baseline.Load() != 0 || full.Load() != 1 {
					t.Errorf("input-only baseline=%d sync=%d", baseline.Load(), full.Load())
				}
				return
			}
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
			if wantChange && (green != 65535 || red != 0) {
				t.Errorf("captured stale screen: red=%d green=%d", red, green)
			}
		})
	}
}

func settleUpdate(w io.Writer, width, height int, green bool) error {
	b := make([]byte, 16+width*height*4)
	b[3] = 1
	binary.BigEndian.PutUint16(b[8:], uint16(width))
	binary.BigEndian.PutUint16(b[10:], uint16(height))
	component := 2
	if green {
		component = 1
	}
	for i := 16; i < len(b); i += 4 {
		b[i+component] = 255
	}
	_, err := w.Write(b)
	return err
}
