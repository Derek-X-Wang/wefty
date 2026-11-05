package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"image/color"
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
	"github.com/Derek-X-Wang/wefty/l1"
	"github.com/coder/websocket"
)

// After a settle timeout the capture stays on the input connection (#1484).
// Each scenario is one way a server can answer the held incremental request
// and the full request that follows it.
func TestSeatScreenTimeoutCapturesOnInputConnection(t *testing.T) {
	blue, green := [4]byte{255, 0, 0, 0}, [4]byte{0, 255, 0, 0}
	for _, scenario := range []string{"merged", "separate", "capture-failure", "capture-cancelled", "change-seen"} {
		t.Run(scenario, func(t *testing.T) {
			t.Setenv("WEFTY_SEAT_SCREEN_TIMING", "1")
			t.Setenv("WEFTY_SEAT_STATE_ROOT", filepath.Join(t.TempDir(), "seats"))
			id := "seat_" + strings.Repeat("f", 32)
			if _, err := storeSeatCredential(id, "capture-secret"); err != nil {
				t.Fatal(err)
			}
			var inputs, views, incremental, finals atomic.Int32
			var heldAtFinal atomic.Bool
			inputClosed, finalSeen := make(chan struct{}), make(chan struct{})
			address := ""
			address = startStubLedger(t, func(w http.ResponseWriter, r *http.Request) {
				if r.Header.Get(l1.SeatCredentialHeader) != "capture-secret" {
					t.Error("missing Seat credential")
					w.WriteHeader(http.StatusForbidden)
					return
				}
				if strings.HasSuffix(r.URL.Path, "/screen-target") {
					_ = json.NewEncoder(w).Encode(map[string]any{"endpoint": "ws://" + address})
					return
				}
				if strings.HasSuffix(r.URL.Path, "/view") {
					views.Add(1)
					w.WriteHeader(http.StatusInternalServerError)
					return
				}
				inputs.Add(1)
				defer close(inputClosed)
				w.Header().Set("Wefty-Seat-Release-Note", base64.RawURLEncoding.EncodeToString([]byte("One combined note.")))
				ws, err := websocket.Accept(w, r, &websocket.AcceptOptions{Subprotocols: []string{"binary"}})
				if err != nil {
					return
				}
				defer ws.CloseNow()
				ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second) // Bounds a hang only.
				defer cancel()
				conn := websocket.NetConn(ctx, ws, websocket.MessageBinary)
				events, held := 0, false
				_ = screenFakeRFBRespond(conn, func([]byte) { events++ }, func(request []byte) error {
					width, height := int(binary.BigEndian.Uint16(request[5:])), int(binary.BigEndian.Uint16(request[7:]))
					if request[0] == 1 {
						incremental.Add(1)
						held = true
						return nil // Held: answered only together with or after the full request.
					}
					if events == 0 || width == 1 && height == 1 {
						// Baseline and Sync are red, except change-seen's Sync: its green
						// pixel reports the change synchronously, with no incremental wait
						// to race the settle timer (rfbclient covers incremental changes).
						return settleUpdate(conn, width, height, scenario == "change-seen" && events > 0)
					}
					finals.Add(1)
					// The capture follows the incremental request it drains.
					heldAtFinal.Store(held)
					close(finalSeen)
					switch scenario {
					case "merged", "change-seen":
						// LibVNCServer merges both requests into one update. neatvnc may
						// answer the full request alone; on the wire that looks the same.
						return settleUpdate(conn, 1280, 800, true)
					case "capture-cancelled":
						return nil // Never answered: only the call's context ends the wait.
					case "separate":
						// The incremental answer, then a split full answer. A newer
						// rectangle overwrites (0,0); (1,0) keeps the incremental pixel.
						if err := rectanglesUpdate(conn, screenRect{0, 0, 2, 1, blue}); err != nil {
							return err
						}
						if err := rectanglesUpdate(conn, screenRect{0, 0, 1, 1, green}); err != nil {
							return err
						}
						return rectanglesUpdate(conn, screenRect{2, 0, 1278, 1, green}, screenRect{0, 1, 1280, 799, green})
					}
					return errors.New("capture refused") // capture-failure closes the stream
				})
			})
			clients, err := newAPIClients(plain.NewNetwork().NewFabric(fabric.Identity{NodeID: "screen-client"}), address, "")
			if err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(t.TempDir(), "capture.png")
			var out, stderr bytes.Buffer
			// The call's 15 s context derives from this one; cancelling it while
			// the capture reply is missing stands in for that deadline.
			callCtx, cancelCall := context.WithCancel(t.Context())
			defer cancelCall()
			if scenario == "capture-cancelled" {
				go func() {
					select {
					case <-finalSeen:
						cancelCall()
					case <-callCtx.Done():
					}
				}()
			}
			done := make(chan error, 1)
			go func() {
				done <- execute(callCtx, clients, true, []string{"seat", "click", id, "1", "2", "--screenshot", path}, &out, &stderr)
			}()
			var runErr error
			select {
			case runErr = <-done:
			case <-time.After(30 * time.Second): // Watchdog against a hang.
				t.Fatal("call did not return")
			}
			wantIncremental := int32(1)
			if scenario == "change-seen" {
				wantIncremental = 0
			}
			if views.Load() != 0 || inputs.Load() != 1 || finals.Load() != 1 || incremental.Load() != wantIncremental {
				t.Errorf("views=%d inputs=%d final full requests=%d incremental=%d", views.Load(), inputs.Load(), finals.Load(), incremental.Load())
			}
			if strings.Count(stderr.String(), "One combined note.") != 1 {
				t.Errorf("release note: %q", stderr.String())
			}
			select {
			case <-inputClosed:
			case <-time.After(10 * time.Second):
				t.Error("input connection still open")
			}
			if heldAtFinal.Load() != (scenario != "change-seen") {
				t.Errorf("capture request with an incremental request held=%t", heldAtFinal.Load())
			}
			if scenario == "capture-failure" || scenario == "capture-cancelled" {
				var api *apiResponseError
				if runErr == nil || !errors.As(runErr, &api) || api.APIError.Details["action_done"] != true || api.APIError.Retryable || !strings.Contains(runErr.Error(), "click done; screenshot failed") {
					t.Fatalf("capture failure lost completed input: %v", runErr)
				}
				if _, err := os.Stat(path); !os.IsNotExist(err) {
					t.Fatalf("failed capture retained PNG: %v", err)
				}
				return
			}
			if runErr != nil {
				t.Fatal(runErr)
			}
			var reply struct {
				After *bool `json:"frame_after_input"`
				Debug struct {
					Phases map[string]float64 `json:"screen_timing_ms"`
				} `json:"debug"`
			}
			if err := json.Unmarshal(out.Bytes(), &reply); err != nil {
				t.Fatal(err)
			}
			p := reply.Debug.Phases
			if _, reopened := p["capture_reopened"]; reopened {
				t.Errorf("reopen mark: %v", p)
			}
			if scenario == "change-seen" {
				_, drained := p["capture_drained"]
				if reply.After == nil || !*reply.After || drained || p["frame_update_seen"] == 0 || p["frame_update_seen"] > p["frame_read"] {
					t.Errorf("change-seen reply: %s", out.String())
				}
			} else {
				timeout, okTimeout := p["frame_update_timeout"]
				drained, okDrained := p["capture_drained"]
				if reply.After == nil || *reply.After {
					t.Errorf("frame_after_input after timeout: %s", out.String())
				}
				if !okTimeout || !okDrained || p["sync_done"] > timeout || timeout > drained || drained > p["frame_read"] {
					t.Errorf("missing or unordered capture marks: %v", p)
				}
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
			want := map[[2]int]color.RGBA{{0, 0}: {0, 255, 0, 255}, {1, 0}: {0, 255, 0, 255}, {640, 400}: {0, 255, 0, 255}}
			if scenario == "separate" {
				want[[2]int{1, 0}] = color.RGBA{0, 0, 255, 255}
			}
			for at, pixel := range want {
				if got := color.RGBAModel.Convert(frame.At(at[0], at[1])); got != pixel {
					t.Errorf("pixel %v=%v want %v", at, got, pixel)
				}
			}
		})
	}
}

type screenRect struct {
	x, y, w, h int
	pixel      [4]byte
}

func rectanglesUpdate(w io.Writer, rects ...screenRect) error {
	b := []byte{0, 0, 0, 0}
	binary.BigEndian.PutUint16(b[2:], uint16(len(rects)))
	for _, r := range rects {
		header := make([]byte, 12)
		for i, n := range []int{r.x, r.y, r.w, r.h} {
			binary.BigEndian.PutUint16(header[i*2:], uint16(n))
		}
		b = append(b, header...)
		b = append(b, bytes.Repeat(r.pixel[:], r.w*r.h)...)
	}
	_, err := w.Write(b)
	return err
}
