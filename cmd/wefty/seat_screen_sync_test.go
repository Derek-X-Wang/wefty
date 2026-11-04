package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"image/png"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/fabric/plain"
	"github.com/coder/websocket"
)

func TestSeatScreenSyncDeliversQueuedInputBeforeClose(t *testing.T) {
	t.Setenv("WEFTY_SEAT_SCREEN_TIMING", "1")
	t.Setenv("WEFTY_SEAT_STATE_ROOT", filepath.Join(t.TempDir(), "seats"))
	id := "seat_" + strings.Repeat("9", 32)
	if _, err := storeSeatCredential(id, "proxy-secret"); err != nil {
		t.Fatal(err)
	}
	var delivered, syncs, incremental, views atomic.Int32
	var closedBeforeDelivery atomic.Bool
	inputClosed := make(chan struct{})
	address := ""
	address = startStubLedger(t, func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/screen-target") {
			_ = json.NewEncoder(w).Encode(map[string]any{"endpoint": "ws://" + address})
			return
		}
		isView := strings.HasSuffix(r.URL.Path, "/view")
		ws, err := websocket.Accept(w, r, &websocket.AcceptOptions{Subprotocols: []string{"binary"}})
		if err != nil {
			return
		}
		defer ws.CloseNow()
		ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
		defer cancel()
		front := websocket.NetConn(ctx, ws, websocket.MessageBinary)
		if isView {
			views.Add(1)
			_ = screenFakeRFBRespond(front, func([]byte) { t.Error("input repeated on view") }, func([]byte) error { return settleUpdate(front, 1280, 800, true) })
			return
		}
		// A real byte-forwarding boundary: the VNC fake is on a separate stream.
		// A disconnect cancels queued client-to-VNC bytes, just as a closing proxy
		// can do. The framebuffer replies themselves are never artificially delayed.
		backend, server := net.Pipe()
		proxyCtx, stop := context.WithCancel(ctx)
		var work sync.WaitGroup
		work.Add(4)
		go func() {
			defer work.Done()
			defer stop()
			_ = screenFakeRFBRespond(server, func([]byte) { delivered.Add(1) }, func(p []byte) error {
				width, height := int(binary.BigEndian.Uint16(p[5:])), int(binary.BigEndian.Uint16(p[7:]))
				if p[0] == 1 {
					incremental.Add(1)
					return nil
				}
				if width == 1 && height == 1 {
					syncs.Add(1)
				}
				return settleUpdate(server, width, height, false)
			})
		}()
		go func() { defer work.Done(); defer stop(); _, _ = io.Copy(front, backend) }()
		queue := make(chan []byte, 16)
		go func() {
			defer work.Done()
			defer stop()
			// Forward the RFB handshake, format, encoding, and baseline request.
			for _, n := range []int{12, 1, 1, 20, 8, 10} {
				p := make([]byte, n)
				if _, err := io.ReadFull(front, p); err != nil {
					return
				}
				if _, err := backend.Write(p); err != nil {
					return
				}
			}
			for {
				p := make([]byte, 4096)
				n, err := front.Read(p)
				if n > 0 {
					select {
					case queue <- p[:n]:
					case <-proxyCtx.Done():
						return
					}
				}
				if err != nil {
					return
				}
			}
		}()
		go func() {
			defer work.Done()
			defer stop()
			var first []byte
			select {
			case first = <-queue:
			case <-proxyCtx.Done():
				return
			}
			delay := time.NewTimer(1200 * time.Millisecond)
			defer delay.Stop()
			select {
			case <-delay.C:
			case <-proxyCtx.Done():
				return
			}
			if _, err := backend.Write(first); err != nil {
				return
			}
			for {
				select {
				case p := <-queue:
					if _, err := backend.Write(p); err != nil {
						return
					}
				case <-proxyCtx.Done():
					return
				}
			}
		}()
		<-proxyCtx.Done()
		closedBeforeDelivery.Store(delivered.Load() != 2)
		_ = backend.Close()
		_ = server.Close()
		_ = ws.CloseNow()
		work.Wait()
		close(inputClosed)
	})
	clients, err := newAPIClients(plain.NewNetwork().NewFabric(fabric.Identity{NodeID: "screen-client"}), address, "")
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "capture.png")
	var out bytes.Buffer
	start := time.Now()
	if err := execute(t.Context(), clients, true, []string{"seat", "click", id, "1", "2", "--screenshot", path}, &out, io.Discard); err != nil {
		t.Fatal(err)
	}
	elapsed := time.Since(start)
	select {
	case <-inputClosed:
	case <-time.After(time.Second):
		t.Fatal("input connection did not close")
	}
	if closedBeforeDelivery.Load() || delivered.Load() != 2 || syncs.Load() != 1 {
		t.Errorf("input lost before close: delivered=%d Sync=%d closed early=%t", delivered.Load(), syncs.Load(), closedBeforeDelivery.Load())
	}
	if incremental.Load() != 0 {
		t.Errorf("Sync exhausted window but incremental requests=%d", incremental.Load())
	}
	if views.Load() != 1 {
		t.Errorf("view connections=%d", views.Load())
	}
	if elapsed < 1150*time.Millisecond || elapsed > 1900*time.Millisecond {
		t.Errorf("wrong Sync/window latency: %s", elapsed)
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
	if reply.After == nil || *reply.After || p["sync_done"]-p["input_written"] < 1150 || p["sync_done"] > p["frame_update_timeout"] || p["capture_reopened"] < p["frame_update_timeout"] {
		t.Errorf("wrong ordering: %s", out.String())
	}
	t.Logf("call=%s Sync=%.3fms", elapsed, p["sync_done"]-p["input_written"])
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if _, err := png.Decode(f); err != nil {
		t.Fatal(err)
	}
}

func TestSeatScreenSyncReportsChanges(t *testing.T) {
	for _, behavior := range []string{"neatvnc-extra", "libvncserver-incremental", "sync-pixel"} {
		t.Run(behavior, func(t *testing.T) {
			t.Setenv("WEFTY_SEAT_SCREEN_TIMING", "1")
			t.Setenv("WEFTY_SEAT_STATE_ROOT", filepath.Join(t.TempDir(), "seats"))
			id := "seat_" + strings.Repeat("8", 32)
			if _, err := storeSeatCredential(id, "sync-secret"); err != nil {
				t.Fatal(err)
			}
			var syncs, incremental, events, views, full atomic.Int32
			address := ""
			address = startStubLedger(t, func(w http.ResponseWriter, r *http.Request) {
				if strings.HasSuffix(r.URL.Path, "/screen-target") {
					_ = json.NewEncoder(w).Encode(map[string]any{"endpoint": "ws://" + address})
					return
				}
				if strings.HasSuffix(r.URL.Path, "/view") {
					views.Add(1)
					w.WriteHeader(500)
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
				_ = screenFakeRFBRespond(conn, func([]byte) { events.Add(1) }, func(p []byte) error {
					width, height := int(binary.BigEndian.Uint16(p[5:])), int(binary.BigEndian.Uint16(p[7:]))
					if p[0] == 1 {
						incremental.Add(1)
						if syncs.Load() != 1 {
							t.Error("incremental request preceded Sync")
						}
						if behavior != "libvncserver-incremental" {
							return nil
						} // Sync already flushed the damage.
						changed = true
						var reply bytes.Buffer
						if err := settleUpdate(&reply, 1, 1, true); err != nil {
							return err
						}
						b := reply.Bytes()
						binary.BigEndian.PutUint16(b[4:], 10)
						binary.BigEndian.PutUint16(b[6:], 10)
						_, err := conn.Write(b)
						return err
					}
					if width == 1 && height == 1 {
						syncs.Add(1)
						switch behavior {
						case "neatvnc-extra":
							changed = true
							var requested, extra bytes.Buffer
							if err := settleUpdate(&requested, 1, 1, false); err != nil {
								return err
							}
							if err := settleUpdate(&extra, 1, 1, true); err != nil {
								return err
							}
							b := requested.Bytes()
							b[3] = 2
							more := extra.Bytes()
							binary.BigEndian.PutUint16(more[4:], 10)
							binary.BigEndian.PutUint16(more[6:], 10)
							_, err := conn.Write(append(b, more[4:]...))
							return err
						case "sync-pixel":
							changed = true
							return settleUpdate(conn, 1, 1, true)
						default:
							return settleUpdate(conn, 1, 1, false)
						}
					}
					full.Add(1)
					return settleUpdate(conn, width, height, changed)
				})
			})
			clients, err := newAPIClients(plain.NewNetwork().NewFabric(fabric.Identity{NodeID: "screen-client"}), address, "")
			if err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(t.TempDir(), "capture.png")
			var out bytes.Buffer
			if err := execute(t.Context(), clients, true, []string{"seat", "click", id, "1", "2", "--screenshot", path}, &out, io.Discard); err != nil {
				t.Fatal(err)
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
			wantIncremental := int32(0)
			if behavior == "libvncserver-incremental" {
				wantIncremental = 1
			}
			if reply.After == nil || !*reply.After || syncs.Load() != 1 || incremental.Load() != wantIncremental || full.Load() != 2 || views.Load() > wantIncremental || events.Load() != 2 {
				t.Errorf("wrong Sync behavior: reply=%s Sync=%d incremental=%d full=%d views=%d input=%d", out.String(), syncs.Load(), incremental.Load(), full.Load(), views.Load(), events.Load())
			}
			phases := reply.Debug.Phases
			syncDone, hasSync := phases["sync_done"]
			observed, hasObserved := phases["frame_update_seen"]
			if !hasSync || !hasObserved || syncDone < phases["input_written"] || syncDone > observed || observed > phases["frame_read"] {
				t.Errorf("wrong Sync timing: %v", phases)
			}
			t.Logf("Sync=%.3fms", syncDone-phases["input_written"])
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
				t.Errorf("stale final frame: red=%d green=%d", red, green)
			}
		})
	}
}
