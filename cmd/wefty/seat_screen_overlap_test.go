package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"math"
	"net"
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
	"github.com/coder/websocket"
)

// These tests use the CLI and wire protocol boundary. Handshakes park at known
// seams so cancellation and reply ownership do not depend on scheduling sleeps.
func TestSeatScreenOverlap(t *testing.T) {
	for _, scenario := range []string{"timeout", "change", "change-slow-handshake", "change-late-dial", "change-late-open", "change-refused", "timeout-refused", "change-invalid-note", "timeout-invalid-note", "settle-error", "sync-error", "sync-change", "parent-cancel"} {
		t.Run(scenario, func(t *testing.T) {
			t.Setenv("WEFTY_SEAT_SCREEN_TIMING", "1")
			t.Setenv("WEFTY_SEAT_STATE_ROOT", filepath.Join(t.TempDir(), "seats"))
			id := "seat_" + strings.Repeat("7", 32)
			if _, err := storeSeatCredential(id, "overlap-secret"); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			producerOpened, producerDone := make(chan struct{}), make(chan struct{})
			releaseOpen := make(chan struct{})
			var inputMarks, spareMarks map[string]time.Time
			ctx = context.WithValue(ctx, seatScreenSpareTestHookKey{}, func(event string, result seatScreenOpenResult) {
				switch event {
				case "input_opened":
					inputMarks = result.marks
				case "spare_opened":
					spareMarks = result.marks
					close(producerOpened)
					if scenario == "change-late-open" {
						<-releaseOpen
					}
				case "spare_finished":
					close(producerDone)
				}
			})
			viewReady := make(chan struct{})
			viewStarted, viewClosed, inputClosed := make(chan struct{}), make(chan struct{}), make(chan struct{})
			var views, live, inputFull, viewFull atomic.Int32
			var written atomic.Int64
			var earlyView, overlapped atomic.Bool
			changing := strings.HasPrefix(scenario, "change")
			address := "screen.test:80"
			participant, connections := seatOverlapServer(t, func(w http.ResponseWriter, r *http.Request) {
				if strings.HasSuffix(r.URL.Path, "/screen-target") {
					_ = json.NewEncoder(w).Encode(map[string]any{"endpoint": "ws://" + address})
					return
				}
				isView := strings.HasSuffix(r.URL.Path, "/view")
				live.Add(1)
				if isView {
					views.Add(1)
					overlapped.Store(time.Since(time.Unix(0, written.Load())) < seatScreenSettleLimit)
					// Signal before any handshake; the input server can now deliver damage.
					close(viewStarted)
					defer func() { live.Add(-1); close(viewClosed) }()
					if strings.HasSuffix(scenario, "refused") {
						w.WriteHeader(500)
						_ = json.NewEncoder(w).Encode(contract.ErrorResponse{Error: contract.APIError{Code: contract.ErrorInternal, Message: "spare refused", Retryable: true}})
						return
					}
					w.Header().Set("Wefty-Seat-Unread-Events", "9")
				} else {
					defer func() { live.Add(-1); close(inputClosed) }()
					w.Header().Set("Wefty-Seat-Unread-Events", "2")
				}
				note := base64.RawURLEncoding.EncodeToString([]byte("One overlap note."))
				if isView && strings.HasSuffix(scenario, "invalid-note") {
					note = "!"
				}
				w.Header().Set("Wefty-Seat-Release-Note", note)
				ws, err := websocket.Accept(w, r, &websocket.AcceptOptions{Subprotocols: []string{"binary"}})
				if err != nil {
					return
				}
				defer ws.CloseNow()
				serverCtx, stop := context.WithTimeout(r.Context(), 3*time.Second)
				defer stop()
				conn := websocket.NetConn(serverCtx, ws, websocket.MessageBinary)
				if isView && scenario == "change-slow-handshake" {
					// No banner: cancellation must finish this handshake without waiting.
					_, _ = io.Copy(io.Discard, conn)
					return
				}
				var wire io.ReadWriter = conn
				if isView {
					wire = &seatOverlapReadyWire{ReadWriter: conn, remaining: 42, ready: viewReady}
				}
				_ = screenFakeRFBRespond(wire, func([]byte) { written.CompareAndSwap(0, time.Now().UnixNano()) }, func(p []byte) error {
					width, height := int(binary.BigEndian.Uint16(p[5:])), int(binary.BigEndian.Uint16(p[7:]))
					if isView {
						viewFull.Add(1)
						if p[0] != 0 || time.Since(time.Unix(0, written.Load())) < seatScreenSettleLimit {
							earlyView.Store(true)
						}
						select {
						case <-inputClosed:
						case <-time.After(time.Second):
							t.Error("spare capture preceded input close")
						}
						return settleUpdate(conn, width, height, true)
					}
					if width == 1 && height == 1 {
						if scenario == "sync-error" {
							return ws.CloseNow()
						}
						return settleUpdate(conn, 1, 1, scenario == "sync-change")
					}
					if p[0] == 1 {
						if changing || scenario == "settle-error" || scenario == "parent-cancel" {
							ready := viewStarted
							if scenario == "change" || scenario == "change-late-open" || scenario == "settle-error" || scenario == "parent-cancel" {
								ready = producerOpened
							}
							select {
							case <-ready:
							case <-time.After(120 * time.Millisecond):
								t.Error("spare was not dialed during settle wait")
							}
							if scenario == "settle-error" {
								return ws.CloseNow()
							}
							if scenario == "parent-cancel" {
								cancel()
								return nil
							}
							return settleUpdate(conn, 1, 1, true)
						}
						return nil // Leave one incremental request pending.
					}
					inputFull.Add(1)
					if inputFull.Load() > 1 && changing && !strings.HasSuffix(scenario, "refused") {
						// Capture must proceed only after cancelling the unused spare, even
						// when its RFB handshake never completes.
						select {
						case <-viewClosed:
							if scenario == "change-late-open" {
								close(releaseOpen)
							}
						case <-time.After(time.Second):
							t.Error("spare was not cancelled on change")
						}
					}
					return settleUpdate(conn, width, height, inputFull.Load() > 1)
				})
			})
			if scenario == "change-late-dial" {
				participant = &seatOverlapLateFabric{Fabric: participant, closed: viewClosed, started: func() {
					views.Add(1)
					overlapped.Store(time.Since(time.Unix(0, written.Load())) < seatScreenSettleLimit)
					close(viewStarted)
				}}
			}
			clients, err := newAPIClients(participant, address, "")
			if err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(t.TempDir(), "capture.png")
			var out, stderr bytes.Buffer
			runErr := execute(ctx, clients, true, []string{"seat", "click", id, "1", "2", "--screenshot", path}, &out, &stderr)
			failure := scenario == "timeout-refused" || scenario == "timeout-invalid-note" || scenario == "settle-error" || scenario == "sync-error" || scenario == "parent-cancel"
			if failure {
				var api *apiResponseError
				if !errors.As(runErr, &api) || api.APIError.Details["action_done"] != true || api.APIError.Retryable {
					t.Errorf("lost completed action: %v", runErr)
				}
				if _, err := os.Stat(path); !os.IsNotExist(err) {
					t.Errorf("failed capture retained PNG: %v", err)
				}
				if viewFull.Load() != 0 {
					t.Error("failed settle captured on spare")
				}
			} else {
				if runErr != nil {
					t.Fatal(runErr)
				}
				var reply struct {
					After  bool `json:"frame_after_input"`
					Unread int  `json:"unread_events"`
					Debug  struct {
						Start  int64              `json:"screen_timing_start_unix_ns"`
						Phases map[string]float64 `json:"screen_timing_ms"`
					} `json:"debug"`
				}
				if err := json.Unmarshal(out.Bytes(), &reply); err != nil {
					t.Fatal(err)
				}
				wantUnread := 2
				if scenario == "timeout" {
					wantUnread = 9
				}
				if changing {
					for name, at := range inputMarks {
						expected := float64(at.Sub(time.Unix(0, reply.Debug.Start)).Microseconds()) / 1000
						// JSON exposes a wall-clock origin; permit two microseconds of
						// rounding versus the monotonic timestamps used by markAt.
						if math.Abs(reply.Debug.Phases[name]-expected) > 0.002 {
							t.Errorf("discarded spare changed %s: got %v want input %v", name, reply.Debug.Phases[name], expected)
						}
					}
					if scenario == "change" || scenario == "change-late-open" {
						for name, at := range spareMarks {
							discarded := float64(at.Sub(time.Unix(0, reply.Debug.Start)).Microseconds()) / 1000
							if math.Abs(reply.Debug.Phases[name]-discarded) <= 0.002 {
								t.Errorf("discarded spare timing applied for %s", name)
							}
						}
					}
					if _, ok := reply.Debug.Phases["capture_reopened"]; ok {
						t.Error("discarded spare added capture_reopened")
					}
				}
				if reply.Unread != wantUnread || reply.After != (changing || scenario == "sync-change") {
					t.Errorf("wrong reply: %s", out.String())
				}
			}
			if strings.Count(stderr.String(), "One overlap note.") != 1 {
				t.Errorf("note was not printed once: %q", stderr.String())
			}
			wantViews := int32(1)
			if scenario == "sync-error" || scenario == "sync-change" {
				wantViews = 0
			}
			if views.Load() != wantViews {
				t.Errorf("spare dials=%d want %d", views.Load(), wantViews)
			}
			if wantViews > 0 && !overlapped.Load() {
				t.Error("spare dial did not overlap settle wait")
			}
			if earlyView.Load() {
				t.Error("full request on spare before settle limit")
			}
			wantInputFull := int32(1)
			if changing || scenario == "sync-change" {
				wantInputFull = 2
			}
			if inputFull.Load() != wantInputFull {
				t.Errorf("input full requests=%d want %d", inputFull.Load(), wantInputFull)
			}
			if scenario == "timeout" && viewFull.Load() != 1 {
				t.Error("timeout did not capture on spare")
			}
			for _, done := range []chan struct{}{inputClosed, viewClosed} {
				if done == viewClosed && wantViews == 0 {
					continue
				}
				select {
				case <-done:
				case <-time.After(time.Second):
					t.Error("fake connection or handshake goroutine leaked")
				}
			}
			if wantViews > 0 {
				select {
				case <-producerDone:
				case <-time.After(time.Second):
					t.Error("spare producer did not finish")
				}
				if (scenario == "change" || scenario == "change-late-open") && len(spareMarks) == 0 {
					t.Error("discarded successful spare did not return timing metadata")
				}
			}
			clients.l1.client.CloseIdleConnections()
			if live.Load() != 0 || connections.Load() != 0 {
				t.Errorf("open fake streams=%d connections=%d", live.Load(), connections.Load())
			}
		})
	}
}

// In-memory HTTP/WebSocket transport exercises real protocol code without
// requiring a loopback listener. Each accepted pipe has the same close duties.
type seatOverlapListener struct {
	connections chan net.Conn
	done        chan struct{}
	once        sync.Once
}

func (l *seatOverlapListener) Accept() (net.Conn, error) {
	select {
	case c := <-l.connections:
		return c, nil
	case <-l.done:
		return nil, net.ErrClosed
	}
}
func (l *seatOverlapListener) Close() error   { l.once.Do(func() { close(l.done) }); return nil }
func (l *seatOverlapListener) Addr() net.Addr { return seatOverlapAddress("screen.test:80") }

type seatOverlapAddress string

func (a seatOverlapAddress) Network() string { return "pipe" }
func (a seatOverlapAddress) String() string  { return string(a) }

type seatOverlapFabric struct {
	fabric.Fabric
	listener *seatOverlapListener
	active   *atomic.Int32
}

func (f seatOverlapFabric) Dial(ctx context.Context, _, _ string) (net.Conn, error) {
	clientPipe, serverPipe := net.Pipe()
	f.active.Add(1)
	var once sync.Once
	closed := func() { once.Do(func() { f.active.Add(-1) }) }
	client := &seatOverlapTrackedConn{Conn: clientPipe, closed: closed}
	server := &seatOverlapTrackedConn{Conn: serverPipe, closed: closed}
	select {
	case f.listener.connections <- server:
		return client, nil
	case <-ctx.Done():
		client.Close()
		server.Close()
		return nil, ctx.Err()
	case <-f.listener.done:
		client.Close()
		server.Close()
		return nil, net.ErrClosed
	}
}
func seatOverlapServer(t *testing.T, handler http.HandlerFunc) (fabric.Fabric, *atomic.Int32) {
	t.Helper()
	listener := &seatOverlapListener{connections: make(chan net.Conn), done: make(chan struct{})}
	server := &http.Server{Handler: handler}
	done := make(chan struct{})
	go func() { defer close(done); _ = server.Serve(listener) }()
	t.Cleanup(func() { _ = server.Close(); <-done })
	active := &atomic.Int32{}
	return seatOverlapFabric{listener: listener, active: active}, active
}

// RFB client setup ends after banner/security/ClientInit, pixel format and
// encodings (42 client bytes). No framebuffer request is needed for readiness.
type seatOverlapReadyWire struct {
	io.ReadWriter
	remaining int
	ready     chan struct{}
}

func (w *seatOverlapReadyWire) Read(p []byte) (int, error) {
	n, err := w.ReadWriter.Read(p)
	if w.remaining > 0 {
		w.remaining -= n
		if w.remaining <= 0 {
			close(w.ready)
		}
	}
	return n, err
}

// Return a successful raw Dial only after cancellation. HTTP must reclaim the
// connection even though its caller has already returned from the round trip.
type seatOverlapLateFabric struct {
	fabric.Fabric
	dials   atomic.Int32
	started func()
	closed  chan struct{}
}

func (f *seatOverlapLateFabric) Dial(ctx context.Context, network, address string) (net.Conn, error) {
	conn, err := f.Fabric.Dial(ctx, network, address)
	if err != nil || f.dials.Add(1) != 3 {
		return conn, err
	}
	f.started()
	<-ctx.Done()
	return &seatOverlapCloseConn{Conn: conn, closed: f.closed}, nil
}

type seatOverlapCloseConn struct {
	net.Conn
	once   sync.Once
	closed chan struct{}
}

func (c *seatOverlapCloseConn) Close() error {
	err := c.Conn.Close()
	c.once.Do(func() { close(c.closed) })
	return err
}

type seatOverlapTrackedConn struct {
	net.Conn
	closed func()
}

func (c *seatOverlapTrackedConn) Close() error {
	err := c.Conn.Close()
	c.closed()
	return err
}
