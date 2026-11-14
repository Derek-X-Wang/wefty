package main

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/coder/websocket"
)

// seatScreenReceiverPair starts a node that sends the given binary messages,
// then holds the stream open until the client goes away. It returns the
// client's receiver.
func seatScreenReceiverPair(t *testing.T, limit int, messages ...[]byte) *seatScreenReceiver {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ws, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer ws.CloseNow()
		for _, m := range messages {
			if ws.Write(r.Context(), websocket.MessageBinary, m) != nil {
				return
			}
		}
		_, _, _ = ws.Read(r.Context())
	}))
	t.Cleanup(server.Close)
	c, _, err := websocket.Dial(t.Context(), "ws"+strings.TrimPrefix(server.URL, "http"), nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.CloseNow() })
	_ = websocket.NetConn(t.Context(), c, websocket.MessageBinary) // removes the read limit, as in production
	return receiveSeatScreen(t.Context(), c, limit)
}

func waitSeatScreenReceiver(t *testing.T, r *seatScreenReceiver) {
	t.Helper()
	select {
	case <-r.done:
	case <-time.After(10 * time.Second):
		t.Fatal("receiver did not end")
	}
}

// R2 on #1680: the receiver bounds one message and the bytes it holds ahead
// of the RFB client, and ends the stream with an error past either bound.
func TestSeatScreenReceiverBoundsMemory(t *testing.T) {
	t.Run("oversized message", func(t *testing.T) {
		r := seatScreenReceiverPair(t, 1024, make([]byte, 2048))
		waitSeatScreenReceiver(t, r)
		if _, err := r.Read(make([]byte, 4096)); err == nil || websocket.CloseStatus(err) != websocket.StatusMessageTooBig && !strings.Contains(err.Error(), "read limited") {
			t.Fatalf("oversized message read = %v, want the read limit error", err)
		}
	})
	t.Run("queue growth", func(t *testing.T) {
		chunk := make([]byte, 600)
		r := seatScreenReceiverPair(t, 1024, chunk, chunk, chunk)
		waitSeatScreenReceiver(t, r)
		r.mu.Lock()
		held := r.buf.Len()
		r.mu.Unlock()
		if held > 1024 {
			t.Fatalf("receiver holds %d bytes past its 1024-byte limit", held)
		}
		got, err := io.ReadAll(r)
		if len(got) != 600 || err == nil || !strings.Contains(err.Error(), "more than 1024 bytes ahead of the client") {
			t.Fatalf("read %d bytes, err=%v; want 600 bytes then the overflow error", len(got), err)
		}
	})
}

// R2 on #1680: a received take close names only stream failures. A local
// output failure keeps its own error even after the close arrived.
func TestSeatScreenTakeFailureOnlyForStreamErrors(t *testing.T) {
	ended := func(err error) *seatScreenReceiver {
		r := &seatScreenReceiver{done: make(chan struct{}), err: err}
		close(r.done)
		return r
	}
	taken := ended(fmt.Errorf("failed to get reader: %w", websocket.CloseError{Code: contract.SeatInputPersonDrivingClose, Reason: "person_driving"}))
	broken := fmt.Errorf("click: %w", &seatScreenStreamError{errors.New("write: broken pipe")})
	disk := errors.New("write PNG: no space left on device")
	for _, test := range []struct {
		name     string
		err      error
		receiver *seatScreenReceiver
		driving  bool
	}{
		{"stream write after the close", broken, taken, true},
		{"the close itself", taken.err, taken, true},
		{"local output failure after the close", disk, taken, false},
		{"stream write without a take", broken, ended(io.EOF), false},
		{"no receiver", disk, nil, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			got := seatScreenTakeFailure(test.err, test.receiver)
			var api *apiResponseError
			driving := errors.As(got, &api) && api.APIError.Code == seatErrorPersonDriving
			if driving != test.driving || !driving && got != test.err {
				t.Fatalf("seatScreenTakeFailure(%v) = %v, want person_driving=%t", test.err, got, test.driving)
			}
		})
	}
}
