package rfbclient_test

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"image/color"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/internal/rfbclient"
)

func TestWaitForChangeSingleOutstandingRequest(t *testing.T) {
	w := &singleRequestWire{reply: bytes.NewReader(handshake("003", 2, 2, 1))}
	c, err := rfbclient.Open(w)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Frame(); err != nil {
		t.Fatal(err)
	}
	if changed, err := c.SyncAfterInput(color.RGBA{0, 255, 0, 255}); err != nil || changed {
		t.Fatalf("Sync changed=%t err=%v", changed, err)
	}
	// The limit never fires: the change after two empty replies always wins.
	// Pacing pauses use real timers.
	rfbclient.SetTimerForTest(c, func(d time.Duration) (<-chan time.Time, func() bool) {
		if d == time.Hour {
			return make(chan time.Time), func() bool { return true }
		}
		timer := time.NewTimer(d)
		return timer.C, timer.Stop
	})
	start := time.Now()
	seen, err := c.WaitForChange(time.Hour)
	if err != nil || !seen {
		t.Fatalf("wait=%t err=%v", seen, err)
	}
	if w.outstanding != 0 || w.maximum != 1 || w.incremental != 3 {
		t.Fatalf("outstanding=%d maximum=%d incremental=%d", w.outstanding, w.maximum, w.incremental)
	}
	if time.Since(start) < 35*time.Millisecond {
		t.Fatal("empty replies were not paced")
	}
	frame, err := c.Frame()
	if err != nil {
		t.Fatal(err)
	}
	if frame.RGBAAt(0, 0) != (color.RGBA{0, 255, 0, 255}) {
		t.Fatal("final frame lost update")
	}
	if w.outstanding != 0 || w.full != 2 || w.syncs != 1 || w.maximum != 1 {
		t.Fatalf("capture overlapped wait: outstanding=%d full=%d maximum=%d", w.outstanding, w.full, w.maximum)
	}
}

// The fake transport rejects a second request until every byte of the first
// reply has been consumed. Two empty replies precede the changed update.
type singleRequestWire struct {
	reply                                          *bytes.Reader
	outstanding, maximum, incremental, full, syncs int
}

func (w *singleRequestWire) Write(p []byte) (int, error) {
	if len(p) != 10 || p[0] != 3 {
		return len(p), nil
	}
	w.outstanding++
	w.maximum = max(w.maximum, w.outstanding)
	if w.outstanding > 1 {
		return 0, fmt.Errorf("second outstanding framebuffer request")
	}
	if p[1] == 1 {
		w.incremental++
		if w.incremental <= 2 {
			w.reply = bytes.NewReader(update())
			return len(p), nil
		}
	} else if binary.BigEndian.Uint16(p[6:]) == 1 && binary.BigEndian.Uint16(p[8:]) == 1 {
		w.syncs++
		w.reply = bytes.NewReader(update(rectangle(0, 0, 1, 1, 0, 0, 255, 0, 0)))
		return len(p), nil
	} else {
		w.full++
	}
	w.reply = bytes.NewReader(update(rectangle(0, 0, 2, 2, 0, bytes.Repeat([]byte{0, 255, 0, 0}, 4)...)))
	return len(p), nil
}
func (w *singleRequestWire) Read(p []byte) (int, error) {
	// Fragment updates so completion means all header and payload bytes, not
	// only the first byte or rectangle count, were consumed.
	if len(p) > 1 {
		p = p[:1]
	}
	n, err := w.reply.Read(p)
	if w.reply.Len() == 0 && w.outstanding > 0 {
		w.outstanding--
	}
	return n, err
}

func TestWaitForChangeTimeoutRefusesPlainFrame(t *testing.T) {
	w := &pendingWaitWire{Reader: bytes.NewReader(handshake("003", 2, 2, 1)), closed: make(chan struct{})}
	defer close(w.closed)
	c, err := rfbclient.Open(w)
	if err != nil {
		t.Fatal(err)
	}
	if seen, err := c.WaitForChange(20 * time.Millisecond); err != nil || seen {
		t.Fatalf("wait=%t err=%v", seen, err)
	}
	before := w.written.Len()
	if _, err := c.Frame(); err == nil || !strings.Contains(err.Error(), "FrameAfterWait") {
		t.Fatalf("capture accepted pending reply: %v", err)
	}
	if w.written.Len() != before {
		t.Fatal("capture wrote on timed-out stream")
	}
}

func TestWaitForChangeRejectsMalformedUpdates(t *testing.T) {
	for _, message := range [][]byte{
		update(rectangle(0, 0, 0, 1, 0)), update(rectangle(2, 0, 1, 1, 0)), update(rectangle(0, 0, 1, 1, 1)), {99},
	} {
		w := &wire{Reader: bytes.NewReader(append(handshake("003", 2, 2, 1), message...))}
		c, err := rfbclient.Open(w)
		if err != nil {
			t.Fatal(err)
		}
		seen, err := c.WaitForChange(time.Second)
		if err == nil || seen {
			t.Fatalf("malformed update accepted: seen=%t err=%v", seen, err)
		}
	}
}

func TestWaitForChangeRejectsPendingReader(t *testing.T) {
	w := &pendingWaitWire{Reader: bytes.NewReader(handshake("003", 2, 2, 1)), closed: make(chan struct{})}
	defer close(w.closed)
	c, err := rfbclient.Open(w)
	if err != nil {
		t.Fatal(err)
	}
	if seen, err := c.WaitForChange(20 * time.Millisecond); err != nil || seen {
		t.Fatalf("first wait=%t err=%v", seen, err)
	}
	before := w.written.Len()
	if seen, err := c.WaitForChange(20 * time.Millisecond); err == nil || seen || !strings.Contains(err.Error(), "pending framebuffer read") {
		t.Fatalf("second wait=%t err=%v", seen, err)
	}
	if w.written.Len() != before {
		t.Fatal("second wait wrote another request")
	}
}

type pendingWaitWire struct {
	*bytes.Reader
	written bytes.Buffer
	closed  chan struct{}
}

func (w *pendingWaitWire) Read(p []byte) (int, error) {
	if w.Reader.Len() > 0 {
		return w.Reader.Read(p)
	}
	<-w.closed
	return 0, io.EOF
}
func (w *pendingWaitWire) Write(p []byte) (int, error) { return w.written.Write(p) }

func TestSyncAfterInputObservesEntireReply(t *testing.T) {
	red := []byte{0, 0, 255, 0}
	green := []byte{0, 255, 0, 0}
	for _, tc := range []struct {
		name    string
		reply   []byte
		changed bool
	}{
		{"unchanged", update(rectangle(0, 0, 1, 1, 0, red...)), false},
		{"pixel", update(rectangle(0, 0, 1, 1, 0, green...)), true},
		{"extra-after-pixel", update(rectangle(0, 0, 1, 1, 0, red...), rectangle(1, 1, 1, 1, 0, green...)), true},
		{"expanded", update(rectangle(0, 0, 2, 2, 0, bytes.Repeat(red, 4)...)), true},
		{"outside-before-coverage", append(update(rectangle(1, 1, 1, 1, 0, green...)), update(rectangle(0, 0, 1, 1, 0, red...))...), true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := append(handshake("003", 2, 2, 1), tc.reply...)
			b = append(b, update(rectangle(0, 0, 2, 2, 0, bytes.Repeat(green, 4)...))...)
			w := &wire{Reader: oneByteReader{bytes.NewReader(b)}}
			c, err := rfbclient.Open(w)
			if err != nil {
				t.Fatal(err)
			}
			changed, err := c.SyncAfterInput(color.RGBA{255, 0, 0, 255})
			if err != nil || changed != tc.changed {
				t.Fatalf("Sync changed=%t want=%t err=%v", changed, tc.changed, err)
			}
			frame, err := c.Frame()
			if err != nil {
				t.Fatal(err)
			}
			if frame.RGBAAt(0, 0) != (color.RGBA{0, 255, 0, 255}) || frame.RGBAAt(1, 1) != (color.RGBA{0, 255, 0, 255}) {
				t.Fatal("Sync did not drain entire reply")
			}
		})
	}
}
