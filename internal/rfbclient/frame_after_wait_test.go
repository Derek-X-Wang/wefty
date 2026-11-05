package rfbclient_test

import (
	"bytes"
	"encoding/binary"
	"errors"
	"image"
	"image/color"
	"io"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/internal/rfbclient"
)

type pipeRequest struct {
	incremental   bool
	width, height int
	at            time.Time
}

// pipeServer is a real RFB peer over net.Pipe. Its handler runs on the read
// loop and may answer a request at once, later, or never.
type pipeServer struct {
	mu       sync.Mutex
	requests []pipeRequest
	conn     net.Conn
}

func (s *pipeServer) log() []pipeRequest {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]pipeRequest(nil), s.requests...)
}

func (s *pipeServer) write(b []byte) { _, _ = s.conn.Write(b) }

func openPipe(t *testing.T, handler func(s *pipeServer, request pipeRequest)) (*rfbclient.Client, net.Conn, *pipeServer) {
	t.Helper()
	client, server := net.Pipe()
	s := &pipeServer{conn: server}
	t.Cleanup(func() { _ = client.Close(); _ = server.Close() })
	go func() {
		read := func(n int) ([]byte, error) { p := make([]byte, n); _, err := io.ReadFull(server, p); return p, err }
		hs := handshake("003", 2, 2, 1)
		for _, step := range []struct {
			send []byte
			read int
		}{{hs[:12], 12}, {hs[12:16], 1}, {hs[16:], 0}} {
			if _, err := server.Write(step.send); err != nil {
				return
			}
			if _, err := read(step.read); err != nil {
				return
			}
		}
		for {
			kind, err := read(1)
			if err != nil {
				return
			}
			switch kind[0] {
			case 0:
				_, err = read(19)
			case 2:
				var header []byte
				if header, err = read(3); err == nil {
					_, err = read(4 * int(binary.BigEndian.Uint16(header[1:])))
				}
			case 3:
				var p []byte
				if p, err = read(9); err == nil {
					request := pipeRequest{incremental: p[0] == 1, width: int(binary.BigEndian.Uint16(p[5:])), height: int(binary.BigEndian.Uint16(p[7:])), at: time.Now()}
					s.mu.Lock()
					s.requests = append(s.requests, request)
					s.mu.Unlock()
					handler(s, request)
				}
			case 4:
				_, err = read(7)
			case 5:
				_, err = read(5)
			default:
				return
			}
			if err != nil {
				return
			}
		}
	}()
	c, err := rfbclient.Open(client)
	if err != nil {
		t.Fatal(err)
	}
	return c, client, s
}

var (
	pixelRed   = []byte{0, 0, 255, 0}
	pixelGreen = []byte{0, 255, 0, 0}
	pixelBlue  = []byte{255, 0, 0, 0}
	rgbaRed    = color.RGBA{255, 0, 0, 255}
	rgbaGreen  = color.RGBA{0, 255, 0, 255}
	rgbaBlue   = color.RGBA{0, 0, 255, 255}
)

func fullUpdate(pixel []byte) []byte {
	return update(rectangle(0, 0, 2, 2, 0, bytes.Repeat(pixel, 4)...))
}

// waitTimedOut runs the CLI's order: baseline, then a wait that times out with
// one incremental request held by the server.
func waitTimedOut(t *testing.T, c *rfbclient.Client) {
	t.Helper()
	if _, err := c.Frame(); err != nil {
		t.Fatal(err)
	}
	if seen, err := c.WaitForChange(30 * time.Millisecond); err != nil || seen {
		t.Fatalf("wait=%t err=%v", seen, err)
	}
}

// captureWithin returns FrameAfterWait's result, or closes the transport and
// fails when the call hangs. A hang is how discarding pending pixels shows.
func captureWithin(t *testing.T, c *rfbclient.Client, transport net.Conn, drained func()) (*image.RGBA, error) {
	t.Helper()
	type result struct {
		frame *image.RGBA
		err   error
	}
	done := make(chan result, 1)
	go func() { frame, err := c.FrameAfterWait(drained); done <- result{frame, err} }()
	select {
	case r := <-done:
		return r.frame, r.err
	case <-time.After(10 * time.Second): // Watchdog against a hang only.
		_ = transport.Close()
		<-done
		t.Fatal("FrameAfterWait did not return")
		return nil, nil
	}
}

func assertPixels(t *testing.T, frame *image.RGBA, want ...color.RGBA) {
	t.Helper()
	for i, w := range want {
		if got := frame.RGBAAt(i%2, i/2); got != w {
			t.Errorf("pixel %d=%v want %v", i, got, w)
		}
	}
}

// fullAfterHeldIncremental answers the baseline, holds incremental requests,
// and calls answer when the post-wait full request arrives.
func fullAfterHeldIncremental(answer func(s *pipeServer)) func(*pipeServer, pipeRequest) {
	full := 0
	return func(s *pipeServer, r pipeRequest) {
		if r.incremental {
			return
		}
		full++
		if full == 1 {
			s.write(fullUpdate(pixelRed))
			return
		}
		answer(s)
	}
}

func TestFrameAfterWaitMergedReply(t *testing.T) {
	// LibVNCServer unions the held incremental and the full request into one update.
	c, transport, s := openPipe(t, fullAfterHeldIncremental(func(s *pipeServer) { s.write(fullUpdate(pixelGreen)) }))
	waitTimedOut(t, c)
	timedOut := time.Now()
	drained := 0
	frame, err := captureWithin(t, c, transport, func() { drained++ })
	if err != nil {
		t.Fatal(err)
	}
	assertPixels(t, frame, rgbaGreen, rgbaGreen, rgbaGreen, rgbaGreen)
	requests := s.log()
	if drained != 1 || len(requests) != 3 || requests[0].incremental || !requests[1].incremental || requests[2].incremental || requests[2].width != 2 || requests[2].height != 2 {
		t.Fatalf("drained=%d requests=%+v", drained, requests)
	}
	// The full request follows the timeout; it is never sent during the wait.
	if requests[2].at.Before(timedOut) {
		t.Fatal("full request sent before the wait timed out")
	}
}

func TestFrameAfterWaitSeparateRepliesNewerPixelsWin(t *testing.T) {
	c, transport, _ := openPipe(t, fullAfterHeldIncremental(func(s *pipeServer) {
		// The incremental answer covers the top row; the full answer is split,
		// and its first rectangle overwrites one older incremental pixel.
		s.write(update(rectangle(0, 0, 2, 1, 0, append(append([]byte{}, pixelBlue...), pixelBlue...)...)))
		s.write(update(rectangle(0, 0, 1, 1, 0, pixelGreen...)))
		s.write(update(rectangle(0, 1, 2, 1, 0, append(append([]byte{}, pixelGreen...), pixelGreen...)...)))
	}))
	waitTimedOut(t, c)
	frame, err := captureWithin(t, c, transport, nil)
	if err != nil {
		t.Fatal(err)
	}
	assertPixels(t, frame, rgbaGreen, rgbaBlue, rgbaGreen, rgbaGreen)
}

func TestFrameAfterWaitIncrementalNeverAnswered(t *testing.T) {
	// neatvnc may answer the full request first and leave the incremental open.
	// The client sees the same bytes as a merged reply; the server state differs.
	c, transport, _ := openPipe(t, fullAfterHeldIncremental(func(s *pipeServer) { s.write(fullUpdate(pixelGreen)) }))
	waitTimedOut(t, c)
	drained := 0
	frame, err := captureWithin(t, c, transport, func() { drained++ })
	if err != nil || drained != 1 {
		t.Fatalf("drained=%d err=%v", drained, err)
	}
	assertPixels(t, frame, rgbaGreen, rgbaGreen, rgbaGreen, rgbaGreen)
}

func TestFrameAfterWaitEmptyUpdateFirst(t *testing.T) {
	c, transport, _ := openPipe(t, fullAfterHeldIncremental(func(s *pipeServer) {
		s.write(update())
		s.write(fullUpdate(pixelGreen))
	}))
	waitTimedOut(t, c)
	frame, err := captureWithin(t, c, transport, nil)
	if err != nil {
		t.Fatal(err)
	}
	assertPixels(t, frame, rgbaGreen, rgbaGreen, rgbaGreen, rgbaGreen)
}

func TestFrameAfterWaitWithoutOutstandingReadIsFrame(t *testing.T) {
	c, transport, s := openPipe(t, func(s *pipeServer, r pipeRequest) { s.write(fullUpdate(pixelGreen)) })
	drained := 0
	frame, err := captureWithin(t, c, transport, func() { drained++ })
	if err != nil {
		t.Fatal(err)
	}
	assertPixels(t, frame, rgbaGreen, rgbaGreen, rgbaGreen, rgbaGreen)
	if requests := s.log(); drained != 0 || len(requests) != 1 || requests[0].incremental {
		t.Fatalf("drained=%d requests=%+v", drained, requests)
	}
}

func TestFrameAfterWaitPendingReaderError(t *testing.T) {
	c, transport, _ := openPipe(t, fullAfterHeldIncremental(func(s *pipeServer) { s.write([]byte{99}) }))
	waitTimedOut(t, c)
	drained := 0
	if _, err := captureWithin(t, c, transport, func() { drained++ }); err == nil || !strings.Contains(err.Error(), "unsupported RFB message") || drained != 0 {
		t.Fatalf("drained=%d err=%v", drained, err)
	}
}

func TestFrameAfterWaitBlocksUntilTransportCloses(t *testing.T) {
	c, transport, _ := openPipe(t, fullAfterHeldIncremental(func(*pipeServer) {}))
	waitTimedOut(t, c)
	done := make(chan error, 1)
	go func() { _, err := c.FrameAfterWait(nil); done <- err }()
	select {
	case err := <-done:
		t.Fatalf("returned without a reply: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	_ = transport.Close()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("closed transport returned a frame")
		}
	case <-time.After(10 * time.Second): // Watchdog against a hang only.
		t.Fatal("close did not unblock FrameAfterWait")
	}
}

func TestFrameAfterWaitTimeoutDuringPacingPause(t *testing.T) {
	// The limit expires after the empty reply was consumed and the pacing
	// pause began, so no read is outstanding: the capture is a plain Frame.
	c, transport, s := openPipe(t, func(s *pipeServer, r pipeRequest) {
		if r.incremental {
			s.write(update())
			return
		}
		s.write(fullUpdate(pixelGreen))
	})
	limit := make(chan time.Time, 1)
	pauseStarted := make(chan struct{})
	timers := 0
	rfbclient.SetTimerForTest(c, func(time.Duration) (<-chan time.Time, func() bool) {
		timers++
		if timers == 1 {
			return limit, func() bool { return true }
		}
		if timers == 2 {
			close(pauseStarted) // The empty reply has been consumed.
		}
		return make(chan time.Time), func() bool { return true } // The pause never ends by itself.
	})
	waited := make(chan error, 1)
	go func() {
		seen, err := c.WaitForChange(time.Hour)
		if err == nil && seen {
			err = errors.New("empty reply counted as a change")
		}
		waited <- err
	}()
	select {
	case <-pauseStarted:
	case <-time.After(10 * time.Second):
		t.Fatal("pacing pause never began")
	}
	limit <- time.Now()
	select {
	case err := <-waited:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("WaitForChange ignored the limit during the pause")
	}
	drained := 0
	frame, err := captureWithin(t, c, transport, func() { drained++ })
	if err != nil {
		t.Fatal(err)
	}
	assertPixels(t, frame, rgbaGreen, rgbaGreen, rgbaGreen, rgbaGreen)
	requests := s.log()
	if drained != 0 || len(requests) != 2 || !requests[0].incremental || requests[1].incremental {
		t.Fatalf("drained=%d requests=%+v", drained, requests)
	}
}
