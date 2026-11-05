package computerconformance

import (
	"encoding/binary"
	"errors"
	"fmt"
	"image"
	"image/color"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/internal/rfbclient"
)

// The Seat screen lane's idle-wake SC-1 case (#1495). These names are
// exported only to that lane, which is in the external test package.
//
// x11vnc 0.9.16 naps between screen polls for up to 4 s once it has sent no
// framebuffer data for 60 s (scan.c nap_check, -sb), and it notes that time
// only every 2 s (screen.c record_last_fb_update). Only user input ends a nap
// early (#1479). 70 s without input or framebuffer data reaches that state
// with margin; the observer must have watched a static screen for 65 s of it.
const (
	IdleWakeQuiet       = 70 * time.Second
	IdleWakeObservedMin = 65 * time.Second
	// The gate is the baseline frame read on the input connection
	// (rfb_banner to frame_baseline), the phase an x11vnc nap delays. #1479
	// measured 15-130 ms with -nonap -sb 0 and 1025-1868 ms without.
	IdleWakeBaselineGate = 500 * time.Millisecond
	// x11vnc answers a held incremental request with a 1x1 heartbeat about
	// every 5 s on an unchanged screen (scan.c ping_clients).
	IdleWakePendingLimit = 10 * time.Second
)

// IdleWakeObservation is what the lane saw before it timed the idle-wake call.
type IdleWakeObservation struct {
	SinceInput time.Duration   // from the test's last input to the timed call
	FirstFrame time.Time       // when the observer's first full frame was complete
	Observed   time.Duration   // first full frame to the last full check, all static
	Updates    int             // incremental updates compared in between
	Changed    int             // differing pixels in the first update that changed
	ChangedBox image.Rectangle // where they are
	ChangedAt  time.Duration   // when that update arrived, after the first frame
	Err        error           // the observer failed
}

// Premise returns why the timed call would not measure an idle server, or
// nil. A failed premise is a lane failure, never a warm measurement.
func (o IdleWakeObservation) Premise() error {
	switch {
	case o.Err != nil:
		return fmt.Errorf("idle premise not met: screen observer failed: %w", o.Err)
	case o.Changed > 0:
		return fmt.Errorf("idle premise not met: %d pixels in %v changed %.1f s after the first frame", o.Changed, o.ChangedBox, o.ChangedAt.Seconds())
	case o.Observed < IdleWakeObservedMin:
		return fmt.Errorf("idle premise not met: observed a static screen for only %.1f s, need %.0f s", o.Observed.Seconds(), IdleWakeObservedMin.Seconds())
	case o.SinceInput < IdleWakeQuiet:
		return fmt.Errorf("idle premise not met: %.1f s since the last input, need %.0f s", o.SinceInput.Seconds(), IdleWakeQuiet.Seconds())
	}
	return nil
}

// ObserveIdleScreen watches one view-role RFB connection until `until`. It
// takes a full first frame with rfbclient, then reads every later update
// itself and compares each update's pixels with the first frame as it
// arrives, so a change that reverts before a later frame still counts.
// rfbclient reports only whether an update arrived, and its frame captures
// fold updates together, so neither can show this. x11vnc's 1x1 heartbeat on
// an unchanged screen passes because its pixels match. At `until` it waits
// up to IdleWakePendingLimit for the reply to its outstanding request, then,
// with nothing outstanding, asks for the full screen and compares every
// update until all pixels are covered. The caller closes wire afterwards;
// that also bounds every read.
func ObserveIdleScreen(wire io.ReadWriter, until time.Time) IdleWakeObservation {
	return observeIdleScreen(wire, until, IdleWakePendingLimit)
}

func observeIdleScreen(wire io.ReadWriter, until time.Time, pendingLimit time.Duration) (observation IdleWakeObservation) {
	client, err := rfbclient.Open(wire)
	if err != nil {
		observation.Err = err
		return observation
	}
	first, err := client.Frame()
	if err != nil {
		observation.Err = err
		return observation
	}
	firstAt := time.Now()
	observation.FirstFrame = firstAt
	// rfbclient reads the wire unbuffered and has no request outstanding
	// after Frame, so the next byte on the wire starts a new server message.
	updates := make(chan idleWakeUpdate)
	done := make(chan struct{})
	defer close(done)
	go func() {
		for {
			update := readIdleWakeUpdate(wire, first)
			select {
			case updates <- update:
			case <-done:
				return
			}
			if update.err != nil {
				return
			}
		}
	}()
	width, height := client.Width, client.Height
	request := func(incremental bool) error {
		message := []byte{3, 0, 0, 0, 0, 0, 0, 0, 0, 0}
		if incremental {
			message[1] = 1
		}
		binary.BigEndian.PutUint16(message[6:], uint16(width))
		binary.BigEndian.PutUint16(message[8:], uint16(height))
		_, err := wire.Write(message)
		return err
	}
	changed := func(update idleWakeUpdate) bool {
		if update.err != nil {
			observation.Err = update.err
			return true
		}
		if update.changed > 0 {
			observation.Changed, observation.ChangedBox, observation.ChangedAt = update.changed, update.box, time.Since(firstAt)
			return true
		}
		return false
	}
	if observation.Err = request(true); observation.Err != nil {
		return observation
	}
	deadline := time.NewTimer(time.Until(until))
	defer deadline.Stop()
watch:
	for {
		select {
		case update := <-updates:
			if changed(update) {
				return observation
			}
			observation.Updates++
			if len(update.rects) == 0 {
				time.Sleep(20 * time.Millisecond) // Pace empty replies.
			}
			if observation.Err = request(true); observation.Err != nil {
				return observation
			}
		case <-deadline.C:
			break watch
		}
	}
	// One incremental request is outstanding. Compare its reply first, so the
	// final full request is the only one outstanding and no earlier reply can
	// complete the final check.
	pending := time.NewTimer(pendingLimit)
	defer pending.Stop()
	select {
	case update := <-updates:
		if changed(update) {
			return observation
		}
		observation.Updates++
	case <-pending.C:
		observation.Err = fmt.Errorf("observer: no reply to the pending request within %s", pendingLimit)
		return observation
	}
	if observation.Err = request(false); observation.Err != nil {
		return observation
	}
	seen, covered := make([]bool, width*height), 0
	for covered < len(seen) {
		update := <-updates
		if changed(update) {
			return observation
		}
		for _, rect := range update.rects {
			for y := rect.Min.Y; y < rect.Max.Y; y++ {
				for x := rect.Min.X; x < rect.Max.X; x++ {
					if !seen[y*width+x] {
						seen[y*width+x] = true
						covered++
					}
				}
			}
		}
	}
	observation.Observed = time.Since(firstAt)
	return observation
}

type idleWakeUpdate struct {
	rects   []image.Rectangle
	changed int
	box     image.Rectangle
	err     error
}

// readIdleWakeUpdate reads one FramebufferUpdate in rfbclient's negotiated
// format (Raw, 32-bit B,G,R,padding) and compares its pixels with first.
func readIdleWakeUpdate(wire io.Reader, first *image.RGBA) (update idleWakeUpdate) {
	fail := func(err error) idleWakeUpdate { update.err = err; return update }
	bounds := first.Bounds()
	for {
		var kind [1]byte
		if _, err := io.ReadFull(wire, kind[:]); err != nil {
			return fail(err)
		}
		switch kind[0] {
		case 0:
		case 2:
			continue // bell
		case 3: // ServerCutText is skipped and never logged.
			var header [7]byte
			if _, err := io.ReadFull(wire, header[:]); err != nil {
				return fail(err)
			}
			n := binary.BigEndian.Uint32(header[3:])
			if n > 1<<20 {
				return fail(errors.New("RFB clipboard too large"))
			}
			if _, err := io.CopyN(io.Discard, wire, int64(n)); err != nil {
				return fail(err)
			}
			continue
		default:
			return fail(fmt.Errorf("unsupported RFB message %d", kind[0]))
		}
		var header [3]byte
		if _, err := io.ReadFull(wire, header[:]); err != nil {
			return fail(err)
		}
		for count := binary.BigEndian.Uint16(header[1:]); count > 0; count-- {
			var rect [12]byte
			if _, err := io.ReadFull(wire, rect[:]); err != nil {
				return fail(err)
			}
			x, y := int(binary.BigEndian.Uint16(rect[:])), int(binary.BigEndian.Uint16(rect[2:]))
			r := image.Rect(x, y, x+int(binary.BigEndian.Uint16(rect[4:])), y+int(binary.BigEndian.Uint16(rect[6:])))
			if binary.BigEndian.Uint32(rect[8:]) != 0 {
				return fail(errors.New("RFB server ignored Raw encoding negotiation"))
			}
			if r.Empty() || !r.In(bounds) {
				return fail(errors.New("RFB rectangle outside screen"))
			}
			row := make([]byte, r.Dx()*4)
			for yy := r.Min.Y; yy < r.Max.Y; yy++ {
				if _, err := io.ReadFull(wire, row); err != nil {
					return fail(err)
				}
				for xx := r.Min.X; xx < r.Max.X; xx++ {
					o := (xx - r.Min.X) * 4
					if (color.RGBA{R: row[o+2], G: row[o+1], B: row[o], A: 255}) != first.RGBAAt(xx, yy) {
						update.changed++
						update.box = update.box.Union(image.Rect(xx, yy, xx+1, yy+1))
					}
				}
			}
			update.rects = append(update.rects, r)
		}
		return update
	}
}

func TestIdleWakePremise(t *testing.T) {
	quiet, observed := IdleWakeQuiet, IdleWakeObservedMin
	for _, test := range []struct {
		name        string
		observation IdleWakeObservation
		want        string
	}{
		{"idle", IdleWakeObservation{SinceInput: quiet + time.Second, Observed: quiet}, ""},
		{"exact boundaries", IdleWakeObservation{SinceInput: quiet, Observed: observed}, ""},
		{"observer error", IdleWakeObservation{SinceInput: quiet, Observed: observed, Err: errors.New("closed")}, "screen observer failed: closed"},
		{"screen changed", IdleWakeObservation{SinceInput: quiet, Observed: observed, Changed: 12, ChangedBox: image.Rect(0, 0, 4, 3), ChangedAt: 31500 * time.Millisecond}, "12 pixels in (0,0)-(4,3) changed 31.5 s after"},
		{"delayed first frame", IdleWakeObservation{SinceInput: quiet + 5*time.Second, Observed: 60 * time.Second}, "static screen for only 60.0 s, need 65 s"},
		{"observed just short", IdleWakeObservation{SinceInput: quiet, Observed: observed - time.Nanosecond}, "static screen for only"},
		{"input just recent", IdleWakeObservation{SinceInput: quiet - time.Nanosecond, Observed: observed}, "70.0 s since the last input, need 70 s"},
		{"recent input", IdleWakeObservation{SinceInput: 64 * time.Second, Observed: observed}, "64.0 s since the last input, need 70 s"},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := test.observation.Premise()
			if test.want == "" {
				if err != nil {
					t.Fatalf("Premise = %v, want nil", err)
				}
				return
			}
			if err == nil || !strings.HasPrefix(err.Error(), "idle premise not met: ") || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("Premise = %v, want idle premise not met: ...%s", err, test.want)
			}
		})
	}
}

// idleWakeFake is a scripted RFB server on a 4x3 screen. Its first full
// request returns first, later ones last. Each incremental request gets the
// next scripted update after its delay. Once the script ends, a request gets
// a 1x1 heartbeat after heartbeat, as on x11vnc; with no heartbeat it is
// held, and a later full request answers both at once, as LibVNCServer does.
type idleWakeFake struct {
	first, last color.RGBA
	updates     []idleWakeFakeUpdate
	heartbeat   time.Duration
	firstDelay  time.Duration
}

type idleWakeFakeUpdate struct {
	rect   image.Rectangle
	colour color.RGBA
	delay  time.Duration
}

func (f idleWakeFake) serve(t *testing.T) net.Conn {
	t.Helper()
	client, server := net.Pipe()
	done := make(chan struct{})
	t.Cleanup(func() { _ = client.Close(); _ = server.Close(); <-done })
	go func() {
		defer close(done)
		defer server.Close()
		read := func(n int) ([]byte, error) { p := make([]byte, n); _, err := io.ReadFull(server, p); return p, err }
		send := func(update idleWakeFakeUpdate) bool {
			r := update.rect
			reply := make([]byte, 16, 16+r.Dx()*r.Dy()*4)
			reply[3] = 1
			for i, n := range []int{r.Min.X, r.Min.Y, r.Dx(), r.Dy()} {
				binary.BigEndian.PutUint16(reply[4+i*2:], uint16(n))
			}
			for range r.Dx() * r.Dy() {
				reply = append(reply, update.colour.B, update.colour.G, update.colour.R, 0)
			}
			_, err := server.Write(reply)
			return err == nil
		}
		if !fakeRFBHandshake(server, 4, 3) {
			return
		}
		fulls, next := 0, 0
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
				if p, err = read(9); err != nil {
					return
				}
				if p[0] == 1 {
					update := idleWakeFakeUpdate{image.Rect(0, 0, 1, 1), f.first, f.heartbeat}
					if next < len(f.updates) {
						update = f.updates[next]
					} else if f.heartbeat == 0 {
						continue
					}
					next++
					time.Sleep(update.delay)
					if !send(update) {
						return
					}
					continue
				}
				colour := f.last
				if fulls == 0 {
					colour = f.first
					time.Sleep(f.firstDelay)
				}
				fulls++
				if !send(idleWakeFakeUpdate{rect: image.Rect(0, 0, 4, 3), colour: colour}) {
					return
				}
			default:
				t.Errorf("unexpected RFB client message %d", kind[0])
				return
			}
			if err != nil {
				return
			}
		}
	}()
	return client
}

func TestObserveIdleScreen(t *testing.T) {
	a, b := color.RGBA{R: 24, G: 32, B: 48, A: 255}, color.RGBA{G: 255, A: 255}
	heartbeat := idleWakeFakeUpdate{rect: image.Rect(0, 0, 1, 1), colour: a}
	beat := 20 * time.Millisecond
	window := 300 * time.Millisecond
	for _, test := range []struct {
		name     string
		fake     idleWakeFake
		until    time.Duration // from the start of the test
		changed  int
		updates  int
		err      string
		static   bool   // observed static from the first frame to the deadline
		premise  string // the Premise error with lane timings, "" for nil
		maxShort time.Duration
	}{
		// Unchanged heartbeats are compared and accepted.
		{name: "heartbeats on a static screen", fake: idleWakeFake{first: a, last: a, updates: []idleWakeFakeUpdate{heartbeat, heartbeat, heartbeat}, heartbeat: beat}, until: window, updates: 4, static: true},
		// B is seen when it arrives, though the screen and the last frame are A again.
		{name: "A to B to A", fake: idleWakeFake{first: a, last: a, updates: []idleWakeFakeUpdate{heartbeat, {rect: image.Rect(1, 1, 3, 3), colour: b}, {rect: image.Rect(1, 1, 3, 3), colour: a}}, heartbeat: beat}, until: window, changed: 4, updates: 1, premise: "4 pixels in (1,1)-(3,3) changed"},
		// A change seen only by the final full check still fails.
		{name: "changed at the final check", fake: idleWakeFake{first: a, last: b, heartbeat: beat}, until: window, changed: 12, premise: "12 pixels in (0,0)-(4,3) changed"},
		// The pending request's unchanged full-screen reply must not complete
		// the final check before the changed full reply is read.
		{name: "unchanged pending reply then changed final", fake: idleWakeFake{first: a, last: b, updates: []idleWakeFakeUpdate{{rect: image.Rect(0, 0, 4, 3), colour: a, delay: window + 100*time.Millisecond}}, heartbeat: beat}, until: window, changed: 12, updates: 1, premise: "12 pixels in (0,0)-(4,3) changed"},
		// A held request never passes.
		{name: "no reply to the pending request", fake: idleWakeFake{first: a, last: a}, until: window, err: "observer: no reply to the pending request within 200ms", premise: "screen observer failed: observer: no reply"},
		// The window starts at the first frame, not at the caller's clock.
		{name: "delayed first frame", fake: idleWakeFake{first: a, last: a, firstDelay: 250 * time.Millisecond, heartbeat: beat}, until: window, maxShort: 200 * time.Millisecond, premise: "static screen for only"},
		{name: "deadline already expired", fake: idleWakeFake{first: a, last: a, updates: []idleWakeFakeUpdate{heartbeat}, heartbeat: beat}, until: -time.Second, maxShort: 200 * time.Millisecond, premise: "static screen for only"},
	} {
		t.Run(test.name, func(t *testing.T) {
			wire := test.fake.serve(t)
			until := time.Now().Add(test.until)
			observation := observeIdleScreen(wire, until, 200*time.Millisecond)
			if test.err != "" {
				if observation.Err == nil || observation.Err.Error() != test.err {
					t.Fatalf("observation = %+v, want error %q", observation, test.err)
				}
			} else if observation.Err != nil || observation.Changed != test.changed || observation.Updates < test.updates {
				t.Fatalf("observation = %+v, want changed %d after at least %d updates", observation, test.changed, test.updates)
			}
			if test.static {
				// Observed runs from the observer's own first frame, past the deadline.
				if observation.FirstFrame.IsZero() || observation.Observed < until.Sub(observation.FirstFrame) {
					t.Fatalf("observed static %s from %v, want at least until the deadline %v", observation.Observed, observation.FirstFrame, until)
				}
				observation.Observed = IdleWakeObservedMin // scale the passing window to lane length
			}
			if test.maxShort > 0 && observation.Observed > test.maxShort {
				t.Fatalf("observed static %s, want at most %s", observation.Observed, test.maxShort)
			}
			observation.SinceInput = IdleWakeQuiet
			err := observation.Premise()
			if test.premise == "" && err != nil || test.premise != "" && (err == nil || !strings.Contains(err.Error(), test.premise)) {
				t.Fatalf("Premise = %v, want %q", err, test.premise)
			}
		})
	}
}
