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
	"sync"
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
	// After the deadline the observer still compares an update already in
	// flight for this long, then closes its connection.
	IdleWakeGrace = time.Second
)

// IdleWakeObservation is what the lane saw before it timed the idle-wake call.
type IdleWakeObservation struct {
	SinceInput time.Duration   // from the test's last input to the timed call
	Observed   time.Duration   // first full frame to the deadline, with no changed update
	Updates    int             // completed updates compared; 0 is normal on a static screen
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

// ObserveIdleScreen watches one view-role RFB connection until `until`, then
// for IdleWakeGrace more, and closes wire. It takes a full first frame with
// rfbclient, then keeps one incremental request outstanding and compares
// every completed update's pixels with the first frame as it arrives, so a
// change that reverts before a later frame still counts. rfbclient reports
// only whether an update arrived, and its frame captures fold updates
// together, so neither can show this.
//
// There is no final full frame. x11vnc 0.9.16 sends no heartbeat without
// -ping, so it may never answer a held request on a static screen; a fresh
// view connection's first frame can be stale (#1484). Neither is needed: a
// redraw does not reset the control role's no-framebuffer-data nap timer, so
// the premise only has to show that the screen was not busy in the window.
// An update still incomplete when the grace ends is ignored.
func ObserveIdleScreen(wire io.ReadWriteCloser, until time.Time) IdleWakeObservation {
	return observeIdleScreen(wire, until, time.Now, time.After)
}

func observeIdleScreen(wire io.ReadWriteCloser, until time.Time, now func() time.Time, after func(time.Duration) <-chan time.Time) (observation IdleWakeObservation) {
	defer wire.Close() // After the grace this close ends the reader; that is the normal end.
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
	firstAt := now()
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
	request := func() error {
		message := []byte{3, 1, 0, 0, 0, 0, 0, 0, 0, 0}
		binary.BigEndian.PutUint16(message[6:], uint16(client.Width))
		binary.BigEndian.PutUint16(message[8:], uint16(client.Height))
		_, err := wire.Write(message)
		return err
	}
	if observation.Err = request(); observation.Err != nil {
		return observation
	}
	deadline, grace := after(until.Sub(firstAt)), (<-chan time.Time)(nil)
	for {
		select {
		case update := <-updates:
			if update.err != nil {
				observation.Err = update.err
				return observation
			}
			if update.changed > 0 {
				observation.Changed, observation.ChangedBox, observation.ChangedAt = update.changed, update.box, now().Sub(firstAt)
				return observation
			}
			observation.Updates++
			if len(update.rects) == 0 {
				time.Sleep(20 * time.Millisecond) // Pace empty replies.
			}
			if observation.Err = request(); observation.Err != nil {
				return observation
			}
		case <-deadline:
			deadline, grace = nil, after(IdleWakeGrace)
		case <-grace:
			observation.Observed = max(until.Sub(firstAt), 0)
			return observation
		}
	}
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

// idleWakeFake is an RFB server on a 4x3 screen, driven by its test. It
// answers the full request with first and holds every incremental request,
// as x11vnc does when nothing changed, until the test answers it.
type idleWakeFake struct {
	wire     net.Conn
	requests chan struct{} // one per incremental request received
	held     int           // requests taken from requests but not answered
	mu       sync.Mutex    // serializes writes
}

func newIdleWakeFake(t *testing.T, first color.RGBA) (*idleWakeFake, net.Conn) {
	t.Helper()
	client, server := net.Pipe()
	f := &idleWakeFake{wire: server, requests: make(chan struct{}, 16)}
	done := make(chan struct{})
	t.Cleanup(func() { _ = client.Close(); _ = server.Close(); <-done })
	go func() {
		defer close(done)
		defer server.Close()
		if !fakeRFBHandshake(server, 4, 3) {
			return
		}
		read := func(n int) ([]byte, error) { p := make([]byte, n); _, err := io.ReadFull(server, p); return p, err }
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
					if p[0] == 1 {
						f.requests <- struct{}{}
					} else if f.send(image.Rect(0, 0, 4, 3), first) != nil {
						return
					}
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
	return f, client
}

func (f *idleWakeFake) send(r image.Rectangle, colour color.RGBA) error {
	reply := make([]byte, 16, 16+r.Dx()*r.Dy()*4)
	reply[3] = 1
	for i, n := range []int{r.Min.X, r.Min.Y, r.Dx(), r.Dy()} {
		binary.BigEndian.PutUint16(reply[4+i*2:], uint16(n))
	}
	for range r.Dx() * r.Dy() {
		reply = append(reply, colour.B, colour.G, colour.R, 0)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	_, err := f.wire.Write(reply)
	return err
}

// answer waits for a held incremental request and answers it.
func (f *idleWakeFake) answer(t *testing.T, r image.Rectangle, colour color.RGBA) {
	t.Helper()
	if f.held > 0 {
		f.held--
	} else {
		<-f.requests
	}
	if err := f.send(r, colour); err != nil {
		t.Fatalf("fake update: %v", err)
	}
}

// awaitRequest waits until the observer has asked again, so it has compared
// the previous update.
func (f *idleWakeFake) awaitRequest() { <-f.requests; f.held++ }

// idleWakeTimers hands the observer channels that the test fires, so the
// deadline and the grace never race the fake server's replies.
type idleWakeTimers struct {
	asked           chan time.Duration
	deadline, grace chan time.Time
	calls           int
}

func (w *idleWakeTimers) after(d time.Duration) <-chan time.Time {
	w.calls++
	w.asked <- d
	if w.calls == 1 {
		return w.deadline
	}
	return w.grace
}

func TestObserveIdleScreen(t *testing.T) {
	a, b := color.RGBA{R: 24, G: 32, B: 48, A: 255}, color.RGBA{G: 255, A: 255}
	base := time.Date(2026, 11, 5, 12, 0, 0, 0, time.UTC)
	until := base.Add(IdleWakeQuiet)
	all := image.Rect(0, 0, 4, 3)
	type env struct {
		f      *idleWakeFake
		timers *idleWakeTimers
	}
	type step func(*testing.T, env)
	unchanged := func(r image.Rectangle) step {
		return func(t *testing.T, e env) { e.f.answer(t, r, a); e.f.awaitRequest() }
	}
	changed := func(r image.Rectangle) step {
		return func(t *testing.T, e env) { e.f.answer(t, r, b) }
	}
	deadline := func(t *testing.T, e env) {
		<-e.timers.asked
		e.timers.deadline <- until
		if got := <-e.timers.asked; got != IdleWakeGrace {
			t.Errorf("grace %s, want %s", got, IdleWakeGrace)
		}
	}
	grace := func(t *testing.T, e env) { e.timers.grace <- until.Add(IdleWakeGrace) }
	breakServer := func(t *testing.T, e env) { _ = e.f.wire.Close() }
	for _, test := range []struct {
		name     string
		firstAt  time.Time // when the observer's first full frame completes
		steps    []step
		after    step // runs once the observer has returned
		updates  int
		changed  int
		observed time.Duration
		premise  string // "" for nil
	}{
		// (a) x11vnc holds every request on a static screen: nothing arrives.
		{name: "static screen holds every request", firstAt: base, steps: []step{deadline, grace}, observed: IdleWakeQuiet},
		// Unchanged updates are compared and counted, never required.
		{name: "unchanged updates", firstAt: base, steps: []step{unchanged(image.Rect(0, 0, 1, 1)), unchanged(all), deadline, unchanged(all), grace}, updates: 3, observed: IdleWakeQuiet},
		// (b) B is seen when it arrives, though A would follow.
		{name: "A to B to A", firstAt: base, steps: []step{unchanged(image.Rect(0, 0, 1, 1)), changed(image.Rect(1, 1, 3, 3))}, updates: 1, changed: 4, premise: "4 pixels in (1,1)-(3,3) changed"},
		// (c) An update that arrives within the grace is still compared.
		{name: "change within the grace", firstAt: base, steps: []step{deadline, changed(all)}, changed: 12, premise: "12 pixels in (0,0)-(4,3) changed"},
		// (d) The bound: a change after the grace is not seen.
		{name: "change after the grace", firstAt: base, steps: []step{deadline, grace}, after: func(t *testing.T, e env) { _ = e.f.send(all, b) }, observed: IdleWakeQuiet},
		// (e) The window starts at the first frame, not at the caller's clock.
		{name: "delayed first frame", firstAt: base.Add(10 * time.Second), steps: []step{deadline, grace}, observed: 60 * time.Second, premise: "static screen for only 60.0 s, need 65 s"},
		// (f) A read error that the observer did not cause fails.
		{name: "server breaks the connection", firstAt: base, steps: []step{breakServer}, premise: "screen observer failed"},
		// (g) Exact boundary: 65 s observed passes.
		{name: "observed exactly the minimum", firstAt: base.Add(IdleWakeQuiet - IdleWakeObservedMin), steps: []step{deadline, grace}, observed: IdleWakeObservedMin},
	} {
		t.Run(test.name, func(t *testing.T) {
			f, wire := newIdleWakeFake(t, a)
			e := env{f, &idleWakeTimers{asked: make(chan time.Duration, 2), deadline: make(chan time.Time, 1), grace: make(chan time.Time, 1)}}
			result := make(chan IdleWakeObservation, 1)
			go func() {
				result <- observeIdleScreen(wire, until, func() time.Time { return test.firstAt }, e.timers.after)
			}()
			for _, step := range test.steps {
				step(t, e)
			}
			// Only a failure waits this long: every case ends the observer through
			// its steps, so a missed change does not hang the test.
			var observation IdleWakeObservation
			select {
			case observation = <-result:
			case <-time.After(10 * time.Second):
				t.Fatalf("observer did not return after its steps; want %d changed pixels", test.changed)
			}
			if test.after != nil {
				test.after(t, e)
			}
			if observation.Updates != test.updates || observation.Changed != test.changed || observation.Observed != test.observed {
				t.Fatalf("observation = %+v, want %d updates, %d changed, observed %s", observation, test.updates, test.changed, test.observed)
			}
			observation.SinceInput = IdleWakeQuiet
			err := observation.Premise()
			if test.premise == "" && err != nil || test.premise != "" && (err == nil || !strings.Contains(err.Error(), test.premise)) {
				t.Fatalf("Premise = %v, want %q", err, test.premise)
			}
		})
	}
}
