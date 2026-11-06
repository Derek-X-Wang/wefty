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
	// The last warm call's own repaint can arrive after the observer's first
	// frame (#1520 saw one 0.8 s later). A change this soon after the first
	// frame becomes the new reference, and the static window starts there.
	// Any later change fails the premise.
	IdleWakeSettle = 3 * time.Second
)

// IdleWakeObservation is what the lane saw before it timed the idle-wake call.
type IdleWakeObservation struct {
	SinceInput time.Duration   // from the test's last input to the timed call
	Observed   time.Duration   // the static window: the reference frame to the deadline, with no changed update
	Settled    time.Duration   // first frame to the reference frame; 0 without an early repaint
	Repaints   int             // early changed updates folded into the reference
	Updates    int             // completed updates compared; 0 is normal on a static screen
	Changed    int             // differing pixels in the first update that changed after the settle
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
// every completed update's pixels with the reference as it arrives, so a
// change that reverts before a later frame still counts. rfbclient reports
// only whether an update arrived, and its frame captures fold updates
// together, so neither can show this.
//
// The reference starts as the first frame. A changed update that arrives
// within IdleWakeSettle of the first frame is a late repaint of the last
// input: it is folded into the reference, and the static window restarts
// when it arrives. After the settle, any changed pixel fails the premise.
//
// There is no final full frame. x11vnc 0.9.16 sends no heartbeat without
// -ping, so it may never answer a held request on a static screen; a fresh
// view connection's first frame can be stale (#1484). Neither is needed: a
// redraw does not reset the control role's no-framebuffer-data nap timer, so
// the premise only has to show that the screen was not busy in the window.
// An update still incomplete when the grace ends is ignored.
func ObserveIdleScreen(wire io.ReadWriteCloser, until time.Time) IdleWakeObservation {
	return observeIdleScreen(wire, until, time.Now, time.After, nil)
}

// handedOver, when not nil, runs after the reader hands an update over.
func observeIdleScreen(wire io.ReadWriteCloser, until time.Time, now func() time.Time, after func(time.Duration) <-chan time.Time, handedOver func()) (observation IdleWakeObservation) {
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
	referenceAt := firstAt
	// rfbclient reads the wire unbuffered and has no request outstanding
	// after Frame, so the next byte on the wire starts a new server message.
	// An update counts as completed once the reader has handed it over (into
	// this one-slot buffer); anything later is incomplete at the grace end.
	// Only the reader touches first after this point: it folds every changed
	// pixel into it, so first is always the latest reference.
	updates := make(chan idleWakeUpdate, 1)
	done := make(chan struct{})
	defer close(done)
	go func() {
		for {
			update := readIdleWakeUpdate(wire, first)
			select {
			case updates <- update:
				if handedOver != nil {
					handedOver()
				}
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
	// failed reports a reader error or a changed pixel after the settle, and
	// counts the rest. An early change restarts the static window.
	failed := func(update idleWakeUpdate) bool {
		if update.err != nil {
			observation.Err = update.err
			return true
		}
		if update.changed > 0 {
			at := now()
			if at.Sub(firstAt) > IdleWakeSettle {
				observation.Changed, observation.ChangedBox, observation.ChangedAt = update.changed, update.box, at.Sub(firstAt)
				return true
			}
			referenceAt = at
			observation.Repaints++
		}
		observation.Updates++
		return false
	}
	deadline, grace := after(until.Sub(firstAt)), (<-chan time.Time)(nil)
	for {
		select {
		case update := <-updates:
			if failed(update) {
				return observation
			}
			if len(update.rects) == 0 {
				time.Sleep(20 * time.Millisecond) // Pace empty replies.
			}
			if observation.Err = request(); observation.Err != nil {
				return observation
			}
		case <-deadline:
			deadline, grace = nil, after(IdleWakeGrace)
		case <-grace:
			select { // A handed-over update still counts, even if grace won the select.
			case update := <-updates:
				if failed(update) {
					return observation
				}
			default:
			}
			observation.Observed, observation.Settled = max(until.Sub(referenceAt), 0), referenceAt.Sub(firstAt)
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
// format (Raw, 32-bit B,G,R,padding), compares its pixels with reference and
// writes every changed pixel into reference.
func readIdleWakeUpdate(wire io.Reader, reference *image.RGBA) (update idleWakeUpdate) {
	fail := func(err error) idleWakeUpdate { update.err = err; return update }
	bounds := reference.Bounds()
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
					if pixel := (color.RGBA{R: row[o+2], G: row[o+1], B: row[o], A: 255}); pixel != reference.RGBAAt(xx, yy) {
						reference.SetRGBA(xx, yy, pixel)
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
		f.request(t)
	}
	if err := f.send(r, colour); err != nil {
		t.Fatalf("fake update: %v", err)
	}
}

// awaitRequest waits until the observer has asked again, so it has compared
// the previous update.
func (f *idleWakeFake) awaitRequest(t *testing.T) { t.Helper(); f.request(t); f.held++ }

// request takes one incremental request. An observer that returned early
// never asks again, so this fails instead of hanging the test.
func (f *idleWakeFake) request(t *testing.T) {
	t.Helper()
	select {
	case <-f.requests:
	case <-time.After(10 * time.Second):
		t.Fatal("observer stopped asking for updates")
	}
}

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

// idleWakeClock is the observer's clock. A test step sets it before it sends
// an update, so the observer reads that time when it compares the update.
type idleWakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *idleWakeClock) now() time.Time  { c.mu.Lock(); defer c.mu.Unlock(); return c.t }
func (c *idleWakeClock) set(t time.Time) { c.mu.Lock(); defer c.mu.Unlock(); c.t = t }

func TestObserveIdleScreen(t *testing.T) {
	a, b := color.RGBA{R: 24, G: 32, B: 48, A: 255}, color.RGBA{G: 255, A: 255}
	base := time.Date(2026, 11, 5, 12, 0, 0, 0, time.UTC)
	until := base.Add(IdleWakeQuiet)
	all := image.Rect(0, 0, 4, 3)
	type env struct {
		f       *idleWakeFake
		timers  *idleWakeTimers
		clock   *idleWakeClock
		firstAt time.Time
	}
	type step func(*testing.T, env)
	unchanged := func(r image.Rectangle) step {
		return func(t *testing.T, e env) { e.f.answer(t, r, a); e.f.awaitRequest(t) }
	}
	changed := func(r image.Rectangle, colour color.RGBA) step {
		return func(t *testing.T, e env) { e.f.answer(t, r, colour) }
	}
	// repaint sends an early change that the observer folds and asks again.
	repaint := func(colour color.RGBA) step {
		return func(t *testing.T, e env) { e.f.answer(t, all, colour); e.f.awaitRequest(t) }
	}
	// started waits until the observer has read its first-frame time.
	started := func(t *testing.T, e env) { e.f.awaitRequest(t) }
	at := func(d time.Duration) step {
		return func(t *testing.T, e env) { e.clock.set(e.firstAt.Add(d)) }
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
		repaints int
		changed  int
		observed time.Duration
		settled  time.Duration
		premise  string // "" for nil
	}{
		// (a) x11vnc holds every request on a static screen: nothing arrives.
		{name: "static screen holds every request", firstAt: base, steps: []step{deadline, grace}, observed: IdleWakeQuiet},
		// Unchanged updates are compared and counted, never required.
		{name: "unchanged updates", firstAt: base, steps: []step{unchanged(image.Rect(0, 0, 1, 1)), unchanged(all), deadline, unchanged(all), grace}, updates: 3, observed: IdleWakeQuiet},
		// (b) B is seen when it arrives, though A would follow.
		{name: "A to B to A", firstAt: base, steps: []step{unchanged(image.Rect(0, 0, 1, 1)), at(10 * time.Second), changed(image.Rect(1, 1, 3, 3), b)}, updates: 1, changed: 4, premise: "4 pixels in (1,1)-(3,3) changed 10.0 s after"},
		// (c) An update that arrives within the grace is still compared.
		{name: "change within the grace", firstAt: base, steps: []step{deadline, at(IdleWakeQuiet), changed(all, b)}, changed: 12, premise: "12 pixels in (0,0)-(4,3) changed 70.0 s after"},
		// (d) The bound: a change after the grace is not seen.
		{name: "change after the grace", firstAt: base, steps: []step{deadline, grace}, after: func(t *testing.T, e env) { _ = e.f.send(all, b) }, observed: IdleWakeQuiet},
		// (h) #1520: the last warm call's repaint arrives 0.8 s after the first
		// frame. It becomes the reference, and the window restarts there.
		{name: "late repaint restarts the window", firstAt: base, steps: []step{started, at(800 * time.Millisecond), repaint(b), deadline, grace}, updates: 1, repaints: 1, observed: IdleWakeQuiet - 800*time.Millisecond, settled: 800 * time.Millisecond},
		// (i) Several early repaints, one reverting the other, each restart it.
		{name: "repaints within the settle", firstAt: base, steps: []step{started, at(time.Second), repaint(b), at(IdleWakeSettle), repaint(a), deadline, grace}, updates: 2, repaints: 2, observed: IdleWakeQuiet - IdleWakeSettle, settled: IdleWakeSettle},
		// (j) After the settle the premise stays strict, against the folded
		// reference: going back to the first frame's pixels is a change.
		{name: "change after an early repaint", firstAt: base, steps: []step{started, at(800 * time.Millisecond), repaint(b), at(IdleWakeSettle + time.Nanosecond), changed(all, a)}, updates: 1, repaints: 1, changed: 12, premise: "12 pixels in (0,0)-(4,3) changed 3.0 s after"},
		// (k) The settle bound: a change just after it fails.
		{name: "change just after the settle", firstAt: base, steps: []step{started, at(IdleWakeSettle + time.Nanosecond), changed(all, b)}, changed: 12, premise: "changed 3.0 s after the first frame"},
		// (l) An early repaint never extends the deadline: a late first frame
		// and a repaint can leave too short a window.
		{name: "repaint after a delayed first frame", firstAt: base.Add(3 * time.Second), steps: []step{started, at(2500 * time.Millisecond), repaint(b), deadline, grace}, updates: 1, repaints: 1, observed: IdleWakeQuiet - 5500*time.Millisecond, settled: 2500 * time.Millisecond, premise: "static screen for only 64.5 s, need 65 s"},
		// (e) The window starts at the first frame, not at the caller's clock.
		{name: "delayed first frame", firstAt: base.Add(10 * time.Second), steps: []step{deadline, grace}, observed: 60 * time.Second, premise: "static screen for only 60.0 s, need 65 s"},
		// (f) A read error that the observer did not cause fails.
		{name: "server breaks the connection", firstAt: base, steps: []step{breakServer}, premise: "screen observer failed"},
		// (g) Exact boundary: 65 s observed passes.
		{name: "observed exactly the minimum", firstAt: base.Add(IdleWakeQuiet - IdleWakeObservedMin), steps: []step{deadline, grace}, observed: IdleWakeObservedMin},
	} {
		t.Run(test.name, func(t *testing.T) {
			f, wire := newIdleWakeFake(t, a)
			e := env{f, &idleWakeTimers{asked: make(chan time.Duration, 2), deadline: make(chan time.Time, 1), grace: make(chan time.Time, 1)}, &idleWakeClock{t: test.firstAt}, test.firstAt}
			result := make(chan IdleWakeObservation, 1)
			go func() {
				result <- observeIdleScreen(wire, until, e.clock.now, e.timers.after, nil)
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
			if observation.Updates != test.updates || observation.Repaints != test.repaints || observation.Changed != test.changed || observation.Observed != test.observed || observation.Settled != test.settled {
				t.Fatalf("observation = %+v, want %d updates, %d repaints, %d changed, observed %s, settled %s", observation, test.updates, test.repaints, test.changed, test.observed, test.settled)
			}
			observation.SinceInput = IdleWakeQuiet
			err := observation.Premise()
			if test.premise == "" && err != nil || test.premise != "" && (err == nil || !strings.Contains(err.Error(), test.premise)) {
				t.Fatalf("Premise = %v, want %q", err, test.premise)
			}
		})
	}
}

// A changed update the reader has handed over when grace fires must still
// fail. The test holds the observer inside its grace timer call until the
// update is handed over, so both are ready at its next select; select picks
// either at random, so repeat to make a missing drain fail almost surely.
func TestObserveIdleScreenChecksAHandedOverUpdateAtGrace(t *testing.T) {
	a, b := color.RGBA{R: 24, G: 32, B: 48, A: 255}, color.RGBA{G: 255, A: 255}
	base := time.Date(2026, 11, 5, 12, 0, 0, 0, time.UTC)
	for attempt := range 32 {
		f, wire := newIdleWakeFake(t, a)
		deadline, inGrace, release := make(chan time.Time, 1), make(chan struct{}), make(chan struct{})
		fired := make(chan time.Time, 1)
		fired <- base
		calls := 0
		after := func(time.Duration) <-chan time.Time {
			if calls++; calls == 1 {
				return deadline
			}
			close(inGrace)
			<-release
			return fired
		}
		handed := make(chan struct{}, 4)
		result := make(chan IdleWakeObservation, 1)
		clock := &idleWakeClock{t: base}
		go func() {
			result <- observeIdleScreen(wire, base.Add(IdleWakeQuiet), clock.now, after, func() { handed <- struct{}{} })
		}()
		deadline <- base
		<-inGrace
		clock.set(base.Add(IdleWakeQuiet)) // the grace is long after the settle
		f.answer(t, image.Rect(0, 0, 4, 3), b)
		<-handed
		close(release)
		select {
		case observation := <-result:
			if observation.Changed != 12 || observation.Premise() == nil {
				t.Fatalf("attempt %d: observation = %+v, want the handed-over change of 12 pixels", attempt, observation)
			}
		case <-time.After(10 * time.Second):
			t.Fatalf("attempt %d: observer did not return", attempt)
		}
	}
}
