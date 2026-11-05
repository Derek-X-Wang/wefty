package computerconformance_test

import (
	"errors"
	"fmt"
	"image"
	"image/color"
	"strings"
	"testing"
	"time"
)

// The Seat screen lane's idle-wake SC-1 case (#1495). x11vnc 0.9.16 naps
// between screen polls for up to 4 s once it has sent no framebuffer data for
// 60 s (scan.c nap_check, -sb), and it notes that time only every 2 s
// (screen.c record_last_fb_update). Only user input ends a nap early (#1479).
// 70 s without input or framebuffer data reaches that state with margin.
const (
	idleWakeQuiet  = 70 * time.Second
	idleWakeBudget = time.Second // the owner's SC-1 threshold
)

// idleWakeObservation is what the lane saw before it timed the idle-wake call.
type idleWakeObservation struct {
	SinceInput time.Duration   // from the test's last input to the timed call
	Frames     int             // full frames compared with the first one
	Changed    int             // differing pixels in the first frame that changed
	ChangedBox image.Rectangle // where they are
	ChangedAt  time.Duration   // when that frame was read, after the first one
	Err        error           // the observer failed
}

// premise returns why the timed call would not measure an idle server, or nil.
// A failed premise is a lane failure, never a warm measurement.
func (o idleWakeObservation) premise(quiet time.Duration) error {
	switch {
	case o.Err != nil:
		return fmt.Errorf("idle premise not met: screen observer failed: %w", o.Err)
	case o.Changed > 0:
		return fmt.Errorf("idle premise not met: %d pixels in %v changed %.1f s after the first frame", o.Changed, o.ChangedBox, o.ChangedAt.Seconds())
	case o.Frames < 2:
		return fmt.Errorf("idle premise not met: only %d frame(s) observed", o.Frames)
	case o.SinceInput < quiet:
		return fmt.Errorf("idle premise not met: %.1f s since the last input, need %.0f s", o.SinceInput.Seconds(), quiet.Seconds())
	}
	return nil
}

// idleWakeFrameDiff counts differing pixels and bounds them, so a failed
// premise shows where the screen changed. A size change counts all of them.
func idleWakeFrameDiff(a, b *image.RGBA) (int, image.Rectangle) {
	if a.Bounds() != b.Bounds() {
		return max(a.Bounds().Dx()*a.Bounds().Dy(), b.Bounds().Dx()*b.Bounds().Dy()), a.Bounds().Union(b.Bounds())
	}
	count, box := 0, image.Rectangle{}
	for y := a.Bounds().Min.Y; y < a.Bounds().Max.Y; y++ {
		for x := a.Bounds().Min.X; x < a.Bounds().Max.X; x++ {
			if a.RGBAAt(x, y) != b.RGBAAt(x, y) {
				count++
				box = box.Union(image.Rect(x, y, x+1, y+1))
			}
		}
	}
	return count, box
}

func TestIdleWakePremise(t *testing.T) {
	for _, test := range []struct {
		name        string
		observation idleWakeObservation
		want        string
	}{
		{"idle", idleWakeObservation{SinceInput: 71 * time.Second, Frames: 2}, ""},
		{"observer error", idleWakeObservation{SinceInput: 71 * time.Second, Frames: 1, Err: errors.New("closed")}, "screen observer failed: closed"},
		{"screen changed", idleWakeObservation{SinceInput: 71 * time.Second, Frames: 3, Changed: 12, ChangedBox: image.Rect(0, 0, 4, 3), ChangedAt: 31500 * time.Millisecond}, "12 pixels in (0,0)-(4,3) changed 31.5 s after"},
		{"one frame", idleWakeObservation{SinceInput: 71 * time.Second, Frames: 1}, "only 1 frame(s) observed"},
		{"recent input", idleWakeObservation{SinceInput: 64 * time.Second, Frames: 2}, "64.0 s since the last input, need 70 s"},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := test.observation.premise(idleWakeQuiet)
			if test.want == "" {
				if err != nil {
					t.Fatalf("premise = %v, want nil", err)
				}
				return
			}
			if err == nil || !strings.HasPrefix(err.Error(), "idle premise not met: ") || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("premise = %v, want idle premise not met: ...%s", err, test.want)
			}
		})
	}
}

func TestIdleWakeFrameDiff(t *testing.T) {
	a := image.NewRGBA(image.Rect(0, 0, 4, 3))
	b := image.NewRGBA(image.Rect(0, 0, 4, 3))
	if got, box := idleWakeFrameDiff(a, b); got != 0 || !box.Empty() {
		t.Fatalf("identical frames differ in %d pixels in %v", got, box)
	}
	b.SetRGBA(1, 2, color.RGBA{R: 1, A: 255})
	b.SetRGBA(3, 0, color.RGBA{G: 1, A: 255})
	if got, box := idleWakeFrameDiff(a, b); got != 2 || box != image.Rect(1, 0, 4, 3) {
		t.Fatalf("diff = %d in %v, want 2 in (1,0)-(4,3)", got, box)
	}
	if got, box := idleWakeFrameDiff(a, image.NewRGBA(image.Rect(0, 0, 5, 3))); got != 15 || box != image.Rect(0, 0, 5, 3) {
		t.Fatalf("size change diff = %d in %v, want 15 in (0,0)-(5,3)", got, box)
	}
}
