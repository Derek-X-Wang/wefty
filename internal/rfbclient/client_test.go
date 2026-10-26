package rfbclient_test

import (
	"bytes"
	"encoding/binary"
	"image/color"
	"io"
	"reflect"
	"strings"
	"testing"

	"github.com/Derek-X-Wang/wefty/internal/rfbclient"
)

type wire struct {
	io.Reader
	written bytes.Buffer
}

func (w *wire) Write(p []byte) (int, error) { return w.written.Write(p) }
func handshake(version string, width, height uint16, security byte) []byte {
	b := []byte("RFB 003." + version + "\n")
	if version == "003" {
		b = append(b, 0, 0, 0, security)
	} else {
		b = append(b, 1, security)
		if version == "008" {
			b = append(b, 0, 0, 0, 0)
		}
	}
	init := make([]byte, 24)
	binary.BigEndian.PutUint16(init, width)
	binary.BigEndian.PutUint16(init[2:], height)
	binary.BigEndian.PutUint32(init[20:], 4)
	return append(append(b, init...), []byte("seat")...)
}
func update(rectangles ...[]byte) []byte {
	b := []byte{0, 0, 0, byte(len(rectangles))}
	for _, r := range rectangles {
		b = append(b, r...)
	}
	return b
}
func rectangle(x, y, w, h uint16, encoding uint32, pixels ...byte) []byte {
	b := make([]byte, 12)
	for i, n := range []uint16{x, y, w, h} {
		binary.BigEndian.PutUint16(b[i*2:], n)
	}
	binary.BigEndian.PutUint32(b[8:], encoding)
	return append(b, pixels...)
}
func TestHandshakeVersionsAndSecurity(t *testing.T) {
	for _, tc := range []struct {
		version  string
		security byte
		refuse   bool
	}{{"003", 1, false}, {"007", 1, false}, {"008", 1, false}, {"003", 2, true}, {"007", 2, true}, {"008", 2, true}} {
		t.Run(tc.version+string(rune('0'+tc.security)), func(t *testing.T) {
			w := &wire{Reader: bytes.NewReader(handshake(tc.version, 2, 2, tc.security))}
			c, err := rfbclient.Open(w)
			if tc.refuse {
				if err == nil || !strings.Contains(err.Error(), "unsupported security") {
					t.Fatalf("security accepted: %v", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if c.Width != 2 || c.Height != 2 {
				t.Fatalf("dimensions: %dx%d", c.Width, c.Height)
			}
			prefix := []byte("RFB 003." + tc.version + "\n")
			if tc.version != "003" {
				prefix = append(prefix, 1)
			}
			prefix = append(prefix, 1)
			want := append(prefix, []byte{0, 0, 0, 0, 32, 24, 0, 1, 0, 255, 0, 255, 0, 255, 16, 8, 0, 0, 0, 0, 2, 0, 0, 1, 0, 0, 0, 0}...)
			if !bytes.Equal(w.written.Bytes(), want) {
				t.Fatalf("client handshake bytes %x want %x", w.written.Bytes(), want)
			}
		})
	}
}
func TestFrameSplitUpdatesAndMessages(t *testing.T) {
	b := handshake("008", 2, 2, 1)
	b = append(b, update(rectangle(0, 0, 1, 1, 0, 0, 0, 255, 0), rectangle(1, 0, 1, 1, 0, 0, 255, 0, 0))...)
	b = append(b, 2, 3, 0, 0, 0, 0, 0, 0, 4, 'n', 'o', 't', 'e') // Bell and ServerCutText
	b = append(b, update(rectangle(0, 1, 2, 1, 0, 255, 0, 0, 0, 255, 255, 255, 0))...)
	// Limit each transport read to one byte, independently of RFB messages.
	w := &wire{Reader: oneByteReader{bytes.NewReader(b)}}
	c, err := rfbclient.Open(w)
	if err != nil {
		t.Fatal(err)
	}
	im, err := c.Frame()
	if err != nil {
		t.Fatal(err)
	}
	for i, want := range []color.RGBA{{255, 0, 0, 255}, {0, 255, 0, 255}, {0, 0, 255, 255}, {255, 255, 255, 255}} {
		if got := im.RGBAAt(i%2, i/2); got != want {
			t.Errorf("pixel %d=%v want %v", i, got, want)
		}
	}
	if !bytes.HasSuffix(w.written.Bytes(), []byte{3, 0, 0, 0, 0, 0, 0, 2, 0, 2}) {
		t.Fatal("missing full framebuffer request")
	}
}

type oneByteReader struct{ io.Reader }

func (r oneByteReader) Read(p []byte) (int, error) {
	if len(p) > 1 {
		p = p[:1]
	}
	return r.Reader.Read(p)
}
func TestFailClosedFrameAndSize(t *testing.T) {
	for _, tc := range []struct {
		name          string
		width, height uint16
		message       []byte
		reason        string
	}{
		{"large width", 4097, 2, nil, "dimensions"}, {"large height", 2, 4097, nil, "dimensions"},
		{"non Raw", 2, 2, update(rectangle(0, 0, 2, 2, 1)), "Raw"},
		{"outside", 2, 2, update(rectangle(1, 0, 2, 1, 0)), "outside"},
		{"zero width", 2, 2, update(rectangle(0, 0, 0, 1, 0)), "outside"},
		{"zero height", 2, 2, update(rectangle(0, 0, 1, 0, 0)), "outside"},
		{"unknown message", 2, 2, []byte{99}, "unsupported RFB message"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := &wire{Reader: bytes.NewReader(append(handshake("008", tc.width, tc.height, 1), tc.message...))}
			c, err := rfbclient.Open(w)
			if err == nil {
				_, err = c.Frame()
			}
			if err == nil || !strings.Contains(err.Error(), tc.reason) {
				t.Fatalf("fail closed: %v", err)
			}
		})
	}
}
func TestKeys(t *testing.T) {
	for _, tc := range []struct {
		text string
		want []uint32
		fail bool
	}{{"é", []uint32{0xe9}, false}, {"世", []uint32{0x01004e16}, false}, {"\n\t", []uint32{0xff0d, 0xff09}, false}, {string([]byte{0xff}), nil, true}, {"\x00", nil, true}} {
		got, err := rfbclient.TextKeys(tc.text)
		if (err != nil) != tc.fail || !reflect.DeepEqual(got, tc.want) {
			t.Errorf("TextKeys %q=%x err=%v", tc.text, got, err)
		}
	}
	for _, tc := range []struct {
		combo string
		want  []uint32
		fail  bool
	}{{"F1", []uint32{0xffbe}, false}, {"é", []uint32{0xe9}, false}, {"世", []uint32{0x01004e16}, false}, {"ctrl+shift+Tab", []uint32{0xffe3, 0xffe1, 0xff09}, false}, {"plus", []uint32{0x2b}, false}, {"ctrl+ctrl+l", nil, true}, {"ctrl+", nil, true}} {
		got, err := rfbclient.ComboKeys(tc.combo)
		if (err != nil) != tc.fail || (!tc.fail && !reflect.DeepEqual(got, tc.want)) {
			t.Errorf("ComboKeys %q=%x err=%v", tc.combo, got, err)
		}
	}
}

func TestSyncRequestsOnlyOnePixel(t *testing.T) {
	b := append(handshake("008", 1280, 800, 1), update(rectangle(0, 0, 1, 1, 0, 0, 0, 255, 0))...)
	w := &wire{Reader: bytes.NewReader(b)}
	c, err := rfbclient.Open(w)
	if err != nil {
		t.Fatal(err)
	}
	if err = c.Sync(); err != nil {
		t.Fatal(err)
	}
	if !bytes.HasSuffix(w.written.Bytes(), []byte{3, 0, 0, 0, 0, 0, 0, 1, 0, 1}) {
		t.Fatalf("sync requested more than one pixel: %x", w.written.Bytes())
	}
}

func TestSyncLargerUpdatesPreserveFrameAndScreenBounds(t *testing.T) {
	for _, tc := range []struct {
		name          string
		width, height uint16
	}{{"tile", 64, 64}, {"full screen", 1280, 800}} {
		t.Run(tc.name, func(t *testing.T) {
			b := handshake("008", 1280, 800, 1)
			// The first rectangle does not cover the requested pixel. The next
			// covers it and extends beyond the request; all rectangles must drain.
			b = append(b, update(
				rectangle(64, 64, 1, 1, 0, 0, 0, 255, 0),
				rectangle(0, 0, tc.width, tc.height, 0, bytes.Repeat([]byte{0, 255, 0, 0}, int(tc.width)*int(tc.height))...),
				rectangle(100, 100, 1, 1, 0, 0, 0, 255, 0),
			)...)
			b = append(b, update(rectangle(0, 0, 1280, 800, 0, bytes.Repeat([]byte{255, 0, 0, 0}, 1280*800)...))...)
			// Enlarged replies are permitted only within ServerInit's dimensions.
			b = append(b, update(rectangle(0, 0, 1, 1, 0, 0, 0, 255, 0), rectangle(1280, 0, 1, 1, 0))...)
			w := &wire{Reader: bytes.NewReader(b)}
			c, err := rfbclient.Open(w)
			if err != nil {
				t.Fatal(err)
			}
			if err := c.Sync(); err != nil {
				t.Fatalf("legal enlarged sync reply: %v", err)
			}
			im, err := c.Frame()
			if err != nil {
				t.Fatalf("frame after enlarged sync reply: %v", err)
			}
			if im.Bounds().Dx() != 1280 || im.Bounds().Dy() != 800 || im.RGBAAt(0, 0) != (color.RGBA{0, 0, 255, 255}) || im.RGBAAt(1279, 799) != (color.RGBA{0, 0, 255, 255}) {
				t.Fatal("full screenshot pixels changed after enlarged sync reply")
			}
			if err := c.Sync(); err == nil || !strings.Contains(err.Error(), "outside screen") {
				t.Fatalf("rectangle outside real screen accepted: %v", err)
			}
		})
	}
}
