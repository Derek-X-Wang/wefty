package rfbclient

import (
	"bytes"
	"encoding/binary"
	"image"
	"image/color"
	"io"
	"testing"
)

// Sync shares this decoder but does not expose its cropped image.
func TestFrameClipsExpandedRawReply(t *testing.T) {
	for _, width := range []uint16{64, 1280} {
		init := make([]byte, 24)
		binary.BigEndian.PutUint16(init, 1280)
		binary.BigEndian.PutUint16(init[2:], 800)
		b := append([]byte("RFB 003.003\n\x00\x00\x00\x01"), init...)
		header := make([]byte, 16)
		header[3] = 1
		binary.BigEndian.PutUint16(header[8:], width)
		binary.BigEndian.PutUint16(header[10:], 64)
		b = append(b, header...)
		pixels := bytes.Repeat([]byte{0, 255, 0, 0}, int(width)*64)
		copy(pixels, []byte{0, 0, 255, 0})
		reader := bytes.NewReader(append(b, pixels...))
		c, err := Open(struct {
			io.Reader
			io.Writer
		}{reader, io.Discard})
		if err != nil {
			t.Fatal(err)
		}
		im, err := c.frame(1, 1)
		if err != nil {
			t.Fatalf("reply width=%d: %v", width, err)
		}
		if im.Bounds() != image.Rect(0, 0, 1, 1) || im.RGBAAt(0, 0) != (color.RGBA{255, 0, 0, 255}) || reader.Len() != 0 {
			t.Fatalf("reply width=%d: crop or byte drain changed", width)
		}
	}
}
