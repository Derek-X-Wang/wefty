// Package rfbclient implements short RFB 3.x calls. It requests only Raw
// encoding, supported by the default image's x11vnc and Wayland's neatvnc.
// The caller supplies the authenticated transport and its deadline.
package rfbclient

import (
	"encoding/binary"
	"fmt"
	"image"
	"image/color"
	"io"
)

type Client struct {
	wire          io.ReadWriter
	Width, Height int
}

func Open(wire io.ReadWriter) (*Client, error) {
	c := &Client{wire: wire}
	banner := make([]byte, 12)
	if _, err := io.ReadFull(wire, banner); err != nil {
		return nil, err
	}
	version := string(banner)
	if version != "RFB 003.003\n" && version != "RFB 003.007\n" && version != "RFB 003.008\n" {
		return nil, fmt.Errorf("unsupported RFB version")
	}
	if err := c.write(banner); err != nil {
		return nil, err
	}
	if version == "RFB 003.003\n" {
		b := make([]byte, 4)
		if _, err := io.ReadFull(wire, b); err != nil {
			return nil, err
		}
		if binary.BigEndian.Uint32(b) != 1 {
			return nil, fmt.Errorf("RFB server requires unsupported security")
		}
	} else {
		var count [1]byte
		if _, err := io.ReadFull(wire, count[:]); err != nil {
			return nil, err
		}
		if count[0] == 0 {
			return nil, fmt.Errorf("RFB server refused connection")
		}
		modes := make([]byte, int(count[0]))
		if _, err := io.ReadFull(wire, modes); err != nil {
			return nil, err
		}
		none := false
		for _, mode := range modes {
			none = none || mode == 1
		}
		if !none {
			return nil, fmt.Errorf("RFB server requires unsupported security")
		}
		if err := c.write([]byte{1}); err != nil {
			return nil, err
		}
		if version == "RFB 003.008\n" {
			var result [4]byte
			if _, err := io.ReadFull(wire, result[:]); err != nil {
				return nil, err
			}
			if binary.BigEndian.Uint32(result[:]) != 0 {
				return nil, fmt.Errorf("RFB security handshake refused")
			}
		}
	}
	if err := c.write([]byte{1}); err != nil {
		return nil, err
	} // shared desktop
	var init [24]byte
	if _, err := io.ReadFull(wire, init[:]); err != nil {
		return nil, err
	}
	c.Width, c.Height = int(binary.BigEndian.Uint16(init[:])), int(binary.BigEndian.Uint16(init[2:]))
	if c.Width < 1 || c.Height < 1 || c.Width > 4096 || c.Height > 4096 {
		return nil, fmt.Errorf("RFB screen dimensions out of bounds")
	}
	n := binary.BigEndian.Uint32(init[20:])
	if n > 1<<20 {
		return nil, fmt.Errorf("RFB desktop name too long")
	}
	if _, err := io.CopyN(io.Discard, wire, int64(n)); err != nil {
		return nil, err
	}
	// Explicit 32-bit little-endian true colour: B,G,R,padding.
	format := []byte{0, 0, 0, 0, 32, 24, 0, 1, 0, 255, 0, 255, 0, 255, 16, 8, 0, 0, 0, 0}
	if err := c.write(format); err != nil {
		return nil, err
	}
	if err := c.write([]byte{2, 0, 0, 1, 0, 0, 0, 0}); err != nil {
		return nil, err
	}
	return c, nil
}

func (c *Client) write(data []byte) error {
	for len(data) > 0 {
		n, err := c.wire.Write(data)
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
		data = data[n:]
	}
	return nil
}
func (c *Client) Pointer(x, y int, buttons byte) error {
	if x < 0 || y < 0 || x >= c.Width || y >= c.Height {
		return fmt.Errorf("pointer outside screen")
	}
	b := []byte{5, buttons, 0, 0, 0, 0}
	binary.BigEndian.PutUint16(b[2:], uint16(x))
	binary.BigEndian.PutUint16(b[4:], uint16(y))
	return c.write(b)
}
func (c *Client) Key(keysym uint32, down bool) error {
	b := []byte{4, 0, 0, 0, 0, 0, 0, 0}
	if down {
		b[1] = 1
	}
	binary.BigEndian.PutUint32(b[4:], keysym)
	return c.write(b)
}

// Frame requests a full update and assembles all its Raw rectangles. It never
// returns an incomplete framebuffer, including when a server splits updates.
func (c *Client) Frame() (*image.RGBA, error) { return c.frame(c.Width, c.Height) }

// Sync orders prior input before close using the smallest non-incremental
// framebuffer request. The separate view call still captures the full screen.
// Servers may enlarge replies within the real screen. Drain each update and
// return once the requested pixel is covered.
func (c *Client) Sync() error { _, err := c.frame(1, 1); return err }

func (c *Client) frame(width, height int) (*image.RGBA, error) {
	request := []byte{3, 0, 0, 0, 0, 0, 0, 0, 0, 0}
	binary.BigEndian.PutUint16(request[6:], uint16(width))
	binary.BigEndian.PutUint16(request[8:], uint16(height))
	if err := c.write(request); err != nil {
		return nil, err
	}
	frame := image.NewRGBA(image.Rect(0, 0, width, height))
	seen := make([]bool, width*height)
	remaining := len(seen)
	for remaining > 0 {
		var kind [1]byte
		if _, err := io.ReadFull(c.wire, kind[:]); err != nil {
			return nil, err
		}
		switch kind[0] {
		case 2:
			continue // bell
		case 3: // ServerCutText is ignored, bounded, and never logged.
			var header [7]byte
			if _, err := io.ReadFull(c.wire, header[:]); err != nil {
				return nil, err
			}
			n := binary.BigEndian.Uint32(header[3:])
			if n > 1<<20 {
				return nil, fmt.Errorf("RFB clipboard too large")
			}
			if _, err := io.CopyN(io.Discard, c.wire, int64(n)); err != nil {
				return nil, err
			}
			continue
		case 0:
		default:
			return nil, fmt.Errorf("unsupported RFB message %d", kind[0])
		}
		var update [3]byte
		if _, err := io.ReadFull(c.wire, update[:]); err != nil {
			return nil, err
		}
		for count := int(binary.BigEndian.Uint16(update[1:])); count > 0; count-- {
			var rect [12]byte
			if _, err := io.ReadFull(c.wire, rect[:]); err != nil {
				return nil, err
			}
			x, y, w, h := int(binary.BigEndian.Uint16(rect[:])), int(binary.BigEndian.Uint16(rect[2:])), int(binary.BigEndian.Uint16(rect[4:])), int(binary.BigEndian.Uint16(rect[6:]))
			if binary.BigEndian.Uint32(rect[8:]) != 0 {
				return nil, fmt.Errorf("RFB server ignored Raw encoding negotiation")
			}
			if w == 0 || h == 0 || x+w > c.Width || y+h > c.Height {
				return nil, fmt.Errorf("RFB rectangle outside screen")
			}
			row := make([]byte, w*4)
			for yy := y; yy < y+h; yy++ {
				if _, err := io.ReadFull(c.wire, row); err != nil {
					return nil, err
				}
				// Raw bytes outside the request are still consumed and checked,
				// but only the intersection belongs in the result and coverage map.
				if yy >= height {
					continue
				}
				for xx := x; xx < min(x+w, width); xx++ {
					offset := (xx - x) * 4
					frame.SetRGBA(xx, yy, color.RGBA{R: row[offset+2], G: row[offset+1], B: row[offset], A: 255})
					i := yy*width + xx
					if !seen[i] {
						seen[i] = true
						remaining--
					}
				}
			}
		}
	}
	return frame, nil
}
