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
	"time"
)

type Client struct {
	wire          io.ReadWriter
	pending       <-chan frameUpdate
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

// SyncAfterInput orders input and reports changes carried by the Sync reply.
// LibVNCServer 0.9.14 rfbserver.c:3094-3105 retains damage outside the request;
// neatvnc 0.9.1 server.c:865-880 flushes all accumulated damage into this reply.
// Count expanded rectangles now, then incremental requests can observe any
// remaining or subsequent damage without losing the changes flushed by Sync.
func (c *Client) SyncAfterInput(baseline color.RGBA) (bool, error) {
	frame, outside, err := c.frameWithOutside(1, 1)
	if err != nil {
		return false, err
	}
	return outside || frame.RGBAAt(0, 0) != baseline, nil
}

// WaitForChange waits for a non-empty incremental update. Call Frame before
// input to clear this connection's initial damage over the whole screen.
// Only one request is outstanding: consume its entire reply before re-requesting.
// After success, Frame may capture on this transport. After a timeout, close
// the transport and capture on a fresh connection; a reply may still be pending.
// Do not set a read deadline here: WebSocket deadlines close the transport.
func (c *Client) WaitForChange(limit time.Duration) (bool, error) {
	if c.pending != nil {
		return false, fmt.Errorf("pending framebuffer read: close transport after WaitForChange timeout")
	}
	timer := time.NewTimer(limit)
	defer timer.Stop()
	for {
		if err := c.request(c.Width, c.Height, true); err != nil {
			return false, err
		}
		result := make(chan frameUpdate, 1)
		go func() { result <- c.readUpdate() }()
		c.pending = result
		select {
		case update := <-result:
			c.pending = nil
			if update.err != nil {
				return false, update.err
			}
			if update.changed {
				return true, nil
			}
		case <-timer.C:
			return false, nil
		}
		// Empty replies are not changes. Pace re-requests to avoid a busy loop.
		pause := time.NewTimer(20 * time.Millisecond)
		select {
		case <-pause.C:
		case <-timer.C:
			pause.Stop()
			return false, nil
		}
	}
}

func (c *Client) request(width, height int, incremental bool) error {
	request := []byte{3, 0, 0, 0, 0, 0, 0, 0, 0, 0}
	if incremental {
		request[1] = 1
	}
	binary.BigEndian.PutUint16(request[6:], uint16(width))
	binary.BigEndian.PutUint16(request[8:], uint16(height))
	return c.write(request)
}

func (c *Client) frame(width, height int) (*image.RGBA, error) {
	frame, _, err := c.frameWithOutside(width, height)
	return frame, err
}

func (c *Client) frameWithOutside(width, height int) (*image.RGBA, bool, error) {
	if c.pending != nil {
		return nil, false, fmt.Errorf("pending framebuffer read: close transport after WaitForChange timeout")
	}
	if err := c.request(width, height, false); err != nil {
		return nil, false, err
	}
	frame := image.NewRGBA(image.Rect(0, 0, width, height))
	seen := make([]bool, width*height)
	remaining := len(seen)
	outside := false
	for remaining > 0 {
		update := frameUpdate{frame: frame, seen: seen}
		if err := c.readRectangles(width, height, &update); err != nil {
			return nil, false, err
		}
		remaining -= update.covered
		outside = outside || update.outside
	}
	return frame, outside, nil
}

type frameUpdate struct {
	frame   *image.RGBA
	seen    []bool
	changed bool
	outside bool
	covered int
	err     error
}

func (c *Client) readUpdate() (result frameUpdate) {
	result.err = c.readRectangles(0, 0, &result)
	return result
}

func (c *Client) readRectangles(width, height int, result *frameUpdate) error {
	for {
		var kind [1]byte
		if _, err := io.ReadFull(c.wire, kind[:]); err != nil {
			return err
		}
		switch kind[0] {
		case 2:
			continue // bell
		case 3: // ServerCutText is ignored, bounded, and never logged.
			var header [7]byte
			if _, err := io.ReadFull(c.wire, header[:]); err != nil {
				return err
			}
			n := binary.BigEndian.Uint32(header[3:])
			if n > 1<<20 {
				return fmt.Errorf("RFB clipboard too large")
			}
			if _, err := io.CopyN(io.Discard, c.wire, int64(n)); err != nil {
				return err
			}
			continue
		case 0:
		default:
			return fmt.Errorf("unsupported RFB message %d", kind[0])
		}
		var update [3]byte
		if _, err := io.ReadFull(c.wire, update[:]); err != nil {
			return err
		}
		count := int(binary.BigEndian.Uint16(update[1:]))
		if count > 0 {
			result.changed = true
		}
		for ; count > 0; count-- {
			var rect [12]byte
			if _, err := io.ReadFull(c.wire, rect[:]); err != nil {
				return err
			}
			x, y, w, h := int(binary.BigEndian.Uint16(rect[:])), int(binary.BigEndian.Uint16(rect[2:])), int(binary.BigEndian.Uint16(rect[4:])), int(binary.BigEndian.Uint16(rect[6:]))
			if binary.BigEndian.Uint32(rect[8:]) != 0 {
				return fmt.Errorf("RFB server ignored Raw encoding negotiation")
			}
			if w == 0 || h == 0 || x+w > c.Width || y+h > c.Height {
				return fmt.Errorf("RFB rectangle outside screen")
			}
			if x+w > width || y+h > height {
				result.outside = true
			}
			row := make([]byte, w*4)
			for yy := y; yy < y+h; yy++ {
				if _, err := io.ReadFull(c.wire, row); err != nil {
					return err
				}
				if yy >= height {
					continue
				}
				for xx := x; xx < min(x+w, width); xx++ {
					offset := (xx - x) * 4
					result.frame.SetRGBA(xx, yy, color.RGBA{R: row[offset+2], G: row[offset+1], B: row[offset], A: 255})
					if !result.seen[yy*width+xx] {
						result.seen[yy*width+xx] = true
						result.covered++
					}
				}
			}
		}
		return nil
	}
}
