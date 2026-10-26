package main

import (
	"io"
	"os"
	"time"
)

// Test-only timing hook. It records phase names and times, never RFB payloads.
type seatScreenTiming struct {
	start  time.Time
	phases map[string]float64
}

func newSeatScreenTiming(start time.Time) *seatScreenTiming {
	if os.Getenv("WEFTY_SEAT_SCREEN_TIMING") != "1" {
		return nil
	}
	return &seatScreenTiming{start: start, phases: map[string]float64{"cli_dispatch": 0}}
}

func (t *seatScreenTiming) mark(name string) {
	if t != nil {
		if _, exists := t.phases[name]; !exists {
			t.phases[name] = float64(time.Since(t.start).Microseconds()) / 1000
		}
	}
}

func (t *seatScreenTiming) debug() map[string]any {
	return map[string]any{"screen_timing_start_unix_ns": t.start.UnixNano(), "screen_timing_ms": t.phases}
}

type seatScreenBannerWire struct {
	io.ReadWriter
	remaining int
	received  func()
}

func (w *seatScreenBannerWire) Read(p []byte) (int, error) {
	n, err := w.ReadWriter.Read(p)
	if w.remaining > 0 {
		w.remaining -= n
		if w.remaining <= 0 {
			w.received()
		}
	}
	return n, err
}
