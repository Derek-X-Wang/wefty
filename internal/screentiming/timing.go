// Package screentiming provides opt-in screen transport diagnostics for lanes.
// Phase names are constants at call sites; screen bytes and input never enter it.
package screentiming

import (
	"log"
	"os"
	"time"
)

func Start(component string) func(string) {
	if os.Getenv("WEFTY_SEAT_SCREEN_TIMING") != "1" {
		return func(string) {}
	}
	started := time.Now()
	return func(phase string) {
		log.Printf("Seat screen timing component=%s start_unix_ns=%d phase=%s elapsed_ms=%.3f", component, started.UnixNano(), phase, float64(time.Since(started).Microseconds())/1000)
	}
}
