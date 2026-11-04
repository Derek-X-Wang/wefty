package main

import (
	"time"

	"github.com/Derek-X-Wang/wefty/internal/rfbclient"
)

// A speculative open carries its own metadata. Only the caller may apply it
// to reply state or timing, and only if this connection is selected.
type seatScreenOpenResult struct {
	client *rfbclient.Client
	close  func()
	unread *int
	marks  map[string]time.Time
	err    error
}

// Context-only observation seam used by protocol tests, never set by the CLI.
type seatScreenSpareTestHookKey struct{}
