package agent

import (
	"embed"
	"io/fs"
	"net/http"
	"strings"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/l1"
)

const seatViewerSessionPath = "/viewer-session"

//go:embed viewer
var seatViewerFiles embed.FS

type seatViewerSession struct {
	Bearer           string `json:"bearer"`
	RemainingSeconds int    `json:"remaining_seconds"`
}

type seatViewerStatus struct {
	Driver           string `json:"driver"`
	RemainingSeconds int    `json:"remaining_seconds"`
}

func remainingSeatViewerSeconds(now, end time.Time) int {
	return max(0, int((end.Sub(now)+time.Second-1)/time.Second))
}

func isSeatViewerResource(path string) bool {
	return path == "/viewer" || path == "/viewer-status" || strings.HasPrefix(path, "/viewer/")
}

// Called only after Fabric authentication and the installed administrator policy.
// No cookie, query parameter, HTML substitution or persistent browser storage
// carries a capability. The status poll cannot extend the live session's cap.
func (f *computerFrontDoor) serveSeatViewerResource(w http.ResponseWriter, r *http.Request, identity fabric.Identity) {
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("Referrer-Policy", "no-referrer")
	w.Header().Set("Content-Security-Policy", "default-src 'self'; script-src 'self'; style-src 'self' 'unsafe-inline'; connect-src 'self' ws: wss:; img-src 'self' data:; object-src 'none'; base-uri 'none'; frame-ancestors 'none'")
	if r.URL.Path == "/viewer-status" {
		if f.config.seatViewerStatus != nil {
			if err := f.config.seatViewerStatus(r.Context()); err != nil {
				writeSeatScreenError(w, err)
				return
			}
		}
		token := r.Header.Get(computerControlTokenHeader)
		f.mu.Lock()
		active := f.tokens[token]
		closed, wasClosed := f.closedTokens[token]
		f.mu.Unlock()
		if active == nil && wasClosed && sameComputerSessionIdentity(closed.identity, identity) {
			writeComputerControlError(w, http.StatusGone, contract.APIError{Code: contract.ErrorTakeoverSessionEnded, Message: "Viewing session ended; reconnect"}, &closed.receipt)
			return
		}
		if active == nil || !sameComputerSessionIdentity(active.identity, identity) {
			writeComputerControlError(w, http.StatusUnauthorized, contract.APIError{Code: contract.ErrorUnauthorized, Message: "Live viewing session is required"}, nil)
			return
		}
		if active.handle.session.Err() != nil || !f.config.clock.Now().Before(active.expiresAt) {
			receipt := active.handle.terminalReceipt(computerSessionEndReason(active.handle.session, l1.ComputerTakeoverSessionCapExpired))
			writeComputerControlError(w, http.StatusGone, contract.APIError{Code: contract.ErrorTakeoverSessionEnded, Message: "Viewing session ended; reconnect"}, &receipt)
			return
		}
		driver := "AI agent"
		if tenure, ok := active.handle.tenure.(*controllerTenure); ok {
			receipt := tenure.receipt(active.handle.id)
			if receipt.HumanDriving {
				driver = "a person"
				if receipt.HolderSessionID == active.handle.id {
					driver = "you"
				}
			}
		}
		writeComputerControlJSON(w, http.StatusOK, seatViewerStatus{Driver: driver, RemainingSeconds: remainingSeatViewerSeconds(f.config.clock.Now(), active.expiresAt)})
		return
	}
	path := strings.TrimPrefix(r.URL.Path, "/viewer/")
	if r.URL.Path == "/viewer" {
		path = "index.html"
	}
	// ReadFile never lists directories or redirects to unauthenticated paths.
	if !fs.ValidPath(path) {
		http.NotFound(w, r)
		return
	}
	body, err := seatViewerFiles.ReadFile("viewer/" + path)
	if err != nil {
		http.NotFound(w, r)
		return
	}
	switch {
	case strings.HasSuffix(path, ".html"):
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
	case strings.HasSuffix(path, ".js"):
		w.Header().Set("Content-Type", "text/javascript; charset=utf-8")
	}
	_, _ = w.Write(body)
}
