package l1

import (
	"net/http"
	"strings"

	"github.com/Derek-X-Wang/wefty/contract"
)

func (s *Server) getPeerHealth(w http.ResponseWriter, r *http.Request) {
	if strings.TrimSpace(r.Header.Get("Authorization")) != "" {
		writeError(w, protocolError(contract.ErrorPrincipalForbidden, "bearer tokens are not authorized for peer health"))
		return
	}
	w.Header().Set("Cache-Control", "no-store")
	writeJSON(w, http.StatusOK, s.peerProbe.Check(r.Context()))
}

// This route authenticates only the configured ledger, without person
// observation, leases, renewal, or a database read.
func (s *Server) peerPing(w http.ResponseWriter, r *http.Request) {
	identity, err := s.authenticatedIdentity(w, r)
	if err != nil {
		writeError(w, err)
		return
	}
	if identity.NodeID != s.runLedgerNodeID {
		writeError(w, protocolError(contract.ErrorPrincipalForbidden, "only the configured peer may ping this service"))
		return
	}
	writeJSON(w, http.StatusOK, contract.PeerPong{OK: true})
}
