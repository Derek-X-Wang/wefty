package l3

import (
	"net/http"

	"github.com/Derek-X-Wang/wefty/contract"
)

func (s *Server) getPeerHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	writeJSON(w, http.StatusOK, s.peerProbe.Check(r.Context()))
}

func (s *Server) peerPing(w http.ResponseWriter, r *http.Request) {
	if identityFromRequest(r).NodeID != s.controlPlaneNodeID {
		writeError(w, protocolError(contract.ErrorForbidden, "only the configured peer may ping this service"))
		return
	}
	writeJSON(w, http.StatusOK, contract.PeerPong{OK: true})
}
