package ocicontrol

import (
	"context"
	"net"
	"net/http"
	"os"
	"sync"

	"github.com/Derek-X-Wang/wefty/contract"
)

type controlPeerKey struct{}
type controlPeer struct {
	connection net.Conn
	once       sync.Once
	err        error
}

func (s *Server) peerContext(ctx context.Context, c net.Conn) context.Context {
	return context.WithValue(ctx, controlPeerKey{}, &controlPeer{connection: c})
}

// HTTP runs each connection in its own goroutine. The ancestry walk belongs
// here, so one slow or refused caller cannot serialize the accept loop.
func (s *Server) authorizePeer(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !s.refuseDescendants {
			next.ServeHTTP(w, r)
			return
		}
		peer, ok := r.Context().Value(controlPeerKey{}).(*controlPeer)
		if !ok {
			writeControlError(w, 403, contract.ErrorForbidden, "cannot verify this caller; run this command from the owner's own shell")
			return
		}
		peer.once.Do(func() {
			pid, err := s.peerPID(peer.connection)
			if err != nil {
				peer.err = operatorPeerError(0, os.Getpid(), s.parentPID)
				return
			}
			peer.err = operatorPeerError(pid, os.Getpid(), s.parentPID)
		})
		if peer.err != nil {
			writeControlError(w, 403, contract.ErrorForbidden, peer.err.Error())
			return
		}
		next.ServeHTTP(w, r)
	})
}
