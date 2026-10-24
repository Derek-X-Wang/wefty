package l1

import (
	"context"
	"database/sql"
	"errors"
	"net/http"
	"strconv"

	"github.com/Derek-X-Wang/wefty/contract"
)

const SeatCredentialHeader = "X-Wefty-Seat-Credential"

func (s *Server) getSeatLogs(w http.ResponseWriter, r *http.Request) {
	limit := DefaultLogPageLimit
	if value := r.URL.Query().Get("limit"); value != "" {
		var err error
		limit, err = strconv.Atoi(value)
		if err != nil {
			writeError(w, protocolError(contract.ErrorInvalidRequest, "invalid Seat log page limit"))
			return
		}
	}
	page, err := s.store.GetSeatLogs(r.Context(), identityFromRequest(r).NodeID, r.PathValue("seat_id"), r.Header.Get(SeatCredentialHeader), r.URL.Query().Get("cursor"), limit)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, page)
}
func (s *Server) markSeatLogRead(w http.ResponseWriter, r *http.Request) {
	var input SeatLogRead
	if err := decodeJSON(r, &input); err != nil {
		writeError(w, err)
		return
	}
	if err := s.store.MarkSeatLogRead(r.Context(), identityFromRequest(r).NodeID, r.PathValue("seat_id"), r.Header.Get(SeatCredentialHeader), input.Cursor); err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, input)
}

// A presented Seat credential never falls through to a broader Fabric
// principal, even on a route introduced by another feature later.
func (s *Server) seatCredentialProtocol(next, logs http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		secret := r.Header.Get(SeatCredentialHeader)
		if secret == "" {
			next.ServeHTTP(w, r)
			return
		}
		if _, err := s.authenticatedIdentity(w, r); err != nil {
			writeError(w, err)
			return
		}
		var id, reason string
		var ended sql.NullInt64
		err := s.store.withReadSnapshot(r.Context(), nil, func(ctx context.Context, reads readModel) error {
			rd := reads.(*databaseReads)
			return rd.q.QueryRowContext(ctx, `SELECT seat_id,ended_ns,reason FROM seat_logs WHERE credential_hash=? AND credential_hash!=''`, seatCredentialHash(secret)).Scan(&id, &ended, &reason)
		})
		if errors.Is(err, sql.ErrNoRows) {
			err = protocolError(contract.ErrorForbidden, "invalid Seat credential")
		}
		if err != nil {
			writeError(w, err)
			return
		}
		path := "/v1/seats/" + id
		if (r.Method == http.MethodGet && r.URL.Path == path+"/logs") || (r.Method == http.MethodPost && r.URL.Path == path+"/logs/read") {
			logs.ServeHTTP(w, r)
			return
		}
		if ended.Valid {
			writeError(w, protocolError(contract.ErrorCode("seat_ended"), "Seat ended: %s", reason))
			return
		}
		writeError(w, protocolError(contract.ErrorForbidden, "Seat credential does not authorize this operation"))
	})
}
