package l1

import (
	"context"
	"net/http"

	"github.com/Derek-X-Wang/wefty/contract"
)

func (s *Store) ReleaseSeat(ctx context.Context, id, secret string) (SeatCallResult, error) {
	w, err := s.beginWriteTransaction(ctx, nil)
	if err != nil {
		return SeatCallResult{}, err
	}
	defer w.rollback()
	authErr := s.authorizeLiveSeat(ctx, w.tx, id, secret, w.at)
	value, readErr := readSeatRequest(ctx, w.tx, id)
	replay := readErr == nil && value.Reason == "released" && (value.State == SeatEnding || value.State == SeatEnded) && errorCode(authErr) == "seat_ended"
	if authErr != nil && !replay {
		err := authErr
		if errorCode(err) == "seat_ended" {
			if e := w.commit(); e != nil {
				return SeatCallResult{}, e
			}
		}
		return SeatCallResult{}, err
	}
	if readErr != nil {
		return SeatCallResult{}, readErr
	}
	if !replay {
		if err := setSeatLifecycle(ctx, w.tx, id, SeatEnding, "released", w.at); err != nil {
			return SeatCallResult{}, err
		}
		if err := removeLifecycleSeat(ctx, w.tx, value); err != nil {
			return SeatCallResult{}, err
		}
		if _, err := s.computerIdleDue(ctx, w.tx, value.ComputerID, true); err != nil {
			return SeatCallResult{}, err
		}
	}
	var caller string
	if err := w.tx.QueryRowContext(ctx, `SELECT caller_identity FROM seat_request_calls WHERE seat_id=?`, id).Scan(&caller); err != nil {
		return SeatCallResult{}, err
	}
	if err := w.commit(); err != nil {
		return SeatCallResult{}, err
	}
	return s.GetSeatCall(ctx, caller, id)
}
func (s *Server) releaseSeat(w http.ResponseWriter, r *http.Request) {
	value, err := s.store.ReleaseSeat(r.Context(), r.PathValue("seat_id"), r.Header.Get(SeatCredentialHeader))
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, value)
}

// This read may commit an expiry. It never acknowledges or renews activity.
func (s *Store) checkSeatCredentialEnd(ctx context.Context, id string) (SeatRequest, error) {
	var value SeatRequest
	var change seatLifeChange
	err := s.withReadSnapshot(ctx, nil, func(ctx context.Context, reads readModel) error {
		q := reads.(*databaseReads).q
		var err error
		value, err = readSeatRequest(ctx, q, id)
		if err != nil || value.State != SeatLive {
			return err
		}
		change, err = s.planSeatLife(ctx, q, id)
		return err
	})
	if err != nil || !change.needsWrite() {
		return value, err
	}
	w, err := s.beginWriteTransaction(ctx, nil)
	if err != nil {
		return SeatRequest{}, err
	}
	defer w.rollback()
	if err := s.checkSeatLifeTxIfLive(ctx, w.tx, id, w.at); err != nil {
		return SeatRequest{}, err
	}
	value, err = readSeatRequest(ctx, w.tx, id)
	if err != nil {
		return value, err
	}
	return value, w.commit()
}

// A retained log remains readable after expiry. A call renews it only if live.
func (s *Store) seatLogCallHappened(ctx context.Context, id, secret string) error {
	err := s.SeatCallHappened(ctx, id, secret)
	if errorCode(err) == contract.ErrorCode("seat_ended") {
		return nil
	}
	return err
}
