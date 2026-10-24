package l1

import (
	"context"
	"net/http"
)

func (s *Store) GetSeatRequestForNode(ctx context.Context, identity, id string, request SeatNodeRequest) (SeatRequest, error) {
	var value SeatRequest
	var expired bool
	err := s.withAgentReadSnapshot(ctx, func(ctx context.Context, reads readModel) error {
		q := reads.(*databaseReads).q
		if err := authorizeSeatRequest(ctx, q, identity, id, request.NodeID, request.BootSessionID); err != nil {
			return err
		}
		var err error
		value, err = readSeatRequest(ctx, q, id)
		if err == nil {
			expired, err = seatApprovalNeedsExpiry(ctx, q, value, reads.(*databaseReads).now(), s.clock)
		}
		return err
	})
	if err == nil && expired {
		return s.expireSeatApprovalRead(ctx, id, identity, &request)
	}
	return value, err
}
func (s *Server) readSeatRequest(w http.ResponseWriter, r *http.Request) {
	var request SeatNodeRequest
	if err := decodeJSON(r, &request); err != nil {
		writeError(w, err)
		return
	}
	value, err := s.store.GetSeatRequestForNode(r.Context(), identityFromRequest(r).NodeID, r.PathValue("seat_id"), request)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, value)
}
func (s *Server) reserveSeatRoom(w http.ResponseWriter, r *http.Request) {
	var request SeatRoomReservationRequest
	if err := decodeJSON(r, &request); err != nil {
		writeError(w, err)
		return
	}
	value, err := s.store.ReserveSeatRoom(r.Context(), identityFromRequest(r).NodeID, r.PathValue("seat_id"), request)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, value)
}
func (s *Server) startSeatRequest(w http.ResponseWriter, r *http.Request) {
	var request SeatStartRequest
	if err := decodeJSON(r, &request); err != nil {
		writeError(w, err)
		return
	}
	value, err := s.store.StartSeatRequest(r.Context(), identityFromRequest(r).NodeID, r.PathValue("seat_id"), request)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, value)
}
func (s *Server) endSeatRequest(w http.ResponseWriter, r *http.Request) {
	var request SeatEndRequest
	if err := decodeJSON(r, &request); err != nil {
		writeError(w, err)
		return
	}
	value, err := s.store.EndSeatRequest(r.Context(), identityFromRequest(r).NodeID, r.PathValue("seat_id"), request)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, value)
}
func (s *Server) reportComputerRoomMemory(w http.ResponseWriter, r *http.Request) {
	var request SeatRoomMemoryReport
	if err := decodeJSON(r, &request); err != nil {
		writeError(w, err)
		return
	}
	value, err := s.store.ReportComputerRoomMemory(r.Context(), identityFromRequest(r).NodeID, r.PathValue("computer_id"), r.PathValue("job_id"), r.PathValue("attempt_id"), request)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, value)
}

func (s *Server) reportComputerSeatAttemptAbsence(w http.ResponseWriter, r *http.Request) {
	var request ComputerSeatAttemptAbsenceReport
	if err := decodeJSON(r, &request); err != nil {
		writeError(w, err)
		return
	}
	value, err := s.store.ReportComputerSeatAttemptAbsence(r.Context(), identityFromRequest(r).NodeID, r.PathValue("computer_id"), r.PathValue("job_id"), r.PathValue("attempt_id"), request)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, value)
}
