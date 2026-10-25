package l1

import "net/http"

func (s *Server) getSeatCopyTarget(w http.ResponseWriter, r *http.Request) {
	value, err := s.store.GetSeatCopyTarget(r.Context(), r.PathValue("seat_id"), r.Header.Get(SeatCredentialHeader))
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, value)
}
func (s *Server) authorizeSeatCopy(w http.ResponseWriter, r *http.Request) {
	var input SeatCopyAuthorityInput
	if err := decodeJSON(r, &input); err != nil {
		writeError(w, err)
		return
	}
	value, err := s.store.AuthorizeSeatCopy(r.Context(), identityFromRequest(r).NodeID, r.PathValue("seat_id"), input)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, value)
}
func (s *Server) recordSeatCopy(w http.ResponseWriter, r *http.Request) {
	var input SeatCopyReceipt
	if err := decodeJSON(r, &input); err != nil {
		writeError(w, err)
		return
	}
	if err := s.store.RecordSeatCopy(r.Context(), identityFromRequest(r).NodeID, r.PathValue("seat_id"), input); err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, struct{}{})
}

func (s *Server) reportSeatCopyOutcome(w http.ResponseWriter, r *http.Request) {
	var input SeatCopyOutcomeReceipt
	if err := decodeJSON(r, &input); err != nil {
		writeError(w, err)
		return
	}
	if err := s.store.ReportSeatCopyOutcome(r.Context(), r.PathValue("seat_id"), r.Header.Get(SeatCredentialHeader), input); err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, struct{}{})
}
