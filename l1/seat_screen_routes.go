package l1

import "net/http"

func (s *Server) authorizeSeatScreen(w http.ResponseWriter, r *http.Request) {
	var input SeatScreenAuthorityInput
	if err := decodeJSON(r, &input); err != nil {
		writeError(w, err)
		return
	}
	value, err := s.store.AuthorizeSeatScreen(r.Context(), identityFromRequest(r).NodeID, r.PathValue("seat_id"), input)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, value)
}
func (s *Server) recordSeatScreenEvent(w http.ResponseWriter, r *http.Request) {
	var input SeatScreenEventRequest
	if err := decodeJSON(r, &input); err != nil {
		writeError(w, err)
		return
	}
	value, err := s.store.RecordSeatScreenEvent(r.Context(), identityFromRequest(r).NodeID, r.PathValue("seat_id"), input)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, value)
}
