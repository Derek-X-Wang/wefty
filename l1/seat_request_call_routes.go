package l1

import "net/http"

func (s *Server) createSeatCall(w http.ResponseWriter, r *http.Request) {
	var input SeatCallInput
	if err := decodeJSON(r, &input); err != nil {
		writeError(w, err)
		return
	}
	value, err := s.store.CreateSeatCall(r.Context(), identityFromRequest(r).NodeID, input)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, value)
}
func (s *Server) getSeatCall(w http.ResponseWriter, r *http.Request) {
	value, err := s.store.GetSeatCall(r.Context(), identityFromRequest(r).NodeID, r.PathValue("seat_id"))
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, value)
}
func (s *Server) pollSeatCalls(w http.ResponseWriter, r *http.Request) {
	var input SeatCallPoll
	if err := decodeJSON(r, &input); err != nil {
		writeError(w, err)
		return
	}
	value, err := s.store.PollSeatCalls(r.Context(), identityFromRequest(r).NodeID, input)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, value)
}
func (s *Server) advanceSeatCall(w http.ResponseWriter, r *http.Request) {
	var input SeatCallPoll
	if err := decodeJSON(r, &input); err != nil {
		writeError(w, err)
		return
	}
	value, err := s.store.AdvanceSeatCall(r.Context(), identityFromRequest(r).NodeID, r.PathValue("seat_id"), input)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, value)
}
