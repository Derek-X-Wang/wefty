package l1

import "net/http"

func (s *Server) resolveSeatRun(w http.ResponseWriter, r *http.Request) {
	binding, err := s.store.ResolveSeatRun(r.Context(), r.PathValue("seat_id"), r.Header.Get(SeatCredentialHeader))
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, binding)
}
func (s *Server) controlSeatRun(w http.ResponseWriter, r *http.Request) {
	var input SeatRunControlRequest
	if err := decodeJSON(r, &input); err != nil {
		writeError(w, err)
		return
	}
	command, err := s.store.ControlSeatRun(r.Context(), identityFromRequest(r).NodeID, r.PathValue("job_id"), r.PathValue("attempt_id"), input)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, command)
}
