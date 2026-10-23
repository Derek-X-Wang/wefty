package l1

import "net/http"

func (s *Server) computerSeatList(w http.ResponseWriter, r *http.Request) {
	var request ComputerSeatAttemptRequest
	if err := decodeJSON(r, &request); err != nil {
		writeError(w, err)
		return
	}
	value, err := s.store.GetComputerSeatListForAttempt(r.Context(), identityFromRequest(r).NodeID, r.PathValue("computer_id"), r.PathValue("job_id"), r.PathValue("attempt_id"), request)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, value)
}
func (s *Server) computerSeatObservations(w http.ResponseWriter, r *http.Request) {
	var request ComputerSeatObservationReport
	if err := decodeJSON(r, &request); err != nil {
		writeError(w, err)
		return
	}
	value, err := s.store.ReportComputerSeatObservations(r.Context(), identityFromRequest(r).NodeID, r.PathValue("computer_id"), r.PathValue("job_id"), r.PathValue("attempt_id"), request)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, value)
}
