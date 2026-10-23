package l1

import "net/http"

type CloseSeatApprovalWindowsRequest struct {
	NodeID  string `json:"node_id"`
	Network string `json:"network"`
}

type CloseSeatApprovalWindowsResponse struct {
	Closed bool `json:"closed"`
}

func (s *Server) beginSeatApproval(w http.ResponseWriter, r *http.Request) {
	var request SeatApprovalRequest
	if err := decodeJSON(r, &request); err != nil {
		writeError(w, err)
		return
	}
	value, err := s.store.BeginSeatApproval(r.Context(), identityFromRequest(r).NodeID, request)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, value)
}
func (s *Server) answerSeatApproval(w http.ResponseWriter, r *http.Request) {
	var answer SeatApprovalAnswer
	if err := decodeJSON(r, &answer); err != nil {
		writeError(w, err)
		return
	}
	value, err := s.store.AnswerSeatApproval(r.Context(), identityFromRequest(r).NodeID, r.PathValue("seat_id"), answer)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, value)
}
func (s *Server) listSeatApprovalWindows(w http.ResponseWriter, r *http.Request) {
	value, err := s.store.ListSeatApprovalWindows(r.Context(), r.URL.Query().Get("node_id"))
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, value)
}
func (s *Server) closeSeatApprovalWindows(w http.ResponseWriter, r *http.Request) {
	var request CloseSeatApprovalWindowsRequest
	if err := decodeJSON(r, &request); err != nil {
		writeError(w, err)
		return
	}
	if err := s.store.CloseSeatApprovalWindows(r.Context(), request.NodeID, request.Network); err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, CloseSeatApprovalWindowsResponse{Closed: true})
}
