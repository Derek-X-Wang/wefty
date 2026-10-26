package l1

import (
	"context"
	"database/sql"
	"errors"
	"net/http"
	"net/url"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
)

// SeatViewerTarget contains no Seat credential or live session capability.
type SeatViewerTarget struct {
	URL string `json:"url"`
}

// GetSeatViewerTarget uses the owner's person authority, before existence
// discovery, in one query-only snapshot. Watching does not renew Seat life.
func (s *Store) GetSeatViewerTarget(ctx context.Context, identity fabric.Identity, id string) (target SeatViewerTarget, err error) {
	err = s.withReadSnapshot(ctx, nil, func(ctx context.Context, reads readModel) error {
		rd := reads.(*databaseReads)
		if err := rd.currentAdmin(ctx, identity); err != nil {
			return err
		}
		value, err := readSeatRequest(ctx, rd.q, id)
		if err != nil {
			return err
		}
		switch value.State {
		case SeatEnding, SeatEnded, SeatRefused:
			return protocolError(contract.ErrorCode("seat_ended"), "This Seat has ended")
		case SeatLive:
		default:
			return protocolError(contract.ErrorConflict, "Seat is not ready")
		}
		_, attempt, err := seatCopyBinding(ctx, rd.q, rd.now(), id, value)
		if err != nil {
			return err
		}
		var endpoint sql.NullString
		err = rd.q.QueryRowContext(ctx, `SELECT j.display_endpoint FROM computers c JOIN service_jobs j ON j.job_id=c.current_job_id WHERE c.computer_id=? AND j.published_attempt_id=?`, value.ComputerID, attempt).Scan(&endpoint)
		if errors.Is(err, sql.ErrNoRows) || err == nil && (!endpoint.Valid || endpoint.String == "") {
			return protocolError(contract.ErrorUnavailable, "Seat screen is not published")
		}
		if err != nil {
			return err
		}
		parsed, err := url.Parse(endpoint.String)
		if err != nil || parsed.Host == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" || (parsed.Scheme != "ws" && parsed.Scheme != "wss") {
			return protocolError(contract.ErrorUnavailable, "Seat screen address is unavailable")
		}
		if parsed.Scheme == "wss" {
			parsed.Scheme = "https"
		} else {
			parsed.Scheme = "http"
		}
		parsed.Path = "/v1/seats/" + id + "/person/viewer"
		parsed.RawPath = ""
		target.URL = parsed.String()
		return nil
	})
	return
}

func (s *Server) getSeatViewerTarget(w http.ResponseWriter, r *http.Request) {
	target, err := s.store.GetSeatViewerTarget(r.Context(), identityFromRequest(r), r.PathValue("seat_id"))
	if err != nil {
		writeError(w, err)
		return
	}
	w.Header().Set("Cache-Control", "no-store")
	writeJSON(w, http.StatusOK, target)
}
