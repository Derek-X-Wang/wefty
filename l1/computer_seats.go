package l1

import (
	"context"
	"database/sql"
	"errors"
	"slices"

	"github.com/Derek-X-Wang/wefty/contract"
)

// WantedComputerSeat is an immutable identity, distinct from an image place.
// Removing it from the list never makes its ID available again.
type WantedComputerSeat struct {
	SeatID string `json:"seat_id"`
	Place  int    `json:"place"`
}
type ComputerSeatList struct {
	Recovery *ComputerSeatRecovery `json:"recovery,omitempty"`
	Revision int64                 `json:"revision"`
	Seats    []WantedComputerSeat  `json:"seats"`
}

const computerSeatSchema = `
CREATE TABLE IF NOT EXISTS computer_seat_lists (
 computer_id TEXT PRIMARY KEY REFERENCES computers(computer_id) ON DELETE CASCADE,
 revision INTEGER NOT NULL CHECK(revision>0)
);
CREATE TABLE IF NOT EXISTS computer_seat_identities (
 seat_id TEXT PRIMARY KEY,
 computer_id TEXT NOT NULL,
 place INTEGER NOT NULL CHECK(place BETWEEN 1 AND 4),
 introduced_revision INTEGER NOT NULL CHECK(introduced_revision>0),
 first_attempt_id TEXT
);
CREATE TABLE IF NOT EXISTS computer_wanted_seats (
 computer_id TEXT NOT NULL REFERENCES computers(computer_id) ON DELETE CASCADE,
 place INTEGER NOT NULL CHECK(place BETWEEN 1 AND 4),
 seat_id TEXT NOT NULL UNIQUE REFERENCES computer_seat_identities(seat_id),
 PRIMARY KEY(computer_id,place)
);
CREATE TABLE IF NOT EXISTS computer_seat_observations (
 seat_id TEXT NOT NULL REFERENCES computer_seat_identities(seat_id),
 attempt_id TEXT NOT NULL REFERENCES attempts(attempt_id) ON DELETE CASCADE,
 list_revision INTEGER NOT NULL,
 helper_generation INTEGER NOT NULL CHECK(helper_generation>0),
 report_sequence INTEGER NOT NULL CHECK(report_sequence>0),
 state TEXT NOT NULL CHECK(state IN ('starting','ready','failed','removing','cleaned')),
 reason TEXT NOT NULL,
 cleanup_complete INTEGER NOT NULL CHECK(cleanup_complete IN (0,1)),
 observed_ns INTEGER NOT NULL,
 PRIMARY KEY(seat_id,attempt_id)
);
CREATE INDEX IF NOT EXISTS computer_seat_observations_attempt ON computer_seat_observations(attempt_id);
`

// SetComputerWantedSeats is the store-only list seam for #1142/#1143. It does
// not approve requests, advance durable Seat states, or reserve/return Room.
// Lifecycle-managed Seats can only be added/removed through their transitions.
func (s *Store) SetComputerWantedSeats(ctx context.Context, computerID string, expectedRevision int64, wanted []WantedComputerSeat) (ComputerSeatList, error) {
	wanted = slices.Clone(wanted)
	slices.SortFunc(wanted, func(a, b WantedComputerSeat) int { return a.Place - b.Place })
	ids, places := map[string]bool{}, map[int]bool{}
	for _, seat := range wanted {
		if !seatApprovalID.MatchString(seat.SeatID) || seat.Place < 1 || seat.Place > 4 || ids[seat.SeatID] || places[seat.Place] {
			return ComputerSeatList{}, protocolError(contract.ErrorInvalidRequest, "invalid wanted Seat list")
		}
		ids[seat.SeatID], places[seat.Place] = true, true
	}
	write, err := s.beginWriteTransaction(ctx, nil)
	if err != nil {
		return ComputerSeatList{}, err
	}
	defer write.rollback()
	tx := write.tx
	var limit int
	var desired string
	err = tx.QueryRowContext(ctx, `SELECT seat_limit,desired_state FROM computers WHERE computer_id=?`, computerID).Scan(&limit, &desired)
	if errors.Is(err, sql.ErrNoRows) {
		return ComputerSeatList{}, protocolError(contract.ErrorNotFound, "Computer not found")
	}
	if err != nil {
		return ComputerSeatList{}, internalError(err, "read Computer Seat settings")
	}
	if desired != string(contract.ServiceDesiredRunning) || limit < 1 || len(wanted) > limit {
		return ComputerSeatList{}, protocolError(contract.ErrorConflict, "Computer cannot accept this wanted Seat list")
	}
	for _, seat := range wanted {
		value, err := readSeatRequest(ctx, tx, seat.SeatID)
		if err == nil {
			if (value.State != SeatStarting && value.State != SeatLive) || value.ComputerID != computerID || value.Place != seat.Place {
				return ComputerSeatList{}, protocolError(contract.ErrorConflict, "Seat lifecycle does not permit this wanted list")
			}
		} else if errorCodeForSeatLifecycle(err) != contract.ErrorNotFound {
			return ComputerSeatList{}, err
		}
	}
	current, err := readComputerSeatList(ctx, tx, computerID)
	if err != nil {
		return ComputerSeatList{}, err
	}
	if expectedRevision != current.Revision {
		return ComputerSeatList{}, protocolError(contract.ErrorConflict, "wanted Seat list revision changed")
	}
	for _, seat := range current.Seats {
		if slices.Contains(wanted, seat) {
			continue
		}
		_, err := readSeatRequest(ctx, tx, seat.SeatID)
		if err == nil {
			return ComputerSeatList{}, protocolError(contract.ErrorConflict, "end the Seat request before removing its wanted membership")
		}
		if errorCodeForSeatLifecycle(err) != contract.ErrorNotFound {
			return ComputerSeatList{}, err
		}
	}
	if slices.Equal(current.Seats, wanted) {
		return current, write.commit()
	}
	revision := current.Revision + 1
	if revision <= 0 {
		return ComputerSeatList{}, protocolError(contract.ErrorConflict, "wanted Seat revision exhausted")
	}
	existing := map[string]WantedComputerSeat{}
	for _, seat := range current.Seats {
		existing[seat.SeatID] = seat
	}
	for _, seat := range wanted {
		if old, ok := existing[seat.SeatID]; ok {
			if old != seat {
				return ComputerSeatList{}, protocolError(contract.ErrorConflict, "Seat identity is immutable")
			}
			continue
		}
		var known string
		err := tx.QueryRowContext(ctx, `SELECT seat_id FROM computer_seat_identities WHERE seat_id=?`, seat.SeatID).Scan(&known)
		if err == nil {
			return ComputerSeatList{}, protocolError(contract.ErrorConflict, "Seat ID was already used")
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return ComputerSeatList{}, internalError(err, "read Seat identity")
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO computer_seat_identities(seat_id,computer_id,place,introduced_revision) VALUES(?,?,?,?)`, seat.SeatID, computerID, seat.Place, revision); err != nil {
			return ComputerSeatList{}, internalError(err, "retain Seat identity")
		}
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO computer_seat_lists(computer_id,revision) VALUES(?,?) ON CONFLICT(computer_id) DO UPDATE SET revision=excluded.revision`, computerID, revision); err != nil {
		return ComputerSeatList{}, err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM computer_wanted_seats WHERE computer_id=?`, computerID); err != nil {
		return ComputerSeatList{}, err
	}
	for _, seat := range wanted {
		if _, err := tx.ExecContext(ctx, `INSERT INTO computer_wanted_seats(computer_id,place,seat_id) VALUES(?,?,?)`, computerID, seat.Place, seat.SeatID); err != nil {
			return ComputerSeatList{}, err
		}
	}
	if err := write.commit(); err != nil {
		return ComputerSeatList{}, err
	}
	if wanted == nil {
		wanted = []WantedComputerSeat{}
	}
	return ComputerSeatList{Revision: revision, Seats: wanted}, nil
}
func readComputerSeatList(ctx context.Context, q queryer, computerID string) (ComputerSeatList, error) {
	list := ComputerSeatList{Seats: []WantedComputerSeat{}}
	err := q.QueryRowContext(ctx, `SELECT revision FROM computer_seat_lists WHERE computer_id=?`, computerID).Scan(&list.Revision)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return list, internalError(err, "read wanted Seat revision")
	}
	rows, err := q.QueryContext(ctx, `SELECT seat_id,place FROM computer_wanted_seats WHERE computer_id=? ORDER BY place`, computerID)
	if err != nil {
		return list, err
	}
	defer rows.Close()
	for rows.Next() {
		var seat WantedComputerSeat
		if err := rows.Scan(&seat.SeatID, &seat.Place); err != nil {
			return list, err
		}
		list.Seats = append(list.Seats, seat)
	}
	return list, rows.Err()
}
