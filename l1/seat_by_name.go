package l1

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
)

// By-name refusal reasons (spec 10.4, P12). Each starts with a fixed prefix
// before the Computer's name, so the CLI can type it without parsing the
// name; docs/contracts/cli-errors-and-request-ids.md maps them to codes.
const (
	SeatByNameUnavailablePrefix = "Computer unavailable: "
	SeatByNameNetworkPrefix     = "Computer network differs: "
	SeatByNameUnsupportedPrefix = "no Seat support: "
	SeatByNameFullPrefix        = "Computer full: "
)

// seatByNameTarget is one by-name check. reason is a refusal; otherwise
// place is the lowest free Seat place and stopped asks for a start.
type seatByNameTarget struct {
	computer Computer
	place    int
	stopped  bool
	reason   string
}

// computerSeatPlaces is how many Seats a Computer can hold now: its Seat
// limit, capped by its image's Seat places for an owner-made Computer.
func (s *Store) computerSeatPlaces(computer Computer) int {
	if computer.SeatRequestMade {
		return computer.SeatLimit
	}
	return min(computer.SeatLimit, s.imageSeatPlaces(computer.ImageSeatPlaces, computer.CurrentJob.Spec))
}

// imageSeatPlaces applies one fallback to a recorded label reading: an
// installed config pins a default image digest built before the Seat label
// existed, and that image is the 4-place default image. Remove the fallback
// once every install's default image carries the label (#1155;
// docs/contracts/computer-image.md).
func (s *Store) imageSeatPlaces(recorded int, spec contract.JobSpec) int {
	if recorded == 0 && spec.Execution.OCI != nil && s.computerDefaultImage != "" &&
		computerDefaultImageMatches(s.computerDefaultImage, spec.Execution.OCI.Image) {
		return MaxComputerSeatPlaces
	}
	return recorded
}

// checkSeatByName runs every by-name refusal that needs no owner answer, in
// the caller's snapshot or write fence: before the dialog and again after.
func (s *Store) checkSeatByName(ctx context.Context, q queryer, nodeID, name, network string, at time.Time) (seatByNameTarget, error) {
	var id string
	err := q.QueryRowContext(ctx, `SELECT computer_id FROM computers WHERE name=?`, name).Scan(&id)
	if errors.Is(err, sql.ErrNoRows) {
		return seatByNameTarget{reason: fmt.Sprintf("%sno Computer named %q", SeatByNameUnavailablePrefix, name)}, nil
	}
	if err != nil {
		return seatByNameTarget{}, err
	}
	computer, err := readComputerAuthority(ctx, q, id, at)
	if err != nil {
		return seatByNameTarget{}, err
	}
	target := seatByNameTarget{computer: computer, stopped: computer.DesiredState == contract.ServiceDesiredStopped}
	refuse := func(prefix, format string, args ...any) (seatByNameTarget, error) {
		target.reason = prefix + fmt.Sprintf(format, args...)
		return target, nil
	}
	switch {
	case computer.DesiredState == contract.ServiceDesiredRemoved || computer.RemovalOutcome != "":
		return refuse(SeatByNameUnavailablePrefix, "Computer %q was removed", name)
	case computer.PlacementNodeID != nodeID:
		// By name across Macs is #1156; this stage serves only this Mac.
		return refuse(SeatByNameUnavailablePrefix, "Computer %q lives on node %s, not on this Mac", name, computer.PlacementNodeID)
	case computer.CurrentJob.State == contract.JobFailed:
		return refuse(SeatByNameUnavailablePrefix, "Computer %q failed; the owner decides", name)
	case string(computer.Network) != network:
		return refuse(SeatByNameNetworkPrefix, "Computer %q has network %s; ask with --network %s", name, computer.Network, computer.Network)
	case computer.SeatLimit == 0:
		return refuse(SeatByNameUnsupportedPrefix, "Computer %q takes no Seats (Seat limit 0)", name)
	}
	places := s.computerSeatPlaces(computer)
	if places == 0 {
		return refuse(SeatByNameUnsupportedPrefix, "the image of Computer %q does not support Seats", name)
	}
	rows, err := q.QueryContext(ctx, `SELECT i.place FROM computer_seat_identities i JOIN computer_seat_room_reservations r ON r.seat_id=i.seat_id WHERE i.computer_id=? AND r.cleanup_complete=0`, id)
	if err != nil {
		return seatByNameTarget{}, err
	}
	defer rows.Close()
	used := map[int]bool{}
	for rows.Next() {
		var place int
		if err := rows.Scan(&place); err != nil {
			return seatByNameTarget{}, err
		}
		used[place] = true
	}
	if err := rows.Err(); err != nil {
		return seatByNameTarget{}, err
	}
	for place := 1; place <= places; place++ {
		if !used[place] {
			target.place = place
			return target, nil
		}
	}
	return refuse(SeatByNameFullPrefix, "Computer %q holds its %d Seats", name, places)
}

// advanceByNameSeatCall runs after the dialog. The fresh by-name check, the
// approval's expiry, the start of a stopped Computer and the room reservation
// commit together: a crash or a refusal never leaves a started Computer
// behind, and a retry cannot start a Computer the owner stopped since.
func (s *Store) advanceByNameSeatCall(ctx context.Context, identity, id string, poll SeatCallPoll, approval SeatApproval) (SeatRequest, error) {
	value, refusal, err := func() (SeatRequest, string, error) {
		write, err := s.beginWriteTransaction(ctx, nil)
		if err != nil {
			return SeatRequest{}, "", err
		}
		defer write.rollback()
		ctx := write.ctx
		if err := authorizeSeatRequest(ctx, write.tx, identity, id, poll.NodeID, poll.BootSessionID); err != nil {
			return SeatRequest{}, "", err
		}
		if err := expireSeatApproval(ctx, write.tx, id, write.at, s.clock); err != nil {
			return SeatRequest{}, "", err
		}
		value, err := readSeatRequest(ctx, write.tx, id)
		if err != nil {
			return value, "", err
		}
		if value.State != SeatApproved || value.ComputerID != "" {
			return value, "", write.commit() // keeps an expiry refusal
		}
		// The owner may have changed the Computer during the dialog.
		target, err := s.checkSeatByName(ctx, write.tx, poll.NodeID, approval.Request.Computer, approval.Request.Network, write.at)
		if err != nil {
			return SeatRequest{}, "", err
		}
		if target.reason == "" && target.stopped && !approval.Request.StartComputer {
			// Only an approval that said "and start it" may start it.
			target.reason = fmt.Sprintf("%sComputer %q was stopped after the dialog; ask again", SeatByNameUnavailablePrefix, approval.Request.Computer)
		}
		if target.reason != "" {
			return SeatRequest{}, target.reason, nil
		}
		if target.stopped {
			// The owner's approval is the owner's intent to start it (ADR-0004).
			c := target.computer
			if _, err := s.setComputerDesiredStateTx(ctx, write.tx, write.at, c.ComputerID, ComputerDesiredStateRequest{
				ComputerMutationPrecondition: ComputerMutationPrecondition{IntentRevision: c.IntentRevision, StorageID: c.StorageID, StorageGeneration: c.StorageGeneration, Actor: "seat-request"},
				DesiredState:                 contract.ServiceDesiredRunning}); err != nil {
				return SeatRequest{}, "", err
			}
		}
		value, err = s.reserveSeatRoomTx(ctx, write.tx, write.at, identity, id, SeatRoomReservationRequest{NodeID: poll.NodeID, BootSessionID: poll.BootSessionID, ComputerID: target.computer.ComputerID, Place: target.place, Room: poll.Room})
		if err != nil {
			return value, "", err
		}
		if value.State == SeatRefused {
			// Roll the start back; the refusal is recorded on its own below.
			return SeatRequest{}, value.Reason, nil
		}
		return value, "", write.commit()
	}()
	if err != nil {
		return value, err
	}
	if refusal != "" {
		return s.refuseSeatCall(ctx, identity, id, poll, refusal)
	}
	if value.State != SeatApproved {
		return value, nil
	}
	return s.StartSeatRequest(ctx, identity, id, SeatStartRequest{NodeID: poll.NodeID, BootSessionID: poll.BootSessionID, ReadyWait: s.seatCallReadyWait()})
}
