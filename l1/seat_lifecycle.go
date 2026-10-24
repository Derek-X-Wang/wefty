package l1

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"math"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
)

// SeatRequestState is the durable P18 lifecycle, not an image observation.
type SeatRequestState string

const (
	SeatRequested SeatRequestState = "requested"
	SeatApproved  SeatRequestState = "approved"
	SeatStarting  SeatRequestState = "starting"
	SeatLive      SeatRequestState = "live"
	SeatEnding    SeatRequestState = "ending"
	SeatEnded     SeatRequestState = "ended"
	SeatRefused   SeatRequestState = "refused"
)

type SeatRequest struct {
	SeatID            string           `json:"seat_id"`
	State             SeatRequestState `json:"state"`
	Reason            string           `json:"reason"`
	ComputerID        string           `json:"computer_id"`
	Place             int              `json:"place"`
	Deadline          time.Time        `json:"deadline"`
	CleanupComplete   bool             `json:"cleanup_complete"`
	TargetMemoryBytes int64            `json:"target_memory_bytes"`
	RoomRevision      int64            `json:"room_revision"`
}
type SeatNodeRequest struct {
	NodeID        string `json:"node_id"`
	BootSessionID string `json:"boot_session_id"`
}

// SeatStartRequest carries a remaining wait measured on L1's clock. It does not replace
// the helper's independent 60-second deadline from directive publication.
type SeatStartRequest struct {
	NodeID        string        `json:"node_id"`
	BootSessionID string        `json:"boot_session_id"`
	ReadyWait     time.Duration `json:"ready_wait"`
}
type SeatRoomReservationRequest struct {
	NodeID        string             `json:"node_id"`
	BootSessionID string             `json:"boot_session_id"`
	ComputerID    string             `json:"computer_id"`
	Place         int                `json:"place"`
	Room          ComputerRoomConfig `json:"room"`
}
type SeatEndRequest struct {
	NodeID        string `json:"node_id"`
	BootSessionID string `json:"boot_session_id"`
	Reason        string `json:"reason"`
	Refused       bool   `json:"refused"`
	// The request controller cannot end a Seat that already reached Live.
	pendingOnly   bool
	preflightOnly bool
}

const seatRoomSchema = `
CREATE TABLE IF NOT EXISTS computer_seat_room_reservations (
 seat_id TEXT PRIMARY KEY REFERENCES seat_approvals(seat_id),
 computer_id TEXT NOT NULL,
 node_id TEXT NOT NULL REFERENCES nodes(node_id),
 memory_bytes INTEGER NOT NULL CHECK(memory_bytes>0),
 cleanup_complete INTEGER NOT NULL DEFAULT 0 CHECK(cleanup_complete IN (0,1))
);
CREATE INDEX IF NOT EXISTS computer_seat_room_node ON computer_seat_room_reservations(node_id,computer_id);
CREATE TABLE IF NOT EXISTS computer_room_limits (
 computer_id TEXT PRIMARY KEY,
 node_id TEXT NOT NULL REFERENCES nodes(node_id),
 memory_bytes INTEGER NOT NULL CHECK(memory_bytes>=0),
 revision INTEGER NOT NULL DEFAULT 1 CHECK(revision>0)
);
`

func (s *Store) migrateSeatLifecycle(ctx context.Context) error {
	for _, col := range []struct{ name, definition string }{
		{"lifecycle_state", "TEXT NOT NULL DEFAULT '' CHECK(lifecycle_state IN ('','requested','approved','starting','live','ending','ended','refused'))"},
		{"lifecycle_reason", "TEXT NOT NULL DEFAULT ''"},
		{"seat_deadline_ns", "INTEGER NOT NULL DEFAULT 0"},
		{"seat_ready_wait_ns", "INTEGER NOT NULL DEFAULT 0"},
	} {
		if err := s.ensureColumn(ctx, "seat_approvals", col.name, col.definition); err != nil {
			return err
		}
	}
	for _, column := range []struct{ name, definition string }{{"first_attempt_authority_json", "BLOB"}, {"first_attempt_absent", "INTEGER NOT NULL DEFAULT 0 CHECK(first_attempt_absent IN (0,1))"}} {
		if err := s.ensureColumn(ctx, "computer_seat_identities", column.name, column.definition); err != nil {
			return err
		}
	}
	for _, column := range []struct{ name, definition string }{{"memory_attempt_authority_json", "BLOB"}, {"memory_attempt_absent", "INTEGER NOT NULL DEFAULT 0 CHECK(memory_attempt_absent IN (0,1))"}} {
		if err := s.ensureColumn(ctx, "computer_room_limits", column.name, column.definition); err != nil {
			return err
		}
	}
	write, err := s.beginWriteTransaction(ctx, nil)
	if err != nil {
		return err
	}
	defer write.rollback()
	if err := retainSeatAttemptAuthorities(ctx, write.tx, ""); err != nil {
		return err
	}
	return write.commit()
}

func readSeatRequest(ctx context.Context, q queryer, id string) (SeatRequest, error) {
	value := SeatRequest{SeatID: id}
	var ns int64
	err := q.QueryRowContext(ctx, `SELECT COALESCE(NULLIF(a.lifecycle_state,''),a.state),
 CASE WHEN a.lifecycle_state='' THEN a.reason ELSE a.lifecycle_reason END,
 COALESCE(i.computer_id,''),COALESCE(i.place,0),a.seat_deadline_ns,COALESCE(r.cleanup_complete,0),
 COALESCE((SELECT SUM(memory_bytes) FROM computer_seat_room_reservations WHERE computer_id=r.computer_id AND cleanup_complete=0),0),COALESCE(l.revision,0)
 FROM seat_approvals a LEFT JOIN computer_seat_identities i ON i.seat_id=a.seat_id
 LEFT JOIN computer_seat_room_reservations r ON r.seat_id=a.seat_id LEFT JOIN computer_room_limits l ON l.computer_id=r.computer_id WHERE a.seat_id=?`, id).
		Scan(&value.State, &value.Reason, &value.ComputerID, &value.Place, &ns, &value.CleanupComplete, &value.TargetMemoryBytes, &value.RoomRevision)
	if errors.Is(err, sql.ErrNoRows) {
		return value, protocolError(contract.ErrorNotFound, "Seat request not found")
	}
	if ns != 0 {
		value.Deadline = time.Unix(0, ns).UTC()
	}
	return value, err
}
func (s *Store) GetSeatRequest(ctx context.Context, id string) (SeatRequest, error) {
	var value SeatRequest
	var expired bool
	err := s.withReadSnapshot(ctx, nil, func(ctx context.Context, reads readModel) error {
		var err error
		value, err = readSeatRequest(ctx, reads.(*databaseReads).q, id)
		if err == nil {
			expired, err = seatApprovalNeedsExpiry(ctx, reads.(*databaseReads).q, value, reads.(*databaseReads).now())
		}
		return err
	})
	if err == nil && expired {
		return s.expireSeatApprovalRead(ctx, id, "", nil)
	}
	return value, err
}
func authorizeSeatRequest(ctx context.Context, q queryer, identity, id, node, boot string) error {
	if err := requireSeatApprovalNode(ctx, q, identity, node, boot); err != nil {
		return err
	}
	var requestNode string
	if err := q.QueryRowContext(ctx, `SELECT node_id FROM seat_approvals WHERE seat_id=?`, id).Scan(&requestNode); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return protocolError(contract.ErrorNotFound, "Seat request not found")
		}
		return err
	}
	if node != requestNode {
		return protocolError(contract.ErrorForbidden, "Seat request belongs to another node")
	}
	return nil
}

// ReserveSeatRoom is the second room check, after approval. SQLite's immediate
// write transaction fences both the last share of room and the dedicated cap.
// It allocates the existing Seat identity under the approval's ID, never a new ID.
// Callers grow to TargetMemoryBytes before StartSeatRequest publishes the Seat.
func (s *Store) ReserveSeatRoom(ctx context.Context, identity, id string, request SeatRoomReservationRequest) (SeatRequest, error) {
	if err := request.Room.Validate(); err != nil {
		return SeatRequest{}, protocolError(contract.ErrorInvalidRequest, "invalid Computer room configuration")
	}
	if request.ComputerID == "" || request.Place < 1 || request.Place > 4 {
		return SeatRequest{}, protocolError(contract.ErrorInvalidRequest, "invalid Seat reservation")
	}
	write, err := s.beginWriteTransaction(ctx, nil)
	if err != nil {
		return SeatRequest{}, err
	}
	defer write.rollback()
	value, err := s.reserveSeatRoomTx(ctx, write.tx, write.at, identity, id, request)
	if err != nil {
		return value, err
	}
	return value, write.commit()
}

// Creation and first room admission share one transaction: a losing request
// never publishes a runnable Computer, a Storage generation, or a new charge.
func (s *Store) reserveSeatRoomTx(ctx context.Context, q *sql.Tx, at time.Time, identity, id string, request SeatRoomReservationRequest) (SeatRequest, error) {
	if err := authorizeSeatRequest(ctx, q, identity, id, request.NodeID, request.BootSessionID); err != nil {
		return SeatRequest{}, err
	}
	if err := expireSeatApproval(ctx, q, id, at); err != nil {
		return SeatRequest{}, err
	}
	value, err := readSeatRequest(ctx, q, id)
	if err != nil {
		return value, err
	}
	var reservedComputer string
	var reservedMemory int64
	err = q.QueryRowContext(ctx, `SELECT computer_id,memory_bytes FROM computer_seat_room_reservations WHERE seat_id=?`, id).Scan(&reservedComputer, &reservedMemory)
	if err == nil {
		if reservedComputer != request.ComputerID || value.Place != request.Place || reservedMemory != request.Room.SeatMemoryBytes {
			return value, protocolError(contract.ErrorConflict, "Seat reservation ID has different input")
		}
		return value, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return value, err
	}
	if value.State == SeatRefused {
		return value, nil
	}
	if value.State != SeatApproved {
		return value, protocolError(contract.ErrorConflict, "Seat request is not approved")
	}
	var node, network, desired string
	var limit int
	var made bool
	err = q.QueryRowContext(ctx, `SELECT placement_node_id,network,desired_state,seat_limit,seat_request_made FROM computers WHERE computer_id=?`, request.ComputerID).Scan(&node, &network, &desired, &limit, &made)
	if errors.Is(err, sql.ErrNoRows) {
		return value, protocolError(contract.ErrorNotFound, "Computer not found")
	}
	if err != nil {
		return value, err
	}
	approval, err := readSeatApproval(ctx, q, id)
	if err != nil {
		return value, err
	}
	if approval.Request.BootSessionID != request.BootSessionID || node != request.NodeID || network != approval.Request.Network || !made || desired != string(contract.ServiceDesiredRunning) || request.Place > limit || ((approval.Request.Form == "dedicated") != (limit == 1)) {
		return value, protocolError(contract.ErrorConflict, "Computer does not match this approved Seat request")
	}
	var occupied int
	if err := q.QueryRowContext(ctx, `SELECT COUNT(*) FROM computer_seat_identities i JOIN computer_seat_room_reservations r ON r.seat_id=i.seat_id WHERE i.computer_id=? AND i.place=? AND r.cleanup_complete=0`, request.ComputerID, request.Place).Scan(&occupied); err != nil {
		return value, err
	}
	if occupied != 0 {
		return value, protocolError(contract.ErrorConflict, "Seat place still needs cleanup")
	}
	usage, err := readSeatRoomUsage(ctx, q, request.NodeID)
	if err != nil {
		return value, err
	}
	var currentCharge int64
	others := make([]ComputerRoomUsage, 0, len(usage))
	for _, computer := range usage {
		if computer.ComputerID == request.ComputerID {
			currentCharge = computer.MemoryBytes
		} else {
			others = append(others, computer)
		}
	}
	// A pending Computer will start at its declaration, even with one Seat.
	// Reserve that floor before admission can publish a starting Seat.
	floor, err := computerCreationMemoryFloor(ctx, q, request.ComputerID)
	if err != nil {
		return value, err
	}
	currentCharge = max(currentCharge, floor)
	var reserved int64
	if err := q.QueryRowContext(ctx, `SELECT COALESCE(SUM(memory_bytes),0) FROM computer_seat_room_reservations WHERE computer_id=? AND cleanup_complete=0`, request.ComputerID).Scan(&reserved); err != nil {
		return value, err
	}
	if reserved > math.MaxInt64-request.Room.SeatMemoryBytes {
		return value, protocolError(contract.ErrorConflict, "Computer room charge overflows")
	}
	required := max(currentCharge, reserved+request.Room.SeatMemoryBytes)
	var roomErr error
	if required > request.Room.MemoryBytes {
		roomErr = ErrComputerRoomFull
	} else {
		// Check other Computers against the cap; charge this target exactly
		// once, at its projected total, rather than treating it as a competitor.
		budget := request.Room
		budget.MemoryBytes -= required - request.Room.SeatMemoryBytes
		_, roomErr = budget.Check(others, approval.Request.Form == "dedicated")
	}

	if roomErr != nil {
		if !errors.Is(roomErr, ErrComputerRoomFull) && !errors.Is(roomErr, ErrDedicatedComputerCap) {
			return value, roomErr
		}
		if err := setSeatLifecycle(ctx, q, id, SeatRefused, roomErr.Error()); err != nil {
			return value, err
		}
		value, err = readSeatRequest(ctx, q, id)
		if err != nil {
			return value, err
		}
		return value, nil
	}
	list, err := readComputerSeatList(ctx, q, request.ComputerID)
	if err != nil {
		return value, err
	}
	if list.Revision == math.MaxInt64 {
		return value, protocolError(contract.ErrorConflict, "Seat revision exhausted")
	}
	var existing int
	if err := q.QueryRowContext(ctx, `SELECT COUNT(*) FROM computer_seat_identities WHERE seat_id=?`, id).Scan(&existing); err != nil {
		return value, err
	}
	if existing != 0 {
		return value, protocolError(contract.ErrorConflict, "Seat ID was already used")
	}
	if _, err := q.ExecContext(ctx, `INSERT INTO computer_seat_identities(seat_id,computer_id,place,introduced_revision) VALUES(?,?,?,?)`, id, request.ComputerID, request.Place, list.Revision+1); err != nil {
		return value, err
	}
	if _, err := q.ExecContext(ctx, `INSERT INTO computer_seat_room_reservations(seat_id,computer_id,node_id,memory_bytes) VALUES(?,?,?,?)`, id, request.ComputerID, request.NodeID, request.Room.SeatMemoryBytes); err != nil {
		return value, err
	}
	value, err = readSeatRequest(ctx, q, id)
	if err != nil {
		return value, err
	}
	// Charge before the grow. A lost grow response must never return room.
	if _, err := q.ExecContext(ctx, `INSERT INTO computer_room_limits(computer_id,node_id,memory_bytes) VALUES(?,?,?) ON CONFLICT(computer_id) DO UPDATE SET memory_bytes=MAX(memory_bytes,excluded.memory_bytes),revision=revision+1`, request.ComputerID, request.NodeID, max(currentCharge, value.TargetMemoryBytes)); err != nil {
		return value, err
	}
	value, err = readSeatRequest(ctx, q, id)
	if err != nil {
		return value, err
	}
	return value, nil
}

func readSeatRoomUsage(ctx context.Context, q queryer, node string) ([]ComputerRoomUsage, error) {
	rows, err := q.QueryContext(ctx, `SELECT c.computer_id,c.seat_request_made,c.seat_limit,j.state,j.spec_json,
 COALESCE(l.memory_bytes,0),l.computer_id IS NOT NULL,COALESCE((SELECT SUM(r.memory_bytes) FROM computer_seat_room_reservations r WHERE r.computer_id=c.computer_id AND r.cleanup_complete=0),0)
 FROM computers c JOIN jobs j ON j.job_id=c.current_job_id LEFT JOIN computer_room_limits l ON l.computer_id=c.computer_id
 WHERE c.placement_node_id=?`, node)
	if err != nil {
		return nil, err
	}
	result := []ComputerRoomUsage{}
	for rows.Next() {
		var u ComputerRoomUsage
		var state string
		var specBytes []byte
		var reserved int64
		var tracked bool
		if err := rows.Scan(&u.ComputerID, &u.SeatRequestMade, &u.SeatLimit, &state, &specBytes, &u.MemoryBytes, &tracked, &reserved); err != nil {
			rows.Close()
			return nil, err
		}
		u.Running = state == string(contract.JobClaimed) || state == string(contract.JobRunning) || state == string(contract.JobStopping) || state == string(contract.JobRemovalPending) || state == string(contract.JobAgentCleaned)
		// Reservation bytes are authoritative even if the local share setting changes.
		u.MemoryBytes = max(u.MemoryBytes, reserved)
		if reserved > 0 {
			u.Seats = 1
		} // Only occupancy matters here; bytes carry the charge.
		if !u.SeatRequestMade || u.Running {
			var spec contract.JobSpec
			if err := json.Unmarshal(specBytes, &spec); err != nil {
				rows.Close()
				return nil, err
			}
			if spec.Execution.OCI != nil && spec.Execution.OCI.Limits != nil && spec.Execution.OCI.Limits.MemoryBytes != nil {
				u.MemoryBytes = max(u.MemoryBytes, *spec.Execution.OCI.Limits.MemoryBytes)
			}
		}
		result = append(result, u)
	}
	err = errors.Join(rows.Err(), rows.Close())
	if err != nil {
		return nil, err
	}
	// Retained charges outlive Computer deletion, just as Seat identity tombstones do.
	rows, err = q.QueryContext(ctx, `SELECT l.computer_id,l.memory_bytes FROM computer_room_limits l LEFT JOIN computers c ON c.computer_id=l.computer_id WHERE l.node_id=? AND c.computer_id IS NULL AND l.memory_bytes>0`, node)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var u ComputerRoomUsage
		if err := rows.Scan(&u.ComputerID, &u.MemoryBytes); err != nil {
			return nil, err
		}
		result = append(result, u)
	}
	return result, rows.Err()
}

func setSeatLifecycle(ctx context.Context, q seatApprovalQuery, id string, state SeatRequestState, reason string) error {
	_, err := q.ExecContext(ctx, `UPDATE seat_approvals SET lifecycle_state=?,lifecycle_reason=?,state=CASE WHEN ?='refused' THEN 'refused' ELSE state END,reason=CASE WHEN ?='refused' THEN ? ELSE reason END WHERE seat_id=?`, state, reason, state, state, reason, id)
	return err
}
func bumpSeatList(ctx context.Context, q seatApprovalQuery, computer string) error {
	_, err := q.ExecContext(ctx, `INSERT INTO computer_seat_lists(computer_id,revision) VALUES(?,1) ON CONFLICT(computer_id) DO UPDATE SET revision=revision+1`, computer)
	return err
}
func removeLifecycleSeat(ctx context.Context, q seatApprovalQuery, value SeatRequest) error {
	result, err := q.ExecContext(ctx, `DELETE FROM computer_wanted_seats WHERE seat_id=?`, value.SeatID)
	if err != nil {
		return err
	}
	n, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if n > 0 {
		return bumpSeatList(ctx, q, value.ComputerID)
	}
	return nil
}
func (s *Store) StartSeatRequest(ctx context.Context, identity, id string, request SeatStartRequest) (SeatRequest, error) {
	write, err := s.beginWriteTransaction(ctx, nil)
	if err != nil {
		return SeatRequest{}, err
	}
	defer write.rollback()
	q := write.tx
	if err := authorizeSeatRequest(ctx, q, identity, id, request.NodeID, request.BootSessionID); err != nil {
		return SeatRequest{}, err
	}
	if err := expireSeatApproval(ctx, q, id, write.at); err != nil {
		return SeatRequest{}, err
	}
	value, err := readSeatRequest(ctx, q, id)
	if err != nil {
		return value, err
	}
	if value.State == SeatStarting || value.State == SeatLive {
		var wait int64
		if err := q.QueryRowContext(ctx, `SELECT seat_ready_wait_ns FROM seat_approvals WHERE seat_id=?`, id).Scan(&wait); err != nil {
			return value, err
		}
		if wait != int64(request.ReadyWait) {
			return value, protocolError(contract.ErrorConflict, "Seat start retry changed its deadline")
		}
		return value, write.commit()
	}
	if value.State != SeatApproved || value.ComputerID == "" {
		return value, protocolError(contract.ErrorConflict, "Seat has no approved room reservation")
	}
	deadline := write.at.Add(request.ReadyWait)
	if request.ReadyWait <= 0 || deadline.UnixNano() <= write.at.UnixNano() {
		if err := setSeatLifecycle(ctx, q, id, SeatRefused, "invalid Seat readiness wait"); err != nil {
			return value, err
		}
		if err := cleanNeverStartedSeat(ctx, q, value); err != nil {
			return value, err
		}
		if err := write.commit(); err != nil {
			return value, err
		}
		value.State, value.Reason = SeatRefused, "invalid Seat readiness wait"
		return value, protocolError(contract.ErrorInvalidRequest, "Seat start requires a positive readiness wait")
	}
	approval, err := readSeatApproval(ctx, q, id)
	if err != nil {
		return value, err
	}
	if approval.Request.BootSessionID != request.BootSessionID {
		return value, protocolError(contract.ErrorConflict, "Seat approval belongs to an earlier node session")
	}
	if err := setSeatLifecycle(ctx, q, id, SeatStarting, ""); err != nil {
		return value, err
	}
	if _, err := q.ExecContext(ctx, `UPDATE seat_approvals SET seat_deadline_ns=?,seat_ready_wait_ns=? WHERE seat_id=?`, deadline.UnixNano(), int64(request.ReadyWait), id); err != nil {
		return value, err
	}
	if _, err := q.ExecContext(ctx, `INSERT INTO computer_wanted_seats(computer_id,place,seat_id) VALUES(?,?,?)`, value.ComputerID, value.Place, id); err != nil {
		return value, err
	}
	if err := bumpSeatList(ctx, q, value.ComputerID); err != nil {
		return value, err
	}
	value, err = readSeatRequest(ctx, q, id)
	if err != nil {
		return value, err
	}
	return value, write.commit()
}
func (s *Store) EndSeatRequest(ctx context.Context, identity, id string, request SeatEndRequest) (SeatRequest, error) {
	if request.Reason == "" || len(request.Reason) > 8192 {
		return SeatRequest{}, protocolError(contract.ErrorInvalidRequest, "Seat end requires a bounded reason")
	}
	write, err := s.beginWriteTransaction(ctx, nil)
	if err != nil {
		return SeatRequest{}, err
	}
	defer write.rollback()
	q := write.tx
	if err := authorizeSeatRequest(ctx, q, identity, id, request.NodeID, request.BootSessionID); err != nil {
		return SeatRequest{}, err
	}
	if err := expireSeatApproval(ctx, q, id, write.at); err != nil {
		return SeatRequest{}, err
	}
	value, err := readSeatRequest(ctx, q, id)
	if err != nil {
		return value, err
	}
	if request.preflightOnly && (value.State != SeatRequested || value.ComputerID != "") {
		return value, write.commit()
	}
	if request.pendingOnly && (value.State == SeatLive || value.State == SeatEnding || value.State == SeatEnded) {
		return value, write.commit()
	}
	if value.State != SeatEnded && value.State != SeatRefused && value.State != SeatEnding {
		state := SeatEnding
		if request.Refused || value.State == SeatRequested || value.State == SeatApproved || value.State == SeatStarting {
			state = SeatRefused
		}
		if err := setSeatLifecycle(ctx, q, id, state, request.Reason); err != nil {
			return value, err
		}
		if err := removeLifecycleSeat(ctx, q, value); err != nil {
			return value, err
		}
	}
	if err := cleanNeverStartedSeat(ctx, q, value); err != nil {
		return value, err
	}
	value, err = readSeatRequest(ctx, q, id)
	if err != nil {
		return value, err
	}
	return value, write.commit()
}

// Observation and lifecycle publication share the same fenced transaction.
func applySeatLifecycleObservation(ctx context.Context, q seatApprovalQuery, now time.Time, seat ComputerSeatObservation) error {
	value, err := readSeatRequest(ctx, q, seat.SeatID)
	var typed *Error
	if errors.As(err, &typed) && typed.Code == contract.ErrorNotFound {
		return nil
	} // legacy list-only Seat
	if err != nil {
		return err
	}
	switch seat.State {
	case ComputerSeatReady:
		if value.State == SeatStarting {
			if !now.Before(value.Deadline) {
				if err := setSeatLifecycle(ctx, q, seat.SeatID, SeatRefused, "Seat readiness deadline exceeded"); err != nil {
					return err
				}
				return removeLifecycleSeat(ctx, q, value)
			}
			// This positive receipt commits with Live, including when the CLI
			// never receives its answer. Later Seat failure cannot turn an
			// existing Computer back into a failed new start.
			if _, err := q.ExecContext(ctx, `UPDATE seat_request_calls SET start_finished=1 WHERE seat_id=?`, seat.SeatID); err != nil {
				return err
			}
			if _, err := q.ExecContext(ctx, `UPDATE computer_seat_identities SET ever_live=1 WHERE seat_id=?`, seat.SeatID); err != nil {
				return err
			}
			return setSeatLifecycle(ctx, q, seat.SeatID, SeatLive, "")
		}
	case ComputerSeatFailed:
		if value.State == SeatStarting || value.State == SeatLive {
			if err := setSeatLifecycle(ctx, q, seat.SeatID, SeatRefused, seat.Reason); err != nil {
				return err
			}
			return removeLifecycleSeat(ctx, q, value)
		}
	case ComputerSeatCleaned:
		if !seat.CleanupComplete {
			return nil
		}
		if value.State != SeatEnding && value.State != SeatRefused && value.State != SeatEnded {
			return protocolError(contract.ErrorConflict, "active Seat has no cleanup authority")
		}
		if err := markSeatReservationClean(ctx, q, value); err != nil {
			return err
		}
		if value.State == SeatEnding {
			return setSeatLifecycle(ctx, q, seat.SeatID, SeatEnded, value.Reason)
		}
	}
	return nil
}

func errorCodeForSeatLifecycle(err error) contract.ErrorCode {
	var typed *Error
	if errors.As(err, &typed) {
		return typed.Code
	}
	return ""
}

func (s *Store) GetComputerRoomSnapshot(ctx context.Context, node string, config ComputerRoomConfig) (ComputerRoomSnapshot, error) {
	var snapshot ComputerRoomSnapshot
	err := s.withReadSnapshot(ctx, nil, func(ctx context.Context, reads readModel) error {
		usage, err := readSeatRoomUsage(ctx, reads.(*databaseReads).q, node)
		if err != nil {
			return err
		}
		snapshot, err = config.Check(usage, false)
		if errors.Is(err, ErrComputerRoomFull) {
			return nil
		}
		return err
	})
	return snapshot, err
}

type SeatRoomMemoryReport struct {
	RoomRevision int64 `json:"room_revision"`
	ComputerSeatAttemptRequest
	RequestedMemoryBytes int64 `json:"requested_memory_bytes"`
	MemoryLimitBytes     int64 `json:"memory_limit_bytes"`
	HeldMemoryBytes      int64 `json:"held_memory_bytes"`
}
type SeatRoomMemoryReceipt struct {
	RoomRevision      int64 `json:"room_revision"`
	HeldMemoryBytes   int64 `json:"held_memory_bytes"`
	TargetMemoryBytes int64 `json:"target_memory_bytes"`
}

// ReportComputerRoomMemory records the helper's effective charge, not merely
// the requested target. The node agent must pass the actual helper readback.
// A failed shrink can leave HeldMemoryBytes above the final target indefinitely.
func (s *Store) ReportComputerRoomMemory(ctx context.Context, identity, computer, job, attempt string, report SeatRoomMemoryReport) (SeatRoomMemoryReceipt, error) {
	if report.RequestedMemoryBytes <= 0 || report.MemoryLimitBytes <= 0 || report.HeldMemoryBytes < report.MemoryLimitBytes {
		return SeatRoomMemoryReceipt{}, protocolError(contract.ErrorInvalidRequest, "invalid Computer memory evidence")
	}
	write, err := s.beginWriteTransaction(ctx, nil)
	if err != nil {
		return SeatRoomMemoryReceipt{}, err
	}
	defer write.rollback()
	q := write.tx
	if err := validateComputerSeatAttempt(ctx, q, write.at, identity, computer, job, attempt, report.ComputerSeatAttemptRequest); err != nil {
		return SeatRoomMemoryReceipt{}, err
	}

	if err := bindSeatRoomAttempt(ctx, q, computer, job, attempt, report.ComputerSeatAttemptRequest); err != nil {
		return SeatRoomMemoryReceipt{}, err
	}
	pending, err := oldSeatRoomAttemptPending(ctx, q, computer, attempt)
	if err != nil {
		return SeatRoomMemoryReceipt{}, err
	}
	if pending {
		return SeatRoomMemoryReceipt{}, protocolError(contract.ErrorConflict, "earlier Computer memory Attempt still needs absence evidence")
	}

	var revision int64
	if err := q.QueryRowContext(ctx, `SELECT revision FROM computer_room_limits WHERE computer_id=?`, computer).Scan(&revision); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return SeatRoomMemoryReceipt{}, protocolError(contract.ErrorConflict, "Computer has no room reservation")
		}
		return SeatRoomMemoryReceipt{}, err
	}
	if revision != report.RoomRevision {
		return SeatRoomMemoryReceipt{}, protocolError(contract.ErrorConflict, "Computer room reservation revision changed")
	}
	var target int64
	if err := q.QueryRowContext(ctx, `SELECT COALESCE(SUM(memory_bytes),0) FROM computer_seat_room_reservations WHERE computer_id=? AND cleanup_complete=0`, computer).Scan(&target); err != nil {
		return SeatRoomMemoryReceipt{}, err
	}
	if report.RequestedMemoryBytes < target || report.HeldMemoryBytes < target || report.MemoryLimitBytes < target {
		return SeatRoomMemoryReceipt{}, protocolError(contract.ErrorConflict, "memory evidence does not cover the current Seat reservations")
	}
	// An old attempt's pending cleanup cannot be discharged by a new cgroup.
	var old int
	if err := q.QueryRowContext(ctx, `SELECT COUNT(*) FROM computer_seat_room_reservations r JOIN computer_seat_identities i ON i.seat_id=r.seat_id WHERE r.computer_id=? AND i.first_attempt_id IS NOT NULL AND i.first_attempt_id!=? AND (i.first_attempt_absent=0 OR r.cleanup_complete=0)`, computer, attempt).Scan(&old); err != nil {
		return SeatRoomMemoryReceipt{}, err
	}
	if old > 0 {
		return SeatRoomMemoryReceipt{}, protocolError(contract.ErrorConflict, "earlier Computer attempt still needs cleanup evidence")
	}
	result, err := q.ExecContext(ctx, `UPDATE computer_room_limits SET memory_bytes=? WHERE computer_id=? AND node_id=?`, report.HeldMemoryBytes, computer, report.NodeID)
	if err != nil {
		return SeatRoomMemoryReceipt{}, err
	}
	n, err := result.RowsAffected()
	if err != nil {
		return SeatRoomMemoryReceipt{}, err
	}
	if n != 1 {
		return SeatRoomMemoryReceipt{}, protocolError(contract.ErrorConflict, "Computer has no room reservation")
	}
	return SeatRoomMemoryReceipt{HeldMemoryBytes: report.HeldMemoryBytes, TargetMemoryBytes: target, RoomRevision: revision}, write.commit()
}

// A new node agent boot cannot still own the preceding boot's native dialog.
// Re-registering the same boot, or L1 alone restarting, does not lose a dialog.
func refuseLostSeatDialogs(ctx context.Context, q seatApprovalQuery, node, boot string) error {
	rows, err := q.QueryContext(ctx, `SELECT seat_id FROM seat_approvals WHERE node_id=? AND boot_session_id!=? AND COALESCE(NULLIF(lifecycle_state,''),state) IN ('requested','approved')`, node, boot)
	if err != nil {
		return err
	}
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return err
		}
		ids = append(ids, id)
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		return err
	}
	for _, id := range ids {
		v, err := readSeatRequest(ctx, q, id)
		if err != nil {
			return err
		}
		reason := "approval dialog lost"
		if v.State == SeatApproved {
			reason = "node agent restarted before Seat start"
		}
		if err := setSeatLifecycle(ctx, q, id, SeatRefused, reason); err != nil {
			return err
		}
		if err := cleanNeverStartedSeat(ctx, q, v); err != nil {
			return err
		}
	}
	return nil
}
