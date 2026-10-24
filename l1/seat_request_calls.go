package l1

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
)

// SeatCallInput is immutable caller input, never approval authority.
type SeatCallInput struct {
	SeatID      string `json:"seat_id"`
	NodeID      string `json:"node_id"`
	Harness     string `json:"harness"`
	Folder      string `json:"folder"`
	Form        string `json:"form"`
	Network     string `json:"network"`
	WindowHours int    `json:"window_hours"`
}
type SeatCallResult struct {
	SeatRequest
	Credential     string `json:"credential,omitempty"`
	CleanupPending bool   `json:"cleanup_pending"`
}
type SeatCallPoll struct {
	SeatNodeRequest
	Room        ComputerRoomConfig `json:"room"`
	HostEnabled bool               `json:"host_enabled"`
	AnswerWait  time.Duration      `json:"answer_wait"`
}
type SeatCallList struct {
	Requests []SeatCallInput `json:"requests"`
}

const seatCallSchema = `
CREATE TABLE IF NOT EXISTS seat_request_calls (
 seat_id TEXT PRIMARY KEY,
 node_id TEXT NOT NULL REFERENCES nodes(node_id),
 boot_session_id TEXT NOT NULL,
 caller_identity TEXT NOT NULL,
 input_json BLOB NOT NULL,
 credential TEXT NOT NULL,
 created_ns INTEGER NOT NULL,
 start_finished INTEGER NOT NULL DEFAULT 0 CHECK(start_finished IN (0,1))
);
CREATE INDEX IF NOT EXISTS seat_request_calls_node ON seat_request_calls(node_id,created_ns,seat_id);
`

// Selection is node-local and short. Room admission itself still uses #1142's
// SQLite write fence. No probe, dialog or runtime command holds this mutex.
// NewSeatRequestID shares the store ID format with every durable L1 identity.
func NewSeatRequestID() string { return newID("seat") }

var seatCallSelection sync.Mutex

func (s *Store) CreateSeatCall(ctx context.Context, caller string, input SeatCallInput) (SeatCallResult, error) {
	request := SeatApprovalRequest{SeatID: input.SeatID, NodeID: input.NodeID, BootSessionID: "validate", Harness: input.Harness, Folder: input.Folder, Form: input.Form, Network: input.Network, WindowHours: input.WindowHours, AnswerWait: time.Minute}
	if caller == "" {
		return SeatCallResult{}, protocolError(contract.ErrorForbidden, "Seat request requires caller identity")
	}
	if err := request.Validate(); err != nil {
		return SeatCallResult{}, err
	}
	data, err := json.Marshal(input)
	if err != nil {
		return SeatCallResult{}, err
	}
	write, err := s.beginWriteTransaction(ctx, nil)
	if err != nil {
		return SeatCallResult{}, err
	}
	defer write.rollback()
	var old []byte
	var owner string
	err = write.tx.QueryRowContext(ctx, `SELECT caller_identity,input_json FROM seat_request_calls WHERE seat_id=?`, input.SeatID).Scan(&owner, &old)
	if err == nil {
		if owner != caller {
			return SeatCallResult{}, protocolError(contract.ErrorForbidden, "Seat request belongs to another caller")
		}
		if string(old) != string(data) {
			return SeatCallResult{}, protocolError(contract.ErrorDispatchKeyConflict, "Seat request ID has different input")
		}
	} else if errors.Is(err, sql.ErrNoRows) {
		var used int
		if err := write.tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM seat_approvals WHERE seat_id=?`, input.SeatID).Scan(&used); err != nil {
			return SeatCallResult{}, err
		}
		if used != 0 {
			return SeatCallResult{}, protocolError(contract.ErrorDispatchKeyConflict, "Seat ID was already used")
		}
		var boot, state string
		if err := write.tx.QueryRowContext(ctx, `SELECT boot_session_id,state FROM nodes WHERE node_id=?`, input.NodeID).Scan(&boot, &state); err != nil {
			return SeatCallResult{}, protocolError(contract.ErrorNotFound, "Seat request node not found")
		}
		if state != string(contract.NodeAlive) {
			return SeatCallResult{}, protocolError(contract.ErrorUnavailable, "Seat request node is not active")
		}
		secret := make([]byte, 32)
		if _, err := rand.Read(secret); err != nil {
			return SeatCallResult{}, err
		}
		if _, err := write.tx.ExecContext(ctx, `INSERT INTO seat_request_calls(seat_id,node_id,boot_session_id,caller_identity,input_json,credential,created_ns) VALUES(?,?,?,?,?,?,?)`, input.SeatID, input.NodeID, boot, caller, data, hex.EncodeToString(secret), write.at.UnixNano()); err != nil {
			return SeatCallResult{}, err
		}
	} else {
		return SeatCallResult{}, err
	}
	if err := write.commit(); err != nil {
		return SeatCallResult{}, err
	}
	return s.GetSeatCall(ctx, caller, input.SeatID)
}
func (s *Store) GetSeatCall(ctx context.Context, caller, id string) (SeatCallResult, error) {
	var result SeatCallResult
	var expired bool
	err := s.withReadSnapshot(ctx, nil, func(ctx context.Context, reads readModel) error {
		r := reads.(*databaseReads)
		q := r.q
		var owner, secret string
		var startFinished bool
		if err := q.QueryRowContext(ctx, `SELECT caller_identity,credential,start_finished FROM seat_request_calls WHERE seat_id=?`, id).Scan(&owner, &secret, &startFinished); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return protocolError(contract.ErrorNotFound, "Seat request not found")
			}
			return err
		}
		if owner != caller {
			return protocolError(contract.ErrorForbidden, "Seat request belongs to another caller")
		}
		value, err := readSeatRequest(ctx, q, id)
		if errorCodeForSeatLifecycle(err) == contract.ErrorNotFound {
			result = SeatCallResult{SeatRequest: SeatRequest{SeatID: id, State: SeatRequested}}
			return nil
		}
		if err != nil {
			return err
		}
		expired, err = seatApprovalNeedsExpiry(ctx, q, value, r.now())
		if err != nil {
			return err
		}
		result = SeatCallResult{SeatRequest: value}
		if value.State == SeatLive {
			result.Credential = secret
		}
		terminal := value.State == SeatRefused || value.State == SeatEnding || value.State == SeatEnded
		result.CleanupPending = terminal && value.ComputerID != "" && !value.CleanupComplete
		// Only an unfinished new start owes whole-Computer deletion. A Seat
		// that reached Live may fail later beside other Seats on that Computer.
		if value.State == SeatRefused && !startFinished && value.ComputerID != "" {
			finished, err := computerHasFinishedSeatCall(ctx, q, value.ComputerID)
			if err != nil {
				return err
			}
			if !finished {
				var pending int
				if err := q.QueryRowContext(ctx, `SELECT COUNT(*) FROM computers WHERE name=? AND seat_request_made=1 AND removal_outcome!='removed_verified'`, seatCallComputerName(id)).Scan(&pending); err != nil {
					return err
				}
				result.CleanupPending = result.CleanupPending || pending > 0
			}
		}
		return nil
	})
	if err == nil && expired {
		if _, err := s.expireSeatApprovalRead(ctx, id, "", nil); err != nil {
			return SeatCallResult{}, err
		}
		return s.GetSeatCall(ctx, caller, id)
	}
	return result, err
}

func seatCallComputerName(id string) string { return "seat-" + strings.TrimPrefix(id, "seat_") }

// PollSeatCalls authenticates the node before exposing labels. Preflight creates
// the existing approval record, but never asks the owner or starts a Computer.
func (s *Store) PollSeatCalls(ctx context.Context, identity string, poll SeatCallPoll) (SeatCallList, error) {
	if err := poll.Room.Validate(); err != nil {
		return SeatCallList{}, err
	}
	if poll.AnswerWait <= 0 || poll.AnswerWait > MaxSeatApprovalWait {
		return SeatCallList{}, protocolError(contract.ErrorInvalidRequest, "invalid Seat answer wait")
	}
	type candidate struct {
		input   SeatCallInput
		boot    string
		created int64
	}
	var candidates []candidate
	err := s.withAgentReadSnapshot(ctx, func(ctx context.Context, reads readModel) error {
		q := reads.(*databaseReads).q
		if err := requireSeatApprovalNode(ctx, q, identity, poll.NodeID, poll.BootSessionID); err != nil {
			return err
		}
		rows, err := q.QueryContext(ctx, `SELECT c.input_json,c.boot_session_id,c.created_ns FROM seat_request_calls c LEFT JOIN seat_approvals a ON a.seat_id=c.seat_id WHERE c.node_id=? AND c.start_finished=0 AND (a.seat_id IS NULL OR COALESCE(NULLIF(a.lifecycle_state,''),a.state) IN ('requested','approved','starting') OR (COALESCE(NULLIF(a.lifecycle_state,''),a.state)='refused' AND EXISTS(SELECT 1 FROM computers failed WHERE failed.name='seat-' || substr(c.seat_id,6) AND failed.seat_request_made=1 AND failed.removal_outcome!='removed_verified' AND NOT EXISTS(SELECT 1 FROM seat_request_calls finished JOIN computer_seat_identities used ON used.seat_id=finished.seat_id WHERE used.computer_id=failed.computer_id AND finished.start_finished=1)))) ORDER BY c.created_ns,c.seat_id`, poll.NodeID)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var c candidate
			var data []byte
			if err := rows.Scan(&data, &c.boot, &c.created); err != nil {
				return err
			}
			if err := json.Unmarshal(data, &c.input); err != nil {
				return err
			}
			candidates = append(candidates, c)
		}
		return rows.Err()
	})
	if err != nil {
		return SeatCallList{}, err
	}

	result := SeatCallList{Requests: []SeatCallInput{}}
	for _, c := range candidates {
		v, err := s.GetSeatRequest(ctx, c.input.SeatID)
		if errorCodeForSeatLifecycle(err) == contract.ErrorNotFound {
			_, err = s.BeginSeatApproval(ctx, identity, SeatApprovalRequest{SeatID: c.input.SeatID, NodeID: poll.NodeID, BootSessionID: poll.BootSessionID, Harness: c.input.Harness, Folder: c.input.Folder, Form: c.input.Form, Network: c.input.Network, WindowHours: c.input.WindowHours, AnswerWait: poll.AnswerWait})
		}
		if err != nil {
			return result, err
		}
		// Include dispatch delay in the original deadline. Begin's immutable
		// AnswerWait remains the node setting for approval replay.
		if err := s.limitSeatCallDeadline(ctx, identity, c.input.SeatID, poll, time.Unix(0, c.created).Add(poll.AnswerWait)); err != nil {
			return result, err
		}

		reason := ""
		if c.boot != poll.BootSessionID {
			reason = "approval dialog lost"
		} else if (v.State == "" || v.State == SeatRequested) && !s.clock.Now().Before(time.Unix(0, c.created).Add(poll.AnswerWait)) {
			reason = "no answer from the owner"
		} else if !poll.HostEnabled {
			reason = "the host VM is off"
		} else if v.State == "" || v.State == SeatRequested || v.State == SeatApproved && v.ComputerID == "" {
			snap, e := s.GetComputerRoomSnapshot(ctx, poll.NodeID, poll.Room)
			if e != nil {
				return result, e
			}
			if snap.AvailableSeats == 0 {
				reason = ErrComputerRoomFull.Error()
			} else if c.input.Form == "dedicated" && snap.RunningDedicated >= poll.Room.DedicatedCap {
				reason = ErrDedicatedComputerCap.Error()
			}
		}
		if reason == "" && v.State == SeatStarting {
			if !s.clock.Now().Before(v.Deadline) {
				reason = "Seat readiness deadline exceeded"
			}
			if v.ComputerID != "" {
				computer, e := s.GetComputer(ctx, v.ComputerID)
				if e != nil {
					return result, e
				}
				if computer.CurrentJob.State == contract.JobFailed {
					reason = "Computer failed to start"
				}
			}
		}
		if reason != "" && v.State != SeatRefused {
			if _, err := s.EndSeatRequest(ctx, identity, c.input.SeatID, SeatEndRequest{NodeID: poll.NodeID, BootSessionID: poll.BootSessionID, Reason: reason, Refused: true, pendingOnly: true}); err != nil {
				return result, err
			}
		}
		v, err = s.GetSeatRequest(ctx, c.input.SeatID)
		if err != nil {
			return result, err
		}
		if v.State == SeatRefused {
			if err := s.cleanupSeatCallComputer(ctx, c.input.SeatID); err != nil {
				return result, err
			}
			continue
		}
		result.Requests = append(result.Requests, c.input)
	}
	return result, nil
}

// AdvanceSeatCall runs only after the dialog, under the node identity. Retries
// reuse both the permanent reservation and a deterministic creation key.
func (s *Store) AdvanceSeatCall(ctx context.Context, identity, id string, poll SeatCallPoll) (SeatRequest, error) {
	seatCallSelection.Lock()
	defer seatCallSelection.Unlock()
	if err := poll.Room.Validate(); err != nil {
		return SeatRequest{}, err
	}
	var value SeatRequest
	var approval SeatApproval
	var computerID string
	place := 0
	err := s.withAgentReadSnapshot(ctx, func(ctx context.Context, reads readModel) error {
		q := reads.(*databaseReads).q
		if err := authorizeSeatRequest(ctx, q, identity, id, poll.NodeID, poll.BootSessionID); err != nil {
			return err
		}
		var err error
		value, err = readSeatRequest(ctx, q, id)
		if err != nil {
			return err
		}
		if value.State != SeatApproved || value.ComputerID != "" {
			return nil
		}
		approval, err = readSeatApproval(ctx, q, id)
		if err != nil {
			return err
		}
		if approval.Request.Form == "any" {
			err = q.QueryRowContext(ctx, `SELECT c.computer_id,p.place FROM computers c JOIN jobs j ON j.job_id=c.current_job_id JOIN (SELECT 1 AS place UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4) p ON p.place<=c.seat_limit WHERE c.placement_node_id=? AND c.seat_request_made=1 AND c.seat_limit>1 AND c.network=? AND c.desired_state='running' AND j.state IN ('queued','claimed','running') AND NOT EXISTS (SELECT 1 FROM computer_seat_identities i JOIN computer_seat_room_reservations r ON r.seat_id=i.seat_id WHERE i.computer_id=c.computer_id AND i.place=p.place AND r.cleanup_complete=0) ORDER BY c.created_ns,c.computer_id,p.place LIMIT 1`, poll.NodeID, approval.Request.Network).Scan(&computerID, &place)
			if errors.Is(err, sql.ErrNoRows) {
				return nil
			}
			return err
		}
		return nil
	})
	if err != nil {
		return SeatRequest{}, err
	}
	if !poll.HostEnabled {
		return s.refuseSeatCall(ctx, identity, id, poll, "the host VM is off")
	}
	if value.State != SeatApproved {
		return value, nil
	}
	if value.ComputerID != "" {
		return s.StartSeatRequest(ctx, identity, id, SeatStartRequest{NodeID: poll.NodeID, BootSessionID: poll.BootSessionID, ReadyWait: time.Minute})
	}
	if computerID == "" {
		image, e := s.ComputerDefaultImage()
		if e != nil {
			return s.refuseSeatCall(ctx, identity, id, poll, e.Error())
		}
		ref, pin, _ := strings.Cut(image, "@")
		memory := poll.Room.SeatMemoryBytes
		spec := contract.JobSpec{SchemaVersion: contract.SchemaVersionV1, DispatchKey: "seat-request-" + id, Kind: contract.JobKindOCI, Class: contract.JobClassService, Restart: contract.RestartAlways, RoutingTags: []string{contract.StableNodeTagPrefix + poll.NodeID}, Execution: contract.ExecutionSpec{OCI: &contract.OCIExecutionSpec{Image: contract.OCIImageSpec{Reference: ref, Digest: &pin}, Limits: &contract.OCILimits{MemoryBytes: &memory}, Computer: &contract.OCIComputerSpec{Display: contract.OCIComputerDisplaySpec{Protocol: contract.ComputerDisplayProtocolRFBWebSocketV1}, DiskBytes: 8 << 30}}}}
		limit := s.ComputerSharedSeatLimit()
		if approval.Request.Form == "dedicated" {
			limit = 1
		}
		computer, _, e := s.createSeatCallComputer(ctx, identity, id, poll, CreateComputerRequest{Name: seatCallComputerName(id), Actor: "seat-request", Spec: spec, SeatRequestSettings: &ComputerSettings{SeatLimit: limit, IdleAction: ComputerIdleDelete, Network: ComputerNetwork(approval.Request.Network)}})
		if e != nil {
			return s.refuseSeatCall(ctx, identity, id, poll, e.Error())
		}
		computerID, place = computer.ComputerID, 1
	}

	value, err = s.ReserveSeatRoom(ctx, identity, id, SeatRoomReservationRequest{NodeID: poll.NodeID, BootSessionID: poll.BootSessionID, ComputerID: computerID, Place: place, Room: poll.Room})
	if err != nil {
		return s.refuseSeatCall(ctx, identity, id, poll, err.Error())
	}
	if value.State == SeatRefused {
		return value, s.cleanupSeatCallComputer(ctx, id)
	}
	return s.StartSeatRequest(ctx, identity, id, SeatStartRequest{NodeID: poll.NodeID, BootSessionID: poll.BootSessionID, ReadyWait: time.Minute})
}
func (s *Store) refuseSeatCall(ctx context.Context, identity, id string, poll SeatCallPoll, reason string) (SeatRequest, error) {
	v, err := s.EndSeatRequest(ctx, identity, id, SeatEndRequest{NodeID: poll.NodeID, BootSessionID: poll.BootSessionID, Reason: reason, Refused: true, pendingOnly: true})
	if err != nil {
		return v, err
	}
	if v.State != SeatRefused {
		return v, nil
	}
	return v, s.cleanupSeatCallComputer(ctx, id)
}
func (s *Store) cleanupSeatCallComputer(ctx context.Context, id string) error {
	var computer Computer
	err := s.withAgentReadSnapshot(ctx, func(ctx context.Context, reads readModel) error {
		r := reads.(*databaseReads)
		q := r.q
		var computerID string
		err := q.QueryRowContext(ctx, `SELECT computer_id FROM computers WHERE name=? AND seat_request_made=1 AND desired_state!='removed'`, seatCallComputerName(id)).Scan(&computerID)
		if errors.Is(err, sql.ErrNoRows) {
			return nil
		}
		if err != nil {
			return err
		}
		computer, err = readComputerAuthority(ctx, q, computerID, r.now())
		return err
	})
	if err != nil {
		return err
	}
	if computer.ComputerID == "" {
		return nil
	}
	_, err = s.RemoveComputer(ctx, computer.ComputerID, ComputerRemoveRequest{ComputerMutationPrecondition: ComputerMutationPrecondition{IntentRevision: computer.IntentRevision, StorageID: computer.StorageID, StorageGeneration: computer.StorageGeneration, Actor: "seat-request"}, failedSeatRequestID: id})
	return err
}
func (s *Store) limitSeatCallDeadline(ctx context.Context, identity, id string, poll SeatCallPoll, deadline time.Time) error {
	write, err := s.beginWriteTransaction(ctx, nil)
	if err != nil {
		return err
	}
	defer write.rollback()
	if err := authorizeSeatRequest(ctx, write.tx, identity, id, poll.NodeID, poll.BootSessionID); err != nil {
		return err
	}
	if _, err := write.tx.ExecContext(ctx, `UPDATE seat_approvals SET deadline_ns=MIN(deadline_ns,?) WHERE seat_id=? AND state='requested'`, deadline.UnixNano(), id); err != nil {
		return err
	}
	return write.commit()
}

// This trusted wrapper is deliberately separate from public CreateComputer:
// the client route cannot reach any approval or room-granting call graph.
func (s *Store) createSeatCallComputer(ctx context.Context, identity, id string, poll SeatCallPoll, request CreateComputerRequest) (Computer, bool, error) {
	write, err := s.beginWriteTransaction(ctx, nil)
	if err != nil {
		return Computer{}, false, err
	}
	defer write.rollback()
	computer, replay, err := s.createComputerTx(ctx, write.tx, write.at, request)
	if err != nil {
		return Computer{}, false, err
	}
	value, err := s.reserveSeatRoomTx(ctx, write.tx, write.at, identity, id, SeatRoomReservationRequest{NodeID: poll.NodeID, BootSessionID: poll.BootSessionID, ComputerID: computer.ComputerID, Place: 1, Room: poll.Room})
	if err != nil {
		return Computer{}, false, err
	}
	if value.State == SeatRefused {
		return Computer{}, false, errors.New(value.Reason)
	}
	if err := write.commit(); err != nil {
		return Computer{}, false, err
	}
	s.notifyComputerPolicyChanged()
	return computer, replay, nil
}

// Any Seat's positive readiness makes the Computer existing, including when a
// later request becomes ready before the request that created the Computer.
func computerHasFinishedSeatCall(ctx context.Context, q queryer, computer string) (bool, error) {
	var finished bool
	err := q.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM seat_request_calls c JOIN computer_seat_identities i ON i.seat_id=c.seat_id WHERE i.computer_id=? AND c.start_finished=1)`, computer).Scan(&finished)
	return finished, err
}
