package l1

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
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
	UnreadEvents int `json:"unread_events"`
	SeatRequest
	Credential     string `json:"credential,omitempty"`
	CleanupPending bool   `json:"cleanup_pending"`
}
type SeatCallPoll struct {
	SeatNodeRequest
	Room        ComputerRoomConfig `json:"room"`
	HostEnabled bool               `json:"host_enabled"`
	AnswerWait  time.Duration      `json:"answer_wait"`
	WindowHours []int              `json:"window_hours"`
}
type SeatCallList struct {
	Requests []SeatCallInput `json:"requests"`
}

// CapabilitySeatRequests advertises configured room, independently of runtime readiness.
const CapabilitySeatRequests = "computer:seat-requests"

const seatCallSchema = `
CREATE TABLE IF NOT EXISTS seat_request_calls (
 seat_id TEXT PRIMARY KEY,
 node_id TEXT NOT NULL REFERENCES nodes(node_id) ON DELETE CASCADE,
 boot_session_id TEXT NOT NULL,
 caller_identity TEXT NOT NULL,
 input_json BLOB NOT NULL,
 credential TEXT NOT NULL,
 created_ns INTEGER NOT NULL,
 start_finished INTEGER NOT NULL DEFAULT 0 CHECK(start_finished IN (0,1))
);
CREATE INDEX IF NOT EXISTS seat_request_calls_node ON seat_request_calls(node_id,created_ns,seat_id);
`

// NewSeatRequestID shares the store ID format with every durable L1 identity.
func NewSeatRequestID() string { return newID("seat") }

// Selection is node-local and short. Room admission still uses SQLite's
// write fence; no probe, dialog or runtime command holds this mutex.
type seatCallNodeSelection struct {
	sync.Mutex
	users int
}

func (s *Store) lockSeatCallSelection(node string) func() {
	s.seatCallSelectionMu.Lock()
	if s.seatCallSelections == nil {
		s.seatCallSelections = map[string]*seatCallNodeSelection{}
	}
	entry := s.seatCallSelections[node]
	if entry == nil {
		entry = &seatCallNodeSelection{}
		s.seatCallSelections[node] = entry
	}
	entry.users++
	s.seatCallSelectionMu.Unlock()
	entry.Lock()
	return func() {
		entry.Unlock()
		s.seatCallSelectionMu.Lock()
		entry.users--
		if entry.users == 0 {
			delete(s.seatCallSelections, node)
		}
		s.seatCallSelectionMu.Unlock()
	}
}

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
		if err := write.tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM seat_approvals WHERE seat_id=? UNION ALL SELECT 1 FROM computer_seat_identities WHERE seat_id=? UNION ALL SELECT 1 FROM seat_logs WHERE seat_id=?)`, input.SeatID, input.SeatID, input.SeatID).Scan(&used); err != nil {
			return SeatCallResult{}, err
		}
		if used != 0 {
			return SeatCallResult{}, protocolError(contract.ErrorDispatchKeyConflict, "Seat ID was already used")
		}
		var boot, state string
		var capabilities []byte
		if err := write.tx.QueryRowContext(ctx, `SELECT boot_session_id,state,capabilities_json FROM nodes WHERE node_id=?`, input.NodeID).Scan(&boot, &state, &capabilities); err != nil {
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
		if err := createSeatLog(ctx, write.tx, input.SeatID, write.at); err != nil {
			return SeatCallResult{}, err
		}
		if err := appendSeatEvent(ctx, write.tx, write.at, SeatLogEvent{SeatID: input.SeatID, EventID: "requested", Kind: SeatEventRequested}); err != nil {
			return SeatCallResult{}, err
		}
		if _, err := write.tx.ExecContext(ctx, `UPDATE seat_logs SET caller_identity=?,credential_hash=? WHERE seat_id=?`, caller, seatCredentialHash(hex.EncodeToString(secret)), input.SeatID); err != nil {
			return SeatCallResult{}, err
		}
		var caps map[string]bool
		if err := json.Unmarshal(capabilities, &caps); err != nil {
			return SeatCallResult{}, err
		}
		if !caps[CapabilitySeatRequests] {
			if err := refuseUnpickedSeatCall(ctx, write.tx, input, boot, write.at, "Computer room is not configured on this node"); err != nil {
				return SeatCallResult{}, err
			}
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
	var expired, unpickedExpired bool
	var readOrdinal, outcomeOrdinal int64
	readCall := func(ctx context.Context, q queryer, at time.Time) error {
		expired, unpickedExpired = false, false
		var owner, secret string
		var startFinished bool
		var created int64
		if err := q.QueryRowContext(ctx, `SELECT caller_identity,credential,start_finished,created_ns FROM seat_request_calls WHERE seat_id=?`, id).Scan(&owner, &secret, &startFinished, &created); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return protocolError(contract.ErrorNotFound, "Seat request not found")
			}
			return err
		}
		if owner != caller {
			return protocolError(contract.ErrorForbidden, "Seat request belongs to another caller")
		}
		var unread int
		if err := q.QueryRowContext(ctx, `SELECT COUNT(*) FROM seat_log_events e JOIN seat_logs l ON l.seat_id=e.seat_id WHERE e.seat_id=? AND e.ordinal>l.read_ordinal`, id).Scan(&unread); err != nil {
			return err
		}
		value, err := readSeatRequest(ctx, q, id)
		if errorCodeForSeatLifecycle(err) == contract.ErrorNotFound {
			unpickedExpired = !at.Before(time.Unix(0, created).Add(MaxSeatApprovalWait))
			result = SeatCallResult{SeatRequest: SeatRequest{SeatID: id, State: SeatRequested}, UnreadEvents: unread}
			return nil
		}
		if err != nil {
			return err
		}
		expired, err = seatApprovalNeedsExpiry(ctx, q, value, at, s.clock)
		if err != nil {
			return err
		}
		result = SeatCallResult{SeatRequest: value, UnreadEvents: unread}
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
	}
	read := func(ctx context.Context, q queryer, at time.Time) error {
		readOrdinal, outcomeOrdinal = 0, 0
		if err := readCall(ctx, q, at); err != nil {
			return err
		}
		kind := SeatEventKind(result.State)
		switch result.State {
		case SeatLive:
			kind = SeatEventStarted
		case SeatEnding, SeatEnded:
			kind = SeatEventEnded
		}
		err := q.QueryRowContext(ctx, `SELECT read_ordinal,COALESCE((SELECT MAX(ordinal) FROM seat_log_events WHERE seat_id=? AND event_id IN (?,'upgrade-state') AND json_extract(event_json,'$.kind')=?),0) FROM seat_logs WHERE seat_id=?`, id, string(kind), kind, id).Scan(&readOrdinal, &outcomeOrdinal)
		if errors.Is(err, sql.ErrNoRows) {
			return nil
		}
		if err != nil || outcomeOrdinal <= readOrdinal {
			return err
		}
		// A cursor can acknowledge only a contiguous prefix. An owner note or
		// any other non-lifecycle event is still unread even if a later outcome
		// is reported, so stop immediately before that event.
		var barrier int64
		err = q.QueryRowContext(ctx, `SELECT COALESCE(MIN(ordinal),0) FROM seat_log_events WHERE seat_id=? AND ordinal>? AND ordinal<=? AND json_extract(event_json,'$.kind') NOT IN ('requested','approved','refused','starting','started','released','ended')`, id, readOrdinal, outcomeOrdinal).Scan(&barrier)
		if barrier > 0 {
			outcomeOrdinal = barrier - 1
		}

		return err
	}
	err := s.withReadSnapshot(ctx, nil, func(ctx context.Context, reads readModel) error {
		r := reads.(*databaseReads)
		return read(ctx, r.q, r.now())
	})
	if err == nil && !expired && !unpickedExpired && outcomeOrdinal > readOrdinal {
		// Only an outcome not already reported needs a write. Re-read caller,
		// state and event boundary under the same fence that acknowledges it.
		write, e := s.beginWriteTransaction(ctx, nil)
		if e != nil {
			return SeatCallResult{}, e
		}
		defer write.rollback()
		if e = read(ctx, write.tx, write.at); e != nil {
			return SeatCallResult{}, e
		}
		if !expired && !unpickedExpired {
			if _, e = write.tx.ExecContext(ctx, `UPDATE seat_logs SET read_ordinal=MAX(read_ordinal,?) WHERE seat_id=?`, outcomeOrdinal, id); e != nil {
				return SeatCallResult{}, e
			}
			if e = write.tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM seat_log_events e JOIN seat_logs l ON l.seat_id=e.seat_id WHERE e.seat_id=? AND e.ordinal>l.read_ordinal`, id).Scan(&result.UnreadEvents); e != nil {
				return SeatCallResult{}, e
			}
		}
		if e = write.commit(); e != nil {
			return SeatCallResult{}, e
		}
	}
	if err == nil && unpickedExpired {
		if err := s.expireUnpickedSeatCall(ctx, caller, id); err != nil {
			return SeatCallResult{}, err
		}
		return s.GetSeatCall(ctx, caller, id)
	}
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
	windows := poll.WindowHours
	if windows == nil {
		windows = []int{1, 8, 24}
	}
	for _, hours := range windows {
		if hours != 1 && hours != 8 && hours != 24 {
			return SeatCallList{}, protocolError(contract.ErrorInvalidRequest, "invalid Seat approval window hours")
		}
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
		rows, err := q.QueryContext(ctx, `SELECT c.input_json,c.boot_session_id,c.created_ns FROM seat_request_calls c LEFT JOIN seat_approvals a ON a.seat_id=c.seat_id WHERE c.node_id=? AND c.start_finished=0 AND (a.seat_id IS NULL OR COALESCE(NULLIF(a.lifecycle_state,''),a.state) IN ('requested','approved','starting') OR (COALESCE(NULLIF(a.lifecycle_state,''),a.state)='refused' AND EXISTS(SELECT 1 FROM computers failed WHERE failed.name='seat-' || substr(c.seat_id,6) AND failed.seat_request_made=1 AND failed.removal_outcome!='removed_verified' AND NOT EXISTS(SELECT 1 FROM computer_seat_identities used WHERE used.computer_id=failed.computer_id AND used.ever_live=1)))) ORDER BY c.created_ns,c.seat_id`, poll.NodeID)
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

	process := func(c candidate) (bool, error) {
		v, err := s.GetSeatRequest(ctx, c.input.SeatID)
		if errorCodeForSeatLifecycle(err) == contract.ErrorNotFound {
			_, err = s.BeginSeatApproval(ctx, identity, SeatApprovalRequest{SeatID: c.input.SeatID, NodeID: poll.NodeID, BootSessionID: poll.BootSessionID, Harness: c.input.Harness, Folder: c.input.Folder, Form: c.input.Form, Network: c.input.Network, WindowHours: c.input.WindowHours, AnswerWait: poll.AnswerWait})
		}
		if err != nil {
			return false, err
		}
		// Include dispatch delay in the original deadline. Begin's immutable
		// AnswerWait remains the node setting for approval replay.
		if err := s.limitSeatCallDeadline(ctx, identity, c.input.SeatID, poll, time.Unix(0, c.created).Add(poll.AnswerWait)); err != nil {
			return false, err
		}

		reason := ""
		preflightOnly := false
		if c.boot != poll.BootSessionID {
			reason = "approval dialog lost"
		} else if (v.State == "" || v.State == SeatRequested) && !s.clock.Now().Before(time.Unix(0, c.created).Add(poll.AnswerWait)) {
			reason = "no answer from the owner"
		} else if c.input.WindowHours != 0 && !slices.Contains(windows, c.input.WindowHours) && (v.State == "" || v.State == SeatRequested) {
			allowed := []string{}
			for _, h := range windows {
				allowed = append(allowed, fmt.Sprintf("%dh", h))
			}
			label := strings.Join(allowed, ", ")
			if label == "" {
				label = "none"
			}
			reason = fmt.Sprintf("Seat approval window %dh is not enabled; allowed windows: %s", c.input.WindowHours, label)
			preflightOnly = true
		} else if !poll.HostEnabled {
			reason = "the host VM is off"
		} else if v.State == "" || v.State == SeatRequested {
			preflightOnly = true
			snap, e := s.GetComputerRoomSnapshot(ctx, poll.NodeID, poll.Room)
			if e != nil {
				return false, e
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
					return false, e
				}
				if computer.CurrentJob.State == contract.JobFailed {
					reason = "Computer failed to start"
				}
			}
		}
		if reason != "" && v.State != SeatRefused {
			if _, err := s.EndSeatRequest(ctx, identity, c.input.SeatID, SeatEndRequest{NodeID: poll.NodeID, BootSessionID: poll.BootSessionID, Reason: reason, Refused: true, pendingOnly: true, preflightOnly: preflightOnly}); err != nil {
				return false, err
			}
		}
		v, err = s.GetSeatRequest(ctx, c.input.SeatID)
		if err != nil {
			return false, err
		}
		if v.State == SeatRefused {
			if err := s.cleanupSeatCallComputer(ctx, c.input.SeatID); err != nil {
				return false, err
			}
			return false, nil
		}
		return true, nil
	}
	result := SeatCallList{Requests: []SeatCallInput{}}
	for _, c := range candidates {
		include, err := process(c)
		if err != nil {
			s.logf("l1: Seat request %s on node %s: %v", c.input.SeatID, poll.NodeID, err)
			if ctx.Err() != nil {
				return result, ctx.Err()
			}
			continue
		}
		if include {
			result.Requests = append(result.Requests, c.input)
		}
	}
	return result, nil
}

// AdvanceSeatCall runs only after the dialog, under the node identity. Retries
// reuse both the permanent reservation and a deterministic creation key.
func (s *Store) AdvanceSeatCall(ctx context.Context, identity, id string, poll SeatCallPoll) (SeatRequest, error) {
	unlock := s.lockSeatCallSelection(poll.NodeID)
	defer unlock()
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
		return s.StartSeatRequest(ctx, identity, id, SeatStartRequest{NodeID: poll.NodeID, BootSessionID: poll.BootSessionID, ReadyWait: s.seatCallReadyWait()})
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
	return s.StartSeatRequest(ctx, identity, id, SeatStartRequest{NodeID: poll.NodeID, BootSessionID: poll.BootSessionID, ReadyWait: s.seatCallReadyWait()})
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
	err := q.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM computer_seat_identities WHERE computer_id=? AND ever_live=1)`, computer).Scan(&finished)
	return finished, err
}

// Refusal is the only authority available before a node picks up the request.
func refuseUnpickedSeatCall(ctx context.Context, q seatApprovalQuery, input SeatCallInput, boot string, at time.Time, reason string) error {
	request := SeatApprovalRequest{SeatID: input.SeatID, NodeID: input.NodeID, BootSessionID: boot, Harness: input.Harness, Folder: input.Folder, Form: input.Form, Network: input.Network, WindowHours: input.WindowHours, AnswerWait: MaxSeatApprovalWait}
	data, err := json.Marshal(request)
	if err != nil {
		return err
	}
	// This public-call path can only insert a refusal. It cannot grant or
	// overwrite approval, even if another writer picked up the request.
	_, err = q.ExecContext(ctx, `INSERT INTO seat_approvals(seat_id,node_id,boot_session_id,request_json,state,reason,deadline_ns) VALUES(?,?,?,?,'refused',?,?)`, input.SeatID, input.NodeID, boot, data, reason, at.UnixNano())
	if err != nil {
		return err
	}
	return appendSeatEvent(ctx, q, at, SeatLogEvent{SeatID: input.SeatID, EventID: "refused", Kind: SeatEventRefused, Reason: reason})
}
func (s *Store) expireUnpickedSeatCall(ctx context.Context, caller, id string) error {
	write, err := s.beginWriteTransaction(ctx, nil)
	if err != nil {
		return err
	}
	defer write.rollback()
	var owner, boot string
	var data []byte
	var created int64
	var picked bool
	if err := write.tx.QueryRowContext(ctx, `SELECT caller_identity,boot_session_id,input_json,created_ns,EXISTS(SELECT 1 FROM seat_approvals WHERE seat_id=c.seat_id) FROM seat_request_calls c WHERE seat_id=?`, id).Scan(&owner, &boot, &data, &created, &picked); err != nil {
		return err
	}
	if owner != caller {
		return protocolError(contract.ErrorForbidden, "Seat request belongs to another caller")
	}
	if !picked && !write.at.Before(time.Unix(0, created).Add(MaxSeatApprovalWait)) {
		var input SeatCallInput
		if err := json.Unmarshal(data, &input); err != nil {
			return err
		}
		if err := refuseUnpickedSeatCall(ctx, write.tx, input, boot, write.at, "the node did not pick up the request"); err != nil {
			return err
		}
	}
	return write.commit()
}

// EnsureImage may pull and unpack the pinned default image before task start.
// Reuse L1's bounded prestart infrastructure budget, plus the helper's own
// 60-second endpoint-readiness interval. Start persists this duration once;
// polling and replay never reset it, including while a Computer is queued.
const seatCallEndpointReadinessWait = time.Minute

func (s *Store) seatCallReadyWait() time.Duration {
	return s.prestartInfrastructureBudget + seatCallEndpointReadinessWait
}
