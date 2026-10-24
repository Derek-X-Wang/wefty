package l1

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"regexp"
	"time"
	"unicode/utf8"

	"github.com/Derek-X-Wang/wefty/contract"
)

// SeatApprovalRequest is the dialog's immutable input. The Seat request caller
// must check room and OCI intent before handing it to the node agent (#1143).
// AnswerWait is supplied by the node agent's settings, never by the CLI.
type SeatApprovalRequest struct {
	SeatID        string        `json:"seat_id"`
	NodeID        string        `json:"node_id"`
	BootSessionID string        `json:"boot_session_id"`
	Harness       string        `json:"harness"`
	Folder        string        `json:"folder"`
	Form          string        `json:"form"`
	Network       string        `json:"network"` // internet or none; no default
	WindowHours   int           `json:"window_hours"`
	AnswerWait    time.Duration `json:"answer_wait"`
}

type SeatApproval struct {
	Request       SeatApprovalRequest `json:"request"`
	State         string              `json:"state"` // requested, approved, refused
	Reason        string              `json:"reason,omitempty"`
	Source        string              `json:"source,omitempty"` // dialog or window
	Deadline      time.Time           `json:"deadline"`
	RemainingWait time.Duration       `json:"remaining_wait"`
}

type SeatApprovalAnswer struct {
	NodeID        string `json:"node_id"`
	BootSessionID string `json:"boot_session_id"`
	Approved      bool   `json:"approved"`
	Reason        string `json:"reason,omitempty"`
}

type SeatApprovalWindow struct {
	NodeID    string    `json:"node_id"`
	Network   string    `json:"network"`
	ExpiresAt time.Time `json:"expires_at"`
}

const seatApprovalSchema = `
CREATE TABLE IF NOT EXISTS seat_approvals (
 seat_id TEXT PRIMARY KEY,
 node_id TEXT NOT NULL REFERENCES nodes(node_id),
 boot_session_id TEXT NOT NULL,
 request_json BLOB NOT NULL,
 state TEXT NOT NULL CHECK(state IN ('requested','approved','refused')),
 reason TEXT NOT NULL DEFAULT '',
 source TEXT NOT NULL DEFAULT '',
 deadline_ns INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS seat_approval_windows (
 node_id TEXT NOT NULL REFERENCES nodes(node_id),
 boot_session_id TEXT NOT NULL,
 network TEXT NOT NULL CHECK(network IN ('internet','none')),
 expires_ns INTEGER NOT NULL,
 PRIMARY KEY(node_id,network)
);
`

type seatApprovalQuery interface {
	queryer
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}

const MaxSeatApprovalWait = time.Hour

var seatApprovalID = regexp.MustCompile(`^seat_[0-9a-f]{32}$`)

func (r SeatApprovalRequest) Validate() error {
	if !seatApprovalID.MatchString(r.SeatID) || r.NodeID == "" || r.BootSessionID == "" ||
		utf8.RuneCountInString(r.Harness) > 256 || r.Folder == "" || utf8.RuneCountInString(r.Folder) > 4096 ||
		(r.Form != "any" && r.Form != "dedicated") || (r.Network != "internet" && r.Network != "none") ||
		r.AnswerWait <= 0 || r.AnswerWait > MaxSeatApprovalWait || (r.WindowHours != 0 && r.WindowHours != 1 && r.WindowHours != 8 && r.WindowHours != 24) {
		return protocolError(contract.ErrorInvalidRequest, "invalid Seat approval request")
	}
	return nil
}

// BeginSeatApproval only accepts the current, registered node identity. A
// window is consumed inside this write, so a close cannot race a cached allow.
func (s *Store) BeginSeatApproval(ctx context.Context, identityNodeID string, request SeatApprovalRequest) (SeatApproval, error) {
	if err := request.Validate(); err != nil {
		return SeatApproval{}, err
	}
	write, err := s.beginWriteTransaction(ctx, nil)
	if err != nil {
		return SeatApproval{}, err
	}
	defer write.rollback()
	q := seatApprovalQuery(write.tx)
	if err := persistSeatClockAnchor(ctx, q, s.clock); err != nil {
		return SeatApproval{}, err
	}
	if err := requireSeatApprovalNode(ctx, q, identityNodeID, request.NodeID, request.BootSessionID); err != nil {
		return SeatApproval{}, err
	}
	if err := expireSeatApproval(ctx, q, request.SeatID, write.at, s.clock); err != nil {
		return SeatApproval{}, err
	}
	existing, err := readSeatApproval(ctx, q, request.SeatID)
	if err == nil {
		if existing.Request != request {
			return SeatApproval{}, protocolError(contract.ErrorConflict, "Seat approval ID has different input")
		}
		if existing.State == "requested" {
			existing.RemainingWait, err = seatApprovalRemaining(ctx, q, s.clock, existing)
			if err != nil {
				return SeatApproval{}, err
			}
		}
		return existing, write.commit()
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return SeatApproval{}, err
	}
	value := SeatApproval{Request: request, State: "requested", Deadline: write.at.Add(request.AnswerWait), RemainingWait: request.AnswerWait}
	windows, err := readSeatApprovalWindows(ctx, q, request.NodeID, write.at, s.clock)
	if err != nil {
		return SeatApproval{}, err
	}
	// Asking for a new window always needs a dialog: consuming one cannot
	// silently extend it, broaden its network, or open a second window.
	if request.WindowHours == 0 {
		for _, window := range windows {
			if window.Network == request.Network || window.Network == "internet" {
				value.State = "approved"
				value.Source = "window"
				break
			}
		}
	}
	if err := saveSeatApproval(ctx, q, value, write.at); err != nil {
		return SeatApproval{}, err
	}
	boot, ns, err := seatTime(s.clock)
	if err != nil {
		return SeatApproval{}, err
	}
	if _, err := q.ExecContext(ctx, `UPDATE seat_approvals SET wait_boot_id=?,wait_started_ns=? WHERE seat_id=?`, boot, ns, request.SeatID); err != nil {
		return SeatApproval{}, err
	}
	return value, write.commit()
}

func (s *Store) AnswerSeatApproval(ctx context.Context, identityNodeID, seatID string, answer SeatApprovalAnswer) (SeatApproval, error) {
	write, err := s.beginWriteTransaction(ctx, nil)
	if err != nil {
		return SeatApproval{}, err
	}
	defer write.rollback()
	q := seatApprovalQuery(write.tx)
	if err := requireSeatApprovalNode(ctx, q, identityNodeID, answer.NodeID, answer.BootSessionID); err != nil {
		return SeatApproval{}, err
	}
	if err := expireSeatApproval(ctx, q, seatID, write.at, s.clock); err != nil {
		return SeatApproval{}, err
	}
	value, err := readSeatApproval(ctx, q, seatID)
	if errors.Is(err, sql.ErrNoRows) {
		return SeatApproval{}, protocolError(contract.ErrorNotFound, "Seat approval was not found")
	}
	if err != nil {
		return SeatApproval{}, err
	}
	if value.Request.NodeID != answer.NodeID || value.Request.BootSessionID != answer.BootSessionID {
		return SeatApproval{}, protocolError(contract.ErrorForbidden, "Seat approval belongs to another node session")
	}
	if value.State != "requested" {
		return value, write.commit()
	} // a repeated answer cannot extend a window
	value.Source = "dialog"
	value.State = "refused"
	switch {
	case answer.Approved:
		value.State = "approved"
		if value.Request.WindowHours != 0 {
			boot, ns, clockErr := seatTime(s.clock)
			if clockErr != nil {
				return SeatApproval{}, clockErr
			}
			_, err = q.ExecContext(ctx, `INSERT INTO seat_approval_windows(node_id,boot_session_id,network,expires_ns,wait_boot_id,wait_started_ns,wait_duration_ns) VALUES(?,?,?,?,?,?,?)
    ON CONFLICT(node_id,network) DO UPDATE SET boot_session_id=excluded.boot_session_id,expires_ns=excluded.expires_ns,wait_boot_id=excluded.wait_boot_id,wait_started_ns=excluded.wait_started_ns,wait_duration_ns=excluded.wait_duration_ns`,
				answer.NodeID, answer.BootSessionID, value.Request.Network, write.at.Add(time.Duration(value.Request.WindowHours)*time.Hour).UnixNano(), boot, ns, int64(time.Duration(value.Request.WindowHours)*time.Hour))
			if err != nil {
				return SeatApproval{}, err
			}
		}
	default:
		// Refusal text is restricted to node-owned outcomes; untrusted label text
		// never becomes an authority decision or an executable string.
		value.Reason = "denied by the owner"
		if answer.Reason == "no answer from the owner" || answer.Reason == "approval dialog unavailable" || answer.Reason == "approval dialog lost" {
			value.Reason = answer.Reason
		}
	}
	if err := saveSeatApproval(ctx, q, value, write.at); err != nil {
		return SeatApproval{}, err
	}
	return value, write.commit()
}

func requireSeatApprovalNode(ctx context.Context, q queryer, identity, nodeID, boot string) error {
	var storedIdentity, storedBoot, state string
	err := q.QueryRowContext(ctx, `SELECT identity_node_id,boot_session_id,state FROM nodes WHERE node_id=?`, nodeID).Scan(&storedIdentity, &storedBoot, &state)
	if errors.Is(err, sql.ErrNoRows) || err == nil && (identity == "" || identity != storedIdentity || boot != storedBoot || state == string(contract.NodeDead)) {
		return protocolError(contract.ErrorForbidden, "Seat approval requires the current node identity and boot session")
	}
	return err
}

func readSeatApproval(ctx context.Context, q queryer, id string) (SeatApproval, error) {
	var value SeatApproval
	var request []byte
	var deadline int64
	err := q.QueryRowContext(ctx, `SELECT request_json,CASE WHEN lifecycle_state IN ('ending','ended','refused') THEN 'refused' ELSE state END,CASE WHEN lifecycle_state IN ('ending','ended','refused') THEN lifecycle_reason ELSE reason END,source,deadline_ns FROM seat_approvals WHERE seat_id=?`, id).Scan(&request, &value.State, &value.Reason, &value.Source, &deadline)
	if err != nil {
		return value, err
	}
	err = json.Unmarshal(request, &value.Request)
	value.Deadline = time.Unix(0, deadline).UTC()
	return value, err
}
func saveSeatApproval(ctx context.Context, q seatApprovalQuery, value SeatApproval, at time.Time) error {
	request, err := json.Marshal(value.Request)
	if err != nil {
		return err
	}
	_, err = q.ExecContext(ctx, `INSERT INTO seat_approvals(seat_id,node_id,boot_session_id,request_json,state,reason,source,deadline_ns) VALUES(?,?,?,?,?,?,?,?)
 ON CONFLICT(seat_id) DO UPDATE SET state=excluded.state,reason=excluded.reason,source=excluded.source`, value.Request.SeatID, value.Request.NodeID, value.Request.BootSessionID, request, value.State, value.Reason, value.Source, value.Deadline.UnixNano())
	if err != nil {
		return err
	}
	if err := createSeatLog(ctx, q, value.Request.SeatID, at); err != nil {
		return err
	}
	if err := appendSeatEvent(ctx, q, at, SeatLogEvent{SeatID: value.Request.SeatID, EventID: "requested", Kind: SeatEventRequested}); err != nil {
		return err
	}
	if value.State == "requested" {
		return nil
	}
	return appendSeatEvent(ctx, q, at, SeatLogEvent{SeatID: value.Request.SeatID, EventID: value.State, Kind: SeatEventKind(value.State), Reason: value.Reason})
}
func readSeatApprovalWindows(ctx context.Context, q queryer, nodeID string, now time.Time, clock Clock) ([]SeatApprovalWindow, error) {
	rows, err := q.QueryContext(ctx, `SELECT w.node_id,w.network,w.expires_ns,w.wait_boot_id,w.wait_started_ns,w.wait_duration_ns FROM seat_approval_windows w JOIN nodes n ON n.node_id=w.node_id AND n.boot_session_id=w.boot_session_id
 WHERE (?='' OR w.node_id=?) AND n.state!='dead' ORDER BY w.node_id,w.network`, nodeID, nodeID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := []SeatApprovalWindow{}
	for rows.Next() {
		var value SeatApprovalWindow
		var expires, started, duration int64
		var boot string
		if err := rows.Scan(&value.NodeID, &value.Network, &expires, &boot, &started, &duration); err != nil {
			return nil, err
		}
		current, ns, err := seatTime(clock)
		if err != nil {
			return nil, err
		}
		if boot == "" || boot != current || ns < started || time.Duration(ns-started) >= time.Duration(duration) {
			continue
		}
		value.ExpiresAt = time.Unix(0, expires).UTC()
		result = append(result, value)
	}
	return result, rows.Err()
}
func (r *databaseReads) seatApprovalWindows(ctx context.Context, nodeID string) ([]SeatApprovalWindow, error) {
	clock := r.seatClock
	if clock == nil {
		clock = ClockFunc(func() time.Time { return r.at })
	}
	return readSeatApprovalWindows(ctx, r.q, nodeID, r.at, clock)
}
func (s *Store) ListSeatApprovalWindows(ctx context.Context, nodeID string) ([]SeatApprovalWindow, error) {
	var result []SeatApprovalWindow
	err := s.withReadSnapshot(ctx, nil, func(ctx context.Context, reads readModel) error {
		var err error
		result, err = reads.seatApprovalWindows(ctx, nodeID)
		return err
	})
	return result, err
}

// CloseSeatApprovalWindows only removes authority. With no network it closes
// both windows for this node; it never changes an already approved Seat.
func (s *Store) CloseSeatApprovalWindows(ctx context.Context, nodeID, network string) error {
	if nodeID == "" || network != "" && network != "internet" && network != "none" {
		return protocolError(contract.ErrorInvalidRequest, "a node and valid network are required")
	}
	write, err := s.beginWriteTransaction(ctx, nil)
	if err != nil {
		return err
	}
	defer write.rollback()
	if _, err := write.tx.ExecContext(ctx, `DELETE FROM seat_approval_windows WHERE node_id=? AND (?='' OR network=?)`, nodeID, network, network); err != nil {
		return err
	}
	return write.commit()
}
