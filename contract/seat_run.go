package contract

import (
	"encoding/json"
	"errors"
	"regexp"
	"strings"
	"time"
)

// SeatCredentialHeader is shared by the Seat caller and run-ledger protocols.
const SeatCredentialHeader = "X-Wefty-Seat-Credential"

// DefaultSeatCommandRuntime is the single default for every Seat Run.
const DefaultSeatCommandRuntime = time.Hour
const ErrorSeatRunRerun ErrorCode = "seat_run_not_replayable"

// SeatRunInput carries only argv and a deadline. No caller environment, user,
// directory, executable bytes or workload credentials cross into the Seat.
type SeatRunInput struct {
	SeatID            string   `json:"seat_id"`
	Argv              []string `json:"argv"`
	MaxRuntimeSeconds int64    `json:"max_runtime_seconds,omitempty"`
}

func (r *SeatRunInput) UnmarshalJSON(data []byte) error {
	type wire SeatRunInput
	var v wire
	if err := decodeJSONStrict(data, &v); err != nil {
		return err
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		return err
	}
	if _, present := fields["max_runtime_seconds"]; present && v.MaxRuntimeSeconds <= 0 {
		return errors.New("max_runtime_seconds must be positive; omit it for the one-hour default")
	}
	*r = SeatRunInput(v)
	return nil
}

var seatRunIDPattern = regexp.MustCompile(`^seat_[0-9a-f]{32}$`)

func (r *SeatRunInput) Normalize() error {
	if !seatRunIDPattern.MatchString(r.SeatID) || len(r.Argv) == 0 || r.Argv[0] == "" || len(r.Argv) > 4096 {
		return errors.New("Seat Run requires a Seat ID and argv")
	}
	size := 0
	for _, arg := range r.Argv {
		if strings.ContainsRune(arg, 0) {
			return errors.New("Seat Run argv contains NUL")
		}
		size += len(arg) + 1
	}
	if size > 128<<10 {
		return errors.New("Seat Run argv exceeds limit")
	}
	if r.MaxRuntimeSeconds == 0 {
		r.MaxRuntimeSeconds = int64(DefaultSeatCommandRuntime / time.Second)
	}
	if r.MaxRuntimeSeconds < 1 || r.MaxRuntimeSeconds > int64((1<<63-1)/time.Second) {
		return errors.New("Seat Run requires a positive finite max_runtime_seconds")
	}
	return nil
}

// SeatRunBinding contains public identity, never an Attempt or Seat credential.
// L1 rechecks it at dispatch and command start, including the first Attempt.
type SeatRunBinding struct {
	// DefaultMaxRuntimeSeconds is a submission setting, never execution authority.
	DefaultMaxRuntimeSeconds int64  `json:"default_max_runtime_seconds,omitempty"`
	SeatID                   string `json:"seat_id"`
	NodeID                   string `json:"node_id"`
	ComputerID               string `json:"computer_id"`
	ComputerJobID            string `json:"computer_job_id"`
	ComputerAttemptID        string `json:"computer_attempt_id"`
}
type SeatRunParams struct {
	Version int            `json:"seat_run_version"`
	SeatRun SeatRunInput   `json:"seat_run"`
	Binding SeatRunBinding `json:"seat_binding"`
}

func ParseSeatRunParams(raw []byte) (SeatRunParams, bool, error) {
	var fields map[string]json.RawMessage
	if json.Unmarshal(raw, &fields) != nil || fields["seat_run_version"] == nil {
		return SeatRunParams{}, false, nil
	}
	var p SeatRunParams
	var version int
	if json.Unmarshal(fields["seat_run_version"], &version) != nil || version != 1 {
		return p, true, errors.New("unsupported Seat Run metadata version")
	}
	if err := decodeJSONStrict(raw, &p); err != nil {
		return p, true, err
	}
	if err := p.SeatRun.Normalize(); err != nil {
		return p, true, err
	}
	b := p.Binding
	if b.SeatID != p.SeatRun.SeatID || b.NodeID == "" || b.ComputerID == "" || b.ComputerJobID == "" || b.ComputerAttemptID == "" {
		return p, true, errors.New("Seat Run binding is incomplete")
	}
	return p, true, nil
}
