package ocihelper

import (
	"context"
	_ "embed"
	"errors"
	"fmt"
	"strings"
	"time"
)

const MethodExecComputerSeat Method = "ExecComputerSeat"
const SeatCommandChunkBytes = 4096
const seatCommandFrameBytes = 8192
const SeatRefusalCommandNotReady SeatOperationRefusalReason = "seat_not_ready"

// ExecComputerSeatRequest deliberately accepts no environment, user or path.
// Generation fences place reuse and every change of the Seat directive.
type ExecComputerSeatRequest struct {
	Authority  AttemptAuthority `json:"authority"`
	Generation uint64           `json:"generation"`
	Place      int              `json:"place"`
	Args       []string         `json:"args"`
	MaxRuntime time.Duration    `json:"max_runtime"`
}

type SeatCommandExit struct {
	Code   *int `json:"code,omitempty"`
	Signal int  `json:"signal,omitempty"`
}
type SeatCommandEvent struct {
	Stream string           `json:"stream,omitempty"`
	Data   []byte           `json:"data,omitempty"`
	Exit   *SeatCommandExit `json:"exit,omitempty"`
}
type ComputerSeatExecEngine interface {
	ExecComputerSeat(context.Context, ExecComputerSeatRequest, func(SeatCommandEvent) error) error
}

//go:embed computer_seat_exec.py
var seatCommandWrapper string

type seatCommandConfig struct {
	UID, GID    uint32
	Directory   string
	Environment []string
}

func (r ExecComputerSeatRequest) validate() error {
	if err := r.Authority.validate(); err != nil {
		return err
	}
	if r.Generation == 0 || r.Place < 1 || r.Place > 4 || len(r.Args) == 0 || r.Args[0] == "" || r.MaxRuntime <= 0 {
		return errors.New("command requires a Seat generation, place, argv and positive runtime")
	}
	size := 0
	for _, arg := range r.Args {
		if strings.ContainsRune(arg, 0) {
			return errors.New("command contains NUL")
		}
		size += len(arg) + 1
	}
	if size > 128<<10 || len(r.Args) > 4096 {
		return errors.New("command argv exceeds limit")
	}
	return nil
}
func (s *computerSeats) command(r ExecComputerSeatRequest) (seatCommandConfig, error) {
	if s == nil || r.Generation == 0 || s.generation != r.Generation {
		return seatCommandConfig{}, &SeatOperationRefusalError{SeatRefusalGenerationMismatch}
	}
	e := s.entries[r.Place]
	if e == nil || !e.wanted || e.cleaned || !e.ready || e.readyGeneration != r.Generation || e.failure != "" {
		return seatCommandConfig{}, &SeatOperationRefusalError{SeatRefusalCommandNotReady}
	}
	folder := fmt.Sprintf("/wefty/service/seats/%d", r.Place)
	home := folder + "/home"
	return seatCommandConfig{UID: uint32(2000 + r.Place), GID: uint32(2000 + r.Place), Directory: folder, Environment: []string{
		"PATH=/usr/local/bin:/usr/bin:/bin", "LANG=C.UTF-8", "USER=" + e.config.User, "LOGNAME=" + e.config.User,
		"HOME=" + home, "SEAT_ID=" + fmt.Sprint(r.Place), "SEAT_DIR=" + folder, "DISPLAY=:" + fmt.Sprint(r.Place), "XAUTHORITY=" + folder + "/.Xauthority",
		"XDG_RUNTIME_DIR=" + folder + "/run", "TMPDIR=" + folder + "/tmp", "XDG_CACHE_HOME=" + home + "/.cache", "XDG_CONFIG_HOME=" + home + "/.config",
		"PLAYWRIGHT_BROWSERS_PATH=" + home + "/.cache/ms-playwright", "npm_config_cache=" + home + "/.cache/npm", "PIP_CACHE_DIR=" + home + "/.cache/pip",
	}}, nil
}
func (e SeatCommandEvent) validate() error {
	if e.Exit != nil {
		if e.Stream != "" || len(e.Data) != 0 || (e.Exit.Code == nil) == (e.Exit.Signal == 0) || e.Exit.Signal < 0 || e.Exit.Signal > 64 || (e.Exit.Code != nil && (*e.Exit.Code < 0 || *e.Exit.Code > 255)) {
			return errors.New("invalid command exit")
		}
		return nil
	}
	if (e.Stream != "stdout" && e.Stream != "stderr") || len(e.Data) == 0 || len(e.Data) > SeatCommandChunkBytes {
		return errors.New("invalid command output")
	}
	return nil
}

// containerdMaxExecIDLength is containerd's identifier limit for an exec
// process; a longer ID fails Exec with "greater than maximum length".
const containerdMaxExecIDLength = 76

// seatCommandExecID keeps 128 random bits, which is ample for one task's
// concurrent execs and stays under containerd's limit.
func seatCommandExecID(capability string) string {
	return "seat-command-" + capability[:32]
}

// These details distinguish a command's limits from failed runtime mechanics.
const DetailSeatCommandRuntimeLimit ErrorDetail = "runtime_limit"
const DetailSeatCommandStartTimeout ErrorDetail = "start_timeout"

var ErrSeatCommandRuntimeLimit = errors.New("Seat command reached its runtime limit")
var errSeatCommandStartTimeout = errors.New("Seat command start timed out")

const seatCommandFrameFinishTimeout = 5 * time.Second
const seatCommandStartTimeout = 10 * time.Second

// Cleanup failure prevents a limit from claiming that the command stopped.
type seatCommandCleanupError struct{ cause error }

func (e *seatCommandCleanupError) Error() string { return e.cause.Error() }
func (e *seatCommandCleanupError) Unwrap() error { return e.cause }
func seatCommandCleanupUnconfirmed(err error) bool {
	var failure *seatCommandCleanupError
	return errors.As(err, &failure)
}
