//go:build linux

package ocihelper

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"

	containerd "github.com/containerd/containerd/v2/client"
)

// A snapshot owns a duplicate namespace descriptor. Task I/O and observation
// work never hold the attempt's lifecycle or owner-control mutexes.
type computerSeatAttemptSnapshot struct {
	task            containerd.Task
	namespace       *pinnedNetworkNamespace
	sourceNamespace *pinnedNetworkNamespace
	directory       string
	seats           *computerSeats
}

func (engine *ContainerdEngine) ApplyComputerSeats(ctx context.Context, request ApplyComputerSeatsRequest) (ApplyComputerSeatsResponse, error) {
	attempt, err := engine.attempt(request.Authority)
	if err != nil {
		return ApplyComputerSeatsResponse{}, err
	}
	attempt.seatsMu.Lock()
	defer attempt.seatsMu.Unlock()
	snapshot, err := snapshotComputerSeatAttempt(ctx, attempt)
	if err != nil {
		return ApplyComputerSeatsResponse{}, err
	}
	defer snapshot.namespace.close()
	if attempt.seats == nil {
		attempt.seats = newComputerSeats(engine.config.Clock, snapshot.directory)
	}
	response, err := attempt.seats.apply(request.Seats)
	if err != nil {
		return ApplyComputerSeatsResponse{}, err
	}
	if err := engine.validateComputerSeatAttempt(ctx, request.Authority, attempt, snapshot); err != nil {
		return ApplyComputerSeatsResponse{}, err
	}
	return response, nil
}
func (engine *ContainerdEngine) ObserveComputerSeats(ctx context.Context, request ObserveComputerSeatsRequest) (ObserveComputerSeatsResponse, error) {
	return engine.observeComputerSeats(ctx, request, observeComputerSeatFiles)
}
func (engine *ContainerdEngine) observeComputerSeats(ctx context.Context, request ObserveComputerSeatsRequest, observe func(context.Context, computerSeatAttemptSnapshot, ObserveComputerSeatsRequest) (ObserveComputerSeatsResponse, error)) (ObserveComputerSeatsResponse, error) {
	attempt, err := engine.attempt(request.Authority)
	if err != nil {
		return ObserveComputerSeatsResponse{}, err
	}
	attempt.seatsMu.Lock()
	defer attempt.seatsMu.Unlock()
	snapshot, err := snapshotComputerSeatAttempt(ctx, attempt)
	if err != nil {
		return ObserveComputerSeatsResponse{}, err
	}
	defer snapshot.namespace.close()
	if snapshot.seats == nil {
		return ObserveComputerSeatsResponse{}, errors.New("Computer has no Seat directive")
	}
	result, err := observe(ctx, snapshot, request)
	if err != nil {
		return ObserveComputerSeatsResponse{}, err
	}
	if err := engine.validateComputerSeatAttempt(ctx, request.Authority, attempt, snapshot); err != nil {
		return ObserveComputerSeatsResponse{}, err
	}
	return result, nil
}
func snapshotComputerSeatAttempt(ctx context.Context, attempt *containerdAttempt) (computerSeatAttemptSnapshot, error) {
	attempt.mu.Lock()
	live := !attempt.deleted && !attempt.lost && attempt.task != nil && attempt.controlDirectory != "" && attempt.networkNamespace != nil
	snapshot := computerSeatAttemptSnapshot{task: attempt.task, sourceNamespace: attempt.networkNamespace, directory: attempt.controlDirectory, seats: attempt.seats}
	attempt.mu.Unlock()
	if !live {
		return computerSeatAttemptSnapshot{}, errors.New("Seat operation requires a live Computer attempt")
	}
	status, err := snapshot.task.Status(ctx)
	if err != nil {
		return computerSeatAttemptSnapshot{}, err
	}
	if status.Status != containerd.Running {
		return computerSeatAttemptSnapshot{}, errors.New("Computer task is not running")
	}
	file, err := snapshot.sourceNamespace.duplicate()
	if err != nil {
		return computerSeatAttemptSnapshot{}, err
	}
	snapshot.namespace = &pinnedNetworkNamespace{file: file}
	return snapshot, nil
}
func (engine *ContainerdEngine) validateComputerSeatAttempt(ctx context.Context, authority AttemptAuthority, attempt *containerdAttempt, snapshot computerSeatAttemptSnapshot) error {
	status, err := snapshot.task.Status(ctx)
	if err != nil {
		return err
	}
	if status.Status != containerd.Running {
		return errors.New("Computer task is not running")
	}
	current, err := engine.attempt(authority)
	if err != nil {
		return err
	}
	if current != attempt {
		return errors.New("Computer attempt was replaced")
	}
	attempt.mu.Lock()
	defer attempt.mu.Unlock()
	if attempt.authority != authority || attempt.deleted || attempt.lost || attempt.networkNamespace != snapshot.sourceNamespace || attempt.controlDirectory != snapshot.directory {
		return errors.New("Computer attempt lost its Seat authority")
	}
	return nil
}
func observeComputerSeatFiles(ctx context.Context, snapshot computerSeatAttemptSnapshot, request ObserveComputerSeatsRequest) (ObserveComputerSeatsResponse, error) {
	// Hold the kernel process directory across mountinfo, root and namespace
	// reads. A recycled numeric PID cannot change this anchor underneath us.
	process, err := os.Open(fmt.Sprintf("/proc/%d", snapshot.task.Pid()))
	if err != nil {
		return ObserveComputerSeatsResponse{}, err
	}
	defer process.Close()
	proc := fmt.Sprintf("/proc/self/fd/%d", process.Fd())
	if err := seatProcessMatchesNetwork(proc, snapshot.namespace); err != nil {
		return ObserveComputerSeatsResponse{}, err
	}
	mountinfo, err := os.ReadFile(filepath.Join(proc, "mountinfo"))
	if err != nil {
		return ObserveComputerSeatsResponse{}, err
	}
	directory, err := OpenValidatedSeatStatusDirectory(filepath.Join(proc, "root"), mountinfo)
	if err != nil {
		return ObserveComputerSeatsResponse{}, err
	}
	defer directory.Close()
	// Resolve the directive's internal port metadata to fixed Seat paths. These
	// numbers remain for compatibility with ApplyComputerSeats; no TCP dial occurs.
	pins := map[int]map[string]*os.File{}
	defer func() {
		for _, p := range pins {
			for _, f := range p {
				f.Close()
			}
		}
	}()
	result, err := snapshot.seats.observe(ctx, request.Generation, func(name string) ([]byte, error) { return ReadSeatStatusAnswer(directory, name) }, func(ctx context.Context, port uint16) error {
		for place, entry := range snapshot.seats.entries {
			for _, kind := range []string{"view", "control"} {
				matches := kind == "view" && entry.config.ViewPort == port || kind == "control" && entry.config.ControlPort == port
				if !entry.wanted || !matches {
					continue
				}
				pin, err := openSeatEndpoint(proc+"/root", place, kind, uint32(2000+place))
				if err != nil {
					return err
				}
				err = probeComputerSeatGreeting(ctx, func(ctx context.Context) (net.Conn, error) {
					return dialSeatSocket(ctx, proc+"/root", place, kind, uint32(2000+place), pin)
				})
				if err != nil {
					pin.Close()
					return err
				}
				if pins[place] == nil {
					pins[place] = map[string]*os.File{}
				}
				pins[place][kind] = pin
				return nil
			}
		}
		return errors.New("unknown Seat endpoint")
	}, func(seat ComputerSeat) error { return confirmComputerSeatAbsent(proc, seat) })
	if err != nil {
		return result, err
	}
	for i := range result.Seats {
		observation := &result.Seats[i]
		entry := snapshot.seats.entries[observation.Place]
		if observation.State != "ready" {
			entry.closeEndpointPins()
			continue
		}
		fresh := pins[observation.Place]
		if len(fresh) != 2 {
			return ObserveComputerSeatsResponse{}, errors.New("ready Seat lacks pinned endpoints")
		}
		if !sameSeatSocket(entry.endpointPins["view"], fresh["view"]) || !sameSeatSocket(entry.endpointPins["control"], fresh["control"]) {
			entry.endpointRevision++
		}
		entry.closeEndpointPins()
		entry.endpointPins = fresh
		delete(pins, observation.Place)
		observation.Endpoints = map[string]string{"view": entry.endpointName("view"), "control": entry.endpointName("control")}
	}
	return result, nil
}
func seatProcessMatchesNetwork(proc string, namespace *pinnedNetworkNamespace) error {
	pinned, err := namespace.duplicate()
	if err != nil {
		return err
	}
	defer pinned.Close()
	expected, err := pinned.Stat()
	if err != nil {
		return err
	}
	actual, err := os.Stat(filepath.Join(proc, "ns/net"))
	if err != nil {
		return err
	}
	if !os.SameFile(expected, actual) {
		return errors.New("Computer process no longer owns its pinned network namespace")
	}
	return nil
}

// These are host-side observations. They do not run image code or accept an
// image's statement as process/socket/folder absence. The Computer uses no
// user namespace, so its fixed Seat uid is also the host-visible uid.
func confirmComputerSeatAbsent(proc string, seat ComputerSeat) error {
	if err := confirmComputerSeatProcessesAbsent("/proc", filepath.Join(proc, "ns/pid"), seat); err != nil {
		return err
	}
	for _, table := range []string{"tcp", "tcp6", "udp", "udp6"} {
		payload, err := os.ReadFile(filepath.Join(proc, "net", table))
		if err != nil {
			return err
		}
		if err := checkSeatInetSockets(payload, seat); err != nil {
			return err
		}
	}
	payload, err := os.ReadFile(filepath.Join(proc, "net/unix"))
	if err != nil {
		return err
	}
	if err := checkSeatUnixSockets(payload, seat); err != nil {
		return err
	}
	return confirmSeatFolderAbsent(filepath.Join(proc, "root"), seat.Place)
}

var _ ComputerSeatsEngine = (*ContainerdEngine)(nil)
