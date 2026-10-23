//go:build linux

package ocihelper

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	containerd "github.com/containerd/containerd/v2/client"
	"golang.org/x/sys/unix"
)

func (engine *ContainerdEngine) ApplyComputerSeats(ctx context.Context, request ApplyComputerSeatsRequest) (ApplyComputerSeatsResponse, error) {
	attempt, err := engine.attempt(request.Authority)
	if err != nil {
		return ApplyComputerSeatsResponse{}, err
	}
	attempt.controlMu.Lock()
	defer attempt.controlMu.Unlock()
	attempt.mu.Lock()
	defer attempt.mu.Unlock()
	if err := liveComputerSeatAttempt(ctx, attempt); err != nil {
		return ApplyComputerSeatsResponse{}, err
	}
	if attempt.seats == nil {
		attempt.seats = newComputerSeats(engine.config.Clock, attempt.controlDirectory)
	}
	return attempt.seats.apply(request.Seats)
}
func (engine *ContainerdEngine) ObserveComputerSeats(ctx context.Context, request ObserveComputerSeatsRequest) (ObserveComputerSeatsResponse, error) {
	attempt, err := engine.attempt(request.Authority)
	if err != nil {
		return ObserveComputerSeatsResponse{}, err
	}
	attempt.controlMu.Lock()
	defer attempt.controlMu.Unlock()
	attempt.mu.Lock()
	defer attempt.mu.Unlock()
	if err := liveComputerSeatAttempt(ctx, attempt); err != nil {
		return ObserveComputerSeatsResponse{}, err
	}
	if attempt.seats == nil {
		return ObserveComputerSeatsResponse{}, errors.New("Computer has no Seat directive")
	}
	// Hold the kernel process directory across mountinfo, root and namespace
	// reads. A recycled numeric PID cannot change this anchor underneath us.
	process, err := os.Open(fmt.Sprintf("/proc/%d", attempt.task.Pid()))
	if err != nil {
		return ObserveComputerSeatsResponse{}, err
	}
	defer process.Close()
	proc := fmt.Sprintf("/proc/self/fd/%d", process.Fd())
	if err := seatProcessMatchesNetwork(proc, attempt.networkNamespace); err != nil {
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
	result, err := attempt.seats.observe(ctx, request.Generation, func(name string) ([]byte, error) { return ReadSeatStatusAnswer(directory, name) }, func(ctx context.Context, port uint16) error {
		return probeComputerSeatGreeting(ctx, func(ctx context.Context) (net.Conn, error) {
			return dialTaskLoopback(ctx, attempt.networkNamespace, port)
		})
	}, func(seat ComputerSeat) error { return confirmComputerSeatAbsent(proc, seat) })
	if err != nil {
		return ObserveComputerSeatsResponse{}, err
	}
	if err := liveComputerSeatAttempt(ctx, attempt); err != nil {
		return ObserveComputerSeatsResponse{}, err
	}
	return result, nil
}
func liveComputerSeatAttempt(ctx context.Context, attempt *containerdAttempt) error {
	if attempt.deleted || attempt.lost || attempt.task == nil || attempt.controlDirectory == "" || attempt.networkNamespace == nil {
		return errors.New("Seat operation requires a live Computer attempt")
	}
	status, err := attempt.task.Status(ctx)
	if err != nil {
		return err
	}
	if status.Status != containerd.Running {
		return errors.New("Computer task is not running")
	}
	return nil
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
	pidNamespace, err := os.Stat(filepath.Join(proc, "ns/pid"))
	if err != nil {
		return err
	}
	entries, err := os.ReadDir("/proc")
	if err != nil {
		return err
	}
	uid := strconv.Itoa(2000 + seat.Place)
	for _, entry := range entries {
		if _, err := strconv.Atoi(entry.Name()); err != nil {
			continue
		}
		path := filepath.Join("/proc", entry.Name())
		ns, err := os.Stat(filepath.Join(path, "ns/pid"))
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return err
		}
		if !os.SameFile(pidNamespace, ns) {
			continue
		}
		status, err := os.ReadFile(filepath.Join(path, "status"))
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return err
		}
		for _, line := range strings.Split(string(status), "\n") {
			fields := strings.Fields(line)
			if len(fields) > 1 && fields[0] == "Uid:" {
				for _, value := range fields[1:] {
					if value == uid {
						return errors.New("Seat user still has processes")
					}
				}
			}
		}
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
	prefix := fmt.Sprintf("/wefty/service/seats/%d/", seat.Place)
	for _, line := range strings.Split(string(payload), "\n") {
		fields := strings.Fields(line)
		if len(fields) < 8 {
			continue
		}
		path := fields[7]
		if strings.HasPrefix(path, prefix) || path == fmt.Sprintf("/tmp/.X11-unix/X%d", seat.Place) || path == fmt.Sprintf("@/tmp/.X11-unix/X%d", seat.Place) {
			return errors.New("Seat Unix socket remains")
		}
	}
	return confirmSeatFolderAbsent(filepath.Join(proc, "root"), seat.Place)
}
func checkSeatInetSockets(payload []byte, seat ComputerSeat) error {
	scanner := bufio.NewScanner(strings.NewReader(string(payload)))
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) < 10 || fields[0] == "sl" {
			continue
		}
		// TIME_WAIT entries with inode 0 are not live sockets and cannot accept
		// input; requiring their disappearance would add the TCP close timeout.
		if fields[9] == "0" {
			continue
		}
		if fields[7] == strconv.Itoa(2000+seat.Place) {
			return errors.New("Seat user still owns a socket")
		}
		_, portText, ok := strings.Cut(fields[1], ":")
		if !ok {
			return errors.New("invalid kernel socket table")
		}
		port, err := strconv.ParseUint(portText, 16, 16)
		if err != nil {
			return err
		}
		if uint16(port) == seat.ViewPort || uint16(port) == seat.ControlPort {
			return errors.New("Seat endpoint socket remains")
		}
	}
	return scanner.Err()
}
func confirmSeatFolderAbsent(root string, place int) error {
	anchor, err := os.Open(root)
	if err != nil {
		return err
	}
	defer anchor.Close()
	fd := int(anchor.Fd())
	owned := -1
	defer func() {
		if owned >= 0 {
			unix.Close(owned)
		}
	}()
	for _, component := range []string{"wefty", "service", "seats"} {
		next, err := unix.Openat(fd, component, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
		if err != nil {
			return err
		}
		if owned >= 0 {
			unix.Close(owned)
		}
		fd, owned = next, next
	}
	var st unix.Stat_t
	err = unix.Fstatat(fd, strconv.Itoa(place), &st, unix.AT_SYMLINK_NOFOLLOW)
	if errors.Is(err, unix.ENOENT) {
		return nil
	}
	if err != nil {
		return err
	}
	return errors.New("Seat folder remains")
}

var _ ComputerSeatsEngine = (*ContainerdEngine)(nil)
