//go:build linux

package ocihelper

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	containerd "github.com/containerd/containerd/v2/client"
	"github.com/containerd/errdefs"
	"golang.org/x/sys/unix"
)

func (engine *ContainerdEngine) SetComputerMemoryLimit(ctx context.Context, request SetComputerMemoryLimitRequest) (SetComputerMemoryLimitResponse, error) {
	attempt, err := engine.attempt(request.Authority)
	if err != nil {
		return SetComputerMemoryLimitResponse{}, &SeatOperationRefusalError{Reason: SeatRefusalAttemptNotLive}
	}
	// Serializes with directive publication and positive cleanup observation.
	attempt.seatsMu.Lock()
	defer attempt.seatsMu.Unlock()
	attempt.mu.Lock()
	live := attempt.authority == request.Authority && !attempt.deleted && !attempt.lost && attempt.task != nil && attempt.controlDirectory != "" && attempt.networkNamespace != nil
	attempt.mu.Unlock()
	if !live {
		return SetComputerMemoryLimitResponse{}, &SeatOperationRefusalError{Reason: SeatRefusalAttemptNotLive}
	}
	snapshot, err := snapshotComputerSeatAttempt(ctx, attempt)
	if err != nil {
		attempt.mu.Lock()
		task := attempt.task
		attempt.mu.Unlock()
		if task == nil {
			return SetComputerMemoryLimitResponse{}, &SeatOperationRefusalError{Reason: SeatRefusalAttemptNotLive}
		}
		status, statusErr := task.Status(ctx)
		if errdefs.IsNotFound(err) || (statusErr == nil && status.Status != containerd.Running) {
			return SetComputerMemoryLimitResponse{}, &SeatOperationRefusalError{Reason: SeatRefusalAttemptNotLive}
		}
		return SetComputerMemoryLimitResponse{}, err
	}
	defer snapshot.namespace.close()
	if attempt.seats == nil {
		attempt.seats = newComputerSeats(engine.config.Clock, snapshot.directory)
	}
	validate := func() error { return engine.validateComputerMemoryAttempt(ctx, request.Authority, attempt, snapshot) }
	if err := validate(); err != nil {
		return SetComputerMemoryLimitResponse{}, err
	}
	runtime, err := openComputerMemoryRuntime(engine.config.CgroupRoot, attempt.resources.CgroupID, snapshot, engine.config.Clock)
	if err != nil {
		return SetComputerMemoryLimitResponse{}, err
	}
	defer runtime.directory.Close()
	engine.capacityMu.Lock()
	defer engine.capacityMu.Unlock()
	reservation := engine.capacityReservations[request.Authority.JobID]
	if reservation == nil {
		return SetComputerMemoryLimitResponse{}, errors.New("Computer memory reservation is unavailable")
	}
	if _, ok := reservation.attempts[request.Authority.key()]; !ok {
		return SetComputerMemoryLimitResponse{}, &SeatOperationRefusalError{Reason: SeatRefusalAttemptNotLive}
	}
	available, err := engine.availableComputerMemoryLocked()
	if err != nil {
		return SetComputerMemoryLimitResponse{}, err
	}
	clock := engine.config.Clock
	if clock == nil {
		clock = systemClock{}
	}
	return changeComputerMemoryLimit(ctx, request, attempt.seats, reservation.tmpfsCeilingBytes, &reservation.memoryBytes, available, runtime, clock, validate)
}

type computerCgroupMemory struct {
	directory    *os.File
	clock        Clock
	freezeWrites int
	cgroupID     string
}

func openComputerMemoryRuntime(root, id string, snapshot computerSeatAttemptSnapshot, clock Clock) (*computerCgroupMemory, error) {
	if root == "" {
		root = "/sys/fs/cgroup"
	}
	if id == "" || filepath.IsAbs(id) || filepath.Clean(id) != id || id == ".." || strings.HasPrefix(id, "../") {
		return nil, errors.New("invalid Computer cgroup identity")
	}
	process, err := os.Open(fmt.Sprintf("/proc/%d", snapshot.task.Pid()))
	if err != nil {
		return nil, err
	}
	defer process.Close()
	proc := fmt.Sprintf("/proc/self/fd/%d", process.Fd())
	if err := seatProcessMatchesNetwork(proc, snapshot.namespace); err != nil {
		return nil, err
	}
	membership, err := os.ReadFile(filepath.Join(proc, "cgroup"))
	if err != nil {
		return nil, err
	}
	matched := false
	for _, line := range strings.Split(string(membership), "\n") {
		if line == "0::/"+id {
			matched = true
		}
	}
	if !matched {
		return nil, &SeatOperationRefusalError{Reason: SeatRefusalAttemptNotLive}
	}
	directory, err := os.Open(filepath.Join(root, id))
	if err != nil {
		return nil, err
	}
	var stat unix.Statfs_t
	if err = unix.Fstatfs(int(directory.Fd()), &stat); err != nil || stat.Type != unix.CGROUP2_SUPER_MAGIC {
		directory.Close()
		return nil, errors.New("Computer memory operation requires cgroup v2")
	}
	if clock == nil {
		clock = systemClock{}
	}
	return &computerCgroupMemory{directory: directory, clock: clock, cgroupID: id}, nil
}
func (m *computerCgroupMemory) read(name string) ([]byte, error) {
	fd, err := unix.Openat(int(m.directory.Fd()), name, unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW|unix.O_NONBLOCK, 0)
	if err != nil {
		return nil, err
	}
	file := os.NewFile(uintptr(fd), name)
	defer file.Close()
	return io.ReadAll(io.LimitReader(file, 4096))
}
func (m *computerCgroupMemory) readInt(name string) (int64, error) {
	b, err := m.read(name)
	if err != nil {
		return 0, err
	}
	n, err := strconv.ParseInt(strings.TrimSpace(string(b)), 10, 64)
	if err != nil || n < 0 {
		return 0, fmt.Errorf("%s is not a bounded nonnegative integer", name)
	}
	return n, nil
}
func (m *computerCgroupMemory) readLimit() (int64, error) { return m.readInt("memory.max") }
func (m *computerCgroupMemory) readUsage() (int64, error) { return m.readInt("memory.current") }
func (m *computerCgroupMemory) write(name, value string) error {
	fd, err := unix.Openat(int(m.directory.Fd()), name, unix.O_WRONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW|unix.O_NONBLOCK, 0)
	if err != nil {
		return err
	}
	file := os.NewFile(uintptr(fd), name)
	_, writeErr := file.WriteString(value)
	return errors.Join(writeErr, file.Close())
}
func (m *computerCgroupMemory) writeLimit(n int64) error {
	return m.write("memory.max", strconv.FormatInt(n, 10))
}
func (m *computerCgroupMemory) freezerIO() computerMemoryFreezerIO {
	return computerMemoryFreezerIO{
		readRequested: func() (bool, error) {
			value, err := m.readInt("cgroup.freeze")
			if err == nil && value > 1 {
				return false, errors.New("invalid requested freezer state")
			}
			return value == 1, err
		},
		readEvents: func() ([]byte, error) { return m.read("cgroup.events") },
		write: func(on bool) error {
			value := "0"
			if on {
				value = "1"
				m.freezeWrites++
			}
			return m.write("cgroup.freeze", value)
		},
		log: func(event string) { log.Printf("Computer memory freezer event=%s cgroup_id=%q", event, m.cgroupID) },
	}
}
func (m *computerCgroupMemory) recoverFreeze(ctx context.Context) (bool, error) {
	return recoverComputerMemoryFreeze(ctx, m.clock, m.freezerIO())
}
func (m *computerCgroupMemory) shrink(ctx context.Context, target int64) (computerMemoryShrinkResult, error) {
	return shrinkComputerMemory(ctx, m.clock, m.freezerIO(), m.readUsage, m.readLimit, m.writeLimit, target)
}
func (m *computerCgroupMemory) freezerRequests() int { return m.freezeWrites }

var _ ComputerMemoryLimitEngine = (*ContainerdEngine)(nil)

// Keep runtime read failures distinct from evidence that exact authority ended.
// Like Seat observation, the status read holds neither lifecycle mutex.
func (engine *ContainerdEngine) validateComputerMemoryAttempt(ctx context.Context, authority AttemptAuthority, attempt *containerdAttempt, snapshot computerSeatAttemptSnapshot) error {
	status, err := snapshot.task.Status(ctx)
	if err != nil {
		if errdefs.IsNotFound(err) {
			return &SeatOperationRefusalError{Reason: SeatRefusalAttemptNotLive}
		}
		return err
	}
	if status.Status != containerd.Running {
		return &SeatOperationRefusalError{Reason: SeatRefusalAttemptNotLive}
	}
	current, err := engine.attempt(authority)
	if err != nil || current != attempt {
		return &SeatOperationRefusalError{Reason: SeatRefusalAttemptNotLive}
	}
	attempt.mu.Lock()
	defer attempt.mu.Unlock()
	if attempt.authority != authority || attempt.deleted || attempt.lost || attempt.task != snapshot.task || attempt.networkNamespace != snapshot.sourceNamespace || attempt.controlDirectory != snapshot.directory {
		return &SeatOperationRefusalError{Reason: SeatRefusalAttemptNotLive}
	}
	return nil
}
