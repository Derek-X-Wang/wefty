//go:build linux

package ocihelper

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"

	"golang.org/x/sys/unix"
)

// The sole followed link is the trusted kernel task-root anchor. Every image
// component below it uses openat + no-follow. O_PATH pins the socket inode;
// connect through /proc/self/fd never resolves a mutable image pathname.
func openSeatEndpoint(root string, place int, kind string, uid uint32) (*os.File, error) {
	if place < 1 || place > 4 || (kind != "view" && kind != "control") {
		return nil, errors.New("invalid Seat endpoint")
	}
	current, err := os.Open(root)
	if err != nil {
		return nil, err
	}
	defer func() { current.Close() }()
	for i, component := range []string{"wefty", "service", "seats", fmt.Sprint(place), "run", kind + ".ws"} {
		flags := unix.O_PATH | unix.O_NOFOLLOW | unix.O_CLOEXEC
		if i < 5 {
			flags |= unix.O_DIRECTORY
		}
		fd, err := unix.Openat(int(current.Fd()), component, flags, 0)
		if err != nil {
			return nil, err
		}
		next := os.NewFile(uintptr(fd), component)
		current.Close()
		current = next
		var st unix.Stat_t
		if err := unix.Fstat(fd, &st); err != nil {
			return nil, err
		}
		if i >= 3 && (st.Uid != uid || st.Mode&0077 != 0) {
			return nil, errors.New("Seat endpoint is not private to its uid")
		}
		if i == 5 && st.Mode&unix.S_IFMT != unix.S_IFSOCK {
			return nil, errors.New("Seat endpoint is not a filesystem socket")
		}
	}
	fd, err := unix.FcntlInt(current.Fd(), unix.F_DUPFD_CLOEXEC, 0)
	if err != nil {
		return nil, err
	}
	return os.NewFile(uintptr(fd), kind), nil
}
func sameSeatSocket(a, b *os.File) bool {
	if a == nil || b == nil {
		return false
	}
	ai, ae := a.Stat()
	bi, be := b.Stat()
	return ae == nil && be == nil && os.SameFile(ai, bi)
}

// A Unix connect must not wait for a full listener backlog while retaining an
// ext4 inode. SOCK_NONBLOCK refuses a busy endpoint immediately. Control keeps
// the descriptor number from being reused until the one connect syscall ends;
// retirement may close it concurrently without redirecting the connection.
func connectSeatSocket(ctx context.Context, pin *os.File) (net.Conn, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	fd, err := unix.Socket(unix.AF_UNIX, unix.SOCK_STREAM|unix.SOCK_NONBLOCK|unix.SOCK_CLOEXEC, 0)
	if err != nil {
		return nil, err
	}
	socket := os.NewFile(uintptr(fd), "Seat endpoint connection")
	defer socket.Close()
	raw, err := pin.SyscallConn()
	if err != nil {
		return nil, err
	}
	var connectErr error
	if err := raw.Control(func(pinned uintptr) {
		connectErr = unix.Connect(fd, &unix.SockaddrUnix{Name: fmt.Sprintf("/proc/self/fd/%d", pinned)})
	}); err != nil {
		return nil, err
	}
	if connectErr != nil {
		return nil, connectErr
	}
	return net.FileConn(socket)
}

func dialSeatSocket(ctx context.Context, root string, place int, kind string, uid uint32, expected *os.File) (net.Conn, error) {
	return dialSeatSocketWith(ctx, root, place, kind, uid, expected, connectSeatSocket)
}

// The standalone wrapper is used by socket unit tests. Real attempt operations
// share the attempt's registry so retirement also closes their provisional pins.
func dialSeatSocketWith(ctx context.Context, root string, place int, kind string, uid uint32, expected *os.File, connect func(context.Context, *os.File) (net.Conn, error)) (net.Conn, error) {
	seats := newComputerSeats(nil, "")
	defer seats.closeEndpointPins()
	return seats.dialSeatSocket(ctx, root, place, kind, uid, expected, connect)
}

func (s *computerSeats) dialSeatSocket(ctx context.Context, root string, place int, kind string, uid uint32, expected *os.File, connect func(context.Context, *os.File) (net.Conn, error)) (net.Conn, error) {
	pin, err := s.openEndpointPin(func() (*os.File, error) { return openSeatEndpoint(root, place, kind, uid) })
	if err != nil {
		return nil, err
	}
	defer s.closeEndpointPin(pin)
	if !sameSeatSocket(pin, expected) {
		return nil, errors.New("Seat endpoint was replaced")
	}
	c, err := connect(ctx, pin)
	if err != nil {
		return nil, err
	}
	fail := func(err error) (net.Conn, error) { c.Close(); return nil, err }
	raw, err := c.(*net.UnixConn).SyscallConn()
	if err != nil {
		return fail(err)
	}
	var peer *unix.Ucred
	var peerErr error
	if err = raw.Control(func(fd uintptr) { peer, peerErr = unix.GetsockoptUcred(int(fd), unix.SOL_SOCKET, unix.SO_PEERCRED) }); err != nil {
		return fail(err)
	}
	if peerErr != nil || peer == nil || peer.Uid != uid {
		return fail(errors.New("Seat endpoint peer uid mismatch"))
	}
	// Refuse a rename/unlink during connect even though the descriptor kept the
	// connection on the original inode. No replacement ever receives the dial.
	after, err := s.openEndpointPin(func() (*os.File, error) { return openSeatEndpoint(root, place, kind, uid) })
	if err != nil {
		return fail(err)
	}
	defer s.closeEndpointPin(after)
	if !sameSeatSocket(pin, after) {
		return fail(errors.New("Seat endpoint changed during connect"))
	}
	return c, nil
}

func (engine *ContainerdEngine) DialSeatEndpoint(ctx context.Context, r DialSeatEndpointRequest, stream io.ReadWriteCloser) error {
	attempt, err := engine.attempt(r.Authority)
	if err != nil {
		return err
	}
	attempt.seatsMu.Lock()
	snapshot, err := snapshotComputerSeatAttempt(ctx, attempt)
	if err != nil {
		attempt.seatsMu.Unlock()
		return err
	}
	defer snapshot.namespace.close()
	if snapshot.seats == nil {
		attempt.seatsMu.Unlock()
		return &SeatOperationRefusalError{Reason: SeatRefusalEndpointUnavailable}
	}
	kind, err := snapshot.seats.endpoint(r)
	if err != nil {
		attempt.seatsMu.Unlock()
		return err
	}
	if snapshot.seats.entries[r.Place].endpointPins[kind] == nil {
		attempt.seatsMu.Unlock()
		return &SeatOperationRefusalError{Reason: SeatRefusalEndpointUnavailable}
	}
	process, err := os.Open(fmt.Sprintf("/proc/%d", snapshot.task.Pid()))
	if err != nil {
		attempt.seatsMu.Unlock()
		return err
	}
	defer process.Close()
	proc := fmt.Sprintf("/proc/self/fd/%d", process.Fd())
	if err = seatProcessMatchesNetwork(proc, snapshot.namespace); err != nil {
		attempt.seatsMu.Unlock()
		return err
	}
	backend, err := snapshot.seats.dialSeatSocket(ctx, proc+"/root", r.Place, kind, uint32(2000+r.Place), snapshot.seats.entries[r.Place].endpointPins[kind], connectSeatSocket)
	if err == nil {
		err = engine.validateComputerSeatAttempt(ctx, r.Authority, attempt, snapshot)
	}
	attempt.seatsMu.Unlock()
	if err != nil {
		if backend != nil {
			backend.Close()
		}
		reason := SeatRefusalEndpointChanged
		if errors.Is(err, os.ErrNotExist) || errors.Is(err, unix.ECONNREFUSED) || errors.Is(err, unix.EAGAIN) || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			reason = SeatRefusalEndpointUnavailable
		}
		return &SeatOperationRefusalError{Reason: reason}
	}
	defer backend.Close()
	if _, err := stream.Write([]byte{attemptPortBackendReady}); err != nil {
		return err
	}
	return Relay(ctx, stream, backend)
}

var _ ComputerSeatEndpointEngine = (*ContainerdEngine)(nil)
