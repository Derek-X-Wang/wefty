package local

import (
	"context"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/Derek-X-Wang/wefty/fabric"
)

// Participant is a backend already owned by this service. Check must verify
// saved identity before each local check/dial. A failed optional person check
// does not prevent client operations.
type Participant struct {
	NodeID string
	Fabric fabric.Fabric
	Check  func(context.Context) error
}

type Server struct {
	path             string
	participants     map[string]Participant
	peerUID          func(net.Conn) (uint32, error)
	uid              uint32
	slots            chan struct{}
	busySlots        chan struct{}
	handshakeTimeout time.Duration
	listen           func(string, string) (net.Listener, error)
}

func NewServer(path string, participants map[string]Participant) (*Server, error) {
	if err := validateSocketPath(path); err != nil {
		return nil, err
	}
	if _, ok := participants[Client]; !ok {
		return nil, errors.New("client service needs a client participant")
	}
	copyParticipants := make(map[string]Participant, len(participants))
	for role, p := range participants {
		if role != Client && role != Person || p.NodeID == "" || p.Check == nil {
			return nil, errors.New("invalid client-service participant")
		}
		copyParticipants[role] = p
	}
	return &Server{path: path, participants: copyParticipants, peerUID: unixPeerUID, uid: uint32(os.Geteuid()), slots: make(chan struct{}, 128), busySlots: make(chan struct{}, 4), handshakeTimeout: handshakeBudget, listen: net.Listen}, nil
}
func (s *Server) Serve(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	if err := privateDirectory(filepath.Dir(s.path)); err != nil {
		return err
	}
	// A distinct lock serializes socket publication, including services whose
	// profiles refer to different participant folders. It survives stale sockets.
	release, err := acquireSocketLock(s.path)
	if err != nil {
		return err
	}
	defer release()
	if info, err := os.Lstat(s.path); err == nil {
		if info.Mode()&os.ModeSocket == 0 {
			return errors.New("refusing to replace a non-socket client-service endpoint")
		}
		conn, dialErr := net.DialTimeout("unix", s.path, 250*time.Millisecond)
		if dialErr == nil {
			_ = conn.Close()
			return &AlreadyRunningError{}
		}
		// Only a refused dial proves this socket is stale.
		if !socketRefused(dialErr) {
			return errors.New("cannot confirm stale client-service endpoint")
		}
		if err := os.Remove(s.path); err != nil {
			return err
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	listener, err := s.listen("unix", s.path)
	if err != nil {
		return err
	}
	defer listener.Close()
	defer os.Remove(s.path)
	if err := os.Chmod(s.path, 0o600); err != nil {
		return err
	}
	// The parent is already private before bind, so the socket is never
	// accessible to a different user, including between bind and chmod.
	stop := context.AfterFunc(ctx, func() { _ = listener.Close() })
	defer stop()
	var workers sync.WaitGroup
	var retryDelay time.Duration
	defer func() { cancel(); workers.Wait() }()
	for {
		conn, err := listener.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			var temporary interface{ Temporary() bool }
			if errors.Is(err, syscall.EMFILE) || errors.Is(err, syscall.ENFILE) || errors.As(err, &temporary) && temporary.Temporary() {
				if retryDelay == 0 {
					retryDelay = 5 * time.Millisecond
				} else {
					retryDelay = min(retryDelay*2, time.Second)
				}
				timer := time.NewTimer(retryDelay)
				select {
				case <-ctx.Done():
					timer.Stop()
					return nil
				case <-timer.C:
					continue
				}
			}
			return err
		}
		retryDelay = 0
		uid, err := s.peerUID(conn)
		if err != nil || uid != s.uid {
			_ = conn.Close()
			continue
		}
		select {
		case s.slots <- struct{}{}:
		default:
			select {
			case s.busySlots <- struct{}{}:
				workers.Add(1)
				go func() {
					defer workers.Done()
					defer func() { <-s.busySlots }()
					s.rejectBusy(ctx, conn)
				}()
			default:
				_ = conn.Close()
			}
			continue
		}
		workers.Add(1)
		go func() { defer workers.Done(); defer func() { <-s.slots }(); s.handle(ctx, conn) }()
	}
}
func privateDirectory(path string) error {
	if err := os.MkdirAll(path, 0o700); err != nil {
		return err
	}
	info, err := os.Lstat(path)
	if err != nil || !info.IsDir() {
		return errors.New("client-service socket folder must be an unlinked directory")
	}
	if !ownedByUser(info, uint32(os.Geteuid())) {
		return errors.New("client-service socket folder must belong to this user")
	}
	if info.Mode().Perm() != 0o700 {
		return errors.New("client-service socket folder must have mode 0700; choose a private folder")
	}
	return nil
}
func (s *Server) handle(serviceCtx context.Context, conn net.Conn) {
	defer conn.Close()
	stop := context.AfterFunc(serviceCtx, func() { _ = conn.Close() })
	defer stop()
	_ = conn.SetReadDeadline(time.Now().Add(s.handshakeTimeout))
	var req request
	if err := readFrame(conn, &req); err != nil {
		// A caller that left or stayed silent gets no reply. Any other decode
		// failure answers, so a client of a different version is not told the
		// service is busy when it closes without reply bytes.
		var timeout net.Error
		if !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) && !(errors.As(err, &timeout) && timeout.Timeout()) {
			_ = s.writeReply(conn, response{Code: "invalid_request"})
		}
		return
	}
	if req.Version != 1 || req.TimeoutMillis <= 0 || req.TimeoutMillis > dialBudget.Milliseconds() ||
		(req.Operation != "dial" && req.Operation != "check") ||
		req.Operation == "dial" && !validDestination(req.Network, req.Address) ||
		req.Operation == "check" && (req.Network != "" || req.Address != "") {
		_ = s.writeReply(conn, response{Code: "invalid_request"})
		return
	}
	_ = conn.SetReadDeadline(time.Time{})
	p, ok := s.participants[req.Participant]
	if !ok || p.NodeID != req.NodeID {
		code := "machine_identity_required"
		if req.Participant == Person {
			code = "person_identity_required"
		}
		_ = s.writeReply(conn, response{Code: code, Message: "is missing or differs from the client service profile; restart the client service with this profile"})
		return
	}
	ctx, cancel := context.WithTimeout(serviceCtx, time.Duration(req.TimeoutMillis)*time.Millisecond)
	defer cancel()
	// Read concurrently so closing a caller's socket cancels a blocked identity
	// check or dial immediately. The pipe applies backpressure to early data.
	reader, writer := io.Pipe()
	defer reader.Close()
	var establishing atomic.Bool
	establishing.Store(true)
	go func() {
		_, err := io.Copy(writer, conn)
		if establishing.Load() {
			cancel()
		}
		_ = writer.CloseWithError(err)
	}()
	err := p.Check(ctx)
	var upstream net.Conn
	if err == nil && req.Operation == "dial" {
		if p.Fabric == nil {
			err = errors.New("participant backend unavailable")
		} else {
			upstream, err = p.Fabric.Dial(ctx, req.Network, req.Address)
		}
	}
	if err != nil {
		reply := response{Code: "fabric_unreachable"}
		var refused *fabric.DestinationRefusedError
		if errors.As(err, &refused) {
			reply.Code = "fabric_destination_refused"
		}
		var identity *fabric.ParticipantIdentityError
		if errors.As(err, &identity) {
			reply.Code = "machine_identity_required"
			if identity.Person {
				reply.Code = "person_identity_required"
			}
			reply.Message = identity.Reason
		}
		_ = s.writeReply(conn, reply)
		return
	}
	if upstream != nil {
		defer upstream.Close()
	}
	if err := s.writeReply(conn, response{}); err != nil {
		return
	}
	if req.Operation == "check" {
		return
	}
	establishing.Store(false)
	_ = conn.SetWriteDeadline(time.Time{})
	// Establishment has a ceiling; streams have no total lifetime limit. A
	// service stop or a caller close releases both directions and the slot.
	closeUpstream := context.AfterFunc(serviceCtx, func() { _ = upstream.Close() })
	defer closeUpstream()
	copied := make(chan struct{})
	go func() { defer close(copied); _, _ = io.Copy(upstream, reader); _ = upstream.Close() }()
	_, _ = io.Copy(conn, upstream)
	_ = conn.Close()
	_ = reader.Close()
	_ = upstream.Close()
	<-copied
}

func (s *Server) writeReply(conn net.Conn, reply response) error {
	_ = conn.SetWriteDeadline(time.Now().Add(s.handshakeTimeout))
	return writeFrame(conn, reply)
}

// A bounded worker handles overflow outside the accept loop. Consume the
// request so closing the socket cannot discard
// the busy reply while the client is still sending its small frame.
func (s *Server) rejectBusy(ctx context.Context, conn net.Conn) {
	defer conn.Close()
	stop := context.AfterFunc(ctx, func() { _ = conn.Close() })
	defer stop()
	_ = conn.SetReadDeadline(time.Now().Add(s.handshakeTimeout))
	var req request
	if err := readFrame(conn, &req); err != nil {
		return
	}
	_ = s.writeReply(conn, response{Code: "client_service_busy"})
}
