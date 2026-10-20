// Package local carries outbound Fabric connections through the per-user client
// service. It never owns saved identity state or starts an embedded backend.
package local

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/Derek-X-Wang/wefty/fabric"
)

const (
	Client            = "client"
	Person            = "person"
	SocketEnvironment = "WEFTY_CLIENT_SOCKET"
	maximumFrame      = 4096
	handshakeBudget   = 5 * time.Second
	dialBudget        = 30 * time.Second
)

// SocketPath returns the per-user default, or the explicit service endpoint.
func SocketPath(home string) string {
	if path := os.Getenv(SocketEnvironment); path != "" {
		return path
	}
	return filepath.Join(home, ".config", "wefty", "client", "service.sock")
}

// UnavailableError means no usable client-service exchange could be made.
// Its public text never includes a backend diagnostic or another user's path.
type UnavailableError struct{ Cause error }

func (e *UnavailableError) Error() string {
	return "Fabric client service is unavailable; ask the owner to start or repair wefty client-service with this profile before retrying"
}
func (e *UnavailableError) Unwrap() error { return e.Cause }

type request struct {
	Version       int    `json:"version"`
	Operation     string `json:"operation"`
	Participant   string `json:"participant"`
	NodeID        string `json:"node_id"`
	Network       string `json:"network,omitempty"`
	Address       string `json:"address,omitempty"`
	TimeoutMillis int64  `json:"timeout_ms"`
}
type response struct {
	Code    string `json:"code,omitempty"`
	Message string `json:"message,omitempty"`
}

// Dialer implements the outbound half of Fabric. The receiving L1/L3 still
// authenticates the service's actual participant, never the local socket peer.
type Dialer struct {
	path, participant, nodeID string
	peerUID                   func(net.Conn) (uint32, error)
	dialSocket                func(context.Context, string, string) (net.Conn, error)
}

func New(path, participant, nodeID string) (*Dialer, error) {
	if err := validateSocketPath(path); err != nil {
		return nil, err
	}
	if participant != Client && participant != Person || nodeID == "" || len(nodeID) > 512 {
		return nil, errors.New("invalid client-service participant selection")
	}
	return &Dialer{path: path, participant: participant, nodeID: nodeID, peerUID: unixPeerUID, dialSocket: (&net.Dialer{}).DialContext}, nil
}
func (*Dialer) Listen(string, string) (net.Listener, error) {
	return nil, errors.New("local Fabric supports outbound dialing only")
}
func (*Dialer) WhoIs(context.Context, string) (fabric.Identity, error) {
	return fabric.Identity{}, fabric.ErrIdentityNotFound
}
func (*Dialer) ConnectHost() string { return "" }
func (d *Dialer) Check(ctx context.Context) error {
	conn, err := d.exchange(ctx, "check", "", "")
	if conn != nil {
		_ = conn.Close()
	}
	return err
}
func (d *Dialer) Dial(ctx context.Context, network, address string) (net.Conn, error) {
	if !validDestination(network, address) {
		return nil, errors.New("client service requires a bounded TCP Fabric address")
	}
	return d.exchange(ctx, "dial", network, address)
}
func (d *Dialer) exchange(ctx context.Context, operation, network, address string) (net.Conn, error) {
	ctx, cancel := context.WithTimeout(ctx, dialBudget)
	defer cancel()
	if err := ctx.Err(); err != nil {
		return nil, establishmentError(ctx, err)
	}
	conn, err := d.dialSocket(ctx, "unix", d.path)
	if err != nil {
		return nil, establishmentError(ctx, err)
	}
	stop := context.AfterFunc(ctx, func() { _ = conn.Close() })
	defer stop()
	deadline, _ := ctx.Deadline()
	_ = conn.SetDeadline(deadline)
	uid, peerErr := d.peerUID(conn)
	if err := ctx.Err(); err != nil {
		_ = conn.Close()
		return nil, establishmentError(ctx, err)
	}
	if peerErr != nil || uid != uint32(os.Geteuid()) {
		_ = conn.Close()
		return nil, &PeerIdentityError{Cause: peerErr}
	}
	req := request{Version: 1, Operation: operation, Participant: d.participant, NodeID: d.nodeID, Network: network, Address: address, TimeoutMillis: max(1, time.Until(deadline).Milliseconds())}
	err = writeFrame(conn, req)
	var reply response
	received := &replyReader{Reader: conn}
	if err == nil {
		err = decodeFrame(received, &reply, false)
	}
	if err != nil {
		_ = conn.Close()
		// An authenticated live service can close overflow without a reply.
		// A partial reply is a protocol failure, not evidence of saturation.
		if ctx.Err() == nil && received.bytes == 0 && (errors.Is(err, io.EOF) || errors.Is(err, syscall.EPIPE) || errors.Is(err, syscall.ECONNRESET)) {
			return nil, &BusyError{}
		}
		return nil, establishmentError(ctx, err)
	}
	if reply.Code != "" {
		_ = conn.Close()
		switch reply.Code {
		case "person_identity_required":
			return nil, &fabric.ParticipantIdentityError{Reason: reply.Message, Person: true}
		case "machine_identity_required":
			return nil, &fabric.ParticipantIdentityError{Reason: reply.Message}
		case "client_service_busy":
			return nil, &BusyError{}
		case "fabric_destination_refused":
			return nil, &fabric.DestinationRefusedError{}
		case "invalid_request":
			// Our own client only sends valid requests, so the running service
			// speaks a different protocol version and needs the owner's repair.
			return nil, &UnavailableError{Cause: errors.New("client service refused the request format")}
		case "fabric_unreachable":
			return nil, &fabric.ParticipantUnreachableError{Cause: errors.New("client service could not reach or confirm the participant or destination")}
		default:
			return nil, errors.New("client service refused the outbound request")
		}
	}
	// Stop cancellation before handing the stream to net/http, which cancels
	// its establishment context as soon as Dial returns. Requests own stream
	// cancellation through net.Conn.Close and deadlines from this point onward.
	if !stop() {
		_ = conn.Close()
		return nil, establishmentError(ctx, ctx.Err())
	}
	_ = conn.SetDeadline(time.Time{})
	return &streamConn{Conn: conn}, nil
}

type replyReader struct {
	io.Reader
	bytes int
}

func (r *replyReader) Read(p []byte) (int, error) {
	n, err := r.Reader.Read(p)
	r.bytes += n
	return n, err
}

func validDestination(network, address string) bool {
	return network == "tcp" && len(address) > 0 && len(address) <= 512 && strings.IndexFunc(address, func(r rune) bool { return r <= 32 || r == 127 }) < 0
}
func writeFrame(w io.Writer, value any) error {
	payload, err := json.Marshal(value)
	if err != nil {
		return err
	}
	if len(payload) > maximumFrame {
		return errors.New("client-service frame exceeds limit")
	}
	var size [4]byte
	binary.BigEndian.PutUint32(size[:], uint32(len(payload)))
	_, err = io.Copy(w, io.MultiReader(bytes.NewReader(size[:]), bytes.NewReader(payload)))
	return err
}
func readFrame(r io.Reader, value any) error { return decodeFrame(r, value, true) }
func decodeFrame(r io.Reader, value any, strict bool) error {
	var size [4]byte
	if _, err := io.ReadFull(r, size[:]); err != nil {
		return err
	}
	length := binary.BigEndian.Uint32(size[:])
	if length == 0 || length > maximumFrame {
		return errors.New("invalid client-service frame size")
	}
	payload := make([]byte, int(length))
	if _, err := io.ReadFull(r, payload); err != nil {
		return err
	}
	decoder := json.NewDecoder(bytes.NewReader(payload))
	if strict {
		decoder.DisallowUnknownFields()
	}
	if err := decoder.Decode(value); err != nil {
		return err
	}
	if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
		return errors.New("client-service frame must contain one JSON value")
	}
	return nil
}
func validateSocketPath(path string) error {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path || len(path) > 100 {
		return fmt.Errorf("client-service socket needs an absolute path of at most 100 bytes; choose a shorter private folder")
	}
	return nil
}

var _ fabric.Fabric = (*Dialer)(nil)

// A tunnel has net.Conn's full-close semantics; it exposes no half-close API.
type streamConn struct{ net.Conn }

func establishmentError(ctx context.Context, cause error) error {
	if errors.Is(ctx.Err(), context.Canceled) {
		return ctx.Err()
	}
	if errors.Is(cause, syscall.EPERM) {
		return &FencedCallerError{Cause: cause}
	}
	var timeout net.Error
	if errors.Is(ctx.Err(), context.DeadlineExceeded) || errors.Is(cause, context.DeadlineExceeded) || errors.As(cause, &timeout) && timeout.Timeout() {
		return &fabric.ParticipantUnreachableError{Cause: context.DeadlineExceeded}
	}
	return &UnavailableError{Cause: cause}
}

// BusyError reports temporary capacity exhaustion in a running service.
type BusyError struct{}

func (*BusyError) Error() string { return "client service is busy; retry after active requests finish" }

// AlreadyRunningError refuses a second socket publisher.
type AlreadyRunningError struct{}

func (*AlreadyRunningError) Error() string {
	return "client service is already running; use the existing service or ask the owner to stop it"
}

// PeerIdentityError refuses a socket server that is not authenticated as us.
type PeerIdentityError struct{ Cause error }

func (*PeerIdentityError) Error() string {
	return "client service peer could not be authenticated as this user; ask the owner to check the service"
}
func (e *PeerIdentityError) Unwrap() error { return e.Cause }

// FencedCallerError preserves an intentional sandbox boundary.
type FencedCallerError struct{ Cause error }

func (*FencedCallerError) Error() string {
	return "the caller's sandbox intentionally blocks the client service; ask the owner; do not look for another way in"
}
func (e *FencedCallerError) Unwrap() error { return e.Cause }
