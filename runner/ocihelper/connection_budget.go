package ocihelper

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"slices"
	"sync"
	"time"
)

const (
	// defaultControlConnectionReserve is the number of the helper's 64
	// connection slots that long-lived data streams can never take. Every
	// proxied client of a published service, every take-over view or control
	// leg, and each of a Computer's four host-bridge pumps holds one data
	// stream for its whole life; the control connection, each live attempt's
	// Watch, and short control RPCs (Signal, Delete, Run, removal, storage)
	// share the rest. Eight covers the control connection plus a Signal and
	// Delete for several attempts at once, while leaving 56 slots -- 14
	// Computers' bridge pumps, or dozens of keep-alive clients -- to data.
	defaultControlConnectionReserve = 8
	// connectionLimitRefusalBudget bounds the goroutines answering allowed
	// peers accepted over the whole connection limit. Each one reads at most
	// one request frame under connectionLimitRefusalTimeout, so a well-formed
	// agent request is answered in microseconds, and a silent allowed peer
	// holds a worker for at most that long. Peers outside the allowlist never
	// reach a worker. With every worker busy, the accept loop waits for one
	// rather than closing on an allowed peer.
	connectionLimitRefusalBudget  = 8
	connectionLimitRefusalTimeout = time.Second
	// A refusal burst ends after this long without a refusal; a burst that
	// never ends is still reported at most once per connectionLimitLogInterval.
	connectionLimitBurstQuiet  = 30 * time.Second
	connectionLimitLogInterval = time.Minute
)

// dataStreamMethod names the requests that become long-lived raw streams once
// admitted. They are the only connections whose count grows with a workload's
// own traffic, so they alone are held below the control reserve.
func dataStreamMethod(method Method) bool {
	return method == MethodDialAttemptPort || method == MethodDialHostBridge
}

// admitDataStream takes one data-stream slot without waiting. A full budget is
// answered with connection_limit rather than queued: the caller is a single
// proxied client or pump that can retry, and a queued stream would hold its
// connection slot while waiting.
func (server *Server) admitDataStream() (func(), bool) {
	select {
	case server.dataStreams <- struct{}{}:
		return func() { <-server.dataStreams }, true
	default:
		return nil, false
	}
}

// serveAdmitted serves a connection that already holds a connection slot.
func (server *Server) serveAdmitted(ctx context.Context, connection net.Conn) {
	go func() {
		defer func() {
			<-server.connections
			if server.config.connectionReleased != nil {
				server.config.connectionReleased()
			}
		}()
		server.handleConnection(ctx, connection)
	}()
}

// refuseOverLimit handles a connection accepted while every connection slot is
// taken. It runs on the accept loop.
//
// The peer's credentials are read first, synchronously: that is one
// non-blocking getsockopt, so it cannot stall the loop. A peer outside the UID
// allowlist is closed at once and never holds a slot or a refusal worker, so
// no number of foreign local connections can crowd out the agent.
//
// An allowed peer is the agent, and it must never see a bare close: closing on
// an unread request fails the client's write or read with EPIPE or EOF, which
// it rightly reads as runtime loss (#597). So a refusal worker consumes its one
// request frame and answers with a typed connection_limit refusal. When all
// refusal workers are busy, the loop waits for a worker or a connection slot
// rather than closing. Each worker finishes within
// connectionLimitRefusalTimeout, so the wait is bounded by that deadline; the
// connections behind it wait in the kernel backlog, and the client sees
// latency, never EOF.
func (server *Server) refuseOverLimit(ctx context.Context, connection net.Conn) {
	peer, peerErr := authenticateUnixPeer(connection)
	if peerErr != nil || !slices.Contains(server.config.AllowedUIDs, peer.UID) {
		_ = connection.Close()
		return
	}
	select {
	case server.limitRefusals <- struct{}{}:
	case server.connections <- struct{}{}:
		server.serveAdmitted(ctx, connection)
		return
	case <-ctx.Done():
		_ = connection.Close()
		return
	}
	server.noteConnectionLimit("connection_limit")
	go func() {
		defer func() { <-server.limitRefusals }()
		defer connection.Close()
		timeout := min(server.config.RequestTimeout, connectionLimitRefusalTimeout)
		if server.config.limitRefusalTimeout > 0 {
			timeout = server.config.limitRefusalTimeout
		}
		if err := connection.SetDeadline(time.Now().Add(timeout)); err != nil {
			return
		}
		if err := discardRequestFrame(connection); err != nil {
			return
		}
		_ = writeFailure(newFramedConn(connection), CodeConnectionLimit, "OCI helper connection limit is full; retry this operation")
	}()
}

// discardRequestFrame consumes exactly one bounded frame without decoding or
// retaining it. The refusal does not depend on what was asked.
func discardRequestFrame(connection net.Conn) error {
	var header [4]byte
	if _, err := io.ReadFull(connection, header[:]); err != nil {
		return err
	}
	size := binary.BigEndian.Uint32(header[:])
	if size == 0 || size > MaxFrameBytes {
		return fmt.Errorf("OCI helper frame size %d is outside bounds", size)
	}
	_, err := io.CopyN(io.Discard, connection, int64(size))
	return err
}

func (server *Server) noteConnectionLimit(budget string) {
	if report, suppressed := server.limitRefusalLog.Note(time.Now()); report {
		server.config.Logf("OCI helper refused a connection budget=%s connection_limit=%d data_stream_limit=%d suppressed_since_last_report=%d; the session is unaffected",
			budget, cap(server.connections), cap(server.dataStreams), suppressed)
	}
}

// IsConnectionLimitRefusal reports whether err is the helper's typed
// connection_limit refusal: one request refused under connection pressure,
// with the helper session and every attempt untouched.
func IsConnectionLimitRefusal(err error) bool {
	var rpcErr *RPCError
	return errors.As(err, &rpcErr) && rpcErr.Code == CodeConnectionLimit
}

// ConnectionLimitLog reports connection_limit refusals once per burst rather
// than once per connection. A burst is a run of refusals with no quiet gap of
// connectionLimitBurstQuiet; its first refusal is reported, and one that lasts
// is reported again at most once per connectionLimitLogInterval, carrying the
// count of refusals not reported since. The zero value is ready to use.
type ConnectionLimitLog struct {
	mu         sync.Mutex
	lastSeen   time.Time
	lastReport time.Time
	suppressed int
}

// Note records one refusal at now and says whether to report it, with how
// many refusals went unreported before it.
func (burst *ConnectionLimitLog) Note(now time.Time) (bool, int) {
	burst.mu.Lock()
	defer burst.mu.Unlock()
	newBurst := burst.lastSeen.IsZero() || now.Sub(burst.lastSeen) >= connectionLimitBurstQuiet
	burst.lastSeen = now
	if !newBurst && now.Sub(burst.lastReport) < connectionLimitLogInterval {
		burst.suppressed++
		return false, 0
	}
	suppressed := burst.suppressed
	burst.suppressed = 0
	burst.lastReport = now
	return true, suppressed
}
