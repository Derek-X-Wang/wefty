package agent

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"

	"github.com/Derek-X-Wang/wefty/l1"
	"github.com/coder/websocket"
)

// computerSessionRelay owns the client socket and its durable view leg. It can
// pause that view leg, install exactly one control leg, and later return to the
// same view connection without opening a second client input reader.
type immediateWebSocketCloser interface {
	CloseNow() error
}

type computerSessionRelay struct {
	client          net.Conn
	view            net.Conn
	clientWebSocket immediateWebSocketCloser
	viewWebSocket   immediateWebSocketCloser

	mu      sync.Mutex
	cond    *sync.Cond
	current net.Conn
	control *controllerConn
	active  int
	closed  bool

	reason  chan l1.ComputerTakeoverReason
	readers sync.WaitGroup
}

func newComputerSessionRelay(client, view net.Conn, clientWebSocket, viewWebSocket *websocket.Conn) *computerSessionRelay {
	relay := &computerSessionRelay{
		client: client, view: view, clientWebSocket: clientWebSocket, viewWebSocket: viewWebSocket, current: view,
		reason: make(chan l1.ComputerTakeoverReason, 3),
	}
	relay.cond = sync.NewCond(&relay.mu)
	return relay
}

func (relay *computerSessionRelay) Start() {
	relay.readers.Add(2)
	go relay.readClient()
	go relay.readBackend(relay.view, true)
}

func (relay *computerSessionRelay) Reasons() <-chan l1.ComputerTakeoverReason { return relay.reason }

func (relay *computerSessionRelay) readClient() {
	defer relay.readers.Done()
	_, _ = io.Copy(relay, relay.client)
	relay.notify(l1.ComputerTakeoverClientClosed)
}

func (relay *computerSessionRelay) readBackend(source net.Conn, view bool) {
	defer relay.readers.Done()
	buffer := make([]byte, 32*1024)
	for {
		count, err := source.Read(buffer)
		if count > 0 && !relay.deliver(source, buffer[:count]) {
			return
		}
		if err != nil {
			if view && !relay.isClosed() {
				relay.notify(l1.ComputerTakeoverViewBackendClosed)
			}
			return
		}
	}
}

// Write is the one client-input path. During a transition it waits for the
// replacement leg; a failed control write is retried only after tenure has
// removed that leg and restored view.
func (relay *computerSessionRelay) Write(payload []byte) (int, error) {
	written := 0
	for written < len(payload) {
		relay.mu.Lock()
		for relay.current == nil && !relay.closed {
			relay.cond.Wait()
		}
		if relay.closed {
			relay.mu.Unlock()
			return written, net.ErrClosed
		}
		current := relay.current
		relay.active++
		relay.mu.Unlock()

		count, err := current.Write(payload[written:])
		written += count

		relay.mu.Lock()
		relay.active--
		if relay.active == 0 {
			relay.cond.Broadcast()
		}
		stillCurrent := relay.current == current
		isControl := relay.control != nil && current == relay.control
		relay.mu.Unlock()

		if err == nil {
			continue
		}
		if !stillCurrent {
			continue
		}
		if isControl {
			relay.mu.Lock()
			for relay.current == current && !relay.closed {
				relay.cond.Wait()
			}
			relay.mu.Unlock()
			continue
		}
		relay.notify(l1.ComputerTakeoverViewBackendClosed)
		return written, err
	}
	return written, nil
}

func (relay *computerSessionRelay) deliver(source net.Conn, payload []byte) bool {
	relay.mu.Lock()
	if relay.closed {
		relay.mu.Unlock()
		return false
	}
	if relay.current != source {
		relay.mu.Unlock()
		return true
	}
	relay.active++
	relay.mu.Unlock()

	_, err := relay.client.Write(payload)

	relay.mu.Lock()
	relay.active--
	if relay.active == 0 {
		relay.cond.Broadcast()
	}
	relay.mu.Unlock()
	if err != nil {
		relay.notify(l1.ComputerTakeoverClientClosed)
		return false
	}
	return true
}

func (relay *computerSessionRelay) activateControl(control *controllerConn) error {
	relay.mu.Lock()
	if relay.closed || relay.current != relay.view || relay.control != nil {
		relay.mu.Unlock()
		return &ComputerTenureError{Code: ComputerTenureSessionEnded}
	}
	relay.current = nil
	for relay.active != 0 {
		relay.cond.Wait()
	}
	relay.control = control
	relay.current = control
	relay.cond.Broadcast()
	relay.readers.Add(1)
	relay.mu.Unlock()
	go relay.readBackend(control, false)
	return nil
}

func (relay *computerSessionRelay) deactivateControl(control *controllerConn) {
	relay.mu.Lock()
	if relay.control != control {
		relay.mu.Unlock()
		control.closeAndWait()
		return
	}
	relay.current = nil
	for relay.active != 0 {
		relay.cond.Wait()
	}
	relay.control = nil
	relay.mu.Unlock()

	control.closeAndWait()

	relay.mu.Lock()
	if !relay.closed {
		relay.current = relay.view
		relay.cond.Broadcast()
	}
	relay.mu.Unlock()
}

func (relay *computerSessionRelay) Close() {
	relay.mu.Lock()
	if relay.closed {
		relay.mu.Unlock()
		return
	}
	relay.closed = true
	relay.current = nil
	relay.cond.Broadcast()
	control := relay.control
	relay.control = nil
	relay.mu.Unlock()

	// websocket.NetConn.Close performs a five-second close handshake. Force the
	// underlying sockets closed first so peer cooperation and goroutine
	// scheduling cannot delay the session boundary or policy drain.
	if relay.clientWebSocket != nil {
		_ = relay.clientWebSocket.CloseNow()
	}
	if relay.viewWebSocket != nil {
		_ = relay.viewWebSocket.CloseNow()
	}
	_ = relay.client.Close()
	_ = relay.view.Close()
	if control != nil {
		control.closeAndWait()
	}
	relay.mu.Lock()
	for relay.active != 0 {
		relay.cond.Wait()
	}
	relay.mu.Unlock()
	relay.readers.Wait()
}

func (relay *computerSessionRelay) notify(reason l1.ComputerTakeoverReason) {
	select {
	case relay.reason <- reason:
	default:
	}
}

func (relay *computerSessionRelay) isClosed() bool {
	relay.mu.Lock()
	defer relay.mu.Unlock()
	return relay.closed
}

func computerSessionEndReason(ctx context.Context, fallback l1.ComputerTakeoverReason) l1.ComputerTakeoverReason {
	var ended *computerSessionEnd
	if errors.As(context.Cause(ctx), &ended) {
		return ended.reason
	}
	return fallback
}

var _ io.Writer = (*computerSessionRelay)(nil)
