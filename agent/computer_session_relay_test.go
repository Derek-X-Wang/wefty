package agent

import (
	"net"
	"reflect"
	"sync"
	"testing"
)

func TestComputerRelayForceClosesWebSocketsBeforeNetConnHandshake(t *testing.T) {
	var actions []string
	relay := &computerSessionRelay{
		client:          &closeOrderConn{label: "client net.Conn", actions: &actions},
		view:            &closeOrderConn{label: "view net.Conn", actions: &actions},
		clientWebSocket: closeOrderWebSocket{label: "client CloseNow", actions: &actions},
		viewWebSocket:   closeOrderWebSocket{label: "view CloseNow", actions: &actions},
	}
	relay.cond = sync.NewCond(&relay.mu)
	relay.Close()
	want := []string{"client CloseNow", "view CloseNow", "client net.Conn", "view net.Conn"}
	if !reflect.DeepEqual(actions, want) {
		t.Fatalf("relay close order = %v, want %v", actions, want)
	}

	actions = nil
	control := &controllerConn{
		Conn:      &closeOrderConn{label: "control net.Conn", actions: &actions},
		websocket: closeOrderWebSocket{label: "control CloseNow", actions: &actions},
	}
	control.cond = sync.NewCond(&control.mu)
	control.closeAndWait()
	want = []string{"control CloseNow", "control net.Conn"}
	if !reflect.DeepEqual(actions, want) {
		t.Fatalf("control close order = %v, want %v", actions, want)
	}
}

type closeOrderConn struct {
	net.Conn
	label   string
	actions *[]string
}

func (connection *closeOrderConn) Close() error {
	*connection.actions = append(*connection.actions, connection.label)
	return nil
}

type closeOrderWebSocket struct {
	label   string
	actions *[]string
}

func (connection closeOrderWebSocket) CloseNow() error {
	*connection.actions = append(*connection.actions, connection.label)
	return nil
}
