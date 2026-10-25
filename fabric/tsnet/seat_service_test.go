package tsnet

import (
	"errors"
	"net"
	"testing"

	"github.com/Derek-X-Wang/wefty/fabric/internal/naming"
)

func TestSeatServiceAddressListensAsEnrolledParticipantOnPort80(t *testing.T) {
	name, _, err := naming.Parse("wefty://node/enrolled-name")
	if err != nil {
		t.Fatal(err)
	}
	stop := errors.New("test listener")
	address := ""
	f := &Fabric{localName: name, listenNode: func(_, got string) (net.Listener, error) { address = got; return nil, stop }}
	if _, err := f.Listen("tcp", f.LocalServiceAddress()); !errors.Is(err, stop) || address != ":80" {
		t.Fatalf("Seat service address=%s listener=%s err=%v", f.LocalServiceAddress(), address, err)
	}
}
