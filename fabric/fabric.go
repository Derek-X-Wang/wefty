// Package fabric is wefty's only network-layer seam. Public types are owned by
// wefty and do not expose implementation-specific networking types or names.
package fabric

import (
	"context"
	"errors"
	"net"
	"time"
)

// ErrIdentityNotFound means the network could not authenticate the requested
// remote address.
var ErrIdentityNotFound = errors.New("fabric identity not found")

type Fabric interface {
	Listen(network, address string) (net.Listener, error)
	Dial(ctx context.Context, network, address string) (net.Conn, error)
	WhoIs(ctx context.Context, remoteAddress string) (Identity, error)
	// ConnectHost returns the Fabric-owned hostname an operator can combine
	// with a published port. It is presentation data only: callers must never
	// use it for identity, authorization, or scheduling.
	ConnectHost() string
}

type Provisioner interface {
	Provision(ctx context.Context, spec ProvisionSpec) (Credential, error)
	Deprovision(ctx context.Context, nodeID string) error
}

// Identity is authenticated by the Fabric implementation. Tags in this type,
// never fields self-reported by a node registration request, drive claims.
type Identity struct {
	NodeID string
	User   string
	Tags   []string
}

type ProvisionSpec struct {
	NodeID    string
	Tags      []string
	Ephemeral bool
	ExpiresAt time.Time
}

type Credential struct {
	Value     string
	ExpiresAt time.Time
}
