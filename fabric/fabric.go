// Package fabric is wefty's only network-layer seam. Public types are owned by
// wefty and do not expose implementation-specific networking types. As the
// exact #266 exception, a raw Fabric address may appear as a clearly labelled
// secondary connection field behind a wefty-owned friendly name; it is never
// the only handle and callers must not interpret it.
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
	// ConnectHost returns the raw Fabric-owned hostname an operator can combine
	// with a published port. It is secondary presentation data behind a
	// wefty-owned friendly name: callers must never use it for identity,
	// authorization, or scheduling.
	ConnectHost() string
}

// PersonIdentityTrust describes whether a Fabric authenticates person identity
// or merely transports a development identity asserted by its peer.
type PersonIdentityTrust string

const (
	PersonIdentitySelfAsserted  PersonIdentityTrust = "self_asserted"
	PersonIdentityAuthenticated PersonIdentityTrust = "authenticated"
)

// PersonIdentityTrustProvider lets an L1 deployment fail closed when a Fabric
// cannot authenticate person identity. Implementations that omit this seam are
// treated as self-asserted.
type PersonIdentityTrustProvider interface {
	PersonIdentityTrust() PersonIdentityTrust
}

// IdentityKind distinguishes person-capable peers from machine principals.
// An empty kind is retained for compatibility with test/dev implementations;
// IdentityKindMachine is always ineligible for person authorization.
type IdentityKind string

const IdentityKindMachine IdentityKind = "machine"

type Provisioner interface {
	Provision(ctx context.Context, spec ProvisionSpec) (Credential, error)
	Deprovision(ctx context.Context, nodeID string) error
}

// Identity is authenticated by the Fabric implementation. UserID identifies
// one person across devices, while DeviceID retains the peer device evidence.
// FabricID identifies the issuing Fabric so a different issuer cannot silently
// reinterpret an existing UserID. Kind records machine principals without
// exposing implementation-specific tag semantics.
// Both are opaque Fabric-owned identifiers: callers must not parse them or
// substitute display data for policy. Tags in this type, never fields
// self-reported by a node registration request, drive protocol principals and
// claims.
type Identity struct {
	NodeID      string
	UserID      string
	DeviceID    string
	FabricID    string
	Kind        IdentityKind
	DisplayName string
	Tags        []string
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
