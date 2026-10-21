package fabric

import (
	"context"
	"errors"
	"time"
)

// EnrolSpec is a setup-only request. The single-use credential is memory-only;
// it must never enter a profile, service argument, or service environment.
type EnrolSpec struct {
	Name           string
	StateDirectory string
	Credential     Credential `json:"-"`
	CoordinatorURL string
	LoginURL       func(string) `json:"-"`
}

// SelfIdentity is authenticated local enrolment evidence. Name is the Wefty
// logical name; NetworkName is the checked label granted by the network.
// Provider-specific domains and types stay behind the Fabric seam.
type SelfIdentity struct {
	Identity
	Name        string
	NetworkName string
	KeyExpiry   time.Time
}

var ErrGrantedNameMismatch = errors.New("fabric granted a different participant name; retire the stale device before enrolling again")

// Enroller exists only at the setup seam. Plain Fabric cannot enrol.
type Enroller interface {
	Enrol(context.Context, EnrolSpec) (SelfIdentity, error)
	Logout(context.Context, EnrolSpec) error
}
