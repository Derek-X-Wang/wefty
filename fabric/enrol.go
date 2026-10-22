package fabric

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// EnrolSpec is a setup-only request. The single-use credential is memory-only;
// it must never enter a profile, service argument, or service environment.
type EnrolSpec struct {
	Name           string
	StateDirectory string
	Credential     Credential `json:"-"`
	CoordinatorURL string
	LoginURL       func(string)        `json:"-"`
	Progress       func(EnrolProgress) `json:"-"`
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

// EnrolProgress describes setup's current wait without exposing backend states.
// Callbacks are synchronous and must return promptly.
type EnrolProgress string

const (
	EnrolStarting                 EnrolProgress = "starting"
	EnrolWaitingForSignIn         EnrolProgress = "waiting_for_sign_in"
	EnrolWaitingForDeviceApproval EnrolProgress = "waiting_for_device_approval"
	EnrolConnecting               EnrolProgress = "connecting"
)

// EnrolTimeLimit bounds each setup participant, including browser sign-in.
const EnrolTimeLimit = 15 * time.Minute

// EnrolTimeoutError leaves setup's staged profile available for a rerun.
// State is the last neutral wait, never a backend diagnostic.
type EnrolTimeoutError struct {
	State        EnrolProgress
	Limit        time.Duration
	NeedsJoinKey bool
}

func (e *EnrolTimeoutError) Error() string {
	d := e.Limit
	if d <= 0 {
		d = EnrolTimeLimit
	}
	limit := enrolLimitText(d)
	advice := "check network access and the coordinator; rerun setup and sign in again"
	if e.NeedsJoinKey {
		advice = "check network access and the coordinator; rerun setup with a new join key"
	}
	switch e.State {
	case EnrolWaitingForSignIn:
		advice = fmt.Sprintf("rerun setup and complete browser sign-in within %s", limit)
	case EnrolWaitingForDeviceApproval:
		if e.NeedsJoinKey {
			advice = fmt.Sprintf("rerun setup with a new join key and approve the new device within %s, or use a pre-approved key", limit)
		} else {
			advice = fmt.Sprintf("rerun setup, sign in again, and approve the new device within %s", limit)
		}
	}
	return fmt.Sprintf("enrolment time limit reached after %s: %s; %s", limit, e.State.Description(), advice)
}

// enrolLimitText writes whole minutes as words ("15 minutes"), which reads
// better in setup output than Duration's "15m0s".
func enrolLimitText(d time.Duration) string {
	if d%time.Minute != 0 {
		return d.String()
	}
	if d == time.Minute {
		return "1 minute"
	}
	return fmt.Sprintf("%d minutes", int64(d/time.Minute))
}

// Description and Advice use fixed text so arbitrary backend data cannot enter
// progress output or a timeout error.
func (p EnrolProgress) Description() string {
	switch p {
	case EnrolWaitingForSignIn:
		return "waiting for browser sign-in"
	case EnrolWaitingForDeviceApproval:
		return "waiting for the network to approve this device"
	case EnrolConnecting:
		return "connecting to the network"
	default:
		return "starting network enrolment"
	}
}
func (p EnrolProgress) Advice() string {
	switch p {
	case EnrolWaitingForSignIn:
		return "complete sign-in in the browser"
	case EnrolWaitingForDeviceApproval:
		return "approve it in your network's admin console, or use a pre-approved key"
	default:
		return "check network access, the coordinator, and the join key if used"
	}
}
