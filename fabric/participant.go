package fabric

import "fmt"

// ParticipantIdentityError refuses use of a saved participant that needs
// enrolment or no longer matches its recorded identity. It is never permission
// to fall back to another local identity.
type ParticipantIdentityError struct {
	Reason string
	// Person distinguishes person sign-in from a machine-participant refusal.
	// The zero value describes a machine.
	Person bool
	// CoordinatorConflict needs override correction, not identity retirement.
	CoordinatorConflict bool
}

func (e *ParticipantIdentityError) Error() string {
	if e.CoordinatorConflict {
		return "saved participant " + e.Reason + "; use the recorded coordinator URL or unset the conflicting coordinator override"
	}
	return "saved participant " + e.Reason + "; restore its original saved state, or retire the old device before enrolling a replacement identity through fleet setup"
}

// ParticipantUnreachableError means the saved participant could not be checked.
// It makes no assertion about identity validity and never permits a fallback.
type ParticipantUnreachableError struct{ Cause error }

func (e *ParticipantUnreachableError) Error() string {
	return fmt.Sprintf("Fabric participant could not be reached or confirmed; check network reachability and retry with backoff: %v", e.Cause)
}
func (e *ParticipantUnreachableError) Unwrap() error { return e.Cause }

// ParticipantInUseError refuses a second owner without revealing state paths.
type ParticipantInUseError struct{ Participant string }

func (e *ParticipantInUseError) Error() string {
	return fmt.Sprintf("participant %q is already in use; stop its current owner before starting another process", e.Participant)
}

// DestinationRefusedError means an outbound target is outside the Fabric.
// It is a policy refusal, never permission to use a host-network fallback.
type DestinationRefusedError struct{}

func (*DestinationRefusedError) Error() string {
	return "outbound destination is not a Fabric peer; use a Fabric participant endpoint"
}
