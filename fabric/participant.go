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
}

func (e *ParticipantIdentityError) Error() string {
	return "saved participant " + e.Reason + "; retire the old device and enrol a new identity through fleet setup (public fleet setup is not available yet)"
}

// ParticipantUnreachableError means the saved participant could not be checked.
// It makes no assertion about identity validity and never permits a fallback.
type ParticipantUnreachableError struct{ Cause error }

func (e *ParticipantUnreachableError) Error() string {
	return fmt.Sprintf("Fabric participant could not be reached or confirmed; check network reachability and retry with backoff: %v", e.Cause)
}
func (e *ParticipantUnreachableError) Unwrap() error { return e.Cause }
