package fabric

// ParticipantIdentityError refuses use of a saved participant that needs
// enrolment or no longer matches its recorded identity. It is never permission
// to fall back to another local identity.
type ParticipantIdentityError struct{ Reason string }

func (e *ParticipantIdentityError) Error() string {
	return "saved participant " + e.Reason + "; retire the old device and enrol a new identity through fleet setup (public fleet setup is not available yet)"
}
