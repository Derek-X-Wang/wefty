package ocihelper

type capacityReservation struct {
	tmpfsCeilingBytes           int64
	memoryBytes                 int64
	diskBytes                   int64
	pendingDiskBytes            int64
	pendingDiskAdmissionCeiling int64
	diskMaterialized            bool
	memoryOwner                 string
	attempts                    map[string]struct{}
}

// The caller holds capacityMu. Admission and every live limit operation
// fence this reservation to one exact Attempt cgroup.
func (r *capacityReservation) claimMutableMemory(attempt string) error {
	if _, ok := r.attempts[attempt]; !ok {
		return &SeatOperationRefusalError{Reason: SeatRefusalAttemptNotLive}
	}
	if len(r.attempts) != 1 || r.memoryOwner != "" && r.memoryOwner != attempt {
		return &SeatOperationRefusalError{Reason: SeatRefusalAttemptOverlap}
	}
	r.memoryOwner = attempt
	return nil
}
func (r *capacityReservation) checkReplacement(attempt string) error {
	if len(r.attempts) > 0 {
		if _, same := r.attempts[attempt]; !same {
			return &SeatOperationRefusalError{Reason: SeatRefusalAttemptOverlap}
		}
	}
	if r.memoryOwner != "" && r.memoryOwner != attempt {
		return &SeatOperationRefusalError{Reason: SeatRefusalAttemptOverlap}
	}
	return nil
}
