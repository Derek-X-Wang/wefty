package ocihelper

import (
	"errors"
	"testing"
)

func TestMutableComputerMemoryReservationFencesExactAttempt(t *testing.T) {
	immutable := &capacityReservation{memoryBytes: 1 << 30, attempts: map[string]struct{}{"old": {}}}
	if err := immutable.checkReplacement("next"); err == nil {
		t.Fatal("replacement overlapped before the first resize claimed memory")
	}
	r := &capacityReservation{memoryBytes: 1 << 30, attempts: map[string]struct{}{"old": {}, "next": {}}}
	if err := r.claimMutableMemory("old"); err == nil {
		t.Fatal("overlapping cgroups shared a mutable memory reservation")
	}
	delete(r.attempts, "next")
	if err := r.claimMutableMemory("old"); err != nil {
		t.Fatal(err)
	}
	r.memoryBytes = 2 << 30
	var refusal *SeatOperationRefusalError
	if err := r.checkReplacement("next"); !errors.As(err, &refusal) || refusal.Reason != SeatRefusalAttemptOverlap {
		t.Fatalf("replacement after grow was not fenced: %v", err)
	}
	if err := r.claimMutableMemory("next"); !errors.As(err, &refusal) {
		t.Fatalf("foreign Attempt changed memory: %v", err)
	}
	if r.memoryBytes != 2<<30 {
		t.Fatal("refusal returned held room")
	}
}
