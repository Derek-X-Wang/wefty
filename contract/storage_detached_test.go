package contract

import "testing"

func validDetachmentWitness() ComputerStorageDetachmentWitness {
	return ComputerStorageDetachmentWitness{Kind: ComputerStorageDetachedKind, EvidenceKind: ComputerStorageDetachSameBootReap,
		ReceiptID: "receipt", NodeID: "node", ComputerID: "computer", StorageID: "storage", StorageGeneration: 2,
		AttachedJobID: "job-that-ran", AttachedAttemptID: "attempt", AttachedBootSessionID: "boot", HelperGeneration: 3}
}

func TestComputerStorageDetachmentWitnessValidity(t *testing.T) {
	if !validDetachmentWitness().Valid() {
		t.Fatal("a complete same-boot reap witness is invalid")
	}
	sweep := validDetachmentWitness()
	sweep.EvidenceKind, sweep.SweepEpoch = ComputerStorageDetachPriorBootSweep, "epoch"
	if !sweep.Valid() {
		t.Fatal("a complete prior-boot sweep witness is invalid")
	}
	for name, mutate := range map[string]func(*ComputerStorageDetachmentWitness){
		"wrong kind":               func(w *ComputerStorageDetachmentWitness) { w.Kind = ComputerStorageResetVerifiedKind },
		"unknown evidence kind":    func(w *ComputerStorageDetachmentWitness) { w.EvidenceKind = "guess" },
		"reap with a sweep epoch":  func(w *ComputerStorageDetachmentWitness) { w.SweepEpoch = "epoch" },
		"sweep without an epoch":   func(w *ComputerStorageDetachmentWitness) { w.EvidenceKind = ComputerStorageDetachPriorBootSweep },
		"missing receipt":          func(w *ComputerStorageDetachmentWitness) { w.ReceiptID = " " },
		"missing Node":             func(w *ComputerStorageDetachmentWitness) { w.NodeID = "" },
		"missing Computer":         func(w *ComputerStorageDetachmentWitness) { w.ComputerID = "" },
		"missing Storage":          func(w *ComputerStorageDetachmentWitness) { w.StorageID = "" },
		"zero generation":          func(w *ComputerStorageDetachmentWitness) { w.StorageGeneration = 0 },
		"missing attached Job":     func(w *ComputerStorageDetachmentWitness) { w.AttachedJobID = "" },
		"missing attached attempt": func(w *ComputerStorageDetachmentWitness) { w.AttachedAttemptID = "" },
		"missing attached boot":    func(w *ComputerStorageDetachmentWitness) { w.AttachedBootSessionID = "" },
		"zero helper generation":   func(w *ComputerStorageDetachmentWitness) { w.HelperGeneration = 0 },
	} {
		witness := validDetachmentWitness()
		mutate(&witness)
		if witness.Valid() {
			t.Errorf("%s: witness is valid", name)
		}
	}
}

func TestStorageDetachedRemovalAttemptID(t *testing.T) {
	id := StorageDetachedRemovalAttemptID(4)
	if id != "storage-detached-4" || !ValidStorageDetachedRemovalAttemptID(id, 4) {
		t.Fatalf("detached attempt ID = %q", id)
	}
	for _, other := range []string{StorageOnlyRemovalAttemptID(4), StorageAbsentRemovalAttemptID(4), StorageDetachedRemovalAttemptID(5)} {
		if ValidStorageDetachedRemovalAttemptID(other, 4) {
			t.Fatalf("%q read as the generation-4 detached attempt", other)
		}
	}
	if ValidStorageDetachedRemovalAttemptID(StorageDetachedRemovalAttemptID(0), 0) {
		t.Fatal("generation 0 has a detached attempt")
	}
}

// A sweep restamp of one detach keeps the last attachment; a newer attach
// changes it.
func TestComputerStorageDetachmentWitnessSameLastAttachment(t *testing.T) {
	reap := validDetachmentWitness()
	restamp := reap
	restamp.EvidenceKind, restamp.SweepEpoch, restamp.ReceiptID, restamp.HelperGeneration = ComputerStorageDetachPriorBootSweep, "epoch", "new-receipt", 9
	if !reap.SameLastAttachment(restamp) {
		t.Fatal("a sweep restamp read as a newer attachment")
	}
	for name, mutate := range map[string]func(*ComputerStorageDetachmentWitness){
		"newer attempt": func(w *ComputerStorageDetachmentWitness) { w.AttachedAttemptID = "attempt-2" },
		"newer Job":     func(w *ComputerStorageDetachmentWitness) { w.AttachedJobID = "job-2" },
		"newer boot":    func(w *ComputerStorageDetachmentWitness) { w.AttachedBootSessionID = "boot-2" },
		"other Node":    func(w *ComputerStorageDetachmentWitness) { w.NodeID = "node-2" },
		"other gen":     func(w *ComputerStorageDetachmentWitness) { w.StorageGeneration = 3 },
	} {
		other := restamp
		mutate(&other)
		if reap.SameLastAttachment(other) {
			t.Errorf("%s read as the same last attachment", name)
		}
	}
}
