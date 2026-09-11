package ocihelper

// Durable Attempt ownership: the identity rules that decide whether a record
// left on disk by a previous helper generation can be re-adopted. They are kept
// away from containerd mechanics deliberately -- the #412 crash loop was a
// comparison bug, and a comparison bug must be provable on every platform.

const (
	durableAttemptOwnershipVersion = 1

	attemptOwnershipQuarantinedRecordName = "record.json"
	attemptOwnershipQuarantineReceiptName = "quarantine.json"
)

type durableAttemptOwnership struct {
	Version    int                `json:"version"`
	Authority  AttemptAuthority   `json:"authority"`
	Resources  ResourceIdentity   `json:"resources"`
	Retentions []DurableRetention `json:"retentions,omitempty"`
}

// sameAuthorityDerivedResourceNames compares exactly the runtime names an
// Attempt authority determines by itself. The handoff volume directory is
// deliberately excluded: it is derived from the stable owner key
// (DeterministicHandoffVolumeDirectory), so a helper generation holding only
// labelled containerd metadata -- the boot sweep's whole world -- can never
// re-derive it and must reconcile it from whichever side does know it.
func sameAuthorityDerivedResourceNames(left, right ResourceIdentity) bool {
	return left.LeaseID == right.LeaseID && left.SnapshotID == right.SnapshotID &&
		left.ContainerID == right.ContainerID && left.TaskID == right.TaskID &&
		left.ShimID == right.ShimID && left.CgroupID == right.CgroupID &&
		left.LogSegmentDirectory == right.LogSegmentDirectory &&
		left.ServiceVolumeDirectory == right.ServiceVolumeDirectory &&
		left.ServiceVolumeOwnerRecord == right.ServiceVolumeOwnerRecord
}

// attemptOwnershipReadoptionRefusal reports the typed reason an existing record
// cannot be re-adopted under the given fenced authority, or reconcilable=true.
func attemptOwnershipReadoptionRefusal(existing durableAttemptOwnership, name string, authority AttemptAuthority, resources ResourceIdentity) (AttemptOwnershipQuarantineReason, bool) {
	if existing.Version != durableAttemptOwnershipVersion {
		return AttemptOwnershipQuarantineUnknownVersion, false
	}
	if !validDurableAttemptOwnershipIdentity(existing, name) {
		return AttemptOwnershipQuarantineInvalidRecord, false
	}
	// The record's file name is the digest of its complete authority tuple, so
	// a name that matches while the tuple does not is corruption, not a fence.
	if existing.Authority != authority || !sameAuthorityDerivedResourceNames(existing.Resources, resources) {
		return AttemptOwnershipQuarantineAuthorityMismatch, false
	}
	return "", true
}

func validDurableAttemptOwnership(record durableAttemptOwnership, filename string) bool {
	return record.Version == durableAttemptOwnershipVersion && validDurableAttemptOwnershipIdentity(record, filename)
}

func validDurableAttemptOwnershipIdentity(record durableAttemptOwnership, filename string) bool {
	if record.Authority.validate() != nil {
		return false
	}
	expected, err := DeterministicResourceIdentity(record.Authority)
	return err == nil && sameAuthorityDerivedResourceNames(record.Resources, expected) && filename == expected.ContainerID+".json"
}
