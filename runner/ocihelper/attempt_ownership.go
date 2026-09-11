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

// attemptOwnershipDisposition is what the boot sweep does with a durable
// ownership record it found already on disk.
type attemptOwnershipDisposition string

const (
	// attemptOwnershipReadopt keeps the record exactly as written.
	attemptOwnershipReadopt attemptOwnershipDisposition = "readopt"
	// attemptOwnershipDefer leaves the record untouched and publishes nothing.
	// It is reserved for a record this build cannot interpret but has no reason
	// to distrust -- a newer version written by a helper that has since been
	// rolled back. The existing snapshot path already reports it as
	// unknown_version, leaves it unbound so it cannot authorize any removal,
	// and garbage-collects it once its named resources are absent.
	attemptOwnershipDefer attemptOwnershipDisposition = "defer"
	// attemptOwnershipQuarantine moves the record aside with a typed receipt.
	attemptOwnershipQuarantine attemptOwnershipDisposition = "quarantine"
)

// attemptOwnershipDispositionFor decides what to do with an existing record
// under the fenced authority the sweep just proved from container labels.
//
// A version this build does not know is deliberately NOT quarantined. A record
// version bump would otherwise quarantine every live Attempt's perfectly valid
// record on the first boot after an upgrade or rollback, discarding retention
// receipts wholesale. Quarantine is for a record that contradicts itself or
// cannot be read -- evidence of corruption, not of a different vintage.
func attemptOwnershipDispositionFor(existing durableAttemptOwnership, name string, authority AttemptAuthority, resources ResourceIdentity) (attemptOwnershipDisposition, AttemptOwnershipQuarantineReason) {
	if existing.Version != durableAttemptOwnershipVersion {
		return attemptOwnershipDefer, ""
	}
	if !validDurableAttemptOwnershipIdentity(existing, name) {
		return attemptOwnershipQuarantine, AttemptOwnershipQuarantineInvalidRecord
	}
	// The record's file name is the digest of its complete authority tuple, so
	// a name that matches while the tuple does not is corruption, not a fence.
	if existing.Authority != authority || !sameAuthorityDerivedResourceNames(existing.Resources, resources) {
		return attemptOwnershipQuarantine, AttemptOwnershipQuarantineAuthorityMismatch
	}
	return attemptOwnershipReadopt, ""
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
