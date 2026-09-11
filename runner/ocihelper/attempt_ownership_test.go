package ocihelper

import "testing"

func ownershipTestResources(t *testing.T, authority AttemptAuthority, ownerKey string) ResourceIdentity {
	t.Helper()
	resources, err := DeterministicResourceIdentity(authority)
	if err != nil {
		t.Fatal(err)
	}
	if ownerKey == "" {
		return resources
	}
	handoff, err := DeterministicHandoffVolumeDirectory(ownerKey)
	if err != nil {
		t.Fatal(err)
	}
	if handoff == resources.HandoffVolumeDirectory {
		t.Fatal("fixture owner key collides with the authority digest; the test would prove nothing")
	}
	resources.HandoffVolumeDirectory = handoff
	return resources
}

// This is #412 in one assertion. The handoff volume directory is the only name
// in the identity that the Attempt authority does not determine, so a boot
// sweep -- which knows nothing but the authority tuple from container labels --
// must not compare it. Comparing it made the live record from the previous
// helper generation permanently unmatchable and crash-looped the unit.
func TestOwnerKeyHandoffVolumeIsNotAnAuthorityDerivedName(t *testing.T) {
	authority := testAuthority()
	live := ownershipTestResources(t, authority, "job-1:handoff-owner")
	sweep := ownershipTestResources(t, authority, "")

	if live.HandoffVolumeDirectory == sweep.HandoffVolumeDirectory {
		t.Fatal("the fixture no longer reproduces the two different handoff names")
	}
	if !sameAuthorityDerivedResourceNames(live, sweep) {
		t.Fatal("a boot sweep cannot recognize the live record written by its own previous generation")
	}
	// Everything the authority does determine still has to agree.
	if live.ContainerID != sweep.ContainerID || live.LeaseID != sweep.LeaseID || live.CgroupID != sweep.CgroupID {
		t.Fatalf("authority-derived names diverged between generations: live=%+v sweep=%+v", live, sweep)
	}
}

// Every other name still fences. Excluding the handoff directory must not turn
// the comparator into a rubber stamp.
func TestAuthorityDerivedComparisonStillFencesEveryOtherName(t *testing.T) {
	authority := testAuthority()
	base := ownershipTestResources(t, authority, "")
	for name, mutate := range map[string]func(*ResourceIdentity){
		"lease":                func(r *ResourceIdentity) { r.LeaseID = "wefty-lease-other" },
		"snapshot":             func(r *ResourceIdentity) { r.SnapshotID = "wefty-snapshot-other" },
		"container":            func(r *ResourceIdentity) { r.ContainerID = "wefty-container-other" },
		"task":                 func(r *ResourceIdentity) { r.TaskID = "wefty-container-other" },
		"shim":                 func(r *ResourceIdentity) { r.ShimID = "wefty-container-other" },
		"cgroup":               func(r *ResourceIdentity) { r.CgroupID = "wefty-cgroup-other" },
		"log segments":         func(r *ResourceIdentity) { r.LogSegmentDirectory = "wefty-log-segments-other" },
		"service volume":       func(r *ResourceIdentity) { r.ServiceVolumeDirectory = "wefty-service-volume-other" },
		"service owner record": func(r *ResourceIdentity) { r.ServiceVolumeOwnerRecord = "wefty-service-volume-other.owner" },
	} {
		mutated := base
		mutate(&mutated)
		if sameAuthorityDerivedResourceNames(base, mutated) {
			t.Fatalf("a mutated %s name passed the fenced comparison", name)
		}
	}
}

// The re-adoption decision table, stated once.
func TestAttemptOwnershipReadoptionDecisionTable(t *testing.T) {
	authority := testAuthority()
	live := ownershipTestResources(t, authority, "job-1:handoff-owner")
	sweep := ownershipTestResources(t, authority, "")
	name := sweep.ContainerID + ".json"

	foreignAuthority := authority
	foreignAuthority.FencingToken = "fence-2"
	foreignResources := ownershipTestResources(t, foreignAuthority, "job-1:handoff-owner")

	for _, test := range []struct {
		name        string
		record      durableAttemptOwnership
		disposition attemptOwnershipDisposition
		reason      AttemptOwnershipQuarantineReason
	}{
		{
			name:        "a record from a previous generation is re-adopted",
			record:      durableAttemptOwnership{Version: durableAttemptOwnershipVersion, Authority: authority, Resources: live},
			disposition: attemptOwnershipReadopt,
		},
		{
			name:        "a record this generation wrote itself is re-adopted",
			record:      durableAttemptOwnership{Version: durableAttemptOwnershipVersion, Authority: authority, Resources: sweep},
			disposition: attemptOwnershipReadopt,
		},
		{
			// A version bump must not quarantine every live Attempt's valid
			// record on the first boot after an upgrade or rollback.
			name:        "an unknown version is left alone, not quarantined",
			record:      durableAttemptOwnership{Version: durableAttemptOwnershipVersion + 1, Authority: authority, Resources: live},
			disposition: attemptOwnershipDefer,
		},
		{
			name:        "an authority tuple that contradicts the file name is quarantined",
			record:      durableAttemptOwnership{Version: durableAttemptOwnershipVersion, Authority: foreignAuthority, Resources: foreignResources},
			disposition: attemptOwnershipQuarantine,
			reason:      AttemptOwnershipQuarantineInvalidRecord,
		},
		{
			name: "resource names that contradict the record's own authority are quarantined",
			record: durableAttemptOwnership{Version: durableAttemptOwnershipVersion, Authority: authority, Resources: func() ResourceIdentity {
				mutated := live
				mutated.CgroupID = "wefty-cgroup-someone-elses"
				return mutated
			}()},
			disposition: attemptOwnershipQuarantine,
			reason:      AttemptOwnershipQuarantineInvalidRecord,
		},
		{
			name:        "an incomplete authority tuple is quarantined",
			record:      durableAttemptOwnership{Version: durableAttemptOwnershipVersion, Resources: live},
			disposition: attemptOwnershipQuarantine,
			reason:      AttemptOwnershipQuarantineInvalidRecord,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			disposition, reason := attemptOwnershipDispositionFor(test.record, name, authority, sweep)
			if disposition != test.disposition {
				t.Fatalf("disposition = %q, want %q (reason %q)", disposition, test.disposition, reason)
			}
			if reason != test.reason {
				t.Fatalf("reason = %q, want %q", reason, test.reason)
			}
		})
	}
}

// A version bump is the routine upgrade/rollback path, not corruption. It must
// not reach the quarantine arm for any otherwise-valid record.
func TestAnUnknownRecordVersionNeverQuarantines(t *testing.T) {
	authority := testAuthority()
	sweep := ownershipTestResources(t, authority, "")
	name := sweep.ContainerID + ".json"
	for _, resources := range []ResourceIdentity{sweep, ownershipTestResources(t, authority, "job-1:handoff-owner")} {
		for _, version := range []int{durableAttemptOwnershipVersion + 1, durableAttemptOwnershipVersion + 7, 0} {
			record := durableAttemptOwnership{Version: version, Authority: authority, Resources: resources}
			if disposition, reason := attemptOwnershipDispositionFor(record, name, authority, sweep); disposition != attemptOwnershipDefer {
				t.Fatalf("version %d = %q (reason %q), want the record left alone", version, disposition, reason)
			}
		}
	}
}

// A record whose authority tuple differs while its file name still matches
// cannot be produced by hashing, so the authority-mismatch arm exists only for
// a record whose name was tampered with. Prove the arm is reachable and typed.
func TestReadoptionRefusesAForeignAuthorityUnderAMatchingName(t *testing.T) {
	authority := testAuthority()
	foreign := authority
	foreign.NodeID = "node-2"
	foreignResources := ownershipTestResources(t, foreign, "")
	record := durableAttemptOwnership{Version: durableAttemptOwnershipVersion, Authority: foreign, Resources: foreignResources}
	// The record is internally consistent and correctly named for `foreign`,
	// but the sweep proved `authority` from the container's own labels.
	disposition, reason := attemptOwnershipDispositionFor(record, foreignResources.ContainerID+".json", authority, ownershipTestResources(t, authority, ""))
	if disposition != attemptOwnershipQuarantine || reason != AttemptOwnershipQuarantineAuthorityMismatch {
		t.Fatalf("foreign authority under a matching name = %q/%q", disposition, reason)
	}
}
