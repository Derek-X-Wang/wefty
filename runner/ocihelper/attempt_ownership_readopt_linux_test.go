//go:build linux

package ocihelper

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

// liveAttemptResources is the identity the authenticated Run path retains: the
// authority-derived names plus the handoff volume directory derived from the
// stable owner key, which is what the live record on disk carries.
func liveAttemptResources(t *testing.T, authority AttemptAuthority, ownerKey string) ResourceIdentity {
	t.Helper()
	resources, err := DeterministicResourceIdentity(authority)
	if err != nil {
		t.Fatal(err)
	}
	handoff, err := DeterministicHandoffVolumeDirectory(ownerKey)
	if err != nil {
		t.Fatal(err)
	}
	if handoff == resources.HandoffVolumeDirectory {
		t.Fatalf("fixture owner key collides with the authority digest; the test would prove nothing")
	}
	resources.HandoffVolumeDirectory = handoff
	return resources
}

// bootSweepResources is all the boot sweep can reconstruct: labelled containerd
// metadata gives it the authority tuple and nothing else.
func bootSweepResources(t *testing.T, authority AttemptAuthority) ResourceIdentity {
	t.Helper()
	resources, err := DeterministicResourceIdentity(authority)
	if err != nil {
		t.Fatal(err)
	}
	return resources
}

func readOwnershipRecord(t *testing.T, engine *ContainerdEngine, resources ResourceIdentity) durableAttemptOwnership {
	t.Helper()
	payload, err := os.ReadFile(engine.attemptOwnershipPath(resources))
	if err != nil {
		t.Fatal(err)
	}
	var record durableAttemptOwnership
	if err := json.Unmarshal(payload, &record); err != nil {
		t.Fatal(err)
	}
	return record
}

// A helper lost while an Attempt was RUNNING left a durable ownership record
// carrying the owner-key-derived handoff volume directory. The restarted
// helper's boot sweep has only labelled containerd metadata, so it can derive
// every name except that one. It used to compare all of them, never match, and
// crash-loop the unit forever (#412). It must re-adopt the record instead.
func TestBootSweepReadoptsOwnershipRecordFromAPreviousHelperGeneration(t *testing.T) {
	runtimeRoot := t.TempDir()
	authority := testAuthority()
	live := liveAttemptResources(t, authority, "job-1:handoff-owner")

	// Generation one: the Run path publishes the record exactly as production
	// does, at runner/ocihelper/containerd_engine_linux.go's Run call site.
	first := &ContainerdEngine{config: NativeEngineConfig{RuntimeRoot: runtimeRoot}}
	if err := first.ensureAttemptOwnershipRecord(authority, live); err != nil {
		t.Fatal(err)
	}
	published := readOwnershipRecord(t, first, live)
	if published.Resources.HandoffVolumeDirectory != live.HandoffVolumeDirectory {
		t.Fatalf("fixture did not write the live handoff volume directory: %+v", published.Resources)
	}

	// Generation two: a brand new engine over the same durable root, holding
	// only what container labels prove.
	second := &ContainerdEngine{config: NativeEngineConfig{RuntimeRoot: runtimeRoot}}
	if err := second.ensureAttemptOwnershipRecord(authority, bootSweepResources(t, authority)); err != nil {
		t.Fatalf("restarted helper refused to re-adopt its own durable record: %v", err)
	}

	readopted := readOwnershipRecord(t, second, live)
	if !reflect.DeepEqual(readopted, published) {
		t.Fatalf("re-adoption rewrote the record: got %+v, want %+v", readopted, published)
	}
	quarantines, err := second.attemptOwnershipQuarantines()
	if err != nil || len(quarantines) != 0 {
		t.Fatalf("re-adoption quarantined a reconcilable record: %+v err=%v", quarantines, err)
	}
	records, err := second.loadAttemptOwnershipRecords()
	if err != nil || len(records) != 1 || records[authority.key()].Resources.HandoffVolumeDirectory != live.HandoffVolumeDirectory {
		t.Fatalf("re-adopted record did not bind its resources: %+v err=%v", records, err)
	}
}

// Re-adoption must be verbatim: the previous generation's retention receipts
// are facts this generation cannot reconstruct, and silently dropping them
// would convert retained state into unaccounted residue.
func TestReadoptionPreservesRetentionReceiptsItCannotReconstruct(t *testing.T) {
	runtimeRoot := t.TempDir()
	authority := testAuthority()
	live := liveAttemptResources(t, authority, "job-1:handoff-owner")
	deadline := time.Date(2026, 9, 11, 21, 6, 51, 0, time.UTC)
	published := durableAttemptOwnership{
		Version: durableAttemptOwnershipVersion, Authority: authority, Resources: live,
		Retentions: []DurableRetention{{
			Class: RemovalResourceLogSegments, ID: live.LogSegmentDirectory, AttemptID: authority.AttemptID,
			Reason: DurableRetentionReasonLogSpoolSealing, Bound: 5 * time.Minute, Deadline: deadline,
		}},
	}
	first := &ContainerdEngine{config: NativeEngineConfig{RuntimeRoot: runtimeRoot}}
	if err := first.writeAttemptOwnershipRecord(published); err != nil {
		t.Fatal(err)
	}

	second := &ContainerdEngine{config: NativeEngineConfig{RuntimeRoot: runtimeRoot}}
	if err := second.ensureAttemptOwnershipRecord(authority, bootSweepResources(t, authority)); err != nil {
		t.Fatal(err)
	}
	readopted := readOwnershipRecord(t, second, live)
	if len(readopted.Retentions) != 1 || readopted.Retentions[0].ID != live.LogSegmentDirectory ||
		!readopted.Retentions[0].Deadline.Equal(deadline) {
		t.Fatalf("re-adoption dropped the previous generation's retention receipts: %+v", readopted.Retentions)
	}
}

// The record's file name is the digest of its complete authority tuple, so a
// matching name over a non-matching tuple is corruption, not a superseded
// writer. Corruption is quarantined -- moved aside with a typed receipt -- and
// helper startup proceeds. It is never deleted, and never a startup failure.
func TestUnreconcilableOwnershipRecordIsQuarantinedAndStartupProceeds(t *testing.T) {
	authority := testAuthority()
	live := liveAttemptResources(t, authority, "job-1:handoff-owner")
	foreign := authority
	foreign.AttemptID = "attempt-from-another-fence"

	for _, test := range []struct {
		name    string
		payload []byte
		reason  AttemptOwnershipQuarantineReason
	}{
		{name: "unparseable bytes", payload: []byte("{not json"), reason: AttemptOwnershipQuarantineInvalidRecord},
		{
			name: "authority tuple contradicts its own file name",
			payload: mustMarshal(t, durableAttemptOwnership{
				Version: durableAttemptOwnershipVersion, Authority: foreign, Resources: live,
			}),
			reason: AttemptOwnershipQuarantineInvalidRecord,
		},
		{
			name: "unknown record version",
			payload: mustMarshal(t, durableAttemptOwnership{
				Version: durableAttemptOwnershipVersion + 1, Authority: authority, Resources: live,
			}),
			reason: AttemptOwnershipQuarantineUnknownVersion,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			runtimeRoot := t.TempDir()
			clock := newManualClock(time.Unix(1_700_000_000, 0))
			engine := &ContainerdEngine{config: NativeEngineConfig{RuntimeRoot: runtimeRoot, Clock: clock}}
			if err := os.MkdirAll(engine.attemptOwnershipRoot(), 0o700); err != nil {
				t.Fatal(err)
			}
			path := engine.attemptOwnershipPath(live)
			if err := os.WriteFile(path, test.payload, 0o600); err != nil {
				t.Fatal(err)
			}

			if err := engine.ensureAttemptOwnershipRecord(authority, bootSweepResources(t, authority)); err != nil {
				t.Fatalf("an unreconcilable record wedged helper startup instead of quarantining: %v", err)
			}

			quarantines, err := engine.attemptOwnershipQuarantines()
			if err != nil {
				t.Fatal(err)
			}
			if len(quarantines) != 1 {
				t.Fatalf("quarantine receipts = %+v, want exactly one", quarantines)
			}
			receipt := quarantines[0]
			if receipt.Kind != AttemptOwnershipQuarantineKind || receipt.Reason != test.reason ||
				receipt.Record != filepath.Base(path) || receipt.ReceiptID == "" ||
				!receipt.QuarantinedAt.Equal(clock.Now().UTC()) {
				t.Fatalf("quarantine receipt = %+v, want a typed %s receipt", receipt, test.reason)
			}

			// The bytes are retained, not deleted: they are operator-owned.
			quarantined, err := os.ReadFile(filepath.Join(engine.attemptOwnershipQuarantineRoot(), receipt.ReceiptID, attemptOwnershipQuarantinedRecordName))
			if err != nil {
				t.Fatalf("quarantine deleted the record it could not reconcile: %v", err)
			}
			if string(quarantined) != string(test.payload) {
				t.Fatalf("quarantined bytes = %q, want the record verbatim", quarantined)
			}

			// The sweep may now publish the record for the authority it proved
			// from labels, so the namespace it is about to clear stays bound.
			republished := readOwnershipRecord(t, engine, live)
			if republished.Authority != authority || republished.Version != durableAttemptOwnershipVersion {
				t.Fatalf("startup did not republish a record for the proven authority: %+v", republished)
			}
		})
	}
}

// The quarantine is the doctor's business: the helper is serving while
// operator-owned durable state waits on a human, and that must be visible.
func TestDoctorStatusSurfacesAttemptOwnershipQuarantines(t *testing.T) {
	authority := testAuthority()
	live := liveAttemptResources(t, authority, "job-1:handoff-owner")
	engine := &ContainerdEngine{config: NativeEngineConfig{RuntimeRoot: t.TempDir(), Clock: newManualClock(time.Unix(1_700_000_000, 0))}}
	if err := os.MkdirAll(engine.attemptOwnershipRoot(), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(engine.attemptOwnershipPath(live), []byte("{not json"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := engine.ensureAttemptOwnershipRecord(authority, bootSweepResources(t, authority)); err != nil {
		t.Fatal(err)
	}
	quarantines, err := engine.attemptOwnershipQuarantines()
	if err != nil || len(quarantines) != 1 || quarantines[0].Reason != AttemptOwnershipQuarantineInvalidRecord {
		t.Fatalf("doctor-facing quarantine read = %+v err=%v", quarantines, err)
	}
}

// A record whose authority belongs to a previous helper generation and whose
// resources are all gone is stale, not adoptable: the sweep removes it. A
// record whose resources still exist stays bound.
func TestStaleQuiescentOwnershipRecordIsSweptWhileBoundResourcesAreKept(t *testing.T) {
	runtimeRoot := t.TempDir()
	stale := testAuthority()
	stale.BootSessionID = "boot-previous-generation"
	staleResources := liveAttemptResources(t, stale, "job-1:handoff-owner")
	bound := testAuthority()
	bound.AttemptID = "attempt-still-bound"
	boundResources := liveAttemptResources(t, bound, "job-1:handoff-owner")

	engine := &ContainerdEngine{config: NativeEngineConfig{RuntimeRoot: runtimeRoot}}
	for _, resources := range []ResourceIdentity{staleResources, boundResources} {
		authority := stale
		if resources.ContainerID == boundResources.ContainerID {
			authority = bound
		}
		if err := engine.ensureAttemptOwnershipRecord(authority, resources); err != nil {
			t.Fatal(err)
		}
	}

	records, err := engine.loadAttemptOwnershipRecords()
	if err != nil || len(records) != 2 {
		t.Fatalf("records = %+v err=%v", records, err)
	}
	remaining := ResourceInventory{Containers: []string{boundResources.ContainerID}}
	if err := engine.removeQuiescentAttemptOwnershipRecords(records, remaining); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(engine.attemptOwnershipPath(staleResources)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("stale quiescent record survived the sweep: %v", err)
	}
	if _, err := os.Stat(engine.attemptOwnershipPath(boundResources)); err != nil {
		t.Fatalf("sweep removed a record still bound to a live resource: %v", err)
	}
}

// The Run path keeps its exact identity check: a caller whose authority-derived
// names disagree with its own authority is refused, re-adoption or not.
func TestOwnershipPublicationStillRefusesNamesThatContradictTheAuthority(t *testing.T) {
	authority := testAuthority()
	resources := liveAttemptResources(t, authority, "job-1:handoff-owner")
	resources.CgroupID = "wefty-cgroup-someone-elses"
	engine := &ContainerdEngine{config: NativeEngineConfig{RuntimeRoot: t.TempDir()}}
	if err := engine.ensureAttemptOwnershipRecord(authority, resources); err == nil {
		t.Fatal("a resource identity that contradicts its fenced authority was published")
	}
}

func mustMarshal(t *testing.T, value any) []byte {
	t.Helper()
	payload, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return append(payload, '\n')
}
