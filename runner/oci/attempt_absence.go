package oci

import (
	"errors"
	"fmt"
	"time"

	"github.com/Derek-X-Wang/wefty/runner/ocihelper"
)

// attemptAbsentFromNamespace proves absence for an attempt the helper refuses
// to Delete because it no longer holds it as live.
//
// `authorizeDelete` answers `unauthorized_attempt` for an attempt whose own
// guardian or deadman already completed it, and nothing the agent does later
// turns that into a positive Delete receipt, so a removal loop that can only
// retry pins its node service slot forever (#450). A refusal is not absence:
// the helper is saying it will not speak for this authority, not that the
// resources are gone.
//
// The helper offers exactly one absence proof to a session holding no live
// attempt -- the read-only namespace inventory, which `MethodVerify`
// authorizes without an authority -- so absence is taken there and narrowed
// with `ocihelper.ProjectAttemptInventory`, the projection the helper's own
// attempt scope applies. Nothing this attempt named may remain as runtime
// residue, and in the plain inventory only the classes the contract keeps past
// one attempt, plus a bounded, unexpired helper retention, may survive (#417,
// and the shape #421 gave the acceptance driver).
//
// now is passed rather than read so a retention deadline is judged against the
// same clock the caller used; the adapter carries no clock of its own.
func attemptAbsentFromNamespace(verification ocihelper.VerifyResponse, authority ocihelper.AttemptAuthority,
	volumes []ocihelper.ManagedVolumeDescriptor, now time.Time) error {
	identity, err := ocihelper.DeterministicResourceIdentity(authority)
	if err != nil {
		return err
	}
	handoff, storage, err := attemptVolumeNames(volumes)
	if err != nil {
		return err
	}
	// A handoff volume is named from the descriptor's owner key, not the
	// attempt digest, so the digest-derived default would match something that
	// can never appear and prove nothing. An attempt that requested none has no
	// handoff resource at all.
	identity.HandoffVolumeDirectory = handoff
	diskName := ""
	if storage != nil {
		if diskName, err = ocihelper.DeterministicComputerDiskName(*storage); err != nil {
			return err
		}
	}
	if residue := ocihelper.ProjectAttemptInventory(verification.RuntimeResidue, identity, diskName); !ocihelper.InventoryEmpty(residue) {
		return fmt.Errorf("attempt runtime residue remains: %+v", residue)
	}
	survivors := ocihelper.ProjectAttemptInventory(verification.Inventory, identity, diskName)
	// The classes the helper's own contract keeps past one attempt: a service
	// data volume and its owner record belong to the job, a handoff volume is
	// retained for its owner to collect, and a Computer disk is durable Storage
	// the helper projects out of runtime absence while a manifest backs it.
	// Their presence here is the contract working, and the removal deletes them
	// in its own attested step. They were still required absent from the
	// runtime residue above.
	survivors.ManagedVolumes, survivors.ManagedVolumeRecords = nil, nil
	survivors.ComputerDiskImages, survivors.ComputerDiskAllocations, survivors.ComputerDiskQuotas = nil, nil, nil
	survivors.ComputerDiskManifests, survivors.ComputerDiskMounts, survivors.ComputerDiskLoops = nil, nil, nil
	survivors.ComputerAttachments, survivors.ComputerResetManifests, survivors.ComputerQuarantines = nil, nil, nil
	survivors.ComputerStorageDeferred, survivors.ComputerStorageQuarantined = nil, nil
	// Log sealing and cgroup reaping are the only two transient classes the
	// helper may still be finishing, and only under an explicit bounded
	// retention that names this attempt and has not expired. Anything else --
	// including a class added to the helper later, which lands in no branch
	// here -- fails the proof rather than dropping silently out of it.
	survivors.LogSegments = unexplainedSurvivors(survivors.LogSegments, verification.DurableRetentions,
		ocihelper.RemovalResourceLogSegments, ocihelper.DurableRetentionReasonLogSpoolSealing, authority.AttemptID, now)
	survivors.Cgroups = unexplainedSurvivors(survivors.Cgroups, verification.DurableRetentions,
		ocihelper.RemovalResourceCgroup, ocihelper.DurableRetentionReasonCgroupReaping, authority.AttemptID, now)
	if !ocihelper.InventoryEmpty(survivors) {
		return fmt.Errorf("attempt resources survived in the namespace inventory: %+v", survivors)
	}
	return nil
}

// unexplainedSurvivors drops the names a bounded helper retention accounts for
// and returns whatever is left.
func unexplainedSurvivors(names []string, retentions []ocihelper.DurableRetention,
	class ocihelper.RemovalResourceClass, reason ocihelper.DurableRetentionReason, attemptID string, now time.Time) []string {
	var remaining []string
	for _, name := range names {
		if !boundDurableRetention(retentions, class, name, reason, attemptID, now) {
			remaining = append(remaining, name)
		}
	}
	return remaining
}

// attemptVolumeNames resolves the two identities the attempt authority cannot
// name on its own. The helper names a handoff volume from the descriptor's
// OwnerKey rather than the attempt digest, and a Computer disk from its
// Storage reference, so an attempt-derived name would match something that can
// never appear and prove nothing. A descriptor this projection cannot name
// fails the proof rather than dropping that resource out of it.
func attemptVolumeNames(volumes []ocihelper.ManagedVolumeDescriptor) (string, *ocihelper.ComputerStorageReference, error) {
	handoff := ""
	var storage *ocihelper.ComputerStorageReference
	for _, volume := range volumes {
		switch volume.Kind {
		case ocihelper.ManagedVolumeHandoff:
			name, err := ocihelper.DeterministicHandoffVolumeDirectory(volume.OwnerKey)
			if err != nil {
				return "", nil, fmt.Errorf("name the handoff volume this attempt requested: %w", err)
			}
			handoff = name
		case ocihelper.ManagedVolumeComputerDisk:
			if volume.ComputerStorage == nil {
				return "", nil, errors.New("this attempt requested a Computer disk with no Storage identity, so its resources cannot be named")
			}
			reference := *volume.ComputerStorage
			storage = &reference
		case ocihelper.ManagedVolumeServiceData:
			// Named from the stable job identity, which the attempt authority
			// already carries into DeterministicResourceIdentity.
		default:
			return "", nil, fmt.Errorf(
				"this attempt requested managed volume kind %q, which cannot be named in the namespace inventory",
				volume.Kind)
		}
	}
	return handoff, storage, nil
}

// boundDurableRetention is the helper's own binding shape, checked here rather
// than assumed: a retention that does not name this attempt, this class, this
// resource and a closed bounded deadline explains nothing. An expired deadline
// explains nothing either -- the bound is the whole reason a survivor is
// tolerated, so a retention past it is a resource the helper failed to release.
func boundDurableRetention(retentions []ocihelper.DurableRetention, class ocihelper.RemovalResourceClass,
	name string, reason ocihelper.DurableRetentionReason, attemptID string, now time.Time) bool {
	for _, retention := range retentions {
		if retention.Class == class && retention.ID == name && retention.Reason == reason &&
			retention.AttemptID == attemptID && retention.Owner == ocihelper.DurableRetentionOwnerOCIHelper &&
			retention.Bound > 0 && !retention.RecordedAt.IsZero() &&
			retention.Deadline.Equal(retention.RecordedAt.Add(retention.Bound)) &&
			retention.Deadline.After(now) {
			return true
		}
	}
	return false
}
