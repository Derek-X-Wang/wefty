package oci

import (
	"context"
	"errors"
	"fmt"
	"path"
	"strings"

	workloadrunner "github.com/Derek-X-Wang/wefty/runner"
	"github.com/Derek-X-Wang/wefty/runner/ocihelper"
)

// verifyNonLiveAttemptAbsent proves absence for an attempt the helper refuses
// to Delete because it no longer holds it as live.
//
// `authorizeDelete` answers `unauthorized_attempt` for an attempt whose own
// guardian or deadman already completed it, and nothing the agent does later
// turns that into a positive Delete receipt, so a removal loop that can only
// retry pins its node service slot forever (#450). A refusal is not absence:
// the helper is saying it will not speak for this authority, not that the
// resources are gone.
//
// The helper offers exactly one absence proof to a session with no live
// attempt -- the read-only namespace inventory, which `MethodVerify`
// authorizes without an authority -- so absence is taken there and projected
// onto this attempt's deterministic resource names exactly as the helper's own
// attempt scope projects it, with every one of those names required absent
// (#417, and the same shape #421 gave the acceptance driver).
func (adapter *Adapter) verifyNonLiveAttemptAbsent(ctx context.Context, session *ocihelper.Session,
	authority workloadrunner.AttemptAuthority, volumes []ocihelper.ManagedVolumeDescriptor) error {
	if session == nil {
		return errors.New("OCI attempt absence verification requires a helper session")
	}
	verification, err := session.Verify(ctx, ocihelper.VerifyRequest{Scope: ocihelper.VerifyNamespaceReadOnly})
	if err != nil {
		return fmt.Errorf("verify attempt absence after a non-live Delete refusal: %w", err)
	}
	return attemptAbsentFromNamespace(verification, HelperAuthority(authority), volumes)
}

// durableBeyondAttempt are the resource classes the helper's own contract
// keeps past one attempt: a service data volume and its owner record belong to
// the job, a handoff volume is retained for its owner to collect, and a
// Computer disk is durable Storage the helper projects out of runtime absence
// while a manifest backs it. Their presence in the namespace inventory after
// the attempt ended is the contract working, not residue -- they must still
// never appear as runtime residue, which the residue check asserts for every
// class alike. Later deletion of those durable classes is the removal's own
// attested step, not this proof.
var durableBeyondAttempt = map[ocihelper.RemovalResourceClass]bool{
	ocihelper.RemovalResourceHandoffVolume:          true,
	ocihelper.RemovalResourceServiceData:            true,
	ocihelper.RemovalResourceServiceDataRecord:      true,
	ocihelper.RemovalResourceComputerDiskImage:      true,
	ocihelper.RemovalResourceComputerDiskAllocation: true,
	ocihelper.RemovalResourceComputerDiskQuota:      true,
	ocihelper.RemovalResourceComputerDiskManifest:   true,
	ocihelper.RemovalResourceComputerDiskMount:      true,
	ocihelper.RemovalResourceComputerDiskLoop:       true,
	ocihelper.RemovalResourceComputerAttachment:     true,
	ocihelper.RemovalResourceComputerResetManifest:  true,
	ocihelper.RemovalResourceComputerQuarantine:     true,
}

// retainableAfterAttempt are the only two transient classes the helper may
// still show once an attempt has ended, and only under an explicit bounded
// durable retention naming that attempt. Anything else surviving fails the
// proof outright.
var retainableAfterAttempt = map[ocihelper.RemovalResourceClass]ocihelper.DurableRetentionReason{
	ocihelper.RemovalResourceLogSegments: ocihelper.DurableRetentionReasonLogSpoolSealing,
	ocihelper.RemovalResourceCgroup:      ocihelper.DurableRetentionReasonCgroupReaping,
}

// attemptAbsentFromNamespace asserts every resource the helper's own closed
// removal registry names for this attempt is gone from the namespace the
// verification observed. Driving the assertion from ExpectedRemovalResources
// rather than a hand-written list is deliberate: a resource class added to the
// helper cannot quietly drop out of the proof, because attemptInventoryEntries
// refuses a class it does not know how to look up.
func attemptAbsentFromNamespace(verification ocihelper.VerifyResponse, authority ocihelper.AttemptAuthority,
	volumes []ocihelper.ManagedVolumeDescriptor) error {
	identity, err := ocihelper.DeterministicResourceIdentity(authority)
	if err != nil {
		return err
	}
	handoff, storage, err := attemptVolumeNames(volumes)
	if err != nil {
		return err
	}
	resources := ocihelper.ExpectedRemovalResources(identity, handoff, storage)
	if len(resources) == 0 {
		return errors.New("the helper's removal registry named no resources for this attempt")
	}
	for _, resource := range resources {
		residue, err := attemptInventoryEntries(verification.RuntimeResidue, resource)
		if err != nil {
			return err
		}
		if len(residue) != 0 {
			return fmt.Errorf("attempt runtime residue remains: %s %v", resource.Class, residue)
		}
		observed, err := attemptInventoryEntries(verification.Inventory, resource)
		if err != nil {
			return err
		}
		if len(observed) == 0 || durableBeyondAttempt[resource.Class] {
			continue
		}
		reason, retainable := retainableAfterAttempt[resource.Class]
		if !retainable {
			return fmt.Errorf("%s %v survived the attempt in the namespace inventory", resource.Class, observed)
		}
		for _, name := range observed {
			if !boundDurableRetention(verification.DurableRetentions, resource.Class, name, reason, authority.AttemptID) {
				return fmt.Errorf("%s %q survived without a bounded helper retention naming attempt %s",
					resource.Class, name, authority.AttemptID)
			}
		}
	}
	return nil
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

// attemptInventoryEntries is the projection the helper applies for an
// attempt-scoped Verify: the entries of one inventory list carrying this
// attempt's deterministic name.
func attemptInventoryEntries(inventory ocihelper.ResourceInventory, resource ocihelper.RemovalResource) ([]string, error) {
	var candidates []string
	switch resource.Class {
	case ocihelper.RemovalResourceLease:
		candidates = inventory.Leases
	case ocihelper.RemovalResourceSnapshot:
		candidates = inventory.Snapshots
	case ocihelper.RemovalResourceContainer:
		candidates = inventory.Containers
	case ocihelper.RemovalResourceTask:
		candidates = inventory.Tasks
	case ocihelper.RemovalResourceShim:
		candidates = inventory.Shims
	case ocihelper.RemovalResourceLogSegments:
		candidates = inventory.LogSegments
	case ocihelper.RemovalResourceHandoffVolume, ocihelper.RemovalResourceServiceData:
		candidates = inventory.ManagedVolumes
	case ocihelper.RemovalResourceServiceDataRecord:
		candidates = inventory.ManagedVolumeRecords
	// The helper matches every Computer disk class against the one disk name
	// its Storage reference produces, so these are plain equality lookups.
	case ocihelper.RemovalResourceComputerDiskImage:
		candidates = inventory.ComputerDiskImages
	case ocihelper.RemovalResourceComputerDiskAllocation:
		candidates = inventory.ComputerDiskAllocations
	case ocihelper.RemovalResourceComputerDiskQuota:
		candidates = inventory.ComputerDiskQuotas
	case ocihelper.RemovalResourceComputerDiskManifest:
		candidates = inventory.ComputerDiskManifests
	case ocihelper.RemovalResourceComputerDiskMount:
		candidates = inventory.ComputerDiskMounts
	case ocihelper.RemovalResourceComputerDiskLoop:
		candidates = inventory.ComputerDiskLoops
	case ocihelper.RemovalResourceComputerAttachment:
		candidates = inventory.ComputerAttachments
	case ocihelper.RemovalResourceComputerResetManifest:
		candidates = inventory.ComputerResetManifests
	case ocihelper.RemovalResourceComputerQuarantine:
		candidates = inventory.ComputerQuarantines
	case ocihelper.RemovalResourceCgroup:
		// Cgroup inventory entries are guest paths and the helper matches their
		// base name with any .scope suffix trimmed. path, not path/filepath:
		// these are Linux paths that may be observed from a macOS host.
		matched := []string{}
		for _, value := range inventory.Cgroups {
			if strings.TrimSuffix(path.Base(value), ".scope") == resource.ID {
				matched = append(matched, value)
			}
		}
		return matched, nil
	default:
		return nil, fmt.Errorf(
			"the helper's removal registry names resource class %q, which cannot be looked up in the namespace inventory",
			resource.Class)
	}
	matched := []string{}
	for _, value := range candidates {
		if value == resource.ID {
			matched = append(matched, value)
		}
	}
	return matched, nil
}

// boundDurableRetention is the helper's own binding shape, checked here rather
// than assumed: a retention that does not name this attempt, this class, this
// resource and a closed bounded deadline explains nothing.
func boundDurableRetention(retentions []ocihelper.DurableRetention, class ocihelper.RemovalResourceClass,
	name string, reason ocihelper.DurableRetentionReason, attemptID string) bool {
	for _, retention := range retentions {
		if retention.Class == class && retention.ID == name && retention.Reason == reason &&
			retention.AttemptID == attemptID && retention.Owner == ocihelper.DurableRetentionOwnerOCIHelper &&
			retention.Bound > 0 && !retention.RecordedAt.IsZero() &&
			retention.Deadline.Equal(retention.RecordedAt.Add(retention.Bound)) {
			return true
		}
	}
	return false
}
