package ocihelper

import (
	"path/filepath"
	"strings"
)

// ProjectAttemptInventory narrows one namespace inventory to the resources
// carrying a single attempt's deterministic names. It is the projection the
// helper applies for an attempt-scoped Verify and for its own absence checks,
// and it lives here rather than beside the containerd engine because the agent
// needs the identical rule when it proves one attempt absent from a read-only
// namespace inventory -- the only absence proof a helper session holding no
// live attempt will authorize. One definition means those two readings cannot
// silently disagree about which list a resource class lives in.
//
// computerDiskName is the deterministic disk name of the attempt's Computer
// Storage, or empty when the attempt had none; a disk is never named by the
// attempt digest, so it cannot be derived from resources alone.
func ProjectAttemptInventory(inventory ResourceInventory, resources ResourceIdentity, computerDiskName string) ResourceInventory {
	filtered := ResourceInventory{Leases: []string{}, Snapshots: []string{}, Containers: []string{}, Tasks: []string{}, Shims: []string{}, Cgroups: []string{}, LogSegments: []string{}, ImageSpools: []string{}, ManagedVolumes: []string{}, ManagedVolumeRecords: []string{}, ComputerDiskImages: []string{}, ComputerDiskAllocations: []string{}, ComputerDiskQuotas: []string{}, ComputerDiskManifests: []string{}, ComputerDiskMounts: []string{}, ComputerDiskLoops: []string{}, ComputerAttachments: []string{}, ComputerResetManifests: []string{}, ComputerQuarantines: []string{}, ComputerStorageDeferred: []ComputerStorageRecoveryInventoryEntry{}, ComputerStorageQuarantined: []ComputerStorageRecoveryInventoryEntry{}, ComputerDiskAnomalies: []string{}}
	for _, pair := range []struct {
		values []string
		target string
		output *[]string
	}{{inventory.Leases, resources.LeaseID, &filtered.Leases}, {inventory.Snapshots, resources.SnapshotID, &filtered.Snapshots}, {inventory.Containers, resources.ContainerID, &filtered.Containers}, {inventory.Tasks, resources.ContainerID, &filtered.Tasks}, {inventory.Shims, resources.ContainerID, &filtered.Shims}, {inventory.LogSegments, resources.LogSegmentDirectory, &filtered.LogSegments}, {inventory.ManagedVolumes, resources.HandoffVolumeDirectory, &filtered.ManagedVolumes}, {inventory.ManagedVolumes, resources.ServiceVolumeDirectory, &filtered.ManagedVolumes}, {inventory.ManagedVolumeRecords, resources.ServiceVolumeOwnerRecord, &filtered.ManagedVolumeRecords}} {
		for _, value := range pair.values {
			if value == pair.target {
				*pair.output = append(*pair.output, value)
			}
		}
	}
	for _, value := range inventory.Cgroups {
		if logical, managed := cgroupAttemptResourceID(filepath.Base(value)); managed && logical == resources.CgroupID {
			filtered.Cgroups = append(filtered.Cgroups, value)
		}
	}
	if computerDiskName != "" {
		for _, pair := range []struct {
			values []string
			target string
			output *[]string
		}{{inventory.ComputerDiskImages, computerDiskName, &filtered.ComputerDiskImages}, {inventory.ComputerDiskAllocations, computerDiskName, &filtered.ComputerDiskAllocations}, {inventory.ComputerDiskQuotas, computerDiskName, &filtered.ComputerDiskQuotas}, {inventory.ComputerDiskManifests, computerDiskName, &filtered.ComputerDiskManifests}, {inventory.ComputerDiskMounts, computerDiskName, &filtered.ComputerDiskMounts}, {inventory.ComputerDiskLoops, computerDiskName, &filtered.ComputerDiskLoops}, {inventory.ComputerAttachments, computerDiskName, &filtered.ComputerAttachments}, {inventory.ComputerResetManifests, computerDiskName, &filtered.ComputerResetManifests}, {inventory.ComputerQuarantines, computerDiskName, &filtered.ComputerQuarantines}} {
			for _, value := range pair.values {
				if value == pair.target {
					*pair.output = append(*pair.output, value)
				}
			}
		}
		for _, entry := range inventory.ComputerStorageDeferred {
			if entry.DiskName == computerDiskName {
				filtered.ComputerStorageDeferred = append(filtered.ComputerStorageDeferred, entry)
			}
		}
		for _, entry := range inventory.ComputerStorageQuarantined {
			if entry.DiskName == computerDiskName {
				filtered.ComputerStorageQuarantined = append(filtered.ComputerStorageQuarantined, entry)
			}
		}
	}
	return filtered
}

func cgroupAttemptResourceID(name string) (string, bool) {
	logical := name
	if strings.HasSuffix(logical, ".scope") {
		logical = strings.TrimSuffix(logical, ".scope")
	}
	return logical, managedAttemptResourceName(logical, "wefty-cgroup-")
}

func managedAttemptResourceName(name, prefix string) bool {
	suffix, found := strings.CutPrefix(name, prefix)
	if !found || len(suffix) != 32 {
		return false
	}
	for _, character := range suffix {
		if character < '0' || character > '9' {
			if character < 'a' || character > 'f' {
				return false
			}
		}
	}
	return true
}
