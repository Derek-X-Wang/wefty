//go:build linux

package ocihelper

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

const computerStorageResetManifestVersion = 1

type computerStorageResetPhase string

const (
	computerStorageResetPrepared    computerStorageResetPhase = "prepared"
	computerStorageResetQuarantined computerStorageResetPhase = "quarantined"
	computerStorageResetDeleted     computerStorageResetPhase = "deleted"
	computerStorageResetVerified    computerStorageResetPhase = "verified"
)

type computerStorageResetManifest struct {
	Version        int                           `json:"version"`
	Storage        ComputerStorageReference      `json:"storage"`
	NewGeneration  int64                         `json:"new_generation"`
	Authority      ComputerStorageResetAuthority `json:"authority"`
	QuarantineName string                        `json:"quarantine_name"`
	Phase          computerStorageResetPhase     `json:"phase"`
	Receipt        *ComputerStorageResetReceipt  `json:"receipt,omitempty"`
}

func sameComputerStorageResetAuthority(left, right ComputerStorageResetAuthority) bool {
	return left.NodeID == right.NodeID && left.JobID == right.JobID &&
		left.IntentRevision == right.IntentRevision && left.CleanupFence == right.CleanupFence
}

func validResetDetachmentEvidence(evidence *computerDiskEvidence, storage ComputerStorageReference, authority ComputerStorageResetAuthority) bool {
	if evidence == nil || evidence.ReceiptID == "" || evidence.NodeID != authority.NodeID ||
		evidence.JobID != authority.JobID || evidence.ComputerID != storage.ComputerID ||
		evidence.StorageID != storage.StorageID || evidence.StorageGeneration != storage.StorageGeneration {
		return false
	}
	switch evidence.Kind {
	case computerDiskReapReceipt:
		return evidence.BootSessionID == authority.BootSessionID && evidence.SweepEpoch == ""
	case computerDiskSweepReceipt:
		return evidence.BootSessionID != authority.BootSessionID && evidence.SweepEpoch != ""
	default:
		return false
	}
}

func readComputerStorageResetManifest(path string) (computerStorageResetManifest, bool, error) {
	payload, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return computerStorageResetManifest{}, false, nil
	}
	if err != nil {
		return computerStorageResetManifest{}, false, err
	}
	var manifest computerStorageResetManifest
	if err := json.Unmarshal(payload, &manifest); err != nil || manifest.Version != computerStorageResetManifestVersion {
		return computerStorageResetManifest{}, false, errors.New("Computer Storage reset manifest is invalid")
	}
	return manifest, true, nil
}

func writeComputerStorageResetManifest(root, path string, manifest computerStorageResetManifest) error {
	payload, err := json.Marshal(manifest)
	if err != nil {
		return err
	}
	payload = append(payload, '\n')
	temporary, err := os.CreateTemp(root, ".storage-reset.tmp-")
	if err != nil {
		return err
	}
	name := temporary.Name()
	defer os.Remove(name)
	writeErr := temporary.Chmod(0o600)
	if writeErr == nil {
		_, writeErr = temporary.Write(payload)
	}
	if writeErr == nil {
		writeErr = temporary.Sync()
	}
	writeErr = errors.Join(writeErr, temporary.Close())
	if writeErr != nil {
		return writeErr
	}
	if err := os.Rename(name, path); err != nil {
		return err
	}
	return syncDirectory(root)
}

func (engine *ContainerdEngine) storageResetCheckpoint(phase computerStorageResetPhase) error {
	if engine.storageResetHook == nil {
		return nil
	}
	return engine.storageResetHook(phase)
}

func (engine *ContainerdEngine) verifyResetGenerationAbsent(diskRoot, quarantine, mountPath string) error {
	for _, path := range []string{diskRoot, quarantine, mountPath} {
		if _, err := os.Lstat(path); err == nil {
			return fmt.Errorf("Computer Storage reset left %s", filepath.Base(path))
		} else if !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	for _, root := range []string{diskRoot, quarantine} {
		loops, err := engine.computerDiskSystem().loopsForRoot(root)
		if err != nil {
			return err
		}
		if len(loops) != 0 {
			return errors.New("Computer Storage reset left a loop attachment")
		}
	}
	return nil
}

func (engine *ContainerdEngine) ResetComputerStorage(_ context.Context, request ResetComputerStorageRequest) (ResetComputerStorageResponse, error) {
	engine.storageResetMu.Lock()
	defer engine.storageResetMu.Unlock()
	if request.Storage.DiskBytes <= 0 || request.Storage.IntentRevision != request.Authority.IntentRevision || request.NewGeneration != request.Storage.StorageGeneration+1 ||
		request.Authority.NodeID == "" || request.Authority.BootSessionID == "" || request.Authority.JobID == "" ||
		request.Authority.HelperGeneration == 0 || request.Authority.IntentRevision < 1 || request.Authority.CleanupFence == "" {
		return ResetComputerStorageResponse{}, errors.New("Computer Storage reset request is incomplete")
	}
	name, err := deterministicComputerDiskName(request.Storage)
	if err != nil {
		return ResetComputerStorageResponse{}, err
	}
	resetRoot := filepath.Join(engine.config.RuntimeRoot, "computer-storage-resets")
	quarantineRoot := filepath.Join(engine.config.RuntimeRoot, "computer-disk-quarantine")
	if err := os.MkdirAll(resetRoot, 0o700); err != nil {
		return ResetComputerStorageResponse{}, err
	}
	if err := os.MkdirAll(quarantineRoot, 0o700); err != nil {
		return ResetComputerStorageResponse{}, err
	}
	manifestPath := filepath.Join(resetRoot, name+".json")
	quarantineName := name + "-reset-" + fmt.Sprint(request.Authority.IntentRevision)
	manifest, present, err := readComputerStorageResetManifest(manifestPath)
	if err != nil {
		return ResetComputerStorageResponse{}, err
	}
	if present {
		if !sameComputerStorageIdentity(manifest.Storage, request.Storage) || manifest.NewGeneration != request.NewGeneration ||
			!sameComputerStorageResetAuthority(manifest.Authority, request.Authority) || manifest.QuarantineName != quarantineName {
			return ResetComputerStorageResponse{}, errors.New("Computer Storage generation already has a different reset authority")
		}
	} else {
		manifest = computerStorageResetManifest{Version: computerStorageResetManifestVersion, Storage: request.Storage,
			NewGeneration: request.NewGeneration, Authority: request.Authority, QuarantineName: quarantineName,
			Phase: computerStorageResetPrepared}
		diskRoot := filepath.Join(engine.config.RuntimeRoot, "computer-disks", name)
		diskManifest, diskPresent, readErr := readComputerDiskManifest(filepath.Join(diskRoot, "attachment.json"))
		if readErr != nil {
			return ResetComputerStorageResponse{}, readErr
		}
		if diskPresent {
			if !sameComputerStorageIdentity(diskManifest.Storage, request.Storage) || diskManifest.Attached != nil ||
				diskManifest.Pending != nil || !validResetDetachmentEvidence(diskManifest.PreviousDetachment, request.Storage, request.Authority) {
				return ResetComputerStorageResponse{}, errors.New("Computer Storage reset lacks exact detached generation authority")
			}
		} else if _, statErr := os.Lstat(diskRoot); statErr == nil {
			return ResetComputerStorageResponse{}, errors.New("Computer Storage reset found bytes without an authority manifest")
		} else if !errors.Is(statErr, os.ErrNotExist) {
			return ResetComputerStorageResponse{}, statErr
		}
		mountPath := filepath.Join(engine.config.RuntimeRoot, "computer-mounts", name)
		if _, mounted, mountErr := engine.computerDiskSystem().mountedSource(mountPath); mountErr != nil {
			return ResetComputerStorageResponse{}, mountErr
		} else if mounted {
			return ResetComputerStorageResponse{}, errors.New("Computer Storage generation remains mounted during reset")
		}
		loops, loopErr := engine.computerDiskSystem().loopsForRoot(diskRoot)
		if loopErr != nil {
			return ResetComputerStorageResponse{}, loopErr
		}
		if len(loops) != 0 {
			return ResetComputerStorageResponse{}, errors.New("Computer Storage generation remains loop-attached during reset")
		}
		if err := writeComputerStorageResetManifest(resetRoot, manifestPath, manifest); err != nil {
			return ResetComputerStorageResponse{}, err
		}
		if err := engine.storageResetCheckpoint(computerStorageResetPrepared); err != nil {
			return ResetComputerStorageResponse{}, err
		}
	}

	diskRoot := filepath.Join(engine.config.RuntimeRoot, "computer-disks", name)
	quarantine := filepath.Join(quarantineRoot, manifest.QuarantineName)
	mountPath := filepath.Join(engine.config.RuntimeRoot, "computer-mounts", name)
	if manifest.Phase == computerStorageResetPrepared {
		_, diskErr := os.Lstat(diskRoot)
		_, quarantineErr := os.Lstat(quarantine)
		if diskErr == nil && quarantineErr == nil {
			return ResetComputerStorageResponse{}, errors.New("Computer Storage reset found both current and quarantined bytes")
		}
		if diskErr == nil {
			if err := os.Rename(diskRoot, quarantine); err != nil {
				return ResetComputerStorageResponse{}, err
			}
			if err := syncDirectory(filepath.Dir(diskRoot)); err != nil {
				return ResetComputerStorageResponse{}, err
			}
			if err := syncDirectory(quarantineRoot); err != nil {
				return ResetComputerStorageResponse{}, err
			}
		} else if !errors.Is(diskErr, os.ErrNotExist) {
			return ResetComputerStorageResponse{}, diskErr
		} else if quarantineErr != nil && !errors.Is(quarantineErr, os.ErrNotExist) {
			return ResetComputerStorageResponse{}, quarantineErr
		}
		manifest.Phase = computerStorageResetQuarantined
		if err := writeComputerStorageResetManifest(resetRoot, manifestPath, manifest); err != nil {
			return ResetComputerStorageResponse{}, err
		}
		if err := engine.storageResetCheckpoint(computerStorageResetQuarantined); err != nil {
			return ResetComputerStorageResponse{}, err
		}
	}
	if manifest.Phase == computerStorageResetQuarantined {
		if err := os.RemoveAll(quarantine); err != nil {
			return ResetComputerStorageResponse{}, err
		}
		if err := syncDirectory(quarantineRoot); err != nil {
			return ResetComputerStorageResponse{}, err
		}
		manifest.Phase = computerStorageResetDeleted
		if err := writeComputerStorageResetManifest(resetRoot, manifestPath, manifest); err != nil {
			return ResetComputerStorageResponse{}, err
		}
		if err := engine.storageResetCheckpoint(computerStorageResetDeleted); err != nil {
			return ResetComputerStorageResponse{}, err
		}
	}
	if manifest.Phase == computerStorageResetDeleted {
		if err := engine.verifyResetGenerationAbsent(diskRoot, quarantine, mountPath); err != nil {
			return ResetComputerStorageResponse{}, err
		}
		// A reset that resumes after a helper or boot restart keeps the same
		// logical L1 authority, but the positive verification belongs to the
		// helper generation that actually performed it. Persist that generation
		// in the receipt so replay cannot change acknowledgement evidence.
		manifest.Authority.BootSessionID = request.Authority.BootSessionID
		manifest.Authority.HelperGeneration = request.Authority.HelperGeneration
		receiptID, err := randomCapability()
		if err != nil {
			return ResetComputerStorageResponse{}, err
		}
		manifest.Receipt = &ComputerStorageResetReceipt{Kind: "computer_storage_reset_verified", ReceiptID: receiptID,
			ComputerID: request.Storage.ComputerID, StorageID: request.Storage.StorageID,
			OldGeneration: request.Storage.StorageGeneration, NewGeneration: request.NewGeneration,
			NodeID: request.Authority.NodeID, JobID: request.Authority.JobID,
			IntentRevision: request.Authority.IntentRevision, CleanupFence: request.Authority.CleanupFence,
			HelperGeneration: manifest.Authority.HelperGeneration}
		manifest.Phase = computerStorageResetVerified
		if err := writeComputerStorageResetManifest(resetRoot, manifestPath, manifest); err != nil {
			return ResetComputerStorageResponse{}, err
		}
		if err := engine.storageResetCheckpoint(computerStorageResetVerified); err != nil {
			return ResetComputerStorageResponse{}, err
		}
	}
	if manifest.Phase != computerStorageResetVerified || manifest.Receipt == nil ||
		strings.TrimSpace(manifest.Receipt.ReceiptID) == "" {
		return ResetComputerStorageResponse{}, errors.New("Computer Storage reset lacks positive verification")
	}
	return ResetComputerStorageResponse{Verified: true, Receipt: *manifest.Receipt}, nil
}
