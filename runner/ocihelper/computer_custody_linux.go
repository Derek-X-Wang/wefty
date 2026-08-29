//go:build linux

package ocihelper

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

type computerCustodyManifest struct {
	Version           int                      `json:"version"`
	ExportID          string                   `json:"export_id"`
	BackupID          string                   `json:"backup_id"`
	CopyID            string                   `json:"copy_id"`
	Source            ComputerStorageReference `json:"source"`
	AllocatedSize     int64                    `json:"allocated_size"`
	ContentDigest     string                   `json:"content_digest"`
	Encryption        string                   `json:"encryption"`
	NodeID            string                   `json:"node_id"`
	RootInstanceID    string                   `json:"root_instance_id"`
	OperationRevision int64                    `json:"operation_revision"`
	CustodyFence      string                   `json:"custody_fence"`
	DiskFile          string                   `json:"disk_file"`
	Phase             string                   `json:"phase"`
}

func custodyManifestDigest(payload []byte) string {
	digest := sha256.Sum256(payload)
	return "sha256:" + hex.EncodeToString(digest[:])
}

func sameCustodyManifest(manifest computerCustodyManifest, request ExportComputerCustodyRequest) bool {
	return manifest.Version == 1 && manifest.ExportID == request.ExportID &&
		manifest.BackupID == request.BackupID && manifest.CopyID == request.CopyID &&
		sameComputerStorageIdentity(manifest.Source, request.Storage) &&
		manifest.AllocatedSize == request.SourceSize && manifest.ContentDigest == request.SourceDigest &&
		manifest.Encryption == "none" && manifest.NodeID == request.Authority.NodeID &&
		manifest.RootInstanceID == request.Authority.RootInstanceID &&
		manifest.OperationRevision == request.Authority.OperationRevision &&
		manifest.CustodyFence == request.Authority.CustodyFence && manifest.DiskFile == "storage.ext4"
}

func safeExternalCustodyRoot(runtimeRoot, externalPath string) (string, error) {
	if !filepath.IsAbs(externalPath) {
		return "", errors.New("Custody path must be absolute")
	}
	root := filepath.Clean(externalPath)
	runtime, err := filepath.Abs(runtimeRoot)
	if err != nil {
		return "", err
	}
	runtime, err = filepath.EvalSymlinks(runtime)
	if err != nil {
		return "", fmt.Errorf("resolve managed root: %w", err)
	}
	ancestor := root
	for {
		if _, err := os.Lstat(ancestor); err == nil {
			resolved, resolveErr := filepath.EvalSymlinks(ancestor)
			if resolveErr != nil {
				return "", resolveErr
			}
			suffix, suffixErr := filepath.Rel(ancestor, root)
			if suffixErr != nil {
				return "", suffixErr
			}
			root = filepath.Join(resolved, suffix)
			break
		} else if !errors.Is(err, os.ErrNotExist) {
			return "", err
		}
		parent := filepath.Dir(ancestor)
		if parent == ancestor {
			return "", errors.New("Custody path has no existing ancestor")
		}
		ancestor = parent
	}
	relative, err := filepath.Rel(runtime, root)
	if err != nil {
		return "", err
	}
	if relative == "." || (relative != ".." && !strings.HasPrefix(relative, ".."+string(filepath.Separator))) {
		return "", errors.New("Custody path must be outside the managed root")
	}
	return root, nil
}

func writeCustodyManifest(root string, manifest computerCustodyManifest) ([]byte, error) {
	payload, err := json.Marshal(manifest)
	if err != nil {
		return nil, err
	}
	file, err := os.CreateTemp(root, ".custody-manifest.tmp-")
	if err != nil {
		return nil, err
	}
	name := file.Name()
	defer os.Remove(name)
	if err := file.Chmod(0o600); err != nil {
		_ = file.Close()
		return nil, err
	}
	writeErr := error(nil)
	if _, writeErr = file.Write(payload); writeErr == nil {
		writeErr = file.Sync()
	}
	writeErr = errors.Join(writeErr, file.Close())
	if writeErr != nil {
		return nil, writeErr
	}
	if err := os.Rename(name, filepath.Join(root, "custody.json")); err != nil {
		return nil, err
	}
	if err := syncDirectory(root); err != nil {
		return nil, err
	}
	return payload, nil
}

func readCustodyManifest(root string) (computerCustodyManifest, []byte, bool, error) {
	payload, err := os.ReadFile(filepath.Join(root, "custody.json"))
	if errors.Is(err, os.ErrNotExist) {
		return computerCustodyManifest{}, nil, false, nil
	}
	if err != nil {
		return computerCustodyManifest{}, nil, false, err
	}
	var manifest computerCustodyManifest
	decoder := json.NewDecoder(strings.NewReader(string(payload)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&manifest); err != nil || manifest.Version != 1 || manifest.Encryption != "none" ||
		(manifest.Phase != "writing" && manifest.Phase != "complete") {
		return computerCustodyManifest{}, nil, false, errors.New("Custody manifest is invalid")
	}
	return manifest, payload, true, nil
}

func validateImportCustodySource(runtimeRoot string, request CopyComputerStorageRequest) (string, error) {
	root, err := safeExternalCustodyRoot(runtimeRoot, request.ExternalPath)
	if err != nil {
		return "", err
	}
	manifest, payload, present, err := readCustodyManifest(root)
	if err != nil || !present {
		if err == nil {
			err = errors.New("Custody import manifest is missing")
		}
		return "", err
	}
	if manifest.Phase != "complete" || custodyManifestDigest(payload) != request.ManifestDigest ||
		manifest.ExportID != request.ExportID || manifest.BackupID != request.BackupID || manifest.CopyID != request.CopyID ||
		manifest.Source.ComputerID != request.SourceComputerID ||
		manifest.Source.StorageID != request.SourceStorageID || manifest.Source.StorageGeneration != request.SourceGeneration ||
		manifest.AllocatedSize != request.SourceSize || manifest.ContentDigest != request.SourceDigest ||
		manifest.NodeID != request.Authority.NodeID || manifest.RootInstanceID != request.Authority.RootInstanceID {
		return "", errors.New("Custody import manifest conflicts with recorded export evidence")
	}
	disk := filepath.Join(root, manifest.DiskFile)
	info, err := os.Stat(disk)
	if err != nil || !info.Mode().IsRegular() || info.Size() != request.SourceSize {
		return "", errors.New("Custody import disk size conflicts with its manifest")
	}
	digest, err := digestFile(disk)
	if err != nil || digest != request.SourceDigest {
		return "", errors.New("Custody import disk digest conflicts with its manifest")
	}
	return disk, nil
}

func (engine *ContainerdEngine) ExportComputerCustody(_ context.Context, request ExportComputerCustodyRequest) (ExportComputerCustodyResponse, error) {
	engine.computerBackupMu.Lock()
	defer engine.computerBackupMu.Unlock()
	if request.ExportID == "" || request.BackupID == "" || request.CopyID == "" || request.SourceSize < 1 ||
		request.SourceDigest == "" || request.Authority.HelperGeneration == 0 {
		return ExportComputerCustodyResponse{}, errors.New("Custody export request is incomplete")
	}
	externalRoot, err := safeExternalCustodyRoot(engine.config.RuntimeRoot, request.ExternalPath)
	if err != nil {
		return ExportComputerCustodyResponse{}, err
	}
	copyName, err := deterministicComputerBackupCopyName(request.CopyID)
	if err != nil {
		return ExportComputerCustodyResponse{}, err
	}
	sourceRoot := filepath.Join(engine.config.RuntimeRoot, "computer-backups", copyName)
	backup, present, err := readComputerBackupManifest(filepath.Join(sourceRoot, "copy.json"))
	if err != nil || !present || backup.Phase != computerBackupPublished || backup.Receipt == nil ||
		backup.BackupID != request.BackupID || backup.CopyID != request.CopyID ||
		!sameComputerStorageIdentity(backup.Storage, request.Storage) || backup.ContentDigest != request.SourceDigest {
		return ExportComputerCustodyResponse{}, errors.New("Custody export source conflicts with published Backup evidence")
	}
	if backup.Authority.NodeID != request.Authority.NodeID || backup.Authority.RootInstanceID != request.Authority.RootInstanceID {
		return ExportComputerCustodyResponse{}, errors.New("Custody export source belongs to different Node or managed-root authority")
	}
	source := filepath.Join(sourceRoot, backup.PublishedFile)
	if digest, err := digestFile(source); err != nil || digest != request.SourceDigest {
		return ExportComputerCustodyResponse{}, errors.New("Custody export source digest changed")
	}
	if err := os.MkdirAll(externalRoot, 0o700); err != nil {
		return ExportComputerCustodyResponse{}, err
	}
	manifest, _, exists, err := readCustodyManifest(externalRoot)
	if err != nil {
		return ExportComputerCustodyResponse{}, err
	}
	if exists && !sameCustodyManifest(manifest, request) {
		return ExportComputerCustodyResponse{}, errors.New("Custody destination contains a different export")
	}
	if !exists {
		entries, err := os.ReadDir(externalRoot)
		if err != nil {
			return ExportComputerCustodyResponse{}, err
		}
		if len(entries) != 0 {
			return ExportComputerCustodyResponse{}, errors.New("Custody destination is not empty")
		}
		manifest = computerCustodyManifest{Version: 1, ExportID: request.ExportID, BackupID: request.BackupID,
			CopyID: request.CopyID, Source: request.Storage, AllocatedSize: request.SourceSize,
			ContentDigest: request.SourceDigest, Encryption: "none", NodeID: request.Authority.NodeID,
			RootInstanceID: request.Authority.RootInstanceID, OperationRevision: request.Authority.OperationRevision,
			CustodyFence: request.Authority.CustodyFence, DiskFile: "storage.ext4", Phase: "writing"}
		if _, err := writeCustodyManifest(externalRoot, manifest); err != nil {
			return ExportComputerCustodyResponse{}, err
		}
		if engine.computerCustodyHook != nil {
			if err := engine.computerCustodyHook("manifest"); err != nil {
				return ExportComputerCustodyResponse{}, err
			}
		}
	}
	destinationPath := filepath.Join(externalRoot, manifest.DiskFile)
	destination, err := os.OpenFile(destinationPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return ExportComputerCustodyResponse{}, err
	}
	sourceFile, err := os.Open(source)
	if err != nil {
		_ = destination.Close()
		return ExportComputerCustodyResponse{}, err
	}
	copyN := io.CopyN
	if engine.computerCustodyCopyN != nil {
		copyN = engine.computerCustodyCopyN
	}
	written, copyErr := copyN(destination, sourceFile, request.SourceSize)
	if copyErr == nil {
		copyErr = destination.Sync()
	}
	copyErr = errors.Join(copyErr, sourceFile.Close(), destination.Close())
	if copyErr != nil {
		return ExportComputerCustodyResponse{}, copyErr
	}
	if engine.computerCustodyHook != nil {
		if err := engine.computerCustodyHook("copy"); err != nil {
			return ExportComputerCustodyResponse{}, err
		}
	}
	if written != request.SourceSize {
		return ExportComputerCustodyResponse{}, fmt.Errorf("Custody export wrote %d bytes, want %d", written, request.SourceSize)
	}
	if digest, err := digestFile(destinationPath); err != nil || digest != request.SourceDigest {
		return ExportComputerCustodyResponse{}, errors.New("Custody export destination digest mismatch")
	}
	manifest.Phase = "complete"
	payload, err := writeCustodyManifest(externalRoot, manifest)
	if err != nil {
		return ExportComputerCustodyResponse{}, err
	}
	receiptID, err := randomCapability()
	if err != nil {
		return ExportComputerCustodyResponse{}, err
	}
	receipt := ComputerCustodyExportReceipt{Kind: "computer_custody_export_verified", ReceiptID: receiptID,
		ExportID: request.ExportID, BackupID: request.BackupID, CopyID: request.CopyID,
		ComputerID: request.Storage.ComputerID, StorageID: request.Storage.StorageID,
		StorageGeneration: request.Storage.StorageGeneration, NodeID: request.Authority.NodeID,
		RootInstanceID: request.Authority.RootInstanceID, OperationRevision: request.Authority.OperationRevision,
		CustodyFence: request.Authority.CustodyFence, HelperGeneration: request.Authority.HelperGeneration,
		ExternalPath: request.ExternalPath, AllocatedSize: request.SourceSize, ContentDigest: request.SourceDigest,
		ManifestDigest: custodyManifestDigest(payload)}
	return ExportComputerCustodyResponse{Receipt: receipt}, nil
}
