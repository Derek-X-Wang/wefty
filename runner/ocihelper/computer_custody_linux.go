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
	"syscall"

	"github.com/Derek-X-Wang/wefty/contract"
)

type computerCustodyManifest = contract.ComputerCustodyManifest

type custodyExternalOwner struct {
	uid int
	gid int
}

func custodyManifestDigest(payload []byte) string {
	digest := sha256.Sum256(payload)
	return "sha256:" + hex.EncodeToString(digest[:])
}

func sameCustodyManifest(manifest computerCustodyManifest, request ExportComputerCustodyRequest) bool {
	return manifest.Version == 1 && manifest.ExportID == request.ExportID &&
		manifest.BackupID == request.BackupID && manifest.CopyID == request.CopyID &&
		manifest.ComputerID == request.Storage.ComputerID && manifest.StorageID == request.Storage.StorageID &&
		manifest.StorageGeneration == request.Storage.StorageGeneration &&
		manifest.AllocatedSize == request.SourceSize && manifest.ContentDigest == request.SourceDigest &&
		manifest.Encryption == "none" && manifest.NodeID == request.Authority.NodeID &&
		manifest.RootInstanceID == request.Authority.RootInstanceID &&
		manifest.OperationRevision == request.Authority.OperationRevision && manifest.JobSpecHash == request.JobSpecHash &&
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

func readCustodyExternalOwner(root string) (custodyExternalOwner, error) {
	info, err := os.Stat(root)
	if err != nil {
		return custodyExternalOwner{}, err
	}
	if !info.IsDir() {
		return custodyExternalOwner{}, errors.New("Custody path is not a directory")
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return custodyExternalOwner{}, errors.New("Custody path ownership is unavailable")
	}
	return custodyExternalOwner{uid: int(stat.Uid), gid: int(stat.Gid)}, nil
}

func writeCustodyManifest(root string, owner custodyExternalOwner, chown func(*os.File, int, int) error, manifest computerCustodyManifest) ([]byte, error) {
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
	if err := chown(file, owner.uid, owner.gid); err != nil {
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
		manifest.ComputerID != request.SourceComputerID ||
		manifest.StorageID != request.SourceStorageID || manifest.StorageGeneration != request.SourceGeneration ||
		manifest.AllocatedSize != request.SourceSize || manifest.ContentDigest != request.SourceDigest ||
		request.Authority.NodeID == "" || request.Authority.RootInstanceID == "" {
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

type custodyContextReader struct {
	ctx context.Context
	r   io.Reader
}

func (reader custodyContextReader) Read(payload []byte) (int, error) {
	if err := reader.ctx.Err(); err != nil {
		return 0, err
	}
	return reader.r.Read(payload)
}

func custodyExportFailure(request ExportComputerCustodyRequest, code string) (ExportComputerCustodyResponse, error) {
	receiptID, err := randomCapability()
	if err != nil {
		return ExportComputerCustodyResponse{}, err
	}
	return ExportComputerCustodyResponse{Receipt: ComputerCustodyExportReceipt{
		Kind: "computer_custody_export_failed", ReceiptID: receiptID, ExportID: request.ExportID,
		BackupID: request.BackupID, CopyID: request.CopyID, ComputerID: request.Storage.ComputerID,
		StorageID: request.Storage.StorageID, StorageGeneration: request.Storage.StorageGeneration,
		NodeID: request.Authority.NodeID, RootInstanceID: request.Authority.RootInstanceID,
		OperationRevision: request.Authority.OperationRevision, CustodyFence: request.Authority.CustodyFence,
		HelperGeneration: request.Authority.HelperGeneration, ExternalPath: request.ExternalPath,
		AllocatedSize: request.SourceSize, ContentDigest: request.SourceDigest, FailureCode: code,
	}}, nil
}

func (engine *ContainerdEngine) ExportComputerCustody(ctx context.Context, request ExportComputerCustodyRequest) (response ExportComputerCustodyResponse, returnedErr error) {
	defer func() {
		if returnedErr == nil {
			return
		}
		code := ""
		switch {
		case errors.Is(returnedErr, context.Canceled), errors.Is(returnedErr, context.DeadlineExceeded):
			code = "cancelled"
		case errors.Is(returnedErr, syscall.ENOSPC):
			code = "insufficient_disk"
		}
		if code != "" {
			response, returnedErr = custodyExportFailure(request, code)
		}
	}()
	engine.computerBackupMu.Lock()
	defer engine.computerBackupMu.Unlock()
	if request.ExportID == "" || request.BackupID == "" || request.CopyID == "" || request.SourceSize < 1 ||
		request.SourceDigest == "" || request.Authority.HelperGeneration == 0 || request.JobSpecHash == "" {
		return ExportComputerCustodyResponse{}, errors.New("Custody export request is incomplete")
	}
	externalRoot, err := safeExternalCustodyRoot(engine.config.RuntimeRoot, request.ExternalPath)
	if err != nil {
		if strings.Contains(err.Error(), "outside the managed root") {
			return custodyExportFailure(request, "managed_root_path")
		}
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
	if engine.computerCustodyHook != nil {
		if err := engine.computerCustodyHook("before_external_write"); err != nil {
			return ExportComputerCustodyResponse{}, err
		}
	}
	if err := os.MkdirAll(externalRoot, 0o700); err != nil {
		if errors.Is(err, syscall.ENOSPC) {
			return custodyExportFailure(request, "insufficient_disk")
		}
		return ExportComputerCustodyResponse{}, err
	}
	externalOwner, err := readCustodyExternalOwner(externalRoot)
	if err != nil {
		return ExportComputerCustodyResponse{}, err
	}
	chown := func(file *os.File, uid, gid int) error { return file.Chown(uid, gid) }
	if engine.computerCustodyChown != nil {
		chown = engine.computerCustodyChown
	}
	manifest, _, exists, err := readCustodyManifest(externalRoot)
	if err != nil {
		return ExportComputerCustodyResponse{}, err
	}
	if exists && !sameCustodyManifest(manifest, request) {
		return custodyExportFailure(request, "destination_not_empty")
	}
	if !exists {
		entries, err := os.ReadDir(externalRoot)
		if err != nil {
			return ExportComputerCustodyResponse{}, err
		}
		if len(entries) != 0 {
			return custodyExportFailure(request, "destination_not_empty")
		}
		manifest = computerCustodyManifest{Version: 1, ExportID: request.ExportID, BackupID: request.BackupID,
			CopyID: request.CopyID, ComputerID: request.Storage.ComputerID, StorageID: request.Storage.StorageID,
			StorageGeneration: request.Storage.StorageGeneration, AllocatedSize: request.SourceSize,
			ContentDigest: request.SourceDigest, Encryption: "none", NodeID: request.Authority.NodeID,
			RootInstanceID: request.Authority.RootInstanceID, OperationRevision: request.Authority.OperationRevision,
			CustodyFence: request.Authority.CustodyFence, JobSpec: request.JobSpec, JobSpecHash: request.JobSpecHash,
			DiskFile: "storage.ext4", Phase: "writing"}
		if _, err := writeCustodyManifest(externalRoot, externalOwner, chown, manifest); err != nil {
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
	if err := chown(destination, externalOwner.uid, externalOwner.gid); err != nil {
		_ = destination.Close()
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
	written, copyErr := copyN(destination, custodyContextReader{ctx: ctx, r: sourceFile}, request.SourceSize)
	if copyErr == nil {
		copyErr = destination.Sync()
	}
	copyErr = errors.Join(copyErr, sourceFile.Close(), destination.Close())
	if copyErr != nil {
		if errors.Is(copyErr, context.Canceled) || errors.Is(copyErr, context.DeadlineExceeded) {
			return custodyExportFailure(request, "cancelled")
		}
		if errors.Is(copyErr, syscall.ENOSPC) {
			return custodyExportFailure(request, "insufficient_disk")
		}
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
	payload, err := writeCustodyManifest(externalRoot, externalOwner, chown, manifest)
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
