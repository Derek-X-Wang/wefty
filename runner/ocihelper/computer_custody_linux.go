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
	"golang.org/x/sys/unix"
)

type computerCustodyManifest = contract.ComputerCustodyManifest

type custodyExternalOwner struct {
	uid int
	gid int
}

type custodyExportMechanicsError struct {
	code string
	err  error
}

func (err *custodyExportMechanicsError) Error() string { return err.err.Error() }
func (err *custodyExportMechanicsError) Unwrap() error { return err.err }

func custodyMechanicsError(code string, err error) error {
	return &custodyExportMechanicsError{code: code, err: err}
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
	root, _, err := resolveSafeExternalCustodyRoot(runtimeRoot, externalPath)
	return root, err
}

func resolveSafeExternalCustodyRoot(runtimeRoot, externalPath string) (string, string, error) {
	if !filepath.IsAbs(externalPath) {
		return "", "", errors.New("Custody path must be absolute")
	}
	root := filepath.Clean(externalPath)
	runtime, err := filepath.Abs(runtimeRoot)
	if err != nil {
		return "", "", err
	}
	runtime, err = filepath.EvalSymlinks(runtime)
	if err != nil {
		return "", "", fmt.Errorf("resolve managed root: %w", err)
	}
	ancestor := root
	existingAncestor := ""
	for {
		if _, err := os.Lstat(ancestor); err == nil {
			resolved, resolveErr := filepath.EvalSymlinks(ancestor)
			if resolveErr != nil {
				return "", "", resolveErr
			}
			suffix, suffixErr := filepath.Rel(ancestor, root)
			if suffixErr != nil {
				return "", "", suffixErr
			}
			root = filepath.Join(resolved, suffix)
			existingAncestor = resolved
			break
		} else if !errors.Is(err, os.ErrNotExist) {
			return "", "", err
		}
		parent := filepath.Dir(ancestor)
		if parent == ancestor {
			return "", "", errors.New("Custody path has no existing ancestor")
		}
		ancestor = parent
	}
	relative, err := filepath.Rel(runtime, root)
	if err != nil {
		return "", "", err
	}
	if relative == "." || (relative != ".." && !strings.HasPrefix(relative, ".."+string(filepath.Separator))) {
		return "", "", errors.New("Custody path must be outside the managed root")
	}
	return root, existingAncestor, nil
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

func prepareCustodyExternalRoot(root, existingAncestor string, owner custodyExternalOwner, chown func(*os.File, int, int) error) error {
	relative, err := filepath.Rel(existingAncestor, root)
	if err != nil {
		return err
	}
	ancestor, err := os.Open(existingAncestor)
	if err != nil {
		return err
	}
	defer ancestor.Close()
	current := ancestor
	for _, component := range strings.Split(relative, string(filepath.Separator)) {
		if component == "" || component == "." {
			continue
		}
		if err := unix.Mkdirat(int(current.Fd()), component, 0o700); err != nil && !errors.Is(err, syscall.EEXIST) {
			return err
		}
		fd, err := unix.Openat(int(current.Fd()), component, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
		if err != nil {
			return custodyMechanicsError("destination_substituted", fmt.Errorf("open created Custody directory: %w", err))
		}
		next := os.NewFile(uintptr(fd), filepath.Join(current.Name(), component))
		if err := next.Chmod(0o700); err != nil {
			_ = next.Close()
			return custodyMechanicsError("ownership_failed", fmt.Errorf("protect Custody directory: %w", err))
		}
		if err := chown(next, owner.uid, owner.gid); err != nil {
			_ = next.Close()
			return custodyMechanicsError("ownership_failed", fmt.Errorf("assign Custody directory ownership: %w", err))
		}
		if current != ancestor {
			_ = current.Close()
		}
		current = next
	}
	if current != ancestor {
		return current.Close()
	}
	return nil
}

func openCustodyDestination(path string, owner custodyExternalOwner, chown func(*os.File, int, int) error) (*os.File, error) {
	flags := unix.O_RDWR | unix.O_CREAT | unix.O_EXCL | unix.O_NOFOLLOW | unix.O_CLOEXEC
	fd, err := unix.Open(path, flags, 0o600)
	created := err == nil
	if errors.Is(err, syscall.EEXIST) {
		fd, err = unix.Open(path, unix.O_RDWR|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	}
	if err != nil {
		code := "ownership_failed"
		if errors.Is(err, syscall.ELOOP) {
			code = "destination_substituted"
		}
		return nil, custodyMechanicsError(code, fmt.Errorf("open Custody destination without following links: %w", err))
	}
	file := os.NewFile(uintptr(fd), path)
	closeOnError := func(err error) (*os.File, error) {
		_ = file.Close()
		return nil, err
	}
	info, err := file.Stat()
	if err != nil {
		return closeOnError(err)
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || !info.Mode().IsRegular() || (!created && (int(stat.Uid) != owner.uid || int(stat.Gid) != owner.gid)) {
		return closeOnError(custodyMechanicsError("destination_substituted", errors.New("Custody destination is not the expected operator-owned regular inode")))
	}
	if err := file.Chmod(0o600); err != nil {
		return closeOnError(custodyMechanicsError("ownership_failed", fmt.Errorf("protect Custody destination: %w", err)))
	}
	if err := chown(file, owner.uid, owner.gid); err != nil {
		return closeOnError(custodyMechanicsError("ownership_failed", fmt.Errorf("assign Custody destination ownership: %w", err)))
	}
	if err := file.Truncate(0); err != nil {
		return closeOnError(err)
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return closeOnError(err)
	}
	return file, nil
}

func digestCustodyFile(file *os.File) (string, error) {
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return "", err
	}
	digest := sha256.New()
	if _, err := io.Copy(digest, file); err != nil {
		return "", err
	}
	return "sha256:" + hex.EncodeToString(digest.Sum(nil)), nil
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
		return nil, custodyMechanicsError("ownership_failed", fmt.Errorf("assign Custody manifest ownership: %w", err))
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
	fd, err := unix.Open(disk, unix.O_RDONLY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return "", errors.New("Custody import disk size conflicts with its manifest")
	}
	diskFile := os.NewFile(uintptr(fd), disk)
	defer diskFile.Close()
	info, err := diskFile.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Size() != request.SourceSize {
		return "", errors.New("Custody import disk size conflicts with its manifest")
	}
	// This re-verification is load-bearing: import trusts neither a path-derived
	// owner nor prior export time once the portable bytes are operator-owned.
	digest, err := digestCustodyFile(diskFile)
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
		var mechanics *custodyExportMechanicsError
		if errors.As(returnedErr, &mechanics) {
			code = mechanics.code
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
	if quarantined, err := computerDiskQuarantined(engine.config.RuntimeRoot, request.Storage); err != nil {
		return ExportComputerCustodyResponse{}, err
	} else if quarantined {
		return ExportComputerCustodyResponse{}, &ComputerStorageQuarantinedError{Storage: request.Storage}
	}
	externalRoot, existingAncestor, err := resolveSafeExternalCustodyRoot(engine.config.RuntimeRoot, request.ExternalPath)
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
	if digest, err := digestFile(ctx, source); err != nil || digest != request.SourceDigest {
		return ExportComputerCustodyResponse{}, errors.New("Custody export source digest changed")
	}
	if engine.computerCustodyHook != nil {
		if err := engine.computerCustodyHook("before_external_write"); err != nil {
			return ExportComputerCustodyResponse{}, err
		}
	}
	chown := func(file *os.File, uid, gid int) error { return file.Chown(uid, gid) }
	if engine.computerCustodyChown != nil {
		chown = engine.computerCustodyChown
	}
	readOwner := readCustodyExternalOwner
	if engine.computerCustodyOwner != nil {
		readOwner = engine.computerCustodyOwner
	}
	// Ownership is deliberately inherited from the nearest existing ancestor,
	// before any root-created path components can obscure the operator identity.
	externalOwner, err := readOwner(existingAncestor)
	if err != nil {
		return ExportComputerCustodyResponse{}, err
	}
	if err := prepareCustodyExternalRoot(externalRoot, existingAncestor, externalOwner, chown); err != nil {
		return ExportComputerCustodyResponse{}, err
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
	destination, err := openCustodyDestination(destinationPath, externalOwner, chown)
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
	written, copyErr := copyN(destination, custodyContextReader{ctx: ctx, r: sourceFile}, request.SourceSize)
	if copyErr == nil {
		copyErr = destination.Sync()
	}
	copyErr = errors.Join(copyErr, sourceFile.Close())
	if copyErr != nil {
		_ = destination.Close()
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
			_ = destination.Close()
			return ExportComputerCustodyResponse{}, err
		}
	}
	if written != request.SourceSize {
		_ = destination.Close()
		return ExportComputerCustodyResponse{}, fmt.Errorf("Custody export wrote %d bytes, want %d", written, request.SourceSize)
	}
	// The post-copy digest is load-bearing evidence and is read back through
	// the same O_NOFOLLOW-opened inode that was truncated and written.
	if digest, err := digestCustodyFile(destination); err != nil || digest != request.SourceDigest {
		_ = destination.Close()
		return ExportComputerCustodyResponse{}, errors.New("Custody export destination digest mismatch")
	}
	if err := destination.Close(); err != nil {
		return ExportComputerCustodyResponse{}, err
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
		ManifestDigest: custodyManifestDigest(payload), ExternalOwnerUID: uint32(externalOwner.uid),
		ExternalOwnerGID: uint32(externalOwner.gid), OwnershipApplied: true, PrivateModeApplied: true}
	return ExportComputerCustodyResponse{Receipt: receipt}, nil
}
