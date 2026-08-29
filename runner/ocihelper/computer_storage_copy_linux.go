//go:build linux

package ocihelper

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"

	"golang.org/x/sys/unix"
)

type computerStorageCopyPhase string

const (
	computerStorageCopyReserved        computerStorageCopyPhase = "reserved"
	computerStorageCopyAllocated       computerStorageCopyPhase = "allocated"
	computerStorageCopyCopied          computerStorageCopyPhase = "copied"
	computerStorageCopySourceVerified  computerStorageCopyPhase = "source_verified"
	computerStorageCopyMountedRekey    computerStorageCopyPhase = "mounted_rekey"
	computerStorageCopyIdentityRekeyed computerStorageCopyPhase = "identity_rekeyed"
	computerStorageCopyExpanded        computerStorageCopyPhase = "expanded"
	computerStorageCopyManifestWritten computerStorageCopyPhase = "manifest_written"
	computerStorageCopyPublished       computerStorageCopyPhase = "published"
)

type computerStorageCopyFacts struct {
	OSIdentityRekeyed  bool
	FilesystemExpanded bool
}

type computerStorageCopyManifest struct {
	Version            int                         `json:"version"`
	Request            CopyComputerStorageRequest  `json:"request"`
	Phase              computerStorageCopyPhase    `json:"phase"`
	SourceDigest       string                      `json:"source_digest,omitempty"`
	DestinationDigest  string                      `json:"destination_digest,omitempty"`
	OSIdentityRekeyed  bool                        `json:"os_identity_rekeyed,omitempty"`
	FilesystemExpanded bool                        `json:"filesystem_expanded,omitempty"`
	Receipt            *ComputerStorageCopyReceipt `json:"receipt,omitempty"`
}

func (engine *ContainerdEngine) storageCopyCheckpoint(phase computerStorageCopyPhase) error {
	if engine.storageCopyHook == nil {
		return nil
	}
	return engine.storageCopyHook(phase)
}

func writeComputerStorageCopyManifest(root string, manifest computerStorageCopyManifest) error {
	payload, err := json.Marshal(manifest)
	if err != nil {
		return err
	}
	path := filepath.Join(root, "storage-copy.json")
	temporary, err := os.CreateTemp(root, ".storage-copy.json.tmp-")
	if err != nil {
		return err
	}
	temporaryPath := temporary.Name()
	defer os.Remove(temporaryPath)
	if err := temporary.Chmod(0o600); err != nil {
		_ = temporary.Close()
		return err
	}
	if _, err := temporary.Write(payload); err != nil {
		_ = temporary.Close()
		return err
	}
	if err := temporary.Sync(); err != nil {
		_ = temporary.Close()
		return err
	}
	if err := temporary.Close(); err != nil {
		return err
	}
	if err := os.Rename(temporaryPath, path); err != nil {
		return err
	}
	return syncDirectory(root)
}

func readComputerStorageCopyManifest(path string) (computerStorageCopyManifest, bool, error) {
	payload, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return computerStorageCopyManifest{}, false, nil
	}
	if err != nil {
		return computerStorageCopyManifest{}, false, err
	}
	var manifest computerStorageCopyManifest
	if err := json.Unmarshal(payload, &manifest); err != nil {
		return computerStorageCopyManifest{}, false, err
	}
	if manifest.Version != 1 {
		return computerStorageCopyManifest{}, false, errors.New("Computer Storage copy manifest version is unsupported")
	}
	return manifest, true, nil
}

func sameComputerStorageCopyRequest(left, right CopyComputerStorageRequest) bool {
	return left.Operation == right.Operation && left.BackupID == right.BackupID && left.CopyID == right.CopyID &&
		left.SourceComputerID == right.SourceComputerID && left.SourceStorageID == right.SourceStorageID &&
		left.SourceGeneration == right.SourceGeneration && left.SourceSize == right.SourceSize &&
		left.SourceDigest == right.SourceDigest && left.ExportID == right.ExportID &&
		left.ExternalPath == right.ExternalPath && left.ManifestDigest == right.ManifestDigest &&
		sameComputerStorageIdentity(left.Destination, right.Destination) &&
		left.Destination.DiskBytes == right.Destination.DiskBytes && left.Destination.IntentRevision == right.Destination.IntentRevision &&
		left.Authority == right.Authority
}

func validateStorageCopySource(root string, request CopyComputerStorageRequest) (string, error) {
	manifest, present, err := readComputerBackupManifest(filepath.Join(root, "copy.json"))
	if err != nil {
		return "", err
	}
	if !present || manifest.Phase != computerBackupPublished || manifest.Receipt == nil ||
		manifest.BackupID != request.BackupID || manifest.CopyID != request.CopyID ||
		manifest.Storage.ComputerID != request.SourceComputerID || manifest.Storage.StorageID != request.SourceStorageID ||
		manifest.Storage.StorageGeneration != request.SourceGeneration || manifest.Storage.DiskBytes != request.SourceSize ||
		manifest.Authority.NodeID != request.Authority.NodeID || manifest.Authority.RootInstanceID != request.Authority.RootInstanceID ||
		manifest.ContentDigest != request.SourceDigest || manifest.Receipt.ContentDigest != request.SourceDigest {
		return "", errors.New("Computer Storage copy source conflicts with published Backup authority")
	}
	path := filepath.Join(root, "backup.ext4")
	info, err := os.Lstat(path)
	if err != nil {
		return "", err
	}
	if !info.Mode().IsRegular() || info.Size() != request.SourceSize {
		return "", errors.New("Computer Storage copy source is truncated or not a regular file")
	}
	digest, err := digestFile(path)
	if err != nil {
		return "", err
	}
	if digest != request.SourceDigest {
		return "", errors.New("Computer Storage copy source digest mismatch")
	}
	return path, nil
}

func digestFilePrefix(path string, size int64) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", err
	}
	digest := sha256.New()
	copied, copyErr := io.CopyN(digest, file, size)
	err = errors.Join(copyErr, file.Close())
	if err != nil {
		return "", err
	}
	if copied != size {
		return "", io.ErrUnexpectedEOF
	}
	return "sha256:" + hex.EncodeToString(digest.Sum(nil)), nil
}

func runFilesystemTool(ctx context.Context, name string, arguments ...string) ([]byte, error) {
	command := exec.CommandContext(ctx, name, arguments...)
	output, err := command.CombinedOutput()
	if err != nil {
		return nil, fmt.Errorf("%s failed: %w: %s", filepath.Base(name), err, strings.TrimSpace(string(output)))
	}
	return output, nil
}

func ensureRealDirectory(path string) error {
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("clone identity path %q is not a real directory", path)
	}
	return nil
}

func readRegularFile(path string) ([]byte, error) {
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
		return nil, errors.New("identity evidence is not a regular file")
	}
	return os.ReadFile(path)
}

func rekeyCloneIdentity(ctx context.Context, mountPath string) (bool, error) {
	etc := filepath.Join(mountPath, "etc")
	if err := ensureRealDirectory(etc); err != nil {
		return false, err
	}
	machineIDPath := filepath.Join(etc, "machine-id")
	if info, err := os.Lstat(machineIDPath); err == nil && (info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular()) {
		return false, errors.New("clone machine-id is not a regular file")
	} else if err != nil && !errors.Is(err, os.ErrNotExist) {
		return false, err
	}
	oldMachineID, err := readRegularFile(machineIDPath)
	if err != nil {
		return false, err
	}
	identity := make([]byte, 16)
	if _, err := rand.Read(identity); err != nil {
		return false, err
	}
	if err := os.WriteFile(machineIDPath, []byte(hex.EncodeToString(identity)+"\n"), 0o444); err != nil {
		return false, err
	}
	oldKeys := map[string]string{}
	ssh := filepath.Join(etc, "ssh")
	if info, err := os.Lstat(ssh); err == nil {
		if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
			return false, errors.New("clone SSH configuration path is not a real directory")
		}
		matches, err := filepath.Glob(filepath.Join(ssh, "ssh_host_*"))
		if err != nil {
			return false, err
		}
		for _, path := range matches {
			info, err := os.Lstat(path)
			if err != nil {
				return false, err
			}
			if !info.Mode().IsRegular() {
				return false, fmt.Errorf("clone SSH host identity %q is not a regular file", path)
			}
			payload, err := os.ReadFile(path)
			if err != nil {
				return false, err
			}
			oldKeys[filepath.Base(path)] = string(payload)
			if err := os.Remove(path); err != nil {
				return false, err
			}
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return false, err
	}
	sshKeygen, err := findRootTool("ssh-keygen")
	if err != nil {
		return false, err
	}
	if _, err := runFilesystemTool(ctx, sshKeygen, "-A", "-f", mountPath); err != nil {
		return false, err
	}
	newMachineID, err := readRegularFile(machineIDPath)
	if err != nil || string(newMachineID) == string(oldMachineID) {
		return false, errors.New("clone machine-id was not observably rekeyed")
	}
	newKeys, err := filepath.Glob(filepath.Join(ssh, "ssh_host_*_key"))
	if err != nil || len(newKeys) == 0 {
		return false, errors.New("clone SSH host keys were not regenerated")
	}
	for _, path := range newKeys {
		payload, err := readRegularFile(path)
		if err != nil {
			return false, err
		}
		if old, existed := oldKeys[filepath.Base(path)]; existed && old == string(payload) {
			return false, errors.New("clone SSH host key did not change")
		}
	}
	root, err := os.Open(mountPath)
	if err != nil {
		return false, err
	}
	defer root.Close()
	return true, unix.Syncfs(int(root.Fd()))
}

func ext4Geometry(ctx context.Context, imagePath string) (blocks, blockSize int64, returnedErr error) {
	dumpe2fs, err := findRootTool("dumpe2fs")
	if err != nil {
		return 0, 0, err
	}
	output, err := runFilesystemTool(ctx, dumpe2fs, "-h", imagePath)
	if err != nil {
		return 0, 0, err
	}
	for _, line := range strings.Split(string(output), "\n") {
		if key, value, ok := strings.Cut(line, ":"); ok {
			parsed, err := strconv.ParseInt(strings.TrimSpace(value), 10, 64)
			if strings.TrimSpace(key) == "Block count" {
				blocks, returnedErr = parsed, err
			}
			if strings.TrimSpace(key) == "Block size" {
				blockSize, returnedErr = parsed, err
			}
		}
	}
	if returnedErr != nil {
		return 0, 0, returnedErr
	}
	if blocks <= 0 || blockSize <= 0 {
		return 0, 0, errors.New("ext4 block geometry was not reported")
	}
	return blocks, blockSize, nil
}

func (engine *ContainerdEngine) finalizeComputerStorageCopy(ctx context.Context, operation, imagePath, mountPath string, sourceSize int64, expanded bool) (facts computerStorageCopyFacts, returnedErr error) {
	if engine.storageCopyFinalize != nil {
		if operation == "clone" || operation == "import" {
			if err := engine.storageCopyCheckpoint(computerStorageCopyMountedRekey); err != nil {
				return facts, err
			}
		}
		return engine.storageCopyFinalize(ctx, operation, imagePath, mountPath, sourceSize, expanded)
	}
	e2fsck, err := findRootTool("e2fsck")
	if err != nil {
		return facts, err
	}
	if _, err := runFilesystemTool(ctx, e2fsck, "-f", "-n", imagePath); err != nil {
		return facts, err
	}
	beforeBlocks, blockSize, err := ext4Geometry(ctx, imagePath)
	if err != nil {
		return facts, err
	}
	if expanded {
		resize2fs, err := findRootTool("resize2fs")
		if err != nil {
			return facts, err
		}
		if _, err := runFilesystemTool(ctx, resize2fs, imagePath); err != nil {
			return facts, err
		}
		afterBlocks, afterBlockSize, err := ext4Geometry(ctx, imagePath)
		alreadyExpanded := beforeBlocks > sourceSize/blockSize
		if err != nil || afterBlockSize != blockSize || (afterBlocks <= beforeBlocks && !alreadyExpanded) {
			return facts, errors.New("filesystem expansion was not observed in ext4 block count")
		}
		facts.FilesystemExpanded = true
	}
	if operation != "clone" && operation != "import" {
		return facts, nil
	}
	if err := os.MkdirAll(mountPath, 0o700); err != nil {
		return facts, err
	}
	loopPath, err := engine.computerDiskSystem().attachAndMount(ctx, imagePath, mountPath)
	if err != nil {
		return facts, err
	}
	defer func() {
		returnedErr = errors.Join(returnedErr, engine.computerDiskSystem().detach(mountPath, loopPath, imagePath))
	}()
	if err := engine.storageCopyCheckpoint(computerStorageCopyMountedRekey); err != nil {
		return facts, err
	}
	facts.OSIdentityRekeyed, err = rekeyCloneIdentity(ctx, mountPath)
	return facts, err
}

func custodyImportFailureReceipt(runtimeRoot string, request CopyComputerStorageRequest, code string) (CopyComputerStorageResponse, error) {
	destinationName, err := deterministicComputerDiskName(request.Destination)
	if err != nil {
		return CopyComputerStorageResponse{}, err
	}
	destinationRoot := filepath.Join(runtimeRoot, "computer-disks", destinationName)
	if err := os.RemoveAll(destinationRoot); err != nil {
		return CopyComputerStorageResponse{}, err
	}
	if _, err := os.Lstat(destinationRoot); !errors.Is(err, os.ErrNotExist) {
		return CopyComputerStorageResponse{}, errors.New("Custody import staging remains after failure cleanup")
	}
	receiptID, err := randomCapability()
	if err != nil {
		return CopyComputerStorageResponse{}, err
	}
	return CopyComputerStorageResponse{Receipt: ComputerStorageCopyReceipt{
		Kind: "computer_storage_copy_failed_absent", ReceiptID: receiptID, Operation: "import",
		BackupID: request.BackupID, CopyID: request.CopyID, ExportID: request.ExportID,
		ExternalPath: request.ExternalPath, ManifestDigest: request.ManifestDigest,
		SourceComputerID: request.SourceComputerID, SourceStorageID: request.SourceStorageID,
		SourceGeneration: request.SourceGeneration, DestinationComputerID: request.Destination.ComputerID,
		DestinationStorageID: request.Destination.StorageID, DestinationGeneration: request.Destination.StorageGeneration,
		NodeID: request.Authority.NodeID, RootInstanceID: request.Authority.RootInstanceID,
		JobID: request.Authority.JobID, OperationRevision: request.Authority.OperationRevision,
		CleanupFence: request.Authority.CleanupFence, HelperGeneration: request.Authority.HelperGeneration,
		SourceSize: request.SourceSize, DestinationSize: request.Destination.DiskBytes,
		SourceDigest: request.SourceDigest, FailureCode: code, DestinationAbsent: true,
	}}, nil
}

func (engine *ContainerdEngine) CopyComputerStorage(ctx context.Context, request CopyComputerStorageRequest) (response CopyComputerStorageResponse, returnedErr error) {
	defer func() {
		if request.Operation != "import" || returnedErr == nil {
			return
		}
		code := ""
		switch {
		case errors.Is(returnedErr, context.Canceled), errors.Is(returnedErr, context.DeadlineExceeded):
			code = "cancelled"
		case errors.Is(returnedErr, unix.ENOSPC):
			code = "insufficient_disk"
		case strings.Contains(returnedErr.Error(), "digest"):
			code = "digest_mismatch"
		case strings.Contains(returnedErr.Error(), "Custody import manifest"),
			strings.Contains(returnedErr.Error(), "Custody import disk size"):
			code = "manifest_invalid"
		}
		if code != "" {
			response, returnedErr = custodyImportFailureReceipt(engine.config.RuntimeRoot, request, code)
		}
	}()
	engine.computerBackupMu.Lock()
	defer engine.computerBackupMu.Unlock()
	engine.storageCopyMu.Lock()
	defer engine.storageCopyMu.Unlock()
	managedSource := request.Operation == "restore" || request.Operation == "clone"
	importSource := request.Operation == "import"
	if (!managedSource && !importSource) || request.BackupID == "" || request.CopyID == "" ||
		(importSource && (request.ExportID == "" || request.ExternalPath == "" || request.ManifestDigest == "")) ||
		request.SourceComputerID == "" || request.SourceStorageID == "" || request.SourceGeneration < 1 || request.SourceSize < 1 ||
		request.SourceDigest == "" || request.Destination.ComputerID == "" || request.Destination.StorageID == "" ||
		request.Destination.StorageGeneration < 1 || request.Destination.DiskBytes < request.SourceSize ||
		request.Destination.IntentRevision != request.Authority.OperationRevision || request.Authority.NodeID == "" ||
		request.Authority.BootSessionID == "" || request.Authority.HelperGeneration == 0 || request.Authority.RootInstanceID == "" ||
		request.Authority.JobID == "" || request.Authority.OperationRevision < 1 || request.Authority.CleanupFence == "" {
		return CopyComputerStorageResponse{}, errors.New("Computer Storage copy request is incomplete")
	}
	var sourcePath string
	var err error
	if importSource {
		sourcePath, err = validateImportCustodySource(engine.config.RuntimeRoot, request)
	} else {
		var copyName string
		copyName, err = deterministicComputerBackupCopyName(request.CopyID)
		if err == nil {
			sourceRoot := filepath.Join(engine.config.RuntimeRoot, "computer-backups", copyName)
			sourcePath, err = validateStorageCopySource(sourceRoot, request)
		}
	}
	if err != nil {
		return CopyComputerStorageResponse{}, err
	}
	destinationName, err := deterministicComputerDiskName(request.Destination)
	if err != nil {
		return CopyComputerStorageResponse{}, err
	}
	destinationRoot := filepath.Join(engine.config.RuntimeRoot, "computer-disks", destinationName)
	if err := os.MkdirAll(destinationRoot, 0o700); err != nil {
		return CopyComputerStorageResponse{}, err
	}
	lock, err := os.OpenFile(filepath.Join(destinationRoot, "attachment.lock"), os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return CopyComputerStorageResponse{}, err
	}
	defer lock.Close()
	if err := unix.Flock(int(lock.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		return CopyComputerStorageResponse{}, errors.New("Computer Storage copy destination has an attachment owner")
	}
	defer unix.Flock(int(lock.Fd()), unix.LOCK_UN)
	manifestPath := filepath.Join(destinationRoot, "storage-copy.json")
	manifest, present, err := readComputerStorageCopyManifest(manifestPath)
	if err != nil {
		return CopyComputerStorageResponse{}, err
	}
	if present && !sameComputerStorageCopyRequest(manifest.Request, request) {
		return CopyComputerStorageResponse{}, errors.New("Computer Storage copy destination has different durable authority")
	}
	publishedPath := filepath.Join(destinationRoot, "disk.ext4")
	stagingPath := filepath.Join(destinationRoot, "disk.ext4.staging")
	if present && manifest.Phase == computerStorageCopyPublished && manifest.Receipt != nil {
		digest, err := digestFile(publishedPath)
		if err != nil {
			return CopyComputerStorageResponse{}, err
		}
		if digest != manifest.Receipt.DestinationDigest {
			return CopyComputerStorageResponse{}, errors.New("published Computer Storage copy digest changed")
		}
		return CopyComputerStorageResponse{Receipt: *manifest.Receipt}, nil
	}
	if !present {
		manifest = computerStorageCopyManifest{Version: 1, Request: request, Phase: computerStorageCopyReserved}
		if err := writeComputerStorageCopyManifest(destinationRoot, manifest); err != nil {
			return CopyComputerStorageResponse{}, err
		}
		if err := engine.storageCopyCheckpoint(computerStorageCopyReserved); err != nil {
			return CopyComputerStorageResponse{}, err
		}
	}
	if manifest.Phase == computerStorageCopyReserved || manifest.Phase == computerStorageCopyAllocated {
		if err := os.Remove(stagingPath); err != nil && !errors.Is(err, os.ErrNotExist) {
			return CopyComputerStorageResponse{}, err
		}
		file, err := os.OpenFile(stagingPath, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o600)
		if err != nil {
			return CopyComputerStorageResponse{}, err
		}
		if err := engine.allocateComputerBackup(stagingPath, request.SourceSize); err != nil {
			_ = file.Close()
			return CopyComputerStorageResponse{}, err
		}
		if err := file.Close(); err != nil {
			return CopyComputerStorageResponse{}, err
		}
		manifest.Phase = computerStorageCopyAllocated
		if err := writeComputerStorageCopyManifest(destinationRoot, manifest); err != nil {
			return CopyComputerStorageResponse{}, err
		}
		if err := engine.storageCopyCheckpoint(computerStorageCopyAllocated); err != nil {
			return CopyComputerStorageResponse{}, err
		}
		source, err := os.Open(sourcePath)
		if err != nil {
			return CopyComputerStorageResponse{}, err
		}
		destination, err := os.OpenFile(stagingPath, os.O_WRONLY, 0)
		if err != nil {
			_ = source.Close()
			return CopyComputerStorageResponse{}, err
		}
		copied, copyErr := engine.copyComputerBackup(destination, custodyContextReader{ctx: ctx, r: source}, request.SourceSize)
		if copyErr == nil && copied != request.SourceSize {
			copyErr = io.ErrUnexpectedEOF
		}
		if copyErr == nil {
			copyErr = destination.Sync()
		}
		if err := errors.Join(copyErr, source.Close(), destination.Close()); err != nil {
			return CopyComputerStorageResponse{}, err
		}
		manifest.Phase = computerStorageCopyCopied
		if err := writeComputerStorageCopyManifest(destinationRoot, manifest); err != nil {
			return CopyComputerStorageResponse{}, err
		}
		if err := engine.storageCopyCheckpoint(computerStorageCopyCopied); err != nil {
			return CopyComputerStorageResponse{}, err
		}
	}
	sourceDigest, err := digestFile(sourcePath)
	if err != nil {
		return CopyComputerStorageResponse{}, err
	}
	if sourceDigest != request.SourceDigest {
		return CopyComputerStorageResponse{}, errors.New("Computer Storage copy source digest mismatch before publication")
	}
	manifest.SourceDigest = sourceDigest
	if manifest.Phase == computerStorageCopyCopied {
		stagingDigest, err := digestFilePrefix(stagingPath, request.SourceSize)
		if err != nil {
			return CopyComputerStorageResponse{}, err
		}
		if stagingDigest != request.SourceDigest {
			return CopyComputerStorageResponse{}, errors.New("Computer Storage staging digest mismatch before filesystem mutation")
		}
		manifest.Phase = computerStorageCopySourceVerified
		if err := writeComputerStorageCopyManifest(destinationRoot, manifest); err != nil {
			return CopyComputerStorageResponse{}, err
		}
		if err := engine.storageCopyCheckpoint(computerStorageCopySourceVerified); err != nil {
			return CopyComputerStorageResponse{}, err
		}
	}
	facts := computerStorageCopyFacts{}
	if manifest.Phase == computerStorageCopySourceVerified {
		if request.Destination.DiskBytes > request.SourceSize {
			if err := engine.allocateComputerBackup(stagingPath, request.Destination.DiskBytes); err != nil {
				return CopyComputerStorageResponse{}, err
			}
		}
		expanded := request.Destination.DiskBytes > request.SourceSize
		mountPath := filepath.Join(engine.config.RuntimeRoot, "computer-copy-mounts", destinationName)
		facts, err = engine.finalizeComputerStorageCopy(ctx, request.Operation, stagingPath, mountPath, request.SourceSize, expanded)
		if err != nil {
			return CopyComputerStorageResponse{}, err
		}
		manifest.OSIdentityRekeyed = facts.OSIdentityRekeyed
		manifest.FilesystemExpanded = facts.FilesystemExpanded
		manifest.Phase = computerStorageCopyIdentityRekeyed
		if err := writeComputerStorageCopyManifest(destinationRoot, manifest); err != nil {
			return CopyComputerStorageResponse{}, err
		}
		if err := engine.storageCopyCheckpoint(computerStorageCopyIdentityRekeyed); err != nil {
			return CopyComputerStorageResponse{}, err
		}
		if facts.FilesystemExpanded {
			manifest.Phase = computerStorageCopyExpanded
			if err := writeComputerStorageCopyManifest(destinationRoot, manifest); err != nil {
				return CopyComputerStorageResponse{}, err
			}
			if err := engine.storageCopyCheckpoint(computerStorageCopyExpanded); err != nil {
				return CopyComputerStorageResponse{}, err
			}
		}
	}
	if manifest.Phase == computerStorageCopyIdentityRekeyed &&
		request.Destination.DiskBytes > request.SourceSize {
		manifest.Phase = computerStorageCopyExpanded
		if err := writeComputerStorageCopyManifest(destinationRoot, manifest); err != nil {
			return CopyComputerStorageResponse{}, err
		}
		if err := engine.storageCopyCheckpoint(computerStorageCopyExpanded); err != nil {
			return CopyComputerStorageResponse{}, err
		}
	}
	workingPath := stagingPath
	if _, err := os.Lstat(workingPath); errors.Is(err, os.ErrNotExist) {
		if _, publishedErr := os.Lstat(publishedPath); publishedErr != nil {
			return CopyComputerStorageResponse{}, err
		}
		workingPath = publishedPath
	} else if err != nil {
		return CopyComputerStorageResponse{}, err
	}
	if err := verifyComputerDiskAllocation(workingPath, request.Destination.DiskBytes); err != nil {
		return CopyComputerStorageResponse{}, err
	}
	destinationDigest, err := digestFile(workingPath)
	if err != nil {
		return CopyComputerStorageResponse{}, err
	}
	if request.Operation == "restore" && request.Destination.DiskBytes == request.SourceSize && destinationDigest != sourceDigest {
		return CopyComputerStorageResponse{}, errors.New("Computer restore destination digest mismatch")
	}
	manifest.DestinationDigest = destinationDigest
	manifest.Phase = computerStorageCopyManifestWritten
	if err := writeComputerStorageCopyManifest(destinationRoot, manifest); err != nil {
		return CopyComputerStorageResponse{}, err
	}
	if err := engine.storageCopyCheckpoint(computerStorageCopyManifestWritten); err != nil {
		return CopyComputerStorageResponse{}, err
	}
	if workingPath == stagingPath {
		if err := os.Rename(stagingPath, publishedPath); err != nil {
			return CopyComputerStorageResponse{}, err
		}
	}
	if err := syncDirectory(destinationRoot); err != nil {
		return CopyComputerStorageResponse{}, err
	}
	diskManifest := computerDiskManifest{Version: computerDiskManifestVersion, Storage: request.Destination,
		DiskImage: "disk.ext4", MountDirectory: destinationName, Prepared: true}
	if err := writeComputerDiskManifest(destinationRoot, diskManifest); err != nil {
		return CopyComputerStorageResponse{}, err
	}
	if err := engine.storageCopyCheckpoint(computerStorageCopyPublished); err != nil {
		return CopyComputerStorageResponse{}, err
	}
	receiptID, err := randomCapability()
	if err != nil {
		return CopyComputerStorageResponse{}, err
	}
	receipt := ComputerStorageCopyReceipt{Kind: "computer_storage_copy_verified", ReceiptID: receiptID,
		Operation: request.Operation, BackupID: request.BackupID, CopyID: request.CopyID,
		ExportID: request.ExportID, ExternalPath: request.ExternalPath, ManifestDigest: request.ManifestDigest,
		SourceComputerID: request.SourceComputerID, SourceStorageID: request.SourceStorageID,
		SourceGeneration: request.SourceGeneration, DestinationComputerID: request.Destination.ComputerID,
		DestinationStorageID: request.Destination.StorageID, DestinationGeneration: request.Destination.StorageGeneration,
		NodeID: request.Authority.NodeID, RootInstanceID: request.Authority.RootInstanceID,
		JobID: request.Authority.JobID, OperationRevision: request.Authority.OperationRevision,
		CleanupFence: request.Authority.CleanupFence, HelperGeneration: request.Authority.HelperGeneration,
		SourceSize: request.SourceSize, DestinationSize: request.Destination.DiskBytes,
		SourceDigest: sourceDigest, DestinationDigest: destinationDigest,
		OSIdentityRekeyed:  manifest.OSIdentityRekeyed,
		FilesystemExpanded: manifest.FilesystemExpanded}
	manifest.Phase, manifest.Receipt = computerStorageCopyPublished, &receipt
	if err := writeComputerStorageCopyManifest(destinationRoot, manifest); err != nil {
		return CopyComputerStorageResponse{}, err
	}
	return CopyComputerStorageResponse{Receipt: receipt}, nil
}
