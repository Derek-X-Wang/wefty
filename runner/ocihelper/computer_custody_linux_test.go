//go:build linux

package ocihelper

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
)

func custodyFileOwner(t *testing.T, path string) (uint32, uint32) {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		t.Fatalf("Custody path %s has no Linux ownership", path)
	}
	return stat.Uid, stat.Gid
}

func custodyExportTestRequest(source CreateComputerBackupResponse, externalPath string) ExportComputerCustodyRequest {
	receipt := source.Receipt
	return ExportComputerCustodyRequest{ExportID: "export-1", BackupID: receipt.BackupID, CopyID: receipt.CopyID,
		Storage: ComputerStorageReference{ComputerID: receipt.ComputerID, StorageID: receipt.StorageID,
			StorageGeneration: receipt.StorageGeneration, IntentRevision: receipt.OperationRevision,
			DiskBytes: receipt.AllocatedSize},
		SourceSize: receipt.AllocatedSize, SourceDigest: receipt.ContentDigest, ExternalPath: externalPath,
		JobSpecHash: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		Authority: ComputerCustodyExportAuthority{NodeID: receipt.NodeID, BootSessionID: "export-boot",
			HelperGeneration: 4, RootInstanceID: receipt.RootInstanceID,
			OperationRevision: receipt.OperationRevision + 1, CustodyFence: "custody-fence"}}
}

func importFinalize(_ context.Context, operation, imagePath, _ string, _ int64, expanded bool) (computerStorageCopyFacts, error) {
	facts := computerStorageCopyFacts{OSIdentityRekeyed: operation == "import", FilesystemExpanded: expanded}
	file, err := os.OpenFile(imagePath, os.O_WRONLY, 0)
	if err != nil {
		return facts, err
	}
	if operation == "import" {
		_, err = file.WriteAt([]byte("import-machine-id=rekeyed\n"), 8192)
	}
	return facts, errors.Join(err, file.Sync(), file.Close())
}

func TestComputerCustodyExportCrashLeavesPermanentExternalBytesAndResumes(t *testing.T) {
	root, system, source := publishedStorageCopySource(t)
	externalRoot := filepath.Join(t.TempDir(), "operator-custody")
	request := custodyExportTestRequest(source, externalRoot)
	crash := errors.New("injected mid-export crash")
	engine := &ContainerdEngine{config: NativeEngineConfig{RuntimeRoot: root}, diskSystem: system}
	engine.computerCustodyCopyN = func(destination io.Writer, source io.Reader, size int64) (int64, error) {
		written, err := io.CopyN(destination, source, size/2)
		if err != nil {
			return written, err
		}
		return written, crash
	}
	if _, err := engine.ExportComputerCustody(t.Context(), request); !errors.Is(err, crash) {
		t.Fatalf("mid-export failure = %v, want injected crash", err)
	}
	if _, err := os.Stat(filepath.Join(externalRoot, "custody.json")); err != nil {
		t.Fatalf("mid-export lost durable external manifest: %v", err)
	}
	partial, err := os.Stat(filepath.Join(externalRoot, "storage.ext4"))
	if err != nil || partial.Size() == 0 || partial.Size() >= request.SourceSize {
		t.Fatalf("mid-export partial bytes = %#v err=%v", partial, err)
	}
	var chownCalls []string
	engine = &ContainerdEngine{config: NativeEngineConfig{RuntimeRoot: root}, diskSystem: system,
		computerCustodyChown: func(file *os.File, uid, gid int) error {
			chownCalls = append(chownCalls, filepath.Base(file.Name()))
			return file.Chown(uid, gid)
		}}
	completed, err := engine.ExportComputerCustody(t.Context(), request)
	if err != nil || completed.Receipt.Kind != "computer_custody_export_verified" ||
		completed.Receipt.ManifestDigest == "" || completed.Receipt.ContentDigest != request.SourceDigest {
		t.Fatalf("resumed Custody export = %+v err=%v", completed, err)
	}
	if digest, err := digestFile(filepath.Join(externalRoot, "storage.ext4")); err != nil || digest != request.SourceDigest {
		t.Fatalf("resumed Custody bytes digest = %s err=%v", digest, err)
	}
	wantUID, wantGID := custodyFileOwner(t, externalRoot)
	for _, path := range []string{filepath.Join(externalRoot, "custody.json"), filepath.Join(externalRoot, "storage.ext4")} {
		uid, gid := custodyFileOwner(t, path)
		if uid != wantUID || gid != wantGID {
			t.Fatalf("Custody file %s owner = %d:%d, want operator directory owner %d:%d", path, uid, gid, wantUID, wantGID)
		}
	}
	if len(chownCalls) != 2 || chownCalls[0] != "storage.ext4" || !strings.HasPrefix(chownCalls[1], ".custody-manifest.tmp-") {
		t.Fatalf("resumed Custody export ownership calls = %v, want disk then durable manifest temp", chownCalls)
	}
}

func TestComputerCustodyExportRejectsSymlinkBackIntoManagedRoot(t *testing.T) {
	root, system, source := publishedStorageCopySource(t)
	alias := filepath.Join(t.TempDir(), "managed-root-alias")
	if err := os.Symlink(root, alias); err != nil {
		t.Fatal(err)
	}
	request := custodyExportTestRequest(source, filepath.Join(alias, "operator-custody"))
	engine := &ContainerdEngine{config: NativeEngineConfig{RuntimeRoot: root}, diskSystem: system}
	response, err := engine.ExportComputerCustody(t.Context(), request)
	if err != nil || response.Receipt.Kind != "computer_custody_export_failed" || response.Receipt.FailureCode != "managed_root_path" {
		t.Fatalf("Custody export managed-root rejection = %+v err=%v", response, err)
	}
	if _, err := os.Lstat(filepath.Join(root, "operator-custody")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("rejected Custody path created managed bytes: %v", err)
	}
}

func TestComputerCustodyExportCommitBarrierAndCancellationAreTyped(t *testing.T) {
	root, system, source := publishedStorageCopySource(t)
	externalRoot := filepath.Join(t.TempDir(), "operator-custody")
	request := custodyExportTestRequest(source, externalRoot)
	committed := false
	engine := &ContainerdEngine{config: NativeEngineConfig{RuntimeRoot: root}, diskSystem: system,
		computerCustodyHook: func(phase string) error {
			if phase == "before_external_write" && !committed {
				return errors.New("Custody event was not committed")
			}
			return nil
		}}
	if _, err := engine.ExportComputerCustody(t.Context(), request); err == nil {
		t.Fatal("external write began before the durable L1 custody event")
	}
	if _, err := os.Lstat(externalRoot); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("precommit rejection wrote external bytes: %v", err)
	}
	committed = true
	ctx, cancel := context.WithCancel(t.Context())
	engine.computerCustodyCopyN = func(destination io.Writer, source io.Reader, _ int64) (int64, error) {
		payload := make([]byte, 4096)
		count, err := source.Read(payload)
		if err != nil {
			return 0, err
		}
		written, err := destination.Write(payload[:count])
		if err != nil {
			return int64(written), err
		}
		cancel()
		_, err = source.Read(payload)
		return int64(written), err
	}
	response, err := engine.ExportComputerCustody(ctx, request)
	if err != nil || response.Receipt.Kind != "computer_custody_export_failed" ||
		response.Receipt.FailureCode != "cancelled" {
		t.Fatalf("cancelled Custody export = %+v err=%v", response, err)
	}
	partial, err := os.Stat(filepath.Join(externalRoot, "storage.ext4"))
	if err != nil || partial.Size() == 0 {
		t.Fatalf("cancelled export did not retain possibly-secret partial bytes: %#v err=%v", partial, err)
	}
}

func TestComputerCustodyImportRejectsTamperThenResumesMidImport(t *testing.T) {
	root, system, source := publishedStorageCopySource(t)
	externalRoot := filepath.Join(t.TempDir(), "operator-custody")
	exportRequest := custodyExportTestRequest(source, externalRoot)
	engine := &ContainerdEngine{config: NativeEngineConfig{RuntimeRoot: root}, diskSystem: system}
	exported, err := engine.ExportComputerCustody(t.Context(), exportRequest)
	if err != nil {
		t.Fatal(err)
	}
	importRequest := storageCopyTestRequest(source, "import", source.Receipt.AllocatedSize)
	importRequest.Destination.ComputerID = "import-computer"
	importRequest.Destination.StorageID = "import-storage"
	importRequest.Destination.StorageGeneration = 1
	importRequest.ExportID = exportRequest.ExportID
	importRequest.ExternalPath = externalRoot
	importRequest.ManifestDigest = exported.Receipt.ManifestDigest
	importRequest.Authority.JobID = "import-job"

	manifestPath := filepath.Join(externalRoot, "custody.json")
	manifest, err := os.ReadFile(manifestPath)
	if err != nil {
		t.Fatal(err)
	}
	tampered := append([]byte(nil), manifest...)
	tampered[len(tampered)-2] ^= 1
	if err := os.WriteFile(manifestPath, tampered, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := engine.CopyComputerStorage(t.Context(), importRequest); err == nil {
		t.Fatal("Custody import accepted a tampered manifest")
	}
	name, _ := deterministicComputerDiskName(importRequest.Destination)
	if _, err := os.Lstat(filepath.Join(root, "computer-disks", name)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("tampered import created managed destination: %v", err)
	}
	if err := os.WriteFile(manifestPath, manifest, 0o600); err != nil {
		t.Fatal(err)
	}
	crash := errors.New("injected mid-import crash")
	engine = &ContainerdEngine{config: NativeEngineConfig{RuntimeRoot: root}, diskSystem: system,
		storageCopyFinalize: importFinalize}
	engine.storageCopyHook = func(phase computerStorageCopyPhase) error {
		if phase == computerStorageCopyCopied {
			return crash
		}
		return nil
	}
	if _, err := engine.CopyComputerStorage(t.Context(), importRequest); !errors.Is(err, crash) {
		t.Fatalf("mid-import failure = %v, want injected crash", err)
	}
	engine = &ContainerdEngine{config: NativeEngineConfig{RuntimeRoot: root}, diskSystem: system,
		storageCopyFinalize: importFinalize}
	imported, err := engine.CopyComputerStorage(t.Context(), importRequest)
	if err != nil || imported.Receipt.Operation != "import" || !imported.Receipt.OSIdentityRekeyed ||
		imported.Receipt.ExportID != exportRequest.ExportID || imported.Receipt.ManifestDigest != exported.Receipt.ManifestDigest {
		t.Fatalf("resumed Custody import = %+v err=%v", imported, err)
	}
}
