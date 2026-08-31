//go:build service_acceptance_realtiming && (darwin || linux)

package ocihelper

import (
	"context"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

const helperChildEnvironment = "WEFTY_OCI_HELPER_CHILD"

func TestServiceAcceptanceRealtimeRunsHelperChildWithFakeEngine(t *testing.T) {
	socketPath := startRealtimeHelperChild(t)

	client := NewUnixClient(socketPath, "realtime-fake-checksum")
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	session, err := client.OpenSession(ctx, AcquireSessionRequest{
		NodeID: "realtime-node", BootSessionID: "realtime-boot",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	if _, err := session.Sweep(ctx, SweepRequest{}); err != nil {
		t.Fatal(err)
	}
	verification, err := session.Verify(ctx, VerifyRequest{Scope: VerifyNamespace})
	if err != nil {
		t.Fatal(err)
	}
	if !verification.Absent {
		t.Fatal("namespace residue remained after sweep")
	}
	if err := session.EnsureImage(ctx, EnsureImageRequest{Reference: "example.invalid/probe", Digest: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", Platform: testImagePlatform}, nil); err != nil {
		t.Fatal(err)
	}
	authority := AttemptAuthority{
		NodeID: "realtime-node", JobID: "realtime-job", AttemptID: "realtime-attempt",
		FencingToken: "realtime-fence", BootSessionID: "realtime-boot", Class: "one-shot",
		RemovalGeneration: "realtime-removal",
	}
	if _, err := session.Run(ctx, testRunRequest(authority, 5*time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := session.Signal(ctx, SignalRequest{Authority: authority, Signal: "TERM"}); err != nil {
		t.Fatal(err)
	}
	if err := session.Watch(ctx, WatchRequest{Authority: authority}, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := session.Verify(ctx, VerifyRequest{Scope: VerifyAttempt, Authority: &authority}); err != nil {
		t.Fatal(err)
	}
	if _, err := session.Delete(ctx, DeleteRequest{Authority: authority}); err != nil {
		t.Fatal(err)
	}
	if got := session.Handshake().ProtocolVersion; got != ProtocolVersion {
		t.Fatalf("real helper process protocol version = %d, want exact %d", got, ProtocolVersion)
	}
	computerAuthority := authority
	computerAuthority.JobID = "realtime-computer-job"
	computerAuthority.AttemptID = "realtime-computer-attempt"
	computerAuthority.Class = "service"
	computerRequest := testRunRequest(computerAuthority, 5*time.Second)
	computerRequest.Workload.Computer = true
	computerRequest.Workload.Limits.MemoryBytes = 1 << 30
	computerRequest.Workload.ManagedVolumes = testComputerManagedVolumes()
	computerRequest.AllocateEndpoints = []string{"view", "control"}
	if _, err := session.Run(ctx, computerRequest); err != nil {
		t.Fatal(err)
	}
	if err := session.SetComputerControlState(ctx, SetComputerControlStateRequest{Authority: computerAuthority, HumanDriving: true}); err != nil {
		t.Fatal(err)
	}
	if _, err := session.Delete(ctx, DeleteRequest{Authority: computerAuthority}); err != nil {
		t.Fatal(err)
	}
	backupRequest := CreateComputerBackupRequest{BackupID: "realtime-backup", CopyID: "realtime-copy",
		Storage: ComputerStorageReference{ComputerID: "realtime-computer", StorageID: "realtime-storage",
			StorageGeneration: 1, IntentRevision: 2, DiskBytes: 8 << 30},
		Authority: ComputerBackupAuthority{NodeID: "realtime-node", BootSessionID: "realtime-boot",
			HelperGeneration: session.Handshake().SessionGeneration, RootInstanceID: "realtime-root",
			JobID: "realtime-backup-job", PriorJobID: "realtime-computer-job", OperationRevision: 2, CleanupFence: "realtime-backup-fence"}}
	if _, err := session.CreateComputerBackup(ctx, backupRequest); err != nil {
		t.Fatal(err)
	}
	backupRequest.Authority.CleanupFence = "realtime-prune-fence"
	deleteBackupRequest := DeleteComputerBackupCopyRequest{BackupID: backupRequest.BackupID, CopyID: backupRequest.CopyID,
		Storage: backupRequest.Storage, Authority: backupRequest.Authority}
	if removed, err := session.DeleteComputerBackupCopy(ctx, deleteBackupRequest); err != nil || !removed.Receipt.Absent {
		t.Fatalf("real helper process Backup prune = %+v err=%v", removed, err)
	}
}

func startRealtimeHelperChild(t *testing.T) string {
	t.Helper()
	directory, err := os.MkdirTemp("", "wefty-oci-child-")
	if err != nil {
		t.Fatal(err)
	}
	socketPath := filepath.Join(directory, "helper.sock")
	listener, err := net.Listen("unix", socketPath)
	if err != nil {
		t.Fatal(err)
	}
	unixListener := listener.(*net.UnixListener)
	unixListener.SetUnlinkOnClose(false)
	listenerFile, err := unixListener.File()
	if err != nil {
		t.Fatal(err)
	}
	command := exec.Command(os.Args[0], "-test.run=^TestRealtimeOCIHelperChildProcess$")
	command.Env = append(os.Environ(), helperChildEnvironment+"=1")
	command.ExtraFiles = []*os.File{listenerFile}
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	_ = listenerFile.Close()
	_ = listener.Close()
	t.Cleanup(func() {
		_ = command.Process.Kill()
		_ = command.Wait()
		_ = os.RemoveAll(directory)
	})
	return socketPath
}

func TestRealtimeOCIHelperChildProcess(t *testing.T) {
	if os.Getenv(helperChildEnvironment) != "1" {
		return
	}
	engine := &realtimeFakeEngine{fakeEngine: newFakeEngine()}
	if err := RunInvocation(context.Background(), []string{"wefty-agent", InvocationArg}, engine, ServerConfig{
		HelperVersion: "realtime-fake", HelperChecksum: "realtime-fake-checksum",
		HeartbeatTimeout: 10 * time.Second, MaximumAttemptDeadman: 10 * time.Second,
		AllowedUIDs: []uint32{uint32(os.Getuid())},
	}); err != nil {
		t.Fatal(err)
	}
}

type realtimeFakeEngine struct{ *fakeEngine }

func (engine *realtimeFakeEngine) Run(ctx context.Context, request RunRequest) (RunResponse, error) {
	response := RunResponse{Started: true, StartedAt: time.Now().UTC()}
	if len(request.AllocateEndpoints) != 0 {
		response.Endpoints = make(map[string]uint16, len(request.AllocateEndpoints))
		for index, name := range request.AllocateEndpoints {
			response.Endpoints[name] = uint16(42120 + index)
		}
	}
	engine.setRunResponse(response)
	return engine.fakeEngine.Run(ctx, request)
}

func (*realtimeFakeEngine) CreateComputerBackup(_ context.Context, request CreateComputerBackupRequest) (CreateComputerBackupResponse, error) {
	return CreateComputerBackupResponse{Receipt: ComputerBackupCopyReceipt{
		Kind: "computer_backup_copy_verified", ReceiptID: "realtime-backup-receipt",
		BackupID: request.BackupID, CopyID: request.CopyID,
		ComputerID: request.Storage.ComputerID, StorageID: request.Storage.StorageID,
		StorageGeneration: request.Storage.StorageGeneration, NodeID: request.Authority.NodeID,
		RootInstanceID: request.Authority.RootInstanceID, JobID: request.Authority.JobID,
		OperationRevision: request.Authority.OperationRevision, CleanupFence: request.Authority.CleanupFence,
		HelperGeneration: request.Authority.HelperGeneration, AllocatedSize: request.Storage.DiskBytes,
		ContentDigest: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", Encryption: "none",
	}}, nil
}

func (*realtimeFakeEngine) DeleteComputerBackupCopy(_ context.Context, request DeleteComputerBackupCopyRequest) (DeleteComputerBackupCopyResponse, error) {
	return DeleteComputerBackupCopyResponse{Receipt: ComputerBackupCopyRemovalReceipt{
		Kind: "computer_backup_copy_removed", ReceiptID: "realtime-backup-removal-receipt",
		BackupID: request.BackupID, CopyID: request.CopyID,
		ComputerID: request.Storage.ComputerID, StorageID: request.Storage.StorageID,
		StorageGeneration: request.Storage.StorageGeneration, NodeID: request.Authority.NodeID,
		RootInstanceID: request.Authority.RootInstanceID, OperationRevision: request.Authority.OperationRevision,
		CleanupFence: request.Authority.CleanupFence, HelperGeneration: request.Authority.HelperGeneration, Absent: true,
	}}, nil
}
