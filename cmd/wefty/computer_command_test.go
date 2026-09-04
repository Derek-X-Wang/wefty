package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/l1"
)

const computerCLITestDigest = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
const computerCLITestDigestB = "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"

func TestComputerCLIResolvesFriendlyNameAfterExactID(t *testing.T) {
	harness, computer, _, _ := newRunningComputerCLIFixture(t, "primary-handle")
	ctx := context.Background()

	byID := runServiceCLI(t, ctx, harness.clients, true, "services", "status", computer.ComputerID)
	byName := runServiceCLI(t, ctx, harness.clients, true, "services", "status", computer.Name)
	if !bytes.Equal(byName, byID) {
		t.Fatalf("services status <name> = %s, want services status <id> = %s", byName, byID)
	}

	collision := createComputerCLIProjection(t, ctx, harness.clients, computer.ComputerID, "id-shaped-name-create")
	resolved := runServiceCLI(t, ctx, harness.clients, true, "services", "status", computer.ComputerID)
	var projection computerOperatorProjection
	if err := json.Unmarshal(resolved, &projection); err != nil {
		t.Fatal(err)
	}
	if projection.ComputerID != computer.ComputerID || projection.ComputerID == collision.ComputerID {
		t.Fatalf("exact ID resolution selected %q, want %q ahead of same-shaped friendly name %q",
			projection.ComputerID, computer.ComputerID, collision.ComputerID)
	}
}

func TestComputerVerbResolvesExactIDBeforeCollidingFriendlyName(t *testing.T) {
	harness, computer, _, _ := newRunningComputerCLIFixture(t, "exact-id-target")
	ctx := context.Background()
	collision := createComputerCLIProjection(t, ctx, harness.clients, computer.ComputerID, "exact-id-collision")

	output := runServiceCLI(t, ctx, harness.clients, true, "services", "reimage", computer.ComputerID,
		"--expect-current", "--image", "ghcr.io/example/exact-id@"+computerCLITestDigestB,
		"--idempotency-key", "exact-id-reimage", "--terminate-sessions")
	var projected computerOperatorProjection
	if err := json.Unmarshal(output, &projected); err != nil {
		t.Fatal(err)
	}
	if projected.ComputerID != computer.ComputerID || projected.ComputerID == collision.ComputerID {
		t.Fatalf("Computer reimage selected %q, want exact ID %q ahead of same-shaped friendly name on %q",
			projected.ComputerID, computer.ComputerID, collision.ComputerID)
	}
}

func TestClientPrincipalResolvesComputerNameForReimageAndBackupList(t *testing.T) {
	harness, computer, _, _ := newRunningComputerCLIFixture(t, "client-name-target")
	ctx := context.Background()

	backupOutput := runServiceCLI(t, ctx, harness.clients, true, "services", "backup", "list", computer.Name)
	var backups computerBackupInventory
	if err := json.Unmarshal(backupOutput, &backups); err != nil || backups.Backups == nil {
		t.Fatalf("Backup list by friendly name = %#v err=%v output=%s", backups, err, backupOutput)
	}
	reimageOutput := runServiceCLI(t, ctx, harness.clients, true, "services", "reimage", computer.Name,
		"--expect-current", "--image", "ghcr.io/example/client-name@"+computerCLITestDigestB,
		"--idempotency-key", "client-name-reimage", "--terminate-sessions")
	var reimaged computerOperatorProjection
	if err := json.Unmarshal(reimageOutput, &reimaged); err != nil || reimaged.ComputerID != computer.ComputerID {
		t.Fatalf("Computer reimage by friendly name = %#v err=%v output=%s", reimaged, err, reimageOutput)
	}
}

func TestComputerCLIRealRoutesDefaultsLifecycleAndReplay(t *testing.T) {
	harness := newServiceCLIHarness(t)
	harness.clients.images = &fakeImageResolver{digest: computerCLITestDigest}
	ctx := context.Background()
	if _, err := harness.store.RegisterNode(ctx, fabric.Identity{NodeID: "fabric-computer-node"}, contract.NodeRegistration{
		NodeID: "computer-node", BootSessionID: "boot-computer-cli", RootInstanceID: "root-computer-cli",
		OS: "linux", Architecture: "amd64", AgentVersion: "computer-cli-test",
		Capabilities: map[string]bool{"kind:oci": true, "cgroup_v2": true, "computer": true},
	}, l1.NodePolicy{Tags: []string{contract.StableNodeTagPrefix + "computer-node"}, MaxOneshotSlots: 2, MaxServiceSlots: 8}, true); err != nil {
		t.Fatal(err)
	}

	createdBytes := runServiceCLI(t, ctx, harness.clients, true, "services", "create", "--computer",
		"--name", "alice", "--image", "ghcr.io/example/alice:latest", "--node", "computer-node",
		"--idempotency-key", "alice-computer")
	var created computerOperatorProjection
	if err := json.Unmarshal(createdBytes, &created); err != nil {
		t.Fatal(err)
	}
	if created.ComputerID == "" || created.CurrentJobID == "" || created.ComputerID == created.CurrentJobID {
		t.Fatalf("Computer/Job identity projection = %q/%q", created.ComputerID, created.CurrentJobID)
	}
	if created.Capacity.RequestedMemoryBytes == nil || *created.Capacity.RequestedMemoryBytes != defaultComputerMemoryBytes ||
		created.Capacity.RequestedDiskBytes != defaultComputerDiskBytes ||
		created.DesiredDiskBytes != defaultComputerDiskBytes || created.StorageGeneration != 1 || created.BackupCap != 0 {
		t.Fatalf("explicit Computer defaults = %#v", created.Capacity)
	}
	if created.MutationApplied == nil || !*created.MutationApplied || created.IdempotentReplay == nil || *created.IdempotentReplay {
		t.Fatalf("creation mutation receipt = applied %v replay %v", created.MutationApplied, created.IdempotentReplay)
	}
	if created.DisplayEndpoint != nil || created.ControllerTenure != contract.ComputerControlTenureFree ||
		created.Capacity.LastGrow.Status != "NOT-RUN" || created.Capacity.ActiveFailure.Status != "NOT-RUN" {
		t.Fatalf("unavailable projection facts were overstated: %#v", created)
	}

	replayedBytes := runServiceCLI(t, ctx, harness.clients, true, "services", "create", "--computer",
		"--name", "alice", "--image", "ghcr.io/example/alice:latest", "--node", "computer-node",
		"--idempotency-key", "alice-computer")
	var replayed computerOperatorProjection
	if err := json.Unmarshal(replayedBytes, &replayed); err != nil {
		t.Fatal(err)
	}
	if replayed.ComputerID != created.ComputerID || replayed.MutationApplied == nil || *replayed.MutationApplied ||
		replayed.IdempotentReplay == nil || !*replayed.IdempotentReplay {
		t.Fatalf("creation replay receipt = %#v", replayed)
	}

	second := runServiceCLI(t, ctx, harness.clients, true, "services", "create", "--computer",
		"--name", "bob", "--image", "ghcr.io/example/bob@"+computerCLITestDigest, "--node", "computer-node",
		"--idempotency-key", "bob-computer")
	if !bytes.Contains(second, []byte(`"computer_id"`)) {
		t.Fatalf("second Computer output = %s", second)
	}
	firstPage := runServiceCLI(t, ctx, harness.clients, true, "services", "list", "--limit", "1")
	var page serviceListOutput
	if err := json.Unmarshal(firstPage, &page); err != nil {
		t.Fatal(err)
	}
	if len(page.Jobs) != 1 || page.Jobs[0].ComputerID != created.ComputerID || page.Jobs[0].JobID != created.CurrentJobID || page.NextCursor == "" {
		t.Fatalf("first Computer page = %#v", page)
	}
	secondPage := runServiceCLI(t, ctx, harness.clients, true, "services", "list", "--limit", "1", "--cursor", page.NextCursor)
	if err := json.Unmarshal(secondPage, &page); err != nil || len(page.Jobs) != 1 || page.Jobs[0].ComputerID == "" {
		t.Fatalf("second Computer page = %#v, err=%v", page, err)
	}

	humanList := runServiceCLI(t, ctx, harness.clients, false, "services", "list", "--limit", "1")
	for _, want := range []string{"KIND", "COMPUTER ID", "Computer", created.ComputerID, "NEXT CURSOR"} {
		if !bytes.Contains(humanList, []byte(want)) {
			t.Fatalf("human service list missing %q:\n%s", want, humanList)
		}
	}
	human := runServiceCLI(t, ctx, harness.clients, false, "services", "status", created.ComputerID)
	for _, want := range []string{"COMPUTER ID", "DESIRED", "OBSERVED", "STORAGE", "INTENT/APPLIED", "PHASE",
		"JOB ID", "MEMORY", "DISK", "BACKUP CAP", "DISPLAY ENDPOINT", "CONTROLLER TENURE", "LAST FAILURE", created.ComputerID, created.CurrentJobID} {
		if !bytes.Contains(human, []byte(want)) {
			t.Fatalf("human Computer projection missing %q:\n%s", want, human)
		}
	}

	stopped := runComputerMutation(t, ctx, harness.clients, "stop", created.Computer,
		"--intent-revision", "1", "--storage-id", created.StorageID, "--storage-generation", "1")
	if stopped.IntentRevision != 2 || stopped.DesiredState != contract.ServiceDesiredStopped ||
		stopped.MutationApplied == nil || !*stopped.MutationApplied {
		t.Fatalf("stopped Computer = %#v", stopped)
	}
	stoppedAgain := runServiceCLI(t, ctx, harness.clients, true, "services", "stop", created.ComputerID, "--expect-current")
	if err := json.Unmarshal(stoppedAgain, &stopped); err != nil || stopped.MutationApplied == nil || *stopped.MutationApplied {
		t.Fatalf("idempotent desired-state no-op = %#v, err=%v", stopped, err)
	}

	started := runComputerMutation(t, ctx, harness.clients, "start", stopped.Computer,
		"--intent-revision", "2", "--storage-id", stopped.StorageID, "--storage-generation", "1")
	if started.IntentRevision != 3 || started.DesiredState != contract.ServiceDesiredRunning {
		t.Fatalf("started Computer = %#v", started)
	}
	stopped = runComputerMutation(t, ctx, harness.clients, "stop", started.Computer,
		"--intent-revision", "3", "--storage-id", started.StorageID, "--storage-generation", "1")
	restartedBytes := runServiceCLI(t, ctx, harness.clients, true, "services", "restart", created.ComputerID,
		"--intent-revision", "4", "--storage-id", created.StorageID, "--storage-generation", "1",
		"--idempotency-key", "alice-restart")
	var restarted computerOperatorProjection
	if err := json.Unmarshal(restartedBytes, &restarted); err != nil || restarted.IntentRevision != 5 ||
		restarted.MutationApplied == nil || !*restarted.MutationApplied {
		t.Fatalf("restarted Computer = %#v, err=%v", restarted, err)
	}
	restartReplay := runServiceCLI(t, ctx, harness.clients, true, "services", "restart", created.ComputerID,
		"--intent-revision", "4", "--storage-id", created.StorageID, "--storage-generation", "1",
		"--idempotency-key", "alice-restart")
	if err := json.Unmarshal(restartReplay, &restarted); err != nil || restarted.MutationApplied == nil || *restarted.MutationApplied ||
		restarted.IdempotentReplay == nil || !*restarted.IdempotentReplay {
		t.Fatalf("restart replay = %#v, err=%v", restarted, err)
	}

	reimageSubject := createComputerCLIProjection(t, ctx, harness.clients, "reimage-subject", "reimage-subject")
	reimageBytes := runServiceCLI(t, ctx, harness.clients, true, "services", "reimage", reimageSubject.ComputerID,
		"--image", "ghcr.io/example/reimaged@"+computerCLITestDigestB,
		"--intent-revision", "1", "--storage-id", reimageSubject.StorageID, "--storage-generation", "1",
		"--idempotency-key", "reimage-subject-v2")
	var reimaged computerOperatorProjection
	if err := json.Unmarshal(reimageBytes, &reimaged); err != nil || reimaged.IntentRevision != 2 ||
		reimaged.ReconfigurationPhase != l1.ComputerReconfigurationReimaging || reimaged.MutationApplied == nil || !*reimaged.MutationApplied {
		t.Fatalf("reimage projection = %#v, err=%v", reimaged, err)
	}
	reimageReplay := runServiceCLI(t, ctx, harness.clients, true, "services", "reimage", reimageSubject.ComputerID,
		"--image", "ghcr.io/example/reimaged@"+computerCLITestDigestB,
		"--intent-revision", "1", "--storage-id", reimageSubject.StorageID, "--storage-generation", "1",
		"--idempotency-key", "reimage-subject-v2")
	if err := json.Unmarshal(reimageReplay, &reimaged); err != nil || reimaged.IdempotentReplay == nil || !*reimaged.IdempotentReplay ||
		reimaged.MutationApplied == nil || *reimaged.MutationApplied {
		t.Fatalf("reimage replay projection = %#v, err=%v", reimaged, err)
	}

	resetSubject := createComputerCLIProjection(t, ctx, harness.clients, "reset-subject", "reset-subject")
	resetBytes := runServiceCLI(t, ctx, harness.clients, true, "services", "reset", resetSubject.ComputerID,
		"--expect-current", "--idempotency-key", "reset-subject-v2")
	var reset computerOperatorProjection
	if err := json.Unmarshal(resetBytes, &reset); err != nil || reset.ReconfigurationPhase != l1.ComputerReconfigurationResetting ||
		reset.IntentRevision != 2 || reset.MutationApplied == nil || !*reset.MutationApplied {
		t.Fatalf("reset projection = %#v, err=%v", reset, err)
	}

	growSubject := createComputerCLIProjection(t, ctx, harness.clients, "grow-subject", "grow-subject")
	growBytes := runServiceCLI(t, ctx, harness.clients, true, "services", "resize", growSubject.ComputerID,
		"--expect-current", "--disk-bytes", fmt.Sprint(9<<30), "--idempotency-key", "grow-subject-v2")
	var grown computerOperatorProjection
	if err := json.Unmarshal(growBytes, &grown); err != nil || grown.ReconfigurationPhase != l1.ComputerReconfigurationGrowing ||
		grown.IntentRevision != 2 || grown.MutationApplied == nil || !*grown.MutationApplied {
		t.Fatalf("grow projection = %#v, err=%v", grown, err)
	}

	removeSubject := createComputerCLIProjection(t, ctx, harness.clients, "remove-subject", "remove-subject")
	removeBytes := runServiceCLI(t, ctx, harness.clients, true, "services", "remove", removeSubject.ComputerID,
		"--expect-current", "--wait", "100ms", "--poll-interval", "1ms")
	var removed computerOperatorProjection
	if err := json.Unmarshal(removeBytes, &removed); err != nil || removed.DesiredState != contract.ServiceDesiredRemoved ||
		removed.CurrentJob.State != contract.JobRemovedVerified || removed.RemovalOutcome != "removed_verified" ||
		removed.MutationApplied == nil || !*removed.MutationApplied || removed.Observation == nil || removed.Observation.Status != "observed" {
		t.Fatalf("remove projection = %#v, err=%v", removed, err)
	}
	removeReplay := runServiceCLI(t, ctx, harness.clients, true, "services", "remove", removeSubject.ComputerID, "--expect-current")
	if err := json.Unmarshal(removeReplay, &removed); err != nil || removed.MutationApplied == nil || *removed.MutationApplied {
		t.Fatalf("remove replay projection = %#v, err=%v", removed, err)
	}
}

func TestComputerCLIRemoveWaitsForDefinitiveSlotReleasingOutcome(t *testing.T) {
	harness, computer, node, _ := newRunningComputerCLIFixture(t, "remove-wait")
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	finalized := make(chan error, 1)
	pendingObserved := make(chan struct{})
	allowFinalize := make(chan struct{})
	go func() {
		for {
			directives, err := harness.store.ListNodeRemovalDirectives(ctx, "fabric-"+node.NodeID, node.NodeID, node.BootSessionID)
			if err != nil {
				finalized <- err
				return
			}
			if len(directives) == 0 {
				if err := harness.clients.wait(ctx, time.Millisecond); err != nil {
					finalized <- err
					return
				}
				continue
			}
			directive := directives[0]
			nodes, err := harness.clients.listNodes(ctx)
			if err != nil || len(nodes.Nodes) != 1 || nodes.Nodes[0].NodeID != node.NodeID || nodes.Nodes[0].ServiceOccupancy != 1 {
				finalized <- fmt.Errorf("pending removal nodes = %#v err=%v", nodes, err)
				return
			}
			close(pendingObserved)
			select {
			case <-allowFinalize:
			case <-ctx.Done():
				finalized <- ctx.Err()
				return
			}
			_, err = harness.store.AcknowledgeServiceRemoval(ctx, "fabric-"+node.NodeID, computer.CurrentJobID,
				l1.RemovalAcknowledgementRequest{NodeID: node.NodeID, BootSessionID: node.BootSessionID,
					RemovalGeneration: directive.RemovalGeneration, CleanupFence: directive.CleanupFence,
					RootInstanceID: directive.RootInstanceID, IdempotencyKey: "remove-wait-cleaned"})
			if err != nil {
				finalized <- err
				return
			}
			_, changed, err := harness.store.FinalizeServiceRemoval(ctx, computer.CurrentJobID)
			if err == nil && !changed {
				err = errors.New("Computer removal acknowledgement did not finalize")
			}
			finalized <- err
			return
		}
	}()

	var stdout, stderr bytes.Buffer
	executeDone := make(chan error, 1)
	go func() {
		executeDone <- execute(ctx, harness.clients, true, []string{"services", "remove", computer.ComputerID,
			"--expect-current", "--wait", "1s", "--poll-interval", "1ms"}, &stdout, &stderr)
	}()
	select {
	case <-pendingObserved:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	select {
	case err := <-executeDone:
		t.Fatalf("Computer remove returned while the pending removal held its Slot: %v", err)
	default:
	}
	close(allowFinalize)
	err := <-executeDone
	if err != nil {
		t.Fatalf("await Computer removal: %v stderr=%s", err, stderr.String())
	}
	if err := <-finalized; err != nil {
		t.Fatal(err)
	}
	nodes, err := harness.clients.listNodes(ctx)
	if err != nil || len(nodes.Nodes) != 1 || nodes.Nodes[0].NodeID != node.NodeID || nodes.Nodes[0].ServiceOccupancy != 0 {
		t.Fatalf("awaited removal nodes = %#v err=%v", nodes, err)
	}
	var removed computerOperatorProjection
	if err := json.Unmarshal(stdout.Bytes(), &removed); err != nil || removed.CurrentJob.State != contract.JobRemovedVerified ||
		removed.RemovalOutcome != "removed_verified" ||
		removed.CurrentJob.Removal == nil || removed.CurrentJob.Removal.CleanupAcknowledgedAt == nil ||
		removed.Observation == nil || removed.Observation.Status != "observed" {
		t.Fatalf("awaited Computer removal = %#v decode=%v output=%s", removed, err, stdout.String())
	}
}

func TestComputerCLIRemoveWaitReturnsTypedCleanupQuarantine(t *testing.T) {
	harness, computer, node, _ := newRunningComputerCLIFixture(t, "remove-quarantine")
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	acknowledged := make(chan error, 1)
	go func() {
		for {
			directives, err := harness.store.ListNodeRemovalDirectives(ctx, "fabric-"+node.NodeID, node.NodeID, node.BootSessionID)
			if err != nil {
				acknowledged <- err
				return
			}
			if len(directives) == 0 {
				if err := harness.clients.wait(ctx, time.Millisecond); err != nil {
					acknowledged <- err
					return
				}
				continue
			}
			directive := directives[0]
			receipt := l1.ComputerStorageCleanupQuarantine{
				Kind: "managed_volume_cleanup_quarantined", Operation: l1.ComputerStorageCleanupRemoval,
				ReceiptID: "remove-quarantine-receipt", VolumeKind: "computer_disk",
				ComputerID: computer.ComputerID, StorageID: computer.StorageID, StorageGeneration: computer.StorageGeneration,
				NodeID: node.NodeID, BootSessionID: node.BootSessionID, JobID: computer.CurrentJobID,
				RemovalGeneration: directive.RemovalGeneration, CleanupFence: directive.CleanupFence,
				FailureReason: "operation_failed", Attempts: 3,
			}
			_, err = harness.store.AcknowledgeServiceRemoval(ctx, "fabric-"+node.NodeID, computer.CurrentJobID,
				l1.RemovalAcknowledgementRequest{NodeID: node.NodeID, BootSessionID: node.BootSessionID,
					RemovalGeneration: directive.RemovalGeneration, CleanupFence: directive.CleanupFence,
					RootInstanceID: directive.RootInstanceID, IdempotencyKey: receipt.ReceiptID, CleanupQuarantine: &receipt})
			acknowledged <- err
			return
		}
	}()

	var stdout, stderr bytes.Buffer
	err := execute(ctx, harness.clients, true, []string{"services", "remove", computer.ComputerID,
		"--expect-current", "--wait", "1s", "--poll-interval", "1ms"}, &stdout, &stderr)
	if ackErr := <-acknowledged; ackErr != nil {
		t.Fatal(ackErr)
	}
	var responseErr *apiResponseError
	if !errors.As(err, &responseErr) || responseErr.APIError.Code != contract.ErrorConflict || commandExitCode(err) != exitConflict {
		t.Fatalf("quarantined Computer removal = %T %v, want typed conflict", err, err)
	}
	var quarantined computerOperatorProjection
	if decodeErr := json.Unmarshal(stdout.Bytes(), &quarantined); decodeErr != nil || len(quarantined.StorageCleanupQuarantines) != 1 ||
		quarantined.StorageCleanupQuarantines[0].ReceiptID != "remove-quarantine-receipt" ||
		quarantined.CurrentJob.State != contract.JobRemovalPending || quarantined.CurrentJob.Removal == nil ||
		quarantined.CurrentJob.Removal.CleanupStatus != l1.ServiceRemovalCleanupQuarantined ||
		quarantined.CurrentJob.Removal.RemovalOutcome != l1.ServiceRemovalOutcomeCleanupQuarantined ||
		quarantined.Observation == nil || quarantined.Observation.Status != "observed" {
		t.Fatalf("quarantined Computer projection = %#v decode=%v output=%s", quarantined, decodeErr, stdout.String())
	}
}

func TestComputerRemovalTerminalWinsOverStaleQuarantineEvidence(t *testing.T) {
	removedAt := time.Now().UTC()
	computer := l1.Computer{ComputerID: "computer", CurrentJobID: "job", DesiredState: contract.ServiceDesiredRemoved,
		RemovalOutcome: "removed_verified", CurrentJob: l1.Job{JobID: "job", State: contract.JobRemovedVerified,
			Removal: &l1.ServiceRemoval{RemovalDesiredState: contract.ServiceDesiredRemoved, RemovalBoundNodeID: "node",
				RemovalGeneration: 1, CleanupStatus: l1.ServiceRemovalCleanupAcknowledged,
				RemovalOutcome: l1.ServiceRemovalVerified, CleanupAcknowledgedAt: &removedAt}},
		StorageCleanupQuarantines: []l1.ComputerStorageCleanupQuarantine{{Kind: "managed_volume_cleanup_quarantined",
			Operation: l1.ComputerStorageCleanupRemoval, ReceiptID: "stale", JobID: "job", RemovalGeneration: 1}}}
	if err := awaitedComputerRemovalOutcome(computer); err != nil {
		t.Fatalf("verified removal was shadowed by stale quarantine: %v", err)
	}
}

func TestComputerRemovalMismatchReportsPendingSlotWithoutServiceProjection(t *testing.T) {
	computer := l1.Computer{ComputerID: "computer", CurrentJobID: "job", CurrentJob: l1.Job{
		JobID: "job", State: contract.JobRemovalPending, Removal: &l1.ServiceRemoval{
			RemovalDesiredState: contract.ServiceDesiredRemoved, RemovalGeneration: 1,
			CleanupStatus: l1.ServiceRemovalCleanupPending}}}
	err := computerRemovalOutcomeMismatch(computer, "not terminal")
	var responseErr *apiResponseError
	if !errors.As(err, &responseErr) || responseErr.APIError.Details["holds_slot"] != true {
		t.Fatalf("pending removal diagnostic = %#v", err)
	}
}

func TestComputerCLIMutationNegativeReceiptFailsEveryRow(t *testing.T) {
	harness := newServiceCLIHarness(t)
	harness.clients.images = &fakeImageResolver{digest: computerCLITestDigest}
	ctx := context.Background()
	createdBytes := runServiceCLI(t, ctx, harness.clients, true, "services", "create", "--computer",
		"--name", "negative-subject", "--image", "ghcr.io/example/negative@"+computerCLITestDigest,
		"--node", "computer-node", "--idempotency-key", "negative-subject")
	var created computerOperatorProjection
	if err := json.Unmarshal(createdBytes, &created); err != nil {
		t.Fatal(err)
	}

	stale := []string{"--intent-revision", "2", "--storage-id", created.StorageID, "--storage-generation", "1"}
	wrongStorage := []string{"--intent-revision", "1", "--storage-id", "storage-wrong", "--storage-generation", "1"}
	wrongGeneration := []string{"--intent-revision", "1", "--storage-id", created.StorageID, "--storage-generation", "2"}
	keyed := func(base []string, key string) []string {
		return append(append([]string(nil), base...), "--idempotency-key", key)
	}
	tests := []struct {
		name string
		verb string
		args []string
		code contract.ErrorCode
	}{
		{"start stale", "start", stale, contract.ErrorStaleIntentRevision},
		{"start storage", "start", wrongStorage, contract.ErrorStorageReferenceConflict},
		{"start generation", "start", wrongGeneration, contract.ErrorStorageReferenceConflict},
		{"stop stale", "stop", stale, contract.ErrorStaleIntentRevision},
		{"stop storage", "stop", wrongStorage, contract.ErrorStorageReferenceConflict},
		{"stop generation", "stop", wrongGeneration, contract.ErrorStorageReferenceConflict},
		{"restart stale", "restart", keyed(stale, "negative-restart-stale"), contract.ErrorStaleIntentRevision},
		{"restart storage", "restart", keyed(wrongStorage, "negative-restart-storage"), contract.ErrorStorageReferenceConflict},
		{"restart generation", "restart", keyed(wrongGeneration, "negative-restart-generation"), contract.ErrorStorageReferenceConflict},
		{"remove stale", "remove", stale, contract.ErrorStaleIntentRevision},
		{"remove storage", "remove", wrongStorage, contract.ErrorStorageReferenceConflict},
		{"remove generation", "remove", wrongGeneration, contract.ErrorStorageReferenceConflict},
		{"reset stale", "reset", keyed(stale, "negative-reset-stale"), contract.ErrorStaleIntentRevision},
		{"reset storage", "reset", keyed(wrongStorage, "negative-reset-storage"), contract.ErrorStorageReferenceConflict},
		{"reset generation", "reset", keyed(wrongGeneration, "negative-reset-generation"), contract.ErrorStorageReferenceConflict},
		{"grow stale", "resize", append(keyed(stale, "negative-grow-stale"), "--disk-bytes", fmt.Sprint(9<<30)), contract.ErrorStaleIntentRevision},
		{"grow storage", "resize", append(keyed(wrongStorage, "negative-grow-storage"), "--disk-bytes", fmt.Sprint(9<<30)), contract.ErrorStorageReferenceConflict},
		{"grow generation", "resize", append(keyed(wrongGeneration, "negative-grow-generation"), "--disk-bytes", fmt.Sprint(9<<30)), contract.ErrorStorageReferenceConflict},
		{"reimage stale", "reimage", append(keyed(stale, "negative-reimage-stale"), "--image", "ghcr.io/example/new@"+computerCLITestDigestB), contract.ErrorStaleIntentRevision},
		{"reimage storage", "reimage", append(keyed(wrongStorage, "negative-reimage-storage"), "--image", "ghcr.io/example/new@"+computerCLITestDigestB), contract.ErrorStorageReferenceConflict},
		{"abort stale", "abort", keyed(stale, "negative-abort-stale"), contract.ErrorStaleIntentRevision},
		{"abort storage", "abort", keyed(wrongStorage, "negative-abort-storage"), contract.ErrorStorageReferenceConflict},
		{"abort generation", "abort", keyed(wrongGeneration, "negative-abort-generation"), contract.ErrorStorageReferenceConflict},
	}
	for index, test := range tests {
		t.Run(fmt.Sprintf("%02d_%s", index+1, strings.ReplaceAll(test.name, " ", "_")), func(t *testing.T) {
			args := append([]string{"services", test.verb, created.ComputerID}, test.args...)
			var stdout, stderr bytes.Buffer
			err := execute(ctx, harness.clients, true, args, &stdout, &stderr)
			if err == nil {
				t.Fatalf("conformant subject accepted negative row; output=%s", stdout.String())
			}
			var responseErr *apiResponseError
			if !errors.As(err, &responseErr) || responseErr.APIError.Code != test.code {
				t.Fatalf("negative row error = %T %v, want %q", err, err, test.code)
			}
			if commandExitCode(err) != exitConflict {
				t.Fatalf("negative row exit = %d, want %d", commandExitCode(err), exitConflict)
			}
			current, err := harness.clients.getComputer(ctx, created.ComputerID)
			if err != nil || current.IntentRevision != 1 || current.StorageID != created.StorageID || current.StorageGeneration != 1 {
				t.Fatalf("negative row mutated authority: %#v, err=%v", current, err)
			}
		})
	}
}

func TestComputerCLIAbortRequiresDeadNodeAndReplays(t *testing.T) {
	now := time.Date(2026, 8, 28, 12, 0, 0, 0, time.UTC)
	harness := newServiceCLIHarnessWithOptions(t, l1.StoreOptions{Clock: l1.ClockFunc(func() time.Time { return now })})
	harness.clients.images = &fakeImageResolver{digest: computerCLITestDigest}
	ctx := context.Background()
	if _, err := harness.store.RegisterNode(ctx, fabric.Identity{NodeID: "abort-node"}, contract.NodeRegistration{
		NodeID: "abort-node", BootSessionID: "abort-boot", RootInstanceID: "abort-root", OS: "linux", Architecture: "amd64",
		AgentVersion: "abort-test", Capabilities: map[string]bool{"kind:oci": true, "cgroup_v2": true, "computer": true},
	}, l1.NodePolicy{Tags: []string{contract.StableNodeTagPrefix + "abort-node"}, MaxServiceSlots: 4}, true); err != nil {
		t.Fatal(err)
	}
	createdBytes := runServiceCLI(t, ctx, harness.clients, true, "services", "create", "--computer",
		"--name", "abort-subject", "--image", "ghcr.io/example/abort@"+computerCLITestDigest,
		"--node", "abort-node", "--idempotency-key", "abort-create")
	var created computerOperatorProjection
	if err := json.Unmarshal(createdBytes, &created); err != nil {
		t.Fatal(err)
	}
	reimageBytes := runServiceCLI(t, ctx, harness.clients, true, "services", "reimage", created.CurrentJobID,
		"--image", "ghcr.io/example/abort-v2@"+computerCLITestDigestB, "--expect-current", "--idempotency-key", "abort-reimage")
	var reimaged computerOperatorProjection
	if err := json.Unmarshal(reimageBytes, &reimaged); err != nil {
		t.Fatal(err)
	}
	abortArgs := []string{"services", "abort", reimaged.CurrentJobID, "--intent-revision", "2", "--storage-id", reimaged.StorageID,
		"--storage-generation", "1", "--idempotency-key", "abort-dead-node"}
	var stdout, stderr bytes.Buffer
	err := execute(ctx, harness.clients, true, abortArgs, &stdout, &stderr)
	var responseErr *apiResponseError
	if !errors.As(err, &responseErr) || responseErr.APIError.Code != contract.ErrorConflict {
		t.Fatalf("live-node abort = %T %v; output=%s", err, err, stdout.String())
	}
	now = now.Add(l1.DefaultNodeDeadAfter + time.Second)
	if _, err := harness.store.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	abortedBytes := runServiceCLI(t, ctx, harness.clients, true, abortArgs...)
	var aborted computerOperatorProjection
	if err := json.Unmarshal(abortedBytes, &aborted); err != nil {
		t.Fatal(err)
	}
	var failure contract.SpawnFailure
	if aborted.ReconfigurationPhase != l1.ComputerReconfigurationStable || aborted.MutationApplied == nil || !*aborted.MutationApplied ||
		json.Unmarshal(aborted.CurrentJob.LastFailure, &failure) != nil || failure.Code != contract.SpawnFailureReconfigurationAborted {
		t.Fatalf("dead-node abort projection = %#v failure=%#v", aborted, failure)
	}
	replayBytes := runServiceCLI(t, ctx, harness.clients, true, abortArgs...)
	if err := json.Unmarshal(replayBytes, &aborted); err != nil || aborted.IdempotentReplay == nil || !*aborted.IdempotentReplay ||
		aborted.MutationApplied == nil || *aborted.MutationApplied {
		t.Fatalf("abort replay = %#v err=%v", aborted, err)
	}
}

func TestComputerCLIRunningReconfigurationAndRemovalSupersession(t *testing.T) {
	for _, verb := range []string{"reimage", "reset"} {
		t.Run(verb, func(t *testing.T) {
			harness, computer, _, _ := newRunningComputerCLIFixture(t, "running-"+verb)
			args := []string{"services", verb, computer.CurrentJobID, "--expect-current", "--idempotency-key", "running-" + verb}
			if verb == "reimage" {
				args = append(args, "--image", "ghcr.io/example/running-v2@"+computerCLITestDigestB)
			}
			var stdout, stderr bytes.Buffer
			err := execute(context.Background(), harness.clients, true, args, &stdout, &stderr)
			var responseErr *apiResponseError
			if !errors.As(err, &responseErr) || responseErr.APIError.Code != contract.ErrorConflict {
				t.Fatalf("running %s without termination = %T %v", verb, err, err)
			}
			args = append(args, "--terminate-sessions")
			output := runServiceCLI(t, context.Background(), harness.clients, true, args...)
			var projected computerOperatorProjection
			if err := json.Unmarshal(output, &projected); err != nil || projected.CurrentJob.State != contract.JobStopping ||
				projected.MutationApplied == nil || !*projected.MutationApplied {
				t.Fatalf("running %s projection = %#v err=%v", verb, projected, err)
			}
		})
	}

	t.Run("remove supersedes reset", func(t *testing.T) {
		harness, created, _, _ := newRunningComputerCLIFixture(t, "superseded-reset")
		resetBytes := runServiceCLI(t, context.Background(), harness.clients, true, "services", "reset", created.CurrentJobID,
			"--expect-current", "--idempotency-key", "superseded-reset", "--terminate-sessions")
		var resetting computerOperatorProjection
		if err := json.Unmarshal(resetBytes, &resetting); err != nil || resetting.ReconfigurationPhase != l1.ComputerReconfigurationResetting {
			t.Fatalf("resetting projection = %#v err=%v", resetting, err)
		}
		removedBytes := runServiceCLI(t, context.Background(), harness.clients, true, "services", "remove", resetting.CurrentJobID, "--expect-current")
		var removed computerOperatorProjection
		if err := json.Unmarshal(removedBytes, &removed); err != nil || removed.DesiredState != contract.ServiceDesiredRemoved ||
			removed.ReconfigurationPhase != l1.ComputerReconfigurationRemoving || removed.MutationApplied == nil || !*removed.MutationApplied {
			t.Fatalf("superseding removal = %#v err=%v", removed, err)
		}
	})
}

func TestComputerCLIInsufficientDiskLatchRequiresExplicitRestart(t *testing.T) {
	harness, computer, node, claim := newRunningComputerCLIFixture(t, "insufficient-restart")
	ctx := context.Background()
	acknowledged := make(chan error, 1)
	go func() {
		deadline := time.Now().Add(2 * time.Second)
		for time.Now().Before(deadline) {
			directives, err := harness.store.ListNodeComputerStorageGrowDirectives(ctx, "fabric-"+node.NodeID, node.NodeID, node.BootSessionID)
			if err == nil && len(directives) == 1 {
				directive := directives[0]
				receipt := l1.ComputerStorageGrowReceipt{
					Kind: "computer_storage_grow_failed_unchanged", ReceiptID: "insufficient-receipt",
					ComputerID: directive.ComputerID, StorageID: directive.StorageID, StorageGeneration: directive.StorageGeneration,
					NodeID: directive.BoundNodeID, RootInstanceID: directive.RootInstanceID, JobID: directive.JobID,
					OperationRevision: directive.OperationRevision, OperationFence: directive.OperationFence, HelperGeneration: 1,
					OldDiskBytes: directive.OldDiskBytes, NewDiskBytes: directive.NewDiskBytes, Applied: false,
					FailureCode: "insufficient_disk", ObservedAvailableBytes: 512 << 20,
				}
				_, err = harness.store.AcknowledgeComputerStorageGrow(ctx, "fabric-"+node.NodeID, computer.ComputerID,
					l1.ComputerStorageGrowAcknowledgementRequest{NodeID: node.NodeID, BootSessionID: node.BootSessionID,
						IdempotencyKey: receipt.ReceiptID, Receipt: receipt})
				acknowledged <- err
				return
			}
			time.Sleep(time.Millisecond)
		}
		acknowledged <- errors.New("timed out waiting for Computer grow directive")
	}()
	var stdout, stderr bytes.Buffer
	err := execute(ctx, harness.clients, true, []string{"services", "resize", computer.CurrentJobID,
		"--expect-current", "--disk-bytes", fmt.Sprint(computer.DesiredDiskBytes + (1 << 30)),
		"--idempotency-key", "insufficient-resize", "--wait", "2s", "--poll-interval", "1ms"}, &stdout, &stderr)
	var responseErr *apiResponseError
	if !errors.As(err, &responseErr) || responseErr.APIError.Code != contract.ErrorCapacityExhausted || commandExitCode(err) != exitConflict {
		t.Fatalf("awaited grow error = %T %v, want typed capacity exit", err, err)
	}
	if err := <-acknowledged; err != nil {
		t.Fatal(err)
	}
	var failed computerOperatorProjection
	if err := json.Unmarshal(stdout.Bytes(), &failed); err != nil {
		t.Fatalf("decode awaited grow projection: %v\n%s", err, stdout.String())
	}
	var failure contract.SpawnFailure
	if failed.CurrentJob.State != contract.JobRunning || json.Unmarshal(failed.CurrentJob.LastFailure, &failure) != nil ||
		failure.Code != contract.SpawnFailureInsufficientDisk || failed.CurrentJob.CurrentAttemptID != claim.Lease.AttemptID ||
		failed.ReconfigurationPhase != l1.ComputerReconfigurationStable || failed.AppliedRevision != failed.IntentRevision ||
		failed.DesiredDiskBytes != computer.DesiredDiskBytes || failed.Capacity.LastGrow.Status != "FAIL" ||
		failed.Capacity.LastGrow.FailureCode != "insufficient_disk" || failed.Capacity.LastGrow.RequestedBytes == nil ||
		*failed.Capacity.LastGrow.RequestedBytes != computer.DesiredDiskBytes+(1<<30) ||
		failed.Capacity.LastGrow.ObservedAvailableBytes == nil || *failed.Capacity.LastGrow.ObservedAvailableBytes != 512<<20 ||
		failed.Capacity.ActiveFailure.Status != "FAIL" || failed.Capacity.ActiveFailure.FailureCode != "insufficient_disk" ||
		failed.Observation == nil || failed.Observation.Status != "observed" {
		t.Fatalf("latched failure = %#v failure=%#v", failed, failure)
	}
	restartedBytes := runServiceCLI(t, ctx, harness.clients, true, "services", "restart", failed.CurrentJobID,
		"--expect-current", "--idempotency-key", "insufficient-restart")
	var restarted computerOperatorProjection
	if err := json.Unmarshal(restartedBytes, &restarted); err != nil || restarted.CurrentJob.State != contract.JobStopping ||
		len(restarted.CurrentJob.LastFailure) != 0 || restarted.MutationApplied == nil || !*restarted.MutationApplied {
		t.Fatalf("explicit restart = %#v err=%v", restarted, err)
	}
	stdout.Reset()
	stderr.Reset()
	err = execute(ctx, harness.clients, true, []string{"services", "resize", computer.CurrentJobID,
		"--intent-revision", fmt.Sprint(computer.IntentRevision), "--storage-id", computer.StorageID,
		"--storage-generation", fmt.Sprint(computer.StorageGeneration),
		"--disk-bytes", fmt.Sprint(computer.DesiredDiskBytes + (1 << 30)),
		"--idempotency-key", "insufficient-resize", "--wait", "2s", "--poll-interval", "1ms"}, &stdout, &stderr)
	if !errors.As(err, &responseErr) || responseErr.APIError.Code != contract.ErrorCapacityExhausted || commandExitCode(err) != exitConflict {
		t.Fatalf("replayed refused grow after revision advance = %T %v, want typed capacity exit", err, err)
	}
	var replayed computerOperatorProjection
	if decodeErr := json.Unmarshal(stdout.Bytes(), &replayed); decodeErr != nil || replayed.LastGrowOperation == nil ||
		replayed.LastGrowOperation.OperationRevision != computer.IntentRevision+1 || replayed.IntentRevision != restarted.IntentRevision ||
		replayed.IdempotentReplay == nil || !*replayed.IdempotentReplay || replayed.Observation == nil ||
		replayed.Observation.Status != "observed" {
		t.Fatalf("replayed refused grow projection = %#v decode=%v", replayed, decodeErr)
	}
}

func TestComputerCLIResizePollIntervalRequiresWait(t *testing.T) {
	harness, computer, _, _ := newRunningComputerCLIFixture(t, "resize-poll-without-wait")
	err := execute(context.Background(), harness.clients, true, []string{"services", "resize", computer.ComputerID,
		"--expect-current", "--disk-bytes", fmt.Sprint(computer.DesiredDiskBytes + (1 << 30)),
		"--idempotency-key", "resize-poll-without-wait", "--poll-interval", "1ms"}, &bytes.Buffer{}, &bytes.Buffer{})
	var usage usageError
	if !errors.As(err, &usage) || commandExitCode(err) != exitUsage {
		t.Fatalf("poll without wait error = %T %v", err, err)
	}
}

func TestComputerCLIResizeWaitsForAppliedReceipt(t *testing.T) {
	harness, computer, node, _ := newRunningComputerCLIFixture(t, "resize-wait-applied")
	ctx := context.Background()
	requested := computer.DesiredDiskBytes + (1 << 30)
	acknowledged := make(chan error, 1)
	go func() {
		deadline := time.Now().Add(2 * time.Second)
		for time.Now().Before(deadline) {
			directives, err := harness.store.ListNodeComputerStorageGrowDirectives(ctx, "fabric-"+node.NodeID, node.NodeID, node.BootSessionID)
			if err == nil && len(directives) == 1 {
				directive := directives[0]
				receipt := l1.ComputerStorageGrowReceipt{
					Kind: "computer_storage_grow_applied", ReceiptID: "applied-receipt",
					ComputerID: directive.ComputerID, StorageID: directive.StorageID, StorageGeneration: directive.StorageGeneration,
					NodeID: directive.BoundNodeID, RootInstanceID: directive.RootInstanceID, JobID: directive.JobID,
					OperationRevision: directive.OperationRevision, OperationFence: directive.OperationFence, HelperGeneration: 1,
					OldDiskBytes: directive.OldDiskBytes, NewDiskBytes: directive.NewDiskBytes, Applied: true,
				}
				_, err = harness.store.AcknowledgeComputerStorageGrow(ctx, "fabric-"+node.NodeID, computer.ComputerID,
					l1.ComputerStorageGrowAcknowledgementRequest{NodeID: node.NodeID, BootSessionID: node.BootSessionID,
						IdempotencyKey: receipt.ReceiptID, Receipt: receipt})
				acknowledged <- err
				return
			}
			time.Sleep(time.Millisecond)
		}
		acknowledged <- errors.New("timed out waiting for Computer grow directive")
	}()
	output := runServiceCLI(t, ctx, harness.clients, true, "services", "resize", computer.ComputerID,
		"--expect-current", "--disk-bytes", fmt.Sprint(requested), "--idempotency-key", "resize-wait-applied",
		"--wait", "2s", "--poll-interval", "1ms")
	if err := <-acknowledged; err != nil {
		t.Fatal(err)
	}
	var grown computerOperatorProjection
	if err := json.Unmarshal(output, &grown); err != nil || grown.ReconfigurationPhase != l1.ComputerReconfigurationStable ||
		grown.AppliedRevision != grown.IntentRevision || grown.DesiredDiskBytes != requested ||
		grown.Capacity.LastGrow.Status != "PASS" || grown.Capacity.LastGrow.RequestedBytes == nil ||
		*grown.Capacity.LastGrow.RequestedBytes != requested || grown.Capacity.LastGrow.ObservedAvailableBytes != nil ||
		grown.Capacity.ActiveFailure.Status != "NOT-RUN" || grown.Observation == nil || grown.Observation.Status != "observed" {
		t.Fatalf("awaited applied grow = %#v err=%v", grown, err)
	}
}

func TestComputerCLIResizeFirstPollFailureDoesNotEmitComputerDocument(t *testing.T) {
	harness, computer, _, _ := newRunningComputerCLIFixture(t, "resize-first-poll-failure")
	baseTransport := harness.clients.l1.client.Transport
	computerReads := 0
	harness.clients.l1.client.Transport = storageRoundTripFunc(func(request *http.Request) (*http.Response, error) {
		if request.Method == http.MethodGet && request.URL.Path == "/v1/computers/"+computer.ComputerID {
			computerReads++
			if computerReads == 2 {
				return nil, errors.New("poll transport unavailable")
			}
		}
		return baseTransport.RoundTrip(request)
	})
	var stdout, stderr bytes.Buffer
	err := execute(context.Background(), harness.clients, true, []string{"services", "resize", computer.ComputerID,
		"--intent-revision", fmt.Sprint(computer.IntentRevision), "--storage-id", computer.StorageID,
		"--storage-generation", fmt.Sprint(computer.StorageGeneration),
		"--disk-bytes", fmt.Sprint(computer.DesiredDiskBytes + (1 << 30)),
		"--idempotency-key", "resize-first-poll-failure", "--wait", "2s", "--poll-interval", "1ms"}, &stdout, &stderr)
	var observationErr *storageObservationError
	if !errors.As(err, &observationErr) || stdout.Len() != 0 {
		t.Fatalf("first poll failure err=%T %v stdout=%q", err, err, stdout.String())
	}
}

func TestComputerCapacityProjectionSeparatesLastGrowFromActiveFailure(t *testing.T) {
	completed := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	lastFailure, err := json.Marshal(contract.SpawnFailure{Code: contract.SpawnFailureInsufficientMemory,
		RequestedBytes: 2 << 30, ObservedAvailableBytes: 1 << 30})
	if err != nil {
		t.Fatal(err)
	}
	projection := newComputerProjection(l1.Computer{
		LastGrowOperation: &l1.ComputerStorageGrowOutcome{OperationRevision: 7, Status: "superseded",
			RequestedBytes: 12 << 30, CompletedAt: &completed},
		CurrentJob: l1.Job{ServiceJob: &l1.ServiceJob{LastFailure: lastFailure}},
	}, nil, nil)
	if projection.Capacity.LastGrow.Status != "NOT-RUN" || projection.Capacity.LastGrow.Code != "grow_superseded" ||
		projection.Capacity.ActiveFailure.Status != "FAIL" ||
		projection.Capacity.ActiveFailure.FailureCode != string(contract.SpawnFailureInsufficientMemory) ||
		projection.Capacity.ActiveFailure.RequestedBytes == nil || *projection.Capacity.ActiveFailure.RequestedBytes != 2<<30 {
		t.Fatalf("capacity projection = %#v", projection.Capacity)
	}
}

func TestComputerCLIExitCodesAreTyped(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want int
	}{
		{"usage", usageError("bad flags"), exitUsage},
		{"unauthorized", &apiResponseError{APIError: contract.APIError{Code: contract.ErrorUnauthorized}}, exitUnauthorized},
		{"not found", &apiResponseError{APIError: contract.APIError{Code: contract.ErrorNotFound}}, exitNotFound},
		{"stale CAS", &apiResponseError{APIError: contract.APIError{Code: contract.ErrorStaleIntentRevision}}, exitConflict},
		{"capacity", &apiResponseError{APIError: contract.APIError{Code: contract.ErrorCapacityExhausted}}, exitConflict},
		{"other", errors.New("transport unavailable"), exitFailure},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := commandExitCode(test.err); got != test.want {
				t.Fatalf("exit code = %d, want %d", got, test.want)
			}
		})
	}
}

func TestComputerCLIUsageForeignIDAndRoutePaginationErrors(t *testing.T) {
	assertComputerCLIUsageForeignIDAndRoutePaginationErrors(t)
}

func assertComputerCLIUsageForeignIDAndRoutePaginationErrors(t *testing.T) {
	harness := newServiceCLIHarness(t)
	ctx := context.Background()

	foreignArgs := []string{"services", "status", "computer_foreign"}
	var stdout, stderr bytes.Buffer
	err := execute(ctx, harness.clients, true, foreignArgs, &stdout, &stderr)
	if err == nil || commandExitCodeForArgs(err, foreignArgs) != exitNotFound {
		t.Fatalf("foreign Computer = %T %v exit=%d", err, err, commandExitCodeForArgs(err, foreignArgs))
	}

	usageArgs := []string{"services", "reset", "computer-foreign", "--expect-current", "--intent-revision", "1",
		"--storage-id", "storage-foreign", "--storage-generation", "1", "--idempotency-key", "usage-conflict"}
	stdout.Reset()
	err = execute(ctx, harness.clients, true, usageArgs, &stdout, &stderr)
	if err == nil || commandExitCodeForArgs(err, usageArgs) != exitUsage {
		t.Fatalf("conflicting CAS usage = %T %v exit=%d", err, err, commandExitCodeForArgs(err, usageArgs))
	}
	var rendered bytes.Buffer
	writeCommandError(&rendered, err, true)
	var response contract.ErrorResponse
	if json.Unmarshal(rendered.Bytes(), &response) != nil || response.Error.Code != contract.ErrorInvalidRequest {
		t.Fatalf("JSON usage error = %s", rendered.String())
	}

	for _, test := range []struct {
		name   string
		cursor string
		limit  int
	}{
		{name: "zero limit", limit: 0},
		{name: "oversized limit", limit: l1.MaxJobPageLimit + 1},
		{name: "forged cursor", cursor: "not-a-signed-page", limit: 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, routeErr := harness.clients.listComputers(ctx, test.cursor, test.limit)
			var responseErr *apiResponseError
			if !errors.As(routeErr, &responseErr) || responseErr.APIError.Code != contract.ErrorInvalidRequest {
				t.Fatalf("route pagination error = %T %v", routeErr, routeErr)
			}
		})
	}

	plainArgs := []string{"services", "status", "job-foreign"}
	err = execute(ctx, harness.clients, true, plainArgs, &stdout, &stderr)
	if err == nil || commandExitCodeForArgs(err, plainArgs) != exitFailure {
		t.Fatalf("pre-existing service exit changed = %T %v exit=%d", err, err, commandExitCodeForArgs(err, plainArgs))
	}
}

func newRunningComputerCLIFixture(t *testing.T, name string) (*serviceCLIHarness, l1.Computer, l1.Node, *l1.Claim) {
	t.Helper()
	harness := newServiceCLIHarness(t)
	harness.clients.images = &fakeImageResolver{digest: computerCLITestDigest}
	ctx := context.Background()
	nodeID := "computer-node"
	node, err := harness.store.RegisterNode(ctx, fabric.Identity{NodeID: "fabric-" + nodeID}, contract.NodeRegistration{
		NodeID: nodeID, BootSessionID: "boot-" + name, RootInstanceID: "root-" + name,
		OS: "linux", Architecture: "amd64", AgentVersion: "computer-cli-test",
		Capabilities: map[string]bool{"kind:oci": true, "cgroup_v2": true, "computer": true}, CapabilityRevision: 1,
		CapabilityObservedAt: time.Now().UTC(), MissingCapabilities: []string{},
	}, l1.NodePolicy{Tags: []string{contract.StableNodeTagPrefix + nodeID}, MaxServiceSlots: 4}, true)
	if err != nil {
		t.Fatal(err)
	}
	created := createComputerCLIProjection(t, ctx, harness.clients, name, name+"-create")
	claim, err := harness.store.ClaimJob(ctx, "fabric-"+nodeID, nodeID, node.BootSessionID, contract.JobClassService)
	if err != nil || claim == nil || claim.Job.JobID != created.CurrentJobID {
		t.Fatalf("claim = %#v err=%v", claim, err)
	}
	platformDigest := computerCLITestDigestB
	indexDigest := computerCLITestDigest
	if _, err := harness.store.ObserveAttemptImage(ctx, "fabric-"+nodeID, claim.Job.JobID, claim.Lease.AttemptID, l1.ImageObservationRequest{
		FencingToken: claim.Lease.FencingToken, SubmittedReference: "ghcr.io/example/" + name,
		TopLevelDigest: computerCLITestDigest, TopLevelMediaType: "application/vnd.oci.image.index.v1+json",
		IndexDigest: &indexDigest, PlatformManifestDigest: platformDigest,
		Platform: l1.OCIPlatform{OS: "linux", Architecture: "amd64"}, RuntimeHandler: "io.containerd.runc.v2", Snapshotter: "overlayfs",
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := harness.store.StartAttempt(ctx, "fabric-"+nodeID, claim.Job.JobID, claim.Lease.AttemptID,
		l1.StartedRequest{FencingToken: claim.Lease.FencingToken}); err != nil {
		t.Fatal(err)
	}
	computer, err := harness.store.GetComputer(ctx, created.ComputerID)
	if err != nil {
		t.Fatal(err)
	}
	return harness, computer, node, claim
}

func runComputerMutation(t *testing.T, ctx context.Context, clients *apiClients, verb string, computer l1.Computer, args ...string) computerOperatorProjection {
	t.Helper()
	command := append([]string{"services", verb, computer.ComputerID}, args...)
	output := runServiceCLI(t, ctx, clients, true, command...)
	var projection computerOperatorProjection
	if err := json.Unmarshal(output, &projection); err != nil {
		t.Fatal(err)
	}
	return projection
}

func createComputerCLIProjection(t *testing.T, ctx context.Context, clients *apiClients, name, key string) computerOperatorProjection {
	t.Helper()
	output := runServiceCLI(t, ctx, clients, true, "services", "create", "--computer",
		"--name", name, "--image", "ghcr.io/example/"+name+"@"+computerCLITestDigest,
		"--node", "computer-node", "--idempotency-key", key)
	var projection computerOperatorProjection
	if err := json.Unmarshal(output, &projection); err != nil {
		t.Fatal(err)
	}
	return projection
}
