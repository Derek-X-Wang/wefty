package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/l1"
)

const computerCLITestDigest = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
const computerCLITestDigestB = "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"

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

	createdBytes := runServiceCLI(t, ctx, harness.clients, true, "computers", "create",
		"--name", "alice", "--image", "ghcr.io/example/alice:latest", "--node", "computer-node",
		"--idempotency-key", "alice-computer")
	var created computerOperatorProjection
	if err := json.Unmarshal(createdBytes, &created); err != nil {
		t.Fatal(err)
	}
	if created.ComputerID == "" || created.CurrentJobID == "" || created.ComputerID == created.CurrentJobID {
		t.Fatalf("Computer/Job identity projection = %q/%q", created.ComputerID, created.CurrentJobID)
	}
	if created.Capacity.RequestedMemoryBytes != defaultComputerMemoryBytes ||
		created.Capacity.RequestedDiskBytes != defaultComputerDiskBytes ||
		created.DesiredDiskBytes != defaultComputerDiskBytes || created.StorageGeneration != 1 || created.BackupCap != 0 {
		t.Fatalf("explicit Computer defaults = %#v", created.Capacity)
	}
	if created.MutationApplied == nil || !*created.MutationApplied || created.IdempotentReplay == nil || *created.IdempotentReplay {
		t.Fatalf("creation mutation receipt = applied %v replay %v", created.MutationApplied, created.IdempotentReplay)
	}
	if created.DisplayEndpoint != nil || created.ControllerTenure.Status != "NOT-RUN" ||
		created.Capacity.Admission.Status != "NOT-RUN" {
		t.Fatalf("unavailable projection facts were overstated: %#v", created)
	}

	replayedBytes := runServiceCLI(t, ctx, harness.clients, true, "computers", "create",
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

	second := runServiceCLI(t, ctx, harness.clients, true, "computers", "create",
		"--name", "bob", "--image", "ghcr.io/example/bob@"+computerCLITestDigest, "--node", "computer-node",
		"--idempotency-key", "bob-computer")
	if !bytes.Contains(second, []byte(`"computer_id"`)) {
		t.Fatalf("second Computer output = %s", second)
	}
	firstPage := runServiceCLI(t, ctx, harness.clients, true, "computers", "list", "--limit", "1")
	var page computerListProjection
	if err := json.Unmarshal(firstPage, &page); err != nil {
		t.Fatal(err)
	}
	if len(page.Computers) != 1 || page.Computers[0].ComputerID != created.ComputerID || page.NextCursor == "" {
		t.Fatalf("first Computer page = %#v", page)
	}
	secondPage := runServiceCLI(t, ctx, harness.clients, true, "computers", "list", "--limit", "1", "--cursor", page.NextCursor)
	if err := json.Unmarshal(secondPage, &page); err != nil || len(page.Computers) != 1 || page.Computers[0].Name != "bob" {
		t.Fatalf("second Computer page = %#v, err=%v", page, err)
	}

	human := runServiceCLI(t, ctx, harness.clients, false, "computers", "get", created.ComputerID)
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
	stoppedAgain := runServiceCLI(t, ctx, harness.clients, true, "computers", "stop", created.ComputerID, "--expect-current")
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
	restartedBytes := runServiceCLI(t, ctx, harness.clients, true, "computers", "restart", created.ComputerID,
		"--intent-revision", "4", "--storage-id", created.StorageID, "--storage-generation", "1",
		"--idempotency-key", "alice-restart")
	var restarted computerOperatorProjection
	if err := json.Unmarshal(restartedBytes, &restarted); err != nil || restarted.IntentRevision != 5 ||
		restarted.MutationApplied == nil || !*restarted.MutationApplied {
		t.Fatalf("restarted Computer = %#v, err=%v", restarted, err)
	}
	restartReplay := runServiceCLI(t, ctx, harness.clients, true, "computers", "restart", created.ComputerID,
		"--intent-revision", "4", "--storage-id", created.StorageID, "--storage-generation", "1",
		"--idempotency-key", "alice-restart")
	if err := json.Unmarshal(restartReplay, &restarted); err != nil || restarted.MutationApplied == nil || *restarted.MutationApplied ||
		restarted.IdempotentReplay == nil || !*restarted.IdempotentReplay {
		t.Fatalf("restart replay = %#v, err=%v", restarted, err)
	}

	reimageSubject := createComputerCLIProjection(t, ctx, harness.clients, "reimage-subject", "reimage-subject")
	reimageBytes := runServiceCLI(t, ctx, harness.clients, true, "computers", "reimage", reimageSubject.ComputerID,
		"--image", "ghcr.io/example/reimaged@"+computerCLITestDigestB,
		"--intent-revision", "1", "--storage-id", reimageSubject.StorageID, "--storage-generation", "1",
		"--idempotency-key", "reimage-subject-v2")
	var reimaged computerOperatorProjection
	if err := json.Unmarshal(reimageBytes, &reimaged); err != nil || reimaged.IntentRevision != 2 ||
		reimaged.ReconfigurationPhase != l1.ComputerReconfigurationReimaging || reimaged.MutationApplied == nil || !*reimaged.MutationApplied {
		t.Fatalf("reimage projection = %#v, err=%v", reimaged, err)
	}
	reimageReplay := runServiceCLI(t, ctx, harness.clients, true, "computers", "reimage", reimageSubject.ComputerID,
		"--image", "ghcr.io/example/reimaged@"+computerCLITestDigestB,
		"--intent-revision", "1", "--storage-id", reimageSubject.StorageID, "--storage-generation", "1",
		"--idempotency-key", "reimage-subject-v2")
	if err := json.Unmarshal(reimageReplay, &reimaged); err != nil || reimaged.IdempotentReplay == nil || !*reimaged.IdempotentReplay ||
		reimaged.MutationApplied == nil || *reimaged.MutationApplied {
		t.Fatalf("reimage replay projection = %#v, err=%v", reimaged, err)
	}

	resetSubject := createComputerCLIProjection(t, ctx, harness.clients, "reset-subject", "reset-subject")
	resetBytes := runServiceCLI(t, ctx, harness.clients, true, "computers", "reset", resetSubject.ComputerID,
		"--expect-current", "--idempotency-key", "reset-subject-v2")
	var reset computerOperatorProjection
	if err := json.Unmarshal(resetBytes, &reset); err != nil || reset.ReconfigurationPhase != l1.ComputerReconfigurationResetting ||
		reset.IntentRevision != 2 || reset.MutationApplied == nil || !*reset.MutationApplied {
		t.Fatalf("reset projection = %#v, err=%v", reset, err)
	}

	growSubject := createComputerCLIProjection(t, ctx, harness.clients, "grow-subject", "grow-subject")
	growBytes := runServiceCLI(t, ctx, harness.clients, true, "computers", "grow", growSubject.ComputerID,
		"--expect-current", "--disk-bytes", fmt.Sprint(9<<30), "--idempotency-key", "grow-subject-v2")
	var grown computerOperatorProjection
	if err := json.Unmarshal(growBytes, &grown); err != nil || grown.ReconfigurationPhase != l1.ComputerReconfigurationGrowing ||
		grown.IntentRevision != 2 || grown.MutationApplied == nil || !*grown.MutationApplied {
		t.Fatalf("grow projection = %#v, err=%v", grown, err)
	}

	removeSubject := createComputerCLIProjection(t, ctx, harness.clients, "remove-subject", "remove-subject")
	removeBytes := runServiceCLI(t, ctx, harness.clients, true, "computers", "remove", removeSubject.ComputerID, "--expect-current")
	var removed computerOperatorProjection
	if err := json.Unmarshal(removeBytes, &removed); err != nil || removed.DesiredState != contract.ServiceDesiredRemoved ||
		removed.CurrentJob.State != contract.JobRemovedVerified || removed.RemovalOutcome != "removal_pending" ||
		removed.MutationApplied == nil || !*removed.MutationApplied {
		t.Fatalf("remove projection = %#v, err=%v", removed, err)
	}
	removeReplay := runServiceCLI(t, ctx, harness.clients, true, "computers", "remove", removeSubject.ComputerID, "--expect-current")
	if err := json.Unmarshal(removeReplay, &removed); err != nil || removed.MutationApplied == nil || *removed.MutationApplied {
		t.Fatalf("remove replay projection = %#v, err=%v", removed, err)
	}
}

func TestComputerCLIMutationNegativeReceiptFailsEveryRow20Of20(t *testing.T) {
	harness := newServiceCLIHarness(t)
	harness.clients.images = &fakeImageResolver{digest: computerCLITestDigest}
	ctx := context.Background()
	createdBytes := runServiceCLI(t, ctx, harness.clients, true, "computers", "create",
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
		{"grow stale", "grow", append(keyed(stale, "negative-grow-stale"), "--disk-bytes", fmt.Sprint(9<<30)), contract.ErrorStaleIntentRevision},
		{"grow storage", "grow", append(keyed(wrongStorage, "negative-grow-storage"), "--disk-bytes", fmt.Sprint(9<<30)), contract.ErrorStorageReferenceConflict},
		{"grow generation", "grow", append(keyed(wrongGeneration, "negative-grow-generation"), "--disk-bytes", fmt.Sprint(9<<30)), contract.ErrorStorageReferenceConflict},
		{"reimage stale", "reimage", append(keyed(stale, "negative-reimage-stale"), "--image", "ghcr.io/example/new@"+computerCLITestDigestB), contract.ErrorStaleIntentRevision},
		{"reimage storage", "reimage", append(keyed(wrongStorage, "negative-reimage-storage"), "--image", "ghcr.io/example/new@"+computerCLITestDigestB), contract.ErrorStorageReferenceConflict},
	}
	if len(tests) != 20 {
		t.Fatalf("mutation negative receipt has %d rows, want exactly 20", len(tests))
	}
	for index, test := range tests {
		t.Run(fmt.Sprintf("%02d_%s", index+1, strings.ReplaceAll(test.name, " ", "_")), func(t *testing.T) {
			args := append([]string{"computers", test.verb, created.ComputerID}, test.args...)
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

func runComputerMutation(t *testing.T, ctx context.Context, clients *apiClients, verb string, computer l1.Computer, args ...string) computerOperatorProjection {
	t.Helper()
	command := append([]string{"computers", verb, computer.ComputerID}, args...)
	output := runServiceCLI(t, ctx, clients, true, command...)
	var projection computerOperatorProjection
	if err := json.Unmarshal(output, &projection); err != nil {
		t.Fatal(err)
	}
	return projection
}

func createComputerCLIProjection(t *testing.T, ctx context.Context, clients *apiClients, name, key string) computerOperatorProjection {
	t.Helper()
	output := runServiceCLI(t, ctx, clients, true, "computers", "create",
		"--name", name, "--image", "ghcr.io/example/"+name+"@"+computerCLITestDigest,
		"--node", "computer-node", "--idempotency-key", key)
	var projection computerOperatorProjection
	if err := json.Unmarshal(output, &projection); err != nil {
		t.Fatal(err)
	}
	return projection
}
