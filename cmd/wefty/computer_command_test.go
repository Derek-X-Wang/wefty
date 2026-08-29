package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

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
	if created.DisplayEndpoint != nil || created.ControllerTenure.Status != "NOT-RUN" ||
		created.Capacity.Admission.Status != "NOT-RUN" {
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
	removeBytes := runServiceCLI(t, ctx, harness.clients, true, "services", "remove", removeSubject.ComputerID, "--expect-current")
	var removed computerOperatorProjection
	if err := json.Unmarshal(removeBytes, &removed); err != nil || removed.DesiredState != contract.ServiceDesiredRemoved ||
		removed.CurrentJob.State != contract.JobRemovedVerified || removed.RemovalOutcome != "removed_verified" ||
		removed.MutationApplied == nil || !*removed.MutationApplied {
		t.Fatalf("remove projection = %#v, err=%v", removed, err)
	}
	removeReplay := runServiceCLI(t, ctx, harness.clients, true, "services", "remove", removeSubject.ComputerID, "--expect-current")
	if err := json.Unmarshal(removeReplay, &removed); err != nil || removed.MutationApplied == nil || *removed.MutationApplied {
		t.Fatalf("remove replay projection = %#v, err=%v", removed, err)
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
	harness := newServiceCLIHarness(t)
	ctx := context.Background()

	foreignArgs := []string{"services", "status", "computer-foreign"}
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
