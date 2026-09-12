package serviceacceptance

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	limarunner "github.com/Derek-X-Wang/wefty/runner/lima"
	"github.com/Derek-X-Wang/wefty/runner/ocihelper"
)

// ---------------------------------------------------------------------------
// fakes
// ---------------------------------------------------------------------------

type fakeSession struct {
	handshake   ocihelper.AcquireSessionResponse
	health      error
	runs        []ocihelper.RunRequest
	runFunc     func(ocihelper.RunRequest) (ocihelper.RunResponse, error)
	watchFunc   func(ocihelper.AttemptAuthority) []ocihelper.WatchEvent
	deleteResp  ocihelper.DeleteResponse
	deleteErr   error
	verifies    []ocihelper.VerifyRequest
	verifyResp  ocihelper.VerifyResponse
	verifyErr   error
	dialAttempt func(ocihelper.DialAttemptPortRequest) (net.Conn, error)
	dialBridge  func(ocihelper.DialHostBridgeRequest) (net.Conn, error)
}

func (fake *fakeSession) Handshake() ocihelper.AcquireSessionResponse { return fake.handshake }
func (fake *fakeSession) HealthError() error                          { return fake.health }

func (fake *fakeSession) Run(_ context.Context, request ocihelper.RunRequest) (ocihelper.RunResponse, error) {
	fake.runs = append(fake.runs, request)
	if fake.runFunc == nil {
		return ocihelper.RunResponse{}, errors.New("no run behaviour configured")
	}
	return fake.runFunc(request)
}

func (fake *fakeSession) Watch(_ context.Context, request ocihelper.WatchRequest, receive func(ocihelper.WatchEvent) error) error {
	if fake.watchFunc == nil {
		return errors.New("no watch behaviour configured")
	}
	for _, event := range fake.watchFunc(request.Authority) {
		if err := receive(event); err != nil {
			return err
		}
	}
	return nil
}

func (fake *fakeSession) Delete(context.Context, ocihelper.DeleteRequest) (ocihelper.DeleteResponse, error) {
	return fake.deleteResp, fake.deleteErr
}

func (fake *fakeSession) Verify(_ context.Context, request ocihelper.VerifyRequest) (ocihelper.VerifyResponse, error) {
	fake.verifies = append(fake.verifies, request)
	return fake.verifyResp, fake.verifyErr
}

func (fake *fakeSession) DialAttemptPort(_ context.Context, request ocihelper.DialAttemptPortRequest) (net.Conn, error) {
	if fake.dialAttempt == nil {
		return nil, errors.New("no attempt dial behaviour configured")
	}
	return fake.dialAttempt(request)
}

func (fake *fakeSession) DialHostBridge(_ context.Context, request ocihelper.DialHostBridgeRequest) (net.Conn, error) {
	if fake.dialBridge == nil {
		return nil, errors.New("no bridge dial behaviour configured")
	}
	return fake.dialBridge(request)
}

func testConfig() attendedConfig {
	return attendedConfig{
		SessionID: "attended-test", Command: []string{"go", "test", "-run", "Attended"},
		NodeID: "attended-node", BootSessionID: "attended-boot",
		Reference: "registry.invalid/echo:test", Digest: "sha256:" + strings.Repeat("a", 64),
		MountRoot: "/mount/root", LimaInstance: "wefty-oci",
		Deadman: 10 * time.Minute, ProbeDeadman: 30 * time.Second,
	}
}

func startedResponse() ocihelper.RunResponse {
	return ocihelper.RunResponse{
		Started: true, StartedAt: time.Unix(1700000000, 0),
		Image: &ocihelper.ImageEvidence{TopLevelDigest: "sha256:top", PlatformManifestDigest: "sha256:platform"},
	}
}

func logEvents(stdout, stderr string, exitCode int) []ocihelper.WatchEvent {
	code := exitCode
	return []ocihelper.WatchEvent{
		{Kind: ocihelper.WatchProgress, Log: &ocihelper.LogFrame{Stream: "stdout", Sequence: 1, Bytes: []byte(stdout + "\n")}},
		{Kind: ocihelper.WatchProgress, Log: &ocihelper.LogFrame{Stream: "stderr", Sequence: 1, Bytes: []byte(stderr + "\n")}},
		{Kind: ocihelper.WatchProgress, Seal: &ocihelper.LogSeal{Stream: "stdout", Complete: true}},
		{Kind: ocihelper.WatchProgress, Seal: &ocihelper.LogSeal{Stream: "stderr", Complete: true}},
		{Kind: ocihelper.WatchComplete, Result: &ocihelper.WatchResponse{ExitCode: &code}},
	}
}

// ---------------------------------------------------------------------------
// checkRefusal -- the guard that keeps every negative honest
// ---------------------------------------------------------------------------

func TestCheckRefusalRejectsAnAcceptedNegative(t *testing.T) {
	if _, err := checkRefusal("outside the root", nil, ocihelper.CodeEngineFailure); err == nil {
		t.Fatal("a negative the helper accepted must fail the row")
	}
}

func TestCheckRefusalRejectsAnUntypedFailure(t *testing.T) {
	if _, err := checkRefusal("symlink", errors.New("connection reset"), ocihelper.CodeOCISpecRejected); err == nil {
		t.Fatal("an untyped failure is not evidence of the refusal being tested")
	}
}

func TestCheckRefusalRejectsTheWrongTypedCode(t *testing.T) {
	wrong := &ocihelper.RPCError{Code: ocihelper.CodeImageUnavailable, Message: "image"}
	code, err := checkRefusal("symlink", wrong, ocihelper.CodeOCISpecRejected)
	if err == nil {
		t.Fatal("a refusal with an unrelated code must fail the row")
	}
	if code != ocihelper.CodeImageUnavailable {
		t.Fatalf("observed code = %q, want the code actually returned", code)
	}
}

func TestCheckRefusalAcceptsTheExactCode(t *testing.T) {
	refusal := &ocihelper.RPCError{Code: ocihelper.CodeUnauthorizedPort, Message: "endpoint is not allocated"}
	code, err := checkRefusal("unallocated endpoint", refusal, ocihelper.CodeUnauthorizedPort)
	if err != nil || code != ocihelper.CodeUnauthorizedPort {
		t.Fatalf("checkRefusal = (%q, %v), want the exact code and no error", code, err)
	}
}

// ---------------------------------------------------------------------------
// checkLogEvidence
// ---------------------------------------------------------------------------

func TestCheckLogEvidence(t *testing.T) {
	exit := 0
	base := func() logEvidence {
		return logEvidence{
			stdout: []string{"out-marker\n"}, stderr: []string{"err-marker\n"},
			sequences: map[string][]uint64{"stdout": {1, 2}, "stderr": {1}},
			seals:     map[string]bool{"stdout": true, "stderr": true},
			result:    &ocihelper.WatchResponse{ExitCode: &exit},
		}
	}
	if err := checkLogEvidence(base(), "out-marker", "err-marker"); err != nil {
		t.Fatalf("well-formed log evidence must pass: %v", err)
	}

	gapped := base()
	gapped.gaps = 1
	if err := checkLogEvidence(gapped, "out-marker", "err-marker"); err == nil {
		t.Fatal("a gap frame means the log evidence is incomplete and must fail the row")
	}

	unordered := base()
	unordered.sequences["stdout"] = []uint64{2, 2}
	if err := checkLogEvidence(unordered, "out-marker", "err-marker"); err == nil {
		t.Fatal("non-increasing per-stream sequences must fail the row")
	}

	silent := base()
	silent.sequences["stderr"] = nil
	if err := checkLogEvidence(silent, "out-marker", "err-marker"); err == nil {
		t.Fatal("a stream with no frames must fail the row")
	}

	merged := base()
	merged.stdout = []string{"out-marker\nerr-marker\n"}
	if err := checkLogEvidence(merged, "out-marker", "err-marker"); err == nil {
		t.Fatal("stdout carrying the stderr marker means the streams were not distinct")
	}

	failed := base()
	nonZero := 7
	failed.result = &ocihelper.WatchResponse{ExitCode: &nonZero}
	if err := checkLogEvidence(failed, "out-marker", "err-marker"); err == nil {
		t.Fatal("a non-zero terminal exit must fail the row")
	}

	incomplete := base()
	incomplete.result = &ocihelper.WatchResponse{ExitCode: &exit, LogEvidenceIncomplete: true}
	if err := checkLogEvidence(incomplete, "out-marker", "err-marker"); err == nil {
		t.Fatal("helper-reported incomplete log evidence must fail the row")
	}

	missing := base()
	missing.result = nil
	if err := checkLogEvidence(missing, "out-marker", "err-marker"); err == nil {
		t.Fatal("no terminal result must fail the row")
	}

	unsealed := base()
	delete(unsealed.seals, "stderr")
	if err := checkLogEvidence(unsealed, "out-marker", "err-marker"); err == nil {
		t.Fatal("a stream that was never sealed must fail the row")
	}

	partial := base()
	partial.seals["stdout"] = false
	if err := checkLogEvidence(partial, "out-marker", "err-marker"); err == nil {
		t.Fatal("an incomplete seal must fail the row")
	}
}

// ---------------------------------------------------------------------------
// task_logs_delete
// ---------------------------------------------------------------------------

func TestDriveTaskLogsDeletePasses(t *testing.T) {
	session := &fakeSession{
		runFunc: func(ocihelper.RunRequest) (ocihelper.RunResponse, error) { return startedResponse(), nil },
		watchFunc: func(ocihelper.AttemptAuthority) []ocihelper.WatchEvent {
			return logEvents("wefty-attended-task-stdout", "wefty-attended-task-stderr", 0)
		},
		deleteResp: ocihelper.DeleteResponse{Deleted: true},
		verifyResp: ocihelper.VerifyResponse{Absent: true},
	}
	row := driveTaskLogsDelete(context.Background(), session, testConfig())
	if row.Status != "PASS" || row.ExitCode != 0 {
		t.Fatalf("row = %+v, want PASS", row)
	}
	if row.PayloadExecutions != 1 || len(row.AttemptIDs) != 1 {
		t.Fatalf("row = %+v, want exactly one attempt and one payload execution", row)
	}
}

func TestDriveTaskLogsDeleteFailsWithoutPositiveDelete(t *testing.T) {
	session := &fakeSession{
		runFunc: func(ocihelper.RunRequest) (ocihelper.RunResponse, error) { return startedResponse(), nil },
		watchFunc: func(ocihelper.AttemptAuthority) []ocihelper.WatchEvent {
			return logEvents("wefty-attended-task-stdout", "wefty-attended-task-stderr", 0)
		},
		deleteResp: ocihelper.DeleteResponse{Deleted: false},
		verifyResp: ocihelper.VerifyResponse{Absent: true},
	}
	row := driveTaskLogsDelete(context.Background(), session, testConfig())
	if row.Status == "PASS" || row.ExitCode == 0 {
		t.Fatalf("row = %+v, want FAIL when Delete reports no removal", row)
	}
}

func taskLogsIdentity(t *testing.T, config attendedConfig) (ocihelper.AttemptAuthority, ocihelper.ResourceIdentity) {
	t.Helper()
	authority := config.authority(contract.JobClassOneShot, "task-logs-delete")
	identity, err := ocihelper.DeterministicResourceIdentity(authority)
	if err != nil {
		t.Fatal(err)
	}
	return authority, identity
}

func taskLogsSession(verification ocihelper.VerifyResponse) *fakeSession {
	return &fakeSession{
		runFunc: func(ocihelper.RunRequest) (ocihelper.RunResponse, error) { return startedResponse(), nil },
		watchFunc: func(ocihelper.AttemptAuthority) []ocihelper.WatchEvent {
			return logEvents("wefty-attended-task-stdout", "wefty-attended-task-stderr", 0)
		},
		deleteResp: ocihelper.DeleteResponse{Deleted: true},
		verifyResp: verification,
	}
}

func TestDriveTaskLogsDeleteFailsOnResidue(t *testing.T) {
	config := testConfig()
	_, identity := taskLogsIdentity(t, config)
	session := taskLogsSession(ocihelper.VerifyResponse{
		Inventory:      ocihelper.ResourceInventory{Containers: []string{identity.ContainerID}},
		RuntimeResidue: ocihelper.ResourceInventory{Containers: []string{identity.ContainerID}},
	})
	row := driveTaskLogsDelete(context.Background(), session, config)
	if row.Status == "PASS" {
		t.Fatalf("row = %+v, want FAIL when the independent Verify still sees residue", row)
	}
}

// The post-delete Verify must not be attempt-scoped: the helper authorizes an
// attempt-scoped Verify only against a live attempt, which a positive Delete
// is precisely what ends. A row that asks for one can never pass on hardware.
func TestDriveTaskLogsDeleteProvesAbsenceThroughTheReadOnlyNamespace(t *testing.T) {
	session := taskLogsSession(ocihelper.VerifyResponse{})
	row := driveTaskLogsDelete(context.Background(), session, testConfig())
	if row.Status != "PASS" {
		t.Fatalf("row = %+v, want PASS", row)
	}
	if len(session.verifies) != 1 {
		t.Fatalf("row issued %d verifies, want exactly the one independent absence proof", len(session.verifies))
	}
	verification := session.verifies[0]
	if verification.Scope != ocihelper.VerifyNamespaceReadOnly || verification.Authority != nil {
		t.Fatalf("verify request = %+v, want a read-only namespace scope carrying no attempt authority", verification)
	}
}

// The namespace the row reads is shared, so absence has to be asserted over
// this attempt's own deterministic names and nothing else: another attempt's
// container, or the image spools the window's own import left behind, must not
// fail the row, and this attempt's must.
func TestCheckAttemptAbsenceIsScopedToTheAttemptsOwnResources(t *testing.T) {
	config := testConfig()
	authority, identity := taskLogsIdentity(t, config)
	unrelated := ocihelper.ResourceInventory{
		Containers:  []string{"wefty-container-" + strings.Repeat("b", 32)},
		ImageSpools: []string{"spool-from-the-pinned-probe-import"},
	}
	if err := checkAttemptAbsence(ocihelper.VerifyResponse{Inventory: unrelated, RuntimeResidue: unrelated}, authority); err != nil {
		t.Fatalf("another attempt's residue must not fail this row: %v", err)
	}
	for name, inventory := range map[string]ocihelper.ResourceInventory{
		"lease":       {Leases: []string{identity.LeaseID}},
		"snapshot":    {Snapshots: []string{identity.SnapshotID}},
		"container":   {Containers: []string{identity.ContainerID}},
		"task":        {Tasks: []string{identity.TaskID}},
		"shim":        {Shims: []string{identity.ShimID}},
		"cgroup":      {Cgroups: []string{"/sys/fs/cgroup/wefty/" + identity.CgroupID + ".scope"}},
		"log segment": {LogSegments: []string{identity.LogSegmentDirectory}},
	} {
		if err := checkAttemptAbsence(ocihelper.VerifyResponse{Inventory: inventory, RuntimeResidue: inventory}, authority); err == nil {
			t.Fatalf("this attempt's %s left as runtime residue must fail the row", name)
		}
		// Not residue, but still observed: only an explicit bounded retention
		// naming this attempt may explain that, and these have none.
		if err := checkAttemptAbsence(ocihelper.VerifyResponse{Inventory: inventory}, authority); err == nil {
			t.Fatalf("this attempt's %s surviving delete unexplained must fail the row", name)
		}
	}
}

func boundRetention(class ocihelper.RemovalResourceClass, id, attemptID string,
	reason ocihelper.DurableRetentionReason) ocihelper.DurableRetention {
	recorded := time.Unix(1700000000, 0)
	return ocihelper.DurableRetention{
		Class: class, ID: id, Owner: ocihelper.DurableRetentionOwnerOCIHelper, Reason: reason,
		AttemptID: attemptID, State: ocihelper.DurableRetentionStateUnsealed,
		Bound: time.Minute, RecordedAt: recorded, Deadline: recorded.Add(time.Minute),
	}
}

// A sealing log spool is the one shape the helper may still show after a
// positive Delete, and only under a bounded retention that names this attempt.
func TestCheckAttemptAbsenceAcceptsOnlyBoundHelperRetentions(t *testing.T) {
	config := testConfig()
	authority, identity := taskLogsIdentity(t, config)
	observed := ocihelper.ResourceInventory{LogSegments: []string{identity.LogSegmentDirectory}}
	retention := boundRetention(ocihelper.RemovalResourceLogSegments, identity.LogSegmentDirectory,
		authority.AttemptID, ocihelper.DurableRetentionReasonLogSpoolSealing)

	if err := checkAttemptAbsence(ocihelper.VerifyResponse{
		Inventory: observed, DurableRetained: observed,
		DurableRetentions: []ocihelper.DurableRetention{retention},
	}, authority); err != nil {
		t.Fatalf("a sealing log spool bound to this attempt must not fail the row: %v", err)
	}

	for name, mutate := range map[string]func(*ocihelper.DurableRetention){
		"another attempt":   func(r *ocihelper.DurableRetention) { r.AttemptID = "attended-someone-else" },
		"another owner":     func(r *ocihelper.DurableRetention) { r.Owner = "operator" },
		"another reason":    func(r *ocihelper.DurableRetention) { r.Reason = ocihelper.DurableRetentionReasonCgroupReaping },
		"no bound at all":   func(r *ocihelper.DurableRetention) { r.Bound = 0; r.Deadline = r.RecordedAt },
		"unclosed deadline": func(r *ocihelper.DurableRetention) { r.Deadline = r.Deadline.Add(time.Hour) },
	} {
		broken := retention
		mutate(&broken)
		if err := checkAttemptAbsence(ocihelper.VerifyResponse{
			Inventory: observed, DurableRetained: observed,
			DurableRetentions: []ocihelper.DurableRetention{broken},
		}, authority); err == nil {
			t.Fatalf("a retention with %s cannot explain a survivor", name)
		}
	}

	// Even a correctly bound retention cannot excuse the resource still being
	// runtime residue -- that is the helper saying it is not retained at all.
	if err := checkAttemptAbsence(ocihelper.VerifyResponse{
		Inventory: observed, RuntimeResidue: observed,
		DurableRetentions: []ocihelper.DurableRetention{retention},
	}, authority); err == nil {
		t.Fatal("a log spool that is still runtime residue must fail the row")
	}
}

// A service data volume and its owner record belong to the job, not the
// attempt, so they may outlive a Delete -- but never as runtime residue.
func TestCheckAttemptAbsenceLeavesJobScopedVolumesAlone(t *testing.T) {
	config := testConfig()
	authority := config.authority(contract.JobClassService, "host-to-guest")
	identity, err := ocihelper.DeterministicResourceIdentity(authority)
	if err != nil {
		t.Fatal(err)
	}
	if identity.ServiceVolumeDirectory == "" || identity.ServiceVolumeOwnerRecord == "" {
		t.Fatalf("a service attempt must name a service volume and owner record: %+v", identity)
	}
	observed := ocihelper.ResourceInventory{
		ManagedVolumes:       []string{identity.ServiceVolumeDirectory},
		ManagedVolumeRecords: []string{identity.ServiceVolumeOwnerRecord},
	}
	if err := checkAttemptAbsence(ocihelper.VerifyResponse{Inventory: observed, DurableRetained: observed}, authority); err != nil {
		t.Fatalf("the job's service data must not fail an attempt's absence proof: %v", err)
	}
	if err := checkAttemptAbsence(ocihelper.VerifyResponse{Inventory: observed, RuntimeResidue: observed}, authority); err == nil {
		t.Fatal("service data reported as runtime residue must fail the row")
	}
}

// The lookup is driven from the helper's own closed removal registry, so a
// class added there and not mapped here fails the row loudly instead of
// silently dropping out of the proof.
func TestAttemptInventoryEntriesRefusesAnUnmappedResourceClass(t *testing.T) {
	if _, err := attemptInventoryEntries(ocihelper.ResourceInventory{},
		ocihelper.RemovalResource{Class: "a_class_this_row_has_never_seen", ID: "x"}); err == nil {
		t.Fatal("an unmapped resource class must fail the row rather than verify nothing")
	}
	for _, resource := range ocihelper.ExpectedRemovalResources(
		mustIdentity(t, testConfig().authority(contract.JobClassService, "registry-coverage")), "wefty-handoff-volume-x", nil) {
		if _, err := attemptInventoryEntries(ocihelper.ResourceInventory{}, resource); err != nil {
			t.Fatalf("every class the helper's registry names for an attempt must be looked up: %v", err)
		}
	}
}

func mustIdentity(t *testing.T, authority ocihelper.AttemptAuthority) ocihelper.ResourceIdentity {
	t.Helper()
	identity, err := ocihelper.DeterministicResourceIdentity(authority)
	if err != nil {
		t.Fatal(err)
	}
	return identity
}

// ---------------------------------------------------------------------------
// mount_validation -- an accepted negative must fail the row
// ---------------------------------------------------------------------------

func mountFixturesForTest(t *testing.T) (*mountFixtures, string) {
	t.Helper()
	root := t.TempDir()
	positive := filepath.Join(root, "positive")
	if err := os.MkdirAll(positive, 0o755); err != nil {
		t.Fatal(err)
	}
	source := filepath.Join(positive, "source.txt")
	if err := os.WriteFile(source, []byte(attendedMountProbeBytes), 0o644); err != nil {
		t.Fatal(err)
	}
	return &mountFixtures{
		positiveDirectory: positive, positiveFile: source,
		outsidePath: filepath.Join(t.TempDir(), "outside.txt"),
		symlinkPath: filepath.Join(root, "negatives", "symlink"),
		socketPath:  filepath.Join(root, "negatives", "socket"),
		fifoPath:    filepath.Join(root, "negatives", "fifo"),
		devicePath:  filepath.Join(root, "negatives", "device"),
	}, root
}

// mountConfigForTest points the config at the fixture root and answers the
// guest-side read with the bytes the payload is meant to have written.
func mountConfigForTest(t *testing.T, fixtures *mountFixtures, root string) attendedConfig {
	t.Helper()
	config := testConfig()
	config.MountRoot = root
	expected, err := translatedGuestPath(root, filepath.Join(fixtures.positiveDirectory, "wefty-attended-mount-write.txt"))
	if err != nil {
		t.Fatal(err)
	}
	config.GuestPath = func(_ context.Context, guestPath string) (string, error) {
		if guestPath != expected {
			return "", fmt.Errorf("guest read %q, want %q", guestPath, expected)
		}
		return attendedMountProbeBytes, nil
	}
	return config
}

// mountSession answers the positive run truthfully and every negative with the
// refusal the table expects, except the ones named in accept, which it admits.
func mountSession(t *testing.T, fixtures *mountFixtures, config attendedConfig, accept map[string]bool) *fakeSession {
	t.Helper()
	expected := map[string]ocihelper.ErrorCode{}
	for _, negative := range mountNegatives(config, fixtures) {
		expected[negative.nodePath+"\x00"+negative.containerPath] = negative.want
	}
	session := &fakeSession{
		deleteResp: ocihelper.DeleteResponse{Deleted: true},
		verifyResp: ocihelper.VerifyResponse{Absent: true},
	}
	session.runFunc = func(request ocihelper.RunRequest) (ocihelper.RunResponse, error) {
		mount := request.Workload.OperatorMounts[0]
		key := mount.NodePath + "\x00" + mount.ContainerPath
		code, isNegative := expected[key]
		if !isNegative {
			// The positive case: the payload reads the bind source and writes back.
			if err := os.WriteFile(filepath.Join(fixtures.positiveDirectory, "wefty-attended-mount-write.txt"),
				[]byte(attendedMountProbeBytes), 0o644); err != nil {
				t.Fatal(err)
			}
			return startedResponse(), nil
		}
		if accept[key] {
			return startedResponse(), nil
		}
		return ocihelper.RunResponse{}, &ocihelper.RPCError{Code: code, Message: "refused"}
	}
	session.watchFunc = func(ocihelper.AttemptAuthority) []ocihelper.WatchEvent {
		return logEvents(attendedMountProbeBytes+"wefty-attended-mount-read\n"+
			"57 41 0:39 /positive /data rw,relatime - virtiofs wefty-host rw", "unused", 0)
	}
	return session
}

func TestDriveMountValidationPasses(t *testing.T) {
	fixtures, root := mountFixturesForTest(t)
	config := mountConfigForTest(t, fixtures, root)
	row := driveMountValidation(context.Background(), mountSession(t, fixtures, config, nil), config, fixtures)
	if row.Status != "PASS" || row.ExitCode != 0 {
		t.Fatalf("row = %+v, want PASS", row)
	}
	if !strings.Contains(row.Reason, limarunner.GuestAllowedMountRoot) {
		t.Fatalf("row reason must record the guest-side translated path: %s", row.Reason)
	}
	if !strings.Contains(row.Reason, "/data") {
		t.Fatalf("row reason must record the payload's own mountinfo line: %s", row.Reason)
	}
	for _, target := range reservedMountTargets() {
		if !strings.Contains(row.Reason, target) {
			t.Fatalf("row reason omitted the reserved-target negative %q: %s", target, row.Reason)
		}
	}
}

func TestDriveMountValidationFailsWhenANegativeIsAccepted(t *testing.T) {
	fixtures, root := mountFixturesForTest(t)
	config := mountConfigForTest(t, fixtures, root)
	for _, negative := range mountNegatives(config, fixtures) {
		accepted := map[string]bool{negative.nodePath + "\x00" + negative.containerPath: true}
		row := driveMountValidation(context.Background(), mountSession(t, fixtures, config, accepted), config, fixtures)
		if row.Status == "PASS" || row.ExitCode == 0 {
			t.Fatalf("negative %q was accepted yet the row = %+v; an accepted negative must fail the row",
				negative.label, row)
		}
		if !strings.Contains(row.Reason, "ACCEPTED-OR-UNTYPED") {
			t.Fatalf("negative %q reason must record the acceptance: %s", negative.label, row.Reason)
		}
	}
}

func TestDriveMountValidationFailsWhenTheHostSourceIsNotPreserved(t *testing.T) {
	fixtures, root := mountFixturesForTest(t)
	config := mountConfigForTest(t, fixtures, root)
	session := mountSession(t, fixtures, config, nil)
	inner := session.runFunc
	session.runFunc = func(request ocihelper.RunRequest) (ocihelper.RunResponse, error) {
		response, err := inner(request)
		if err == nil && request.Workload.OperatorMounts[0].NodePath == fixtures.positiveDirectory {
			if writeErr := os.WriteFile(fixtures.positiveFile, []byte("clobbered"), 0o644); writeErr != nil {
				t.Fatal(writeErr)
			}
		}
		return response, err
	}
	row := driveMountValidation(context.Background(), session, config, fixtures)
	if row.Status == "PASS" {
		t.Fatalf("row = %+v, want FAIL when the host bind source is not byte-identical afterwards", row)
	}
}

func TestDriveMountValidationFailsWithoutAProvenGuestTranslation(t *testing.T) {
	fixtures, root := mountFixturesForTest(t)

	noReader := mountConfigForTest(t, fixtures, root)
	noReader.GuestPath = nil
	row := driveMountValidation(context.Background(), mountSession(t, fixtures, noReader, nil), noReader, fixtures)
	if row.Status == "PASS" {
		t.Fatalf("row = %+v, want FAIL without a guest-side reader", row)
	}

	wrongBytes := mountConfigForTest(t, fixtures, root)
	wrongBytes.GuestPath = func(context.Context, string) (string, error) { return "something else", nil }
	row = driveMountValidation(context.Background(), mountSession(t, fixtures, wrongBytes, nil), wrongBytes, fixtures)
	if row.Status == "PASS" {
		t.Fatalf("row = %+v, want FAIL when the guest does not see the payload's bytes", row)
	}

	missingMount := mountConfigForTest(t, fixtures, root)
	session := mountSession(t, fixtures, missingMount, nil)
	session.watchFunc = func(ocihelper.AttemptAuthority) []ocihelper.WatchEvent {
		return logEvents(attendedMountProbeBytes+"wefty-attended-mount-read", "unused", 0)
	}
	row = driveMountValidation(context.Background(), session, missingMount, fixtures)
	if row.Status == "PASS" {
		t.Fatalf("row = %+v, want FAIL when the payload reported no mount at /data", row)
	}
}

func TestTranslatedGuestPathRefusesNonDescendants(t *testing.T) {
	if _, err := translatedGuestPath("/mount/root", "/elsewhere/file"); err == nil {
		t.Fatal("a path outside the operator mount root has no translation")
	}
	got, err := translatedGuestPath("/mount/root", "/mount/root/positive/file")
	if err != nil || got != limarunner.GuestAllowedMountRoot+"/positive/file" {
		t.Fatalf("translatedGuestPath = (%q, %v)", got, err)
	}
}

func TestMountNegativesCoverEveryRequiredRefusal(t *testing.T) {
	fixtures, root := mountFixturesForTest(t)
	config := mountConfigForTest(t, fixtures, root)
	negatives := mountNegatives(config, fixtures)
	labels := map[string]bool{}
	for _, negative := range negatives {
		labels[negative.label] = true
		if negative.want == "" || negative.nodePath == "" || negative.containerPath == "" {
			t.Fatalf("negative %+v is incompletely specified", negative)
		}
	}
	for _, required := range []string{
		"root itself", "outside the root", "symlink component", "unix socket", "fifo", "device node",
	} {
		if !labels[required] {
			t.Fatalf("the refusal table omits %q", required)
		}
	}
	if len(negatives) != 6+len(reservedMountTargets()) {
		t.Fatalf("refusal table has %d entries, want 6 source negatives plus every reserved target", len(negatives))
	}
}

func TestPrepareMountFixturesNamesTheDeviceNodeCommand(t *testing.T) {
	_, err := prepareMountFixtures(t.TempDir(), t.TempDir())
	if err == nil {
		t.Fatal("a missing device-node fixture must refuse rather than silently skip the negative")
	}
	if !strings.Contains(err.Error(), "sudo mknod") {
		t.Fatalf("refusal must name the exact command: %v", err)
	}
}

// ---------------------------------------------------------------------------
// host_to_guest
// ---------------------------------------------------------------------------

// echoOrigin is a stand-in for the acceptance image's payload: /echo mirrors
// the body, /healthz produces a distinct payload-authored marker.
func echoOrigin(t *testing.T) net.Listener {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/echo", func(writer http.ResponseWriter, request *http.Request) {
		_, _ = io.Copy(writer, request.Body)
	})
	mux.HandleFunc("/healthz", func(writer http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(writer).Encode(map[string]any{
			"pid": 4321, "service_directory": contract.OCIContainerServiceDirectory, "listening_port": 18080,
		})
	})
	server := &http.Server{Handler: mux, ReadHeaderTimeout: time.Second}
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() { _ = server.Close() })
	return listener
}

func hostToGuestSession(t *testing.T, acceptNegative string) *fakeSession {
	t.Helper()
	origin := echoOrigin(t)
	return &fakeSession{
		runFunc: func(ocihelper.RunRequest) (ocihelper.RunResponse, error) {
			response := startedResponse()
			response.Endpoints = map[string]uint16{"service": 18080}
			return response, nil
		},
		deleteResp: ocihelper.DeleteResponse{Deleted: true},
		verifyResp: ocihelper.VerifyResponse{Absent: true},
		dialAttempt: func(request ocihelper.DialAttemptPortRequest) (net.Conn, error) {
			live := request.Name == "service" && request.Authority.AttemptID == "attended-host-to-guest" &&
				!strings.HasSuffix(request.Authority.FencingToken, "-mutated")
			if live {
				return net.Dial("tcp", origin.Addr().String())
			}
			switch {
			case acceptNegative == "name" && request.Name != "service":
				return net.Dial("tcp", origin.Addr().String())
			case acceptNegative == "attempt" && request.Authority.AttemptID != "attended-host-to-guest":
				return net.Dial("tcp", origin.Addr().String())
			case acceptNegative == "fence" && strings.HasSuffix(request.Authority.FencingToken, "-mutated"):
				return net.Dial("tcp", origin.Addr().String())
			}
			if request.Name != "service" {
				return nil, &ocihelper.RPCError{Code: ocihelper.CodeUnauthorizedPort, Message: "endpoint is not allocated"}
			}
			return nil, &ocihelper.RPCError{Code: ocihelper.CodeAttemptOutsideSession, Message: "attempt is outside this helper session"}
		},
	}
}

func TestDriveHostToGuestPasses(t *testing.T) {
	row := driveHostToGuest(context.Background(), hostToGuestSession(t, ""), testConfig())
	if row.Status != "PASS" || !row.RoundTrip {
		t.Fatalf("row = %+v, want PASS with a proven round trip", row)
	}
	if !strings.Contains(row.Reason, "pid=4321") {
		t.Fatalf("row reason must record the payload-produced response marker: %s", row.Reason)
	}
}

func TestDriveHostToGuestFailsWhenANegativeIsAccepted(t *testing.T) {
	for _, accepted := range []string{"name", "attempt", "fence"} {
		row := driveHostToGuest(context.Background(), hostToGuestSession(t, accepted), testConfig())
		if row.Status == "PASS" || row.ExitCode == 0 {
			t.Fatalf("%s negative was accepted yet the row = %+v", accepted, row)
		}
	}
}

func TestDriveHostToGuestFailsWithoutAnAllocatedEndpoint(t *testing.T) {
	session := hostToGuestSession(t, "")
	session.runFunc = func(ocihelper.RunRequest) (ocihelper.RunResponse, error) { return startedResponse(), nil }
	row := driveHostToGuest(context.Background(), session, testConfig())
	if row.Status == "PASS" {
		t.Fatalf("row = %+v, want FAIL when no service endpoint was allocated", row)
	}
}

// ---------------------------------------------------------------------------
// row shape and fragment
// ---------------------------------------------------------------------------

func TestAttendedFragmentDecodesIntoTheReceiptRowShape(t *testing.T) {
	path := filepath.Join(t.TempDir(), "rows.json")
	row := newAttendedRow("attended-test", []string{"go", "test"})
	row.pass("proof")
	if err := writeAttendedFragment(path, map[string]attendedRow{"task_logs_delete": row}); err != nil {
		t.Fatal(err)
	}
	payload, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	var fragment struct {
		Rows map[string]struct {
			Status              string            `json:"status"`
			Reason              string            `json:"reason"`
			SessionID           string            `json:"session_id"`
			Command             []string          `json:"command"`
			ExitCode            int               `json:"exit_code"`
			HelperGenerations   []uint64          `json:"helper_generations"`
			CapabilityRevisions []int64           `json:"capability_revisions"`
			Inventories         []json.RawMessage `json:"inventories"`
			RoundTrip           bool              `json:"round_trip"`
			DynamicListeners    map[string]bool   `json:"dynamic_listeners"`
			AttemptIDs          []string          `json:"attempt_ids"`
			TopLevelDigests     []string          `json:"top_level_digests"`
			PlatformDigests     []string          `json:"platform_digests"`
			PayloadExecutions   int               `json:"payload_executions"`
			StdoutMarkers       []string          `json:"stdout_markers"`
			StderrMarkers       []string          `json:"stderr_markers"`
			HandoffMarkerBytes  []string          `json:"handoff_marker_bytes"`
			HandoffAbsent       bool              `json:"handoff_absent_after_completion"`
		} `json:"rows"`
	}
	if err := decoder.Decode(&fragment); err != nil {
		t.Fatalf("fragment must decode into the receipt row shape under DisallowUnknownFields: %v", err)
	}
	decoded := fragment.Rows["task_logs_delete"]
	if decoded.Status != "PASS" || decoded.ExitCode != 0 || decoded.SessionID != "attended-test" || len(decoded.Command) == 0 {
		t.Fatalf("decoded row = %+v, want the gate's PASS shape", decoded)
	}
	for name, value := range map[string]bool{
		"helper_generations": decoded.HelperGenerations == nil, "capability_revisions": decoded.CapabilityRevisions == nil,
		"inventories": decoded.Inventories == nil, "attempt_ids": decoded.AttemptIDs == nil,
		"stdout_markers": decoded.StdoutMarkers == nil, "dynamic_listeners": decoded.DynamicListeners == nil,
	} {
		if value {
			t.Fatalf("%s decoded as null; every non-omitempty field must be emitted", name)
		}
	}
}

func TestAttendedRowFailCannotBeMistakenForSuccess(t *testing.T) {
	row := newAttendedRow("attended-test", []string{"go", "test"})
	if row.Status == "PASS" || row.ExitCode == 0 {
		t.Fatalf("a fresh row = %+v, want FAIL until an assertion sequence completes", row)
	}
	row.fail(errors.New("boom"))
	if row.Status != "FAIL" || row.ExitCode != 1 || !strings.Contains(row.Reason, "boom") {
		t.Fatalf("row = %+v, want FAIL carrying the cause", row)
	}
}
