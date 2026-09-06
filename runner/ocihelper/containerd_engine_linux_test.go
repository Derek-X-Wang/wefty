//go:build linux

package ocihelper

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	containerd "github.com/containerd/containerd/v2/client"
	"github.com/containerd/containerd/v2/core/content"
	"github.com/containerd/containerd/v2/core/leases"
	contentlocal "github.com/containerd/containerd/v2/plugins/content/local"
	"github.com/containerd/errdefs"
	"github.com/containerd/platforms"
	digest "github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"golang.org/x/sys/unix"
)

func TestContainerdEngineRejectsPATHResolvedRunc(t *testing.T) {
	_, err := NewContainerdEngine(NativeEngineConfig{RuntimeRoot: t.TempDir(), RuncExecutable: "runc"})
	if err == nil || !strings.Contains(err.Error(), "runc executable must be absolute") {
		t.Fatalf("PATH-resolved runc = %v", err)
	}
}

func TestContainerdEngineRejectsComputerPortRangeBeyondAddressSpace(t *testing.T) {
	_, err := NewContainerdEngine(NativeEngineConfig{RuntimeRoot: t.TempDir(), AttemptPortMin: 1, AttemptPortMax: 32769})
	if err == nil || !strings.Contains(err.Error(), "32768 disjoint /30 allocations") {
		t.Fatalf("oversized Computer port range = %v", err)
	}
}

func TestCopyManagedNetworkFileIsContainerReadable(t *testing.T) {
	directory := t.TempDir()
	source := filepath.Join(directory, "source-resolv.conf")
	target := filepath.Join(directory, "managed-resolv.conf")
	if err := os.WriteFile(source, []byte("nameserver 127.0.0.53\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	// Reproduce a snapshot created by the old helper. Refresh must repair its
	// mode as well as its contents because the Computer does not run as root.
	if err := os.WriteFile(target, []byte("stale\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := copyManagedNetworkFile(source, target); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(target)
	if err != nil {
		t.Fatal(err)
	}
	if got := info.Mode().Perm(); got != 0o644 {
		t.Fatalf("managed network file mode = %04o, want 0644", got)
	}
}

func TestDialComputerAttemptPortRejectsMalformedAuthority(t *testing.T) {
	engine := &ContainerdEngine{}
	left, right := net.Pipe()
	defer left.Close()
	defer right.Close()
	err := engine.DialAttemptPort(t.Context(), DialAttemptPortRequest{Name: contract.ComputerDisplayEndpointView, Port: 42000}, left)
	var refusal *ComputerAttemptAuthorityRefusalError
	if !errors.As(err, &refusal) {
		t.Fatalf("malformed Computer authority = %v, want typed refusal", err)
	}
}

func TestDoctorCacheReadIsBoundedBehindPullLocks(t *testing.T) {
	engine := &ContainerdEngine{}
	engine.imageContentMu.Lock()
	defer engine.imageContentMu.Unlock()
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	started := time.Now()
	_, err := engine.ImageCacheStatus(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("blocked cache read = %v", err)
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("blocked cache read was not bounded: %s", elapsed)
	}
}

func TestAttemptNetworkTeardownDoesNotHoldEngineLock(t *testing.T) {
	tool := filepath.Join(t.TempDir(), "slow-ip")
	if err := os.WriteFile(tool, []byte("#!/bin/sh\nsleep 1\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	authority := testAuthority()
	engine := &ContainerdEngine{
		attempts: map[string]*containerdAttempt{
			authority.key(): {authority: authority, endpointHolds: map[string]net.Listener{}, computerNetwork: &computerNetworkAttachment{hostLink: "wftchtest", ipPath: tool}},
		},
		ports: map[uint16]string{},
	}
	done := make(chan error, 1)
	go func() { done <- engine.releaseAttemptRuntimeState(t.Context(), authority.key()) }()
	time.Sleep(50 * time.Millisecond)
	lockAcquired := make(chan struct{})
	go func() {
		engine.mu.Lock()
		engine.mu.Unlock()
		close(lockAcquired)
	}()
	select {
	case <-lockAcquired:
	case <-time.After(250 * time.Millisecond):
		t.Fatal("Computer network teardown held the engine-wide lock")
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestSegmentTailerMarksTruncatedFinalFrameIncomplete(t *testing.T) {
	path := filepath.Join(t.TempDir(), "stdout.frames")
	var complete bytes.Buffer
	if err := writeLogFrame(&complete, 0, []byte("complete")); err != nil {
		t.Fatal(err)
	}
	var truncated bytes.Buffer
	if err := writeLogFrame(&truncated, 1, []byte("truncated")); err != nil {
		t.Fatal(err)
	}
	payload := append(complete.Bytes(), truncated.Bytes()[:logFrameHeaderBytes+3]...)
	if err := os.WriteFile(path, payload, 0o600); err != nil {
		t.Fatal(err)
	}
	terminal := make(chan struct{})
	close(terminal)
	events := make(chan logTailEvent, 4)
	tailLogSegment(context.Background(), "stdout", path, terminal, time.Millisecond, 0, events)
	var data, gap, incomplete bool
	for len(events) > 0 {
		event := (<-events).event
		data = data || (event.Log != nil && string(event.Log.Bytes) == "complete")
		gap = gap || (event.Log != nil && event.Log.Gap != nil && event.Log.Gap.LostByteCount == uint64(len("truncated")))
		incomplete = incomplete || (event.Seal != nil && !event.Seal.Complete)
	}
	if !data || !gap || !incomplete {
		t.Fatalf("tail evidence data=%t gap=%t incomplete=%t", data, gap, incomplete)
	}
}

func TestRuntimeDeletionValidatesIdentityBeforeNotFound(t *testing.T) {
	authority := testAuthority()
	authority.Class = contract.JobClassService
	authority.RemovalGeneration = "1"
	resources, err := DeterministicResourceIdentity(authority)
	if err != nil {
		t.Fatal(err)
	}
	if err := validateRuntimeResourceLabels("lease", resources.LeaseID, resources.LeaseID, resources.Labels, authority); err != nil {
		t.Fatalf("matching resource identity was rejected: %v", err)
	}
	wrong := make(map[string]string, len(resources.Labels))
	for name, value := range resources.Labels {
		wrong[name] = value
	}
	wrong["io.wefty/job_id"] = "different-job"
	if err := validateRuntimeResourceLabels("lease", resources.LeaseID, resources.LeaseID, wrong, authority); err == nil {
		t.Fatal("mismatched resource authority reached the NotFound path")
	}
	if err := validateRuntimeResourceLabels("lease", "different-name", resources.LeaseID, resources.Labels, authority); err == nil {
		t.Fatal("mismatched deterministic identity reached the NotFound path")
	}
}

func TestAuthorityLabelsRequireTheFullTuple(t *testing.T) {
	authority := AttemptAuthority{NodeID: "node", JobID: "job", AttemptID: "attempt", FencingToken: "fence", BootSessionID: "boot", Class: "one-shot", RemovalGeneration: "remove"}
	resources, err := DeterministicResourceIdentity(authority)
	if err != nil {
		t.Fatal(err)
	}
	delete(resources.Labels, "io.wefty/job_id")
	if _, err := authorityFromLabels(resources.Labels); err == nil {
		t.Fatal("partial authority labels accepted")
	}
}

func TestOOMEvidenceUsesConfiguredCgroupRoot(t *testing.T) {
	root := t.TempDir()
	cgroupID := "wefty-cgroup-test"
	if err := os.Mkdir(filepath.Join(root, cgroupID), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, cgroupID, "memory.events"), []byte("oom 1\noom_kill 1\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if !cgroupReportedOOM(root, cgroupID) {
		t.Fatal("configured cgroup root did not report oom_kill")
	}
	if err := os.WriteFile(filepath.Join(root, cgroupID, "memory.events"), []byte("oom 1\noom_kill 0\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if cgroupReportedOOM(root, cgroupID) {
		t.Fatal("plain oom counter was classified as an oom_kill")
	}
}

func TestSweepLostAttemptLogSegmentsWaitsForSealThenRemoves(t *testing.T) {
	authority := testAuthority()
	resources, err := DeterministicResourceIdentity(authority)
	if err != nil {
		t.Fatal(err)
	}
	runtimeRoot := t.TempDir()
	directory := filepath.Join(runtimeRoot, "logs", resources.LogSegmentDirectory)
	if err := os.MkdirAll(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	for _, stream := range []string{"stdout", "stderr"} {
		file, err := os.Create(filepath.Join(directory, stream+".frames"))
		if err != nil {
			t.Fatal(err)
		}
		if err := writeLogFrame(file, 0, []byte(stream+" live")); err != nil {
			t.Fatal(err)
		}
		if err := file.Close(); err != nil {
			t.Fatal(err)
		}
	}

	engine := &ContainerdEngine{config: NativeEngineConfig{RuntimeRoot: runtimeRoot, LogSealTimeout: time.Second}}
	engine.afterLogSealObservation = func() {
		engine.afterLogSealObservation = nil
		for _, stream := range []string{"stdout", "stderr"} {
			file, openErr := os.OpenFile(filepath.Join(directory, stream+".frames"), os.O_WRONLY|os.O_APPEND, 0)
			if openErr == nil {
				openErr = writeLogRecord(file, logSealMagic, 1, nil)
			}
			if file != nil {
				openErr = errors.Join(openErr, file.Close())
			}
			if openErr != nil {
				t.Fatalf("seal %s: %v", stream, openErr)
			}
		}
	}
	if err := engine.ensureAttemptOwnershipRecord(authority, resources); err != nil {
		t.Fatal(err)
	}
	ownership, err := engine.loadAttemptOwnershipRecords()
	if err != nil {
		t.Fatal(err)
	}
	retained, evidence, err := engine.sweepLostAttemptLogSegments(t.Context(), []string{resources.LogSegmentDirectory}, ownership)
	if err != nil {
		t.Fatal(err)
	}
	if len(retained) != 0 {
		t.Fatalf("sealed lost-attempt spool retained = %+v", retained)
	}
	if len(evidence) != 1 || evidence[0].Action != SweepActionRemoved || evidence[0].AttemptID != authority.AttemptID {
		t.Fatalf("log sweep evidence = %+v", evidence)
	}
	if _, err := os.Stat(directory); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("sealed lost-attempt spool remained: %v", err)
	}
}

func TestSweepLostAttemptLogSegmentsResetsOffsetAfterTruncateAndRegrow(t *testing.T) {
	authority := testAuthority()
	resources, err := DeterministicResourceIdentity(authority)
	if err != nil {
		t.Fatal(err)
	}
	runtimeRoot := t.TempDir()
	directory := filepath.Join(runtimeRoot, "logs", resources.LogSegmentDirectory)
	if err := os.MkdirAll(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	for _, stream := range []string{"stdout.frames", "stderr.frames"} {
		file, createErr := os.Create(filepath.Join(directory, stream))
		if createErr == nil {
			createErr = writeLogFrame(file, 0, []byte("long frame contents before truncation"))
		}
		if file != nil {
			createErr = errors.Join(createErr, file.Close())
		}
		if createErr != nil {
			t.Fatal(createErr)
		}
	}
	engine := &ContainerdEngine{config: NativeEngineConfig{RuntimeRoot: runtimeRoot, LogSealTimeout: time.Second}}
	engine.afterLogSealObservation = func() {
		engine.afterLogSealObservation = nil
		for _, stream := range []string{"stdout.frames", "stderr.frames"} {
			file, openErr := os.OpenFile(filepath.Join(directory, stream), os.O_WRONLY|os.O_TRUNC, 0)
			if openErr == nil {
				openErr = writeLogFrame(file, 1, bytes.Repeat([]byte("replacement"), 32))
			}
			if openErr == nil {
				openErr = writeLogRecord(file, logSealMagic, 2, nil)
			}
			if file != nil {
				openErr = errors.Join(openErr, file.Close())
			}
			if openErr != nil {
				t.Fatal(openErr)
			}
		}
	}
	if err := engine.ensureAttemptOwnershipRecord(authority, resources); err != nil {
		t.Fatal(err)
	}
	ownership, err := engine.loadAttemptOwnershipRecords()
	if err != nil {
		t.Fatal(err)
	}
	retained, evidence, err := engine.sweepLostAttemptLogSegments(t.Context(), []string{resources.LogSegmentDirectory}, ownership)
	if err != nil || len(retained) != 0 || len(evidence) != 1 || evidence[0].Method != "sealed_or_empty" {
		t.Fatalf("truncated spool retained=%+v evidence=%+v err=%v", retained, evidence, err)
	}
}

func TestSweepLostAttemptLogSegmentsRemovesCorruptFramesWithEvidence(t *testing.T) {
	authority := testAuthority()
	resources, err := DeterministicResourceIdentity(authority)
	if err != nil {
		t.Fatal(err)
	}
	runtimeRoot := t.TempDir()
	directory := filepath.Join(runtimeRoot, "logs", resources.LogSegmentDirectory)
	if err := os.MkdirAll(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(directory, "stdout.frames"), make([]byte, logFrameHeaderBytes), 0o600); err != nil {
		t.Fatal(err)
	}
	engine := &ContainerdEngine{config: NativeEngineConfig{RuntimeRoot: runtimeRoot, LogSealTimeout: time.Second}}
	if err := engine.ensureAttemptOwnershipRecord(authority, resources); err != nil {
		t.Fatal(err)
	}
	ownership, err := engine.loadAttemptOwnershipRecords()
	if err != nil {
		t.Fatal(err)
	}
	_, evidence, err := engine.sweepLostAttemptLogSegments(t.Context(), []string{resources.LogSegmentDirectory}, ownership)
	if err != nil || len(evidence) != 1 || evidence[0].Method != "corrupt_frames" || evidence[0].Action != SweepActionRemoved {
		t.Fatalf("corrupt spool evidence=%+v err=%v", evidence, err)
	}
	if _, err := os.Stat(directory); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("corrupt spool remained: %v", err)
	}
}

func TestLoadAttemptOwnershipRecordsIgnoresUnknownAndStaleEntries(t *testing.T) {
	runtimeRoot := t.TempDir()
	root := filepath.Join(runtimeRoot, "attempt-ownership")
	if err := os.MkdirAll(filepath.Join(root, "unexpected-dir"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "unknown"), []byte("unknown\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "stale.json"), []byte(`{"version":0}`), 0o600); err != nil {
		t.Fatal(err)
	}
	engine := &ContainerdEngine{config: NativeEngineConfig{RuntimeRoot: runtimeRoot}}
	records, err := engine.loadAttemptOwnershipRecords()
	if err != nil || len(records) != 0 {
		t.Fatalf("unknown ownership entries records=%v err=%v", records, err)
	}
}

func TestAttemptOwnershipSnapshotCannotOverlapPublication(t *testing.T) {
	authority := testAuthority()
	resources, err := DeterministicResourceIdentity(authority)
	if err != nil {
		t.Fatal(err)
	}
	publicationEntered := make(chan struct{})
	releasePublication := make(chan struct{})
	releaseOnce := sync.OnceFunc(func() { close(releasePublication) })
	defer releaseOnce()
	engine := &ContainerdEngine{
		config: NativeEngineConfig{RuntimeRoot: t.TempDir()},
		afterAttemptOwnershipSync: func() {
			close(publicationEntered)
			<-releasePublication
		},
	}
	record := durableAttemptOwnership{Version: durableAttemptOwnershipVersion, Authority: authority, Resources: resources}
	writeDone := make(chan error, 1)
	go func() { writeDone <- engine.ensureAttemptOwnershipRecord(authority, resources) }()
	<-publicationEntered

	type snapshotResult struct {
		records map[string]durableAttemptOwnership
		err     error
	}
	snapshotDone := make(chan snapshotResult, 1)
	go func() {
		records, err := engine.loadAttemptOwnershipRecords()
		snapshotDone <- snapshotResult{records: records, err: err}
	}()
	select {
	case result := <-snapshotDone:
		t.Fatalf("Attempt ownership snapshot overlapped publication: records=%v err=%v", result.records, result.err)
	case <-time.After(50 * time.Millisecond):
	}

	releaseOnce()
	if err := <-writeDone; err != nil {
		t.Fatal(err)
	}
	select {
	case result := <-snapshotDone:
		if result.err != nil || len(result.records) != 1 || result.records[authority.key()].Authority != authority {
			t.Fatalf("serialized Attempt ownership snapshot = records=%v err=%v", result.records, result.err)
		}
	case <-time.After(time.Second):
		t.Fatal("Attempt ownership snapshot did not proceed after publication")
	}
	if err := engine.removeAttemptOwnershipRecord(record); err != nil {
		t.Fatal(err)
	}
	if info, err := os.Stat(engine.attemptOwnershipRoot()); err != nil || !info.IsDir() {
		t.Fatalf("removing the final Attempt ownership record removed its parent: info=%v err=%v", info, err)
	}
}

func TestUnknownVersionOwnershipStaysUnboundUntilResourcesAreAbsentThenGCs(t *testing.T) {
	authority := testAuthority()
	resources, err := DeterministicResourceIdentity(authority)
	if err != nil {
		t.Fatal(err)
	}
	engine := &ContainerdEngine{config: NativeEngineConfig{RuntimeRoot: t.TempDir()}}
	record := durableAttemptOwnership{Version: durableAttemptOwnershipVersion + 1, Authority: authority, Resources: resources}
	if err := engine.writeAttemptOwnershipRecord(record); err != nil {
		t.Fatal(err)
	}
	observed := ResourceInventory{Cgroups: []string{resources.CgroupID}}
	projected, retained, err := engine.runtimeAbsenceInventory(observed, time.Now())
	if err != nil || !slices.Equal(projected.Cgroups, observed.Cgroups) || len(retained) != 0 {
		t.Fatalf("unknown-version ownership classification projected=%+v retained=%+v err=%v", projected, retained, err)
	}
	if _, err := os.Stat(engine.attemptOwnershipPath(resources)); err != nil {
		t.Fatalf("unknown-version record was GC'd while resource remained: %v", err)
	}
	if _, _, err := engine.runtimeAbsenceInventory(ResourceInventory{}, time.Now()); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(engine.attemptOwnershipPath(resources)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("quiescent unknown-version record was not GC'd: %v", err)
	}
}

func TestSweepLostAttemptLogSegmentsRetainsPendingSealWithOwnerAndReason(t *testing.T) {
	authority := testAuthority()
	resources, err := DeterministicResourceIdentity(authority)
	if err != nil {
		t.Fatal(err)
	}
	runtimeRoot := t.TempDir()
	directory := filepath.Join(runtimeRoot, "logs", resources.LogSegmentDirectory)
	foreignName := "wefty-log-segments-foreign"
	foreignDirectory := filepath.Join(runtimeRoot, "logs", foreignName)
	if err := os.MkdirAll(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(foreignDirectory, 0o700); err != nil {
		t.Fatal(err)
	}
	for _, stream := range []string{"stdout", "stderr"} {
		if err := os.WriteFile(filepath.Join(directory, stream+".frames"), nil, 0o600); err != nil {
			t.Fatal(err)
		}
	}

	clock := &observedClock{manualClock: newManualClock(time.Unix(1_000, 0)), timerCreated: make(chan struct{}, 8)}
	engine := &ContainerdEngine{config: NativeEngineConfig{RuntimeRoot: runtimeRoot, LogSealTimeout: time.Second, LostAttemptRetention: time.Minute, Clock: clock}}
	if err := engine.ensureAttemptOwnershipRecord(authority, resources); err != nil {
		t.Fatal(err)
	}
	ownership, err := engine.loadAttemptOwnershipRecords()
	if err != nil {
		t.Fatal(err)
	}
	engine.afterLogSealObservation = func() { clock.Advance(time.Second) }
	retained, _, err := engine.sweepLostAttemptLogSegments(t.Context(), []string{foreignName, resources.LogSegmentDirectory}, ownership)
	if err != nil {
		t.Fatal(err)
	}
	if len(retained) != 1 || retained[0].ID != resources.LogSegmentDirectory || retained[0].AttemptID != authority.AttemptID || retained[0].Bound != time.Minute || retained[0].Reason != DurableRetentionReasonLogSpoolSealing {
		t.Fatalf("pending lost-attempt spool retention = %+v", retained)
	}
	if _, err := os.Stat(directory); err != nil {
		t.Fatalf("pending lost-attempt spool was not retained: %v", err)
	}
	if _, err := os.Stat(foreignDirectory); err != nil {
		t.Fatalf("foreign prefix-shaped log directory was removed: %v", err)
	}
	residue, verifiedRetained, err := engine.runtimeAbsenceInventory(ResourceInventory{LogSegments: []string{foreignName, resources.LogSegmentDirectory}}, retained[0].RecordedAt)
	if err != nil || !slices.Equal(residue.LogSegments, []string{foreignName}) || !slices.Equal(verifiedRetained, retained) {
		t.Fatalf("pending spool classification = residue %+v retained %+v err %v", residue, verifiedRetained, err)
	}
	clock.Advance(time.Minute)
	ownership, err = engine.loadAttemptOwnershipRecords()
	if err != nil {
		t.Fatal(err)
	}
	retained, evidence, err := engine.sweepLostAttemptLogSegments(t.Context(), []string{resources.LogSegmentDirectory}, ownership)
	if err != nil || len(retained) != 0 || len(evidence) != 1 || evidence[0].Action != SweepActionRetentionBoundReaped {
		t.Fatalf("expired spool retention retained=%+v evidence=%+v err=%v", retained, evidence, err)
	}
	if _, err := os.Stat(directory); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("expired retained spool remained: %v", err)
	}
}

func TestSweepLostAttemptCgroupKillsPopulatedOwnedTree(t *testing.T) {
	authority := testAuthority()
	resources, err := DeterministicResourceIdentity(authority)
	if err != nil {
		t.Fatal(err)
	}
	cgroupRoot := t.TempDir()
	owned := filepath.Join(cgroupRoot, resources.CgroupID)
	foreign := filepath.Join(cgroupRoot, "wefty-cgroup-foreign")
	for _, directory := range []string{filepath.Join(owned, "nested.scope"), foreign} {
		if err := os.MkdirAll(directory, 0o700); err != nil {
			t.Fatal(err)
		}
	}

	killed := false
	engine := &ContainerdEngine{
		config: NativeEngineConfig{RuntimeRoot: t.TempDir(), CgroupRoot: cgroupRoot, TaskReleaseTimeout: time.Second},
		cgroupKill: func(path string) (cgroupKillResult, error) {
			if path != owned {
				t.Fatalf("killed cgroup %q, want %q", path, owned)
			}
			killed = true
			return cgroupKillResult{Method: "test", PIDs: []int{42}}, nil
		},
		cgroupPopulated: func(path string) (bool, error) { return !killed, nil },
		cgroupRemove:    os.RemoveAll,
	}
	if err := engine.ensureAttemptOwnershipRecord(authority, resources); err != nil {
		t.Fatal(err)
	}
	ownership, err := engine.loadAttemptOwnershipRecords()
	if err != nil {
		t.Fatal(err)
	}
	retained, evidence, err := engine.sweepLostAttemptCgroups(t.Context(), []string{resources.CgroupID, filepath.Base(foreign)}, ownership)
	if err != nil {
		t.Fatal(err)
	}
	if len(retained) != 0 || len(evidence) != 1 || evidence[0].Method != "test" || !slices.Equal(evidence[0].PIDs, []int{42}) {
		t.Fatalf("cgroup sweep retained=%+v evidence=%+v", retained, evidence)
	}
	if !killed {
		t.Fatal("populated helper-owned cgroup did not receive KILL escalation")
	}
	if _, err := os.Stat(owned); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("helper-owned cgroup remained: %v", err)
	}
	if _, err := os.Stat(foreign); err != nil {
		t.Fatalf("foreign prefix-shaped cgroup was removed: %v", err)
	}
}

func TestSweepLostAttemptCgroupBudgetExpiryCreatesBoundedRetention(t *testing.T) {
	authority := testAuthority()
	resources, err := DeterministicResourceIdentity(authority)
	if err != nil {
		t.Fatal(err)
	}
	clock := newManualClock(time.Unix(2_000, 0))
	cgroupRoot := t.TempDir()
	if err := os.Mkdir(filepath.Join(cgroupRoot, resources.CgroupID), 0o700); err != nil {
		t.Fatal(err)
	}
	engine := &ContainerdEngine{
		config: NativeEngineConfig{RuntimeRoot: t.TempDir(), CgroupRoot: cgroupRoot, TaskReleaseTimeout: time.Second, LostAttemptRetention: time.Minute, Clock: clock},
		cgroupKill: func(string) (cgroupKillResult, error) {
			return cgroupKillResult{Method: "test", PIDs: []int{42}}, nil
		},
		cgroupPopulated: func(string) (bool, error) { return true, nil },
		cgroupRemove:    os.RemoveAll,
	}
	engine.afterCgroupObservation = func() {
		engine.afterCgroupObservation = nil
		clock.Advance(time.Second)
	}
	if err := engine.ensureAttemptOwnershipRecord(authority, resources); err != nil {
		t.Fatal(err)
	}
	ownership, err := engine.loadAttemptOwnershipRecords()
	if err != nil {
		t.Fatal(err)
	}
	retained, evidence, err := engine.sweepLostAttemptCgroups(t.Context(), []string{resources.CgroupID}, ownership)
	if err != nil || len(retained) != 1 || retained[0].Reason != DurableRetentionReasonCgroupReaping || retained[0].Bound != time.Minute || len(evidence) != 1 || evidence[0].Action != SweepActionRetained {
		t.Fatalf("cgroup timeout retained=%+v evidence=%+v err=%v", retained, evidence, err)
	}
	residue, verifiedRetained, err := engine.runtimeAbsenceInventory(ResourceInventory{Cgroups: []string{resources.CgroupID}}, retained[0].RecordedAt)
	if err != nil || len(residue.Cgroups) != 0 || !slices.Equal(verifiedRetained, retained) {
		t.Fatalf("cgroup retention verification residue=%+v retained=%+v err=%v", residue, verifiedRetained, err)
	}
}

func TestDeleteSweepKillsOwnedCgroupAndQuiescentReleaseKeepsRecordUntilItIsGone(t *testing.T) {
	authority := testAuthority()
	resources, err := DeterministicResourceIdentity(authority)
	if err != nil {
		t.Fatal(err)
	}
	runtimeRoot := t.TempDir()
	cgroupRoot := t.TempDir()
	cgroupPath := filepath.Join(cgroupRoot, resources.CgroupID)
	if err := os.Mkdir(cgroupPath, 0o700); err != nil {
		t.Fatal(err)
	}
	killed := false
	engine := &ContainerdEngine{
		config:   NativeEngineConfig{RuntimeRoot: runtimeRoot, CgroupRoot: cgroupRoot, TaskReleaseTimeout: time.Second},
		attempts: map[string]*containerdAttempt{authority.key(): {authority: authority, resources: resources, deleted: true}},
		cgroupKill: func(string) (cgroupKillResult, error) {
			killed = true
			return cgroupKillResult{Method: "test", PIDs: []int{42}}, nil
		},
		cgroupPopulated: func(string) (bool, error) { return !killed, nil },
		cgroupRemove:    os.RemoveAll,
	}
	if err := engine.ensureAttemptOwnershipRecord(authority, resources); err != nil {
		t.Fatal(err)
	}
	records, err := engine.loadAttemptOwnershipRecords()
	if err != nil {
		t.Fatal(err)
	}
	record := records[authority.key()]
	record, retention, err := engine.retainAttemptResource(record, RemovalResourceCgroup, resources.CgroupID, DurableRetentionReasonCgroupReaping, DurableRetentionStatePopulated, time.Now())
	if err != nil || retention.Bound <= 0 {
		t.Fatalf("create retained cgroup binding: retention=%+v err=%v", retention, err)
	}
	retryErr := engine.removeAttemptOwnershipRecordAfterInventory(record, ResourceInventory{}, errors.New("inventory unavailable"))
	var retryable *AttemptOwnershipInventoryRetryError
	if !errors.As(retryErr, &retryable) || !retryable.Retryable() || deferRetryableAttemptOwnershipRelease(retryErr) != nil {
		t.Fatalf("inventory release outcome = %T %v", retryErr, retryErr)
	}
	if _, err := os.Stat(engine.attemptOwnershipPath(resources)); err != nil {
		t.Fatalf("retryable inventory failure removed ownership record: %v", err)
	}
	if err := engine.removeAttemptOwnershipRecordIfInventoryQuiescent(record, ResourceInventory{Cgroups: []string{resources.CgroupID}}); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(engine.attemptOwnershipPath(resources)); err != nil {
		t.Fatalf("release removed ownership while retained cgroup remained: %v", err)
	}
	if err := engine.sweepDeletedAttemptCgroup(t.Context(), resources); err != nil {
		t.Fatal(err)
	}
	if !killed {
		t.Fatal("Delete cgroup sweep did not issue KILL")
	}
	if _, err := os.Stat(cgroupPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("Delete cgroup sweep left cgroup behind: %v", err)
	}
	if err := engine.removeAttemptOwnershipRecordIfInventoryQuiescent(record, ResourceInventory{}); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(engine.attemptOwnershipPath(resources)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("quiescent release kept ownership record: %v", err)
	}
}

func TestSweepLostAttemptResourcesRequireDurableLostBinding(t *testing.T) {
	lostAuthority := testAuthority()
	lostAuthority.AttemptID = "lost-attempt"
	lostAuthority.FencingToken = "lost-fence"
	liveAuthority := testAuthority()
	liveAuthority.AttemptID = "live-attempt"
	liveAuthority.FencingToken = "live-fence"
	unboundAuthority := testAuthority()
	unboundAuthority.AttemptID = "unbound-attempt"
	unboundAuthority.FencingToken = "unbound-fence"
	lost, err := DeterministicResourceIdentity(lostAuthority)
	if err != nil {
		t.Fatal(err)
	}
	live, err := DeterministicResourceIdentity(liveAuthority)
	if err != nil {
		t.Fatal(err)
	}
	unbound, err := DeterministicResourceIdentity(unboundAuthority)
	if err != nil {
		t.Fatal(err)
	}
	runtimeRoot := t.TempDir()
	cgroupRoot := t.TempDir()
	for _, name := range []string{lost.LogSegmentDirectory, live.LogSegmentDirectory, unbound.LogSegmentDirectory} {
		if err := os.MkdirAll(filepath.Join(runtimeRoot, "logs", name), 0o700); err != nil {
			t.Fatal(err)
		}
	}
	lostCgroupName := lost.CgroupID + ".scope"
	for _, name := range []string{lostCgroupName, live.CgroupID, unbound.CgroupID} {
		if err := os.MkdirAll(filepath.Join(cgroupRoot, name), 0o700); err != nil {
			t.Fatal(err)
		}
	}
	var killed []string
	engine := &ContainerdEngine{
		config: NativeEngineConfig{RuntimeRoot: runtimeRoot, CgroupRoot: cgroupRoot, LogSealTimeout: time.Second, TaskReleaseTimeout: time.Second},
		attempts: map[string]*containerdAttempt{
			liveAuthority.key(): {authority: liveAuthority, resources: live},
		},
		cgroupKill: func(path string) (cgroupKillResult, error) {
			killed = append(killed, filepath.Base(path))
			return cgroupKillResult{Method: "test"}, nil
		},
		cgroupPopulated: func(string) (bool, error) { return false, nil },
		cgroupRemove:    os.RemoveAll,
	}
	for _, pair := range []struct {
		authority AttemptAuthority
		resources ResourceIdentity
	}{{lostAuthority, lost}, {liveAuthority, live}} {
		if err := engine.ensureAttemptOwnershipRecord(pair.authority, pair.resources); err != nil {
			t.Fatal(err)
		}
	}
	ownership, err := engine.loadAttemptOwnershipRecords()
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := engine.sweepLostAttemptLogSegments(t.Context(), []string{lost.LogSegmentDirectory, live.LogSegmentDirectory, unbound.LogSegmentDirectory}, ownership); err != nil {
		t.Fatal(err)
	}
	for _, check := range []struct {
		name    string
		removed bool
	}{{lost.LogSegmentDirectory, true}, {live.LogSegmentDirectory, false}, {unbound.LogSegmentDirectory, false}} {
		_, statErr := os.Stat(filepath.Join(runtimeRoot, "logs", check.name))
		if check.removed != errors.Is(statErr, os.ErrNotExist) {
			t.Fatalf("log %s removed=%t, stat=%v", check.name, check.removed, statErr)
		}
	}
	if _, _, err := engine.sweepLostAttemptCgroups(t.Context(), []string{lostCgroupName, live.CgroupID, unbound.CgroupID}, ownership); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(killed, []string{lostCgroupName}) {
		t.Fatalf("killed cgroups = %v, want only durable LOST binding %s", killed, lostCgroupName)
	}
	for _, name := range []string{live.CgroupID, unbound.CgroupID} {
		if _, err := os.Stat(filepath.Join(cgroupRoot, name)); err != nil {
			t.Fatalf("unowned/live cgroup %s was removed: %v", name, err)
		}
	}
}

type unsupportedCgroupKillWriter struct {
	writeErr error
	closeErr error
}

func (writer unsupportedCgroupKillWriter) Write([]byte) (int, error) { return 0, writer.writeErr }
func (writer unsupportedCgroupKillWriter) Close() error              { return writer.closeErr }

func TestKillCgroupTreeFallsBackWhenKillWriteOrCloseIsUnsupported(t *testing.T) {
	for _, test := range []struct {
		name   string
		writer unsupportedCgroupKillWriter
	}{
		{name: "write", writer: unsupportedCgroupKillWriter{writeErr: syscall.EOPNOTSUPP}},
		{name: "close", writer: unsupportedCgroupKillWriter{closeErr: syscall.EOPNOTSUPP}},
	} {
		t.Run(test.name, func(t *testing.T) {
			root := t.TempDir()
			if err := os.WriteFile(filepath.Join(root, "cgroup.procs"), nil, 0o600); err != nil {
				t.Fatal(err)
			}
			result, err := killCgroupTreeWithOpen(root, func(string) (io.WriteCloser, error) { return test.writer, nil })
			if err != nil || result.Method != "recursive_signal" {
				t.Fatalf("unsupported cgroup.kill %s result=%+v err=%v", test.name, result, err)
			}
		})
	}
}

func TestParseRetryAfterAcceptsSecondsAndHTTPDate(t *testing.T) {
	now := time.Date(2026, time.August, 23, 12, 0, 0, 0, time.UTC)
	if delay := parseRetryAfter("17", now); delay != 17*time.Second {
		t.Fatalf("delta Retry-After = %s, want 17s", delay)
	}
	if delay := parseRetryAfter(now.Add(23*time.Second).Format(http.TimeFormat), now); delay != 23*time.Second {
		t.Fatalf("date Retry-After = %s, want 23s", delay)
	}
	if delay := parseRetryAfter("invalid", now); delay != 0 {
		t.Fatalf("invalid Retry-After = %s, want zero", delay)
	}
}

func TestRegistryTransportReturnsTransientStatusToAgentWithoutMechanicsRetry(t *testing.T) {
	tracker := &retryAfterTracker{}
	transport := retryAfterTransport{tracker: tracker, base: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusTooManyRequests,
			Header:     http.Header{"Retry-After": []string{"11"}},
			Body:       io.NopCloser(bytes.NewReader(nil)),
		}, nil
	})}
	response, err := transport.RoundTrip(&http.Request{})
	var statusFailure *registryStatusError
	if response != nil || !errors.As(err, &statusFailure) || statusFailure.statusCode != http.StatusTooManyRequests {
		t.Fatalf("transient registry response=%+v err=%v", response, err)
	}
	if tracker.Delay() != 11*time.Second {
		t.Fatalf("tracked Retry-After = %s, want 11s", tracker.Delay())
	}
}

func TestPublicRegistryMechanicsFactsComeFromRegistryBehavior(t *testing.T) {
	index := []byte(`{"schemaVersion":2,"mediaType":"application/vnd.oci.image.index.v1+json","manifests":[]}`)
	tests := []struct {
		name       string
		status     int
		mediaType  string
		retryAfter string
		tokenError bool
		delay      time.Duration
		wantKind   ImageFailureKind
		wantStatus int
		wantRetry  time.Duration
	}{
		{name: "503", status: 503, retryAfter: "7", wantKind: ImageFailureHTTP, wantStatus: 503, wantRetry: 7 * time.Second},
		{name: "429", status: 429, retryAfter: "11", wantKind: ImageFailureHTTP, wantStatus: 429, wantRetry: 11 * time.Second},
		{name: "404", status: 404, wantKind: ImageFailureHTTP, wantStatus: 404},
		{name: "401", tokenError: true, wantKind: ImageFailureHTTP, wantStatus: 401},
		{name: "unsupported media type", status: 200, mediaType: "text/plain", wantKind: ImageFailureManifestRejected},
		{name: "response timeout", status: 200, mediaType: ocispec.MediaTypeImageIndex, delay: 100 * time.Millisecond, wantKind: ImageFailureNetwork},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var server *httptest.Server
			server = httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
				switch request.URL.Path {
				case "/v2/":
					response.WriteHeader(http.StatusOK)
				case "/token":
					if test.tokenError {
						response.WriteHeader(http.StatusUnauthorized)
						return
					}
					response.Header().Set("Content-Type", "application/json")
					_, _ = response.Write([]byte(`{"token":"test-token"}`))
				case "/v2/repo/manifests/tag":
					if request.Header.Get("Authorization") == "" {
						response.Header().Set("WWW-Authenticate", `Bearer realm="`+server.URL+`/token",service="test",scope="repository:repo:pull"`)
						response.WriteHeader(http.StatusUnauthorized)
						return
					}
					if test.delay > 0 {
						time.Sleep(test.delay)
					}
					if test.retryAfter != "" {
						response.Header().Set("Retry-After", test.retryAfter)
					}
					if test.status != 0 && test.status != http.StatusOK {
						response.WriteHeader(test.status)
						return
					}
					mediaType := test.mediaType
					if mediaType == "" {
						mediaType = ocispec.MediaTypeImageIndex
					}
					response.Header().Set("Content-Type", mediaType)
					response.Header().Set("Docker-Content-Digest", digest.FromBytes(index).String())
					response.Header().Set("Content-Length", fmt.Sprint(len(index)))
					if request.Method != http.MethodHead {
						_, _ = response.Write(index)
					}
				default:
					response.WriteHeader(http.StatusNotFound)
				}
			}))
			defer server.Close()
			tracker := &retryAfterTracker{}
			ctx := t.Context()
			if test.delay > 0 {
				var cancel context.CancelFunc
				ctx, cancel = context.WithTimeout(ctx, 10*time.Millisecond)
				defer cancel()
			}
			_, descriptor, err := publicResolver(tracker).Resolve(ctx, strings.TrimPrefix(server.URL, "http://")+"/repo:tag")
			if err == nil {
				err = validateResolvedImageDescriptor(descriptor)
			}
			if err == nil {
				t.Fatal("registry behavior unexpectedly resolved")
			}
			var mechanics *ImageMechanicsError
			if !errors.As(classifyRegistryError(err, tracker.Delay(), ""), &mechanics) {
				t.Fatalf("registry error has no mechanics fact: %v", err)
			}
			if mechanics.Fact.Kind != test.wantKind || mechanics.Fact.HTTPStatus != test.wantStatus || mechanics.Fact.RetryAfter != test.wantRetry {
				t.Fatalf("mechanics fact = %+v", mechanics.Fact)
			}
		})
	}
}

func TestPullPublicImagePinnedDigestRefusalIsNetworkMechanics(t *testing.T) {
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	registryAddress := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}

	expectedDigest := digest.FromString("pinned public pull refusal").String()
	reference, resolvedDigest, err := (&ContainerdEngine{}).resolvePublicImage(t.Context(), registryAddress+"/repo:tag", expectedDigest)
	if err != nil || resolvedDigest != expectedDigest {
		t.Fatalf("pinned image resolution = (%q, %q, %v)", reference, resolvedDigest, err)
	}
	puller := publicImagePullFunc(func(ctx context.Context, pinnedReference string, options ...containerd.RemoteOpt) (containerd.Image, error) {
		remoteContext := &containerd.RemoteContext{}
		for _, option := range options {
			if optionErr := option(nil, remoteContext); optionErr != nil {
				return nil, optionErr
			}
		}
		if remoteContext.Resolver == nil {
			return nil, errors.New("public pull omitted its registry resolver")
		}
		_, _, pullErr := remoteContext.Resolver.Resolve(ctx, pinnedReference)
		if pullErr == nil {
			return nil, errors.New("closed registry port unexpectedly resolved")
		}
		if !strings.Contains(strings.ToLower(pullErr.Error()), "connection refused") {
			t.Fatalf("closed registry port error = %v, want ECONNREFUSED", pullErr)
		}
		return nil, fmt.Errorf("failed to resolve reference %q: %w", pinnedReference, pullErr)
	})
	err = pullPublicImageContent(t.Context(), puller, reference, resolvedDigest, platforms.DefaultStrict())
	var mechanics *ImageMechanicsError
	if !errors.As(err, &mechanics) || mechanics.Fact.Kind != ImageFailureNetwork || mechanics.Fact.TopLevelDigest != expectedDigest {
		t.Fatalf("pinned pull refusal mechanics = %+v err=%v, want network for %s", mechanics, err, expectedDigest)
	}
	helperConnection, agentConnection := net.Pipe()
	go func() {
		writeImageStreamResult(newFramedConn(helperConnection), err)
		_ = helperConnection.Close()
	}()
	rpcErr := decodeResponse(newFramedConn(agentConnection), nil)
	_ = agentConnection.Close()
	var rpcFailure *RPCError
	if !errors.As(rpcErr, &rpcFailure) || rpcFailure.Code != CodeImageUnavailable || rpcFailure.ImageFailure == nil || rpcFailure.ImageFailure.Kind != ImageFailureNetwork {
		t.Fatalf("pinned pull refusal RPC = %v, want image_unavailable with network mechanics", rpcErr)
	}
	if rpcErrorProvesRuntimeLoss(rpcFailure) {
		t.Fatalf("pinned pull refusal would mark the helper session lost: %+v", rpcFailure)
	}
}

func TestPublicRegistryTokenChallengeResolvesIndex(t *testing.T) {
	manifest := []byte(`{"schemaVersion":2,"mediaType":"application/vnd.oci.image.manifest.v1+json","config":{"mediaType":"application/vnd.oci.image.config.v1+json","digest":"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","size":2},"layers":[]}`)
	manifestDescriptor := ocispec.Descriptor{MediaType: ocispec.MediaTypeImageManifest, Digest: digest.FromBytes(manifest), Size: int64(len(manifest)), Platform: &ocispec.Platform{OS: "linux", Architecture: "amd64"}}
	index, err := json.Marshal(struct {
		SchemaVersion int                  `json:"schemaVersion"`
		MediaType     string               `json:"mediaType"`
		Manifests     []ocispec.Descriptor `json:"manifests"`
	}{SchemaVersion: 2, MediaType: ocispec.MediaTypeImageIndex, Manifests: []ocispec.Descriptor{manifestDescriptor}})
	if err != nil {
		t.Fatal(err)
	}
	var server *httptest.Server
	server = httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if request.URL.Path == "/token" {
			_, _ = response.Write([]byte(`{"token":"test-token"}`))
			return
		}
		if request.URL.Path == "/v2/repo/manifests/tag" && request.Header.Get("Authorization") == "" {
			response.Header().Set("WWW-Authenticate", `Bearer realm="`+server.URL+`/token",service="test",scope="repository:repo:pull"`)
			response.WriteHeader(http.StatusUnauthorized)
			return
		}
		payload := index
		mediaType := ocispec.MediaTypeImageIndex
		if strings.HasSuffix(request.URL.Path, manifestDescriptor.Digest.String()) {
			payload = manifest
			mediaType = ocispec.MediaTypeImageManifest
		}
		response.Header().Set("Content-Type", mediaType)
		response.Header().Set("Docker-Content-Digest", digest.FromBytes(payload).String())
		response.Header().Set("Content-Length", fmt.Sprint(len(payload)))
		if request.Method != http.MethodHead {
			_, _ = response.Write(payload)
		}
	}))
	defer server.Close()
	resolver := publicResolver(&retryAfterTracker{})
	name := strings.TrimPrefix(server.URL, "http://") + "/repo:tag"
	_, descriptor, err := resolver.Resolve(t.Context(), name)
	if err != nil || descriptor.MediaType != ocispec.MediaTypeImageIndex || descriptor.Digest != digest.FromBytes(index) {
		t.Fatalf("token challenge index resolution = (%+v, %v)", descriptor, err)
	}
	fetcher, err := resolver.Fetcher(t.Context(), name)
	if err != nil {
		t.Fatal(err)
	}
	for _, expected := range []struct {
		descriptor ocispec.Descriptor
		payload    []byte
	}{{descriptor: descriptor, payload: index}, {descriptor: manifestDescriptor, payload: manifest}} {
		reader, fetchErr := fetcher.Fetch(t.Context(), expected.descriptor)
		if fetchErr != nil {
			t.Fatal(fetchErr)
		}
		payload, readErr := io.ReadAll(reader)
		_ = reader.Close()
		if readErr != nil || !bytes.Equal(payload, expected.payload) {
			t.Fatalf("fetched registry descriptor %s = (%q, %v)", expected.descriptor.Digest, payload, readErr)
		}
	}
}

func TestSelectedManifestRejectsWrongArchitectureAndMislabeledIndexes(t *testing.T) {
	if runtime.GOARCH != "amd64" {
		t.Skip("amd64 runtime is the authority for the arm64-only row")
	}
	store, err := contentlocal.NewStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	ctx := t.Context()
	wrongManifest := testContentManifest(t, ctx, store, ocispec.Platform{OS: "linux", Architecture: "arm64"})
	armLabel := ocispec.Platform{OS: "linux", Architecture: "arm64"}
	hostLabel := ocispec.Platform{OS: "linux", Architecture: "amd64"}
	tests := []struct {
		name   string
		target ocispec.Descriptor
	}{
		{name: "wrong-arch root manifest", target: wrongManifest},
		{name: "mislabeled index", target: testContentIndex(t, ctx, store, descriptorWithPlatform(wrongManifest, hostLabel))},
		{name: "arm64-only index", target: testContentIndex(t, ctx, store, descriptorWithPlatform(wrongManifest, armLabel))},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, _, err := selectedManifest(ctx, store, test.target, platforms.DefaultStrict())
			var mechanics *ImageMechanicsError
			if !errors.As(err, &mechanics) || mechanics.Fact.Kind != ImageFailurePlatformMismatch {
				t.Fatalf("platform selection error = %v", err)
			}
		})
	}
}

func TestSweepSkipsSpoolOwnedByLiveImageOperation(t *testing.T) {
	root := t.TempDir()
	imports := filepath.Join(root, "imports")
	if err := os.MkdirAll(imports, 0o700); err != nil {
		t.Fatal(err)
	}
	live := filepath.Join(imports, "wefty-image-live.tar")
	stale := filepath.Join(imports, "wefty-image-stale.tar")
	for _, path := range []string{live, stale} {
		if err := os.WriteFile(path, []byte("archive"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	engine := &ContainerdEngine{config: NativeEngineConfig{RuntimeRoot: root}, activeSpools: map[string]struct{}{live: {}}, activeLeases: make(map[string]struct{})}
	if err := engine.sweepImageSpools(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(live); err != nil {
		t.Fatalf("live spool was removed: %v", err)
	}
	if _, err := os.Stat(stale); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("stale spool remained: %v", err)
	}
}

func TestHandoffRetentionRefreshesAndExpiresStableOwnerVolumes(t *testing.T) {
	root := t.TempDir()
	engine := &ContainerdEngine{config: NativeEngineConfig{RuntimeRoot: root, HandoffRetention: time.Hour}}
	name, err := DeterministicHandoffVolumeDirectory("run-owner")
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, "handoffs", name)
	if err := os.MkdirAll(path, 0o700); err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	if err := os.Chtimes(path, now.Add(-30*time.Minute), now.Add(-30*time.Minute)); err != nil {
		t.Fatal(err)
	}
	if err := engine.cleanupExpiredHandoffs(now); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("retry-window handoff was removed: %v", err)
	}
	if err := os.Chtimes(path, now.Add(-2*time.Hour), now.Add(-2*time.Hour)); err != nil {
		t.Fatal(err)
	}
	if err := engine.cleanupExpiredHandoffs(now); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("expired handoff remained: %v", err)
	}
}

func TestOwnerKeyedHandoffBytesSurviveAttemptsUntilExplicitFinalization(t *testing.T) {
	root := t.TempDir()
	engine := &ContainerdEngine{config: NativeEngineConfig{RuntimeRoot: root}}
	name, err := DeterministicHandoffVolumeDirectory("run-owner")
	if err != nil {
		t.Fatal(err)
	}
	request := RunRequest{
		Resources: ResourceIdentity{HandoffVolumeDirectory: name},
		Workload:  WorkloadInput{ManagedVolumes: []ManagedVolumeDescriptor{{Kind: ManagedVolumeHandoff, OwnerKey: "run-owner"}}},
	}
	first, _, _, err := engine.managedVolumeSources(t.Context(), &request)
	if err != nil {
		t.Fatal(err)
	}
	marker := filepath.Join(first[ManagedVolumeHandoff], "marker")
	if err := os.WriteFile(marker, []byte("handoff bytes\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	second, _, _, err := engine.managedVolumeSources(t.Context(), &request)
	if err != nil {
		t.Fatal(err)
	}
	payload, err := os.ReadFile(filepath.Join(second[ManagedVolumeHandoff], "marker"))
	if err != nil || string(payload) != "handoff bytes\n" {
		t.Fatalf("reused handoff marker = %q err=%v", payload, err)
	}
	deleted, err := engine.DeleteManagedVolume(t.Context(), DeleteManagedVolumeRequest{Kind: ManagedVolumeHandoff, OwnerKey: "run-owner"})
	if err != nil || !deleted.Deleted {
		t.Fatalf("handoff finalization = %+v err=%v", deleted, err)
	}
	if _, err := os.Stat(first[ManagedVolumeHandoff]); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("finalized handoff remains: %v", err)
	}
}

func TestServiceDataDeletionRemovesBytesAndOwnerRecord(t *testing.T) {
	root := t.TempDir()
	engine := &ContainerdEngine{config: NativeEngineConfig{RuntimeRoot: root}}
	authority := AttemptAuthority{
		NodeID: "node", BootSessionID: "boot", JobID: "service-delete", AttemptID: "attempt",
		FencingToken: "fence", Class: contract.JobClassService, RemovalGeneration: "1",
	}
	resources, err := DeterministicResourceIdentity(authority)
	if err != nil {
		t.Fatal(err)
	}
	volume := filepath.Join(root, "service-data", resources.ServiceVolumeDirectory)
	if err := os.MkdirAll(volume, 0o700); err != nil {
		t.Fatal(err)
	}
	fresh, err := serviceVolumeCreationAt(volume)
	if err != nil {
		t.Fatal(err)
	}
	if err := engine.initializeServiceVolume(volume, resources.ServiceVolumeOwnerRecord, fresh, uint32(os.Getuid()), uint32(os.Getgid())); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(volume, "tenant-bytes"), []byte("delete me"), 0o600); err != nil {
		t.Fatal(err)
	}
	removal := &ManagedVolumeRemovalAuthority{
		NodeID: authority.NodeID, BootSessionID: authority.BootSessionID, JobID: authority.JobID,
		RemovalGeneration: 1, CleanupFence: "cleanup-fence",
	}
	response, err := engine.DeleteManagedVolume(t.Context(), DeleteManagedVolumeRequest{Kind: ManagedVolumeServiceData, OwnerKey: authority.JobID, Removal: removal})
	if err != nil || !response.Deleted {
		t.Fatalf("service data deletion = %+v err=%v", response, err)
	}
	for _, path := range []string{volume, filepath.Join(root, "service-data-state", resources.ServiceVolumeOwnerRecord)} {
		if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("deleted service-data path %q remains: %v", path, err)
		}
	}
	if _, err := engine.DeleteManagedVolume(t.Context(), DeleteManagedVolumeRequest{Kind: ManagedVolumeServiceData, OwnerKey: authority.JobID, Removal: removal}); err != nil {
		t.Fatalf("idempotent service data deletion: %v", err)
	}
}

func TestServiceDataDeletionRequiresRemovalAuthority(t *testing.T) {
	engine := &ContainerdEngine{config: NativeEngineConfig{RuntimeRoot: t.TempDir()}}
	if _, err := engine.DeleteManagedVolume(t.Context(), DeleteManagedVolumeRequest{Kind: ManagedVolumeServiceData, OwnerKey: "service-delete"}); err == nil {
		t.Fatal("service data deletion without removal authority succeeded")
	}
}

func TestRemovalReceiptRowsAreAssertionDerived(t *testing.T) {
	authority := AttemptAuthority{
		NodeID: "node", BootSessionID: "boot", JobID: "service-attest", AttemptID: "attempt",
		FencingToken: "fence", Class: contract.JobClassService, RemovalGeneration: "1",
	}
	identity, err := DeterministicResourceIdentity(authority)
	if err != nil {
		t.Fatal(err)
	}
	request := AttestRemovalRequest{
		JobID: authority.JobID, RemovalGeneration: authority.RemovalGeneration,
		Attempts: []RemovalAttemptManifest{{Authority: authority, Resources: expectedRemovalResources(identity)}},
	}
	residue := ResourceInventory{Tasks: []string{identity.TaskID}}
	if response, err := attestRemovalInventory(residue, request); err == nil || len(response.Assertions) != 0 {
		t.Fatalf("receipt recorded PASS with task residue: response=%+v err=%v", response, err)
	}
	response, err := attestRemovalInventory(ResourceInventory{}, request)
	if err != nil || len(response.Assertions) != len(request.Attempts[0].Resources) {
		t.Fatalf("complete removal assertions = %+v err=%v", response, err)
	}
	for _, assertion := range response.Assertions {
		if !assertion.Absent {
			t.Fatalf("executed assertion did not record absence: %+v", assertion)
		}
	}
	invalid := request
	invalid.Attempts = []RemovalAttemptManifest{{Authority: authority, Resources: append(slices.Clone(request.Attempts[0].Resources), RemovalResource{Class: "future-unverified", ID: "disk"})}}
	if response, err := attestRemovalInventory(ResourceInventory{}, invalid); err == nil || len(response.Assertions) != 0 {
		t.Fatalf("unexecuted future class recorded PASS: response=%+v err=%v", response, err)
	}
}

func TestComputerRemovalReceiptAssertsEveryDiskInventoryClass(t *testing.T) {
	authority := AttemptAuthority{
		NodeID: "node", BootSessionID: "boot", JobID: "computer-attest", AttemptID: "attempt",
		FencingToken: "fence", Class: contract.JobClassService, RemovalGeneration: "1",
	}
	identity, err := DeterministicResourceIdentity(authority)
	if err != nil {
		t.Fatal(err)
	}
	storage := &ComputerStorageReference{ComputerID: "computer", StorageID: "storage", StorageGeneration: 2, DiskBytes: 8 << 30}
	resources := expectedRemovalResources(identity, storage)
	request := AttestRemovalRequest{JobID: authority.JobID, RemovalGeneration: authority.RemovalGeneration,
		Attempts: []RemovalAttemptManifest{{Authority: authority, ComputerStorage: storage, Resources: resources}}}
	if err := validateAttestRemovalRequest(request, authority.NodeID); err != nil {
		t.Fatalf("Computer removal manifest rejected: %v", err)
	}
	name, err := DeterministicComputerDiskName(*storage)
	if err != nil {
		t.Fatal(err)
	}
	for _, residue := range []ResourceInventory{
		{ComputerDiskImages: []string{name}}, {ComputerDiskAllocations: []string{name}},
		{ComputerDiskQuotas: []string{name}}, {ComputerDiskManifests: []string{name}},
		{ComputerDiskMounts: []string{name}}, {ComputerDiskLoops: []string{name}},
		{ComputerAttachments: []string{name}},
		{ComputerDiskAnomalies: []string{name + ":image_not_regular"}},
	} {
		if response, err := attestRemovalInventory(residue, request); err == nil || len(response.Assertions) != 0 {
			t.Fatalf("Computer residue recorded PASS: inventory=%+v response=%+v err=%v", residue, response, err)
		}
	}
	response, err := attestRemovalInventory(ResourceInventory{}, request)
	if err != nil || len(response.Assertions) != len(resources) {
		t.Fatalf("Computer absence receipt = %+v err=%v, want %d rows", response, err, len(resources))
	}
}

func TestPreparedComputerStorageRemovalInventoryCarriesOnlyStorageRows(t *testing.T) {
	storage := &ComputerStorageReference{ComputerID: "computer", StorageID: "storage", StorageGeneration: 3, DiskBytes: 8 << 30}
	removal := ManagedVolumeRemovalAuthority{NodeID: "node", BootSessionID: "boot", JobID: "prepared-computer", RemovalGeneration: 2, CleanupFence: "cleanup"}
	request := InventoryRemovalRequest{Removal: removal, RootInstanceID: "root"}
	receipt := &ComputerStorageResetReceipt{Kind: "computer_storage_reset_verified", ReceiptID: "receipt", ComputerID: storage.ComputerID,
		StorageID: storage.StorageID, NewGeneration: storage.StorageGeneration, NodeID: removal.NodeID, RootInstanceID: "root",
		JobID: removal.JobID, IntentRevision: 1, CleanupFence: "preparation", HelperGeneration: 1}
	attempt, eligible, err := preparedComputerStorageRemovalAttempt(request, t.TempDir(), computerDiskManifest{
		Version: computerDiskManifestVersion, Storage: *storage, DiskImage: "disk.ext4", MountDirectory: "disk", Prepared: true,
		PreparationReceipt: receipt,
	})
	if err != nil || !eligible {
		t.Fatalf("prepared Computer Storage-only inventory = %+v eligible=%t err=%v", attempt, eligible, err)
	}
	attest := AttestRemovalRequest{JobID: removal.JobID, RemovalGeneration: "2", Attempts: []RemovalAttemptManifest{attempt}}
	if err := validateAttestRemovalRequest(attest, removal.NodeID); err != nil {
		t.Fatalf("prepared Computer Storage-only inventory rejected: %v", err)
	}
	if len(attempt.Resources) != 9 {
		t.Fatalf("prepared Computer Storage-only rows = %+v", attempt.Resources)
	}
	for _, resource := range attempt.Resources {
		switch resource.Class {
		case RemovalResourceLease, RemovalResourceSnapshot, RemovalResourceContainer, RemovalResourceTask,
			RemovalResourceShim, RemovalResourceCgroup, RemovalResourceLogSegments, RemovalResourceHandoffVolume:
			t.Fatalf("prepared Computer inventory invented runtime row: %+v", resource)
		}
	}
	notPrepared := computerDiskManifest{Version: computerDiskManifestVersion, Storage: *storage, Prepared: true, PreparationReceipt: receipt,
		PreviousDetachment: &computerDiskEvidence{ReceiptID: "prior"}}
	if attempt, eligible, err := preparedComputerStorageRemovalAttempt(request, t.TempDir(), notPrepared); err != nil || eligible {
		t.Fatalf("historically attached Computer became Storage-only inventory: %+v eligible=%t err=%v", attempt, eligible, err)
	}
	for name, mutate := range map[string]func(*computerDiskManifest){
		"attached":   func(m *computerDiskManifest) { m.Attached = &AttemptAuthority{AttemptID: "attached"} },
		"pending":    func(m *computerDiskManifest) { m.Pending = &AttemptAuthority{AttemptID: "pending"} },
		"retirement": func(m *computerDiskManifest) { m.Retirement = &ComputerStorageResetAuthority{JobID: "retire"} },
		"loop":       func(m *computerDiskManifest) { m.LoopDevice = "/dev/loop0" },
	} {
		t.Run(name, func(t *testing.T) {
			manifest := computerDiskManifest{Version: computerDiskManifestVersion, Storage: *storage, Prepared: true, PreparationReceipt: receipt}
			mutate(&manifest)
			if attempt, eligible, err := preparedComputerStorageRemovalAttempt(request, t.TempDir(), manifest); err != nil || eligible {
				t.Fatalf("attachment lineage became Storage-only: %+v eligible=%t err=%v", attempt, eligible, err)
			}
		})
	}
	t.Run("copy-published-import-witness", func(t *testing.T) {
		root := t.TempDir()
		copyReceipt := ComputerStorageCopyReceipt{Kind: contract.ComputerStorageCopyVerifiedKind, ReceiptID: "copy-receipt",
			DestinationComputerID: storage.ComputerID, DestinationStorageID: storage.StorageID,
			DestinationGeneration: storage.StorageGeneration, NodeID: removal.NodeID, RootInstanceID: "root",
			JobID: removal.JobID, OperationRevision: 2, CleanupFence: "copy-fence", HelperGeneration: 3}
		if err := writeComputerStorageCopyManifest(root, computerStorageCopyManifest{Version: 1, Phase: computerStorageCopyPublished, Receipt: &copyReceipt}); err != nil {
			t.Fatal(err)
		}
		manifest := computerDiskManifest{Version: computerDiskManifestVersion, Storage: *storage, Prepared: true}
		attempt, eligible, err := preparedComputerStorageRemovalAttempt(request, root, manifest)
		if err != nil || !eligible || attempt.StoragePreparation == nil || attempt.StoragePreparation.ReceiptID != copyReceipt.ReceiptID {
			t.Fatalf("published copy witness = %+v eligible=%t err=%v", attempt, eligible, err)
		}
	})
}

func TestDetachedComputerStorageRemovalInventoryReturnsTypedEmptyEvidence(t *testing.T) {
	runtimeRoot := t.TempDir()
	storage := ComputerStorageReference{ComputerID: "computer", StorageID: "storage", StorageGeneration: 3, DiskBytes: 8 << 30}
	name, err := DeterministicComputerDiskName(storage)
	if err != nil {
		t.Fatal(err)
	}
	diskRoot := filepath.Join(runtimeRoot, "computer-disks", name)
	if err := os.MkdirAll(diskRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := writeComputerDiskManifest(diskRoot, computerDiskManifest{
		Version: computerDiskManifestVersion, Storage: storage, DiskImage: "disk.ext4", MountDirectory: name,
		PreviousDetachment: &computerDiskEvidence{ReceiptID: "detached"},
	}); err != nil {
		t.Fatal(err)
	}
	engine := &ContainerdEngine{config: NativeEngineConfig{RuntimeRoot: runtimeRoot}, diskSystem: newFakeComputerDiskSystem()}
	request := InventoryRemovalRequest{Removal: ManagedVolumeRemovalAuthority{
		NodeID: "node", BootSessionID: "boot", JobID: "detached-computer", RemovalGeneration: 2, CleanupFence: "cleanup",
	}, RootInstanceID: "root", ComputerStorage: &storage}

	response, err := engine.inventoryComputerStorageRemoval(t.Context(), request, ResourceInventory{})
	if err != nil || !response.NoStorageEvidence || response.NoRuntimeAttempts || len(response.Attempts) != 0 {
		t.Fatalf("detached Computer Storage inventory = %+v err=%v", response, err)
	}
}

func TestAbsentComputerStorageInventoryWaitsForReimageSerialization(t *testing.T) {
	runtimeRoot := t.TempDir()
	storage := ComputerStorageReference{ComputerID: "computer", StorageID: "storage", StorageGeneration: 3, DiskBytes: 8 << 30}
	engine := &ContainerdEngine{config: NativeEngineConfig{RuntimeRoot: runtimeRoot}, diskSystem: newFakeComputerDiskSystem()}
	request := InventoryRemovalRequest{Removal: ManagedVolumeRemovalAuthority{
		NodeID: "node", BootSessionID: "boot", JobID: "detached-computer", RemovalGeneration: 2, CleanupFence: "cleanup",
	}, RootInstanceID: "root", ComputerStorage: &storage}
	engine.computerReimageMu.Lock()
	defer engine.computerReimageMu.Unlock()
	ctx, cancel := context.WithTimeout(t.Context(), 25*time.Millisecond)
	defer cancel()
	got, err := engine.inventoryComputerStorageRemoval(ctx, request, ResourceInventory{})
	if !errors.Is(err, context.DeadlineExceeded) || got.NoStorageEvidence || len(got.Attempts) != 0 {
		t.Fatalf("absence was determined outside reimage serialization: response=%+v err=%v", got, err)
	}
}

func TestAbsentComputerStorageRemovalInventoryIsTypedAndAttestable(t *testing.T) {
	storage := ComputerStorageReference{ComputerID: "computer", StorageID: "storage", StorageGeneration: 2, DiskBytes: 8 << 30}
	request := InventoryRemovalRequest{Removal: ManagedVolumeRemovalAuthority{
		NodeID: "node", BootSessionID: "boot", JobID: "prepared-computer", RemovalGeneration: 3, CleanupFence: "cleanup",
	}, RootInstanceID: "root", ComputerStorage: &storage}
	attempt, err := absentComputerStorageRemovalAttempt(request, storage)
	if err != nil {
		t.Fatal(err)
	}
	if !attempt.StorageOnly || !attempt.StorageAbsent || attempt.StoragePreparation != nil ||
		attempt.Authority.AttemptID != contract.StorageAbsentRemovalAttemptID(storage.StorageGeneration) || len(attempt.Resources) != 9 {
		t.Fatalf("already-deleted Storage evidence = %+v", attempt)
	}
	attest := AttestRemovalRequest{JobID: request.Removal.JobID, RemovalGeneration: "3", Attempts: []RemovalAttemptManifest{attempt}}
	if err := validateAttestRemovalRequest(attest, request.Removal.NodeID); err != nil {
		t.Fatalf("already-deleted Storage evidence was not attestable: %v", err)
	}
	attempt.StorageAbsent = false
	if err := validateAttestRemovalRequest(AttestRemovalRequest{JobID: request.Removal.JobID, RemovalGeneration: "3", Attempts: []RemovalAttemptManifest{attempt}}, request.Removal.NodeID); err == nil {
		t.Fatal("already-deleted Storage authority was accepted without its typed evidence bit")
	}
}

func TestComputerRemovalInventoryReimageSerializationIsContextBounded(t *testing.T) {
	var mutex sync.Mutex
	mutex.Lock()
	ctx, cancel := context.WithTimeout(t.Context(), 25*time.Millisecond)
	defer cancel()
	started := time.Now()
	if lockComputerReimageMutex(ctx, &mutex) {
		mutex.Unlock()
		t.Fatal("context-bounded Computer removal serialization acquired an owned reimage mutex")
	}
	mutex.Unlock()
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("Computer removal serialization ignored context for %s", elapsed)
	}
}

func TestServiceDataVolumeInitializesOwnerOnlyOnce(t *testing.T) {
	root := t.TempDir()
	engine := &ContainerdEngine{config: NativeEngineConfig{RuntimeRoot: root}}
	resources, err := DeterministicResourceIdentity(AttemptAuthority{
		NodeID: "node", BootSessionID: "boot", JobID: "service-job", AttemptID: "attempt-1",
		FencingToken: "fence-1", Class: contract.JobClassService, RemovalGeneration: "attempt",
	})
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, "service-data", resources.ServiceVolumeDirectory)
	if err := os.MkdirAll(path, 0o700); err != nil {
		t.Fatal(err)
	}
	uid, gid := uint32(os.Getuid()), uint32(os.Getgid())
	fresh, err := serviceVolumeCreationAt(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := engine.initializeServiceVolume(path, resources.ServiceVolumeOwnerRecord, fresh, uid, gid); err != nil {
		t.Fatal(err)
	}
	var stat syscall.Stat_t
	if err := syscall.Stat(path, &stat); err != nil || stat.Uid != uid || stat.Gid != gid {
		t.Fatalf("service data owner = %d:%d err=%v, want %d:%d", stat.Uid, stat.Gid, err, uid, gid)
	}
	marker := filepath.Join(path, "retained")
	if err := os.WriteFile(marker, []byte("service bytes\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := engine.initializeServiceVolume(path, resources.ServiceVolumeOwnerRecord, nil, uid, gid); err != nil {
		t.Fatal(err)
	}
	if payload, err := os.ReadFile(marker); err != nil || string(payload) != "service bytes\n" {
		t.Fatalf("service data after repeated initialization = %q err=%v", payload, err)
	}
	if err := engine.initializeServiceVolume(path, resources.ServiceVolumeOwnerRecord, nil, uid+1, gid); err == nil {
		t.Fatal("service data volume was re-owned for a different image identity")
	}
}

func TestServiceDataOwnerRecordRecoveryRows(t *testing.T) {
	for _, row := range []struct {
		name                                                                             string
		fresh, recordPresent, recordIdentity, recordOwner, actualOwner, empty, rootOwned bool
		wantChown, wantWrite                                                             bool
	}{
		{name: "orphan-marker", fresh: true, recordPresent: true, actualOwner: false, empty: true, rootOwned: true, wantChown: true, wantWrite: true},
		{name: "missing-marker-with-data", recordPresent: false, actualOwner: true, empty: false, wantWrite: true},
		{name: "crash-between-mkdir-and-marker", recordPresent: false, actualOwner: false, empty: true, rootOwned: true, wantChown: true, wantWrite: true},
	} {
		t.Run("decision-"+row.name, func(t *testing.T) {
			decision := decideServiceVolumeInitialization(row.fresh, row.recordPresent, row.recordIdentity, row.recordOwner, row.actualOwner, row.empty, row.rootOwned)
			if decision.rejection != "" || decision.chown != row.wantChown || decision.writeRecord != row.wantWrite {
				t.Fatalf("decision = %+v, want chown=%t write=%t", decision, row.wantChown, row.wantWrite)
			}
		})
	}

	uid, gid := uint32(os.Getuid()), uint32(os.Getgid())
	initializeFresh := func(t *testing.T, engine *ContainerdEngine, path, recordName string) error {
		t.Helper()
		fresh, err := serviceVolumeCreationAt(path)
		if err != nil {
			return err
		}
		return engine.initializeServiceVolume(path, recordName, fresh, uid, gid)
	}
	newFixture := func(t *testing.T, jobID string) (*ContainerdEngine, ResourceIdentity, string, string) {
		t.Helper()
		root := t.TempDir()
		engine := &ContainerdEngine{config: NativeEngineConfig{RuntimeRoot: root}}
		resources, err := DeterministicResourceIdentity(AttemptAuthority{
			NodeID: "node", BootSessionID: "boot", JobID: jobID, AttemptID: "attempt-1",
			FencingToken: "fence-1", Class: contract.JobClassService, RemovalGeneration: "attempt",
		})
		if err != nil {
			t.Fatal(err)
		}
		path := filepath.Join(root, "service-data", resources.ServiceVolumeDirectory)
		record := filepath.Join(root, "service-data-state", resources.ServiceVolumeOwnerRecord)
		return engine, resources, path, record
	}

	t.Run("orphan-marker", func(t *testing.T) {
		engine, resources, path, record := newFixture(t, "orphan-marker")
		if err := os.MkdirAll(path, 0o700); err != nil {
			t.Fatal(err)
		}
		if err := initializeFresh(t, engine, path, resources.ServiceVolumeOwnerRecord); err != nil {
			t.Fatal(err)
		}
		before, err := os.ReadFile(record)
		if err != nil {
			t.Fatal(err)
		}
		var stale serviceVolumeOwnerRecord
		if err := json.Unmarshal(before, &stale); err != nil {
			t.Fatal(err)
		}
		stale.Inode++
		stalePayload, err := json.Marshal(stale)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(record, stalePayload, 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.RemoveAll(path); err != nil {
			t.Fatal(err)
		}
		if err := os.Mkdir(path, 0o700); err != nil {
			t.Fatal(err)
		}
		if err := initializeFresh(t, engine, path, resources.ServiceVolumeOwnerRecord); err != nil {
			t.Fatalf("fresh directory was rejected by orphan record: %v", err)
		}
		after, err := os.ReadFile(record)
		if err != nil {
			t.Fatal(err)
		}
		if bytes.Equal(stalePayload, after) {
			t.Fatal("orphan owner record was not rebound to the fresh directory identity")
		}
		var rebound serviceVolumeOwnerRecord
		var stat unix.Stat_t
		if err := json.Unmarshal(after, &rebound); err != nil {
			t.Fatal(err)
		}
		if err := unix.Stat(path, &stat); err != nil {
			t.Fatal(err)
		}
		if rebound.Device != uint64(stat.Dev) || rebound.Inode != stat.Ino || rebound.UID != uid || rebound.GID != gid {
			t.Fatalf("rebound owner record = %+v, directory = %+v", rebound, stat)
		}
	})

	t.Run("missing-marker-with-data", func(t *testing.T) {
		engine, resources, path, record := newFixture(t, "missing-marker-with-data")
		if err := os.MkdirAll(path, 0o700); err != nil {
			t.Fatal(err)
		}
		if err := initializeFresh(t, engine, path, resources.ServiceVolumeOwnerRecord); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(path, "data"), []byte("retained\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.Remove(record); err != nil {
			t.Fatal(err)
		}
		var before unix.Stat_t
		if err := unix.Stat(path, &before); err != nil {
			t.Fatal(err)
		}
		if err := engine.initializeServiceVolume(path, resources.ServiceVolumeOwnerRecord, nil, uid, gid); err != nil {
			t.Fatalf("matching directory without record was rejected: %v", err)
		}
		var after unix.Stat_t
		if err := unix.Stat(path, &after); err != nil {
			t.Fatal(err)
		}
		if before.Ctim != after.Ctim {
			t.Fatal("matching data directory was re-chowned while reconstructing its record")
		}
		if _, err := os.Stat(record); err != nil {
			t.Fatalf("owner record was not reconstructed: %v", err)
		}
	})

	t.Run("crash-between-mkdir-and-marker", func(t *testing.T) {
		engine, resources, path, record := newFixture(t, "mkdir-marker-crash")
		if err := os.MkdirAll(path, 0o700); err != nil {
			t.Fatal(err)
		}
		if err := engine.initializeServiceVolume(path, resources.ServiceVolumeOwnerRecord, nil, uid, gid); err != nil {
			t.Fatalf("interrupted empty directory initialization was not recovered: %v", err)
		}
		if _, err := os.Stat(record); err != nil {
			t.Fatalf("owner record was not durably completed: %v", err)
		}
	})

	t.Run("owner-mismatch-is-typed", func(t *testing.T) {
		engine, resources, path, _ := newFixture(t, "owner-mismatch")
		if err := os.MkdirAll(path, 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(path, "data"), []byte("retained\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		err := engine.initializeServiceVolume(path, resources.ServiceVolumeOwnerRecord, nil, uid+1, gid)
		var rejection *ServiceDataRejectionError
		if !errors.As(err, &rejection) || rejection.ActualUID != uid || rejection.WantedUID != uid+1 {
			t.Fatalf("owner mismatch = %#v, want typed actual/wanted rejection", err)
		}
	})

	t.Run("fresh-directory-swap", func(t *testing.T) {
		engine, resources, path, _ := newFixture(t, "fresh-directory-swap")
		if err := os.MkdirAll(path, 0o700); err != nil {
			t.Fatal(err)
		}
		fresh, err := serviceVolumeCreationAt(path)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.Rename(path, path+"-replaced"); err != nil {
			t.Fatal(err)
		}
		if err := os.Mkdir(path, 0o700); err != nil {
			t.Fatal(err)
		}
		err = engine.initializeServiceVolume(path, resources.ServiceVolumeOwnerRecord, fresh, uid, gid)
		var rejection *ServiceDataRejectionError
		if !errors.As(err, &rejection) || !strings.Contains(rejection.Reason, "identity changed") {
			t.Fatalf("fresh directory swap = %v, want typed identity rejection", err)
		}
	})
}

func TestServiceDataDirectoryAndOwnerRecordAreInventorySubjects(t *testing.T) {
	root := t.TempDir()
	resources, err := DeterministicResourceIdentity(AttemptAuthority{
		NodeID: "node", BootSessionID: "boot", JobID: "inventory-service", AttemptID: "attempt-1",
		FencingToken: "fence-1", Class: contract.JobClassService, RemovalGeneration: "attempt",
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(root, "service-data", resources.ServiceVolumeDirectory), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(root, "service-data-state"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "service-data-state", resources.ServiceVolumeOwnerRecord), []byte("record\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	inventory := ResourceInventory{ManagedVolumes: []string{}, ManagedVolumeRecords: []string{}}
	if err := inventoryManagedVolumeResources(root, &inventory); err != nil {
		t.Fatal(err)
	}
	filtered := filterInventory(inventory, resources, nil)
	if !slices.Equal(filtered.ManagedVolumes, []string{resources.ServiceVolumeDirectory}) || !slices.Equal(filtered.ManagedVolumeRecords, []string{resources.ServiceVolumeOwnerRecord}) {
		t.Fatalf("service data inventory = %+v, want directory and owner record", filtered)
	}
}

func TestFilterInventoryMatchesOnlyExactCgroupOrSystemdScope(t *testing.T) {
	resources, err := DeterministicResourceIdentity(testAuthority())
	if err != nil {
		t.Fatal(err)
	}
	nearCollision := resources.CgroupID + "0"
	inventory := ResourceInventory{Cgroups: []string{resources.CgroupID, resources.CgroupID + ".scope", nearCollision, "prefix-" + resources.CgroupID}}
	filtered := filterInventory(inventory, resources, nil)
	if !slices.Equal(filtered.Cgroups, []string{resources.CgroupID, resources.CgroupID + ".scope"}) {
		t.Fatalf("filtered cgroups = %v", filtered.Cgroups)
	}
}

func TestCgroupInventoryDescendsThroughBroadWrapperAndSweepsBoundChild(t *testing.T) {
	authority := testAuthority()
	resources, err := DeterministicResourceIdentity(authority)
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	broad := "system.slice-cri-wefty-cgroup-legacy.scope"
	for _, name := range []string{broad, "unrelated"} {
		if err := os.Mkdir(filepath.Join(root, name), 0o700); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.Mkdir(filepath.Join(root, broad, resources.CgroupID), 0o700); err != nil {
		t.Fatal(err)
	}
	inventory := ResourceInventory{}
	if err := inventoryAttemptCgroups(root, &inventory); err != nil {
		t.Fatal(err)
	}
	want := []string{broad, filepath.Join(broad, resources.CgroupID)}
	if !slices.Equal(inventory.Cgroups, want) {
		t.Fatalf("broad cgroup inventory = %v", inventory.Cgroups)
	}
	engine := &ContainerdEngine{
		config:          NativeEngineConfig{RuntimeRoot: t.TempDir(), CgroupRoot: root, TaskReleaseTimeout: time.Second},
		cgroupKill:      func(string) (cgroupKillResult, error) { return cgroupKillResult{Method: "test"}, nil },
		cgroupPopulated: func(string) (bool, error) { return false, nil },
		cgroupRemove:    os.RemoveAll,
	}
	if err := engine.ensureAttemptOwnershipRecord(authority, resources); err != nil {
		t.Fatal(err)
	}
	ownership, err := engine.loadAttemptOwnershipRecords()
	if err != nil {
		t.Fatal(err)
	}
	if _, evidence, err := engine.sweepLostAttemptCgroups(t.Context(), inventory.Cgroups, ownership); err != nil || len(evidence) != 1 || evidence[0].ID != filepath.Join(broad, resources.CgroupID) {
		t.Fatalf("nested bound sweep evidence=%+v err=%v", evidence, err)
	}
	if _, err := os.Stat(filepath.Join(root, broad)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("empty broad wrapper remained after bound child reap: %v", err)
	}
}

func TestHandoffInventoryUsesDurableHandoffsRoot(t *testing.T) {
	root := t.TempDir()
	name, err := DeterministicHandoffVolumeDirectory("inventory-owner")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(root, "handoffs", name), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(root, "volumes", "legacy-wrong-root"), 0o700); err != nil {
		t.Fatal(err)
	}
	inventory := ResourceInventory{ManagedVolumes: []string{}, ManagedVolumeRecords: []string{}}
	if err := inventoryManagedVolumeResources(root, &inventory); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(inventory.ManagedVolumes, []string{name}) {
		t.Fatalf("managed volume inventory = %v, want durable handoff %q only", inventory.ManagedVolumes, name)
	}
}

func TestRetainedHandoffProjectionKeepsObservedInventoryAndExpiredResidue(t *testing.T) {
	handoff, err := DeterministicHandoffVolumeDirectory("retained-owner")
	if err != nil {
		t.Fatal(err)
	}
	inventory := ResourceInventory{
		ManagedVolumes:       []string{handoff, "wefty-service-volume-retained", "wefty-service-volume-orphan", "unexpected-volume"},
		ManagedVolumeRecords: []string{"wefty-service-volume-retained.owner", "wefty-service-volume-record-only.owner"},
		Containers:           []string{"unexpected-container"},
	}
	projected, err := projectRuntimeAbsenceInventory(inventory, func(volume, record string) (bool, error) {
		return volume == "wefty-service-volume-retained" && record == "wefty-service-volume-retained.owner", nil
	}, func(name string) (bool, error) {
		return name == handoff, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if slices.Contains(projected.ManagedVolumes, handoff) || slices.Contains(projected.ManagedVolumes, "wefty-service-volume-retained") {
		t.Fatalf("retained binding or service data remained in quiescence projection: %+v", projected)
	}
	if !slices.Equal(projected.ManagedVolumes, []string{"wefty-service-volume-orphan", "unexpected-volume"}) ||
		!slices.Equal(projected.ManagedVolumeRecords, []string{"wefty-service-volume-record-only.owner"}) ||
		!slices.Equal(projected.Containers, []string{"unexpected-container"}) {
		t.Fatalf("quiescence projection hid runtime residue: %+v", projected)
	}
	if !slices.Equal(inventory.ManagedVolumes, []string{handoff, "wefty-service-volume-retained", "wefty-service-volume-orphan", "unexpected-volume"}) {
		t.Fatalf("projection mutated observed inventory: %+v", inventory)
	}
	retained := subtractResourceInventory(inventory, projected)
	if !slices.Equal(retained.ManagedVolumes, []string{handoff, "wefty-service-volume-retained"}) ||
		!slices.Equal(retained.ManagedVolumeRecords, []string{"wefty-service-volume-retained.owner"}) || len(retained.Containers) != 0 {
		t.Fatalf("durable retained inventory = %+v", retained)
	}
	expired, err := projectRuntimeAbsenceInventory(ResourceInventory{ManagedVolumes: []string{handoff}}, func(string, string) (bool, error) {
		return false, nil
	}, func(string) (bool, error) {
		return false, nil
	})
	if err != nil || !slices.Equal(expired.ManagedVolumes, []string{handoff}) {
		t.Fatalf("expired handoff was projected as retained: inventory=%+v err=%v", expired, err)
	}
}

func TestComputerDiskProjectionRetainsOnlyManifestOwnedNonAnomalousLineage(t *testing.T) {
	inventory := ResourceInventory{
		ComputerDiskImages:      []string{"wefty-computer-disk-live", "wefty-computer-disk-orphan", "wefty-computer-disk-anomaly"},
		ComputerDiskAllocations: []string{"wefty-computer-disk-live", "wefty-computer-disk-anomaly"},
		ComputerDiskQuotas:      []string{"wefty-computer-disk-live", "wefty-computer-disk-anomaly"},
		ComputerDiskManifests:   []string{"wefty-computer-disk-live", "wefty-computer-disk-anomaly"},
		ComputerDiskAnomalies:   []string{"wefty-computer-disk-anomaly:allocation_mismatch"},
	}
	projected, err := projectRuntimeAbsenceInventory(inventory, func(string, string) (bool, error) { return false, nil }, func(string) (bool, error) { return false, nil })
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(projected.ComputerDiskImages, []string{"wefty-computer-disk-orphan", "wefty-computer-disk-anomaly"}) ||
		!slices.Equal(projected.ComputerDiskAllocations, []string{"wefty-computer-disk-anomaly"}) ||
		!slices.Equal(projected.ComputerDiskManifests, []string{"wefty-computer-disk-anomaly"}) ||
		!slices.Equal(projected.ComputerDiskAnomalies, inventory.ComputerDiskAnomalies) || InventoryEmpty(projected) {
		t.Fatalf("Computer disk projection hid orphan or anomalous residue: %+v", projected)
	}
	retained := subtractResourceInventory(inventory, projected)
	if !slices.Equal(retained.ComputerDiskImages, []string{"wefty-computer-disk-live"}) ||
		!slices.Equal(retained.ComputerDiskManifests, []string{"wefty-computer-disk-live"}) || len(retained.ComputerDiskAnomalies) != 0 {
		t.Fatalf("Computer disk retained projection = %+v", retained)
	}
}

func TestImageOperationMechanicsNeverClaimsUnknownErrorsAreInvalidManifests(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want ImageFailureKind
	}{
		{name: "unknown", err: errors.New("opaque engine failure"), want: ImageFailureUnavailable},
		{name: "ENOSPC", err: syscall.ENOSPC, want: ImageFailureResourceExhausted},
		{name: "snapshotter", err: errors.New("snapshotter prepare failed"), want: ImageFailureResourceExhausted},
		{name: "platform wrapped not found", err: fmt.Errorf("no match for platform: %w", os.ErrNotExist), want: ImageFailurePlatformMismatch},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var mechanics *ImageMechanicsError
			if !errors.As(classifyImageOperationError(test.err, "sha256:test"), &mechanics) || mechanics.Fact.Kind != test.want {
				t.Fatalf("mechanics classification = %+v", mechanics)
			}
		})
	}
}

func testContentManifest(t *testing.T, ctx context.Context, store content.Store, platform ocispec.Platform) ocispec.Descriptor {
	t.Helper()
	config, err := json.Marshal(ocispec.Image{Platform: platform})
	if err != nil {
		t.Fatal(err)
	}
	configDescriptor := ocispec.Descriptor{MediaType: ocispec.MediaTypeImageConfig, Digest: digest.FromBytes(config), Size: int64(len(config))}
	if err := content.WriteBlob(ctx, store, "test-config-"+configDescriptor.Digest.Encoded(), bytes.NewReader(config), configDescriptor); err != nil {
		t.Fatal(err)
	}
	manifest := []byte(`{"schemaVersion":2,"mediaType":"application/vnd.oci.image.manifest.v1+json","config":{"mediaType":"` + ocispec.MediaTypeImageConfig + `","digest":"` + configDescriptor.Digest.String() + `","size":` + fmt.Sprint(configDescriptor.Size) + `},"layers":[]}`)
	descriptor := ocispec.Descriptor{MediaType: ocispec.MediaTypeImageManifest, Digest: digest.FromBytes(manifest), Size: int64(len(manifest))}
	if err := content.WriteBlob(ctx, store, "test-manifest-"+descriptor.Digest.Encoded(), bytes.NewReader(manifest), descriptor); err != nil {
		t.Fatal(err)
	}
	return descriptor
}

func testContentIndex(t *testing.T, ctx context.Context, store content.Store, manifest ocispec.Descriptor) ocispec.Descriptor {
	t.Helper()
	index, err := json.Marshal(struct {
		SchemaVersion int                  `json:"schemaVersion"`
		MediaType     string               `json:"mediaType"`
		Manifests     []ocispec.Descriptor `json:"manifests"`
	}{SchemaVersion: 2, MediaType: ocispec.MediaTypeImageIndex, Manifests: []ocispec.Descriptor{manifest}})
	if err != nil {
		t.Fatal(err)
	}
	descriptor := ocispec.Descriptor{MediaType: ocispec.MediaTypeImageIndex, Digest: digest.FromBytes(index), Size: int64(len(index))}
	if err := content.WriteBlob(ctx, store, "test-index-"+descriptor.Digest.Encoded(), bytes.NewReader(index), descriptor); err != nil {
		t.Fatal(err)
	}
	return descriptor
}

func descriptorWithPlatform(descriptor ocispec.Descriptor, platform ocispec.Platform) ocispec.Descriptor {
	descriptor.Platform = &platform
	return descriptor
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (function roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return function(request)
}

type publicImagePullFunc func(context.Context, string, ...containerd.RemoteOpt) (containerd.Image, error)

func (function publicImagePullFunc) Pull(ctx context.Context, reference string, options ...containerd.RemoteOpt) (containerd.Image, error) {
	return function(ctx, reference, options...)
}

func TestLimaOperatorMountTranslationStaysWithinConfiguredRoots(t *testing.T) {
	engine := &ContainerdEngine{config: NativeEngineConfig{HostMountRoot: "/Users/operator/wefty", GuestMountRoot: "/mnt/wefty-host"}}
	translated, err := engine.translateOperatorMountSource("/Users/operator/wefty/project/data")
	if err != nil || translated != "/mnt/wefty-host/project/data" {
		t.Fatalf("translated source = %q, %v", translated, err)
	}
	for _, source := range []string{"/Users/operator/wefty", "/Users/operator/other", "/Users/operator/wefty/../escape"} {
		if _, err := engine.translateOperatorMountSource(source); err == nil {
			t.Fatalf("accepted unsafe host mount source %q", source)
		}
	}
}

func TestNewContainerdEngineRejectsGuestMountRootOutsideAllowedRoots(t *testing.T) {
	_, err := NewContainerdEngine(NativeEngineConfig{
		RuntimeRoot: t.TempDir(), HostMountRoot: "/Users/operator/wefty",
		GuestMountRoot: "/mnt/wefty-host", AllowedMountRoots: []string{"/srv/wefty"},
	})
	if err == nil || !strings.Contains(err.Error(), "inside an allowed mount root") {
		t.Fatalf("constructor error = %v", err)
	}
}

func TestDialAttemptPortProxiesOnlyTheRequestedLoopbackPort(t *testing.T) {
	backend, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer backend.Close()
	backendPort := uint16(backend.Addr().(*net.TCPAddr).Port)
	backendDone := make(chan error, 1)
	go func() {
		connection, err := backend.Accept()
		if err != nil {
			backendDone <- err
			return
		}
		defer connection.Close()
		payload := make([]byte, 4)
		if _, err := io.ReadFull(connection, payload); err == nil && string(payload) != "ping" {
			err = io.ErrUnexpectedEOF
		}
		if err == nil {
			_, err = connection.Write([]byte("pong"))
		}
		backendDone <- err
	}()
	client, helper := net.Pipe()
	engineDone := make(chan error, 1)
	go func() {
		engine := &ContainerdEngine{}
		engineDone <- engine.DialAttemptPort(t.Context(), DialAttemptPortRequest{Port: backendPort}, helper)
	}()
	ready := make([]byte, 1)
	if _, err := io.ReadFull(client, ready); err != nil || ready[0] != attemptPortBackendReady {
		t.Fatalf("attempt-port backend readiness = %v, %v", ready, err)
	}
	if _, err := client.Write([]byte("ping")); err != nil {
		t.Fatal(err)
	}
	response := make([]byte, 4)
	if _, err := io.ReadFull(client, response); err != nil || string(response) != "pong" {
		t.Fatalf("attempt-port response = %q, %v", response, err)
	}
	_ = client.Close()
	if err := <-backendDone; err != nil {
		t.Fatal(err)
	}
	if err := <-engineDone; err != nil && err != context.Canceled {
		t.Fatal(err)
	}
}

func TestComputerEndpointEnvironmentOverridesReservedPortsAndOmitsServicePort(t *testing.T) {
	environment, err := computerEndpointEnvironment([]EnvironmentVariable{
		{Name: contract.EnvServiceDir, Value: contract.OCIContainerServiceDirectory},
		{Name: contract.EnvServicePort, Value: "attacker-service"},
		{Name: contract.EnvComputerViewPort, Value: "attacker-view"},
		{Name: contract.EnvComputerControlPort, Value: "attacker-control"},
	}, map[string]uint16{"view": 42111, "control": 42112})
	if err != nil {
		t.Fatal(err)
	}
	values := make(map[string]string, len(environment))
	for _, variable := range environment {
		values[variable.Name] = variable.Value
	}
	if values[contract.EnvComputerViewPort] != "42111" || values[contract.EnvComputerControlPort] != "42112" ||
		values[contract.EnvServiceDir] != contract.OCIContainerServiceDirectory {
		t.Fatalf("Computer reserved environment = %+v", values)
	}
	if _, present := values[contract.EnvServicePort]; present {
		t.Fatalf("Computer retained WEFTY_SERVICE_PORT: %+v", values)
	}
	if _, err := computerEndpointEnvironment(nil, map[string]uint16{"view": 42111, "control": 42111}); err == nil {
		t.Fatal("Computer accepted duplicate endpoint ports")
	}
}

func TestDialAttemptPortAllowsClientHalfCloseBeforeFullResponse(t *testing.T) {
	backend, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer backend.Close()
	backendPort := uint16(backend.Addr().(*net.TCPAddr).Port)
	requestEOF := make(chan struct{})
	releaseResponse := make(chan struct{})
	go func() {
		connection, acceptErr := backend.Accept()
		if acceptErr != nil {
			return
		}
		defer connection.Close()
		_, _ = io.ReadAll(connection)
		close(requestEOF)
		<-releaseResponse
		_, _ = io.WriteString(connection, "full-response-after-request-eof")
	}()

	client, helper := tcpConnectionPair(t)
	defer client.Close()
	defer helper.Close()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	engineDone := make(chan error, 1)
	go func() {
		engine := &ContainerdEngine{}
		engineDone <- engine.DialAttemptPort(ctx, DialAttemptPortRequest{Port: backendPort}, &operationStream{Conn: helper, cancel: cancel})
	}()
	var marker [1]byte
	if _, err := io.ReadFull(client, marker[:]); err != nil || marker[0] != attemptPortBackendReady {
		t.Fatalf("backend marker = %v, %v", marker, err)
	}
	if _, err := io.WriteString(client, "request"); err != nil {
		t.Fatal(err)
	}
	if err := client.CloseWrite(); err != nil {
		t.Fatal(err)
	}
	<-requestEOF
	select {
	case <-ctx.Done():
		t.Fatal("request EOF cancelled the helper tunnel before the response")
	default:
	}
	close(releaseResponse)
	response, err := io.ReadAll(client)
	if err != nil || string(response) != "full-response-after-request-eof" {
		t.Fatalf("response = %q, %v", response, err)
	}
	if err := <-engineDone; err != nil {
		t.Fatal(err)
	}
}

func tcpConnectionPair(t *testing.T) (*net.TCPConn, *net.TCPConn) {
	t.Helper()
	listener, err := net.ListenTCP("tcp4", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	accepted := make(chan *net.TCPConn, 1)
	go func() {
		connection, _ := listener.AcceptTCP()
		accepted <- connection
	}()
	client, err := net.DialTCP("tcp4", nil, listener.Addr().(*net.TCPAddr))
	if err != nil {
		t.Fatal(err)
	}
	return client, <-accepted
}

func TestAttemptPortAllocationRemainsAttemptScopedUntilRelease(t *testing.T) {
	probe, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := uint16(probe.Addr().(*net.TCPAddr).Port)
	probe.Close()
	engine := &ContainerdEngine{
		config: NativeEngineConfig{AttemptPortMin: port, AttemptPortMax: port},
		ports:  make(map[uint16]string), nextPort: port,
	}
	allocated, hold, err := engine.reserveAttemptPort("attempt-a")
	if err != nil || allocated != port {
		t.Fatalf("first allocation = %d, %v", allocated, err)
	}
	if _, _, err := engine.reserveAttemptPort("attempt-b"); err == nil {
		t.Fatal("allocated one reserved port to two attempts")
	}
	engine.releaseAttemptPort(port, "attempt-b")
	if _, _, err := engine.reserveAttemptPort("attempt-b"); err == nil {
		t.Fatal("wrong attempt released another attempt's port")
	}
	_ = hold.Close()
	engine.releaseAttemptPort(port, "attempt-a")
	if allocated, hold, err := engine.reserveAttemptPort("attempt-b"); err != nil || allocated != port {
		t.Fatalf("allocation after exact release = %d, %v", allocated, err)
	} else {
		_ = hold.Close()
	}
}

func TestTwoComputersReceiveFourUniqueLoopbackPorts(t *testing.T) {
	engine := &ContainerdEngine{
		config: NativeEngineConfig{AttemptPortMin: 25000, AttemptPortMax: 40000},
		ports:  make(map[uint16]string), nextPort: 25000,
	}
	type allocation struct {
		authority string
		port      uint16
		hold      net.Listener
	}
	allocations := make([]allocation, 0, 4)
	for _, authority := range []string{"computer-a", "computer-b"} {
		for range []string{"view", "control"} {
			port, hold, err := engine.reserveAttemptPort(authority)
			if err != nil {
				t.Fatal(err)
			}
			allocations = append(allocations, allocation{authority: authority, port: port, hold: hold})
		}
	}
	defer func() {
		for _, allocation := range allocations {
			_ = allocation.hold.Close()
			engine.releaseAttemptPort(allocation.port, allocation.authority)
		}
	}()
	ports := make(map[uint16]struct{}, len(allocations))
	for _, allocation := range allocations {
		host, port, err := net.SplitHostPort(allocation.hold.Addr().String())
		if err != nil || host != "127.0.0.1" || port != fmt.Sprint(allocation.port) {
			t.Fatalf("Computer allocation %q = %q, port=%d err=%v", allocation.authority, allocation.hold.Addr(), allocation.port, err)
		}
		if _, duplicate := ports[allocation.port]; duplicate {
			t.Fatalf("Computer endpoint port %d was allocated twice", allocation.port)
		}
		ports[allocation.port] = struct{}{}
	}
}

func TestAttemptEndpointOwnershipRejectsWildcardListener(t *testing.T) {
	wildcard, err := net.Listen("tcp4", "0.0.0.0:0")
	if err != nil {
		t.Fatal(err)
	}
	defer wildcard.Close()
	port := uint16(wildcard.Addr().(*net.TCPAddr).Port)
	if inode, found, err := loopbackListenInode(nil, port); err != nil || found || inode != "" {
		t.Fatalf("wildcard listener was accepted as loopback ownership: inode=%q found=%t err=%v", inode, found, err)
	}
}

func TestResidualVerificationRetainsPortAgainstConcurrentAllocation(t *testing.T) {
	probe, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := uint16(probe.Addr().(*net.TCPAddr).Port)
	_ = probe.Close()
	engine := &ContainerdEngine{config: NativeEngineConfig{AttemptPortMin: port, AttemptPortMax: port}, attempts: make(map[string]*containerdAttempt), ports: make(map[uint16]string), nextPort: port}
	allocated, hold, err := engine.reserveAttemptPort("attempt-a")
	if err != nil {
		t.Fatal(err)
	}
	engine.attempts["attempt-a"] = &containerdAttempt{
		endpoints:     map[string]uint16{"service": allocated},
		endpointHolds: map[string]net.Listener{"service": hold},
	}
	// Forced residual verification means releaseVerifiedAttempt is deliberately
	// not called. Both logical and kernel ownership must remain unavailable.
	if _, _, err := engine.reserveAttemptPort("attempt-b"); err == nil {
		t.Fatal("residual verification recycled a live attempt port")
	}
	if err := engine.releaseVerifiedAttempt(t.Context(), "attempt-a"); err != nil {
		t.Fatal(err)
	}
	if _, nextHold, err := engine.reserveAttemptPort("attempt-b"); err != nil {
		t.Fatalf("verified absence did not release port: %v", err)
	} else {
		_ = nextHold.Close()
	}
}

func TestVerifiedAttemptPropagatesPinDeletionAndRetainsRetryState(t *testing.T) {
	key := testImageOperationKey("sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	manager := &failingImageLeaseDeletionManager{err: errors.New("containerd lease delete failed")}
	engine := &ContainerdEngine{
		imageLeaseDeletes: manager,
		attemptImagePins:  map[string]imageOperationKey{"attempt-a": key},
		attempts:          map[string]*containerdAttempt{"attempt-a": {}},
		ports:             map[uint16]string{},
	}
	err := engine.releaseVerifiedAttempt(t.Context(), "attempt-a")
	if err == nil || !strings.Contains(err.Error(), "containerd lease delete failed") {
		t.Fatalf("release error = %v", err)
	}
	if _, retained := engine.attemptImagePins["attempt-a"]; !retained {
		t.Fatal("failed lease deletion discarded retryable attempt pin state")
	}
	if _, retained := engine.attempts["attempt-a"]; retained {
		t.Fatal("verified runtime state was retained with independent image-pin failure")
	}
	if !manager.sawDeadline {
		t.Fatal("attempt image-pin deletion was not bounded")
	}
}

func TestStructLiteralEngineCloseIsNilChannelSafeAndIdempotent(t *testing.T) {
	engine := &ContainerdEngine{imageOperations: newImageOperationGroup()}
	if err := engine.Close(); err != nil {
		t.Fatal(err)
	}
	if err := engine.Close(); err != nil {
		t.Fatal(err)
	}
}

type failingImageLeaseDeletionManager struct {
	err         error
	sawDeadline bool
}

func (manager *failingImageLeaseDeletionManager) Delete(ctx context.Context, _ leases.Lease, _ ...leases.DeleteOpt) error {
	_, manager.sawDeadline = ctx.Deadline()
	return manager.err
}

func TestAttemptPortRejectsAdversarialBindOutsidePayloadCgroup(t *testing.T) {
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	port := uint16(listener.Addr().(*net.TCPAddr).Port)
	cgroupRoot := t.TempDir()
	cgroupID := "attempt-cgroup"
	if err := os.Mkdir(filepath.Join(cgroupRoot, cgroupID), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(cgroupRoot, cgroupID, "cgroup.procs"), []byte("99999999\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	engine := &ContainerdEngine{config: NativeEngineConfig{CgroupRoot: cgroupRoot}}
	err = engine.waitAttemptPortOwnership(t.Context(), cgroupID, nil, port)
	if err == nil || !strings.Contains(err.Error(), "outside the attempt cgroup") {
		t.Fatalf("adversarial bind error = %v", err)
	}
}

func TestAttemptPortAcceptsListenerInNestedPayloadCgroup(t *testing.T) {
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	port := uint16(listener.Addr().(*net.TCPAddr).Port)
	cgroupRoot := t.TempDir()
	cgroupID := "attempt-cgroup"
	cgroupPath := filepath.Join(cgroupRoot, cgroupID)
	nestedPath := filepath.Join(cgroupPath, "desktop.slice", "session.scope")
	if err := os.MkdirAll(nestedPath, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(cgroupPath, "cgroup.procs"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(cgroupPath, "desktop.slice", "cgroup.procs"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(nestedPath, "cgroup.procs"), []byte(strconv.Itoa(os.Getpid())+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	engine := &ContainerdEngine{config: NativeEngineConfig{CgroupRoot: cgroupRoot}}
	if err := engine.waitAttemptPortOwnership(t.Context(), cgroupID, nil, port); err != nil {
		t.Fatalf("nested payload listener ownership: %v", err)
	}
}

func TestCgroupSocketWalkIgnoresVanishedScopesAndHonorsBound(t *testing.T) {
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "desktop.slice", "vanished.scope"), 0o700); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{filepath.Join(root, "cgroup.procs"), filepath.Join(root, "desktop.slice", "cgroup.procs")} {
		if err := os.WriteFile(path, nil, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	// WalkDir observed this descendant, but its cgroup.procs disappeared before
	// ownership proof. That session churn is "not here", not a proof failure.
	owned, err := cgroupSubtreeOwnsSocket(t.Context(), root, "unused")
	if err != nil || owned {
		t.Fatalf("vanished cgroup scope ownership = %t, %v", owned, err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := cgroupSubtreeOwnsSocket(ctx, root, "unused"); !errors.Is(err, context.Canceled) {
		t.Fatalf("bounded cgroup walk error = %v, want context cancellation", err)
	}
}

func TestIncompleteLoggerEvidencePublishesDiscardedByteGap(t *testing.T) {
	path := filepath.Join(t.TempDir(), "stdout.frames")
	file, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	payload, err := json.Marshal(loggerIncompleteEvidence{Reason: "termination deadline", LostByteCount: 17})
	if err != nil {
		t.Fatal(err)
	}
	if err := writeLogRecord(file, logIncompleteMagic, 0, payload); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	terminal := make(chan struct{})
	close(terminal)
	events := make(chan logTailEvent, 2)
	tailLogSegment(t.Context(), "stdout", path, terminal, time.Second, 0, events)
	gap := <-events
	seal := <-events
	if gap.event.Log == nil || gap.event.Log.Gap == nil || gap.event.Log.Gap.ThroughSequence != 0 || gap.event.Log.Gap.LostByteCount != 17 || gap.event.Log.Gap.Reason != "logger_source_incomplete" {
		t.Fatalf("discarded logger bytes gap = %+v", gap.event.Log)
	}
	if seal.event.Seal == nil || seal.event.Seal.Complete || seal.event.Seal.Reason != "termination deadline" {
		t.Fatalf("incomplete logger seal = %+v", seal.event.Seal)
	}
}

func TestPreparedMacFallbackRewritesEndpointOnlyWhenActivated(t *testing.T) {
	original := []EnvironmentVariable{{Name: contract.EnvL3Endpoint, Value: "http://host.lima.internal:4242/l3"}}
	address := &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 4343}
	dormant, guestEndpoint, err := fallbackBridgeEnvironment(original, address, false)
	if err != nil {
		t.Fatal(err)
	}
	if dormant[0].Value != original[0].Value || guestEndpoint != "http://127.0.0.1:4343/l3" {
		t.Fatalf("dormant fallback environment=%v guest=%q", dormant, guestEndpoint)
	}
	active, guestEndpoint, err := fallbackBridgeEnvironment(original, address, true)
	if err != nil {
		t.Fatal(err)
	}
	if active[0].Value != guestEndpoint || guestEndpoint != "http://127.0.0.1:4343/l3" {
		t.Fatalf("active fallback environment=%v guest=%q", active, guestEndpoint)
	}
	defaultOff, guestEndpoint, err := fallbackBridgeEnvironment(nil, address, false)
	if err != nil || len(defaultOff) != 0 || guestEndpoint == "" {
		t.Fatalf("default-off dormant fallback environment=%v guest=%q err=%v", defaultOff, guestEndpoint, err)
	}
}

func TestDialHostBridgePairsOnlyTheAttemptsGuestListener(t *testing.T) {
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	authority := AttemptAuthority{NodeID: "node", JobID: "job", AttemptID: "attempt", FencingToken: "fence", BootSessionID: "boot", Class: "one-shot", RemovalGeneration: "attempt"}
	engine := &ContainerdEngine{attempts: map[string]*containerdAttempt{authority.key(): {authority: authority, hostBridge: listener}}}
	host, helper := net.Pipe()
	engineDone := make(chan error, 1)
	go func() {
		engineDone <- engine.DialHostBridge(t.Context(), DialHostBridgeRequest{Authority: authority}, helper)
	}()
	guest, err := net.Dial("tcp4", listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := guest.Write([]byte("guest")); err != nil {
		t.Fatal(err)
	}
	marker := []byte{0}
	if _, err := io.ReadFull(host, marker); err != nil || marker[0] != hostBridgeBackendReady {
		t.Fatalf("host bridge ready marker = %d, %v", marker[0], err)
	}
	payload := make([]byte, 5)
	if _, err := io.ReadFull(host, payload); err != nil || string(payload) != "guest" {
		t.Fatalf("host payload = %q, %v", payload, err)
	}
	if _, err := host.Write([]byte("host")); err != nil {
		t.Fatal(err)
	}
	payload = make([]byte, 4)
	if _, err := io.ReadFull(guest, payload); err != nil || string(payload) != "host" {
		t.Fatalf("guest payload = %q, %v", payload, err)
	}
	_ = guest.Close()
	_ = host.Close()
	if err := <-engineDone; err != nil && err != context.Canceled {
		t.Fatal(err)
	}
}

func TestDialHostBridgeCancellationDrainsFourAcceptsAfterOneGuest(t *testing.T) {
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	tcpListener := listener.(*net.TCPListener)
	defer tcpListener.Close()
	listenerAddress := tcpListener.Addr().String()
	authority := AttemptAuthority{NodeID: "node", JobID: "job", AttemptID: "attempt", FencingToken: "fence", BootSessionID: "boot", Class: "one-shot", RemovalGeneration: "attempt"}
	engine := &ContainerdEngine{attempts: map[string]*containerdAttempt{authority.key(): {authority: authority, hostBridge: tcpListener}}}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	const bridgeConcurrency = 4
	hosts := make([]net.Conn, 0, bridgeConcurrency)
	results := make(chan error, bridgeConcurrency)
	for range bridgeConcurrency {
		host, helper := net.Pipe()
		hosts = append(hosts, host)
		go func() {
			results <- engine.DialHostBridge(ctx, DialHostBridgeRequest{Authority: authority}, helper)
		}()
	}
	defer func() {
		for _, host := range hosts {
			_ = host.Close()
		}
	}()
	waitForHostBridgePumps(t, bridgeConcurrency, time.Second)

	guest, err := net.Dial("tcp4", tcpListener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer guest.Close()
	ready := make(chan error, bridgeConcurrency)
	for _, host := range hosts {
		if err := host.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
			t.Fatal(err)
		}
		go func(connection net.Conn) {
			var marker [1]byte
			_, err := io.ReadFull(connection, marker[:])
			if err == nil && marker[0] != hostBridgeBackendReady {
				err = fmt.Errorf("host-bridge marker = %d", marker[0])
			}
			ready <- err
		}(host)
	}
	select {
	case err := <-ready:
		if err != nil {
			t.Fatalf("one guest did not release a host-bridge pump: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("one guest did not release a host-bridge pump")
	}

	cancel()
	finished := 0
	timer := time.NewTimer(time.Second)
	defer timer.Stop()
	for finished < bridgeConcurrency {
		select {
		case <-results:
			finished++
		case <-timer.C:
			_ = tcpListener.Close()
			t.Fatalf("host-bridge cancellation returned %d/%d pumps before the listener was reaped", finished, bridgeConcurrency)
		}
	}
	if err := tcpListener.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
		t.Fatal(err)
	}
	if connection, err := net.DialTimeout("tcp4", listenerAddress, time.Second); err == nil {
		_ = connection.Close()
		t.Fatal("host-bridge listener accepted a connection after reap close")
	}
}

func TestSessionInvalidationDrainsFourHostBridgeOperationsBeforeReap(t *testing.T) {
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	tcpListener := listener.(*net.TCPListener)
	defer tcpListener.Close()
	listenerAddress := tcpListener.Addr().String()
	authority := testAuthority()
	terminalReady := make(chan struct{})
	close(terminalReady)

	engine, err := NewContainerdEngine(NativeEngineConfig{
		Address:             filepath.Join(t.TempDir(), "missing-containerd.sock"),
		RuntimeRoot:         t.TempDir(),
		ContainerdStateRoot: t.TempDir(),
		CgroupRoot:          t.TempDir(),
		IPExecutable:        "/bin/true",
		IPTablesExecutable:  "/bin/true",
		IP6TablesExecutable: "/bin/true",
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = engine.Close() })
	engine.attempts[authority.key()] = &containerdAttempt{
		authority: authority, task: &hostBridgeReapTask{}, terminalReady: terminalReady,
		hostBridge: tcpListener, endpointHolds: make(map[string]net.Listener),
	}
	reapingEngine := &observedHostBridgeReapEngine{ContainerdEngine: engine, started: make(chan struct{})}
	server, err := NewServer(reapingEngine, ServerConfig{AllowedUIDs: []uint32{uint32(os.Getuid())}, ReapTimeout: 2 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	server.serveCtx = context.Background()
	helperControl, agentControl := net.Pipe()
	defer helperControl.Close()
	defer agentControl.Close()
	bridgeCapability := "bridge-capability"
	serverSession := &serverSession{
		server: server, identity: SessionIdentity{NodeID: authority.NodeID, BootSessionID: authority.BootSessionID},
		helper: HelperSession{HelperInstanceID: "helper-test", SessionGeneration: 1}, capability: "session-capability", control: helperControl,
		heartbeatChanged: make(chan struct{}, 1), done: make(chan struct{}), operations: make(map[*sessionOperation]struct{}), sweepVerified: true,
		attempts: map[string]*serverAttempt{authority.key(): {
			authority: authority, state: attemptLive, bridgeCapability: bridgeCapability,
			watchDone: make(chan struct{}), reaped: make(chan struct{}),
		}},
	}
	server.active = serverSession

	serverResults := make(chan error, 4)
	client := &Client{Version: ProtocolVersion, ExpectedChecksum: "checksum-test"}
	client.Dial = func(ctx context.Context) (net.Conn, error) {
		agent, helper := net.Pipe()
		go func() {
			defer helper.Close()
			wire := newFramedConn(helper)
			var request frame
			if err := wire.read(&request); err != nil {
				serverResults <- err
				return
			}
			operation, rpcErr := serverSession.beginOperation(ctx, helper)
			if rpcErr != nil {
				serverResults <- writeRPCError(wire, rpcErr)
				return
			}
			defer operation.finish()
			server.dispatch(operation, wire, request)
			serverResults <- nil
		}()
		return agent, nil
	}
	clientSession := &Session{client: client, capability: serverSession.capability, control: agentControl}

	const bridgeConcurrency = 4
	type dialResult struct {
		stream net.Conn
		err    error
	}
	dialResults := make(chan dialResult, bridgeConcurrency)
	for range bridgeConcurrency {
		go func() {
			stream, err := clientSession.DialHostBridge(t.Context(), DialHostBridgeRequest{Authority: authority, BridgeCapability: bridgeCapability})
			dialResults <- dialResult{stream: stream, err: err}
		}()
	}
	waitForHostBridgePumps(t, bridgeConcurrency, time.Second)

	guest, err := net.Dial("tcp4", listenerAddress)
	if err != nil {
		t.Fatal(err)
	}
	defer guest.Close()
	var attached net.Conn
	select {
	case result := <-dialResults:
		if result.err != nil {
			t.Fatalf("one guest did not release a host-bridge operation: %v", result.err)
		}
		attached = result.stream
		defer attached.Close()
	case <-time.After(time.Second):
		t.Fatal("one guest did not release a host-bridge operation")
	}

	// With one serialized Accept in flight, cancellation should need no more
	// than one 250 ms poll. A second poll is the explicit scheduler margin and
	// catches canceled waiters that enter another round after acquiring the lock.
	drainBound := 2 * hostBridgeAcceptPollInterval
	invalidated := make(chan struct{})
	started := time.Now()
	go func() {
		serverSession.invalidate("host-bridge reap regression")
		close(invalidated)
	}()

	clientReturned := 1
	serverReturned := 0
	reapStarted := false
	reapStartedCh := reapingEngine.started
	listenerClosed := false
	timer := time.NewTimer(drainBound)
	defer timer.Stop()
	for clientReturned < bridgeConcurrency || serverReturned < bridgeConcurrency || !reapStarted || !listenerClosed {
		select {
		case result := <-dialResults:
			clientReturned++
			if result.stream != nil {
				_ = result.stream.Close()
			}
			if result.err == nil {
				t.Fatal("more than one host-bridge operation attached to the sole guest")
			}
		case err := <-serverResults:
			serverReturned++
			if err != nil {
				t.Fatalf("host-bridge server operation failed before invalidation: %v", err)
			}
		case <-reapStartedCh:
			reapStarted = true
			reapStartedCh = nil
		default:
			if reapStarted {
				connection, err := net.DialTimeout("tcp4", listenerAddress, 10*time.Millisecond)
				if err != nil {
					listenerClosed = true
				} else {
					_ = connection.Close()
				}
			}
			runtime.Gosched()
		}
		select {
		case <-timer.C:
			_ = tcpListener.Close()
			t.Fatalf("session invalidation returned client=%d/%d server=%d/%d reap_started=%t listener_closed=%t within %s", clientReturned, bridgeConcurrency, serverReturned, bridgeConcurrency, reapStarted, listenerClosed, drainBound)
		default:
		}
	}
	t.Logf("session invalidation joined four host-bridge operations and reached listener close in %s (bound %s)", time.Since(started), drainBound)

	select {
	case <-invalidated:
	case <-time.After(3 * time.Second):
		t.Fatal("session invalidation did not return after the bounded reap context")
	}
}

type hostBridgeReapTask struct{ containerd.Task }

func (*hostBridgeReapTask) Kill(context.Context, syscall.Signal, ...containerd.KillOpts) error {
	return errdefs.ErrNotFound
}

type observedHostBridgeReapEngine struct {
	*ContainerdEngine
	started chan struct{}
}

func (engine *observedHostBridgeReapEngine) ReapSession(ctx context.Context, identity SessionIdentity) (SweepResponse, error) {
	close(engine.started)
	return engine.ContainerdEngine.ReapSession(ctx, identity)
}

func TestDialHostBridgeMarkerFailureClosesAcceptedGuest(t *testing.T) {
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	tcpListener := listener.(*net.TCPListener)
	defer tcpListener.Close()
	authority := AttemptAuthority{NodeID: "node", JobID: "job", AttemptID: "attempt", FencingToken: "fence", BootSessionID: "boot", Class: "one-shot", RemovalGeneration: "attempt"}
	engine := &ContainerdEngine{attempts: map[string]*containerdAttempt{authority.key(): {authority: authority, hostBridge: tcpListener}}}
	host, helper := net.Pipe()
	_ = host.Close()
	engineDone := make(chan error, 1)
	go func() {
		engineDone <- engine.DialHostBridge(t.Context(), DialHostBridgeRequest{Authority: authority}, helper)
	}()
	guest, err := net.Dial("tcp4", tcpListener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer guest.Close()
	select {
	case err := <-engineDone:
		if err == nil {
			t.Fatal("closed helper stream accepted a host-bridge marker")
		}
	case <-time.After(time.Second):
		t.Fatal("host-bridge marker failure did not return")
	}
	if err := guest.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	var value [1]byte
	if _, err := guest.Read(value[:]); err == nil {
		t.Fatal("accepted guest remained open after marker failure")
	} else if timeout, ok := err.(net.Error); ok && timeout.Timeout() {
		t.Fatal("accepted guest leaked after marker failure")
	}
}

// These exact stack symbols intentionally couple the checkpoint to the real
// host-bridge accept path so either production rename fails loudly here.
var hostBridgePumpStackSymbols = struct {
	dial   []byte
	accept []byte
}{
	dial:   []byte("(*ContainerdEngine).DialHostBridge"),
	accept: []byte("(*TCPListener).Accept"),
}

func waitForHostBridgePumps(t *testing.T, count int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		stacks := make([]byte, 1<<20)
		stackCount := runtime.Stack(stacks, true)
		pumps := 0
		accepts := 0
		for _, stack := range bytes.Split(stacks[:stackCount], []byte("\n\n")) {
			if bytes.Contains(stack, hostBridgePumpStackSymbols.dial) {
				pumps++
			}
			if bytes.Contains(stack, hostBridgePumpStackSymbols.dial) && bytes.Contains(stack, hostBridgePumpStackSymbols.accept) {
				accepts++
			}
		}
		if pumps >= count && accepts >= 1 {
			return
		}
		if !time.Now().Before(deadline) {
			t.Fatalf("observed %d/%d host-bridge pumps with %d blocked in Accept", pumps, count, accepts)
		}
		runtime.Gosched()
	}
}
