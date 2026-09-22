//go:build darwin || linux

package agent

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	processrunner "github.com/Derek-X-Wang/wefty/runner/process"
)

// TestTheNodesHandoffRootIsAuthoritative states the rule the whole defect came
// down to: the ledger's path is a default, the node's --handoff-root is the
// answer, and a path under neither is refused rather than accepted and lost.
func TestTheNodesHandoffRootIsAuthoritative(t *testing.T) {
	nodeRoot := filepath.Join(t.TempDir(), "node-handoffs")
	ledgerRoot := filepath.Join(t.TempDir(), "ledger-handoffs")
	manager := newHandoffManager(nodeRoot, t.TempDir(), "node-1", time.Hour, nil)
	manager.ledgerRoot = ledgerRoot
	// Another name for this node's root, to prove the rule is about the path
	// that was dispatched and not about where it might eventually lead. The
	// target is deliberately never created: resolution reads no filesystem.
	alias := filepath.Join(t.TempDir(), "alias")
	if err := os.Symlink(nodeRoot, alias); err != nil {
		t.Fatal(err)
	}
	// Every path below is spelled exactly as it would arrive, because
	// filepath.Join in a test would clean a traversal away before the resolver
	// ever saw it -- and cleaning it is the resolver's job to be judged on.

	for _, testCase := range []struct {
		name       string
		runID      string
		dispatched string
		want       string
	}{
		{
			name:       "a path under the ledger's default root is adopted under this node's",
			runID:      "run_mapped",
			dispatched: filepath.Join(ledgerRoot, "run_mapped"),
			want:       filepath.Join(nodeRoot, "run_mapped"),
		},
		{
			name:       "a path already under this node's root is left alone",
			runID:      "run_native",
			dispatched: filepath.Join(nodeRoot, "run_native"),
			want:       filepath.Join(nodeRoot, "run_native"),
		},
		{
			name:       "an untidy spelling of the same directory is the same directory",
			runID:      "run_untidy",
			dispatched: ledgerRoot + "/./run_untidy",
			want:       filepath.Join(nodeRoot, "run_untidy"),
		},
		{
			name:       "a trailing separator does not make it another directory",
			runID:      "run_trailing",
			dispatched: ledgerRoot + "/run_trailing/",
			want:       filepath.Join(nodeRoot, "run_trailing"),
		},
		{
			name:       "traversal out of a known root is refused, not cleaned into one",
			runID:      "run_escape",
			dispatched: ledgerRoot + "/../run_escape",
		},
		{
			name:       "traversal back onto the root itself is refused",
			runID:      "run_dotdot",
			dispatched: ledgerRoot + "/run_dotdot/..",
		},
		{
			name:       "the ledger's root is not a run's directory",
			runID:      "run_at_root",
			dispatched: ledgerRoot,
		},
		{
			name:       "this node's root is not a run's directory either",
			runID:      "run_at_node_root",
			dispatched: nodeRoot,
		},
		{
			name:       "a symlink pointing at this node's root is not this node's root",
			runID:      "run_aliased",
			dispatched: alias + "/run_aliased",
		},
		{
			name:       "a path under neither root is refused",
			runID:      "run_foreign",
			dispatched: filepath.Join(t.TempDir(), "somewhere-else", "run_foreign"),
		},
		{
			name:       "a leaf that is not this run's is refused even under a known root",
			runID:      "run_leaf",
			dispatched: filepath.Join(ledgerRoot, "run_someone_else"),
		},
		{
			name:       "a relative path is refused",
			runID:      "run_relative",
			dispatched: "handoffs/run_relative",
		},
		{
			name:       "a run that is not one path component is refused",
			runID:      "../escape",
			dispatched: filepath.Join(ledgerRoot, "escape"),
		},
		{
			// Such a job never reaches this function in the lifecycle -- it has
			// no results to place -- and the rule is stated here anyway,
			// because a run-keyed root with no run is not a directory anything
			// could resolve.
			name:       "a job that names no handoff owner is refused",
			dispatched: filepath.Join(ledgerRoot, "run_anonymous"),
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			spec := handoffClaim(testCase.runID, testCase.dispatched, nil).Job.Spec
			if testCase.runID == "" {
				delete(spec.Labels, "run_id")
			}
			got, err := manager.resolveHandoffDirectory(spec)
			if testCase.want == "" {
				if !errors.Is(err, errUnmanagedHandoffDirectory) {
					t.Fatalf("resolveHandoffDirectory(%q) = (%q, %v), want the typed refusal",
						testCase.dispatched, got, err)
				}
				return
			}
			if err != nil || got != testCase.want {
				t.Fatalf("resolveHandoffDirectory(%q) = (%q, %v), want %q",
					testCase.dispatched, got, err, testCase.want)
			}
		})
	}
	// Resolution decides; it never creates. Neither root exists yet, and an
	// adopted path that was only resolved has nothing on disk behind it.
	for _, root := range []string{nodeRoot, ledgerRoot} {
		if _, err := os.Lstat(root); !os.IsNotExist(err) {
			t.Fatalf("resolving handoff paths created %q: %v", root, err)
		}
	}
}

// TestTheLedgersDefaultRootIsTheContractsOwn keeps the seam above honest: the
// tests point ledgerRoot at a temp directory so they never write into the real
// default, so something has to assert what a real agent is built with.
func TestTheLedgersDefaultRootIsTheContractsOwn(t *testing.T) {
	manager := newHandoffManager(t.TempDir(), t.TempDir(), "node-1", time.Hour, nil)
	if manager.ledgerRoot != filepath.Clean(contract.DefaultHandoffRoot) {
		t.Fatalf("ledger root = %q, want %q", manager.ledgerRoot, contract.DefaultHandoffRoot)
	}
}

// TestPreparationNeverAcceptsADirectoryWithoutOwningIt is the defect's shape in
// one assertion. Preparation used to answer a path this node did not manage
// with no ownership and no error, which is what made every later step -- the
// retention record, the result upload, `wefty results` -- quietly do nothing.
func TestPreparationNeverAcceptsADirectoryWithoutOwningIt(t *testing.T) {
	nodeRoot := filepath.Join(t.TempDir(), "node-handoffs")
	manager := newHandoffManager(nodeRoot, t.TempDir(), "node-1", time.Hour, nil)
	manager.ledgerRoot = filepath.Join(t.TempDir(), "ledger-handoffs")
	foreign := filepath.Join(t.TempDir(), "not-ours", "run_foreign")
	spec := handoffClaim("run_foreign", foreign, nil).Job.Spec

	lease, err := manager.lock(t.Context(), spec)
	if err != nil {
		t.Fatal(err)
	}
	defer lease.release()
	owner, err := manager.prepare(lease, spec, "node-1")
	if owner != nil || !errors.Is(err, errUnmanagedHandoffDirectory) {
		t.Fatalf("prepare() = (%#v, %v), want no ownership and the typed refusal", owner, err)
	}
	// The refusal is also a refusal to touch the directory: creating it would
	// leave a run's files somewhere nothing sweeps.
	if _, err := os.Stat(foreign); !os.IsNotExist(err) {
		t.Fatalf("a refused handoff directory was created anyway: %v", err)
	}
}

// TestACustomHandoffRootStillCarriesTheResult is the #479 defect end to end: a
// node started with --handoff-root elsewhere answered every `wefty results`
// with an absence while `inspect` advertised retention, because the run wrote
// into the ledger's path and the node managed its own.
//
// The workload writes the directory it was actually given into its own result,
// so one assertion covers both halves: what reached the ledger, and where the
// run was told to put it.
func TestACustomHandoffRootStillCarriesTheResult(t *testing.T) {
	harness := newRetentionHarness(t, time.Hour)
	// The ledger's default root is a wire value, not a place on this machine.
	// Pointing it at a directory of its own keeps this test from writing into
	// the real /tmp/wefty/handoffs, and lets it assert that nothing does.
	ledgerRoot := filepath.Join(t.TempDir(), "ledger-handoffs")
	harness.manager.ledgerRoot = ledgerRoot
	runID := "run_custom_root"
	dispatched := filepath.Join(ledgerRoot, runID)
	managed := filepath.Join(harness.root, runID)

	script := []byte("#!/bin/sh\nprintf '{\"handoff_dir\":\"%s\"}' \"$WEFTY_HANDOFF_DIR\" > \"$WEFTY_HANDOFF_DIR/result.json\"\n")
	digest := sha256.Sum256(script)
	recorder := &resultUploadRecorder{}
	lifecycle := uploadingLifecycle(t, harness.manager, recorder,
		func(ctx context.Context, request processrunner.Request, sink processrunner.OutputSink) (contract.ProcessResult, error) {
			return processrunner.New(processrunner.Config{}).Run(ctx, request, sink)
		})
	claim := retentionClaim(t, runID, dispatched)
	claim.Job.Spec.Execution.Executable = contract.ExecutableSpec{
		InlineBase64: base64.StdEncoding.EncodeToString(script),
		SHA256:       hex.EncodeToString(digest[:]),
		Interpreter:  []string{"/bin/sh"}, Mode: 0o700,
	}
	claim.Job.Spec.Execution.Argv = []string{"wefty-inline-" + runID}
	claim.Job.Spec.Execution.Env = map[string]string{contract.EnvHandoffDir: dispatched}

	if _, err := lifecycle.execute(t.Context(), claim, time.Now()); err != nil {
		t.Fatal(err)
	}

	requests, _ := recorder.observed()
	if len(requests) != 1 {
		t.Fatalf("uploaded %d results, want exactly one", len(requests))
	}
	want := `{"handoff_dir":"` + managed + `"}`
	if string(requests[0].Document) != want || requests[0].SkipReason != "" {
		t.Fatalf("uploaded document %q (skip %q), want %q with no skip",
			requests[0].Document, requests[0].SkipReason, want)
	}
	// The node keeps its own copy under the root it was configured with, and
	// the record that decides its retention names that same directory.
	if _, err := os.Stat(filepath.Join(managed, handoffResultName)); err != nil {
		t.Fatalf("the node kept no copy under its own handoff root: %v", err)
	}
	record := requireRetentionRecord(t, harness.manager, runID)
	if record.Directory != managed {
		t.Fatalf("retention record directory = %q, want %q", record.Directory, managed)
	}
	if record.RetainUntil.IsZero() {
		t.Fatalf("retention record carries no window: %#v", record)
	}
	upload := requireUploadRecord(t, harness.manager, runID)
	if !upload.Uploaded || upload.Reason != "" {
		t.Fatalf("upload record = %#v, want an upload with no skip reason", upload)
	}
	// Nothing was created under the path the ledger dispatched: the run never
	// ran there, so there is nothing there to go looking for.
	if _, err := os.Stat(dispatched); !os.IsNotExist(err) {
		t.Fatalf("the dispatched handoff path was populated on this node anyway: %v", err)
	}
}

// TestAJobWithNoRunIdentityKeepsItsOwnDirectory is the other half of the rule,
// and the reason the refusal above is scoped rather than universal. A one-shot
// process job submitted straight to L1 carries a handoff directory and no run:
// the L1 job spec asks for one and says nothing about where it lives. Such a
// job has no retained results and no result row on this node either way, so it
// keeps the directory it was given -- it is simply not this node's to sweep.
func TestAJobWithNoRunIdentityKeepsItsOwnDirectory(t *testing.T) {
	manager := newHandoffManager(filepath.Join(t.TempDir(), "node-handoffs"), t.TempDir(), "node-1", time.Hour, nil)
	elsewhere := filepath.Join(t.TempDir(), "submitter-chosen")
	spec := handoffClaim("", elsewhere, nil).Job.Spec
	delete(spec.Labels, "run_id")

	if manager.ownsHandoff(spec) {
		t.Fatal("a job with no run identity was treated as one this node retains results for")
	}
	if err := manager.prepareUnownedDirectory(spec); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(elsewhere)
	if err != nil || !info.IsDir() || info.Mode().Perm() != 0o700 {
		t.Fatalf("unowned handoff directory = (%v, %v), want a private directory", info, err)
	}
	if records := manager.loadRecords(); len(records) != 0 {
		t.Fatalf("an unowned directory produced retention records: %#v", records)
	}
}

// TestAnUnmanageableHandoffFailsTheAttemptWithANamedCode carries the refusal
// the whole way out. A reader does not see the sentinel; what L1 records, and
// what an operator reads back, is the attempt's completion code -- so the code
// is what this asserts, along with the two things that must not have happened:
// the workload did not run, and the directory was not created.
func TestAnUnmanageableHandoffFailsTheAttemptWithANamedCode(t *testing.T) {
	nodeRoot := filepath.Join(t.TempDir(), "node-handoffs")
	manager := newHandoffManager(nodeRoot, t.TempDir(), "node-1", time.Hour, nil)
	manager.ledgerRoot = filepath.Join(t.TempDir(), "ledger-handoffs")
	foreign := filepath.Join(t.TempDir(), "not-ours", "run_refused")
	executor := &preflightExecutor{}
	lifecycle := newAttemptLifecycle(attemptLifecycleDependencies{
		runtimes: workloadRuntimeSet{contract.JobKindProcess: processrunner.NewAdapter(executor)},
		clock:    systemClock{}, nodeID: "node-1", bootSessionID: "boot-1", handoffs: manager,
	})

	result, err := lifecycle.runWorkload(t.Context(), handoffClaim("run_refused", foreign, nil))
	if err == nil || result.SpawnError == nil || result.SpawnError.Code != contract.SpawnFailureHandoffPreparation {
		t.Fatalf("attempt result = (%#v, %v), want %s", result, err, contract.SpawnFailureHandoffPreparation)
	}
	if !errors.Is(err, errUnmanagedHandoffDirectory) {
		t.Fatalf("attempt error = %v, want the typed refusal beneath the code", err)
	}
	if executor.calls != 0 {
		t.Fatalf("a refused handoff still ran the workload %d times", executor.calls)
	}
	if _, statErr := os.Stat(foreign); !os.IsNotExist(statErr) {
		t.Fatalf("a refused handoff directory was created anyway: %v", statErr)
	}
}
