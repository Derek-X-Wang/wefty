package agent

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

// TestAdoptionReplacesATornRecordAtItsOwnName: power loss can leave a record
// that was being replaced empty or cut short. It names no run, loadRecords
// skips it, and refusing to replace it left the directory unsweepable for as
// long as the node lived (#599).
func TestAdoptionReplacesATornRecordAtItsOwnName(t *testing.T) {
	for _, testCase := range []struct {
		name    string
		planted string
	}{
		{name: "an empty record", planted: ""},
		{name: "a record cut short", planted: `{"run_id":"run_planted","node_id":"no`},
		{name: "a record that is not JSON", planted: "{not json"},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			harness := newRetentionHarness(t, time.Hour)
			path := harness.plantDirectory("run_planted", &handoffMarker{
				RunID: "run_planted", NodeID: "node-1", RetainUntil: harness.now.Add(time.Hour).UTC(),
			})
			file := harness.manager.recordPath("run_planted")
			if err := os.MkdirAll(filepath.Dir(file), 0o700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(file, []byte(testCase.planted), 0o600); err != nil {
				t.Fatal(err)
			}

			if err := harness.manager.adoptResidue(); err != nil {
				t.Fatal(err)
			}
			record, err := harness.manager.readRecord(file)
			if err != nil {
				t.Fatalf("the torn record was not replaced: %v (logs %v)", err, harness.logs)
			}
			if record.RunID != "run_planted" || record.NodeID != "node-1" || record.Directory != path || !record.Adopted {
				t.Fatalf("adoption wrote %#v", record)
			}
			if status := harness.account(); status.Unrecorded != 0 {
				t.Fatalf("the adopted directory is still unrecorded: %#v", status)
			}
		})
	}
}

// TestAdoptionStillRefusesWhatItCannotIdentify: the repair is only for a
// regular file at this run's own name whose bytes name no run. A name that is
// not a regular file is refused as before, and a torn file under another name
// -- here the older agent's shared name, which is also another run's current
// one -- is never touched.
func TestAdoptionStillRefusesWhatItCannotIdentify(t *testing.T) {
	t.Run("a link at the run's own name", func(t *testing.T) {
		harness := newRetentionHarness(t, time.Hour)
		path := harness.plantDirectory("run_planted", &handoffMarker{
			RunID: "run_planted", NodeID: "node-1", RetainUntil: harness.now.Add(time.Hour).UTC(),
		})
		file := harness.manager.recordPath("run_planted")
		if err := os.MkdirAll(filepath.Dir(file), 0o700); err != nil {
			t.Fatal(err)
		}
		target := filepath.Join(t.TempDir(), "garbage")
		if err := os.WriteFile(target, []byte("{not json"), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(target, file); err != nil {
			t.Fatal(err)
		}

		if err := harness.manager.adoptResidue(); err != nil {
			t.Fatal(err)
		}
		if info, err := os.Lstat(file); err != nil || info.Mode()&os.ModeSymlink == 0 {
			t.Fatalf("adoption replaced a record name that is not a regular file: %v, %v", info, err)
		}
		if payload, _ := os.ReadFile(target); string(payload) != "{not json" {
			t.Fatalf("adoption wrote through a link: %q", payload)
		}
		if !harness.logged("could not be read") {
			t.Fatalf("the refusal was silent: %v", harness.logs)
		}
		if _, err := os.Stat(path); err != nil {
			t.Fatalf("a directory adoption refused was removed: %v", err)
		}
		if status := harness.account(); status.Unrecorded == 0 {
			t.Fatalf("a directory adoption refused is not reported as unrecorded: %#v", status)
		}
	})
	t.Run("a torn file under another name", func(t *testing.T) {
		harness := newRetentionHarness(t, time.Hour)
		const runID = "run.planted"
		path := harness.plantDirectory(runID, &handoffMarker{
			RunID: runID, NodeID: "node-1", RetainUntil: harness.now.Add(time.Hour).UTC(),
		})
		foreign := filepath.Join(filepath.Dir(harness.manager.recordPath(runID)), legacyRecordComponent(runID))
		if foreign == harness.manager.recordPath(runID) {
			t.Fatal("the fixture needs a run whose older name differs from its own")
		}
		if err := os.MkdirAll(filepath.Dir(foreign), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(foreign, []byte("{not json"), 0o600); err != nil {
			t.Fatal(err)
		}

		if err := harness.manager.adoptResidue(); err != nil {
			t.Fatal(err)
		}
		if payload, err := os.ReadFile(foreign); err != nil || string(payload) != "{not json" {
			t.Fatalf("adoption touched a torn file under another name: %q, %v", payload, err)
		}
		record, err := harness.manager.readRecord(harness.manager.recordPath(runID))
		if err != nil || record.RunID != runID || record.Directory != path {
			t.Fatalf("the run's own record = %#v, %v", record, err)
		}
	})
}

// TestHandoffMarkerRewriteNeverLeavesTheNameEmpty: the marker used to be
// replaced by deleting it and then creating it, and a crash between the two
// made the next preparation refuse the directory as unmanaged (#599). A
// reader watching the name while the marker is rewritten must never find it
// missing.
func TestHandoffMarkerRewriteNeverLeavesTheNameEmpty(t *testing.T) {
	run, err := os.OpenRoot(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer run.Close()
	marker := handoffMarker{RunID: "run_marker", NodeID: "node-1", RetainUntil: time.Date(2026, 9, 30, 0, 0, 0, 0, time.UTC)}
	if err := writeHandoffMarker(run, marker); err != nil {
		t.Fatal(err)
	}
	var stop atomic.Bool
	var missing atomic.Int64
	done := make(chan struct{})
	go func() {
		defer close(done)
		for !stop.Load() {
			if _, err := run.Lstat(handoffMarkerName); errors.Is(err, fs.ErrNotExist) {
				missing.Add(1)
			}
		}
	}()
	for index := range 300 {
		marker.RetainUntil = marker.RetainUntil.Add(time.Duration(index) * time.Second)
		if err := writeHandoffMarker(run, marker); err != nil {
			stop.Store(true)
			<-done
			t.Fatal(err)
		}
	}
	stop.Store(true)
	<-done
	if count := missing.Load(); count != 0 {
		t.Fatalf("the marker was missing %d times while it was rewritten", count)
	}
	read, exists, err := readHandoffMarker(run)
	if err != nil || !exists || !read.RetainUntil.Equal(marker.RetainUntil) {
		t.Fatalf("readHandoffMarker = %#v, %v, %v; want the last marker written", read, exists, err)
	}
}

// TestAMarkerWriteCutShortLeavesNoUnmanagedFile: a crash after the marker is
// staged and before it is renamed leaves the staging file. It is the agent's
// own, so it does not make the directory look unmanaged, and the next marker
// write replaces it.
func TestAMarkerWriteCutShortLeavesNoUnmanagedFile(t *testing.T) {
	directory := t.TempDir()
	run, err := os.OpenRoot(directory)
	if err != nil {
		t.Fatal(err)
	}
	defer run.Close()
	if err := os.WriteFile(filepath.Join(directory, handoffMarkerStagingName), []byte(`{"run_id":"run_m`), 0o600); err != nil {
		t.Fatal(err)
	}
	if hasFiles, err := handoffHasFiles(run); err != nil || hasFiles {
		t.Fatalf("handoffHasFiles = %v, %v; a leftover marker staging file is not a workload file", hasFiles, err)
	}
	marker := handoffMarker{RunID: "run_marker", NodeID: "node-1"}
	if err := writeHandoffMarker(run, marker); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(filepath.Join(directory, handoffMarkerStagingName)); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("the staging file outlived the next marker write: %v", err)
	}
	if read, exists, err := readHandoffMarker(run); err != nil || !exists || read.RunID != "run_marker" {
		t.Fatalf("readHandoffMarker = %#v, %v, %v", read, exists, err)
	}
}
