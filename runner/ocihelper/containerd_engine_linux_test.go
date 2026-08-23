//go:build linux

package ocihelper

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

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
