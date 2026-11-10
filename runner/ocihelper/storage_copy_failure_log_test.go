package ocihelper

import (
	"errors"
	"fmt"
	"strings"
	"testing"
)

func TestStorageCopyFailureCauseHidesOperatorPaths(t *testing.T) {
	mountRoot := "/Users/operator/wefty-mounts"
	custody := mountRoot + "/exports/tax \"2026\"/custody-1"
	paths := []operatorPath{
		{"", "<unused>"},
		{mountRoot, "<operator-mount-root>"},
		{custody, "<custody-path>"},
	}
	err := errors.Join(
		fmt.Errorf("Custody import source %q does not exist under operator mount root %q", custody, mountRoot),
		fmt.Errorf("open %s/storage.ext4: input/output error", custody),
		errors.New("stat "+mountRoot+"/other: no such file or directory"),
	)
	got := storageCopyFailureCause(err, paths)
	want := "Custody import source \"<custody-path>\" does not exist under operator mount root \"<operator-mount-root>\"\n" +
		"open <custody-path>/storage.ext4: input/output error\n" +
		"stat <operator-mount-root>/other: no such file or directory"
	if got != want {
		t.Fatalf("cause = %q\nwant    %q", got, want)
	}
	if strings.Contains(got, "/Users/operator") {
		t.Fatalf("cause kept an operator path: %q", got)
	}
}

func TestStorageCopyFailureCauseIsBounded(t *testing.T) {
	got := storageCopyFailureCause(errors.New("e2fsck failed: "+strings.Repeat("x", 3*storageCopyFailureCauseLimit)), nil)
	if len(got) != storageCopyFailureCauseLimit+len("...(truncated)") || !strings.HasSuffix(got, "...(truncated)") ||
		!strings.HasPrefix(got, "e2fsck failed: ") {
		t.Fatalf("bounded cause has length %d: %q...", len(got), got[:40])
	}
	if got := storageCopyFailureCause(errors.New("short"), nil); got != "short" {
		t.Fatalf("short cause = %q", got)
	}
}
