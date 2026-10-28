package computerconformance_test

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Keep failure output bounded even when a desktop has written a large log.
// The complete Computer log is retained separately by the root-only lane.
func seatReadinessDiagnostics(logPath, lastStatus string) string {
	prefix := "last Seat status: " + lastStatus + "\n"
	file, err := os.Open(logPath)
	if err != nil {
		return prefix + fmt.Sprintf("Computer log unavailable: %v", err)
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return prefix + fmt.Sprintf("Computer log unavailable: %v", err)
	}
	const limit = 16 << 10
	start := max(0, info.Size()-limit)
	tail, err := io.ReadAll(io.NewSectionReader(file, start, limit))
	if err != nil {
		return prefix + fmt.Sprintf("Computer log unavailable: %v", err)
	}
	return prefix + "Computer log tail:\n" + string(tail)
}

func TestSeatReadinessDiagnosticsRetainsLastStatusAndBoundedLogTail(t *testing.T) {
	path := filepath.Join(t.TempDir(), "computer.log")
	const last = `{"event":"readiness_deadline","place":1,"readiness":{"waiting_for":"control"}}`
	if err := os.WriteFile(path, []byte(strings.Repeat("earlier log\n", 4096)+last), 0600); err != nil {
		t.Fatal(err)
	}
	status := `{"state":"failed","reason":"readiness_deadline"}`
	got := seatReadinessDiagnostics(path, status)
	if !strings.Contains(got, status) || !strings.HasSuffix(got, last) || len(got) > 17000 {
		t.Fatalf("missing last status/log or unbounded diagnostics: %d bytes: %s", len(got), got)
	}
}

func TestSeatReadinessDiagnosticsKeepsStatusWhenLogIsMissing(t *testing.T) {
	got := seatReadinessDiagnostics(filepath.Join(t.TempDir(), "missing.log"), "status read: unavailable")
	if !strings.Contains(got, "status read: unavailable") || !strings.Contains(got, "Computer log unavailable:") {
		t.Fatalf("diagnostics hid a read failure: %s", got)
	}
}
