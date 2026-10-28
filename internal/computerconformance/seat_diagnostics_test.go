package computerconformance_test

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
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

func waitSeatReadinessDiagnostics(logPath, lastStatus string, now func() time.Time, sleep func(time.Duration)) string {
	deadline := now().Add(2 * time.Second)
	// The status file and the JSON log serialize the same answer. Match its
	// complete value so an older generation or another place cannot end the wait.
	answer := `"answer":` + strings.TrimSpace(lastStatus)
	for {
		diagnostics := seatReadinessDiagnostics(logPath, lastStatus)
		if strings.Contains(diagnostics, answer) {
			return diagnostics
		}
		remaining := deadline.Sub(now())
		if remaining <= 0 {
			return diagnostics
		}
		sleep(min(100*time.Millisecond, remaining))
	}
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

func TestSeatReadinessDiagnosticsWaitsForDelayedMatchingAnswer(t *testing.T) {
	path := filepath.Join(t.TempDir(), "computer.log")
	status := `{"generation":2,"place":3,"state":"failed","reason":"readiness_deadline"}` + "\n"
	clock := time.Unix(0, 0)
	polls := 0
	sleep := func(delay time.Duration) {
		clock = clock.Add(delay)
		polls++
		data := `{"event":"seat_answer","answer":{"generation":1,"place":3,"state":"failed","reason":"readiness_deadline"}}` + "\n"
		if polls == 2 {
			data += `{"event":"readiness_deadline","readiness":{"waiting_for":"view"}}` + "\n"
			data += `{"event":"seat_answer","answer":` + strings.TrimSpace(status) + "}\n"
		}
		if err := os.WriteFile(path, []byte(data), 0600); err != nil {
			t.Fatal(err)
		}
	}
	got := waitSeatReadinessDiagnostics(path, status, func() time.Time { return clock }, sleep)
	if polls != 2 || !strings.Contains(got, `"waiting_for":"view"`) || !strings.Contains(got, `"answer":`+strings.TrimSpace(status)) {
		t.Fatalf("returned before the shim copied the matching answer: polls=%d %s", polls, got)
	}
}

func TestSeatReadinessDiagnosticsStopsWaitingAfterTwoSeconds(t *testing.T) {
	clock := time.Unix(0, 0)
	start := clock
	got := waitSeatReadinessDiagnostics(filepath.Join(t.TempDir(), "missing.log"), `{"state":"failed"}`,
		func() time.Time { return clock }, func(delay time.Duration) { clock = clock.Add(delay) })
	if clock.Sub(start) != 2*time.Second || !strings.Contains(got, "Computer log unavailable:") {
		t.Fatalf("diagnostic retry was not bounded at two seconds: elapsed=%s %s", clock.Sub(start), got)
	}
}
