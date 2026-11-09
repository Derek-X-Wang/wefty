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

// seatCgroupMemoryReport is best effort. It shows whether a Seat command was
// killed at the Computer memory limit (#1507), and how much of the charge was
// page cache (file, file_dirty) rather than memory that cannot be reclaimed.
func seatCgroupMemoryReport(cgroup string) string {
	parts := []string{}
	for _, name := range []string{"memory.max", "memory.current", "memory.peak", "memory.events"} {
		value, err := os.ReadFile(filepath.Join(cgroup, name))
		if err != nil {
			parts = append(parts, fmt.Sprintf("%s unavailable (%v)", name, err))
			continue
		}
		parts = append(parts, name+"="+strings.ReplaceAll(strings.TrimSpace(string(value)), "\n", ", "))
	}
	stat, err := os.ReadFile(filepath.Join(cgroup, "memory.stat"))
	if err != nil {
		parts = append(parts, fmt.Sprintf("memory.stat unavailable (%v)", err))
	} else {
		kept := []string{}
		for _, line := range strings.Split(string(stat), "\n") {
			key, _, _ := strings.Cut(line, " ")
			switch key {
			case "anon", "file", "file_dirty", "file_writeback", "shmem":
				kept = append(kept, line)
			}
		}
		parts = append(parts, "memory.stat="+strings.Join(kept, ", "))
	}
	return "Computer cgroup " + cgroup + ": " + strings.Join(parts, "; ")
}

func TestSeatCgroupMemoryReportShowsLimitEventsAndPageCache(t *testing.T) {
	cgroup := t.TempDir()
	for name, value := range map[string]string{
		"memory.max":     "1073741824\n",
		"memory.current": "1069834240\n",
		"memory.events":  "low 0\nhigh 0\nmax 41\noom 1\noom_kill 1\noom_group_kill 1\n",
		"memory.stat":    "anon 300000000\nfile 700000000\nkernel 9000\nfile_dirty 500000000\nfile_writeback 1000\nshmem 2000\n",
	} {
		if err := os.WriteFile(filepath.Join(cgroup, name), []byte(value), 0600); err != nil {
			t.Fatal(err)
		}
	}
	got := seatCgroupMemoryReport(cgroup)
	for _, want := range []string{
		"memory.max=1073741824", "memory.current=1069834240",
		"memory.events=low 0, high 0, max 41, oom 1, oom_kill 1, oom_group_kill 1",
		"memory.peak unavailable", // Older kernels have no memory.peak; the report keeps going.
		"memory.stat=anon 300000000, file 700000000, file_dirty 500000000, file_writeback 1000, shmem 2000",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("report lacks %q: %s", want, got)
		}
	}
	if strings.Contains(got, "kernel") {
		t.Fatalf("report kept an unrequested memory.stat key: %s", got)
	}
}
