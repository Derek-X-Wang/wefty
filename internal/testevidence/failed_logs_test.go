package testevidence

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

type fakeRow struct {
	name     string
	failed   bool
	cleanups []func()
	messages []string
}

func (row *fakeRow) Name() string     { return row.name }
func (row *fakeRow) Failed() bool     { return row.failed }
func (row *fakeRow) Cleanup(f func()) { row.cleanups = append(row.cleanups, f) }
func (row *fakeRow) Logf(f string, a ...any) {
	row.messages = append(row.messages, fmt.Sprintf(f, a...))
}
func (row *fakeRow) cleanup() {
	for i := len(row.cleanups) - 1; i >= 0; i-- {
		row.cleanups[i]()
	}
}

func TestCaptureFailedAndPassedRows(t *testing.T) {
	for _, failed := range []bool{false, true} {
		t.Run(fmt.Sprint(failed), func(t *testing.T) {
			directory := t.TempDir()
			t.Setenv("WEFTY_REALTIME_EVIDENCE_DIR", directory)
			helper := filepath.Join(t.TempDir(), "helper.log")
			if err := os.WriteFile(helper, []byte("deadman renewal\n"), 0o600); err != nil {
				t.Fatal(err)
			}
			t.Setenv("WEFTY_REALTIME_HELPER_LOG", helper)
			row := &fakeRow{name: "TestCancel/TERM_ignored", failed: failed}
			output := Capture(row)
			_, _ = output.Write([]byte("agent renewal\n"))
			// Registered later, like process.stop: trailing output is retained.
			row.Cleanup(func() { _, _ = output.Write([]byte("agent shutdown\n")) })
			row.cleanup()
			entries, err := os.ReadDir(directory)
			if err != nil {
				t.Fatal(err)
			}
			if !failed {
				if len(entries) != 0 {
					t.Fatal("passing row created evidence")
				}
				return
			}
			assertLog(t, directory, "node-agent-*.log", "agent renewal\nagent shutdown\n")
			assertLog(t, directory, "oci-helper-deadman-*.log", "deadman renewal\n")
			// Later helper output must not change this failed row's snapshot.
			if err := os.WriteFile(helper, []byte("later row\n"), 0o600); err != nil {
				t.Fatal(err)
			}
			assertLog(t, directory, "oci-helper-deadman-*.log", "deadman renewal\n")
		})
	}
}

func TestCaptureTeardownFailureAndRestarts(t *testing.T) {
	directory := t.TempDir()
	t.Setenv("WEFTY_REALTIME_EVIDENCE_DIR", directory)
	t.Setenv("WEFTY_REALTIME_HELPER_LOG", "")
	row := &fakeRow{name: "TestRestart"}
	for _, text := range []string{"first boot", "second boot"} {
		_, _ = Capture(row).Write([]byte(text))
	}
	row.Cleanup(func() { row.failed = true })
	row.cleanup()
	paths, err := filepath.Glob(filepath.Join(directory, "failed-row-logs", "*", "node-agent-*.log"))
	if err != nil || len(paths) != 2 {
		t.Fatalf("restarted agent logs=%v err=%v", paths, err)
	}
	seen := map[string]bool{}
	for _, path := range paths {
		payload, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		seen[string(payload)] = true
	}
	if !seen["first boot"] || !seen["second boot"] {
		t.Fatalf("restart logs overwritten: %v", seen)
	}
}

func TestLogAndFileCapsKeepTail(t *testing.T) {
	output := &Log{enabled: true}
	for _, payload := range [][]byte{bytes.Repeat([]byte("a"), logBytes+17), []byte("latest renewal\n")} {
		if n, err := output.Write(payload); n != len(payload) || err != nil {
			t.Fatalf("Write=%d,%v", n, err)
		}
	}
	want := append(bytes.Repeat([]byte("a"), logBytes-len("latest renewal\n")), []byte("latest renewal\n")...)
	if !bytes.Equal(output.data, want) {
		t.Fatal("bounded buffer did not keep the exact tail")
	}
	path := filepath.Join(t.TempDir(), "helper.log")
	if err := os.WriteFile(path, append([]byte("old"), want...), 0o600); err != nil {
		t.Fatal(err)
	}
	got, err := fileTail(path, logBytes)
	if err != nil || !bytes.Equal(got, want) {
		t.Fatalf("helper file tail differs: %v", err)
	}
	directory := t.TempDir()
	row := &fakeRow{name: "TestCap", failed: true}
	retain(row, directory, "node-agent-*.log", append([]byte("old"), want...), totalLogBytes)
	assertLog(t, directory, "node-agent-*.log", string(want))
}

func TestRetainTotalCapIncludesEarlierRows(t *testing.T) {
	directory := t.TempDir()
	first := &fakeRow{name: "TestFirst", failed: true}
	second := &fakeRow{name: "TestSecond", failed: true}
	retain(first, directory, "node-agent-*.log", []byte("123456"), 10)
	retain(second, directory, "node-agent-*.log", []byte("discardTAIL"), 10)
	retain(second, directory, "oci-helper-deadman-*.log", []byte("no room"), 10)
	var total int64
	err := filepath.WalkDir(directory, func(path string, entry os.DirEntry, err error) error {
		if err != nil || entry.IsDir() {
			return err
		}
		info, err := entry.Info()
		if err == nil {
			total += info.Size()
		}
		return err
	})
	if err != nil || total != 10 {
		t.Fatalf("total bytes=%d err=%v", total, err)
	}
	payload, err := os.ReadFile(mustSingleLog(t, filepath.Join(directory, "failed-row-logs", rowDirectory(second.name), "node-agent-*.log")))
	if err != nil || string(payload) != "TAIL" {
		t.Fatalf("total cap did not keep tail: %q %v", payload, err)
	}
	if !strings.Contains(strings.Join(second.messages, "\n"), "total cap reached") {
		t.Fatal("omission at total cap was not reported")
	}
}

func TestRowDirectorySanitizesAndAvoidsCollisions(t *testing.T) {
	seen := map[string]bool{}
	for _, name := range []string{"TestCancel/TERM_ignored", "TestCancel_TERM_ignored", "../../bad\\name:空", "/", "", strings.Repeat("x", 400), strings.Repeat("x", 401)} {
		got := rowDirectory(name)
		if len(got) > 113 || got == "" || strings.ContainsAny(got, "/\\. :") || seen[got] {
			t.Fatalf("unsafe or colliding directory for %q: %q", name, got)
		}
		seen[got] = true
	}
}

func TestCaptureMissingLogsAndWriteErrorsAreDiagnostic(t *testing.T) {
	directory := t.TempDir()
	t.Setenv("WEFTY_REALTIME_EVIDENCE_DIR", directory)
	t.Setenv("WEFTY_REALTIME_HELPER_LOG", filepath.Join(t.TempDir(), "missing.log"))
	row := &fakeRow{name: "TestMissing", failed: true}
	Capture(row)
	row.cleanup()
	assertLog(t, directory, "node-agent-*.log", "log unavailable: no output captured\n")
	if !strings.Contains(strings.Join(row.messages, "\n"), "helper/deadman log unavailable") {
		t.Fatal("missing helper log was not reported")
	}
	// A non-directory evidence destination deterministically fails even as root.
	path := filepath.Join(t.TempDir(), "file")
	if err := os.WriteFile(path, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	retain(row, path, "node-agent-*.log", []byte("diagnostic"), totalLogBytes)
	if !strings.Contains(strings.Join(row.messages, "\n"), "cannot measure total size") {
		t.Fatal("evidence write failure was not reported")
	}
}

func TestCaptureUnsetDirectoryDoesNothing(t *testing.T) {
	t.Setenv("WEFTY_REALTIME_EVIDENCE_DIR", "")
	row := &fakeRow{name: "TestNoEvidence", failed: true}
	output := Capture(row)
	_, _ = output.Write([]byte("discard"))
	row.cleanup()
	if len(output.data) != 0 || len(row.messages) != 0 {
		t.Fatal("unset evidence directory captured output")
	}
}

func assertLog(t *testing.T, directory, pattern, want string) {
	t.Helper()
	path := mustSingleLog(t, filepath.Join(directory, "failed-row-logs", "*", pattern))
	payload, err := os.ReadFile(path)
	if err != nil || string(payload) != want {
		t.Fatalf("log %s differs: bytes=%d err=%v", pattern, len(payload), err)
	}
}

func mustSingleLog(t *testing.T, pattern string) string {
	t.Helper()
	paths, err := filepath.Glob(pattern)
	if err != nil || len(paths) != 1 {
		t.Fatalf("logs matching %s: %v err=%v", pattern, paths, err)
	}
	return paths[0]
}

func TestCaptureLogfPreservesCallerAndShutdownOutput(t *testing.T) {
	directory := t.TempDir()
	t.Setenv("WEFTY_REALTIME_EVIDENCE_DIR", directory)
	t.Setenv("WEFTY_REALTIME_HELPER_LOG", "")
	row := &fakeRow{name: "in-process", failed: true}
	var messages []string
	logger := CaptureLogf(row, func(format string, args ...any) { messages = append(messages, fmt.Sprintf(format, args...)) })
	logger("registered %s", "node")
	row.Cleanup(func() { logger("shutdown") })
	row.cleanup()
	if strings.Join(messages, ";") != "registered node;shutdown" {
		t.Fatalf("caller logs=%v", messages)
	}
	paths, _ := filepath.Glob(filepath.Join(directory, "failed-row-logs", "*", "node-agent-*.log"))
	if len(paths) != 1 {
		t.Fatalf("logs=%v", paths)
	}
	data, err := os.ReadFile(paths[0])
	if err != nil || !strings.Contains(string(data), "registered node\n") || !strings.Contains(string(data), "shutdown\n") {
		t.Fatalf("log=%s err=%v", data, err)
	}
}

func TestCaptureLogfUnsetPreservesNilLogger(t *testing.T) {
	t.Setenv("WEFTY_REALTIME_EVIDENCE_DIR", "")
	row := &fakeRow{name: "disabled"}
	if CaptureLogf(row, nil) != nil {
		t.Fatal("disabled capture changed nil logger")
	}
}

func TestCaptureCommandPreservesFinalWriters(t *testing.T) {
	for _, existing := range []bool{false, true} {
		t.Run(fmt.Sprint(existing), func(t *testing.T) {
			directory := t.TempDir()
			t.Setenv("WEFTY_REALTIME_EVIDENCE_DIR", directory)
			t.Setenv("WEFTY_REALTIME_HELPER_LOG", "")
			row := &fakeRow{name: "subprocess", failed: true}
			command := exec.Command("/bin/sh", "-c", "printf stdout; printf stderr >&2")
			var out, stderr bytes.Buffer
			if existing {
				command.Stdout, command.Stderr = &out, &stderr
			}
			CaptureCommand(row, command)
			if err := command.Run(); err != nil {
				t.Fatal(err)
			}
			row.cleanup()
			if existing && (out.String() != "stdout" || stderr.String() != "stderr") {
				t.Fatalf("stdout=%q stderr=%q", &out, &stderr)
			}
			paths, _ := filepath.Glob(filepath.Join(directory, "failed-row-logs", "*", "node-agent-*.log"))
			if len(paths) != 1 {
				t.Fatalf("logs=%v", paths)
			}
			data, err := os.ReadFile(paths[0])
			if err != nil || !strings.Contains(string(data), "stdout") || !strings.Contains(string(data), "stderr") {
				t.Fatalf("log=%s err=%v", data, err)
			}
		})
	}
}

func TestCaptureCommandPreservesSharedWriter(t *testing.T) {
	t.Setenv("WEFTY_REALTIME_EVIDENCE_DIR", t.TempDir())
	row := &fakeRow{name: "shared"}
	var output bytes.Buffer
	command := exec.Command("/bin/sh", "-c", "printf stdout; printf stderr >&2")
	command.Stdout, command.Stderr = &output, &output
	CaptureCommand(row, command)
	if command.Stdout != command.Stderr {
		t.Fatal("capture split a shared writer into concurrent os/exec copy loops")
	}
	if err := command.Run(); err != nil {
		t.Fatal(err)
	}
	if output.String() != "stdoutstderr" {
		t.Fatalf("output=%q", &output)
	}
}

func TestCaptureCommandUnsetPreservesWriters(t *testing.T) {
	t.Setenv("WEFTY_REALTIME_EVIDENCE_DIR", "")
	row := &fakeRow{name: "disabled"}
	command := exec.Command("/bin/sh")
	CaptureCommand(row, command)
	if command.Stdout != nil || command.Stderr != nil {
		t.Fatal("disabled capture changed nil writers")
	}
}
