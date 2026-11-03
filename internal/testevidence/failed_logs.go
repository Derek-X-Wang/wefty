// Package testevidence supports test harnesses. It is not used by product code.
package testevidence

import (
	"crypto/sha256"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

const (
	logBytes = 2 << 20
	// The workflows run packages serially (-p 1). Scan existing files so this
	// budget also includes failed rows retained by earlier package processes.
	totalLogBytes = 32 << 20
)

type rowTest interface {
	Name() string
	Failed() bool
	Cleanup(func())
	Logf(string, ...any)
}

// Log retains only the last logBytes bytes while preserving the writer contract.
type Log struct {
	mu      sync.Mutex
	enabled bool
	data    []byte
}

// Capture registers before process cleanup, so LIFO cleanup captures shutdown
// output and teardown failures too. Passing rows and unset evidence dirs write
// nothing. Repeated calls keep distinct logs for agent restarts in the same row.
func Capture(t rowTest) *Log {
	directory := os.Getenv("WEFTY_REALTIME_EVIDENCE_DIR")
	helperPath := os.Getenv("WEFTY_REALTIME_HELPER_LOG")
	output := &Log{enabled: directory != ""}
	t.Cleanup(func() {
		if !t.Failed() || directory == "" {
			return
		}
		output.mu.Lock()
		payload := append([]byte(nil), output.data...)
		output.mu.Unlock()
		retain(t, directory, "node-agent-*.log", payload, totalLogBytes)
		if helperPath != "" {
			payload, err := fileTail(helperPath, logBytes)
			if err != nil {
				t.Logf("failed-row logs: OCI helper/deadman log unavailable: %v", err)
				payload = []byte("OCI helper/deadman log unavailable: " + err.Error() + "\n")
			}
			// The helper is shared. Snapshot it at this row's cleanup rather
			// than letting later rows displace the failure from its tail.
			retain(t, directory, "oci-helper-deadman-*.log", payload, totalLogBytes)
		}
	})
	return output
}

func (output *Log) Write(payload []byte) (int, error) {
	n := len(payload)
	if !output.enabled {
		return n, nil
	}
	output.mu.Lock()
	defer output.mu.Unlock()
	if len(payload) >= logBytes {
		output.data = append(output.data[:0], payload[len(payload)-logBytes:]...)
	} else {
		if excess := len(output.data) + len(payload) - logBytes; excess > 0 {
			output.data = append(output.data[:0], output.data[excess:]...)
		}
		output.data = append(output.data, payload...)
	}
	return n, nil
}

// CaptureCommand wraps the final writers, after the caller has configured them.
// Register the process stop/join cleanup after this call.
func CaptureCommand(t rowTest, command *exec.Cmd) {
	output := Capture(t)
	if !output.enabled {
		return
	}
	// Callers use comparable pointer writers. Preserve shared-writer identity
	// so os/exec keeps stdout/stderr writes serialized into the caller's buffer.
	shared := command.Stdout == command.Stderr
	if command.Stdout == nil {
		command.Stdout = output
	} else {
		command.Stdout = io.MultiWriter(command.Stdout, output)
	}
	if shared {
		command.Stderr = command.Stdout
	} else if command.Stderr == nil {
		command.Stderr = output
	} else {
		command.Stderr = io.MultiWriter(command.Stderr, output)
	}
}

// CaptureLogf preserves an existing logger and also retains failed-row evidence.
func CaptureLogf(t rowTest, existing func(string, ...any)) func(string, ...any) {
	output := Capture(t)
	if !output.enabled {
		return existing
	}
	return func(format string, args ...any) {
		output.Logf(format, args...)
		if existing != nil {
			existing(format, args...)
		}
	}
}

// Logf supplies the existing agent.Config.Logf seam for in-process test agents.
func (output *Log) Logf(format string, args ...any) {
	_, _ = fmt.Fprintf(output, "%s %s\n", time.Now().UTC().Format(time.RFC3339Nano), fmt.Sprintf(format, args...))
}

func rowDirectory(name string) string {
	var safe strings.Builder
	for _, c := range name {
		if safe.Len() >= 100 {
			break
		}
		if c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '-' || c == '_' {
			safe.WriteRune(c)
		} else {
			safe.WriteByte('_')
		}
	}
	// Preserve readable row names without collisions from slashes, punctuation,
	// or truncation, and without allowing path traversal.
	sum := sha256.Sum256([]byte(name))
	return fmt.Sprintf("%s-%x", safe.String(), sum[:6])
}

var retainMu sync.Mutex

func retain(t rowTest, directory, pattern string, payload []byte, budget int64) {
	retainMu.Lock()
	defer retainMu.Unlock()
	root := filepath.Join(directory, "failed-row-logs")
	row := filepath.Join(root, rowDirectory(t.Name()))
	if len(payload) == 0 {
		t.Logf("failed-row logs: %s missing (no output captured)", pattern)
		payload = []byte("log unavailable: no output captured\n")
	}
	var used int64
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if os.IsNotExist(err) && path == root {
			return nil
		}
		if err != nil {
			return err
		}
		if !entry.IsDir() {
			info, err := entry.Info()
			if err != nil {
				return err
			}
			used += info.Size()
		}
		return nil
	})
	if err != nil {
		t.Logf("failed-row logs: cannot measure total size: %v", err)
		return
	}
	limit := min(int64(logBytes), budget-used)
	if limit <= 0 {
		t.Logf("failed-row logs: total cap reached; omitted %s", pattern)
		return
	}
	if int64(len(payload)) > limit {
		payload = payload[int64(len(payload))-limit:]
		t.Logf("failed-row logs: %s truncated to last %d bytes", pattern, limit)
	}
	if err := os.MkdirAll(row, 0o700); err != nil {
		t.Logf("failed-row logs: create row directory: %v", err)
		return
	}
	file, err := os.CreateTemp(row, pattern)
	if err != nil {
		t.Logf("failed-row logs: create log: %v", err)
		return
	}
	_, writeErr := file.Write(payload)
	closeErr := file.Close()
	if writeErr != nil || closeErr != nil {
		t.Logf("failed-row logs: write=%v close=%v", writeErr, closeErr)
		return
	}
	t.Logf("failed-row logs: retained %s (%d bytes)", file.Name(), len(payload))
}

func fileTail(path string, limit int64) ([]byte, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return nil, err
	}
	// SectionReader also bounds a concurrently growing helper log.
	return io.ReadAll(io.NewSectionReader(file, max(0, info.Size()-limit), limit))
}
