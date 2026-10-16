package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"time"
)

const execSweepLimit = 64
const execSweepBudget = 2 * time.Second
const execOrphanAge = time.Hour

// Tests inject the caller OS while retaining the real capture and sweep paths.
var execStorageGOOS = runtime.GOOS

// Prepare the root and perform an optional allocation as one write attempt.
// Preserve the original refusal if the macOS fallback also fails.
func execStorageLocation(allocate func(string) error) (string, error) {
	root, err := execStorageRoot()
	if err != nil {
		return "", err
	}
	attempt := func(root string) error {
		if err := prepareExecStorageRoot(root); err != nil {
			return err
		}
		if allocate != nil {
			return execStorageWriteError(root, allocate(root))
		}
		return nil
	}
	err = attempt(root)
	if err == nil {
		return root, nil
	}
	var denied *execSnapshotWriteError
	if execStorageGOOS != "darwin" || os.Getenv("WEFTY_EXEC_STORAGE_ROOT") != "" || !errors.As(err, &denied) {
		return "", err
	}
	fallback := filepath.Join(os.TempDir(), "wefty-exec")
	if fallback != root && attempt(fallback) == nil {
		return fallback, nil
	}
	return "", err
}

type execSnapshotWriteError struct {
	root  string
	cause error
}

func (e *execSnapshotWriteError) Error() string {
	return fmt.Sprintf("cannot write exec snapshot folder %q; allow writes to this folder, or set WEFTY_EXEC_STORAGE_ROOT to a writable folder outside the checkout that the node can read (see the wefty skill: skills/wefty/SKILL.md)", e.root)
}

func (e *execSnapshotWriteError) Unwrap() error { return e.cause }

func prepareExecStorageRoot(root string) error {
	if err := os.MkdirAll(root, 0700); err != nil {
		return execStorageWriteError(root, err)
	}
	return execStorageWriteError(root, os.Chmod(root, 0700))
}

func execStorageWriteError(root string, err error) error {
	if errors.Is(err, os.ErrPermission) || errors.Is(err, syscall.EROFS) {
		return &execSnapshotWriteError{root: root, cause: err}
	}
	return err
}

func recordExecStorageRun(storage, runID string, stderr io.Writer) {
	// The wrapper can finish and remove its storage before the reply arrives.
	if err := os.WriteFile(filepath.Join(storage, "run-id"), []byte(runID+"\n"), 0600); err != nil && !os.IsNotExist(err) {
		fmt.Fprintf(stderr, "wefty: warning: could not record run %s at %s: %v\n", runID, storage, err)
	}
}

// Sweep only immediate capture directories. Unverifiable runs, symlinks, and
// young unaccepted captures stay intact. Probes and retirement share the
// deadline; deletion runs independently. Enumeration is capped at 64 entries.
func sweepExecStorage(caller context.Context, clients *apiClients, stderr io.Writer) error {
	ctx, stop := context.WithTimeout(withoutClientRetry(caller), execSweepBudget)
	defer stop()
	warn := func(err error) { fmt.Fprintf(stderr, "wefty: warning: exec storage sweep: %v\n", err) }
	root, err := execStorageLocation(nil)
	if err != nil {
		// Capture needs a writable root too. Report this once at the caller.
		return err
	}
	roots := []string{root}
	if execStorageGOOS == "darwin" && os.Getenv("WEFTY_EXEC_STORAGE_ROOT") == "" {
		fallback := filepath.Join(os.TempDir(), "wefty-exec")
		if fallback != root {
			roots = append(roots, fallback)
		}
	}
	// Reserve a share for each root so a full state root cannot hide fallback
	// trash. Both roots share the original entry cap and foreground deadline.
	var storages []string
	for _, root := range roots {
		if ctx.Err() != nil {
			warn(ctx.Err())
			return nil
		}
		dir, err := os.Open(root)
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			warn(err)
			continue
		}
		entries, err := dir.ReadDir(execSweepLimit / len(roots))
		_ = dir.Close()
		if err != nil && err != io.EOF {
			warn(err)
			continue
		}
		for _, entry := range entries {
			if entry.IsDir() {
				storages = append(storages, filepath.Join(root, entry.Name()))
			}
		}
	}
	// Trash needs no lookup. Dispatch it before probing any old capture.
	for _, trash := range []bool{true, false} {
		for _, storage := range storages {
			if ctx.Err() != nil {
				warn(ctx.Err())
				return nil
			}
			name := filepath.Base(storage)
			if trash && strings.HasPrefix(name, ".wefty-trash-") {
				err = retireExecStorageContext(ctx, storage, "trash")
			} else if !trash && strings.HasPrefix(name, "wefty-exec-") {
				err = sweepExecCapture(ctx, clients, storage)
			} else {
				continue
			}
			if err != nil {
				var sandbox *networkSandboxError
				if errors.As(err, &sandbox) {
					// Exec also needs L3. Report its required network allowance once.
					return &unavailableError{cause: sandbox}
				}
				warn(err)
			}
		}
	}
	return nil
}

func sweepExecCapture(ctx context.Context, clients *apiClients, storage string) error {
	marker := filepath.Join(storage, "run-id")
	info, err := os.Lstat(marker)
	if os.IsNotExist(err) {
		folder, e := os.Lstat(storage)
		if os.IsNotExist(e) {
			return nil
		}
		if e != nil {
			return e
		}
		if time.Since(folder.ModTime()) < execOrphanAge {
			return nil
		}
		// Acceptance can outlive a lost reply or a killed CLI. Age alone does
		// not prove this snapshot is unused, including before the wrapper starts.
		record, found, e := findExecAcceptance(ctx, clients, storage)
		if e != nil {
			return fmt.Errorf("could not verify capture %s: %w", storage, e)
		}
		if found && !runIsTerminal(record.Status) {
			return nil
		}
	} else {
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() || info.Size() > 256 {
			return fmt.Errorf("invalid run marker at %s", marker)
		}
		raw, e := os.ReadFile(marker)
		if os.IsNotExist(e) {
			return nil
		}
		if e != nil {
			return e
		}
		runID := strings.TrimSpace(string(raw))
		if runID == "" {
			return fmt.Errorf("empty run marker at %s", marker)
		}
		record, e := clients.getRun(ctx, runID)
		if e != nil {
			return fmt.Errorf("could not verify capture %s for run %s: %w", storage, runID, e)
		}
		if !runIsTerminal(record.Status) {
			return nil
		}
	}
	return cleanupExecStorageContext(ctx, storage)
}
