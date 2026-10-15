package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"
)

const execSweepLimit = 64
const execSweepBudget = 2 * time.Second
const execOrphanAge = time.Hour

func prepareExecStorageRoot(root string) error {
	if err := os.MkdirAll(root, 0700); err != nil {
		return execStorageWriteError(root, err)
	}
	return execStorageWriteError(root, os.Chmod(root, 0700))
}

func execStorageWriteError(root string, err error) error {
	if errors.Is(err, os.ErrPermission) || errors.Is(err, syscall.EROFS) {
		return fmt.Errorf("cannot write exec snapshot folder %q; a sandboxed AI agent must allow writes to this folder (see the wefty skill: skills/wefty/SKILL.md)", root)
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
// young unaccepted captures stay intact. Every probe and removal shares the
// same deadline, and directory enumeration itself is capped at 64 entries.
func sweepExecStorage(caller context.Context, clients *apiClients, stderr io.Writer) error {
	ctx, stop := context.WithTimeout(withoutClientRetry(caller), execSweepBudget)
	defer stop()
	warn := func(err error) { fmt.Fprintf(stderr, "wefty: warning: exec storage sweep: %v\n", err) }
	root, err := execStorageRoot()
	if err != nil {
		return err
	}
	if err = prepareExecStorageRoot(root); err != nil {
		// Root preparation is also required for the new capture. Let the
		// caller report it once rather than warning and then failing again.
		return err
	}
	dir, err := os.Open(root)
	if err != nil {
		warn(err)
		return nil
	}
	defer dir.Close()
	entries, err := dir.ReadDir(execSweepLimit)
	if err != nil && err != io.EOF {
		warn(err)
		return nil
	}
	for _, entry := range entries {
		if ctx.Err() != nil {
			warn(ctx.Err())
			return nil
		}
		if !entry.IsDir() || !strings.HasPrefix(entry.Name(), "wefty-exec-") {
			continue
		}
		storage := filepath.Join(root, entry.Name())
		if err = sweepExecCapture(ctx, clients, storage); err != nil {
			warn(err)
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
