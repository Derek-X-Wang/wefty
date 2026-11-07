package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/Derek-X-Wang/wefty/internal/execsweep"
)

const execSweepNotebookEnv = "WEFTY_EXEC_SWEEP_NOTEBOOK"
const execSweepAttemptEnv = "WEFTY_EXEC_SWEEP_ATTEMPT_ID"

// The wrapper publishes these facts before moving version 1 storage once.
type execSweepEntry struct {
	Source             string `json:"source"`
	Pending            string `json:"pending"`
	StorageIdentity    string `json:"storage_identity"`
	AttemptID          string `json:"attempt_id"`
	OriginalStorage    string `json:"original_storage"`
	Allocation         string `json:"allocation"`
	AllocationIdentity string `json:"allocation_identity"`
	CreatedAt          int64  `json:"created_at"`
}

func (m *handoffManager) execSweepNotebook() string {
	return filepath.Join(m.root, ".wefty-exec-sweep", m.nodeID)
}

func (m *handoffManager) noteExecSweepReaped(attemptID string) {
	// A reaped publisher cannot publish later. Retain proof only for an entry
	// it actually left, so normally completed runs do not grow this map.
	entries, err := os.ReadDir(m.execSweepNotebook())
	if err != nil {
		return
	}
	for _, dir := range entries {
		if !dir.IsDir() || strings.HasSuffix(dir.Name(), ".tmp") {
			continue
		}
		raw, err := os.ReadFile(filepath.Join(m.execSweepNotebook(), dir.Name(), "entry.json"))
		var entry execSweepEntry
		if err != nil || json.Unmarshal(raw, &entry) != nil || entry.AttemptID != attemptID {
			continue
		}
		m.mu.Lock()
		if m.execSweepReaped == nil {
			m.execSweepReaped = make(map[string]bool)
		}
		m.execSweepReaped[attemptID] = true
		m.mu.Unlock()
		return
	}
}

func execDirectoryIdentity(info os.FileInfo) string {
	stat := info.Sys().(*syscall.Stat_t)
	return fmt.Sprintf("%d:%d", stat.Dev, stat.Ino)
}

type execNotebookDirectory struct {
	parent, root *os.Root
	name         string
	identity     string
}

// An absent or replaced path resolves only this part of the entry. All acts
// use a retained handle and recheck the name against its recorded identity.
func openExecNotebookDirectory(path, identity string) (*execNotebookDirectory, error) {
	parent, err := os.OpenRoot(filepath.Dir(path))
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	name := filepath.Base(path)
	info, err := parent.Lstat(name)
	if os.IsNotExist(err) {
		parent.Close()
		return nil, nil
	}
	if err != nil {
		parent.Close()
		return nil, err
	}
	if !info.IsDir() || info.Sys().(*syscall.Stat_t).Uid != uint32(os.Getuid()) || execDirectoryIdentity(info) != identity {
		parent.Close()
		return nil, nil
	}
	root, err := openHandoffDirectory(parent, name)
	if err != nil {
		parent.Close()
		return nil, err
	}
	d := &execNotebookDirectory{parent: parent, root: root, name: name, identity: identity}
	if err := d.verify(); err != nil {
		d.close()
		return nil, err
	}
	return d, nil
}

func (d *execNotebookDirectory) close() { d.root.Close(); d.parent.Close() }
func (d *execNotebookDirectory) verify() error {
	held, err := d.root.Stat(".")
	if err != nil {
		return err
	}
	current, err := d.parent.Lstat(d.name)
	if err != nil {
		return err
	}
	if !held.IsDir() || held.Sys().(*syscall.Stat_t).Uid != uint32(os.Getuid()) || execDirectoryIdentity(held) != d.identity || !os.SameFile(held, current) {
		return errors.New("exec sweep directory identity changed")
	}
	return nil
}

func (d *execNotebookDirectory) remove(ctx context.Context) error {
	if err := d.verify(); err != nil {
		return err
	}
	if err := removeExecNotebookContents(ctx, d.root); err != nil {
		return err
	}
	if err := d.verify(); err != nil {
		return err
	}
	return d.parent.Remove(d.name)
}

func removeExecNotebookContents(ctx context.Context, root *os.Root) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := root.Chmod(".", 0700); err != nil {
		return err
	}
	file, err := root.Open(".")
	if err != nil {
		return err
	}
	entries, err := file.ReadDir(-1)
	file.Close()
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if err := ctx.Err(); err != nil {
			return err
		}
		if entry.IsDir() {
			if err := root.Chmod(entry.Name(), 0700); err != nil {
				return err
			}
			child, err := openHandoffDirectory(root, entry.Name())
			if err != nil {
				return err
			}
			err = removeExecNotebookContents(ctx, child)
			child.Close()
			if err != nil {
				return err
			}
		}
		if err := root.Remove(entry.Name()); err != nil {
			return err
		}
	}
	return nil
}

func (d *execNotebookDirectory) sweep(ctx context.Context, deadline time.Duration) (execSweepOutcome, string) {
	if err := d.verify(); err != nil {
		return execSweepSkipped, err.Error()
	}
	file, err := d.root.Open(".")
	if err != nil {
		return execSweepSkipped, err.Error()
	}
	defer file.Close()
	physical, err := execMailboxPhysicalPath(file)
	if err != nil {
		return execSweepSkipped, err.Error()
	}
	sweepCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	command := exec.CommandContext(sweepCtx, "/bin/bash", "-c", execsweep.Script+`
cd -P -- "$1" || exit 1
[ "$(exec_storage_identity .)" = "$2" ] || exit 1
physical=$(pwd -P && printf '.') || exit 1
physical=${physical%$'\n.'}
exec_scan_deadline=$3
sweep_exec_storage_processes "$physical" "$physical" separate
printf '%s\n' "$exec_sweep_outcome"
`, "wefty-node-exec-resweep", physical, d.identity, execScanDeadlineSeconds(deadline))
	command.Dir = "/"
	command.WaitDelay = 100 * time.Millisecond
	var stdout, stderr bytes.Buffer
	command.Stdout, command.Stderr = &stdout, &stderr
	if err := command.Run(); err != nil {
		return execSweepSkipped, fmt.Sprintf("%v: %s", err, strings.TrimSpace(stderr.String()))
	}
	return execSweepOutcome(strings.TrimSpace(stdout.String())), strings.TrimSpace(stderr.String())
}

// This is called only at startup and on the existing hourly collector tick.
// Its lease has the same node scope as the notebook. Receipts live only in
// this node agent: a restart never turns unknown runtime state into proof.
func (m *handoffManager) collectExecSweeps(ctx context.Context, deadline time.Duration) error {
	if m.stateRoot != "" {
		guard, err := m.sharedLease("node-results-collection")
		if errors.Is(err, errSharedLeaseBusy) {
			return nil
		}
		if err != nil {
			return err
		}
		defer guard.Close()
	}
	m.collectMu.Lock()
	defer m.collectMu.Unlock()
	notebook, err := os.OpenRoot(m.execSweepNotebook())
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	defer notebook.Close()
	file, err := notebook.Open(".")
	if err != nil {
		return err
	}
	entries, err := file.ReadDir(-1)
	file.Close()
	if err != nil {
		return err
	}
	now := m.now()
	for _, dir := range entries {
		if err := ctx.Err(); err != nil {
			return err
		}
		if !dir.IsDir() {
			continue
		}
		if strings.HasSuffix(dir.Name(), ".tmp") {
			info, err := dir.Info()
			if err == nil && now.Sub(info.ModTime()) >= time.Hour {
				if err := notebook.RemoveAll(dir.Name()); err != nil {
					m.log("node agent: warning: remove stale exec sweep staging entry %s: %v", dir.Name(), err)
				}
			}
			continue
		}
		entryRoot, err := openHandoffDirectory(notebook, dir.Name())
		if err != nil {
			m.log("node agent: warning: read exec sweep entry %s: %v", dir.Name(), err)
			continue
		}
		raw, err := entryRoot.ReadFile("entry.json")
		entryRoot.Close()
		var entry execSweepEntry
		if err == nil {
			err = json.Unmarshal(raw, &entry)
		}
		if err == nil && (!filepath.IsAbs(entry.Source) || !filepath.IsAbs(entry.Allocation) || entry.OriginalStorage != entry.Source || filepath.Dir(entry.Source) != filepath.Dir(entry.Pending) || !strings.HasPrefix(filepath.Base(entry.Pending), ".wefty-sweep-pending-") || entry.CreatedAt <= 0 || entry.AttemptID == "" || entry.StorageIdentity == "" || entry.AllocationIdentity == "") {
			err = errors.New("invalid exec sweep entry")
		}
		if err == nil {
			err = m.collectExecSweepEntry(ctx, entry, now, deadline)
		}
		if errors.Is(err, errExecSweepAwaitReap) {
			continue
		}
		if err != nil {
			m.log("node agent: warning: exec sweep entry %s: %v", dir.Name(), err)
			continue
		}
		if err := notebook.RemoveAll(dir.Name()); err != nil {
			m.log("node agent: warning: remove exec sweep entry %s: %v", dir.Name(), err)
			continue
		}
		if now.Sub(time.Unix(entry.CreatedAt, 0)) >= 24*time.Hour {
			m.log("node agent: warning: giving up exec sweep entry %s after 24 hours", dir.Name())
		}
		m.mu.Lock()
		delete(m.execSweepReaped, entry.AttemptID)
		m.mu.Unlock()
	}
	return nil
}

var errExecSweepPending = errors.New("process cleanup remains pending")
var errExecSweepAwaitReap = errors.New("awaiting runtime reap proof")

func (m *handoffManager) collectExecSweepEntry(ctx context.Context, e execSweepEntry, now time.Time, deadline time.Duration) error {
	// Source must be checked before pending, including on the give-up path.
	source, err := openExecNotebookDirectory(e.Source, e.StorageIdentity)
	if err != nil {
		return err
	}
	if source != nil {
		defer source.close()
	}
	allocation, err := openExecNotebookDirectory(e.Allocation, e.AllocationIdentity)
	if err != nil {
		return err
	}
	if allocation != nil {
		defer allocation.close()
	}
	giveUp := now.Sub(time.Unix(e.CreatedAt, 0)) >= 24*time.Hour
	if source != nil && !giveUp {
		m.mu.Lock()
		reaped := m.execSweepReaped[e.AttemptID]
		m.mu.Unlock()
		if !reaped {
			return errExecSweepAwaitReap
		}
		// Recovery resumes the publication flow at its marker step. The
		// publisher may have died just after publishing, before protecting
		// the allocation. Protect it before the collector moves its owner.
		if allocation != nil {
			if err := allocation.verify(); err != nil {
				return err
			}
			marker, err := allocation.root.OpenFile("sweep-pending", os.O_WRONLY|os.O_CREATE, 0600)
			if err != nil {
				return err
			}
			if err := marker.Close(); err != nil {
				return err
			}
		}
		if err := source.verify(); err != nil {
			return err
		}
		if _, err := source.parent.Lstat(filepath.Base(e.Pending)); !os.IsNotExist(err) {
			return errors.New("pending exec storage name is occupied")
		}
		if err := source.root.Chmod(".", 0700); err != nil {
			return err
		}
		if err := source.verify(); err != nil {
			return err
		}
		if err := source.parent.Rename(source.name, filepath.Base(e.Pending)); err != nil {
			return err
		}
	}
	pending, err := openExecNotebookDirectory(e.Pending, e.StorageIdentity)
	if err != nil {
		return err
	}
	if pending != nil {
		defer pending.close()
	}
	if giveUp {
		if allocation != nil {
			if err := allocation.verify(); err != nil {
				return err
			}
			if err := allocation.root.Remove("sweep-pending"); err != nil && !os.IsNotExist(err) {
				return err
			}
		}
		// No scan and no signal here, even when the publishing wrapper is live.
		if source == nil && pending != nil {
			if err := pending.remove(ctx); err != nil {
				return err
			}
		}
		return nil
	}
	// Resolve the two parts separately. A crash after either deletion leaves
	// the other part discoverable from the same published entry.
	for _, part := range []*execNotebookDirectory{pending, allocation} {
		if part == nil {
			continue
		}
		outcome, diagnostics := part.sweep(ctx, deadline)
		if outcome != execSweepComplete {
			// Keep one warning per failed entry, including when the shared
			// sweep emitted several diagnostic lines.
			diagnostics = strings.ReplaceAll(diagnostics, "wefty: warning: ", "")
			diagnostics = strings.ReplaceAll(diagnostics, "\n", "; ")
			return fmt.Errorf("%w (%s): %s", errExecSweepPending, outcome, diagnostics)
		}
		if diagnostics != "" {
			m.log("node agent: %s", diagnostics)
		}
		if err := part.remove(ctx); err != nil {
			return err
		}
	}
	return nil
}
