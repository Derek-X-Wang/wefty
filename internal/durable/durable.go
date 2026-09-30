// Package durable writes small state files so that an acknowledged write
// survives power loss or a kernel panic, not only a process crash.
//
// Every write stages the bytes under another name in the same directory,
// syncs them, publishes them with one rename or link, and then syncs the
// directory. After a crash the name holds either the old document or the new
// one, never an empty or half-written file, and once a call returns the new
// one is on stable storage. On darwin, os.File.Sync is F_FULLFSYNC, which also
// flushes the drive's cache.
package durable

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"syscall"
)

// StagingSuffix is appended to a name to form the fixed staging name
// WriteFile uses. A reader that lists the directory can skip it, and a
// leftover from a crash is replaced by the next write.
const StagingSuffix = ".tmp"

// beforePublish is a test seam. It runs after the staged bytes are synced and
// before they are published, so a test can prove a failure there leaves the
// previous document in place. Nothing outside a test sets it.
var beforePublish func() error

// syncDirectory is SyncDir, behind a seam so a test can prove every publish
// syncs its directory.
var syncDirectory = SyncDir

// WriteFile atomically and durably replaces name in dir with payload.
//
// The staging name is name+StagingSuffix. It is removed first rather than
// truncated, and created exclusively, so whatever stood there -- a leftover
// from a crash, or a link -- is replaced rather than written through. The
// rename replaces a link at name rather than following it.
func WriteFile(dir *os.Root, name string, payload []byte, mode os.FileMode) error {
	staging := name + StagingSuffix
	if err := dir.Remove(staging); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("clear staging file %q: %w", staging, err)
	}
	if err := writeStaged(dir, staging, payload, mode); err != nil {
		return err
	}
	if beforePublish != nil {
		if err := beforePublish(); err != nil {
			_ = dir.Remove(staging)
			return err
		}
	}
	if err := dir.Rename(staging, name); err != nil {
		_ = dir.Remove(staging)
		return err
	}
	return syncDirectory(dir)
}

// CreateFile durably publishes payload at name only if nothing stands there.
// It reports an error satisfying errors.Is(err, fs.ErrExist) when the name is
// taken, so of any number of racing callers exactly one wins, and the name
// never holds a partial document even across power loss: the bytes are synced
// under a unique staging name and published with a hard link, which never
// replaces.
func CreateFile(dir *os.Root, name string, payload []byte, mode os.FileMode) error {
	suffix := make([]byte, 8)
	if _, err := rand.Read(suffix); err != nil {
		return err
	}
	staging := name + StagingSuffix + "-" + hex.EncodeToString(suffix)
	if err := writeStaged(dir, staging, payload, mode); err != nil {
		return err
	}
	defer dir.Remove(staging)
	if beforePublish != nil {
		if err := beforePublish(); err != nil {
			return err
		}
	}
	if err := dir.Link(staging, name); err != nil {
		return err
	}
	if err := dir.Remove(staging); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	return syncDirectory(dir)
}

// durableDirectories memoizes, per process, the cleaned absolute paths of
// directories whose entry this process has made durable by syncing the
// parent after the directory existed. The first MkdirAll in a process syncs
// even a directory that already exists -- a crashed predecessor may have
// created it without syncing -- and later calls skip it. A pointer so tests
// can start from an empty memo.
var durableDirectories = &sync.Map{}

// MkdirAll makes base/rel exist with every entry at and below base durable.
//
// base is a configured root: a state, spool or handoff directory. Its own
// entry is synced in its parent once per process, and each component of rel
// is created if missing and has its parent synced once per process, whether
// it was just created or already there. Nothing above base's parent is ever
// opened or synced: base's missing ancestors are created as os.MkdirAll
// creates them, and their durability is the operator's configuration, not
// this helper's. rel must be local to base ("" or "." for base alone).
func MkdirAll(base, rel string, mode os.FileMode) error {
	if rel == "" {
		rel = "."
	}
	if !filepath.IsLocal(rel) {
		return fmt.Errorf("durable: %q is not a path below %q", rel, base)
	}
	base, err := filepath.Abs(base)
	if err != nil {
		return err
	}
	if err := ensureDurableDirectory(base, mode, true); err != nil {
		return err
	}
	current := base
	for _, component := range strings.Split(filepath.Clean(rel), string(filepath.Separator)) {
		if component == "." {
			continue
		}
		current = filepath.Join(current, component)
		if err := ensureDurableDirectory(current, mode, false); err != nil {
			return err
		}
	}
	return nil
}

// ensureDurableDirectory makes path exist as a directory and makes its entry
// durable, syncing its parent unless this process already did so for a
// directory that has existed since.
func ensureDurableDirectory(path string, mode os.FileMode, isBase bool) error {
	info, err := os.Stat(path)
	created := false
	switch {
	case err == nil:
		if !info.IsDir() {
			return &fs.PathError{Op: "mkdir", Path: path, Err: syscall.ENOTDIR}
		}
	case errors.Is(err, fs.ErrNotExist):
		if isBase {
			if err := os.MkdirAll(filepath.Dir(path), mode); err != nil {
				return err
			}
		}
		if err := os.Mkdir(path, mode); err != nil {
			// A racing creator made it; this caller still syncs its parent
			// below before relying on it.
			if info, statErr := os.Stat(path); !errors.Is(err, fs.ErrExist) || statErr != nil || !info.IsDir() {
				return err
			}
		}
		created = true
	default:
		return err
	}
	if !created {
		if _, done := durableDirectories.Load(path); done {
			return nil
		}
	}
	parent := filepath.Dir(path)
	if parent != path {
		if err := syncDirectoryPath(parent); err != nil {
			return err
		}
	}
	durableDirectories.Store(path, struct{}{})
	return nil
}

// Mkdir creates name in dir and syncs dir, so the entry is durable. It syncs
// dir when name already exists too -- an interrupted earlier creation may
// never have synced it -- and still returns os.Mkdir's error, fs.ErrExist
// included.
func Mkdir(dir *os.Root, name string, mode os.FileMode) error {
	mkdirErr := dir.Mkdir(name, mode)
	if mkdirErr != nil && !errors.Is(mkdirErr, fs.ErrExist) {
		return mkdirErr
	}
	if err := syncDirectory(dir); err != nil {
		return err
	}
	return mkdirErr
}

func syncDirectoryPath(path string) error {
	dir, err := os.OpenRoot(path)
	if err != nil {
		return err
	}
	defer dir.Close()
	return syncDirectory(dir)
}

// SyncDir syncs dir itself, which is what makes a rename, link or removal in
// it durable.
func SyncDir(dir *os.Root) error {
	handle, err := dir.Open(".")
	if err != nil {
		return err
	}
	syncErr := handle.Sync()
	return errors.Join(syncErr, handle.Close())
}

func writeStaged(dir *os.Root, staging string, payload []byte, mode os.FileMode) error {
	file, err := dir.OpenFile(staging, os.O_WRONLY|os.O_CREATE|os.O_EXCL, mode)
	if err != nil {
		return err
	}
	_, writeErr := file.Write(payload)
	if writeErr == nil {
		writeErr = file.Sync()
	}
	if writeErr = errors.Join(writeErr, file.Close()); writeErr != nil {
		_ = dir.Remove(staging)
		return writeErr
	}
	return nil
}

// SQLitePragmas are the durability pragmas every SQLite connection that
// writes must carry, as `_pragma` DSN values. They are per connection, so
// they belong in the DSN rather than in one Exec on one pooled connection.
//
// On darwin a plain fsync leaves the write in the drive's cache, where power
// loss can drop a commit SQLite already reported, so SQLite is told to use
// F_FULLFSYNC for commits and for checkpoints. Elsewhere fsync reaches stable
// storage and there is nothing to add.
func SQLitePragmas() []string {
	if sqliteFullFsyncDisabled() {
		return nil
	}
	return sqlitePragmas(runtime.GOOS)
}

func sqlitePragmas(goos string) []string {
	if goos == "darwin" {
		return []string{"fullfsync(1)", "checkpoint_fullfsync(1)"}
	}
	return nil
}
