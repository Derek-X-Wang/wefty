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
	"runtime"
	"sync/atomic"
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
	if sqliteFullFsyncDisabledForTests.Load() {
		return nil
	}
	return sqlitePragmas(runtime.GOOS)
}

// sqliteFullFsyncDisabledForTests is set only by
// DisableSQLiteFullFsyncForTests. No production code path sets it.
var sqliteFullFsyncDisabledForTests atomic.Bool

// DisableSQLiteFullFsyncForTests drops the durability pragmas from every
// SQLite DSN this process opens afterwards, and EnableSQLiteFullFsyncForTests
// puts them back. They exist for package tests' TestMain only: F_FULLFSYNC per
// commit makes the L1, L3 and agent suites several times slower on darwin and
// proves nothing a test asserts, since no test cuts power.
//
// They are Go calls, never an environment variable or flag, so nothing outside
// a compiled test can reach them, and the package is internal, so nothing
// outside this module can either. TestTheTestSwitchIsCalledOnlyFromTests
// fails if any non-test file calls them.
func DisableSQLiteFullFsyncForTests() { sqliteFullFsyncDisabledForTests.Store(true) }

// EnableSQLiteFullFsyncForTests undoes DisableSQLiteFullFsyncForTests, for a
// test that must open a store exactly as production does.
func EnableSQLiteFullFsyncForTests() { sqliteFullFsyncDisabledForTests.Store(false) }

func sqlitePragmas(goos string) []string {
	if goos == "darwin" {
		return []string{"fullfsync(1)", "checkpoint_fullfsync(1)"}
	}
	return nil
}
