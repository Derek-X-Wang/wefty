package durable

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"slices"
	"strings"
	"sync"
	"testing"
)

func openRoot(t *testing.T) (*os.Root, string) {
	t.Helper()
	path := t.TempDir()
	root, err := os.OpenRoot(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { root.Close() })
	return root, path
}

func countDirectorySyncs(t *testing.T) *int {
	t.Helper()
	calls := new(int)
	previous := syncDirectory
	syncDirectory = func(dir *os.Root) error {
		*calls++
		return SyncDir(dir)
	}
	t.Cleanup(func() { syncDirectory = previous })
	return calls
}

func TestWriteFileReplacesAndSyncsTheDirectory(t *testing.T) {
	root, path := openRoot(t)
	syncs := countDirectorySyncs(t)
	if err := WriteFile(root, "state.json", []byte("old"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := WriteFile(root, "state.json", []byte("new"), 0o600); err != nil {
		t.Fatal(err)
	}
	payload, err := os.ReadFile(filepath.Join(path, "state.json"))
	if err != nil || string(payload) != "new" {
		t.Fatalf("state.json = %q, %v; want the new document", payload, err)
	}
	if *syncs != 2 {
		t.Fatalf("two publishes synced their directory %d times", *syncs)
	}
	info, err := os.Stat(filepath.Join(path, "state.json"))
	if err != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("state.json mode = %v, %v", info.Mode(), err)
	}
	assertOnly(t, path, "state.json")
}

// TestWriteFileFailingBeforePublishKeepsTheOldDocument simulates a crash after
// the new bytes are staged and before they are published: the name must still
// hold the whole previous document, never an empty or partial one.
func TestWriteFileFailingBeforePublishKeepsTheOldDocument(t *testing.T) {
	root, path := openRoot(t)
	if err := WriteFile(root, "state.json", []byte("old document"), 0o600); err != nil {
		t.Fatal(err)
	}
	crash := errors.New("simulated crash before rename")
	beforePublish = func() error { return crash }
	t.Cleanup(func() { beforePublish = nil })
	if err := WriteFile(root, "state.json", []byte("new document"), 0o600); !errors.Is(err, crash) {
		t.Fatalf("WriteFile = %v, want the simulated failure", err)
	}
	payload, err := os.ReadFile(filepath.Join(path, "state.json"))
	if err != nil || string(payload) != "old document" {
		t.Fatalf("state.json = %q, %v; want the old document intact", payload, err)
	}
	assertOnly(t, path, "state.json")
}

// TestWriteFileReplacesAStaleStagingFileAndALinkRatherThanWritingThrough: a
// crash can leave the staging name behind, and anything at the staging or the
// final name must be replaced, not written through.
func TestWriteFileReplacesAStaleStagingFileAndALinkRatherThanWritingThrough(t *testing.T) {
	root, path := openRoot(t)
	target := filepath.Join(t.TempDir(), "target")
	if err := os.WriteFile(target, []byte("untouched"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, filepath.Join(path, "state.json"+StagingSuffix)); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, filepath.Join(path, "state.json")); err != nil {
		t.Fatal(err)
	}
	if err := WriteFile(root, "state.json", []byte("new"), 0o600); err != nil {
		t.Fatal(err)
	}
	if payload, _ := os.ReadFile(target); string(payload) != "untouched" {
		t.Fatalf("a link's target was written through: %q", payload)
	}
	info, err := os.Lstat(filepath.Join(path, "state.json"))
	if err != nil || !info.Mode().IsRegular() {
		t.Fatalf("state.json is %v, %v; want a regular file", info.Mode(), err)
	}
	assertOnly(t, path, "state.json")
}

func TestCreateFileNeverReplaces(t *testing.T) {
	root, path := openRoot(t)
	syncs := countDirectorySyncs(t)
	if err := CreateFile(root, "identity", []byte("first"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := CreateFile(root, "identity", []byte("second"), 0o600); !errors.Is(err, fs.ErrExist) {
		t.Fatalf("a second create = %v, want fs.ErrExist", err)
	}
	payload, err := os.ReadFile(filepath.Join(path, "identity"))
	if err != nil || string(payload) != "first" {
		t.Fatalf("identity = %q, %v; want the first document", payload, err)
	}
	if *syncs != 1 {
		t.Fatalf("one publish synced its directory %d times", *syncs)
	}
	assertOnly(t, path, "identity")
}

func TestCreateFileFailingBeforePublishLeavesNothing(t *testing.T) {
	root, path := openRoot(t)
	crash := errors.New("simulated crash before link")
	beforePublish = func() error { return crash }
	t.Cleanup(func() { beforePublish = nil })
	if err := CreateFile(root, "identity", []byte("first"), 0o600); !errors.Is(err, crash) {
		t.Fatalf("CreateFile = %v, want the simulated failure", err)
	}
	assertOnly(t, path)
}

func TestCreateFileHasExactlyOneWinnerAmongRacers(t *testing.T) {
	root, path := openRoot(t)
	const racers = 16
	var wait sync.WaitGroup
	results := make([]error, racers)
	for index := range racers {
		wait.Go(func() {
			results[index] = CreateFile(root, "identity", []byte{byte('a' + index)}, 0o600)
		})
	}
	wait.Wait()
	winners := 0
	for index, err := range results {
		switch {
		case err == nil:
			winners++
		case !errors.Is(err, fs.ErrExist):
			t.Fatalf("racer %d: %v", index, err)
		}
	}
	if winners != 1 {
		t.Fatalf("%d racers won; want exactly one", winners)
	}
	assertOnly(t, path, "identity")
}

// TestMkdirAllSyncsTheParentOfEveryComponent: a directory entry is
// durable only once its parent is synced, and a later durable write inside a
// directory whose own entry power loss can drop is not durable at all.
// recordDirectorySyncs records the path of every directory SyncDir syncs, and
// starts the test from an empty per-process memo, as a fresh process would.
func recordDirectorySyncs(t *testing.T) *[]string {
	t.Helper()
	synced := new([]string)
	previous, previousMemo := syncDirectory, durableDirectories
	durableDirectories = &sync.Map{}
	syncDirectory = func(dir *os.Root) error {
		*synced = append(*synced, filepath.Clean(dir.Name()))
		return SyncDir(dir)
	}
	t.Cleanup(func() { syncDirectory, durableDirectories = previous, previousMemo })
	return synced
}

// TestMkdirAllMakesEveryEntryAtAndBelowBaseDurableOncePerProcess: base's
// own entry and every component below it has its parent synced -- whether
// it was created now or already existed, since a crashed predecessor may
// have created it without a sync -- once per process, and nothing above
// base's parent is ever opened.
func TestMkdirAllMakesEveryEntryAtAndBelowBaseDurableOncePerProcess(t *testing.T) {
	for _, start := range []string{"absent", "already there"} {
		t.Run(start, func(t *testing.T) {
			parent := t.TempDir()
			base := filepath.Join(parent, "state")
			if start == "already there" {
				if err := os.MkdirAll(filepath.Join(base, "a", "b"), 0o700); err != nil {
					t.Fatal(err)
				}
			}
			synced := recordDirectorySyncs(t)
			if err := MkdirAll(base, filepath.Join("a", "b"), 0o700); err != nil {
				t.Fatal(err)
			}
			if info, err := os.Stat(filepath.Join(base, "a", "b")); err != nil || !info.IsDir() {
				t.Fatalf("base/a/b = %v, %v", info, err)
			}
			want := []string{parent, base, filepath.Join(base, "a")}
			if !slices.Equal(*synced, want) {
				t.Fatalf("synced %v, want %v: base's entry once in its parent, then each component's parent", *synced, want)
			}
			*synced = nil
			if err := MkdirAll(base, filepath.Join("a", "b"), 0o700); err != nil {
				t.Fatal(err)
			}
			if err := MkdirAll(base, "", 0o700); err != nil {
				t.Fatal(err)
			}
			if len(*synced) != 0 {
				t.Fatalf("a second call in the same process synced %v, want nothing", *synced)
			}
		})
	}
}

// TestMkdirAllNeverOpensAboveBasesParent: base's missing ancestors are
// created as os.MkdirAll creates them, but nothing above base's parent is
// opened or synced.
func TestMkdirAllNeverOpensAboveBasesParent(t *testing.T) {
	top := t.TempDir()
	base := filepath.Join(top, "x", "y", "state")
	synced := recordDirectorySyncs(t)
	if err := MkdirAll(base, "records", 0o700); err != nil {
		t.Fatal(err)
	}
	for _, path := range *synced {
		if path != filepath.Dir(base) && path != base && !strings.HasPrefix(path, base+string(filepath.Separator)) {
			t.Fatalf("synced %q, above base's parent %q (all: %v)", path, filepath.Dir(base), *synced)
		}
	}
	if want := []string{filepath.Dir(base), base}; !slices.Equal(*synced, want) {
		t.Fatalf("synced %v, want %v", *synced, want)
	}
}

// TestMkdirAllSyncsADirectoryItRecreates: the memo says an entry was made
// durable, not that the directory is still there; one this process has to
// create again is synced again.
func TestMkdirAllSyncsADirectoryItRecreates(t *testing.T) {
	base := filepath.Join(t.TempDir(), "state")
	synced := recordDirectorySyncs(t)
	if err := MkdirAll(base, "records", 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(base, "records")); err != nil {
		t.Fatal(err)
	}
	*synced = nil
	if err := MkdirAll(base, "records", 0o700); err != nil {
		t.Fatal(err)
	}
	if want := []string{base}; !slices.Equal(*synced, want) {
		t.Fatalf("recreating records synced %v, want %v", *synced, want)
	}
}

func TestMkdirAllRefusesWhatIsNotADirectoryBelowBase(t *testing.T) {
	base := t.TempDir()
	recordDirectorySyncs(t)
	if err := MkdirAll(base, filepath.Join("..", "escape"), 0o700); err == nil {
		t.Fatal("MkdirAll accepted a path that leaves base")
	}
	if err := os.WriteFile(filepath.Join(base, "file"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := MkdirAll(base, filepath.Join("file", "below"), 0o700); err == nil {
		t.Fatal("MkdirAll created a directory below a file")
	}
}

func TestMkdirSyncsItsParent(t *testing.T) {
	root, path := openRoot(t)
	syncs := countDirectorySyncs(t)
	if err := Mkdir(root, "run", 0o700); err != nil {
		t.Fatal(err)
	}
	if *syncs != 1 {
		t.Fatalf("Mkdir synced its parent %d times, want once", *syncs)
	}
	if err := Mkdir(root, "run", 0o700); !errors.Is(err, fs.ErrExist) {
		t.Fatalf("a second Mkdir = %v, want fs.ErrExist", err)
	}
	if *syncs != 2 {
		t.Fatalf("Mkdir of an existing directory did not sync its parent: %d syncs, want 2", *syncs)
	}
	assertOnly(t, path, "run")
}

func TestSyncDirOnARealDirectory(t *testing.T) {
	root, _ := openRoot(t)
	if err := SyncDir(root); err != nil {
		t.Fatal(err)
	}
}

func TestSQLitePragmasFullFsyncOnlyOnDarwin(t *testing.T) {
	if got := sqlitePragmas("darwin"); !slices.Equal(got, []string{"fullfsync(1)", "checkpoint_fullfsync(1)"}) {
		t.Fatalf("darwin pragmas = %v", got)
	}
	if got := sqlitePragmas("linux"); len(got) != 0 {
		t.Fatalf("linux pragmas = %v, want none", got)
	}
}

func assertOnly(t *testing.T, path string, names ...string) {
	t.Helper()
	entries, err := os.ReadDir(path)
	if err != nil {
		t.Fatal(err)
	}
	var found []string
	for _, entry := range entries {
		found = append(found, entry.Name())
	}
	if !slices.Equal(found, names) {
		t.Fatalf("directory holds %v; want exactly %v (no staging leftovers)", found, names)
	}
}

func TestTheTestSwitchDropsOnlyTheSwitchablePragmas(t *testing.T) {
	DisableSQLiteFullFsyncForTests()
	t.Cleanup(EnableSQLiteFullFsyncForTests)
	if got := SQLitePragmas(); len(got) != 0 {
		t.Fatalf("disabled pragmas = %v, want none", got)
	}
	EnableSQLiteFullFsyncForTests()
	if got, want := SQLitePragmas(), sqlitePragmas(runtime.GOOS); !slices.Equal(got, want) {
		t.Fatalf("re-enabled pragmas = %v, want %v", got, want)
	}
}

// TestTheTestSwitchIsCalledOnlyFromTests: the full-fsync switch must never
// reach a production binary. Only testswitch.go, which declares it and nothing
// else, and _test.go files may name it -- so an init() anywhere else,
// durable.go included, fails here.
func TestTheTestSwitchIsCalledOnlyFromTests(t *testing.T) {
	moduleRoot, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(moduleRoot, "go.mod")); err != nil {
		t.Fatalf("module root %q not found: %v", moduleRoot, err)
	}
	declaration, err := filepath.Abs("testswitch.go")
	if err != nil {
		t.Fatal(err)
	}
	switchName := regexp.MustCompile(`FullFsync\w*ForTests`)
	var offenders []string
	err = filepath.WalkDir(moduleRoot, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			if name := entry.Name(); path != moduleRoot && (strings.HasPrefix(name, ".") || name == "vendor" || name == "node_modules") {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") || path == declaration {
			return nil
		}
		payload, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if switchName.Match(payload) {
			offenders = append(offenders, path)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(offenders) != 0 {
		t.Fatalf("non-test files name the full-fsync test switch: %v", offenders)
	}
}

// switchDisabledAtStart and pragmasAtStart are read by TestMain, after every
// init() in the package has run and before any test can touch the switch.
var (
	switchDisabledAtStart bool
	pragmasAtStart        []string
)

// TestMain records the switch before any test runs. It must never disable it:
// this package is where the default is proved.
func TestMain(main *testing.M) {
	switchDisabledAtStart = sqliteFullFsyncDisabled()
	pragmasAtStart = SQLitePragmas()
	os.Exit(main.Run())
}

// TestTheFullFsyncSwitchDefaultsToOn: a binary that never calls the switch
// opens SQLite with the production pragmas. It reads what TestMain recorded
// and resets nothing.
func TestTheFullFsyncSwitchDefaultsToOn(t *testing.T) {
	if switchDisabledAtStart {
		t.Fatal("the full-fsync switch was already disabled before any test ran")
	}
	if want := sqlitePragmas(runtime.GOOS); !slices.Equal(pragmasAtStart, want) {
		t.Fatalf("pragmas before any test = %v, want %v", pragmasAtStart, want)
	}
}
