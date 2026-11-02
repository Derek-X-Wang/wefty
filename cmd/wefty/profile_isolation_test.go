package main

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"testing"

	"github.com/Derek-X-Wang/wefty/fabric/local"
	"github.com/Derek-X-Wang/wefty/internal/connprofile"
	"github.com/Derek-X-Wang/wefty/internal/durable"
	"strings"
	"sync"
)

// packageTestDirectory is TestMain's temporary folder; it also holds what the
// package's tests share, such as the one CLI build. packageTestEnvironment is
// the environment before any test changed it.
var (
	packageTestDirectory   string
	packageTestSocket      string
	packageTestEnvironment []string
)

// productionSQLiteSync serializes withProductionSQLiteSync.
var productionSQLiteSync sync.Mutex

// withProductionSQLiteSync runs open with the SQLite durability pragmas a
// real binary uses. TestMain turns F_FULLFSYNC off for the package's own
// stores (#1035); the upgrade-recovery and migration fixtures open theirs
// through here, so they keep the production setting. L1 and the agent's log
// spool read the switch only while they build their DSNs, so it stays on for
// just this open; a parallel test that opens a store meanwhile gets the
// production setting too, which is only slower.
func withProductionSQLiteSync(open func()) {
	productionSQLiteSync.Lock()
	defer productionSQLiteSync.Unlock()
	durable.EnableSQLiteFullFsyncForTests()
	defer durable.DisableSQLiteFullFsyncForTests()
	open()
}

// TestMain keeps the owner's installed connection profile out of every test
// here. On a Mac where `wefty setup` ran, ~/.config/wefty/profile.json would
// otherwise fill endpoints and person identities into tests that pass none,
// and into every real binary they start. An empty profile means "built-in
// defaults"; a test about the profile names its own.
//
// It also opens the package's in-process SQLite stores without F_FULLFSYNC,
// as the l1, l3 and agent tests do: on darwin that sync is most of a store
// fixture's time, and only power loss could tell the difference. Real
// binaries the tests start are separate processes and keep the production
// DSN; withProductionSQLiteSync keeps it for the recovery fixtures.
func TestMain(main *testing.M) {
	if arguments, present := os.LookupEnv(diagnosticGuardArguments); present {
		if err := resolveDiagnosticFixtureEndpoints(arguments); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		os.Exit(0)
	}
	durable.DisableSQLiteFullFsyncForTests()
	directory, err := os.MkdirTemp("", "wefty-cli-profile-")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	packageTestDirectory = directory
	// Isolate every exec allocation and orphan sweep, including subprocesses.
	tempRoot := os.Getenv("WEFTY_EXEC_TEMP_ROOT")
	ownsTempRoot := tempRoot == ""
	if ownsTempRoot {
		var err error
		tempRoot, err = os.MkdirTemp("", "wefty-cli-exec-temp-")
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		if err := os.Setenv("WEFTY_EXEC_TEMP_ROOT", tempRoot); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	}
	packageTestEnvironment = os.Environ()
	path := filepath.Join(directory, connprofile.FileName)
	if err := os.WriteFile(path, []byte("{\"version\":1}\n"), 0o600); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if err := os.Setenv(connprofile.EnvironmentVariable, path); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	// Keep CLI subprocesses on an explicit temporary node configuration too,
	// while leaving HOME (and therefore Go's default cache locations) alone.
	if err := os.Setenv("WEFTY_NODE_CONFIG", filepath.Join(directory, "node.json")); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	socketDir, err := os.MkdirTemp(os.TempDir(), "ws-")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	packageTestSocket = filepath.Join(socketDir, "s")
	if err := os.Setenv(local.SocketEnvironment, packageTestSocket); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	filtered := packageTestEnvironment[:0]
	for _, entry := range packageTestEnvironment {
		if !strings.HasPrefix(entry, local.SocketEnvironment+"=") {
			filtered = append(filtered, entry)
		}
	}
	packageTestEnvironment = append(filtered, local.SocketEnvironment+"="+packageTestSocket)
	code := main.Run()
	if ownsTempRoot {
		_ = os.RemoveAll(tempRoot)
	}
	_ = os.RemoveAll(socketDir)
	_ = os.RemoveAll(directory)
	os.Exit(code)
}

// TestWithProductionSQLiteSyncTurnsTheDurabilityPragmasOnForOneOpen pins the
// switch, not the fixtures that call the helper: package stores get no
// durability pragmas, and inside withProductionSQLiteSync they are exactly
// the ones a real binary uses. It stays serial, so no parallel test holds
// the switch while it looks.
func TestWithProductionSQLiteSyncTurnsTheDurabilityPragmasOnForOneOpen(t *testing.T) {
	if got := durable.SQLitePragmas(); len(got) != 0 {
		t.Fatalf("package test stores carry durability pragmas %q", got)
	}
	var inside []string
	withProductionSQLiteSync(func() { inside = durable.SQLitePragmas() })
	var want []string
	if runtime.GOOS == "darwin" {
		want = []string{"fullfsync(1)", "checkpoint_fullfsync(1)"}
	}
	if !slices.Equal(inside, want) {
		t.Fatalf("recovery fixture pragmas = %q, want %q", inside, want)
	}
	if got := durable.SQLitePragmas(); len(got) != 0 {
		t.Fatalf("withProductionSQLiteSync left pragmas %q on", got)
	}
}
