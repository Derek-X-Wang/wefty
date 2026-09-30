package durable

import "sync/atomic"

// This file holds the SQLite full-fsync test switch and nothing else, so the
// guard in durable_test.go can exclude exactly this file: any other non-test
// file that names the switch -- an init() in durable.go included -- fails it.

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
// fails if any non-test file other than this one names them.
func DisableSQLiteFullFsyncForTests() { sqliteFullFsyncDisabledForTests.Store(true) }

// EnableSQLiteFullFsyncForTests undoes DisableSQLiteFullFsyncForTests, for a
// test that must open a store exactly as production does.
func EnableSQLiteFullFsyncForTests() { sqliteFullFsyncDisabledForTests.Store(false) }

func sqliteFullFsyncDisabled() bool { return sqliteFullFsyncDisabledForTests.Load() }
