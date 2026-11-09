package l1

import (
	"strings"
	"sync"
	"testing"
	"time"
)

// testReadSnapshotHardLimit is the read-snapshot hold limit that l1 test
// fixtures give their Store. On a busy CI runner the 200 ms production limit can
// expire an ordinary read that a test does not examine (#1511, #1573). The other
// packages' in-process harnesses use the same value.
const testReadSnapshotHardLimit = 2 * time.Second

// productionReadBudgetTests holds the names of tests that opted into the
// production limit with keepProductionReadBudget.
var productionReadBudgetTests sync.Map

// openTestStore opens a Store for an l1 test. It gives the Store the generous
// testReadSnapshotHardLimit, unless the test or one of its parent tests called
// keepProductionReadBudget. Fixtures for the read door itself (snapshotStore,
// openRemovalWALStore) call OpenStore directly and keep the production limit.
func openTestStore(t testing.TB, path string, options StoreOptions) (*Store, error) {
	t.Helper()
	store, err := OpenStore(path, options)
	if err != nil || keepsProductionReadBudget(t) {
		return store, err
	}
	store.SetReadSnapshotHardLimitForTest(testReadSnapshotHardLimit)
	return store, nil
}

// keepProductionReadBudget makes every Store that openTestStore opens for this
// test and its subtests keep the 200 ms production hold limit. A test that
// asserts read-snapshot expiry, admission expiry, or WAL checkpoint progress
// behind a held reader calls it before it builds its fixture.
func keepProductionReadBudget(t testing.TB) {
	t.Helper()
	name := t.Name()
	productionReadBudgetTests.Store(name, true)
	t.Cleanup(func() { productionReadBudgetTests.Delete(name) })
}

func keepsProductionReadBudget(t testing.TB) bool {
	for name := t.Name(); ; {
		if _, ok := productionReadBudgetTests.Load(name); ok {
			return true
		}
		parent := strings.LastIndex(name, "/")
		if parent < 0 {
			return false
		}
		name = name[:parent]
	}
}

func TestOpenTestStoreReadBudget(t *testing.T) {
	open := func(t *testing.T) *Store {
		t.Helper()
		store, err := openTestStore(t, t.TempDir()+"/l1.sqlite", StoreOptions{})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { store.Close() })
		return store
	}
	t.Run("fixture", func(t *testing.T) {
		if got := open(t).snapshotHardLimit(t.Context()); got != testReadSnapshotHardLimit {
			t.Fatalf("fixture hard limit = %v, want %v", got, testReadSnapshotHardLimit)
		}
	})
	t.Run("production", func(t *testing.T) {
		keepProductionReadBudget(t)
		t.Run("subtest", func(t *testing.T) {
			if got := open(t).snapshotHardLimit(t.Context()); got != readSnapshotHardLimit {
				t.Fatalf("opted-out subtest hard limit = %v, want production %v", got, readSnapshotHardLimit)
			}
		})
	})
	t.Run("sibling", func(t *testing.T) {
		if got := open(t).snapshotHardLimit(t.Context()); got != testReadSnapshotHardLimit {
			t.Fatalf("a sibling's opt-out leaked: hard limit = %v", got)
		}
	})
}
