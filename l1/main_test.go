package l1

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/internal/durable"
)

// TestMain opens every test store without F_FULLFSYNC, which makes this
// suite several times slower on darwin and proves nothing a test asserts: no
// test cuts power. Production DSNs always carry it; the pragma tests
// re-enable it and assert exactly that (#599).
func TestMain(main *testing.M) {
	durable.DisableSQLiteFullFsyncForTests()
	// Test-only slow-machine simulation; no production environment knob.
	if value := os.Getenv("WEFTY_TEST_READ_PAGE_CUTOFF"); value != "" {
		cutoff, err := time.ParseDuration(value)
		if err != nil || cutoff <= 0 || cutoff > readSnapshotPageSoftLimit {
			fmt.Fprintln(os.Stderr, "invalid WEFTY_TEST_READ_PAGE_CUTOFF:", value)
			os.Exit(2)
		}
		readSnapshotPageCutoff = cutoff
	}
	os.Exit(main.Run())
}
