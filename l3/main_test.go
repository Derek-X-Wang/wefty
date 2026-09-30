package l3

import (
	"os"
	"testing"

	"github.com/Derek-X-Wang/wefty/internal/durable"
)

// TestMain opens every test store without F_FULLFSYNC, which makes this
// suite several times slower on darwin and proves nothing a test asserts: no
// test cuts power. Production DSNs always carry it; the pragma test turns it
// back on and asserts exactly that (#599). The L1 stores some tests open go
// through internal/durable's switch; L3's own stores through l3's copy.
func TestMain(main *testing.M) {
	sqliteFullFsyncOffInTests.Store(true)
	durable.DisableSQLiteFullFsyncForTests()
	os.Exit(main.Run())
}
