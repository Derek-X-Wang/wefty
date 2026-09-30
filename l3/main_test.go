package l3

import (
	"os"
	"testing"

	"github.com/Derek-X-Wang/wefty/internal/durable"
)

// TestMain opens every test store without F_FULLFSYNC, which makes this
// suite several times slower on darwin and proves nothing a test asserts: no
// test cuts power. Production DSNs always carry it; the pragma tests
// re-enable it and assert exactly that (#599).
func TestMain(main *testing.M) {
	durable.DisableSQLiteFullFsyncForTests()
	os.Exit(main.Run())
}
