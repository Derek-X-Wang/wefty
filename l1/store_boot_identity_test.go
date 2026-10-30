package l1

import (
	"errors"
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/Derek-X-Wang/wefty/internal/hostboot"
)

func TestStoreSystemClockBootIdentity(t *testing.T) {
	// Read the real host independently so a regression that replaces the
	// system clock's strict reader with the test identity cannot pass.
	boot, hostErr := hostboot.Identity()
	path := filepath.Join(t.TempDir(), "l1.sqlite")
	store, err := OpenStore(path, StoreOptions{Clock: systemClock{}})
	if store != nil {
		t.Cleanup(func() { store.Close() })
	}
	if hostErr != nil {
		if store != nil || err == nil || err.Error() != "l1: read host boot identity at OpenStore: "+hostErr.Error() {
			t.Fatalf("OpenStore must refuse unavailable host identity: opened=%t err=%v hostErr=%v", store != nil, err, hostErr)
		}
		if _, statErr := os.Stat(path); !errors.Is(statErr, os.ErrNotExist) {
			t.Fatalf("OpenStore created a database without host identity: %v", statErr)
		}
		if errors.Is(hostErr, syscall.EPERM) || errors.Is(hostErr, syscall.EACCES) {
			t.Skipf("host boot-identity success path unavailable; OpenStore refusal verified: %v", hostErr)
		}
		t.Fatalf("read real host boot identity: %v", hostErr)
	}
	if err != nil {
		t.Fatal(err)
	}
	got, _, err := seatTime(store.clock)
	if err != nil || !hostboot.ValidIdentity(got) || got != boot {
		t.Fatalf("store host boot identity=%q, want %q: %v", got, boot, err)
	}
}
