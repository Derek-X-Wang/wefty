package hostboot

import (
	"errors"
	"syscall"
	"testing"
)

func TestIdentityKernelReader(t *testing.T) {
	value, err := Identity()
	if errors.Is(err, syscall.EPERM) {
		t.Skipf("NOT-RUN — sandbox denied host boot reader: %v", err)
	}
	if err != nil || !ValidIdentity(value) {
		t.Fatalf("kernel boot identity=%q err=%v", value, err)
	}
	again, err := Identity()
	if err != nil || again != value {
		t.Fatalf("kernel boot identity changed: %q -> %q err=%v", value, again, err)
	}
}

func TestIdentityValidation(t *testing.T) {
	for _, value := range []string{"", "darwin:0:0", "darwin:1:1000000", "darwin:-1:0", "darwin:1", "linux:garbage", "linux:00000000-0000-0000-0000-00000000000g"} {
		if ValidIdentity(value) {
			t.Fatalf("accepted malformed host identity %q", value)
		}
	}
	for _, value := range []string{"darwin:1:0:00000000-0000-0000-0000-000000000001", "linux:00000000-0000-0000-0000-000000000001"} {
		if !ValidIdentity(value) {
			t.Fatalf("rejected valid host identity %q", value)
		}
	}
	if SamePlatform("darwin:1:0:00000000-0000-0000-0000-000000000001", "linux:00000000-0000-0000-0000-000000000001") {
		t.Fatal("cross-platform identity is reboot evidence")
	}
}

func TestDarwinCalendarAdjustmentIsNotReboot(t *testing.T) {
	first := "darwin:1:0:00000000-0000-0000-0000-000000000001"
	adjusted := "darwin:2:0:00000000-0000-0000-0000-000000000001"
	rebooted := "darwin:2:0:00000000-0000-0000-0000-000000000002"
	if !SameBoot(first, adjusted) || SameBoot(first, rebooted) {
		t.Fatal("Darwin reboot proof confused clock change with boot UUID change")
	}
	if ValidIdentity("darwin:1:0") {
		t.Fatal("timestamp alone became Darwin reboot evidence")
	}
}

func TestDarwinUUIDOnlyIdentity(t *testing.T) {
	uuid := "darwin:00000000-0000-0000-0000-000000000001"
	old := "darwin:1:0:00000000-0000-0000-0000-000000000001"
	if !ValidIdentity(uuid) || !SameBoot(uuid, old) || !SameBoot(old, uuid) {
		t.Fatal("UUID-only identity lost compatibility with recorded Darwin boots")
	}
	if SameBoot(uuid, "darwin:00000000-0000-0000-0000-000000000002") {
		t.Fatal("different boot UUID accepted as same boot")
	}
}
