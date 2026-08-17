//go:build darwin

package managedroot

import (
	"testing"

	"golang.org/x/sys/unix"
)

func TestDarwinMountIdentityDistinguishesNestedMount(t *testing.T) {
	rootFD, err := unix.Open("/", unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer unix.Close(rootFD)
	rootIdentity, err := platformMountIdentityFD(rootFD)
	if err != nil {
		t.Fatal(err)
	}
	devIdentity, err := platformMountIdentityAt(rootFD, "dev")
	if err != nil {
		t.Fatal(err)
	}
	if rootIdentity == devIdentity {
		t.Fatalf("root and /dev mount identities are both %#v", rootIdentity)
	}
}
