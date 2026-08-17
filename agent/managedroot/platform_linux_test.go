//go:build linux

package managedroot

import (
	"testing"

	"golang.org/x/sys/unix"
)

func TestLinuxMountIdentityDistinguishesNestedMount(t *testing.T) {
	rootFD, err := unix.Open("/", unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer unix.Close(rootFD)
	rootIdentity, err := platformMountIdentityFD(rootFD)
	if err != nil {
		t.Fatal(err)
	}
	procIdentity, err := platformMountIdentityAt(rootFD, "proc")
	if err != nil {
		t.Fatal(err)
	}
	if rootIdentity == procIdentity {
		t.Fatalf("root and /proc mount identities are both %#v", rootIdentity)
	}
}
