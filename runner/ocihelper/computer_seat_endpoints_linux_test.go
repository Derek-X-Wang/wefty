//go:build linux

package ocihelper

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func seatEndpointFixture(t *testing.T) (string, *net.UnixListener) {
	t.Helper()
	root, err := os.MkdirTemp("", "seat-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(root) })
	path := filepath.Join(root, seatEndpointPath(1, "view"))
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		t.Fatal(err)
	}
	l, err := net.ListenUnix("unix", &net.UnixAddr{Name: path, Net: "unix"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { l.Close() })
	if err := os.Chmod(path, 0600); err != nil {
		t.Fatal(err)
	}
	return root, l
}
func TestSeatSocketDialRefusesReplacementSymlinkAndNeighbourComputer(t *testing.T) {
	root, own := seatEndpointFixture(t)
	expected, err := openSeatEndpoint(root, 1, "view", uint32(os.Getuid()))
	if err != nil {
		t.Fatal(err)
	}
	defer expected.Close()
	if c, err := dialSeatSocket(context.Background(), root, 1, "view", uint32(os.Getuid()), expected); err != nil {
		t.Fatal(err)
	} else {
		c.Close()
	}
	neighbour, _ := seatEndpointFixture(t)
	if c, err := dialSeatSocket(t.Context(), neighbour, 1, "view", uint32(os.Getuid()), expected); err == nil {
		c.Close()
		t.Fatal("cross-Computer socket accepted")
	}
	path := filepath.Join(root, seatEndpointPath(1, "view"))
	own.SetUnlinkOnClose(false)
	own.Close()
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	replacement, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	defer replacement.Close()
	if err := os.Chmod(path, 0600); err != nil {
		t.Fatal(err)
	}
	if c, err := dialSeatSocket(t.Context(), root, 1, "view", uint32(os.Getuid()), expected); err == nil {
		c.Close()
		t.Fatal("replacement endpoint accepted")
	}
	replacement.Close()
	if err := os.Symlink(filepath.Join(neighbour, seatEndpointPath(1, "view")), path); err != nil {
		t.Fatal(err)
	}
	if _, err := openSeatEndpoint(root, 1, "view", uint32(os.Getuid())); err == nil {
		t.Fatal("endpoint symlink accepted")
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(filepath.Dir(path), filepath.Dir(path)+"-old"); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Dir(filepath.Join(neighbour, seatEndpointPath(1, "view"))), filepath.Dir(path)); err != nil {
		t.Fatal(err)
	}
	if _, err := openSeatEndpoint(root, 1, "view", uint32(os.Getuid())); err == nil {
		t.Fatal("parent symlink accepted")
	}
}
func TestSeatSocketRefusesOtherUserAndUnsafePermissions(t *testing.T) {
	root, _ := seatEndpointFixture(t)
	if _, err := openSeatEndpoint(root, 1, "view", uint32(os.Getuid()+1)); err == nil {
		t.Fatal("other Seat uid accepted")
	}
	path := filepath.Join(root, seatEndpointPath(1, "view"))
	if err := os.Chmod(path, 0666); err != nil {
		t.Fatal(err)
	}
	if _, err := openSeatEndpoint(root, 1, "view", uint32(os.Getuid())); err == nil {
		t.Fatal("public endpoint accepted")
	}
}

func TestSeatSocketReplacementDuringConnectNeverReachesNeighbour(t *testing.T) {
	root, _ := seatEndpointFixture(t)
	neighbour, listener := seatEndpointFixture(t)
	expected, err := openSeatEndpoint(root, 1, "view", uint32(os.Getuid()))
	if err != nil {
		t.Fatal(err)
	}
	defer expected.Close()
	path := filepath.Join(root, seatEndpointPath(1, "view"))
	connected := false
	c, err := dialSeatSocketWith(t.Context(), root, 1, "view", uint32(os.Getuid()), expected, func(ctx context.Context, address string) (net.Conn, error) {
		connected = true
		if err := os.Rename(path, path+".old"); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(filepath.Join(neighbour, seatEndpointPath(1, "view")), path); err != nil {
			t.Fatal(err)
		}
		return (&net.Dialer{}).DialContext(ctx, "unix", address)
	})
	if c != nil {
		c.Close()
	}
	if !connected || err == nil {
		t.Fatalf("replacement during connect was accepted: %v", err)
	}
	if err := listener.SetDeadline(time.Now()); err != nil {
		t.Fatal(err)
	}
	if c, err := listener.Accept(); err == nil {
		c.Close()
		t.Fatal("dial reached neighbour socket")
	}
}
