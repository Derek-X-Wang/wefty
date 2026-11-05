//go:build linux

package process

import (
	"context"
	"encoding/binary"
	"fmt"
	"io/fs"
	"net"
	"reflect"
	"strings"
	"syscall"
	"testing"
)

func TestPortListenersReadsListenRowsAndTheirAddresses(t *testing.T) {
	row := func(slot int, ip string, port int, state string, inode string) string {
		return fmt.Sprintf("%4d: %s:%04X 00000000:0000 %s 00000000:00000000 00:00000000 00000000  1000        0 %s 1 0000000000000000 100 0 0 10 0",
			slot, procNetHex(ip), port, state, inode)
	}
	header := "  sl  local_address rem_address   st tx_queue rx_queue tr tm->when retrnsmt   uid  timeout inode"
	table := strings.Join([]string{header,
		row(0, "127.0.0.1", 8080, "0A", "101"),
		row(1, "0.0.0.0", 8080, "0A", "102"),
		row(2, "10.0.0.5", 8080, "0A", "103"),
		row(3, "127.0.0.1", 8081, "0A", "104"), // another port
		row(4, "127.0.0.1", 8080, "01", "105"), // established, not a listener
		row(5, "::", 8080, "0A", "106"),
		row(6, "::1", 8080, "0A", "107"),
		row(7, "::ffff:127.0.0.1", 8080, "0A", "108"),
	}, "\n")
	listeners, err := portListeners([]byte(table), 8080)
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, listener := range listeners {
		got = append(got, listener.ip.String()+"="+listener.inode)
	}
	want := []string{"127.0.0.1=101", "0.0.0.0=102", "10.0.0.5=103", "::=106", "::1=107", "127.0.0.1=108"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("listeners = %v, want %v", got, want)
	}
	backend := serviceBackend{ip: net.ParseIP("127.0.0.1"), port: 8080}
	var receiving []string
	for _, listener := range backend.receiving(procServiceListeners(listeners, nil, nil)) {
		receiving = append(receiving, listener.ip.String())
	}
	// The exact backend address takes every connection, so wildcards and
	// other addresses do not count.
	if want := []string{"127.0.0.1", "127.0.0.1"}; !reflect.DeepEqual(receiving, want) {
		t.Fatalf("receiving = %v, want %v", receiving, want)
	}
}

func TestUnreadableDescriptorLinkMakesTheListenerInconclusive(t *testing.T) {
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	backend := mustServiceBackend(t, listener.Addr().String())
	prior := readProcLink
	t.Cleanup(func() { readProcLink = prior })
	// Linux checks ptrace access per descriptor link, so a member whose fd
	// directory lists can still refuse each link.
	readProcLink = func(string) (string, error) {
		return "", &fs.PathError{Op: "readlink", Path: "fd", Err: syscall.EACCES}
	}
	verdict, err := inspectServiceListeners(context.Background(), backend, syscall.Getpgrp())
	if err == nil {
		t.Fatalf("verdict with unreadable links = %d, want an inconclusive error, never foreign", verdict)
	}
}

// procNetHex encodes ip the way the kernel prints it: /proc/net/tcp for an
// IPv4 text, /proc/net/tcp6 (16 bytes, even when v4-mapped) for an IPv6 one.
func procNetHex(text string) string {
	ip := net.ParseIP(text)
	raw := []byte(ip.To16())
	if !strings.Contains(text, ":") {
		raw = ip.To4()
	}
	var encoded strings.Builder
	for offset := 0; offset < len(raw); offset += 4 {
		fmt.Fprintf(&encoded, "%08X", binary.NativeEndian.Uint32(raw[offset:]))
	}
	return encoded.String()
}
