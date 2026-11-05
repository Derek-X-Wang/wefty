//go:build linux

package process

import (
	"encoding/binary"
	"fmt"
	"net"
	"reflect"
	"strings"
	"testing"
)

func TestReceivingListenerInodesMatchesTheBackendAddressAndWildcards(t *testing.T) {
	backend := serviceBackend{ip: net.ParseIP("127.0.0.1"), port: 8080}
	row := func(slot int, ip string, port int, state string, inode string) string {
		return fmt.Sprintf("%4d: %s:%04X 00000000:0000 %s 00000000:00000000 00:00000000 00000000  1000        0 %s 1 0000000000000000 100 0 0 10 0",
			slot, procNetHex(ip), port, state, inode)
	}
	header := "  sl  local_address rem_address   st tx_queue rx_queue tr tm->when retrnsmt   uid  timeout inode"
	table := strings.Join([]string{header,
		row(0, "127.0.0.1", 8080, "0A", "101"),        // the backend
		row(1, "0.0.0.0", 8080, "0A", "102"),          // IPv4 wildcard
		row(2, "10.0.0.5", 8080, "0A", "103"),         // another interface
		row(3, "127.0.0.1", 8081, "0A", "104"),        // another port
		row(4, "127.0.0.1", 8080, "01", "105"),        // established, not a listener
		row(5, "::", 8080, "0A", "106"),               // IPv6 wildcard, maybe dual-stack
		row(6, "::1", 8080, "0A", "107"),              // IPv6 loopback
		row(7, "::ffff:127.0.0.1", 8080, "0A", "108"), // v4-mapped backend
	}, "\n")
	inodes, err := receivingListenerInodes([]byte(table), backend)
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"101", "102", "106", "108"}; !reflect.DeepEqual(inodes, want) {
		t.Fatalf("receiving listener inodes = %v, want %v", inodes, want)
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
