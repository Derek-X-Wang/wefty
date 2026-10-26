package computerconformance_test

import (
	"fmt"
	"strconv"
	"strings"
	"testing"
)

func TestSeatScreenTCPListenerCheck(t *testing.T) {
	for _, tc := range []struct {
		name, rows string
		refused    bool
	}{
		{"IPv4 Seat listener", "0: 0100007F:170C 00000000:0000 0A 00000000:00000000 00:00000000 00000000 2001 0 42", true},
		{"IPv6 Seat listener", "0: 00000000000000000000000001000000:170C 00000000000000000000000000000000:0000 0A 00000000:00000000 00:00000000 00000000 2001 0 42", true},
		{"other user listener", "0: 00000000:170C 00000000:0000 0A 00000000:00000000 00:00000000 00000000 0 0 42", false},
		{"Seat established", "0: 0100007F:170C 0100007F:170D 01 00000000:00000000 00:00000000 00000000 2001 0 42", false},
		{"malformed table", "broken", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := checkSeatScreenTCPListeners([]byte("  sl local_address rem_address st tx_queue rx_queue tr tm->when retrnsmt uid timeout inode\n"+tc.rows+"\n"), 2001)
			if (err != nil) != tc.refused {
				t.Fatalf("TCP listener check: %v", err)
			}
		})
	}
}

// /proc/PID/net is the Computer's network namespace, not the host namespace.
func checkSeatScreenTCPListeners(payload []byte, uid uint32) error {
	lines := strings.Split(strings.TrimSpace(string(payload)), "\n")
	if len(lines) == 0 || !strings.Contains(lines[0], "local_address") {
		return fmt.Errorf("invalid TCP table header")
	}
	for _, line := range lines[1:] {
		fields := strings.Fields(line)
		if len(fields) == 0 {
			continue
		}
		if len(fields) < 10 {
			return fmt.Errorf("invalid TCP table row")
		}
		owner, err := strconv.ParseUint(fields[7], 10, 32)
		if err != nil {
			return fmt.Errorf("invalid TCP table uid: %w", err)
		}
		if uint32(owner) == uid && fields[3] == "0A" {
			return fmt.Errorf("Seat uid %d owns TCP listener %s", uid, fields[1])
		}
	}
	return nil
}
