//go:build linux

package hostboot

import (
	"fmt"
	"os"
	"strings"
)

// boot_id changes on reboot and does not depend on wall-clock corrections.
// A missing or unreadable procfs is an error; there is no weaker fallback.
func Identity() (string, error) {
	data, err := os.ReadFile("/proc/sys/kernel/random/boot_id")
	if err != nil {
		return "", fmt.Errorf("read host boot_id: %w", err)
	}
	value := strings.TrimSpace(string(data))
	if !validUUID(value) {
		return "", fmt.Errorf("invalid host boot_id %q", value)
	}
	return "linux:" + value, nil
}
