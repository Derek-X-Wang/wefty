//go:build darwin

// Package hostboot reads the kernel's host boot identity, independently of
// the random session ID assigned to each agent start.
package hostboot

import (
	"fmt"
	"strings"

	"golang.org/x/sys/unix"
)

func Identity() (string, error) {
	// The boot UUID is stable across calendar-clock adjustments. No
	// kern.boottime read is required for proof or service admission.
	bootUUID, err := unix.Sysctl("kern.bootsessionuuid")
	if err != nil {
		return "", fmt.Errorf("read kern.bootsessionuuid: %w", err)
	}
	bootUUID = strings.ToLower(strings.TrimSpace(bootUUID))
	if !validUUID(bootUUID) {
		return "", fmt.Errorf("invalid kern.bootsessionuuid %q", bootUUID)
	}
	return "darwin:" + bootUUID, nil
}
