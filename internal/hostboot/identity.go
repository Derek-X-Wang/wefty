package hostboot

import (
	"strconv"
	"strings"
)

func validUUID(id string) bool {
	if len(id) != 36 {
		return false
	}
	for i, c := range id {
		if i == 8 || i == 13 || i == 18 || i == 23 {
			if c != '-' {
				return false
			}
			continue
		}
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return false
		}
	}
	return true
}

// ValidIdentity rejects missing, malformed, or cross-platform reboot evidence.
// Darwin uses a stable boot session UUID; old timestamp-plus-UUID records
// remain valid and compare by UUID only.
func ValidIdentity(value string) bool {
	parts := strings.Split(value, ":")
	if len(parts) == 2 && (parts[0] == "linux" || parts[0] == "darwin") {
		return validUUID(parts[1])
	}
	if len(parts) == 4 && parts[0] == "darwin" {
		seconds, err := strconv.ParseUint(parts[1], 10, 64)
		if err != nil || seconds == 0 {
			return false
		}
		micros, err := strconv.ParseUint(parts[2], 10, 64)
		return err == nil && micros < 1000000 && validUUID(parts[3])
	}
	return false
}

func SamePlatform(left, right string) bool {
	return strings.Split(left, ":")[0] == strings.Split(right, ":")[0]
}

// SameBoot deliberately ignores the Darwin wall-clock boot timestamp. XNU
// adjusts kern.boottime when the calendar clock is set; that is not a reboot.
func SameBoot(left, right string) bool {
	if !ValidIdentity(left) || !ValidIdentity(right) || !SamePlatform(left, right) {
		return false
	}
	l, r := strings.Split(left, ":"), strings.Split(right, ":")
	if l[0] == "darwin" {
		return l[len(l)-1] == r[len(r)-1]
	}
	return l[1] == r[1]
}
