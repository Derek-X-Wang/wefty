package hostboot

import (
	"crypto/sha256"
	"fmt"
)

// DigestIdentity identifies one kernel boot without exposing its raw identity.
// Both L1 and the node agent hash the exact string returned by Identity.
func DigestIdentity(identity string) string {
	if identity == "" {
		return ""
	}
	return fmt.Sprintf("sha256:%x", sha256.Sum256([]byte(identity)))
}
