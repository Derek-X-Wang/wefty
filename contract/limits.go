package contract

import (
	"fmt"
	"math"
	"time"
)

// MaxLimitSeconds is the largest whole-second limit convertible to a Go
// time.Duration. Zero is unset; it never acquires a runtime cap.
const MaxLimitSeconds = math.MaxInt64 / int64(time.Second)

// ValidateLimitSeconds is shared by job admission, run admission, and the CLI.
// Validate before multiplication, which can otherwise wrap a positive limit.
func ValidateLimitSeconds(field string, seconds int) error {
	if seconds < 0 || int64(seconds) > MaxLimitSeconds {
		return fmt.Errorf("%s must be between 0 (unset) and %d seconds", field, MaxLimitSeconds)
	}
	return nil
}
