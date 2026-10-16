// Package nodeload reads this machine's one-minute load for local admission.
package nodeload

import (
	"fmt"
	"math"
	"runtime"
	"strconv"
	"strings"
)

const Environment = "WEFTY_ONESHOT_LOAD_LIMIT"

func DefaultLimit() float64 { return 1.5 * float64(runtime.NumCPU()) }

func ValidateLimit(limit float64) error {
	if limit < 0 || math.IsNaN(limit) || math.IsInf(limit, 0) {
		return fmt.Errorf("one-shot load limit must be finite and nonnegative")
	}
	return nil
}

// ResolveLimit preserves an explicit zero. Environment overrides the profile.
func ResolveLimit(configured *float64, environment string) (float64, error) {
	limit := DefaultLimit()
	if configured != nil {
		limit = *configured
	}
	if strings.TrimSpace(environment) != "" {
		parsed, err := strconv.ParseFloat(environment, 64)
		if err != nil {
			return 0, fmt.Errorf("%s: %w", Environment, err)
		}
		limit = parsed
	}
	return limit, ValidateLimit(limit)
}

func Holding(occupied int, load, limit float64) bool {
	return occupied > 0 && limit > 0 && load >= limit
}
