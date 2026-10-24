package main

import (
	"fmt"
	"strings"
)

type targetIDKind string

const (
	targetRun  targetIDKind = "run"
	targetJob  targetIDKind = "job"
	targetSeat targetIDKind = "seat"
)

// Old Run/Job handles (including hyphenated IDs) remain readable. Every
// underscore-prefixed ID declares its kind and must never use that fallback.
func parseTargetID(id string, legacy targetIDKind) (targetIDKind, error) {
	if id == "" || strings.HasPrefix(id, "-") {
		return "", usageError("an ID is required")
	}
	prefix, _, typed := strings.Cut(id, "_")
	if !typed {
		if strings.HasPrefix(id, "run-") {
			return targetRun, nil
		}
		if strings.HasPrefix(id, "job-") {
			return targetJob, nil
		}
		return legacy, nil
	}
	switch targetIDKind(prefix) {
	case targetRun, targetJob, targetSeat:
		return targetIDKind(prefix), nil
	}
	return "", usageError(fmt.Sprintf("unknown ID kind %q in %q; expected run_, job_ or seat_", prefix, id))
}
