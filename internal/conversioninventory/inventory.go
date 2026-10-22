// Package conversioninventory defines the private offline conversion inventory
// and the refusal rules shared with the live inventory. It grants no authority.
package conversioninventory

import "github.com/Derek-X-Wang/wefty/contract"

// Result is the version-1 JSON emitted by wefty-l1 --conversion-inventory.
// Blockers is always an array, including when empty; entries are sorted/unique.
type Result struct {
	Version  int      `json:"version"`
	Blockers []string `json:"blockers"`
}

// JobBlocker expects the effective GetJob projection: a service removal row
// supplies state and clears desired, regardless of the stored service intent.
func JobBlocker(id, class string, state contract.JobState, desired contract.ServiceDesiredState) string {
	if class == contract.JobClassService && (desired == contract.ServiceDesiredRunning || state == contract.JobStopping || state == contract.JobRemovalPending) {
		return "service job " + id
	}
	if class == contract.JobClassOneShot {
		switch state {
		case contract.JobQueued:
			return "queued job " + id
		case contract.JobClaimed:
			return "claimed job " + id
		}
	}
	return ""
}

func ComputerRetired(desired contract.ServiceDesiredState, outcome string, jobState contract.JobState) bool {
	return desired == contract.ServiceDesiredRemoved && outcome != "removal_pending" && jobState != contract.JobRemovalPending
}

func UnfinishedRunStates() []contract.RunState {
	return []contract.RunState{contract.RunPending, contract.RunDispatching, contract.RunQueued, contract.RunRunning, contract.RunAwaitingInput}
}
