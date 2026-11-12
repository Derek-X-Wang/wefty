//go:build acceptance_short_storage_budget

package l1

import "time"

// The acceptance lane's Storage operation failure budget (#1634). The rule is
// the production rule: the same count, the same "count AND window" test, the
// same doubling pace. Only the durations are short, so a live lane can watch
// an operation spend its budget in about a minute instead of 10 to 15.
// Nothing but the serviceacceptance harness builds L1 with this tag, and the
// lane's receipt names it as a deviation.
const StorageOperationFailureWindow = 30 * time.Second

const (
	storageOperationFailureRetryBase    = 2 * time.Second
	storageOperationFailureRetryCeiling = 8 * time.Second
)
