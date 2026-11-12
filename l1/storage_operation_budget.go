//go:build !acceptance_short_storage_budget

package l1

import "time"

// StorageOperationFailureWindow is the time half of the Storage operation
// failure budget (#1608): an operation ends only after
// MaximumStorageOperationFailures counted failures AND this long since the
// first, on the L1 clock.
const StorageOperationFailureWindow = 10 * time.Minute

// storageOperationFailureRetryBase and storageOperationFailureRetryCeiling
// pace the next attempt of a failing Storage operation directive: the delay
// doubles with each counted failure, so an operation that cannot succeed ends
// 10 to 15 minutes after its first failure, after about seven calls.
const (
	storageOperationFailureRetryBase    = 15 * time.Second
	storageOperationFailureRetryCeiling = 5 * time.Minute
)
