//go:build service_acceptance_realtiming && (darwin || linux)

package serviceacceptance

import "time"

// The realtiming lane runs an L1 built with the short Storage operation
// failure budget (#1634), so the Linux Computer matrix can watch an operation
// spend its budget live. The rule and the count are the production ones; only
// the window and the retry pace are short. The matrix receipt records this as
// the deviation dev.short_storage_failure_budget.
const controlPlaneBuildTags = "acceptance_short_storage_budget"

// acceptanceStorageFailureWindow is the window that tag compiles into L1
// (l1/storage_operation_budget_acceptance.go). The matrix requires each ended
// operation to have failed over at least this long, so a drift between the
// two shows as a failed row, not as a silent pass.
const acceptanceStorageFailureWindow = 30 * time.Second
