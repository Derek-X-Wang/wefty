//go:build !unix

package agent

import "os"

// inodeOf has no portable equivalent outside unix; every name is charged once
// there, which is the safe direction for a budget.
func inodeOf(os.FileInfo) (inodeIdentity, uint64, bool) { return inodeIdentity{}, 0, false }
