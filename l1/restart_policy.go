package l1

import "github.com/Derek-X-Wang/wefty/contract"

// restartableSpawnFailureCodes is deliberately owned by L1: agents report
// facts, while the durable control plane owns restart policy. Unknown codes
// and every code absent from this allowlist are terminal.
var restartableSpawnFailureCodes = map[contract.SpawnFailureCode]struct{}{
	contract.SpawnFailureStartupReadinessTimeout: {},
}

// IsRestartableSpawnFailure reports whether service policy may retry a coded
// pre-execution failure. It is fail-closed for unknown future codes.
func IsRestartableSpawnFailure(code contract.SpawnFailureCode) bool {
	_, ok := restartableSpawnFailureCodes[code]
	return ok
}
