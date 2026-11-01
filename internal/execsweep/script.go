// Package execsweep shares the exec storage sweep and retirement script.
package execsweep

import _ "embed"

// Script defines sweep_exec_storage_processes and retire_exec_storage.
// Callers choose whether to sweep only or retire storage as well.
//
//go:embed exec_cleanup.sh
var Script string
