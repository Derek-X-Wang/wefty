// Package dogfood is the dogfood workflow's npm lockfile, read from Go.
//
// workflows/dogfood is where wefty's TypeScript build toolchain is pinned and
// proven: Dependabot bumps it here, and the dogfood-workflow CI job installs,
// lints, builds and runs it. `wefty workflow init --lang ts` pins the same
// toolchain, so it derives its scaffold's package.json and package-lock.json
// from this lockfile instead of keeping a copy that every bump had to edit by
// hand (#952).
package dogfood

import _ "embed"

//go:embed package-lock.json
var packageLock []byte

// PackageLock returns workflows/dogfood/package-lock.json as it was when the
// binary was built. The caller gets its own copy.
func PackageLock() []byte { return append([]byte(nil), packageLock...) }
