//go:build !unix

package systemduser

import "io/fs"

func rootOwned(fs.FileInfo) bool { return false }
