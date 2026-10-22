//go:build !unix

package main

import (
	"github.com/Derek-X-Wang/wefty/internal/systemduser"
	"testing"
)

func linuxTestFiles(t *testing.T) (systemduser.FileSystem, string) {
	t.Helper()
	t.Skip("Linux root ownership fixtures require Unix metadata")
	return systemduser.FileSystem{}, ""
}
