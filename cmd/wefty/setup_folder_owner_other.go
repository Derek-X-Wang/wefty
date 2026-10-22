//go:build !unix

package main

import "io/fs"

func setupFolderOwnedBy(fs.FileInfo, int) bool { return false }
