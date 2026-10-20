//go:build !darwin && !linux

package local

import "os"

func ownedByUser(os.FileInfo, uint32) bool { return false }
func socketRefused(error) bool             { return false }
