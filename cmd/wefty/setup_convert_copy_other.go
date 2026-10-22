//go:build !darwin

package main

import (
	"context"
	"os"
	"syscall"
)

func cloneConversionInventoryFile(context.Context, *os.File, string) error { return syscall.ENOTSUP }
