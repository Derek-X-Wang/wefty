//go:build !linux

package main

import (
	"context"
	"io"
)

func maybeExecutePrivilegedLinuxSetup(context.Context, globalOptions, []string, io.Writer, io.Writer) (bool, error) {
	return false, nil
}
