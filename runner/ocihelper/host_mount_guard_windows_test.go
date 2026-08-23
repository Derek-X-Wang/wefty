//go:build windows

package ocihelper

import "os"

func makeTestFIFO(path string) error { return os.WriteFile(path, nil, 0o600) }
