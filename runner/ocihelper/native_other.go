//go:build !linux

package ocihelper

import (
	"errors"
	"io"
)

// OpenNativeEngine fails closed because the v1 native adapter is Linux-only.
func OpenNativeEngine(NativeEngineConfig) (Engine, io.Closer, error) {
	return nil, nil, errors.New("native OCI engine is available only on Linux")
}
