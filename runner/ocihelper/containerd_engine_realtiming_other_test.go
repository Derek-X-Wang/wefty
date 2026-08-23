//go:build service_acceptance_realtiming && !linux

package ocihelper_test

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

func TestNativeLinuxOCIAdapterNotRunOnUnsupportedHost(t *testing.T) {
	reason := "NOT-RUN: native Linux OCI adapter requires Linux; Mac/Lima is owner-hardware acceptance\n"
	t.Logf("%s (%s/%s)", reason[:len(reason)-1], runtime.GOOS, runtime.GOARCH)
	if directory := os.Getenv("WEFTY_REALTIME_EVIDENCE_DIR"); directory != "" {
		if err := os.WriteFile(filepath.Join(directory, "native-linux-oci-NOT-RUN.txt"), []byte(reason), 0o600); err != nil {
			t.Fatal(err)
		}
	}
}
