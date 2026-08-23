//go:build linux

package ocihelper

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

func TestContainerdSeccompFixtureMatchesGuestGenerator(t *testing.T) {
	for _, architecture := range []string{"amd64", "arm64"} {
		t.Run(architecture, func(t *testing.T) {
			if runtime.GOARCH != architecture {
				t.Skipf("fixture requires linux/%s, running linux/%s", architecture, runtime.GOARCH)
			}
			spec, err := generateContainerdBaseline(context.Background(), architecture, "seccomp-oracle-"+architecture)
			if err != nil {
				t.Fatal(err)
			}
			spec.Process.Capabilities = explicitIsolationCapabilities()
			spec.Process.NoNewPrivileges = true
			profile, err := generateGuestSeccomp(GuestKernelFacts{
				Architecture: architecture, KernelRelease: "observed-by-linux-oracle",
			}, spec)
			if err != nil {
				t.Fatal(err)
			}
			actual := marshalIndentedJSON(t, profile)
			expected, err := os.ReadFile(filepath.Join("testdata", "containerd-v2.3.4", "seccomp-linux-"+architecture+".json"))
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(actual, expected) {
				t.Fatalf("real containerd %s seccomp generator differs from linux/%s fixture", ContainerdBaselineVersion, architecture)
			}
		})
	}
}

func TestPublicRuntimeSpecHandoffUsesRealGuestGenerator(t *testing.T) {
	if runtime.GOARCH != "amd64" && runtime.GOARCH != "arm64" {
		t.Skipf("wefty-v1 does not target linux/%s", runtime.GOARCH)
	}
	input := goldenRuntimeSpecInput(t, runtime.GOARCH)
	materializeGoldenMountSources(t, &input)
	document, err := BuildRuntimeSpec(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}
	defer document.Close()
	if err := document.RevalidateMounts(); err != nil {
		t.Fatal(err)
	}
	containerdSpec, err := document.ContainerdSpec()
	if err != nil {
		t.Fatal(err)
	}
	if containerdSpec.TypeUrl != containerdRuntimeSpecTypeURL || len(document.JSON()) == 0 {
		t.Fatal("public runtime-spec handoff did not retain canonical containerd bytes")
	}
}
