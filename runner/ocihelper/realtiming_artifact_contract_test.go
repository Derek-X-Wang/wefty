package ocihelper_test

import (
	"fmt"
	"testing"
)

const (
	linuxXFCEComputerReference    = "ghcr.io/derek-x-wang/wefty-computer-reference"
	linuxWaylandComputerReference = "ghcr.io/derek-x-wang/wefty-computer-wayland-reference"
)

type nativeLinuxComputerArtifacts struct {
	Variant string

	GenericReference string
	GenericArchive   string

	SelectedReference string
	SelectedDigest    string
	SelectedArchive   string

	WaylandReference string
	WaylandDigest    string
	WaylandArchive   string
}

func validateNativeLinuxComputerArtifacts(artifacts nativeLinuxComputerArtifacts) error {
	if artifacts.SelectedReference == artifacts.GenericReference || artifacts.SelectedArchive == artifacts.GenericArchive {
		return fmt.Errorf("selected Computer artifact aliases generic OCI acceptance")
	}
	if artifacts.WaylandReference != linuxWaylandComputerReference ||
		artifacts.WaylandReference == artifacts.GenericReference || artifacts.WaylandArchive == artifacts.GenericArchive {
		return fmt.Errorf("Wayland reference Computer artifact is not separate from generic OCI acceptance")
	}
	switch artifacts.Variant {
	case "xfce":
		if artifacts.SelectedReference != linuxXFCEComputerReference {
			return fmt.Errorf("XFCE matrix row selected %q", artifacts.SelectedReference)
		}
		if artifacts.SelectedReference == artifacts.WaylandReference || artifacts.SelectedDigest == artifacts.WaylandDigest || artifacts.SelectedArchive == artifacts.WaylandArchive {
			return fmt.Errorf("XFCE and Wayland Computer artifacts are not separate")
		}
	case "wayland":
		if artifacts.SelectedReference != linuxWaylandComputerReference {
			return fmt.Errorf("Wayland matrix row selected %q", artifacts.SelectedReference)
		}
		if artifacts.SelectedReference != artifacts.WaylandReference || artifacts.SelectedDigest != artifacts.WaylandDigest || artifacts.SelectedArchive != artifacts.WaylandArchive {
			return fmt.Errorf("Wayland matrix selection does not name the published Wayland artifact")
		}
	default:
		return fmt.Errorf("unknown Computer matrix variant %q", artifacts.Variant)
	}
	return nil
}

func TestNativeLinuxComputerArtifactSeparationAcceptsBothMatrixVariants(t *testing.T) {
	genericReference := "ghcr.io/derek-x-wang/wefty-echo-service"
	genericArchive := "/release/echo.oci.tar"
	wayland := nativeLinuxComputerArtifacts{
		Variant: "wayland", GenericReference: genericReference, GenericArchive: genericArchive,
		SelectedReference: linuxWaylandComputerReference, SelectedDigest: "sha256:wayland", SelectedArchive: "/release/wayland.oci.tar",
		WaylandReference: linuxWaylandComputerReference, WaylandDigest: "sha256:wayland", WaylandArchive: "/release/wayland.oci.tar",
	}
	xfce := nativeLinuxComputerArtifacts{
		Variant: "xfce", GenericReference: genericReference, GenericArchive: genericArchive,
		SelectedReference: linuxXFCEComputerReference, SelectedDigest: "sha256:xfce", SelectedArchive: "/release/xfce.oci.tar",
		WaylandReference: linuxWaylandComputerReference, WaylandDigest: "sha256:wayland", WaylandArchive: "/release/wayland.oci.tar",
	}
	for _, artifacts := range []nativeLinuxComputerArtifacts{xfce, wayland} {
		t.Run(artifacts.Variant, func(t *testing.T) {
			if err := validateNativeLinuxComputerArtifacts(artifacts); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestNativeLinuxComputerArtifactSeparationRejectsAliasing(t *testing.T) {
	wayland := nativeLinuxComputerArtifacts{
		Variant: "wayland", GenericReference: "ghcr.io/derek-x-wang/wefty-echo-service", GenericArchive: "/release/echo.oci.tar",
		SelectedReference: linuxWaylandComputerReference, SelectedDigest: "sha256:wayland", SelectedArchive: "/release/wayland.oci.tar",
		WaylandReference: linuxWaylandComputerReference, WaylandDigest: "sha256:wayland", WaylandArchive: "/release/wayland.oci.tar",
	}
	xfce := nativeLinuxComputerArtifacts{
		Variant: "xfce", GenericReference: wayland.GenericReference, GenericArchive: wayland.GenericArchive,
		SelectedReference: linuxXFCEComputerReference, SelectedDigest: "sha256:xfce", SelectedArchive: "/release/xfce.oci.tar",
		WaylandReference: wayland.WaylandReference, WaylandDigest: wayland.WaylandDigest, WaylandArchive: wayland.WaylandArchive,
	}
	tests := map[string]struct {
		base   nativeLinuxComputerArtifacts
		mutate func(*nativeLinuxComputerArtifacts)
	}{
		"generic archive alias": {base: wayland, mutate: func(artifacts *nativeLinuxComputerArtifacts) { artifacts.SelectedArchive = artifacts.GenericArchive }},
		"wrong selected name": {base: wayland, mutate: func(artifacts *nativeLinuxComputerArtifacts) {
			artifacts.SelectedReference = linuxXFCEComputerReference
		}},
		"wayland alias mismatch": {base: wayland, mutate: func(artifacts *nativeLinuxComputerArtifacts) { artifacts.SelectedDigest = "sha256:different" }},
		"xfce digest alias":      {base: xfce, mutate: func(artifacts *nativeLinuxComputerArtifacts) { artifacts.SelectedDigest = artifacts.WaylandDigest }},
		"xfce archive alias":     {base: xfce, mutate: func(artifacts *nativeLinuxComputerArtifacts) { artifacts.SelectedArchive = artifacts.WaylandArchive }},
		"unknown variant":        {base: wayland, mutate: func(artifacts *nativeLinuxComputerArtifacts) { artifacts.Variant = "future" }},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			artifacts := test.base
			test.mutate(&artifacts)
			if err := validateNativeLinuxComputerArtifacts(artifacts); err == nil {
				t.Fatal("invalid artifact relationship was accepted")
			}
		})
	}
}
