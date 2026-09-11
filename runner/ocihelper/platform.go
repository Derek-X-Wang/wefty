package ocihelper

import (
	"github.com/containerd/platforms"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
)

// NormalizePlatform reduces a platform to containerd's normal form: the single
// spelling containerd itself uses for a piece of hardware, where arm64 carries
// no variant and "v8"/"8"/"v8.0" all collapse onto it. Every platform the helper
// reports as image evidence is produced through this same normalization, because
// manifest selection runs through platforms.Normalize. Agent and helper compare
// platforms in this form and nowhere else; a second spelling on either side can
// never equal the evidence the other side produces.
func NormalizePlatform(platform OCIPlatform) OCIPlatform {
	normalized := platforms.Normalize(ocispec.Platform{
		OS: platform.OS, Architecture: platform.Architecture, Variant: platform.Variant,
	})
	return OCIPlatform{OS: normalized.OS, Architecture: normalized.Architecture, Variant: normalized.Variant}
}

// SamePlatform reports whether two platforms name the same hardware in
// containerd's normal form. Comparing normal forms also lets a platform recorded
// before this normalization — an "arm64/v8" binding pin in the ledger, say —
// still match the hardware that wrote it.
func SamePlatform(left, right OCIPlatform) bool {
	return NormalizePlatform(left) == NormalizePlatform(right)
}

// PlatformString renders a platform in containerd's normal form, so every key
// built from a platform is byte-identical for the same hardware.
func PlatformString(platform OCIPlatform) string {
	normalized := NormalizePlatform(platform)
	return platforms.Format(ocispec.Platform{
		OS: normalized.OS, Architecture: normalized.Architecture, Variant: normalized.Variant,
	})
}

// NormalizePlatformTriple reduces the loose (os, architecture, variant) triple
// that persisted rows and wire fields carry into containerd's normal form. It is
// the same reduction as NormalizePlatform, for callers that hold the three
// strings rather than an OCIPlatform.
func NormalizePlatformTriple(os, architecture, variant string) (string, string, string) {
	normalized := NormalizePlatform(OCIPlatform{OS: os, Architecture: architecture, Variant: variant})
	return normalized.OS, normalized.Architecture, normalized.Variant
}
