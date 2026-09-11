package ocihelper

import "testing"

// The helper reports every image evidence platform in containerd's normal form,
// so that is the only form agent and helper can compare in. arm64 hardware is
// where a second spelling hurts: containerd drops the "v8" variant, and a side
// that keeps it can never equal the other's evidence.
func TestNormalizePlatformIsContainerdNormalForm(t *testing.T) {
	for _, test := range []struct {
		name  string
		input OCIPlatform
		want  OCIPlatform
		key   string
	}{
		{name: "arm64 hardware evidence", input: OCIPlatform{OS: "linux", Architecture: "arm64"},
			want: OCIPlatform{OS: "linux", Architecture: "arm64"}, key: "linux/arm64"},
		{name: "arm64 spelled with the v8 variant", input: OCIPlatform{OS: "linux", Architecture: "arm64", Variant: "v8"},
			want: OCIPlatform{OS: "linux", Architecture: "arm64"}, key: "linux/arm64"},
		{name: "arm64 v9 stays distinct", input: OCIPlatform{OS: "linux", Architecture: "arm64", Variant: "v9"},
			want: OCIPlatform{OS: "linux", Architecture: "arm64", Variant: "v9"}, key: "linux/arm64/v9"},
		{name: "amd64 identity", input: OCIPlatform{OS: "linux", Architecture: "amd64"},
			want: OCIPlatform{OS: "linux", Architecture: "amd64"}, key: "linux/amd64"},
		{name: "aarch64 alias", input: OCIPlatform{OS: "linux", Architecture: "aarch64"},
			want: OCIPlatform{OS: "linux", Architecture: "arm64"}, key: "linux/arm64"},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := NormalizePlatform(test.input); got != test.want {
				t.Fatalf("NormalizePlatform(%+v) = %+v, want %+v", test.input, got, test.want)
			}
			if got := PlatformString(test.input); got != test.key {
				t.Fatalf("PlatformString(%+v) = %q, want %q", test.input, got, test.key)
			}
		})
	}
}

func TestSamePlatformComparesNormalFormsOnly(t *testing.T) {
	hardware := OCIPlatform{OS: "linux", Architecture: "arm64"}
	// A platform recorded before this normalization — a binding pin the agent
	// wrote as "arm64/v8" — still names the hardware that wrote it.
	if !SamePlatform(OCIPlatform{OS: "linux", Architecture: "arm64", Variant: "v8"}, hardware) {
		t.Fatal("an arm64 v8 spelling did not match the arm64 hardware it names")
	}
	if SamePlatform(OCIPlatform{OS: "linux", Architecture: "arm64", Variant: "v9"}, hardware) {
		t.Fatal("arm64 v9 matched plain arm64")
	}
	if SamePlatform(OCIPlatform{OS: "linux", Architecture: "amd64"}, hardware) {
		t.Fatal("amd64 matched arm64")
	}
}
