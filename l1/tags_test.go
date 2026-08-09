package l1

import (
	"slices"
	"testing"
)

func TestNormalizeTags(t *testing.T) {
	t.Parallel()
	got := NormalizeTags([]string{" Linux ", "ARM64", "linux", "", "  ", "arm64"})
	want := []string{"arm64", "linux"}
	if !slices.Equal(got, want) {
		t.Fatalf("NormalizeTags() = %v, want %v", got, want)
	}
}

func TestTagsMatchMatrix(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		job  []string
		node []string
		want bool
	}{
		{name: "empty job matches empty node", want: true},
		{name: "empty job matches tagged node", node: []string{"linux"}, want: true},
		{name: "exact", job: []string{"linux"}, node: []string{"linux"}, want: true},
		{name: "subset", job: []string{"linux"}, node: []string{"arm64", "linux"}, want: true},
		{name: "normalization and duplicates", job: []string{" Linux ", "linux", ""}, node: []string{"LINUX", "linux"}, want: true},
		{name: "missing", job: []string{"gpu"}, node: []string{"linux"}, want: false},
		{name: "partial subset", job: []string{"linux", "gpu"}, node: []string{"linux"}, want: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if got := TagsMatch(test.job, test.node); got != test.want {
				t.Fatalf("TagsMatch(%v, %v) = %v, want %v", test.job, test.node, got, test.want)
			}
		})
	}
}
