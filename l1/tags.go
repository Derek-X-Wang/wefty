package l1

import (
	"slices"
	"strings"
)

// NormalizeTags trims and lowercases tags, removes empty values and
// duplicates, and returns a sorted canonical set.
func NormalizeTags(tags []string) []string {
	set := make(map[string]struct{}, len(tags))
	for _, tag := range tags {
		tag = strings.ToLower(strings.TrimSpace(tag))
		if tag != "" {
			set[tag] = struct{}{}
		}
	}
	normalized := make([]string, 0, len(set))
	for tag := range set {
		normalized = append(normalized, tag)
	}
	slices.Sort(normalized)
	return normalized
}

// TagsMatch reports whether every normalized job tag is present in the
// normalized authoritative node tag set. An untagged job matches every node.
func TagsMatch(jobTags, nodeTags []string) bool {
	nodeSet := make(map[string]struct{}, len(nodeTags))
	for _, tag := range NormalizeTags(nodeTags) {
		nodeSet[tag] = struct{}{}
	}
	for _, tag := range NormalizeTags(jobTags) {
		if _, ok := nodeSet[tag]; !ok {
			return false
		}
	}
	return true
}

func validTag(tag string) bool {
	if tag == "" {
		return false
	}
	for i, r := range tag {
		if r >= 'a' && r <= 'z' || r >= '0' && r <= '9' || i > 0 && strings.ContainsRune("._:-", r) {
			continue
		}
		return false
	}
	return true
}
