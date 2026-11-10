package ocihelper

import (
	"cmp"
	"slices"
	"strconv"
	"strings"
)

// storageCopyFailureCauseLimit bounds one logged cause. Filesystem tool
// output is part of some copy errors and has no length of its own.
const storageCopyFailureCauseLimit = 2048

// operatorPath is one host path the helper must not write to its log, and
// the placeholder that stands in for it.
type operatorPath struct {
	path        string
	placeholder string
}

// storageCopyFailureCause renders a failed Computer Storage copy's error for
// the helper log. The wire carries only the method and a closed reason, so
// this line is the only record of why a copy failed (#1128). Operator paths
// are replaced before the text is logged: no other helper log line shows the
// Custody path an operator named or the configured mount roots, so this one
// must not either. A path can appear raw or Go-quoted inside an error, and a
// longer path is replaced before any root it contains.
func storageCopyFailureCause(err error, paths []operatorPath) string {
	text := err.Error()
	type replacement struct{ old, new string }
	var replacements []replacement
	for _, path := range paths {
		if path.path == "" {
			continue
		}
		replacements = append(replacements, replacement{path.path, path.placeholder})
		if quoted := strconv.Quote(path.path); quoted[1:len(quoted)-1] != path.path {
			replacements = append(replacements, replacement{quoted[1 : len(quoted)-1], path.placeholder})
		}
	}
	slices.SortStableFunc(replacements, func(left, right replacement) int {
		return cmp.Compare(len(right.old), len(left.old))
	})
	for _, replacement := range replacements {
		text = strings.ReplaceAll(text, replacement.old, replacement.new)
	}
	if len(text) > storageCopyFailureCauseLimit {
		text = text[:storageCopyFailureCauseLimit] + "...(truncated)"
	}
	return text
}
