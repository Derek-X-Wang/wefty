// Package pathflags resolves the filesystem-path flags of the wefty daemons
// against the directory they were started in.
//
// A relative --db broke all three daemons with "invalid uri authority" (#604):
// the SQLite DSN is a file: URI, and a relative path's first segment reads as
// its authority. The store also created its authority-instance marker next to
// the relative path before the open failed, leaving a stray file. Resolving
// every path flag once, at startup and before anything touches the disk, makes
// a relative path mean what it says -- relative to where the daemon was
// started -- for the life of the process, whatever it later changes into.
package pathflags

import (
	"fmt"
	"path/filepath"
	"strings"
)

// Flag is one path-valued flag: its name, for the error, and its value.
type Flag struct {
	Name  string
	Value *string
}

// Absolutize rewrites each non-empty flag value to an absolute, cleaned path.
// An empty value stays empty: for these flags it means "not set" or "use the
// default", and inventing the working directory for it would change that.
func Absolutize(flags ...Flag) error {
	for _, flag := range flags {
		if flag.Value == nil || strings.TrimSpace(*flag.Value) == "" {
			continue
		}
		absolute, err := filepath.Abs(*flag.Value)
		if err != nil {
			return fmt.Errorf("resolve -%s=%s: %w", flag.Name, *flag.Value, err)
		}
		*flag.Value = absolute
	}
	return nil
}
