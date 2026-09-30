package pathflags

import (
	"os"
	"path/filepath"
	"testing"
)

func TestAbsolutizeResolvesAgainstTheWorkingDirectory(t *testing.T) {
	working, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	relative := "state/l1.sqlite"
	dotted := "./state/../spool"
	absolute := filepath.Join(t.TempDir(), "l3.sqlite")
	empty := ""
	if err := Absolutize(
		Flag{Name: "db", Value: &relative},
		Flag{Name: "log-spool-dir", Value: &dotted},
		Flag{Name: "state-dir", Value: &absolute},
		Flag{Name: "handoff-root", Value: &empty},
		Flag{Name: "unset", Value: nil},
	); err != nil {
		t.Fatal(err)
	}
	if relative != filepath.Join(working, "state", "l1.sqlite") {
		t.Fatalf("relative = %q", relative)
	}
	if dotted != filepath.Join(working, "spool") {
		t.Fatalf("dotted = %q", dotted)
	}
	if !filepath.IsAbs(absolute) || filepath.Base(absolute) != "l3.sqlite" {
		t.Fatalf("absolute = %q", absolute)
	}
	if empty != "" {
		t.Fatalf("an unset path became %q", empty)
	}
}
