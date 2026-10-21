package serviceplan

import (
	"errors"
	"flag"
	"fmt"
	"io"

	"github.com/Derek-X-Wang/wefty/internal/connprofile"
)

// ProfilePreflight is a local, read-only binary compatibility gate. It never
// opens participant state, an authority database, or a network listener.
// Keep this before normal startup so unknown maintenance flags fail closed in
// older binaries, rather than starting an old service against a new profile.
func ProfilePreflight(args []string, out io.Writer) (bool, error) {
	if len(args) == 0 || args[0] != "--profile-preflight" {
		return false, nil
	}
	flags := flag.NewFlagSet("profile-preflight", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	check := flags.Bool("profile-preflight", false, "validate the active profile and installation role, then exit")
	path := flags.String("profile", "", "active profile")
	role := flags.String("role", "", "recorded installation role")
	if err := flags.Parse(args); err != nil {
		return true, err
	}
	if !*check || *path == "" || flags.NArg() != 0 || Role(*role) == Legacy {
		return true, errors.New("profile preflight requires --profile and a fleet --role")
	}
	profile, err := connprofile.Read(*path)
	if err != nil {
		return true, err
	}
	if err := ValidateProfile(Role(*role), profile); err != nil {
		return true, err
	}
	_, err = fmt.Fprintln(out, "profile-readable", *role)
	return true, err
}
