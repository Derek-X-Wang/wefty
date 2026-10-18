package main

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/Derek-X-Wang/wefty/internal/connprofile"
)

// packageTestDirectory is TestMain's temporary folder; it also holds what the
// package's tests share, such as the one CLI build. packageTestEnvironment is
// the environment before any test changed it.
var (
	packageTestDirectory   string
	packageTestEnvironment []string
)

// TestMain keeps the owner's installed connection profile out of every test
// here. On a Mac where `wefty setup` ran, ~/.config/wefty/profile.json would
// otherwise fill endpoints and person identities into tests that pass none,
// and into every real binary they start. An empty profile means "built-in
// defaults"; a test about the profile names its own.
func TestMain(main *testing.M) {
	packageTestEnvironment = os.Environ()
	directory, err := os.MkdirTemp("", "wefty-cli-profile-")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	packageTestDirectory = directory
	path := filepath.Join(directory, connprofile.FileName)
	if err := os.WriteFile(path, []byte("{\"version\":1}\n"), 0o600); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if err := os.Setenv(connprofile.EnvironmentVariable, path); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	// Keep CLI subprocesses on an explicit temporary node configuration too,
	// while leaving HOME (and therefore Go's default cache locations) alone.
	if err := os.Setenv("WEFTY_NODE_CONFIG", filepath.Join(directory, "node.json")); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	code := main.Run()
	_ = os.RemoveAll(directory)
	os.Exit(code)
}
