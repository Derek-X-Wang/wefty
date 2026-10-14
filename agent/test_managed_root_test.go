//go:build darwin || linux

package agent

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func agentTestFakeHomeGoPaths(home string) []string {
	return []string{
		filepath.Join(home, "go"),
		filepath.Join(home, "Library", "Caches", "go-build"),
		filepath.Join(home, ".cache", "go-build"),
	}
}

func TestAgentGoEnvironmentSurvivesHomeIsolation(t *testing.T) {
	// Resolve through the actual child Go command, as each helper build does.
	output, err := exec.Command("go", "env", "-json", "GOCACHE", "GOMODCACHE", "GOPATH", "GOENV").CombinedOutput()
	if err != nil {
		t.Fatalf("resolve isolated Go environment: %v\n%s", err, output)
	}
	var resolved map[string]string
	if err := json.Unmarshal(output, &resolved); err != nil {
		t.Fatal(err)
	}
	for key, original := range agentTestGoEnvironment {
		if got := resolved[key]; got != original {
			t.Errorf("%s changed after HOME isolation: got %q, want %q", key, got, original)
		}
	}
	for _, path := range agentTestFakeHomeGoPaths(os.Getenv("HOME")) {
		if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
			t.Errorf("Go state appeared under fake HOME at %q (stat: %v)", path, err)
		} else {
			t.Logf("absent: %s", path)
		}
	}
}

// requireTestManagedRoot checks the final flag value, as the real flag parser
// does for --name=value arguments (managed-root also accepts a separate value).
// Reject other bare flags so their values cannot masquerade as a root flag.
// The caller must name its disposable root; HOME isolation in TestMain
// provides a second line of defense if a new launcher bypasses this helper.
func requireTestManagedRoot(arguments []string) error {
	var root string
	for index := 0; index < len(arguments); index++ {
		argument := arguments[index]
		if argument == "--" {
			break
		}
		if !strings.HasPrefix(argument, "-") {
			return fmt.Errorf("test agent flags must use --name=value; got %q", argument)
		}
		flag := strings.TrimLeft(argument, "-")
		if strings.HasPrefix(flag, "managed-root=") {
			root = strings.TrimPrefix(flag, "managed-root=")
		} else if flag == "managed-root" && index+1 < len(arguments) {
			index++
			root = arguments[index]
		} else if !strings.Contains(flag, "=") {
			return fmt.Errorf("test agent flags must use --name=value; got %q", argument)
		}
	}
	if !filepath.IsAbs(root) {
		return fmt.Errorf("test agent requires an explicit absolute temporary --managed-root; got %q", root)
	}
	defaultRoot, err := DefaultManagedRootDirectory()
	if err != nil {
		return err
	}
	relative, err := filepath.Rel(defaultRoot, root)
	if err != nil {
		return err
	}
	if relative != ".." && !strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
		return fmt.Errorf("test agent --managed-root %q must be outside the default managed root %q", root, defaultRoot)
	}
	return nil
}

// Resolve macOS's temporary-directory aliases: managedroot refuses symlinks.
func temporaryManagedRoot(t *testing.T) string {
	t.Helper()
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	return root
}

func TestAgentLaunchRequiresManagedRoot(t *testing.T) {
	root := temporaryManagedRoot(t)
	defaultRoot, err := DefaultManagedRootDirectory()
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name      string
		args      []string
		wantError bool
	}{
		{"missing", nil, true},
		{"empty", []string{"--managed-root="}, true},
		{"relative", []string{"--managed-root=managed"}, true},
		{"temporary", []string{"--managed-root=" + root}, false},
		{"separate value", []string{"--managed-root", root}, false},
		{"single dash", []string{"-managed-root=" + root}, false},
		{"last value wins", []string{"--managed-root=" + root, "--managed-root="}, true},
		{"another flag cannot consume the root", []string{"--control-plane", "--managed-root=" + root}, true},
		{"after flags end", []string{"--", "--managed-root=" + root}, true},
		{"default", []string{"--managed-root=" + defaultRoot}, true},
		{"default child", []string{"--managed-root=" + filepath.Join(defaultRoot, "child")}, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			if err := requireTestManagedRoot(test.args); (err != nil) != test.wantError {
				t.Fatalf("requireTestManagedRoot(%q) = %v, wantError=%t", test.args, err, test.wantError)
			}
		})
	}
}
