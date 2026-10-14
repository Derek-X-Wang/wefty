package scripts

import (
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// Check launch commands, including continued lines, instead of build commands
// or references to the binary. Keep acceptance and quickstart commands safe to paste.
func TestAcceptanceAgentCommandsNameManagedRoot(t *testing.T) {
	launch := regexp.MustCompile(`^\s*(?:exec\s+)?(?:"[^"\n]*/wefty-agent"|[^\s"']*/wefty-agent|wefty-agent|go\s+run\s+[^\n]*cmd/wefty-agent)(?:\s|$)`)
	launches := 0
	for _, source := range []string{".", "../docs/acceptance", "../README.md"} {
		err := filepath.WalkDir(source, func(path string, entry fs.DirEntry, walkErr error) error {
			if walkErr != nil {
				return walkErr
			}
			if entry.IsDir() || (filepath.Ext(path) != ".sh" && filepath.Ext(path) != ".md") {
				return nil
			}
			payload, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			for _, command := range strings.Split(strings.ReplaceAll(string(payload), "\\\n", " "), "\n") {
				if !launch.MatchString(command) {
					continue
				}
				launches++
				if !strings.Contains(command, "--managed-root=") && !strings.Contains(command, "--managed-root ") {
					t.Errorf("%s: agent acceptance command requires an explicit temporary --managed-root: %s", path, strings.TrimSpace(command))
				}
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	if launches == 0 {
		t.Fatal("no acceptance agent launch commands were checked")
	}
}
