package scripts

import (
	"io/fs"
	"path/filepath"
	"strings"
	"testing"
)

// This guards the shipped source contract, not third-party image derivations
// or a hardware acceptance receipt (agent-computer spec section 11 item 4).
func TestComputerReferenceImagesStayOptional(t *testing.T) {
	for _, path := range []string{
		"../examples/computer/README.md",
		"../examples/computer-wayland/README.md",
	} {
		text := strings.Join(strings.Fields(string(mustReadFile(t, path))), " ")
		if !strings.Contains(text, "not a required base image, runtime layer, compatibility target, or security profile") {
			t.Errorf("%s lost the reference-image scope contract", path)
		}
	}
	text := strings.Join(strings.Fields(string(mustReadFile(t, "../docs/contracts/computer-image.md"))), " ")
	if !strings.Contains(text, "neither image is a required base or compatibility target") {
		t.Fatal("Computer image contract lost the reference-image scope restriction")
	}

	// Inspect ARG values as well as FROM lines, so a generic Dockerfile cannot
	// select a reference desktop through a substituted base-image argument.
	for _, root := range []string{"../examples", "../agent", "../contract", "../l1", "../l3", "../runner", "../cmd"} {
		err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if entry.IsDir() {
				return nil
			}
			name := entry.Name()
			genericDockerfile := strings.HasPrefix(name, "Dockerfile") &&
				!strings.HasPrefix(filepath.ToSlash(path), "../examples/computer/") &&
				!strings.HasPrefix(filepath.ToSlash(path), "../examples/computer-wayland/")
			productGo := filepath.Ext(name) == ".go" && !strings.HasSuffix(name, "_test.go")
			if !genericDockerfile && !productGo {
				return nil
			}
			text := strings.ToLower(string(mustReadFile(t, path)))
			for _, reference := range []string{"wefty-computer-reference", "wefty-computer-wayland-reference", "examples/computer"} {
				if strings.Contains(text, reference) {
					t.Errorf("%s makes a reference Computer image a product or generic-image dependency (%s)", path, reference)
				}
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
}
