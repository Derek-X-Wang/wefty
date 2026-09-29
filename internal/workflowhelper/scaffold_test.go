package workflowhelper

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// packageManifest is the part of a package.json these tests read.
type packageManifest struct {
	Name            string            `json:"name"`
	Type            string            `json:"type"`
	Private         bool              `json:"private"`
	Engines         map[string]string `json:"engines"`
	Scripts         map[string]string `json:"scripts"`
	Dependencies    map[string]string `json:"dependencies"`
	DevDependencies map[string]string `json:"devDependencies"`
}

func readManifest(t *testing.T, path string) packageManifest {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	var manifest packageManifest
	if err := json.Unmarshal(raw, &manifest); err != nil {
		t.Fatalf("%s is not a package manifest: %v", path, err)
	}
	return manifest
}

func initWorkflow(t *testing.T, args ...string) (string, string) {
	t.Helper()
	parent := filepath.Join(t.TempDir(), "workflows")
	var stdout bytes.Buffer
	if err := ExecuteWorkflow(append(append([]string{"init"}, args...), "--dir", parent), false, &stdout); err != nil {
		t.Fatalf("wefty workflow init %s: %v", strings.Join(args, " "), err)
	}
	return parent, stdout.String()
}

// TestWorkflowInitTypeScriptWritesTheDogfoodShape pins what --lang ts is: the
// dogfood build, not a script with a .ts name. The source is bundled by the
// author's own npm run build into one dist/NAME.mjs, and that bundle is the
// thing the README submits.
func TestWorkflowInitTypeScriptWritesTheDogfoodShape(t *testing.T) {
	parent, stdout := initWorkflow(t, "Demo_Flow", "--lang", "ts")
	target := filepath.Join(parent, "Demo_Flow")

	for _, name := range []string{
		"src/Demo_Flow.ts", "package.json", "package-lock.json", "tsconfig.json",
		".gitignore", "README.md", "demoflow_integration_test.go",
	} {
		info, err := os.Stat(filepath.Join(target, name))
		if err != nil {
			t.Fatalf("the scaffold did not write %s: %v", name, err)
		}
		if info.Mode().Perm() != 0o644 {
			t.Fatalf("%s has mode %s, want 0644", name, info.Mode().Perm())
		}
	}
	if _, err := os.Stat(filepath.Join(target, "Demo_Flow.sh")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("a TypeScript scaffold also wrote a bash starter: %v", err)
	}

	manifest := readManifest(t, filepath.Join(target, "package.json"))
	// npm refuses an uppercase package name; the file names keep the case.
	if manifest.Name != "demo_flow" || manifest.Type != "module" || !manifest.Private {
		t.Fatalf("package.json identity = %+v", manifest)
	}
	wantBuild := "esbuild src/Demo_Flow.ts --bundle --platform=node --format=esm --target=node22 " +
		"--outfile=dist/Demo_Flow.mjs --banner:js='#!/usr/bin/env node'"
	if manifest.Scripts["build"] != wantBuild {
		t.Fatalf("build script = %q, want %q", manifest.Scripts["build"], wantBuild)
	}
	if manifest.Scripts["typecheck"] != "tsc --noEmit" || !strings.Contains(manifest.Scripts["test"], "npm run build") {
		t.Fatalf("scripts = %v", manifest.Scripts)
	}
	if manifest.Engines["node"] != ">=22.7" {
		t.Fatalf("engines = %v, want the Node version the README states", manifest.Engines)
	}
	// No run-time dependency at all: the mailbox writer is inlined.
	if len(manifest.Dependencies) != 0 {
		t.Fatalf("the starter depends on %v; it must depend on no package", manifest.Dependencies)
	}

	source, err := os.ReadFile(filepath.Join(target, "src", "Demo_Flow.ts"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(source), InlineTypeScriptWriter()) {
		t.Fatal("src/Demo_Flow.ts does not carry the inline writer verbatim, so the conformance test does not cover it")
	}
	if !strings.Contains(string(source), `const WORKFLOW = "Demo_Flow";`) {
		t.Fatal("the starter does not name its workflow")
	}

	readme, err := os.ReadFile(filepath.Join(target, "README.md"))
	if err != nil {
		t.Fatal(err)
	}
	for _, fragment := range []string{
		"--script=dist/Demo_Flow.mjs", "--interpreter=node", "--required-envelope",
		"Node.js 22.7 or later", "npm run build",
	} {
		if !strings.Contains(string(readme), fragment) {
			t.Fatalf("README.md does not mention %q", fragment)
		}
	}
	gitignore, err := os.ReadFile(filepath.Join(target, ".gitignore"))
	if err != nil {
		t.Fatal(err)
	}
	if string(gitignore) != "node_modules/\ndist/\n" {
		t.Fatalf(".gitignore = %q", gitignore)
	}
	if !strings.Contains(stdout, "--script="+filepath.Join(target, "dist", "Demo_Flow.mjs")+" --interpreter=node") {
		t.Fatalf("the next-step hint does not submit the bundle:\n%s", stdout)
	}
}

// TestTypeScriptScaffoldPinsTheDogfoodToolchain keeps the starter on the
// toolchain the dogfood workflow is proven with. A bump there must be a bump
// here, and the lockfile has to agree with the manifest or `npm ci` refuses it.
func TestTypeScriptScaffoldPinsTheDogfoodToolchain(t *testing.T) {
	parent, _ := initWorkflow(t, "pins", "--lang", "ts")
	target := filepath.Join(parent, "pins")
	manifest := readManifest(t, filepath.Join(target, "package.json"))
	dogfood := readManifest(t, filepath.Join("..", "..", "workflows", "dogfood", "package.json"))

	for _, tool := range []string{"esbuild", "typescript", "@types/node"} {
		pinned := manifest.DevDependencies[tool]
		if pinned == "" || pinned != dogfood.DevDependencies[tool] {
			t.Fatalf("%s is pinned at %q; workflows/dogfood pins %q", tool, pinned, dogfood.DevDependencies[tool])
		}
	}
	if len(manifest.DevDependencies) != 3 {
		t.Fatalf("devDependencies = %v, want only the build toolchain", manifest.DevDependencies)
	}
	for name := range manifest.DevDependencies {
		if strings.Contains(name, "sandcastle") {
			t.Fatalf("the starter depends on %s; Sandcastle stays inside workflows/dogfood", name)
		}
	}
	// The dogfood build flags, which are what make the bundle runnable as one
	// extensionless file on a node.
	for _, flag := range []string{"--bundle", "--platform=node", "--format=esm", "--target=node22", "--banner:js='#!/usr/bin/env node'"} {
		if !strings.Contains(dogfood.Scripts["build"], flag) || !strings.Contains(manifest.Scripts["build"], flag) {
			t.Fatalf("the build flag %s is not shared with the dogfood build", flag)
		}
	}

	raw, err := os.ReadFile(filepath.Join(target, "package-lock.json"))
	if err != nil {
		t.Fatal(err)
	}
	var lock struct {
		Name     string `json:"name"`
		Packages map[string]struct {
			Name            string            `json:"name"`
			Version         string            `json:"version"`
			DevDependencies map[string]string `json:"devDependencies"`
		} `json:"packages"`
	}
	if err := json.Unmarshal(raw, &lock); err != nil {
		t.Fatalf("package-lock.json: %v", err)
	}
	if lock.Name != "pins" || lock.Packages[""].Name != "pins" {
		t.Fatalf("the lockfile names %q/%q, want the package name", lock.Name, lock.Packages[""].Name)
	}
	for tool, version := range manifest.DevDependencies {
		if lock.Packages[""].DevDependencies[tool] != version || lock.Packages["node_modules/"+tool].Version != version {
			t.Fatalf("the lockfile resolves %s to %q (root %q), want %s",
				tool, lock.Packages["node_modules/"+tool].Version, lock.Packages[""].DevDependencies[tool], version)
		}
	}
}

func TestWorkflowInitLanguageDefaultsToBash(t *testing.T) {
	parent, stdout := initWorkflow(t, "plain")
	target := filepath.Join(parent, "plain")
	if _, err := os.Stat(filepath.Join(target, "plain.sh")); err != nil {
		t.Fatalf("the default scaffold wrote no bash starter: %v", err)
	}
	for _, name := range []string{"package.json", "src"} {
		if _, err := os.Stat(filepath.Join(target, name)); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("the default scaffold wrote %s: %v", name, err)
		}
	}
	if !strings.Contains(stdout, "--interpreter=bash") {
		t.Fatalf("the next-step hint does not submit a bash script:\n%s", stdout)
	}
}

func TestWorkflowInitReportsItsLanguage(t *testing.T) {
	for _, test := range []struct{ lang, want string }{
		{"ts", LanguageTypeScript},
		{"typescript", LanguageTypeScript},
		{"bash", LanguageBash},
	} {
		parent := filepath.Join(t.TempDir(), "workflows")
		var stdout bytes.Buffer
		if err := ExecuteWorkflow([]string{"init", "demo", "--lang", test.lang, "--dir", parent}, true, &stdout); err != nil {
			t.Fatalf("--lang %s: %v", test.lang, err)
		}
		var document struct {
			Language string   `json:"language"`
			Files    []string `json:"files"`
		}
		if err := json.Unmarshal(stdout.Bytes(), &document); err != nil {
			t.Fatalf("--json printed %q: %v", stdout.String(), err)
		}
		if document.Language != test.want || len(document.Files) == 0 {
			t.Fatalf("--lang %s reported %+v, want language %s", test.lang, document, test.want)
		}
	}
}

func TestWorkflowInitRefusesAnUnknownLanguageBeforeWriting(t *testing.T) {
	parent := filepath.Join(t.TempDir(), "workflows")
	var stdout bytes.Buffer
	err := ExecuteWorkflow([]string{"init", "demo", "--lang", "python", "--dir", parent}, false, &stdout)
	var usage UsageError
	if !errors.As(err, &usage) {
		t.Fatalf("an unknown language returned %v (%T), want a usage error", err, err)
	}
	if !strings.Contains(err.Error(), `"python"`) || !strings.Contains(err.Error(), "bash or ts") {
		t.Fatalf("refused with %q, want the language and the ones that exist", err)
	}
	if _, statErr := os.Stat(parent); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("a refused scaffold still created %s: %v", parent, statErr)
	}
}
