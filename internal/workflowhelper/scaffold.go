package workflowhelper

import (
	_ "embed"
	"encoding/json"
	"fmt"
	"go/format"
	"io"
	"os"
	"path/filepath"
	"strings"
	"text/template"
)

//go:embed templates/inline-writer.sh
var inlineWriter string

//go:embed templates/starter.sh.tmpl
var bashStarterTemplate string

//go:embed templates/README.md.tmpl
var readmeTemplate string

//go:embed templates/integration_test.go.tmpl
var integrationTestTemplate string

// WorkflowUsage is the scaffold's whole surface.
const WorkflowUsage = `Usage: wefty workflow init NAME [--lang bash] [--dir DIR]

Write a runnable workflow starter that reports through the run mailbox: one
step, one envelope, one gate and one result, plus a README and a test that
exercises it without a cluster.

  --lang bash      the starter's language; bash is the only one for now
  --dir DIR        the parent directory (default: workflows)
  --json           print the written paths as JSON
`

// InlineBashWriter is the POSIX writer the bash scaffold embeds, verbatim. It
// is exported so the conformance test can prove it writes byte-identical event
// files to the ones the `wefty run` subcommands write: an OCI image without the
// wefty binary must not become a second, subtly different protocol.
func InlineBashWriter() string { return inlineWriter }

// scaffold is what every template is rendered against.
type scaffold struct {
	Name       string
	ScriptName string
	TestFile   string
	// GoPackage and TestPrefix are the sanitized forms of Name: a workflow may
	// be called branch-gates, a Go package may not.
	GoPackage    string
	TestPrefix   string
	InlineWriter string
}

// scaffoldFile is one written file: its name in the workflow directory, the
// template it comes from, and the mode it needs (the starter is executable).
type scaffoldFile struct {
	name     string
	template string
	mode     os.FileMode
}

// ExecuteWorkflow dispatches `wefty workflow ...`. Like `wefty run`, it needs
// no cluster and no identity: it writes files.
func ExecuteWorkflow(args []string, jsonOutput bool, stdout io.Writer) error {
	if len(args) == 0 {
		return UsageError("a wefty workflow subcommand is required: init")
	}
	if wantsHelp(args) {
		_, err := io.WriteString(stdout, WorkflowUsage)
		return err
	}
	switch args[0] {
	case "init":
		return workflowInit(args[1:], jsonOutput, stdout)
	default:
		return UsageError(fmt.Sprintf("unknown wefty workflow subcommand %q", args[0]))
	}
}

func workflowInit(args []string, jsonOutput bool, stdout io.Writer) error {
	flags := newFlagSet("init")
	lang := flags.String("lang", "bash", "the starter's language; bash is the only one for now")
	directory := flags.String("dir", "workflows", "the parent directory to write the workflow into")
	flags.BoolVar(&jsonOutput, "json", jsonOutput, "print the written paths as JSON")
	name, err := parseWithPositional(flags, args, false)
	if err != nil {
		return UsageError("usage: wefty workflow init NAME [--lang bash] [--dir DIR]")
	}
	if err := validWorkflowName(name); err != nil {
		return err
	}
	if *lang != "bash" {
		// A TypeScript starter needs a bundle step, and this scaffold has no
		// place to put one. A submission carries one inline script, which the
		// node materializes as a file with no extension, and every TypeScript
		// runtime decides whether to strip types from that extension. The lane
		// that works is the dogfood shape -- src/NAME.ts plus a package.json,
		// bundled to a single dist/NAME.mjs and submitted -- and that is a
		// follow-up to #476, not something to fake here.
		if *lang == "ts" || *lang == "typescript" {
			return UsageError(
				"--lang ts is not available: a TypeScript workflow needs a bundle step -- src/NAME.ts and a package.json bundled to one dist/NAME.mjs, which is what gets submitted -- and that lane is a follow-up to #476. The scaffold writes bash")
		}
		return UsageError(fmt.Sprintf("--lang %q is not bash; bash is the only language the scaffold writes", *lang))
	}
	target := filepath.Join(*directory, name)
	if _, err := os.Stat(target); err == nil {
		return UsageError(fmt.Sprintf("%s already exists; pick another name or remove it", target))
	}

	data := scaffold{
		Name:         name,
		GoPackage:    goIdentifier(name) + "_test",
		TestPrefix:   exportedIdentifier(name),
		InlineWriter: inlineWriter,
	}
	data.TestFile = goIdentifier(name) + "_integration_test.go"
	data.ScriptName = name + ".sh"

	files := []scaffoldFile{
		{data.ScriptName, bashStarterTemplate, 0o755},
		{"README.md", readmeTemplate, 0o644},
		{data.TestFile, integrationTestTemplate, 0o644},
	}

	if err := os.MkdirAll(target, 0o755); err != nil {
		return fmt.Errorf("create %s: %w", target, err)
	}
	written := make([]string, 0, len(files))
	for _, file := range files {
		path := filepath.Join(target, file.name)
		rendered, err := render(file.name, file.template, data)
		if err != nil {
			return err
		}
		if err := os.WriteFile(path, rendered, file.mode); err != nil {
			return fmt.Errorf("write %s: %w", path, err)
		}
		written = append(written, path)
	}

	if jsonOutput {
		document, err := json.Marshal(struct {
			Workflow string   `json:"workflow"`
			Language string   `json:"language"`
			Files    []string `json:"files"`
		}{Workflow: name, Language: *lang, Files: written})
		if err != nil {
			return err
		}
		_, err = fmt.Fprintf(stdout, "%s\n", document)
		return err
	}
	for _, path := range written {
		if _, err := fmt.Fprintln(stdout, path); err != nil {
			return err
		}
	}
	_, err = fmt.Fprintf(stdout,
		"\nNext: read %s, then submit with\n  wefty --json submit --script=%s --interpreter=bash --required-envelope --tag=<routing-tag>\n",
		filepath.Join(target, "README.md"), filepath.Join(target, data.ScriptName))
	return err
}

func render(name, body string, data scaffold) ([]byte, error) {
	parsed, err := template.New(name).Parse(body)
	if err != nil {
		return nil, fmt.Errorf("parse the %s template: %w", name, err)
	}
	var out strings.Builder
	if err := parsed.Execute(&out, data); err != nil {
		return nil, fmt.Errorf("render %s: %w", name, err)
	}
	if !strings.HasSuffix(name, ".go") {
		return []byte(out.String()), nil
	}
	// A scaffolded Go file usually lands inside this repository, where
	// `gofmt -l .` is a gate. Template conditionals leave blank lines a person
	// would not write, so the generated source is formatted rather than
	// handed over as a gate failure waiting to happen.
	formatted, err := format.Source([]byte(out.String()))
	if err != nil {
		return nil, fmt.Errorf("format %s: %w", name, err)
	}
	return formatted, nil
}

// validWorkflowName keeps a scaffolded name usable as a directory, a script
// name and, once sanitized, a Go package.
func validWorkflowName(name string) error {
	if name == "" {
		return UsageError("a workflow name is required")
	}
	if len(name) > 64 {
		return UsageError("a workflow name is at most 64 characters")
	}
	if first := name[0]; !(first >= 'a' && first <= 'z') && !(first >= 'A' && first <= 'Z') {
		return UsageError(fmt.Sprintf("workflow name %q must start with a letter", name))
	}
	for index := 0; index < len(name); index++ {
		character := name[index]
		switch {
		case character >= 'a' && character <= 'z',
			character >= 'A' && character <= 'Z',
			character >= '0' && character <= '9',
			character == '-' || character == '_':
		default:
			return UsageError(fmt.Sprintf("workflow name %q may contain only letters, digits, - and _", name))
		}
	}
	return nil
}

func goIdentifier(name string) string {
	identifier := make([]byte, 0, len(name))
	for index := 0; index < len(name); index++ {
		character := name[index]
		switch {
		case character >= 'a' && character <= 'z', character >= '0' && character <= '9':
			identifier = append(identifier, character)
		case character >= 'A' && character <= 'Z':
			identifier = append(identifier, character+('a'-'A'))
		}
	}
	return string(identifier)
}

func exportedIdentifier(name string) string {
	identifier := make([]byte, 0, len(name))
	upper := true
	for index := 0; index < len(name); index++ {
		character := name[index]
		switch {
		case character == '-' || character == '_':
			upper = true
		case character >= 'a' && character <= 'z':
			if upper {
				character -= 'a' - 'A'
			}
			identifier = append(identifier, character)
			upper = false
		case (character >= 'A' && character <= 'Z') || (character >= '0' && character <= '9'):
			identifier = append(identifier, character)
			upper = false
		}
	}
	return string(identifier)
}
