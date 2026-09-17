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

//go:embed templates/starter.ts.tmpl
var typeScriptStarterTemplate string

//go:embed templates/README.md.tmpl
var readmeTemplate string

//go:embed templates/integration_test.go.tmpl
var integrationTestTemplate string

// WorkflowUsage is the scaffold's whole surface.
const WorkflowUsage = `Usage: wefty workflow init NAME [--lang bash|ts] [--dir DIR]

Write a runnable workflow starter that reports through the run mailbox: one
step, one envelope, one gate and one result, plus a README and a test that
exercises it without a cluster.

  --lang bash|ts   the starter's language (default: bash)
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
	Lang       string
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
	lang := flags.String("lang", "bash", "bash or ts")
	directory := flags.String("dir", "workflows", "the parent directory to write the workflow into")
	flags.BoolVar(&jsonOutput, "json", jsonOutput, "print the written paths as JSON")
	name, err := parseWithPositional(flags, args, false)
	if err != nil {
		return UsageError("usage: wefty workflow init NAME [--lang bash|ts] [--dir DIR]")
	}
	if err := validWorkflowName(name); err != nil {
		return err
	}
	if *lang != "bash" && *lang != "ts" {
		return UsageError(fmt.Sprintf("--lang %q is not bash or ts", *lang))
	}
	target := filepath.Join(*directory, name)
	if _, err := os.Stat(target); err == nil {
		return UsageError(fmt.Sprintf("%s already exists; pick another name or remove it", target))
	}

	data := scaffold{
		Name:         name,
		Lang:         *lang,
		GoPackage:    goIdentifier(name) + "_test",
		TestPrefix:   exportedIdentifier(name),
		InlineWriter: inlineWriter,
	}
	data.TestFile = goIdentifier(name) + "_integration_test.go"
	if *lang == "bash" {
		data.ScriptName = name + ".sh"
	} else {
		data.ScriptName = name + ".ts"
	}

	files := []scaffoldFile{
		{data.ScriptName, bashStarterTemplate, 0o755},
		{"README.md", readmeTemplate, 0o644},
		{data.TestFile, integrationTestTemplate, 0o644},
	}
	if *lang == "ts" {
		// One file, deliberately: a submission carries one inline script, and
		// the node materializes exactly that. A starter that imported a
		// sibling would scaffold something that cannot be submitted.
		files[0] = scaffoldFile{data.ScriptName, typeScriptStarterTemplate, 0o644}
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
	_, err = fmt.Fprintf(stdout, "\nNext: read %s, then submit with\n  wefty --json submit --script=%s --interpreter=%s --tag=<routing-tag>\n",
		filepath.Join(target, "README.md"), filepath.Join(target, data.ScriptName), interpreterFor(*lang))
	return err
}

func interpreterFor(lang string) string {
	if lang == "ts" {
		return "node"
	}
	return "bash"
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
