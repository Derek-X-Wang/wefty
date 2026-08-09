package contract

import (
	"bytes"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"path/filepath"
	"strings"
	"testing"

	"github.com/santhosh-tekuri/jsonschema/v6"
)

//go:embed schemas/v1/*.json testdata/schemas/*/*.json
var contractFiles embed.FS

var schemaIDs = map[string]string{
	"job-spec":    "https://wefty.dev/schemas/v1/job-spec.schema.json",
	"envelope":    "https://wefty.dev/schemas/v1/envelope.schema.json",
	"gate-result": "https://wefty.dev/schemas/v1/gate-result.schema.json",
	"run-record":  "https://wefty.dev/schemas/v1/run-record.schema.json",
}

func TestSchemaFixtures(t *testing.T) {
	t.Parallel()

	schemas := compileSchemas(t)
	paths, err := fs.Glob(contractFiles, "testdata/schemas/*/*.json")
	if err != nil {
		t.Fatal(err)
	}

	for _, path := range paths {
		path := path
		t.Run(path, func(t *testing.T) {
			t.Parallel()

			parts := strings.Split(path, "/")
			contractName := parts[2]
			wantValid := strings.HasPrefix(filepath.Base(path), "valid")
			instance := unmarshalJSONFile(t, path)
			validationErr := schemas[contractName].Validate(instance)
			if wantValid && validationErr != nil {
				t.Fatalf("valid fixture rejected: %v", validationErr)
			}
			if !wantValid && validationErr == nil {
				t.Fatal("invalid fixture was accepted")
			}
		})
	}
}

func TestValidFixturesRoundTripThroughGoTypes(t *testing.T) {
	t.Parallel()

	cases := []struct {
		path string
		new  func() any
	}{
		{"testdata/schemas/job-spec/valid-process.json", func() any { return new(JobSpec) }},
		{"testdata/schemas/job-spec/valid-unknown-kind.json", func() any { return new(JobSpec) }},
		{"testdata/schemas/envelope/valid.json", func() any { return new(Envelope) }},
		{"testdata/schemas/gate-result/valid.json", func() any { return new(GateResult) }},
		{"testdata/schemas/run-record/valid.json", func() any { return new(RunRecord) }},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.path, func(t *testing.T) {
			t.Parallel()

			raw, err := contractFiles.ReadFile(tc.path)
			if err != nil {
				t.Fatal(err)
			}
			first := tc.new()
			if err := json.Unmarshal(raw, first); err != nil {
				t.Fatalf("decode fixture: %v", err)
			}
			encoded, err := json.Marshal(first)
			if err != nil {
				t.Fatalf("encode Go value: %v", err)
			}
			second := tc.new()
			if err := json.Unmarshal(encoded, second); err != nil {
				t.Fatalf("decode round trip: %v", err)
			}
			reencoded, err := json.Marshal(second)
			if err != nil {
				t.Fatalf("re-encode Go value: %v", err)
			}
			if !bytes.Equal(encoded, reencoded) {
				t.Fatalf("round trip changed JSON:\nfirst:  %s\nsecond: %s", encoded, reencoded)
			}
		})
	}
}

func TestUnknownKindParsesButExecutionRejects(t *testing.T) {
	t.Parallel()

	raw, err := contractFiles.ReadFile("testdata/schemas/job-spec/valid-unknown-kind.json")
	if err != nil {
		t.Fatal(err)
	}
	var spec JobSpec
	if err := json.Unmarshal(raw, &spec); err != nil {
		t.Fatalf("unknown kind must decode: %v", err)
	}
	if spec.Kind != "future.microvm" {
		t.Fatalf("kind changed during decode: %q", spec.Kind)
	}

	err = CheckExecutableKind(spec.Kind)
	var executionErr *ExecutionError
	if !errors.As(err, &executionErr) {
		t.Fatalf("expected ExecutionError, got %v", err)
	}
	if executionErr.Code() != ErrorUnsupportedKind {
		t.Fatalf("unexpected error code: %q", executionErr.Code())
	}
}

func TestJobKindSchemaIsOpen(t *testing.T) {
	t.Parallel()

	doc := unmarshalJSONFile(t, "schemas/v1/job-spec.schema.json")
	root, ok := doc.(map[string]any)
	if !ok {
		t.Fatal("schema root is not an object")
	}
	properties := root["properties"].(map[string]any)
	kind := properties["kind"].(map[string]any)
	if _, closed := kind["enum"]; closed {
		t.Fatal("job kind must not use a closed JSON Schema enum")
	}
}

func compileSchemas(t *testing.T) map[string]*jsonschema.Schema {
	t.Helper()

	compiler := jsonschema.NewCompiler()
	compiler.AssertFormat()
	for _, id := range schemaIDs {
		name := strings.TrimPrefix(id, "https://wefty.dev/schemas/v1/")
		raw, err := contractFiles.ReadFile("schemas/v1/" + name)
		if err != nil {
			t.Fatal(err)
		}
		doc, err := jsonschema.UnmarshalJSON(bytes.NewReader(raw))
		if err != nil {
			t.Fatalf("parse %s: %v", name, err)
		}
		if err := compiler.AddResource(id, doc); err != nil {
			t.Fatalf("add %s: %v", name, err)
		}
	}

	compiled := make(map[string]*jsonschema.Schema, len(schemaIDs))
	for name, id := range schemaIDs {
		schema, err := compiler.Compile(id)
		if err != nil {
			t.Fatalf("compile %s: %v", name, err)
		}
		compiled[name] = schema
	}
	return compiled
}

func unmarshalJSONFile(t *testing.T, path string) any {
	t.Helper()

	raw, err := contractFiles.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	value, err := jsonschema.UnmarshalJSON(bytes.NewReader(raw))
	if err != nil {
		t.Fatal(fmt.Errorf("parse %s: %w", path, err))
	}
	return value
}
