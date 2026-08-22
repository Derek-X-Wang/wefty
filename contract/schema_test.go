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

func TestJobSpecSchemaAndGoValidationAgree(t *testing.T) {
	t.Parallel()

	schema := compileSchemas(t)["job-spec"]
	paths, err := fs.Glob(contractFiles, "testdata/schemas/job-spec/*.json")
	if err != nil {
		t.Fatal(err)
	}
	for _, path := range paths {
		path := path
		t.Run(filepath.Base(path), func(t *testing.T) {
			t.Parallel()
			raw, err := contractFiles.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			assertJobSpecValidatorsAgree(t, schema, raw)
		})
	}

	cases := map[string]string{
		"trailing slash container path": `{"schema_version":1,"dispatch_key":"oci:trailing","kind":"oci","class":"one-shot","execution":{"oci":{"image":{"reference":"alpine:latest"},"working_directory":"/opt/app/"}}}`,
		"bare root container path":      `{"schema_version":1,"dispatch_key":"oci:root","kind":"oci","class":"one-shot","execution":{"oci":{"image":{"reference":"alpine:latest"},"working_directory":"/"}}}`,
		"unknown mount member":          `{"schema_version":1,"dispatch_key":"oci:mount-extra","kind":"oci","class":"one-shot","routing_tags":["wefty:node:a"],"execution":{"oci":{"image":{"reference":"alpine:latest"},"mounts":[{"node_path":"/srv/input","container_path":"/input","extra":true}]}}}`,
		"unknown limits member":         `{"schema_version":1,"dispatch_key":"oci:limits-extra","kind":"oci","class":"one-shot","execution":{"oci":{"image":{"reference":"alpine:latest"},"limits":{"cpu_millicores":1,"extra":true}}}}`,
		"null mount read only":          `{"schema_version":1,"dispatch_key":"oci:mount-null","kind":"oci","class":"one-shot","routing_tags":["wefty:node:a"],"execution":{"oci":{"image":{"reference":"alpine:latest"},"mounts":[{"node_path":"/srv/input","container_path":"/input","read_only":null}]}}}`,
		"null memory limit":             `{"schema_version":1,"dispatch_key":"oci:memory-null","kind":"oci","class":"one-shot","execution":{"oci":{"image":{"reference":"alpine:latest"},"limits":{"memory_bytes":null,"cpu_millicores":1}}}}`,
		"process null OCI arm":          `{"schema_version":1,"dispatch_key":"process:null-oci","kind":"process","class":"one-shot","execution":{"executable":{"path":"/bin/true"},"argv":["true"],"working_directory":"/tmp","handoff_directory":"/tmp/out","oci":null}}`,
		"OCI null executable":           `{"schema_version":1,"dispatch_key":"oci:null-exec","kind":"oci","class":"one-shot","execution":{"executable":null,"oci":{"image":{"reference":"alpine:latest"}}}}`,
		"OCI empty executable":          `{"schema_version":1,"dispatch_key":"oci:empty-exec","kind":"oci","class":"one-shot","execution":{"executable":{},"oci":{"image":{"reference":"alpine:latest"}}}}`,
		"OCI null process argv":         `{"schema_version":1,"dispatch_key":"oci:null-argv","kind":"oci","class":"one-shot","execution":{"argv":null,"oci":{"image":{"reference":"alpine:latest"}}}}`,
		"OCI empty process working dir": `{"schema_version":1,"dispatch_key":"oci:empty-workdir","kind":"oci","class":"one-shot","execution":{"working_directory":"","oci":{"image":{"reference":"alpine:latest"}}}}`,
		"OCI null process working dir":  `{"schema_version":1,"dispatch_key":"oci:null-workdir","kind":"oci","class":"one-shot","execution":{"working_directory":null,"oci":{"image":{"reference":"alpine:latest"}}}}`,
		"OCI null handoff dir":          `{"schema_version":1,"dispatch_key":"oci:null-handoff","kind":"oci","class":"one-shot","execution":{"handoff_directory":null,"oci":{"image":{"reference":"alpine:latest"}}}}`,
		"empty routing tag":             `{"schema_version":1,"dispatch_key":"process:empty-tag","kind":"process","class":"one-shot","routing_tags":[""],"execution":{"executable":{"path":"/bin/true"},"argv":["true"],"working_directory":"/tmp","handoff_directory":"/tmp/out"}}`,
	}
	for name, raw := range cases {
		name, raw := name, raw
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			assertJobSpecValidatorsAgree(t, schema, []byte(raw))
		})
	}
}

func TestImageProgramSchemaAndGoValidationAgree(t *testing.T) {
	t.Parallel()

	schema := compileSchemas(t)["run-record"]
	cases := map[string]string{
		"complete valid program":       `{"reference":"ghcr.io/example/tool:v1","digest":"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","argv":["","run"],"working_directory":"/","mounts":[{"node_path":"/srv/input","container_path":"/input","read_only":false}],"limits":{"memory_bytes":1,"cpu_millicores":1},"runtime_handler":""}`,
		"argv all empty":               `{"reference":"alpine:latest","argv":[""]}`,
		"trailing working directory":   `{"reference":"alpine:latest","working_directory":"/workspace/"}`,
		"dot working directory":        `{"reference":"alpine:latest","working_directory":"/workspace/../tmp"}`,
		"root node mount":              `{"reference":"alpine:latest","mounts":[{"node_path":"/","container_path":"/input"}]}`,
		"reserved mount target":        `{"reference":"alpine:latest","mounts":[{"node_path":"/srv/input","container_path":"/wefty/handoff/result"}]}`,
		"empty limits":                 `{"reference":"alpine:latest","limits":{}}`,
		"limit beyond int64":           `{"reference":"alpine:latest","limits":{"memory_bytes":9223372036854775808}}`,
		"explicit null optional field": `{"reference":"alpine:latest","working_directory":null}`,
		"unknown member":               `{"reference":"alpine:latest","future":true}`,
	}
	cases["reference too long"] = `{"reference":"` + strings.Repeat("a", 2049) + `"}`
	for name, imageRaw := range cases {
		name, imageRaw := name, imageRaw
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			assertImageProgramValidatorsAgree(t, schema, []byte(imageRaw))
		})
	}
}

func assertImageProgramValidatorsAgree(t *testing.T, schema *jsonschema.Schema, imageRaw []byte) {
	t.Helper()

	recordRaw := []byte(fmt.Sprintf(`{"schema_version":1,"run_id":"run_image","dispatch_key":"run:image","status":"pending","trigger":{"type":"manual","principal":"tester"},"workflow":{"image":%s},"params":{},"tags":["wefty:node:node-a"],"created_at":"2026-08-22T12:00:00Z","updated_at":"2026-08-22T12:00:00Z"}`, imageRaw))
	instance, schemaDecodeErr := jsonschema.UnmarshalJSON(bytes.NewReader(recordRaw))
	schemaErr := schemaDecodeErr
	if schemaErr == nil {
		schemaErr = schema.Validate(instance)
	}
	var program ImageProgram
	goErr := json.Unmarshal(imageRaw, &program)
	if goErr == nil {
		goErr = ValidateImageProgram(program, JobClassOneShot)
	}
	if goErr == nil {
		goErr = ValidatePinnedRouting(program, []string{StableNodeTagPrefix + "node-a"})
	}
	if (schemaErr == nil) != (goErr == nil) {
		t.Fatalf("schema and Go validation disagree:\nschema: %v\nGo: %v\nimage: %s", schemaErr, goErr, imageRaw)
	}
}

func assertJobSpecValidatorsAgree(t *testing.T, schema *jsonschema.Schema, raw []byte) {
	t.Helper()

	instance, schemaDecodeErr := jsonschema.UnmarshalJSON(bytes.NewReader(raw))
	schemaErr := schemaDecodeErr
	if schemaErr == nil {
		schemaErr = schema.Validate(instance)
	}

	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var spec JobSpec
	goErr := decoder.Decode(&spec)
	if goErr == nil {
		goErr = ValidateJobSpec(&spec)
	}
	if (schemaErr == nil) != (goErr == nil) {
		t.Fatalf("schema and Go validation disagree:\nschema: %v\nGo: %v\ninstance: %s", schemaErr, goErr, raw)
	}
}

func TestValidFixturesRoundTripThroughGoTypes(t *testing.T) {
	t.Parallel()

	cases := []struct {
		path string
		new  func() any
	}{
		{"testdata/schemas/job-spec/valid-oci-one-shot.json", func() any { return new(JobSpec) }},
		{"testdata/schemas/job-spec/valid-oci-reserved-environment-names.json", func() any { return new(JobSpec) }},
		{"testdata/schemas/job-spec/valid-oci-service.json", func() any { return new(JobSpec) }},
		{"testdata/schemas/job-spec/valid-process.json", func() any { return new(JobSpec) }},
		{"testdata/schemas/job-spec/valid-service.json", func() any { return new(JobSpec) }},
		{"testdata/schemas/job-spec/valid-unknown-class.json", func() any { return new(JobSpec) }},
		{"testdata/schemas/job-spec/valid-unknown-kind.json", func() any { return new(JobSpec) }},
		{"testdata/schemas/envelope/valid.json", func() any { return new(Envelope) }},
		{"testdata/schemas/gate-result/valid.json", func() any { return new(GateResult) }},
		{"testdata/schemas/run-record/valid.json", func() any { return new(RunRecord) }},
		{"testdata/schemas/run-record/valid-image.json", func() any { return new(RunRecord) }},
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

func TestOCIJobSpecRoundTripOmitsProcessArm(t *testing.T) {
	t.Parallel()

	digest := "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	workingDirectory := "/workspace"
	spec := JobSpec{
		SchemaVersion: SchemaVersionV1,
		DispatchKey:   "oci:round-trip",
		Kind:          JobKindOCI,
		Class:         JobClassOneShot,
		Execution: ExecutionSpec{
			OCI: &OCIExecutionSpec{
				Image:            OCIImageSpec{Reference: "ghcr.io/example/tool:latest", Digest: &digest},
				Argv:             []string{"tool", "run"},
				WorkingDirectory: &workingDirectory,
			},
		},
	}

	raw, err := json.Marshal(spec)
	if err != nil {
		t.Fatal(err)
	}
	var wire map[string]any
	if err := json.Unmarshal(raw, &wire); err != nil {
		t.Fatal(err)
	}
	execution := wire["execution"].(map[string]any)
	for _, processField := range []string{"executable", "argv", "working_directory", "handoff_directory"} {
		if _, present := execution[processField]; present {
			t.Errorf("OCI wire payload emitted process field %q: %s", processField, raw)
		}
	}
	if _, present := execution["oci"]; !present {
		t.Fatalf("OCI wire payload omitted execution.oci: %s", raw)
	}
}

func TestProcessJobSpecFixtureStaysByteCompatible(t *testing.T) {
	t.Parallel()

	raw, err := contractFiles.ReadFile("testdata/schemas/job-spec/valid-process.json")
	if err != nil {
		t.Fatal(err)
	}
	var compact bytes.Buffer
	if err := json.Compact(&compact, raw); err != nil {
		t.Fatal(err)
	}
	var spec JobSpec
	if err := json.Unmarshal(raw, &spec); err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(spec)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(encoded, compact.Bytes()) {
		t.Fatalf("process fixture changed on the wire:\nwant: %s\n got: %s", compact.Bytes(), encoded)
	}
}

func TestOCIReservedEnvironmentNamesAreExact(t *testing.T) {
	t.Parallel()

	want := []string{
		EnvHandoffDir,
		EnvServiceDir,
		EnvServicePort,
		EnvL3Endpoint,
		EnvRunToken,
	}
	for _, name := range want {
		if !IsOCIReservedEnvironmentName(name) {
			t.Errorf("M3 reserved name %q is not recognized", name)
		}
	}
	if IsOCIReservedEnvironmentName(EnvRunID) || IsOCIReservedEnvironmentName("WEFTY_CUSTOM") {
		t.Fatal("OCI reserved-name set widened beyond the five M3 names")
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

func TestUnknownClassParsesButExecutionRejects(t *testing.T) {
	t.Parallel()

	raw, err := contractFiles.ReadFile("testdata/schemas/job-spec/valid-unknown-class.json")
	if err != nil {
		t.Fatal(err)
	}
	var spec JobSpec
	if err := json.Unmarshal(raw, &spec); err != nil {
		t.Fatalf("unknown class must decode: %v", err)
	}
	if spec.Class != "scheduled" {
		t.Fatalf("class changed during decode: %q", spec.Class)
	}

	err = CheckWorkloadClass(spec.Class)
	var executionErr *ClassExecutionError
	if !errors.As(err, &executionErr) {
		t.Fatalf("expected ClassExecutionError, got %v", err)
	}
	if executionErr.Code() != ErrorUnsupportedClass {
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

func TestJobClassSchemaIsOpen(t *testing.T) {
	t.Parallel()

	doc := unmarshalJSONFile(t, "schemas/v1/job-spec.schema.json")
	root := doc.(map[string]any)
	properties := root["properties"].(map[string]any)
	class := properties["class"].(map[string]any)
	if _, closed := class["enum"]; closed {
		t.Fatal("job class must not use a closed JSON Schema enum")
	}
	if _, closed := class["const"]; closed {
		t.Fatal("job class must not use a JSON Schema const")
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
