package contract

import (
	"bytes"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"path/filepath"
	"slices"
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
			assertJobSpecValidatorsAgree(t, schema, raw, strings.HasPrefix(filepath.Base(path), "valid"))
		})
	}

	type validatorCase struct {
		raw       string
		wantValid bool
	}
	cases := map[string]validatorCase{
		"trailing slash container path":            {raw: `{"schema_version":1,"dispatch_key":"oci:trailing","kind":"oci","class":"one-shot","execution":{"oci":{"image":{"reference":"alpine:latest"},"working_directory":"/opt/app/"}}}`, wantValid: false},
		"bare root container path":                 {raw: `{"schema_version":1,"dispatch_key":"oci:root","kind":"oci","class":"one-shot","execution":{"oci":{"image":{"reference":"alpine:latest"},"working_directory":"/"}}}`, wantValid: true},
		"unknown mount member":                     {raw: `{"schema_version":1,"dispatch_key":"oci:mount-extra","kind":"oci","class":"one-shot","routing_tags":["wefty:node:a"],"execution":{"oci":{"image":{"reference":"alpine:latest"},"mounts":[{"node_path":"/srv/input","container_path":"/input","extra":true}]}}}`, wantValid: false},
		"unknown limits member":                    {raw: `{"schema_version":1,"dispatch_key":"oci:limits-extra","kind":"oci","class":"one-shot","execution":{"oci":{"image":{"reference":"alpine:latest"},"limits":{"cpu_millicores":1,"extra":true}}}}`, wantValid: false},
		"null mount read only":                     {raw: `{"schema_version":1,"dispatch_key":"oci:mount-null","kind":"oci","class":"one-shot","routing_tags":["wefty:node:a"],"execution":{"oci":{"image":{"reference":"alpine:latest"},"mounts":[{"node_path":"/srv/input","container_path":"/input","read_only":null}]}}}`, wantValid: false},
		"null memory limit":                        {raw: `{"schema_version":1,"dispatch_key":"oci:memory-null","kind":"oci","class":"one-shot","execution":{"oci":{"image":{"reference":"alpine:latest"},"limits":{"memory_bytes":null,"cpu_millicores":1}}}}`, wantValid: false},
		"process null OCI arm":                     {raw: `{"schema_version":1,"dispatch_key":"process:null-oci","kind":"process","class":"one-shot","execution":{"executable":{"path":"/bin/true"},"argv":["true"],"working_directory":"/tmp","handoff_directory":"/tmp/out","oci":null}}`, wantValid: false},
		"OCI null executable":                      {raw: `{"schema_version":1,"dispatch_key":"oci:null-exec","kind":"oci","class":"one-shot","execution":{"executable":null,"oci":{"image":{"reference":"alpine:latest"}}}}`, wantValid: false},
		"OCI empty executable":                     {raw: `{"schema_version":1,"dispatch_key":"oci:empty-exec","kind":"oci","class":"one-shot","execution":{"executable":{},"oci":{"image":{"reference":"alpine:latest"}}}}`, wantValid: false},
		"OCI null process argv":                    {raw: `{"schema_version":1,"dispatch_key":"oci:null-argv","kind":"oci","class":"one-shot","execution":{"argv":null,"oci":{"image":{"reference":"alpine:latest"}}}}`, wantValid: false},
		"OCI empty process working dir":            {raw: `{"schema_version":1,"dispatch_key":"oci:empty-workdir","kind":"oci","class":"one-shot","execution":{"working_directory":"","oci":{"image":{"reference":"alpine:latest"}}}}`, wantValid: false},
		"OCI null process working dir":             {raw: `{"schema_version":1,"dispatch_key":"oci:null-workdir","kind":"oci","class":"one-shot","execution":{"working_directory":null,"oci":{"image":{"reference":"alpine:latest"}}}}`, wantValid: false},
		"OCI null handoff dir":                     {raw: `{"schema_version":1,"dispatch_key":"oci:null-handoff","kind":"oci","class":"one-shot","execution":{"handoff_directory":null,"oci":{"image":{"reference":"alpine:latest"}}}}`, wantValid: false},
		"empty routing tag":                        {raw: `{"schema_version":1,"dispatch_key":"process:empty-tag","kind":"process","class":"one-shot","routing_tags":[""],"execution":{"executable":{"path":"/bin/true"},"argv":["true"],"working_directory":"/tmp","handoff_directory":"/tmp/out"}}`, wantValid: false},
		"valid computer":                           {raw: `{"schema_version":1,"dispatch_key":"oci:computer","kind":"oci","class":"service","restart":"always","routing_tags":["wefty:node:node-a"],"execution":{"oci":{"image":{"reference":"ghcr.io/example/computer:v1","digest":"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},"limits":{"memory_bytes":1},"computer":{"display":{"protocol":"rfb-websocket-v1"},"disk_bytes":1}}}}`, wantValid: true},
		"computer writable operator mount":         {raw: `{"schema_version":1,"dispatch_key":"oci:computer-writable-mount","kind":"oci","class":"service","restart":"always","routing_tags":["wefty:node:node-a"],"execution":{"oci":{"image":{"reference":"ghcr.io/example/computer:v1","digest":"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},"mounts":[{"node_path":"/srv/input","container_path":"/input"}],"limits":{"memory_bytes":1},"computer":{"display":{"protocol":"rfb-websocket-v1"},"disk_bytes":1}}}}`, wantValid: false},
		"computer null":                            {raw: `{"schema_version":1,"dispatch_key":"oci:computer-null","kind":"oci","class":"service","restart":"always","routing_tags":["wefty:node:node-a"],"execution":{"oci":{"image":{"reference":"ghcr.io/example/computer:v1","digest":"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},"limits":{"memory_bytes":1},"computer":null}}}`, wantValid: false},
		"computer empty":                           {raw: `{"schema_version":1,"dispatch_key":"oci:computer-empty","kind":"oci","class":"service","restart":"always","routing_tags":["wefty:node:node-a"],"execution":{"oci":{"image":{"reference":"ghcr.io/example/computer:v1","digest":"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},"limits":{"memory_bytes":1},"computer":{}}}}`, wantValid: false},
		"computer display null":                    {raw: `{"schema_version":1,"dispatch_key":"oci:computer-display-null","kind":"oci","class":"service","restart":"always","routing_tags":["wefty:node:node-a"],"execution":{"oci":{"image":{"reference":"ghcr.io/example/computer:v1","digest":"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},"limits":{"memory_bytes":1},"computer":{"display":null,"disk_bytes":1}}}}`, wantValid: false},
		"computer display empty":                   {raw: `{"schema_version":1,"dispatch_key":"oci:computer-display-empty","kind":"oci","class":"service","restart":"always","routing_tags":["wefty:node:node-a"],"execution":{"oci":{"image":{"reference":"ghcr.io/example/computer:v1","digest":"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},"limits":{"memory_bytes":1},"computer":{"display":{},"disk_bytes":1}}}}`, wantValid: false},
		"computer unknown member":                  {raw: `{"schema_version":1,"dispatch_key":"oci:computer-extra","kind":"oci","class":"service","restart":"always","routing_tags":["wefty:node:node-a"],"execution":{"oci":{"image":{"reference":"ghcr.io/example/computer:v1","digest":"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},"limits":{"memory_bytes":1},"computer":{"display":{"protocol":"rfb-websocket-v1"},"disk_bytes":1,"extra":true}}}}`, wantValid: false},
		"computer display unknown":                 {raw: `{"schema_version":1,"dispatch_key":"oci:computer-display-extra","kind":"oci","class":"service","restart":"always","routing_tags":["wefty:node:node-a"],"execution":{"oci":{"image":{"reference":"ghcr.io/example/computer:v1","digest":"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},"limits":{"memory_bytes":1},"computer":{"display":{"protocol":"rfb-websocket-v1","extra":true},"disk_bytes":1}}}}`, wantValid: false},
		"computer bad protocol":                    {raw: `{"schema_version":1,"dispatch_key":"oci:computer-protocol","kind":"oci","class":"service","restart":"always","routing_tags":["wefty:node:node-a"],"execution":{"oci":{"image":{"reference":"ghcr.io/example/computer:v1","digest":"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},"limits":{"memory_bytes":1},"computer":{"display":{"protocol":"rfb-v1"},"disk_bytes":1}}}}`, wantValid: false},
		"computer zero disk":                       {raw: `{"schema_version":1,"dispatch_key":"oci:computer-zero-disk","kind":"oci","class":"service","restart":"always","routing_tags":["wefty:node:node-a"],"execution":{"oci":{"image":{"reference":"ghcr.io/example/computer:v1","digest":"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},"limits":{"memory_bytes":1},"computer":{"display":{"protocol":"rfb-websocket-v1"},"disk_bytes":0}}}}`, wantValid: false},
		"computer negative disk":                   {raw: `{"schema_version":1,"dispatch_key":"oci:computer-negative-disk","kind":"oci","class":"service","restart":"always","routing_tags":["wefty:node:node-a"],"execution":{"oci":{"image":{"reference":"ghcr.io/example/computer:v1","digest":"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},"limits":{"memory_bytes":1},"computer":{"display":{"protocol":"rfb-websocket-v1"},"disk_bytes":-1}}}}`, wantValid: false},
		"computer null disk":                       {raw: `{"schema_version":1,"dispatch_key":"oci:computer-null-disk","kind":"oci","class":"service","restart":"always","routing_tags":["wefty:node:node-a"],"execution":{"oci":{"image":{"reference":"ghcr.io/example/computer:v1","digest":"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},"limits":{"memory_bytes":1},"computer":{"display":{"protocol":"rfb-websocket-v1"},"disk_bytes":null}}}}`, wantValid: false},
		"computer disk overflow":                   {raw: `{"schema_version":1,"dispatch_key":"oci:computer-overflow","kind":"oci","class":"service","restart":"always","routing_tags":["wefty:node:node-a"],"execution":{"oci":{"image":{"reference":"ghcr.io/example/computer:v1","digest":"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},"limits":{"memory_bytes":1},"computer":{"display":{"protocol":"rfb-websocket-v1"},"disk_bytes":9223372036854775808}}}}`, wantValid: false},
		"computer missing memory":                  {raw: `{"schema_version":1,"dispatch_key":"oci:computer-no-memory","kind":"oci","class":"service","restart":"always","routing_tags":["wefty:node:node-a"],"execution":{"oci":{"image":{"reference":"ghcr.io/example/computer:v1","digest":"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},"computer":{"display":{"protocol":"rfb-websocket-v1"},"disk_bytes":1}}}}`, wantValid: false},
		"computer CPU without memory":              {raw: `{"schema_version":1,"dispatch_key":"oci:computer-cpu-no-memory","kind":"oci","class":"service","restart":"always","routing_tags":["wefty:node:node-a"],"execution":{"oci":{"image":{"reference":"ghcr.io/example/computer:v1","digest":"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},"limits":{"cpu_millicores":1},"computer":{"display":{"protocol":"rfb-websocket-v1"},"disk_bytes":1}}}}`, wantValid: false},
		"computer memory overflow":                 {raw: `{"schema_version":1,"dispatch_key":"oci:computer-memory-overflow","kind":"oci","class":"service","restart":"always","routing_tags":["wefty:node:node-a"],"execution":{"oci":{"image":{"reference":"ghcr.io/example/computer:v1","digest":"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},"limits":{"memory_bytes":9223372036854775808},"computer":{"display":{"protocol":"rfb-websocket-v1"},"disk_bytes":1}}}}`, wantValid: false},
		"computer one shot":                        {raw: `{"schema_version":1,"dispatch_key":"oci:computer-one-shot","kind":"oci","class":"one-shot","routing_tags":["wefty:node:node-a"],"execution":{"oci":{"image":{"reference":"ghcr.io/example/computer:v1","digest":"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},"limits":{"memory_bytes":1},"computer":{"display":{"protocol":"rfb-websocket-v1"},"disk_bytes":1}}}}`, wantValid: false},
		"computer unknown class":                   {raw: `{"schema_version":1,"dispatch_key":"oci:computer-unknown","kind":"oci","class":"scheduled","routing_tags":["wefty:node:node-a"],"execution":{"oci":{"image":{"reference":"ghcr.io/example/computer:v1","digest":"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},"limits":{"memory_bytes":1},"computer":{"display":{"protocol":"rfb-websocket-v1"},"disk_bytes":1}}}}`, wantValid: false},
		"computer missing digest":                  {raw: `{"schema_version":1,"dispatch_key":"oci:computer-no-digest","kind":"oci","class":"service","restart":"always","routing_tags":["wefty:node:node-a"],"execution":{"oci":{"image":{"reference":"ghcr.io/example/computer:v1"},"limits":{"memory_bytes":1},"computer":{"display":{"protocol":"rfb-websocket-v1"},"disk_bytes":1}}}}`, wantValid: false},
		"computer published port":                  {raw: `{"schema_version":1,"dispatch_key":"oci:computer-port","kind":"oci","class":"service","restart":"always","published_port":8080,"routing_tags":["wefty:node:node-a"],"execution":{"oci":{"image":{"reference":"ghcr.io/example/computer:v1","digest":"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},"limits":{"memory_bytes":1},"computer":{"display":{"protocol":"rfb-websocket-v1"},"disk_bytes":1}}}}`, wantValid: false},
		"computer null published port":             {raw: `{"schema_version":1,"dispatch_key":"oci:computer-null-port","kind":"oci","class":"service","restart":"always","published_port":null,"routing_tags":["wefty:node:node-a"],"execution":{"oci":{"image":{"reference":"ghcr.io/example/computer:v1","digest":"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},"limits":{"memory_bytes":1},"computer":{"display":{"protocol":"rfb-websocket-v1"},"disk_bytes":1}}}}`, wantValid: false},
		"computer missing node tag":                {raw: `{"schema_version":1,"dispatch_key":"oci:computer-no-node","kind":"oci","class":"service","restart":"always","execution":{"oci":{"image":{"reference":"ghcr.io/example/computer:v1","digest":"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},"limits":{"memory_bytes":1},"computer":{"display":{"protocol":"rfb-websocket-v1"},"disk_bytes":1}}}}`, wantValid: false},
		"normalized OCI computer missing node tag": {raw: `{"schema_version":1,"dispatch_key":"oci:computer-normalized-no-node","kind":" OCI ","class":"service","restart":"always","execution":{"oci":{"image":{"reference":"ghcr.io/example/computer:v1","digest":"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},"limits":{"memory_bytes":1},"computer":{"display":{"protocol":"rfb-websocket-v1"},"disk_bytes":1}}}}`, wantValid: false},
		"computer empty mounts no tag":             {raw: `{"schema_version":1,"dispatch_key":"oci:computer-empty-mounts","kind":"oci","class":"service","restart":"always","execution":{"oci":{"image":{"reference":"ghcr.io/example/computer:v1","digest":"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},"mounts":[],"limits":{"memory_bytes":1},"computer":{"display":{"protocol":"rfb-websocket-v1"},"disk_bytes":1}}}}`, wantValid: false},
		"computer two node tags":                   {raw: `{"schema_version":1,"dispatch_key":"oci:computer-two-node","kind":"oci","class":"service","restart":"always","routing_tags":["wefty:node:node-a","wefty:node:node-b"],"execution":{"oci":{"image":{"reference":"ghcr.io/example/computer:v1","digest":"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},"limits":{"memory_bytes":1},"computer":{"display":{"protocol":"rfb-websocket-v1"},"disk_bytes":1}}}}`, wantValid: false},
		"computer on process kind":                 {raw: `{"schema_version":1,"dispatch_key":"process:computer","kind":"process","class":"service","restart":"always","routing_tags":["wefty:node:node-a"],"execution":{"executable":{"path":"/bin/true"},"argv":["true"],"working_directory":"/tmp","oci":{"image":{"reference":"ghcr.io/example/computer:v1","digest":"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},"limits":{"memory_bytes":1},"computer":{"display":{"protocol":"rfb-websocket-v1"},"disk_bytes":1}}}}`, wantValid: false},
	}
	computerNumber := func(dispatchKey, diskBytes, memoryBytes string) string {
		return fmt.Sprintf(`{"schema_version":1,"dispatch_key":%q,"kind":"oci","class":"service","restart":"always","routing_tags":["wefty:node:node-a"],"execution":{"oci":{"image":{"reference":"ghcr.io/example/computer:v1","digest":"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},"limits":{"memory_bytes":%s},"computer":{"display":{"protocol":"rfb-websocket-v1"},"disk_bytes":%s}}}}`, dispatchKey, memoryBytes, diskBytes)
	}
	for _, tc := range []struct {
		name, diskBytes, memoryBytes string
		wantValid                    bool
	}{
		{name: "computer disk decimal integral", diskBytes: "1.0", memoryBytes: "1", wantValid: true},
		{name: "computer disk exponent integral", diskBytes: "1e0", memoryBytes: "1", wantValid: true},
		{name: "computer disk fractional", diskBytes: "1.5", memoryBytes: "1", wantValid: false},
		{name: "computer disk max int64", diskBytes: "9223372036854775807", memoryBytes: "1", wantValid: true},
		{name: "computer disk overflow numeric", diskBytes: "9223372036854775808", memoryBytes: "1", wantValid: false},
		{name: "computer memory decimal integral", diskBytes: "1", memoryBytes: "1.0", wantValid: true},
		{name: "computer memory exponent integral", diskBytes: "1", memoryBytes: "1e0", wantValid: true},
		{name: "computer memory fractional", diskBytes: "1", memoryBytes: "1.5", wantValid: false},
		{name: "computer memory max int64", diskBytes: "1", memoryBytes: "9223372036854775807", wantValid: true},
		{name: "computer memory overflow numeric", diskBytes: "1", memoryBytes: "9223372036854775808", wantValid: false},
	} {
		cases[tc.name] = validatorCase{raw: computerNumber("oci:"+strings.ReplaceAll(tc.name, " ", "-"), tc.diskBytes, tc.memoryBytes), wantValid: tc.wantValid}
	}
	ociCPUNumber := func(dispatchKey, cpuMillicores string) string {
		return fmt.Sprintf(`{"schema_version":1,"dispatch_key":%q,"kind":"oci","class":"one-shot","execution":{"oci":{"image":{"reference":"alpine:latest"},"limits":{"cpu_millicores":%s}}}}`, dispatchKey, cpuMillicores)
	}
	for _, tc := range []struct {
		name, cpuMillicores string
		wantValid           bool
	}{
		{name: "OCI CPU decimal integral", cpuMillicores: "1.0", wantValid: true},
		{name: "OCI CPU exponent integral", cpuMillicores: "1e0", wantValid: true},
		{name: "OCI CPU fractional", cpuMillicores: "1.5", wantValid: false},
		{name: "OCI CPU max int64", cpuMillicores: "9223372036854775807", wantValid: true},
		{name: "OCI CPU overflow", cpuMillicores: "9223372036854775808", wantValid: false},
	} {
		cases[tc.name] = validatorCase{raw: ociCPUNumber("oci:"+strings.ReplaceAll(tc.name, " ", "-"), tc.cpuMillicores), wantValid: tc.wantValid}
	}
	for name, tc := range cases {
		name, tc := name, tc
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			assertJobSpecValidatorsAgree(t, schema, []byte(tc.raw), tc.wantValid)
		})
	}
}

func TestImageProgramSchemaAndGoValidationAgree(t *testing.T) {
	t.Parallel()

	schema := compileSchemas(t)["run-record"]
	type validatorCase struct {
		raw       string
		wantValid bool
	}
	cases := map[string]validatorCase{
		"complete valid program":       {raw: `{"reference":"ghcr.io/example/tool:v1","digest":"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","argv":["","run"],"working_directory":"/","mounts":[{"node_path":"/srv/input","container_path":"/input","read_only":false}],"limits":{"memory_bytes":1,"cpu_millicores":1},"runtime_handler":""}`, wantValid: true},
		"argv all empty":               {raw: `{"reference":"alpine:latest","argv":[""]}`, wantValid: false},
		"trailing working directory":   {raw: `{"reference":"alpine:latest","working_directory":"/workspace/"}`, wantValid: false},
		"dot working directory":        {raw: `{"reference":"alpine:latest","working_directory":"/workspace/../tmp"}`, wantValid: false},
		"root node mount":              {raw: `{"reference":"alpine:latest","mounts":[{"node_path":"/","container_path":"/input"}]}`, wantValid: false},
		"reserved mount target":        {raw: `{"reference":"alpine:latest","mounts":[{"node_path":"/srv/input","container_path":"/wefty/handoff/result"}]}`, wantValid: false},
		"empty limits":                 {raw: `{"reference":"alpine:latest","limits":{}}`, wantValid: false},
		"limit beyond int64":           {raw: `{"reference":"alpine:latest","limits":{"memory_bytes":9223372036854775808}}`, wantValid: false},
		"explicit null optional field": {raw: `{"reference":"alpine:latest","working_directory":null}`, wantValid: false},
		"unknown member":               {raw: `{"reference":"alpine:latest","future":true}`, wantValid: false},
	}
	for _, tc := range []struct {
		name, field, number string
		wantValid           bool
	}{
		{name: "image memory decimal integral", field: "memory_bytes", number: "1.0", wantValid: true},
		{name: "image memory exponent integral", field: "memory_bytes", number: "1e0", wantValid: true},
		{name: "image memory fractional", field: "memory_bytes", number: "1.5", wantValid: false},
		{name: "image memory max int64", field: "memory_bytes", number: "9223372036854775807", wantValid: true},
		{name: "image memory overflow", field: "memory_bytes", number: "9223372036854775808", wantValid: false},
		{name: "image CPU decimal integral", field: "cpu_millicores", number: "1.0", wantValid: true},
		{name: "image CPU exponent integral", field: "cpu_millicores", number: "1e0", wantValid: true},
		{name: "image CPU fractional", field: "cpu_millicores", number: "1.5", wantValid: false},
		{name: "image CPU max int64", field: "cpu_millicores", number: "9223372036854775807", wantValid: true},
		{name: "image CPU overflow", field: "cpu_millicores", number: "9223372036854775808", wantValid: false},
	} {
		cases[tc.name] = validatorCase{
			raw:       fmt.Sprintf(`{"reference":"alpine:latest","limits":{"%s":%s}}`, tc.field, tc.number),
			wantValid: tc.wantValid,
		}
	}
	cases["reference too long"] = validatorCase{raw: `{"reference":"` + strings.Repeat("a", 2049) + `"}`, wantValid: false}
	for name, tc := range cases {
		name, tc := name, tc
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			assertImageProgramValidatorsAgree(t, schema, []byte(tc.raw), tc.wantValid)
		})
	}
}

func assertImageProgramValidatorsAgree(t *testing.T, schema *jsonschema.Schema, imageRaw []byte, wantValid bool) {
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
	if (schemaErr == nil) != wantValid {
		t.Fatalf("validation = schema %v, Go %v, want valid %v\nimage: %s", schemaErr, goErr, wantValid, imageRaw)
	}
}

func assertJobSpecValidatorsAgree(t *testing.T, schema *jsonschema.Schema, raw []byte, wantValid bool) {
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
	if (schemaErr == nil) != wantValid {
		t.Fatalf("validation = schema %v, Go %v, want valid %v\ninstance: %s", schemaErr, goErr, wantValid, raw)
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
		{"testdata/schemas/job-spec/valid-oci-computer.json", func() any { return new(JobSpec) }},
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

func TestExistingJobSpecFixturesStayByteCompatible(t *testing.T) {
	t.Parallel()

	for _, path := range []string{
		"testdata/schemas/job-spec/valid-process.json",
		"testdata/schemas/job-spec/valid-oci-one-shot.json",
		"testdata/schemas/job-spec/valid-oci-computer.json",
		"testdata/schemas/job-spec/valid-oci-service.json",
	} {
		raw, err := contractFiles.ReadFile(path)
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
			t.Fatalf("fixture %s changed on the wire:\nwant: %s\n got: %s", path, compact.Bytes(), encoded)
		}
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
		EnvComputerToken,
		EnvComputerViewPort,
		EnvComputerControlPort,
	}
	if !slices.Equal(ociReservedEnvironmentNames[:], want) {
		t.Fatalf("OCI reserved environment names = %v, want exactly %v", ociReservedEnvironmentNames, want)
	}
	for _, name := range want {
		if !IsOCIReservedEnvironmentName(name) {
			t.Errorf("M3 reserved name %q is not recognized", name)
		}
	}
	if IsOCIReservedEnvironmentName(EnvRunID) || IsOCIReservedEnvironmentName("WEFTY_CUSTOM") {
		t.Fatal("OCI reserved-name set differs from the eight ratified names")
	}
}

func TestUnknownKindParses(t *testing.T) {
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
