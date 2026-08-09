package openapi_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

var protocolFiles = []string{
	"l1-client.v1.json",
	"l1-agent.v1.json",
	"l3.v1.json",
}

func TestProtocolDocumentsUseSharedErrorShape(t *testing.T) {
	t.Parallel()

	for _, name := range protocolFiles {
		name := name
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			doc := readObject(t, name)
			if doc["openapi"] != "3.1.0" {
				t.Fatalf("openapi = %v, want 3.1.0", doc["openapi"])
			}
			paths := object(t, doc["paths"], "paths")
			if len(paths) == 0 {
				t.Fatal("protocol has no paths")
			}
			if componentsValue, ok := doc["components"]; ok {
				components := object(t, componentsValue, "components")
				if schemasValue, ok := components["schemas"]; ok {
					schemas := object(t, schemasValue, "components.schemas")
					if _, duplicated := schemas["Error"]; duplicated {
						t.Fatal("protocol duplicated Error instead of using common.v1.json")
					}
					if _, duplicated := schemas["ErrorResponse"]; duplicated {
						t.Fatal("protocol duplicated ErrorResponse instead of using common.v1.json")
					}
				}
			}

			raw, err := os.ReadFile(name)
			if err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(string(raw), "./common.v1.json#/components/responses/") {
				t.Fatal("protocol has no shared common error response references")
			}
		})
	}
}

func TestReservedRoutesExplicitlyReturn501(t *testing.T) {
	t.Parallel()

	cases := []struct {
		file string
		path string
	}{
		{"l1-client.v1.json", "/v1/jobs/{job_id}/prompt"},
		{"l1-client.v1.json", "/v1/jobs/{job_id}/cancel"},
		{"l3.v1.json", "/v1/runs/{run_id}/cancel"},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.file+tc.path, func(t *testing.T) {
			t.Parallel()
			doc := readObject(t, tc.file)
			pathItem := object(t, object(t, doc["paths"], "paths")[tc.path], tc.path)
			operation := object(t, pathItem["post"], tc.path+".post")
			if operation["x-wefty-implementation"] != "reserved" {
				t.Fatalf("reserved marker = %v", operation["x-wefty-implementation"])
			}
			responses := object(t, operation["responses"], tc.path+".responses")
			response501 := object(t, responses["501"], tc.path+".responses.501")
			if response501["$ref"] != "./common.v1.json#/components/responses/NotImplemented" {
				t.Fatalf("501 response ref = %v", response501["$ref"])
			}
		})
	}
}

func TestAgentProtocolCarriesAttemptFenceAndLogContract(t *testing.T) {
	t.Parallel()

	doc := readObject(t, "l1-agent.v1.json")
	paths := object(t, doc["paths"], "paths")
	for _, path := range []string{
		"/v1/agent/jobs/{job_id}/attempts/{attempt_id}/lease",
		"/v1/agent/jobs/{job_id}/attempts/{attempt_id}/logs",
		"/v1/agent/jobs/{job_id}/attempts/{attempt_id}/complete",
	} {
		if _, ok := paths[path]; !ok {
			t.Errorf("missing fenced attempt route %s", path)
		}
	}

	common := readObject(t, "common.v1.json")
	components := object(t, common["components"], "components")
	schemas := object(t, components["schemas"], "components.schemas")
	logEvent := object(t, schemas["LogEvent"], "LogEvent")
	required := stringSet(t, logEvent["required"])
	for _, field := range []string{"attempt_id", "stream", "sequence", "timestamp", "bytes"} {
		if !required[field] {
			t.Errorf("LogEvent missing required field %q", field)
		}
	}
	processResult := object(t, schemas["ProcessResult"], "ProcessResult")
	if len(processResult["oneOf"].([]any)) != 3 {
		t.Fatal("ProcessResult must distinguish spawn error, exit code, and signal death")
	}

	nodeRegistration := object(t, schemas["NodeRegistration"], "NodeRegistration")
	properties := object(t, nodeRegistration["properties"], "NodeRegistration.properties")
	if _, selfReported := properties["tags"]; selfReported {
		t.Fatal("NodeRegistration must not accept self-reported tags")
	}
}

func TestL1RouteGroupsUseFabricIdentity(t *testing.T) {
	t.Parallel()

	for _, name := range []string{"l1-client.v1.json", "l1-agent.v1.json"} {
		name := name
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			doc := readObject(t, name)
			security, ok := doc["security"].([]any)
			if !ok || len(security) != 1 {
				t.Fatalf("security = %#v, want one requirement", doc["security"])
			}
			requirement := object(t, security[0], "security[0]")
			if _, ok := requirement["fabricIdentity"]; !ok {
				t.Fatalf("security requirement = %#v, want fabricIdentity", requirement)
			}
		})
	}
}

func TestL3SavedWorkflowAndRerunRoutesArePublished(t *testing.T) {
	t.Parallel()

	doc := readObject(t, "l3.v1.json")
	paths := object(t, doc["paths"], "paths")
	for _, path := range []string{
		"/v1/workflows/{workflow_id}/versions",
		"/v1/workflows/{workflow_id}/versions/{version}",
		"/v1/runs/{run_id}/rerun",
	} {
		if _, ok := paths[path]; !ok {
			t.Errorf("missing saved-workflow route %s", path)
		}
	}

	components := object(t, doc["components"], "components")
	schemas := object(t, components["schemas"], "components.schemas")
	for _, schema := range []string{"WorkflowVersionInput", "WorkflowVersion"} {
		if _, ok := schemas[schema]; !ok {
			t.Errorf("missing saved-workflow schema %s", schema)
		}
	}
}

func TestL3InRunProtocolRoutesAndReplayResponsesArePublished(t *testing.T) {
	t.Parallel()

	doc := readObject(t, "l3.v1.json")
	paths := object(t, doc["paths"], "paths")
	for _, path := range []string{
		"/v1/runs/{run_id}/lineage",
		"/v1/runs/{run_id}/envelopes",
		"/v1/runs/{run_id}/gates",
	} {
		if _, ok := paths[path]; !ok {
			t.Errorf("missing in-run protocol route %s", path)
		}
	}
	for _, path := range []string{"/v1/runs/{run_id}/envelopes", "/v1/runs/{run_id}/gates"} {
		operation := object(t, object(t, paths[path], path)["post"], path+".post")
		responses := object(t, operation["responses"], path+".responses")
		if _, ok := responses["200"]; !ok {
			t.Errorf("%s does not publish idempotent replay response", path)
		}
		if _, ok := responses["201"]; !ok {
			t.Errorf("%s does not publish append response", path)
		}
	}
	components := object(t, doc["components"], "components")
	schemas := object(t, components["schemas"], "components.schemas")
	for _, schema := range []string{"LineageEntry", "RunLineage"} {
		if _, ok := schemas[schema]; !ok {
			t.Errorf("missing lineage schema %s", schema)
		}
	}
}

func TestAllOpenAPIFilesAreJSON(t *testing.T) {
	t.Parallel()

	files, err := filepath.Glob("*.json")
	if err != nil {
		t.Fatal(err)
	}
	for _, file := range files {
		file := file
		t.Run(file, func(t *testing.T) {
			t.Parallel()
			_ = readObject(t, file)
		})
	}
}

func readObject(t *testing.T, path string) map[string]any {
	t.Helper()

	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var value map[string]any
	if err := json.Unmarshal(raw, &value); err != nil {
		t.Fatalf("parse %s: %v", path, err)
	}
	return value
}

func object(t *testing.T, value any, name string) map[string]any {
	t.Helper()

	object, ok := value.(map[string]any)
	if !ok {
		t.Fatalf("%s is %T, want object", name, value)
	}
	return object
}

func stringSet(t *testing.T, value any) map[string]bool {
	t.Helper()

	values, ok := value.([]any)
	if !ok {
		t.Fatalf("value is %T, want array", value)
	}
	set := make(map[string]bool, len(values))
	for _, value := range values {
		text, ok := value.(string)
		if !ok {
			t.Fatalf("array item is %T, want string", value)
		}
		set[text] = true
	}
	return set
}
