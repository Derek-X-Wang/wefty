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
		"/v1/agent/jobs/{job_id}/attempts/{attempt_id}/publication",
		"/v1/agent/jobs/{job_id}/attempts/{attempt_id}/logs",
		"/v1/agent/jobs/{job_id}/attempts/{attempt_id}/complete",
	} {
		if _, ok := paths[path]; !ok {
			t.Errorf("missing fenced attempt route %s", path)
		}
	}
	publicationPath := object(t, paths["/v1/agent/jobs/{job_id}/attempts/{attempt_id}/publication"], "publication path")
	publication := object(t, publicationPath["put"], "publication PUT")
	publicationBody := object(t, publication["requestBody"], "publication requestBody")
	publicationContent := object(t, publicationBody["content"], "publication requestBody.content")
	publicationMedia := object(t, publicationContent["application/json"], "publication request media type")
	publicationSchema := object(t, publicationMedia["schema"], "publication request schema")
	publicationRequired := stringSet(t, publicationSchema["required"])
	if !publicationRequired["fencing_token"] || !publicationRequired["ready"] {
		t.Fatal("publication request must require fencing_token and ready")
	}
	publicationProperties := object(t, publicationSchema["properties"], "publication request properties")
	if _, arbitraryPort := publicationProperties["published_port"]; arbitraryPort {
		t.Fatal("publication request must not accept an agent-supplied port")
	}

	common := readObject(t, "common.v1.json")
	components := object(t, common["components"], "components")
	schemas := object(t, components["schemas"], "components.schemas")
	attemptLease := object(t, schemas["AttemptLease"], "AttemptLease")
	attemptLeaseRequired := stringSet(t, attemptLease["required"])
	for _, field := range []string{"attempt_id", "fencing_token", "lease_expires_at", "lease_ttl"} {
		if !attemptLeaseRequired[field] {
			t.Errorf("AttemptLease missing required field %q", field)
		}
	}
	attemptLeaseProperties := object(t, attemptLease["properties"], "AttemptLease.properties")
	directive := object(t, attemptLeaseProperties["directive"], "AttemptLease.directive")
	directiveValues := stringSet(t, directive["enum"])
	for _, value := range []string{"stop", "restart"} {
		if !directiveValues[value] {
			t.Errorf("AttemptLease directive missing value %q", value)
		}
	}
	logEvent := object(t, schemas["LogEvent"], "LogEvent")
	required := stringSet(t, logEvent["required"])
	for _, field := range []string{"attempt_id", "stream", "sequence", "timestamp"} {
		if !required[field] {
			t.Errorf("LogEvent missing required field %q", field)
		}
	}
	if required["bytes"] {
		t.Error("LogEvent globally requires bytes instead of selecting exactly one of bytes or gap")
	}
	if len(logEvent["oneOf"].([]any)) != 2 {
		t.Fatal("LogEvent must select exactly one of raw bytes or a gap declaration")
	}
	logGap := object(t, schemas["LogGap"], "LogGap")
	gapRequired := stringSet(t, logGap["required"])
	for _, field := range []string{"through_sequence", "lost_event_count", "lost_byte_count", "reason"} {
		if !gapRequired[field] {
			t.Errorf("LogGap missing required field %q", field)
		}
	}
	gapReason := object(t, object(t, logGap["properties"], "LogGap.properties")["reason"], "LogGap.reason")
	gapReasons := stringSet(t, gapReason["enum"])
	for _, reason := range []string{"spool_eviction", "oversized_event", "replay_rejected", "late_evidence_window_expired"} {
		if !gapReasons[reason] {
			t.Errorf("LogGap reason is missing %q", reason)
		}
	}
	serviceTruncation := object(t, schemas["ServiceLogTruncation"], "ServiceLogTruncation")
	truncationRequired := stringSet(t, serviceTruncation["required"])
	for _, field := range []string{"bound_kind", "evicted_event_count", "evicted_byte_count", "evicted_through_ordinal", "earliest_retained_at", "updated_at"} {
		if !truncationRequired[field] {
			t.Errorf("ServiceLogTruncation missing required field %q", field)
		}
	}
	truncationBound := object(t, object(t, serviceTruncation["properties"], "ServiceLogTruncation.properties")["bound_kind"], "ServiceLogTruncation.bound_kind")
	truncationBounds := stringSet(t, truncationBound["enum"])
	for _, bound := range []string{"bytes", "age"} {
		if !truncationBounds[bound] {
			t.Errorf("ServiceLogTruncation bound_kind is missing %q", bound)
		}
	}
	processResult := object(t, schemas["ProcessResult"], "ProcessResult")
	if len(processResult["oneOf"].([]any)) != 4 {
		t.Fatal("ProcessResult must distinguish spawn error, output failure, exit code, and signal death")
	}
	processProperties := object(t, processResult["properties"], "ProcessResult.properties")
	if _, ok := processProperties["termination_cause"]; !ok {
		t.Fatal("ProcessResult must carry a structured termination cause")
	}
	spawnFailure := object(t, schemas["SpawnFailure"], "SpawnFailure")
	spawnRequired := stringSet(t, spawnFailure["required"])
	if !spawnRequired["code"] || !spawnRequired["message"] {
		t.Fatal("SpawnFailure must carry stable code and diagnostic message")
	}
	job := object(t, schemas["Job"], "Job")
	jobProperties := object(t, job["properties"], "Job.properties")
	for _, field := range []string{
		"status", "desired_state", "bound_node_id", "node_state", "holds_slot", "unschedulable_reason",
		"restart_suppressed_reason", "restart_streak", "lifetime_restart_count", "next_restart_at",
		"published_port", "ready", "last_failure", "healthy_since_at",
	} {
		if _, ok := jobProperties[field]; !ok {
			t.Errorf("Job response missing service-only %s", field)
		}
	}
	if _, leaked := jobProperties["published_attempt_id"]; leaked {
		t.Error("Job response exposes the internal publication marker instead of computed readiness")
	}

	nodeRegistration := object(t, schemas["NodeRegistration"], "NodeRegistration")
	properties := object(t, nodeRegistration["properties"], "NodeRegistration.properties")
	if _, selfReported := properties["tags"]; selfReported {
		t.Fatal("NodeRegistration must not accept self-reported tags")
	}
	for _, field := range []string{"max_oneshot_slots", "max_service_slots"} {
		if _, selfReported := properties[field]; selfReported {
			t.Fatalf("NodeRegistration must not accept self-reported %s", field)
		}
	}
	node := object(t, schemas["Node"], "Node")
	nodeParts := node["allOf"].([]any)
	nodeProjection := object(t, nodeParts[1], "Node.allOf[1]")
	nodeProperties := object(t, nodeProjection["properties"], "Node.properties")
	for _, field := range []string{
		"max_oneshot_slots", "max_service_slots", "authority_generation", "claims_enabled",
		"intent_revision", "intent_reason", "intent_updated_at", "intent_actor",
	} {
		if _, ok := nodeProperties[field]; !ok {
			t.Errorf("Node response missing authoritative %s", field)
		}
	}

	client := readObject(t, "l1-client.v1.json")
	clientPaths := object(t, client["paths"], "paths")
	if _, ok := clientPaths["/v1/nodes/{node_id}/claims"]; !ok {
		t.Error("client protocol is missing the durable node-claims intent route")
	}
}

func TestServiceOperatorRoutesRequireClassSelector(t *testing.T) {
	t.Parallel()
	doc := readObject(t, "l1-client.v1.json")
	paths := object(t, doc["paths"], "paths")
	operations := []struct {
		path   string
		method string
	}{
		{path: "/v1/jobs", method: "get"},
		{path: "/v1/jobs/{job_id}", method: "get"},
		{path: "/v1/jobs/{job_id}/logs", method: "get"},
		{path: "/v1/jobs/{job_id}/desired-state", method: "put"},
		{path: "/v1/jobs/{job_id}/restart", method: "post"},
		{path: "/v1/jobs/{job_id}/remove", method: "post"},
		{path: "/v1/jobs/{job_id}/forget", method: "post"},
	}
	for _, expected := range operations {
		path := object(t, paths[expected.path], expected.path)
		operation := object(t, path[expected.method], expected.path+"."+expected.method)
		parameters, ok := operation["parameters"].([]any)
		if !ok {
			t.Fatalf("%s %s has no operation parameters", expected.method, expected.path)
		}
		found := false
		for _, value := range parameters {
			parameter := object(t, value, expected.path+" parameter")
			if parameter["name"] != "class" {
				continue
			}
			found = true
			if parameter["required"] != true || parameter["in"] != "query" {
				t.Fatalf("%s %s class selector = %#v", expected.method, expected.path, parameter)
			}
			schema := object(t, parameter["schema"], expected.path+" class schema")
			if schema["const"] != "service" {
				t.Fatalf("%s %s class const = %v", expected.method, expected.path, schema["const"])
			}
		}
		if !found {
			t.Fatalf("%s %s is missing class=service", expected.method, expected.path)
		}
	}
}

func TestAgentClaimRequiresFixedWorkloadClassSelector(t *testing.T) {
	t.Parallel()

	doc := readObject(t, "l1-agent.v1.json")
	paths := object(t, doc["paths"], "paths")
	claimPath := object(t, paths["/v1/agent/jobs/claim"], "claim path")
	claimOperation := object(t, claimPath["post"], "claim operation")
	requestBody := object(t, claimOperation["requestBody"], "claim requestBody")
	content := object(t, requestBody["content"], "claim requestBody.content")
	mediaType := object(t, content["application/json"], "claim request media type")
	schema := object(t, mediaType["schema"], "claim request schema")
	required := stringSet(t, schema["required"])
	if !required["class"] {
		t.Fatal("claim request does not require class")
	}
	properties := object(t, schema["properties"], "claim request properties")
	class := object(t, properties["class"], "claim request class")
	values := stringSet(t, class["enum"])
	if !values["one-shot"] || !values["service"] || len(values) != 2 {
		t.Fatalf("claim class values = %#v, want exactly one-shot and service", values)
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

func TestL3UsesFabricAuthenticationAndRunTokenAuthorization(t *testing.T) {
	t.Parallel()
	doc := readObject(t, "l3.v1.json")
	components := object(t, doc["components"], "components")
	schemes := object(t, components["securitySchemes"], "components.securitySchemes")
	if _, exists := schemes["callerToken"]; exists {
		t.Fatal("L3 advertises callerToken even though runtime has no caller bearer token")
	}
	if _, exists := schemes["fabricIdentity"]; !exists {
		t.Fatal("L3 does not advertise mandatory Fabric authentication")
	}
	paths := object(t, doc["paths"], "paths")
	for _, path := range []string{"/v1/runs/{run_id}/envelopes", "/v1/runs/{run_id}/gates"} {
		operation := object(t, object(t, paths[path], path)["post"], path+".post")
		security, ok := operation["security"].([]any)
		if !ok || len(security) != 1 {
			t.Fatalf("%s security = %#v, want one combined requirement", path, operation["security"])
		}
		requirement := object(t, security[0], path+".security[0]")
		if _, ok := requirement["fabricIdentity"]; !ok {
			t.Errorf("%s omits Fabric authentication", path)
		}
		if _, ok := requirement["runToken"]; !ok {
			t.Errorf("%s omits bearer run-token authorization", path)
		}
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
		requestBody := object(t, operation["requestBody"], path+".requestBody")
		content := object(t, requestBody["content"], path+".requestBody.content")
		jsonContent := object(t, content["application/json"], path+".requestBody.content.application/json")
		requestSchema := object(t, jsonContent["schema"], path+".requestBody.schema")
		requestSchemaRef, _ := requestSchema["$ref"].(string)
		requestSchemaName := strings.TrimPrefix(requestSchemaRef, "#/components/schemas/")
		components := object(t, doc["components"], "components")
		schemas := object(t, components["schemas"], "components.schemas")
		writeSchema := object(t, schemas[requestSchemaName], requestSchemaName)
		if stringSet(t, writeSchema["required"])["attempt_id"] {
			t.Errorf("%s request requires attempt_id instead of binding it from the run token", path)
		}
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
