package openapi_test

import (
	"encoding/json"
	"os"
	"strings"
	"testing"
)

func TestInstanceKeyContractPublished(t *testing.T) {
	raw, err := os.ReadFile("../../contract/schemas/v1/job-spec.schema.json")
	if err != nil {
		t.Fatal(err)
	}
	var schema map[string]any
	if err := json.Unmarshal(raw, &schema); err != nil {
		t.Fatal(err)
	}
	key := object(t, object(t, schema["properties"], "JobSpec properties")["instance_key"], "instance_key")
	if key["type"] != "string" || key["minLength"] != float64(1) || key["maxLength"] != float64(255) || key["pattern"] != "^[!-~]+$" {
		t.Fatalf("instance key bounds=%+v", key)
	}
	raw, err = os.ReadFile("l1-client.v1.json")
	if err != nil {
		t.Fatal(err)
	}
	var client map[string]any
	if err := json.Unmarshal(raw, &client); err != nil {
		t.Fatal(err)
	}
	route := object(t, object(t, object(t, client["paths"], "paths")["/v1/jobs"], "jobs")["post"], "post")
	description, _ := route["description"].(string)
	for _, promise := range []string{"instance_key_conflict", "instance_key_not_supported", "Fabric", "tombstones", "canceled", "stalled_cleanup_unverified", "Computers"} {
		if !strings.Contains(description, promise) {
			t.Fatalf("submission contract missing %q", promise)
		}
	}
}
