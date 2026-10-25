package openapi_test

import (
	"encoding/json"
	"os"
	"testing"
)

func TestSeatRunBackendSchemas(t *testing.T) {
	for _, tc := range []struct{ file, path string }{{"l1-client.v1.json", "/v1/seats/{seat_id}/run-target"}, {"l1-agent.v1.json", "/v1/agent/jobs/{job_id}/attempts/{attempt_id}/seat-run"}} {
		raw, err := os.ReadFile(tc.file)
		if err != nil {
			t.Fatal(err)
		}
		var doc map[string]any
		if err = json.Unmarshal(raw, &doc); err != nil {
			t.Fatal(err)
		}
		if doc["paths"].(map[string]any)[tc.path] == nil {
			t.Errorf("missing %s", tc.path)
		}
	}
	raw, err := os.ReadFile("l3.v1.json")
	if err != nil {
		t.Fatal(err)
	}
	var doc map[string]any
	if err = json.Unmarshal(raw, &doc); err != nil {
		t.Fatal(err)
	}
	source := doc["paths"].(map[string]any)["/v1/runs"].(map[string]any)["post"].(map[string]any)["requestBody"].(map[string]any)["content"].(map[string]any)["application/json"].(map[string]any)["schema"].(map[string]any)
	raw, err = os.ReadFile("common.v1.json")
	if err != nil {
		t.Fatal(err)
	}
	var common map[string]any
	if err = json.Unmarshal(raw, &common); err != nil {
		t.Fatal(err)
	}
	schemas := common["components"].(map[string]any)["schemas"].(map[string]any)
	for _, name := range []string{"SeatRunInput", "SeatRunBinding", "SeatRunControlRequest", "SeatRunCommand"} {
		schema := schemas[name].(map[string]any)
		if schema["additionalProperties"] != false {
			t.Errorf("%s is not closed", name)
		}
	}
	if source["properties"].(map[string]any)["seat_run"] == nil {
		t.Fatal("Seat Run source is not published")
	}
}
