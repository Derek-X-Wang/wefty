package openapi_test

import (
	"reflect"
	"testing"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/l1"
)

func TestJobCollectionPublishesFiltersAndScopedPaging(t *testing.T) {
	doc := readObject(t, "l1-client.v1.json")
	route := object(t, object(t, object(t, doc["paths"], "paths")["/v1/jobs"], "jobs")["get"], "get")
	params := map[string]map[string]any{}
	for _, raw := range route["parameters"].([]any) {
		param := object(t, raw, "parameter")
		name := param["name"].(string)
		params[name] = param
		if param["required"] == true || param["in"] != "query" {
			t.Fatalf("%s should be an optional query parameter: %v", name, param)
		}
	}
	for _, name := range []string{"class", "kind", "state", "submitter", "cursor", "limit"} {
		if params[name] == nil {
			t.Errorf("missing filter %s", name)
		}
	}
	class := object(t, params["class"]["schema"], "class schema")
	if !reflect.DeepEqual(class["enum"], []any{"one-shot", "service"}) {
		t.Errorf("class enum=%v", class["enum"])
	}
	state := object(t, params["state"]["schema"], "state schema")
	vocabulary := stringSet(t, state["enum"])
	if len(vocabulary) != len(contract.JobTransitions) {
		t.Fatalf("state vocabulary=%v", vocabulary)
	}
	for value := range contract.JobTransitions {
		if !vocabulary[string(value)] {
			t.Errorf("missing persisted state %s", value)
		}
	}
	submitter := object(t, params["submitter"]["schema"], "submitter schema")
	if submitter["const"] != "me" {
		t.Fatalf("submitter=%v", submitter)
	}
	limit := object(t, params["limit"]["schema"], "limit schema")
	if limit["default"] != float64(l1.DefaultJobPageLimit) || limit["minimum"] != float64(1) || limit["maximum"] != float64(l1.MaxJobPageLimit) {
		t.Fatalf("limit=%v", limit)
	}
	if len(route["security"].([]any)) != 2 {
		t.Fatal("collection must publish client and attempt-credential authentication")
	}
}
