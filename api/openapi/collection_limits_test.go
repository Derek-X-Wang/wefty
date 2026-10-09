package openapi_test

import (
	"testing"

	"github.com/Derek-X-Wang/wefty/l1"
)

func TestCollectionLimitMaximumMatchesServer(t *testing.T) {
	// Children and jobs deliberately share a cap; other collections retain theirs.
	maxima := map[string]int{
		// The schema maximum is the accepted input range; the server clamps job pages to l1.MaxJobListingPageLimit.
		"listJobs":                  l1.MaxJobPageLimit,
		"listChildJobs":             l1.MaxJobPageLimit,
		"getJobLogs":                l1.MaxLogPageLimit,
		"listNodes":                 l1.MaxJobPageLimit,
		"listAdminPolicyAudit":      l1.MaxJobPageLimit,
		"listComputers":             l1.MaxJobPageLimit,
		"listComputerIntents":       l1.MaxJobPageLimit,
		"listComputerPolicyAudit":   l1.MaxJobPageLimit,
		"listComputerTakeoverAudit": l1.MaxJobPageLimit,
	}
	doc := readObject(t, "l1-client.v1.json")
	seen := map[string]bool{}
	for _, rawPath := range object(t, doc["paths"], "paths") {
		path := object(t, rawPath, "path")
		for _, rawOperation := range path {
			op, ok := rawOperation.(map[string]any)
			if !ok {
				continue
			}
			id, _ := op["operationId"].(string)
			maximum, tracked := maxima[id]
			if !tracked {
				continue
			}
			t.Run(id, func(t *testing.T) {
				parameters, _ := op["parameters"].([]any)
				inherited, _ := path["parameters"].([]any)
				for _, raw := range append(inherited, parameters...) {
					param := object(t, raw, "parameter")
					if param["name"] != "limit" {
						continue
					}
					seen[id] = true
					schema := object(t, param["schema"], "limit schema")
					if schema["maximum"] != float64(maximum) {
						t.Fatalf("OpenAPI maximum=%v; server=%d", schema["maximum"], maximum)
					}
					return
				}
				t.Fatal("limit parameter missing")
			})
		}
	}
	for id := range maxima {
		if !seen[id] {
			t.Errorf("operation %s missing", id)
		}
	}
}
