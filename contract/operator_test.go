package contract

import (
	"encoding/json"
	"testing"
)

func TestAllowedActionOmitsAbsentRequirements(t *testing.T) {
	for _, requirements := range []map[string]any{nil, {}} {
		data, err := json.Marshal(AllowedAction{Verb: "inspect", Requires: requirements})
		if err != nil {
			t.Fatal(err)
		}
		var fields map[string]json.RawMessage
		if err := json.Unmarshal(data, &fields); err != nil {
			t.Fatal(err)
		}
		if value, ok := fields["requires"]; ok && string(value) != "{}" {
			t.Fatalf("absent requirements must be omitted or {}: %s", data)
		}
	}
}
