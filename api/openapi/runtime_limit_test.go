package openapi_test

import (
	"strings"
	"testing"

	"github.com/santhosh-tekuri/jsonschema/v6"
)

func TestReachedLimitCompletionSchemaPreservesPayloadArms(t *testing.T) {
	schema := compileProtocolSchema(t, "file:///api/openapi/common.v1.json#/components/schemas/ProcessResult")
	for _, test := range []struct {
		payload string
		valid   bool
	}{
		{`{"exit_code":0}`, true},
		{`{"exit_code":0,"limit_reached":"max_runtime"}`, true},
		{`{"exit_code":0,"limit_reached":"idle_timeout"}`, true},
		{`{"signal":"killed","termination_cause":"agent","limit_reached":"max_runtime"}`, true},
		{`{"exit_code":0,"limit_reached":"agent"}`, false},
		{`{"exit_code":0,"limit_reached":null}`, false},
	} {
		instance, err := jsonschema.UnmarshalJSON(strings.NewReader(test.payload))
		if err != nil {
			t.Fatal(err)
		}
		if err := schema.Validate(instance); (err == nil) != test.valid {
			t.Errorf("valid=%t payload=%s err=%v", test.valid, test.payload, err)
		}
	}
}
