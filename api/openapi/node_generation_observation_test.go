package openapi_test

import (
	"strings"
	"testing"

	"github.com/santhosh-tekuri/jsonschema/v6"
)

func TestNodeGenerationAttemptTerminationProvenance(t *testing.T) {
	schema := compileProtocolSchema(t, "file:///api/openapi/common.v1.json#/components/schemas/NodeGenerationAttempt")
	for _, tc := range []struct {
		fields string
		valid  bool
	}{
		{``, true},
		{`,"termination_initiator":"agent"`, true},
		{`,"termination_initiator":"guardian"`, true},
		{`,"termination_initiator":"owner_cancel"`, false},
		{`,"termination_initiator":"spontaneous"`, false},
		{`,"termination_initiator":null`, false},
		{`,"unknown_completion_evidence":true`, false},
	} {
		payload := `{"job_id":"job","attempt_id":"attempt","state":"failed","exit_code":17,"last_renewed_at":null,"lease_expires_at":null` + tc.fields + `}`
		instance, err := jsonschema.UnmarshalJSON(strings.NewReader(payload))
		if err != nil {
			t.Fatal(err)
		}
		if err := schema.Validate(instance); (err == nil) != tc.valid {
			t.Errorf("valid=%t payload=%s err=%v", tc.valid, payload, err)
		}
	}
}
