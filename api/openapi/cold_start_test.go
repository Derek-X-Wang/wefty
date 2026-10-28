package openapi_test

import (
	"strings"
	"testing"

	"github.com/santhosh-tekuri/jsonschema/v6"
)

func TestColdStartHintAndSeatReplySchema(t *testing.T) {
	doc := readObject(t, "l1-agent.v1.json")
	path := object(t, object(t, doc["paths"], "paths")["/v1/agent/jobs/claim"], "claim")
	response := object(t, object(t, object(t, path["post"], "post")["responses"], "responses")["200"], "200")
	schema := object(t, object(t, object(t, response["content"], "content")["application/json"], "media")["schema"], "schema")
	if _, ok := object(t, schema["properties"], "properties")["queued_oci_work"]; !ok {
		t.Fatal("claim poll omits the queued OCI hint")
	}
	compiled := compileProtocolSchema(t, "file:///api/openapi/l1-agent.v1.json#/paths/~1v1~1agent~1jobs~1claim/post/responses/200/content/application~1json/schema")
	for _, check := range []struct {
		body  string
		valid bool
	}{
		{`{"queued_oci_work":true}`, true},
		{`{"queued_oci_work":false}`, false},
		{`{"queued_oci_work":true,"attempt_token":"not-authority"}`, false},
		{`{"queued_oci_work":true,"unknown":true}`, false},
		{`{}`, false},
	} {
		instance, err := jsonschema.UnmarshalJSON(strings.NewReader(check.body))
		if err != nil {
			t.Fatal(err)
		}
		if err := compiled.Validate(instance); (err == nil) != check.valid {
			t.Errorf("hint wire %s: valid=%t error=%v", check.body, check.valid, err)
		}
	}
	request := object(t, object(t, path["post"], "post")["requestBody"], "requestBody")
	requestSchema := object(t, object(t, object(t, request["content"], "request content")["application/json"], "request media")["schema"], "request schema")
	if _, ok := object(t, requestSchema["properties"], "request properties")["accept_queued_oci_hint"]; !ok {
		t.Fatal("claim request has no explicit hint reader opt-in")
	}
	if stringSet(t, requestSchema["required"])["accept_queued_oci_hint"] {
		t.Fatal("old readers must not require the new opt-in")
	}
	requestValidator := compileProtocolSchema(t, "file:///api/openapi/l1-agent.v1.json#/paths/~1v1~1agent~1jobs~1claim/post/requestBody/content/application~1json/schema")
	for _, check := range []struct {
		body  string
		valid bool
	}{
		{`{"node_id":"node","boot_session_id":"boot","class":"one-shot"}`, true},
		{`{"node_id":"node","boot_session_id":"boot","class":"one-shot","accept_queued_oci_hint":true}`, true},
		{`{"node_id":"node","boot_session_id":"boot","class":"one-shot","accept_queued_oci_hint":false}`, true},
		{`{"node_id":"node","boot_session_id":"boot","class":"one-shot","accept_queued_oci_hint":"true"}`, false},
	} {
		instance, err := jsonschema.UnmarshalJSON(strings.NewReader(check.body))
		if err != nil {
			t.Fatal(err)
		}
		if err := requestValidator.Validate(instance); (err == nil) != check.valid {
			t.Errorf("opt-in wire %s: valid=%t error=%v", check.body, check.valid, err)
		}
	}
	common := readObject(t, "common.v1.json")
	schemas := object(t, object(t, common["components"], "components")["schemas"], "schemas")
	reply := object(t, schemas["SeatCallResult"], "SeatCallResult")
	if _, ok := object(t, reply["properties"], "properties")["host_vm_starting"]; !ok {
		t.Fatal("Seat reply omits the cold-start field")
	}
}
