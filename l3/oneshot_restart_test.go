package l3

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/Derek-X-Wang/wefty/contract"
)

func TestRestartableExecPolicyValidation(t *testing.T) {
	request := execCaptureRunRequest(strings.Repeat("a", 64), 1)
	setPolicy := func(r CreateRunRequest, value bool) CreateRunRequest {
		var params map[string]any
		_ = json.Unmarshal(r.Params, &params)
		params["restartable"] = value
		r.Params, _ = json.Marshal(params)
		return r
	}
	valid := setPolicy(request, true)
	valid.Restartable = true
	for _, tt := range []struct {
		name    string
		request CreateRunRequest
		allowed bool
	}{
		{"default", request, true}, {"captured restart", valid, true},
		{"typed mismatch", func() CreateRunRequest { r := request; r.Restartable = true; return r }(), false},
		{"params mismatch", setPolicy(request, true), false},
		{"dispatch authority", func() CreateRunRequest { r := valid; r.DispatchAuthority = true; return r }(), false},
		{"version one", func() CreateRunRequest {
			r := valid
			r.Params = json.RawMessage(`{"exec_version":1,"restartable":true}`)
			return r
		}(), false},
		{"workflow", func() CreateRunRequest { r := valid; r.InlineScript = nil; r.WorkflowRef = "test"; return r }(), false},
		{"OCI", func() CreateRunRequest { r := valid; r.Image = &contract.ImageProgram{}; return r }(), false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			err := validateExecCaptureRun(CreateRunInput{FabricClient: true, Request: tt.request})
			if (err == nil) != tt.allowed {
				t.Fatalf("validation=%v allowed=%t", err, tt.allowed)
			}
		})
	}
}

func TestRestartableExecPersistsAndDispatches(t *testing.T) {
	h := newIntegrationHarness(t)
	payload := []byte("capture")
	hash := uploadExecFixture(t, h, payload)
	request := execCaptureRunRequest(hash, len(payload))
	request.Restartable = true
	var params map[string]any
	_ = json.Unmarshal(request.Params, &params)
	params["restartable"] = true
	request.Params, _ = json.Marshal(params)
	accepted := h.submit(request, "restartable")
	record, err := h.l3Store.GetRun(t.Context(), accepted.RunID)
	if err != nil || !record.Restartable {
		t.Fatalf("persisted %+v %v", record, err)
	}
	intents, err := h.l3Store.pendingDispatches(t.Context())
	if err != nil || len(intents) != 1 {
		t.Fatalf("dispatch %+v %v", intents, err)
	}
	spec := intents[0].jobSpec("token")
	if !spec.Restartable || spec.Kind != contract.JobKindProcess || spec.Labels[contract.LabelDispatchAuthority] != "" {
		t.Fatalf("spec %+v", spec)
	}
}
