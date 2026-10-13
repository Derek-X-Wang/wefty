package l1

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
)

func TestOCIExplicitIdleLimitRefusedAtSubmission(t *testing.T) {
	h := newIntegrationHarness(t, nil)
	client := h.client(fabric.Identity{NodeID: "client", Tags: []string{DefaultClientPrincipalTag}})
	for _, value := range []string{"600", "0", "-1", "null"} {
		t.Run(value, func(t *testing.T) {
			spec := json.RawMessage(`{"schema_version":1,"dispatch_key":"oci-idle-` + value + `","kind":"oci","class":"one-shot","execution":{"oci":{"image":{"reference":"example/tool:latest"}}},"limits":{"idle_timeout_seconds":` + value + `}}`)
			status, _, body := h.do(client, http.MethodPost, "/v1/jobs", spec)
			var response contract.ErrorResponse
			if err := json.Unmarshal(body, &response); err != nil {
				t.Fatal(err)
			}
			if status != http.StatusBadRequest || response.Error.Code != contract.ErrorInvalidRequest || response.Error.Retryable ||
				!strings.Contains(response.Error.Message, "limits.idle_timeout_seconds applies only to process jobs") {
				t.Fatalf("explicit idle limit: status=%d body=%s", status, body)
			}
		})
	}
}

func TestSubmissionLimitsPreserveProcessAndOCIRuntime(t *testing.T) {
	h := newIntegrationHarness(t, nil)
	client := h.client(fabric.Identity{NodeID: "client", Tags: []string{DefaultClientPrincipalTag}})
	for i, limits := range []string{"", `,"limits":{}`, `,"limits":{"max_runtime_seconds":30}`} {
		spec := json.RawMessage(fmt.Sprintf(`{"schema_version":1,"dispatch_key":"oci-runtime-%d","kind":"oci","class":"one-shot","execution":{"oci":{"image":{"reference":"example/tool:latest"}}}%s}`, i, limits))
		status, _, body := h.do(client, http.MethodPost, "/v1/jobs", spec)
		if status != http.StatusCreated {
			t.Fatalf("OCI omitted idle limit: status=%d body=%s", status, body)
		}
	}
	spec := validJobSpec("process-idle", nil)
	spec.Limits = &contract.JobLimits{IdleTimeoutSeconds: 600, MaxRuntimeSeconds: 30}
	status, _, body := h.do(client, http.MethodPost, "/v1/jobs", spec)
	if status != http.StatusCreated {
		t.Fatalf("process limits: status=%d body=%s", status, body)
	}
}
