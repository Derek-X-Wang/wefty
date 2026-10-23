package agent

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	workloadrunner "github.com/Derek-X-Wang/wefty/runner"
)

func TestSeatApprovalBridgeRoutesExcludeNodeProtocol(t *testing.T) {
	for _, route := range attemptCredentialBridgeRoutes {
		if strings.Contains(route.Path, "/v1/agent/") {
			t.Errorf("Attempt credential bridge exposes node route: %+v", route)
		}
	}
	for _, route := range computerBridgeRoutes {
		if strings.Contains(route.Path, "/v1/agent/") {
			t.Errorf("Computer bridge exposes node route: %+v", route)
		}
	}
}

func TestSeatApprovalRunBridgeRefusesNodeAnswer(t *testing.T) {
	var dials atomic.Int32
	participant := bridgeDialFabric{dial: func(context.Context, string, string) (net.Conn, error) {
		dials.Add(1)
		return nil, fmt.Errorf("approval must never reach L1 through the run bridge")
	}}
	bridge, err := newWorkflowBridgeWithSurface(t.Context(), participant, "", workloadrunner.WorkflowBridgeBinding{}, workflowBridgeSurfaceRun, true, withoutRunLedgerSurface(), withControlPlaneSurface(participant, "wefty://control-plane"))
	if err != nil {
		t.Fatal(err)
	}
	defer bridge.close()
	request := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/l1/v1/agent/seat-approvals/x/answer", strings.NewReader(`{"approved":true}`))
	request.Header.Set("Authorization", "Bearer attempt-credential")
	response := httptest.NewRecorder()
	bridge.server.Handler.ServeHTTP(response, request)
	if response.Code != http.StatusForbidden || dials.Load() != 0 {
		t.Fatalf("node answer through bridge: status=%d upstream dials=%d body=%s", response.Code, dials.Load(), response.Body)
	}
}
