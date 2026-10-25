package l1

import (
	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"net/http"
	"strings"
	"testing"
)

func TestSubmitPublishedPort80ReservedForSeatDoor(t *testing.T) {
	h := newIntegrationHarness(t, nil)
	client := h.client(fabric.Identity{NodeID: "caller", Tags: []string{DefaultClientPrincipalTag}})
	spec := validJobSpec("reserved-port", nil)
	spec.Class = contract.JobClassService
	spec.Execution.HandoffDirectory = ""
	port := 80
	spec.PublishedPort = &port
	status, _, body := h.do(client, http.MethodPost, "/v1/jobs", spec)
	if status != http.StatusBadRequest || !strings.Contains(string(body), "invalid_request") || !strings.Contains(string(body), contract.SeatFrontDoorPortReserved) {
		t.Fatalf("reserved submit: %d %s", status, body)
	}
}
