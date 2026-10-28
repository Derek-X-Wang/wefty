package agent

import (
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/runner/lima"
)

func TestColdStartClaimPollFeedsMacDemand(t *testing.T) {
	for _, platform := range []string{"darwin", "linux"} {
		t.Run(platform, func(t *testing.T) {
			a := newHostVMControlTestAgent(t)
			a.session.registration.OS = platform
			if _, err := a.session.publishCapabilityHeartbeat(t.Context(), nil); err != nil {
				t.Fatal(err)
			}
			hint := true
			a.session.client.httpClient.Transport = captureRoundTrip(func(request *http.Request) (*http.Response, error) {
				var fields map[string]any
				if err := json.NewDecoder(request.Body).Decode(&fields); err != nil {
					t.Fatal(err)
				}
				if fields["accept_queued_oci_hint"] != true {
					t.Error("new reader did not opt into queued OCI hints")
				}
				body := `{"queued_oci_work":true}`
				if !hint {
					body = ""
				}
				status := 200
				if !hint {
					status = 204
				}
				return &http.Response{StatusCode: status, Body: io.NopCloser(strings.NewReader(body)), Header: make(http.Header)}, nil
			})
			snapshot := &lima.DemandState{}
			a.ConfigureHostVMDemand(snapshot, nil)
			claim, _, err := a.session.claim(t.Context(), newAdmissionGate(1), contract.JobClassOneShot, false)
			if err != nil || claim != nil {
				t.Fatalf("hint became a claim: %+v %v", claim, err)
			}
			items, err := a.HostVMDemand(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			if (len(items) > 0) != (platform == "darwin") {
				t.Fatalf("%s demand=%v", platform, items)
			}
			hint = false
			if _, _, err := a.session.claim(t.Context(), newAdmissionGate(1), contract.JobClassOneShot, false); err != nil {
				t.Fatal(err)
			}
			items, err = a.HostVMDemand(t.Context())
			if err != nil || len(items) != 0 {
				t.Fatalf("cleared hint=%v %v", items, err)
			}
		})
	}
}

func TestColdStartOwnerCommandDuringStopHasRetryAdvice(t *testing.T) {
	a := newHostVMControlTestAgent(t)
	a.session.hostVM.stopping.Store(true)
	err := a.RecoverOCIRuntimeCapabilities(t.Context())
	if err == nil || !strings.Contains(err.Error(), "host VM is stopping; retry in a moment") {
		t.Fatalf("owner refusal=%v", err)
	}
}
