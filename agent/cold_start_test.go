package agent

import (
	"encoding/json"
	"io"
	"net/http"
	"reflect"
	"strings"
	"testing"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/runner/lima"
)

func TestColdStartClaimPollFeedsMacDemand(t *testing.T) {
	for _, platform := range []string{"darwin", "linux", "darwin-unconfigured"} {
		t.Run(platform, func(t *testing.T) {
			a := newHostVMControlTestAgent(t)
			a.session.registration.OS = strings.TrimSuffix(platform, "-unconfigured")
			if _, err := a.session.publishCapabilityHeartbeat(t.Context(), nil); err != nil {
				t.Fatal(err)
			}
			hint := true
			a.session.client.httpClient.Transport = captureRoundTrip(func(request *http.Request) (*http.Response, error) {
				var fields map[string]any
				if err := json.NewDecoder(request.Body).Decode(&fields); err != nil {
					t.Fatal(err)
				}
				if (fields["accept_queued_oci_hint"] == true) != (platform == "darwin") {
					t.Errorf("%s poll has opt-in=%v", platform, fields["accept_queued_oci_hint"])
				}
				body := `{"queued_oci_work":true}`
				if platform != "darwin" {
					body = ""
				}
				if !hint {
					body = ""
				}
				status := 200
				if !hint || platform != "darwin" {
					status = 204
				}
				return &http.Response{StatusCode: status, Body: io.NopCloser(strings.NewReader(body)), Header: make(http.Header)}, nil
			})
			snapshot := &lima.DemandState{}
			if platform != "darwin-unconfigured" {
				a.ConfigureHostVMDemand(snapshot, nil)
			}
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

func TestColdStartOldL1FallbackUntilReconnect(t *testing.T) {
	a := newHostVMControlTestAgent(t)
	a.session.registration.OS = "darwin"
	a.ConfigureHostVMDemand(&lima.DemandState{}, nil)
	var requests []string
	a.session.client.httpClient.Transport = captureRoundTrip(func(r *http.Request) (*http.Response, error) {
		body, _ := io.ReadAll(r.Body)
		if strings.HasSuffix(r.URL.Path, "/register") {
			return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(`{"node_id":"node","boot_session_id":"boot"}`)), Header: make(http.Header)}, nil
		}
		requests = append(requests, string(body))
		if strings.Contains(string(body), `"accept_queued_oci_hint":true`) {
			return &http.Response{StatusCode: 400, Body: io.NopCloser(strings.NewReader(`{"error":{"code":"invalid_request","message":"json: unknown field \"accept_queued_oci_hint\""}}`)), Header: make(http.Header)}, nil
		}
		return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(`{"job":{"job_id":"job_old"},"lease":{"attempt_id":"attempt_old"},"attempt_token":"token"}`)), Header: make(http.Header)}, nil
	})
	poll := func() {
		t.Helper()
		reply, err := a.session.client.ClaimPoll(t.Context(), "node", "boot", contract.JobClassOneShot, "job_excluded")
		if err != nil || reply.Claim == nil || reply.Job.JobID != "job_old" || reply.AttemptToken != "token" {
			t.Fatalf("old L1 claim=%+v %v", reply, err)
		}
	}
	poll()
	poll()
	if len(requests) != 3 {
		t.Fatalf("requests=%v", requests)
	}
	var first, retried map[string]any
	json.Unmarshal([]byte(requests[0]), &first)
	json.Unmarshal([]byte(requests[1]), &retried)
	delete(first, "accept_queued_oci_hint")
	if !reflect.DeepEqual(first, retried) || strings.Contains(requests[2], "accept_queued_oci_hint") {
		t.Fatalf("fallback changed poll or forgot session: %v", requests)
	}
	if _, err := a.session.register(t.Context()); err != nil {
		t.Fatal(err)
	}
	poll()
	if len(requests) != 5 || !strings.Contains(requests[3], `"accept_queued_oci_hint":true`) {
		t.Fatalf("reconnect did not renegotiate: %v", requests)
	}
}
