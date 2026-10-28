package agent

import (
	"context"
	"io"
	"net/http"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/runner/lima"
)

// HV-4 node half: delivery crosses the real claim-poll reader and demand
// publication. The Lima matrix separately covers the serialized shutdown.
func TestColdStartHintIdleStopRaceMatrix(t *testing.T) {
	for _, arrival := range []string{"before-withdrawal", "during-withdrawal", "at-acknowledgement", "after-claim-recheck", "during-shutdown", "after-shutdown"} {
		t.Run(arrival, func(t *testing.T) {
			a := newHostVMControlTestAgent(t)
			a.session.registration.OS = "darwin"
			if _, err := a.session.publishCapabilityHeartbeat(t.Context(), nil); err != nil {
				t.Fatal(err)
			}
			clock := a.capabilities.clock.(*manualClock)
			snapshot := &lima.DemandState{}
			a.ConfigureHostVMDemand(snapshot, nil)
			ensures := 0
			barrier := &recordingOCIBootBarrier{ready: true, ensure: func(context.Context) error { ensures++; return nil }}
			a.session.ociBootBarrier = barrier
			original := a.session.client.httpClient.Transport
			claimDone := make(chan error, 1)
			pollHint := func() error {
				claim, _, err := a.session.claim(t.Context(), newAdmissionGate(1), contract.JobClassOneShot, false)
				if claim != nil {
					t.Error("hint minted an Attempt")
				}
				return err
			}
			delivered := false
			a.session.client.httpClient.Transport = captureRoundTrip(func(r *http.Request) (*http.Response, error) {
				if strings.HasSuffix(r.URL.Path, "/claim") {
					return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(`{"queued_oci_work":true}`)), Header: make(http.Header)}, nil
				}
				deliver := func() {
					delivered = true
					go func() { claimDone <- pollHint() }()
					// Claim admission waits for this capability acknowledgement while it
					// owns claimMu. Thus the final idle recheck must follow hint delivery.
					deadline := time.Now().Add(time.Second)
					for a.session.claimMu.TryLock() {
						a.session.claimMu.Unlock()
						if time.Now().After(deadline) {
							t.Fatal("claim did not reach publication barrier")
						}
						runtime.Gosched()
					}
				}
				if !delivered && arrival == "during-withdrawal" {
					deliver()
				}
				response, err := original.RoundTrip(r)
				if !delivered && arrival == "at-acknowledgement" {
					deliver()
				}
				return response, err
			})
			if arrival == "before-withdrawal" {
				if err := pollHint(); err != nil {
					t.Fatal(err)
				}
			}
			shutdowns := 0
			err := a.PollHostVM(t.Context(), fakeHostVMIdleCycle{due: true, stop: func(check func() bool) (bool, error) {
				if !check() {
					return false, nil
				}
				if arrival == "after-claim-recheck" {
					if err := pollHint(); err != nil {
						return false, err
					}
					items, err := a.HostVMDemand(t.Context())
					if err != nil {
						return false, err
					}
					if len(items) > 0 {
						return false, nil
					}
				}
				barrier.Invalidate()
				shutdowns++
				if arrival == "during-shutdown" {
					if err := a.RecoverOCIRuntimeCapabilities(t.Context()); err == nil || !strings.Contains(err.Error(), "host VM is stopping; retry in a moment") {
						t.Fatalf("owner stop reply=%v", err)
					}
					if err := pollHint(); err != nil {
						return false, err
					}
				}
				clock.Advance(20 * time.Second)
				return true, nil
			}})
			if err != nil {
				t.Fatal(err)
			}
			if delivered {
				if err := <-claimDone; err != nil {
					t.Fatal(err)
				}
			}
			if arrival == "after-shutdown" {
				if err := pollHint(); err != nil {
					t.Fatal(err)
				}
			}
			items, demandErr := a.HostVMDemand(t.Context())
			if demandErr != nil || len(items) == 0 {
				t.Fatalf("queued hint was lost: %v %v", items, demandErr)
			}
			early := arrival != "during-shutdown" && arrival != "after-shutdown"
			if early && (shutdowns != 0 || ensures != 0 || !a.CapabilitySnapshot().Capabilities["kind:oci"]) {
				t.Fatalf("early hint did not preserve ready generation: shutdowns=%d ensures=%d snapshot=%+v", shutdowns, ensures, a.CapabilitySnapshot())
			}
			if !early {
				if shutdowns != 1 {
					t.Fatal("shutdown did not finish")
				}
				if err := a.RecoverOCIRuntimeCapabilities(t.Context()); err != nil {
					t.Fatal(err)
				}
				if ensures != 1 || !a.CapabilitySnapshot().Capabilities["kind:oci"] {
					t.Fatalf("late hint did not recover: ensures=%d", ensures)
				}
			}
		})
	}
}
