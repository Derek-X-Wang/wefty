package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"testing/iotest"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/internal/connprofile"
	"github.com/Derek-X-Wang/wefty/internal/serviceplan"
)

func fleetPeerReplies(f *setupFixture, status string) {
	open := f.host.fleetClients
	f.host.fleetClients = func(ctx context.Context, paths setupPaths, p connprofile.Profile) (*apiClients, error) {
		clients, err := open(ctx, paths, p)
		if err != nil {
			return clients, err
		}
		for _, client := range []*apiClient{clients.l1, clients.l3} {
			base := client.client.Transport
			client.client.Transport = fleetRoundTripper(func(r *http.Request) (*http.Response, error) {
				if r.URL.Path != "/v1/peer-health" {
					return base.RoundTrip(r)
				}
				code := 200
				result := contract.PeerHealth{Reachable: status == "VERIFIED", CheckedAt: time.Now().UTC()}
				if status == "FAILED" {
					result.ErrorClass = "unavailable"
				}
				body, _ := json.Marshal(result)
				if status == "NOT-RUN" {
					code = 404
					body = []byte("404 page not found\n")
				}
				return &http.Response{StatusCode: code, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(string(body)))}, nil
			})
		}
		return clients, nil
	}
}

func assertFleetPeerChecks(t *testing.T, checks []setupReadinessCheck, status string) {
	t.Helper()
	for _, name := range []string{"L1 → L3 direction", "L3 → L1 direction"} {
		found := false
		for _, check := range checks {
			if check.Check == name {
				found = true
				if check.Status != status {
					t.Errorf("%s: %+v, want %s", name, check, status)
				}
				if status == "NOT-RUN" && check.Detail != "peer probe unavailable on this version" {
					t.Errorf("missing-version detail: %+v", check)
				}
			}
		}
		if !found {
			t.Errorf("missing %s", name)
		}
	}
}

func TestFleetPeerProbeSetupAndDoctor(t *testing.T) {
	for _, role := range []serviceplan.Role{serviceplan.ControlPlane, serviceplan.Join} {
		for _, status := range []string{"VERIFIED", "FAILED", "NOT-RUN"} {
			t.Run(string(role)+"/"+status, func(t *testing.T) {
				f := healthyDoctorFixture(t, role)
				fleetPeerReplies(f, status)
				f.host.readinessBudget = 20 * time.Millisecond
				paths, _ := resolveSetupPaths(f.home)
				report, err := runSetup(t.Context(), f.host, setupRequest{skipSmokeRun: true})
				if status == "FAILED" {
					if classifyCommandReport(err).exitCode != 12 {
						t.Errorf("setup failure: %v", err)
					}
				} else if err != nil {
					t.Fatal(err)
				}
				assertFleetPeerChecks(t, report.Readiness, status)
				doctor, err := runFleetDoctor(t.Context(), f.host, globalOptions{}, paths, role)
				if status == "FAILED" {
					if classifyCommandReport(err).exitCode != 12 {
						t.Errorf("doctor failure: %v", err)
					}
				} else if err != nil {
					t.Fatal(err)
				}
				assertFleetPeerChecks(t, doctor.PolicyEvidence, status)
			})
		}
	}
}

func TestFleetDoctorPeerObserverReadFailures(t *testing.T) {
	for _, direction := range []int{0, 1} {
		for _, mode := range []string{"transport", "reset", "401", "403", "401 reset", "403 reset", "401 oversized", "403 oversized", "invalid observation", "oversized observation"} {
			t.Run(fmt.Sprintf("%d/%s", direction, mode), func(t *testing.T) {
				f := healthyDoctorFixture(t, serviceplan.ControlPlane)
				open := f.host.fleetClients
				f.host.fleetClients = func(ctx context.Context, paths setupPaths, p connprofile.Profile) (*apiClients, error) {
					clients, err := open(ctx, paths, p)
					if err != nil {
						return clients, err
					}
					client := []*apiClient{clients.l1, clients.l3}[direction]
					base := client.client.Transport
					client.client.Transport = fleetRoundTripper(func(r *http.Request) (*http.Response, error) {
						if r.URL.Path != "/v1/peer-health" {
							return base.RoundTrip(r)
						}
						if mode == "transport" {
							return nil, errors.New("observer connection unavailable")
						}
						code, body := 200, "{}"
						if strings.HasPrefix(mode, "401") {
							code, body = 401, "refused"
						}
						if strings.HasPrefix(mode, "403") {
							code, body = 403, "refused"
						}
						if mode == "oversized observation" || strings.HasSuffix(mode, "oversized") {
							body = strings.Repeat("x", 4097)
						}
						reader := io.NopCloser(strings.NewReader(body))
						if strings.HasSuffix(mode, "reset") {
							reader = io.NopCloser(iotest.ErrReader(io.ErrUnexpectedEOF))
						}
						return &http.Response{StatusCode: code, Header: http.Header{}, Body: reader}, nil
					})
					return clients, nil
				}
				paths, _ := resolveSetupPaths(f.home)
				report, err := runFleetDoctor(t.Context(), f.host, globalOptions{}, paths, serviceplan.ControlPlane)
				expected, exit := "NOT-RUN", 13
				if strings.HasPrefix(mode, "401") || strings.HasPrefix(mode, "403") {
					expected, exit = "REFUSED", 3
				}
				if mode == "invalid observation" || mode == "oversized observation" {
					expected, exit = "FAILED", 12
				}
				check := report.PolicyEvidence[3+direction]
				if check.Status != expected || classifyCommandReport(err).exitCode != exit {
					t.Fatalf("observer: %+v error=%v want %s exit %d", check, err, expected, exit)
				}
				if expected == "NOT-RUN" && check.Detail != "peer observer could not be read; direction unverified" {
					t.Fatalf("unobserved direction: %+v", check)
				}
				if report.PolicyEvidence[3+1-direction].Status != "VERIFIED" {
					t.Fatalf("other direction lost: %+v", report.PolicyEvidence)
				}
			})
		}
	}
}

func TestFleetDoctorPeerBudgetExhaustion(t *testing.T) {
	f := healthyDoctorFixture(t, serviceplan.ControlPlane)
	// Use the production 15-second budget: two bounded host commands and two
	// bounded client reads consume twelve seconds before the first peer read.
	f.host.doctorCommand = func(ctx context.Context, _ string, _ ...string) ([]byte, error) { <-ctx.Done(); return nil, ctx.Err() }
	open := f.host.fleetClients
	var peerReads [2]atomic.Int32
	f.host.fleetClients = func(ctx context.Context, paths setupPaths, p connprofile.Profile) (*apiClients, error) {
		clients, err := open(ctx, paths, p)
		if err != nil {
			return clients, err
		}
		for i, client := range []*apiClient{clients.l1, clients.l3} {
			client.client.Transport = fleetRoundTripper(func(r *http.Request) (*http.Response, error) {
				if r.URL.Path == "/v1/peer-health" {
					peerReads[i].Add(1)
				}
				<-r.Context().Done()
				return nil, r.Context().Err()
			})
		}
		return clients, nil
	}
	paths, _ := resolveSetupPaths(f.home)
	report, err := runFleetDoctor(t.Context(), f.host, globalOptions{}, paths, serviceplan.ControlPlane)
	for _, check := range report.PolicyEvidence[3:5] {
		if check.Status != "NOT-RUN" || check.Detail != "doctor time budget expired" {
			t.Errorf("budget exhaustion: %+v", check)
		}
	}
	if peerReads[0].Load() != 1 || peerReads[1].Load() != 0 {
		t.Errorf("peer reads after budget expiry: L1=%d L3=%d", peerReads[0].Load(), peerReads[1].Load())
	}
	if classifyCommandReport(err).exitCode != 13 {
		t.Errorf("transport uncertainty exit: %v", err)
	}
}
