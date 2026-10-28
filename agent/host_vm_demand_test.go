package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"reflect"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/l1"
	"github.com/Derek-X-Wang/wefty/runner/lima"
)

func TestHostVMDemandIncludesCleanupAndExcludesProcessWork(t *testing.T) {
	a := &Agent{session: &agentSession{residentKind: map[string]string{"computer-1": contract.JobKindOCI, "process-1": contract.JobKindProcess}}}
	a.session.observeHostVMDirectives(l1.HeartbeatResponse{BackupDirectives: []l1.ComputerBackupDirective{{BackupID: "backup-1"}}})
	items, err := a.HostVMDemand(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	want := []lima.DemandItem{{Kind: "oci", ID: "computer-1"}, {Kind: "backup", ID: "backup-1"}}
	if !reflect.DeepEqual(items, want) {
		t.Fatalf("demand=%v want=%v", items, want)
	}
	a.session.residentKind = map[string]string{}
	a.session.observeHostVMDirectives(l1.HeartbeatResponse{})
	release := a.BeginHostVMLoadImage()
	items, err = a.HostVMDemand(t.Context())
	if err != nil || len(items) != 1 || items[0].Kind != "load-image" {
		t.Fatalf("load-image demand=%v %v", items, err)
	}
	release()
	items, err = a.HostVMDemand(t.Context())
	if err != nil || len(items) != 0 {
		t.Fatalf("finished load-image demand=%v %v", items, err)
	}
}

type fakeHostVMIdleCycle struct {
	due  bool
	stop func(func() bool) (bool, error)
}

func (cycle fakeHostVMIdleCycle) IdleDue(context.Context) bool              { return cycle.due }
func (cycle fakeHostVMIdleCycle) IdleReason() contract.CapabilityReasonCode { return "" }
func (cycle fakeHostVMIdleCycle) IdleStop(_ context.Context, check func() bool) (bool, error) {
	return cycle.stop(check)
}

func TestHostVMIdleWaitsForL1AcknowledgementAndRechecksCleanup(t *testing.T) {
	for _, mode := range []string{"acknowledged", "unacknowledged", "cleanup-arrives"} {
		t.Run(mode, func(t *testing.T) {
			clock := newManualClock(time.Unix(1000, 0))
			probe := capabilityProbeFunc(func(context.Context) (CapabilityProbeResult, error) {
				return CapabilityProbeResult{Capabilities: map[string]bool{"kind:oci": true}}, nil
			})
			capabilities := newCapabilityState(map[string]bool{"kind:process": true}, probe, clock, 0, nil)
			if err := capabilities.refresh(t.Context()); err != nil {
				t.Fatal(err)
			}
			a := &Agent{capabilities: capabilities, session: &agentSession{capabilities: capabilities, registration: contract.NodeRegistration{NodeID: "node", BootSessionID: "boot"}, residentKind: map[string]string{}}}
			published := false
			a.session.client = &Client{baseURL: "http://control.invalid", operationTimeout: time.Second, httpClient: &http.Client{Transport: captureRoundTrip(func(request *http.Request) (*http.Response, error) {
				var heartbeat l1.HeartbeatRequest
				if err := json.NewDecoder(request.Body).Decode(&heartbeat); err != nil {
					t.Fatal(err)
				}
				if !published && (heartbeat.Capabilities["kind:oci"] || heartbeat.CapabilityReasonCode != contract.CapabilityReasonHostVMIdle) {
					t.Fatalf("first heartbeat was not idle withdrawal: %+v", heartbeat)
				}
				published = true
				snapshot := capabilities.snapshot()
				node := l1.Node{NodeID: "node", BootSessionID: "boot", Capabilities: snapshot.Capabilities, CapabilityRevision: snapshot.Revision, CapabilityObservedAt: snapshot.ObservedAt, MissingCapabilities: snapshot.MissingCapabilities, CapabilityReasonCode: snapshot.ReasonCode}
				response := l1.HeartbeatResponse{Node: node}
				if mode == "unacknowledged" {
					response.CapabilityRevision = 0
				}
				if mode == "cleanup-arrives" {
					response.BackupDirectives = []l1.ComputerBackupDirective{{BackupID: "backup-new"}}
				}
				payload, _ := json.Marshal(response)
				return &http.Response{StatusCode: 200, Body: io.NopCloser(bytes.NewReader(payload)), Header: make(http.Header)}, nil
			})}}
			stopped := false
			err := a.PollHostVM(t.Context(), fakeHostVMIdleCycle{due: true, stop: func(check func() bool) (bool, error) {
				if !published {
					t.Fatal("cycle acquired before withdrawal acknowledgement")
				}
				if a.session.claimMu.TryLock() {
					a.session.claimMu.Unlock()
					t.Fatal("cycle ran without claim lock")
				}
				if mode == "unacknowledged" {
					t.Fatal("idle stop ran without L1 acknowledgement")
				}
				if !check() {
					return false, nil
				}
				stopped = true
				return true, nil
			}})
			if mode == "acknowledged" && (err != nil || !stopped) {
				t.Fatalf("idle stop=%t err=%v", stopped, err)
			}
			if mode == "unacknowledged" && (err == nil || stopped) {
				t.Fatalf("unacknowledged stop=%t err=%v", stopped, err)
			}
			if mode == "cleanup-arrives" && (err != nil || stopped || !a.CapabilitySnapshot().Capabilities["kind:oci"]) {
				t.Fatalf("cleanup did not cancel and restore: stopped=%t err=%v snapshot=%+v", stopped, err, a.CapabilitySnapshot())
			}
		})
	}
}
