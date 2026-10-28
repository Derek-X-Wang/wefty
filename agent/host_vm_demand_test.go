package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"reflect"
	"strings"
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
	snapshot := &lima.DemandState{}
	a.ConfigureHostVMDemand(snapshot, nil)
	release := a.BeginHostVMLoadImage()
	if snapshot.LastDemandAt().IsZero() {
		t.Fatal("image import did not stamp demand between watchdog polls")
	}
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

func (cycle fakeHostVMIdleCycle) MarkStopping(context.Context) error { return nil }
func (cycle fakeHostVMIdleCycle) FinishStopping() error              { return nil }

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
			if mode == "cleanup-arrives" {
				a.session.ociBootBarrier = &recordingOCIBootBarrier{ready: true, invalidate: func() { t.Error("idle cancel invalidated a live helper session") }, ensure: func(context.Context) error { return errors.New("cancel replaced helper session") }}
			}
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

func TestSlowHostVMStopDoesNotBlockNodeOperations(t *testing.T) {
	clock := newManualClock(time.Unix(1000, 0))
	probe := capabilityProbeFunc(func(context.Context) (CapabilityProbeResult, error) {
		return CapabilityProbeResult{Capabilities: map[string]bool{"kind:oci": true}}, nil
	})
	caps := newCapabilityState(map[string]bool{"kind:process": true}, probe, clock, 0, nil)
	if err := caps.refresh(t.Context()); err != nil {
		t.Fatal(err)
	}
	a := &Agent{capabilities: caps, session: &agentSession{capabilities: caps, ociBootBarrier: readyOCIBootBarrier{}, registration: contract.NodeRegistration{NodeID: "node", BootSessionID: "boot"}, residentKind: map[string]string{}}}
	a.session.client = &Client{baseURL: "http://control.invalid", operationTimeout: time.Second, httpClient: &http.Client{Transport: captureRoundTrip(func(request *http.Request) (*http.Response, error) {
		if strings.HasSuffix(request.URL.Path, "/claim") {
			return &http.Response{StatusCode: 204, Body: io.NopCloser(strings.NewReader("")), Header: make(http.Header)}, nil
		}
		snapshot := caps.snapshot()
		payload, _ := json.Marshal(l1.HeartbeatResponse{Node: l1.Node{NodeID: "node", BootSessionID: "boot", Capabilities: snapshot.Capabilities, CapabilityRevision: snapshot.Revision, MissingCapabilities: snapshot.MissingCapabilities, CapabilityReasonCode: snapshot.ReasonCode}})
		return &http.Response{StatusCode: 200, Body: io.NopCloser(bytes.NewReader(payload)), Header: make(http.Header)}, nil
	})}}
	entered, release, done := make(chan struct{}), make(chan struct{}), make(chan error, 1)
	go func() {
		done <- a.PollHostVM(t.Context(), fakeHostVMIdleCycle{due: true, stop: func(check func() bool) (bool, error) {
			if !check() {
				return false, nil
			}
			close(entered)
			<-release
			return true, nil
		}})
	}()
	<-entered
	defer func() {
		close(release)
		if err := <-done; err != nil {
			t.Error(err)
		}
	}()
	for _, operation := range []struct {
		name string
		run  func() error
	}{
		{"heartbeat", func() error { _, err := a.session.heartbeatOCIPinned(t.Context()); return err }},
		{"process claim", func() error {
			_, _, err := a.session.claim(t.Context(), newAdmissionGate(1), contract.JobClassOneShot, false)
			return err
		}},
		{"doctor demand", func() error { _, err := a.HostVMDemand(t.Context()); return err }},
	} {
		result := make(chan error, 1)
		go func() { result <- operation.run() }()
		select {
		case err := <-result:
			if err != nil {
				t.Errorf("%s: %v", operation.name, err)
			}
		case <-time.After(time.Second):
			t.Errorf("slow stop blocked %s", operation.name)
		}
	}
}

type quietIdleBarrier struct{ recordingOCIBootBarrier }

func (*quietIdleBarrier) OCIRecoverySkip(context.Context) error { return &lima.HostVMIdleSkipError{} }

func newHostVMControlTestAgent(t *testing.T) *Agent {
	t.Helper()
	clock := newManualClock(time.Unix(1000, 0))
	probe := capabilityProbeFunc(func(context.Context) (CapabilityProbeResult, error) {
		return CapabilityProbeResult{Capabilities: map[string]bool{"kind:oci": true}}, nil
	})
	caps := newCapabilityState(map[string]bool{"kind:process": true}, probe, clock, 0, nil)
	if err := caps.refresh(t.Context()); err != nil {
		t.Fatal(err)
	}
	a := &Agent{capabilities: caps, session: &agentSession{capabilities: caps, registration: contract.NodeRegistration{NodeID: "node", BootSessionID: "boot"}, residentKind: map[string]string{}, ociIntent: func(context.Context) (OCIIntentObservation, error) {
		return OCIIntentObservation{Enabled: true, Revision: 2}, nil
	}}}
	a.session.client = &Client{baseURL: "http://control.invalid", operationTimeout: time.Second, httpClient: &http.Client{Transport: captureRoundTrip(func(*http.Request) (*http.Response, error) {
		snapshot := caps.snapshot()
		payload, _ := json.Marshal(l1.HeartbeatResponse{Node: l1.Node{NodeID: "node", BootSessionID: "boot", Capabilities: snapshot.Capabilities, CapabilityRevision: snapshot.Revision, MissingCapabilities: snapshot.MissingCapabilities, CapabilityReasonCode: snapshot.ReasonCode}})
		return &http.Response{StatusCode: 200, Body: io.NopCloser(bytes.NewReader(payload)), Header: make(http.Header)}, nil
	})}}
	return a
}

func TestQuietIdleHeartbeatSkipsEnsureAndPublishesOnce(t *testing.T) {
	a := newHostVMControlTestAgent(t)
	ensures, publications, logs := 0, 0, 0
	a.session.ociBootBarrier = &quietIdleBarrier{recordingOCIBootBarrier: recordingOCIBootBarrier{ensure: func(context.Context) error { ensures++; return errors.New("idle helper must not start") }}}
	transport := a.session.client.httpClient.Transport
	a.session.client.httpClient.Transport = captureRoundTrip(func(r *http.Request) (*http.Response, error) { publications++; return transport.RoundTrip(r) })
	a.session.logf = func(string, ...any) { logs++ }
	for range 3 {
		if _, err := a.session.heartbeatOCIPinned(t.Context()); err != nil {
			t.Fatal(err)
		}
		if snapshot := a.CapabilitySnapshot(); snapshot.ReasonCode != contract.CapabilityReasonHostVMIdle || snapshot.PendingPublicationRevision != 0 {
			t.Fatalf("idle transition was not published: %+v", snapshot)
		}
	}
	if ensures != 0 || publications != 3 || logs != 1 {
		t.Fatalf("quiet idle: ensure=%d publications=%d transition logs=%d", ensures, publications, logs)
	}
}

func TestOnDemandClearsOffLatchAndPublishesIdle(t *testing.T) {
	a := newHostVMControlTestAgent(t)
	a.capabilities.suppressOCIIntent(errOCIIntentDisabled)
	a.session.ociBootBarrier = &recordingOCIBootBarrier{ensure: func(context.Context) error { t.Fatal("on-demand started the host VM"); return nil }}
	if err := a.EnableOnDemand(t.Context(), 2); err != nil {
		t.Fatal(err)
	}
	snapshot := a.CapabilitySnapshot()
	if a.capabilities.ociIntentDisabled.Load() || snapshot.ReasonCode != contract.CapabilityReasonHostVMIdle || snapshot.PendingPublicationRevision != 0 {
		t.Fatalf("on-demand did not publish idle: %+v", snapshot)
	}
}
