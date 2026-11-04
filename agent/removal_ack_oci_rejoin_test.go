//go:build darwin || linux

package agent

import (
	"context"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/internal/testwait"
)

func TestRemovalAcknowledgementOCIRejoinResumesRefusalAfterRegistrationScan(t *testing.T) {
	var calls, scans, purges atomic.Int32
	var refuseHeartbeat, pauseProbe atomic.Bool
	ackInFlight, releaseACK := make(chan struct{}), make(chan struct{})
	afterScan, releaseRegistration := make(chan struct{}), make(chan struct{})
	var releaseACKOnce, releaseRegistrationOnce sync.Once
	defer func() {
		releaseACKOnce.Do(func() { close(releaseACK) })
		releaseRegistrationOnce.Do(func() { close(releaseRegistration) })
	}()
	var session *agentSession
	session, heartbeatClock, ackClock, pending, registrations := newRemovalACKHandoffFixture(t, func(r *http.Request) (*http.Response, bool) {
		if strings.HasSuffix(r.URL.Path, "/removal-acknowledgement") {
			switch calls.Add(1) {
			case 1:
				return nil, true // The response is lost; delivery continues separately.
			case 2:
				close(ackInFlight)
				select {
				case <-releaseACK:
				case <-r.Context().Done():
				}
				return removalACKHandoffResponse(contract.ErrorNodeNotRegistered), true
			default:
				// A refusal from a newly resumed delivery must belong to the
				// new sequence, so another loss cannot be coalesced away.
				if sequence := session.rejoinSequence.Load(); sequence != 1 {
					t.Errorf("resumed ACK observed rejoin sequence %d, want 1", sequence)
				}
				return removalACKHandoffResponse(""), true
			}
		}
		if strings.HasSuffix(r.URL.Path, "/heartbeat") && refuseHeartbeat.CompareAndSwap(true, false) {
			return removalACKHandoffResponse(contract.ErrorNodeNotRegistered), true
		}
		return nil, false
	})
	controller := session.removals
	session.ociBootBarrier = readyOCIBootBarrier{}
	probe := capabilityProbeFunc(func(ctx context.Context) (CapabilityProbeResult, error) {
		if pauseProbe.CompareAndSwap(true, false) {
			close(afterScan)
			select {
			case <-releaseRegistration:
			case <-ctx.Done():
				return CapabilityProbeResult{}, ctx.Err()
			}
		}
		return CapabilityProbeResult{Capabilities: map[string]bool{
			"kind:oci": true, "runtime_handler:io.containerd.runc.v2": true,
		}}, nil
	})
	session.capabilities = newCapabilityState(session.registration.Capabilities, probe, ackClock, time.Minute, nil)
	purge := controller.purgeJob
	controller.purgeJob = func(ctx context.Context, jobID string) error {
		purges.Add(1)
		return purge(ctx, jobID)
	}
	if err := controller.resume(t.Context()); err != nil {
		t.Fatal(err)
	}
	ackClock.Advance(waitRemovalACKDelay(t, ackClock))
	reviewWait(t, "ACK replay held in flight", ackInFlight)
	controller.managed.(*removalACKResumeResource).onResume = func() {
		scans.Add(1)
		pauseProbe.Store(true) // The functional probe follows this removal scan.
	}
	notified := make(chan removalAcknowledgementFailure, 1)
	notify := controller.notifyAcknowledgementFailure
	controller.notifyAcknowledgementFailure = func(failure removalAcknowledgementFailure) {
		notify(failure)
		notified <- failure
	}
	ctx, cancel := context.WithCancel(t.Context())
	loopDone := make(chan struct{})
	failures := make(chan destinationError, 1)
	go func() { defer close(loopDone); session.heartbeatLoop(ctx, failures) }()
	defer func() { cancel(); reviewWait(t, "OCI heartbeat loop stopped", loopDone) }()
	waitRemovalACKDelay(t, heartbeatClock)
	refuseHeartbeat.Store(true)
	session.heartbeatWake <- struct{}{}
	reviewWait(t, "OCI registration held after removal scan", afterScan)
	if sequence := session.rejoinSequence.Load(); sequence != 0 || !controller.acknowledgementPending(pending) {
		t.Fatalf("registration scan did not skip a live ACK at sequence 0: sequence=%d", sequence)
	}
	releaseACKOnce.Do(func() { close(releaseACK) })
	select {
	case failure := <-notified:
		if failure.observedRejoin != 0 {
			t.Fatalf("delayed refusal observed sequence %d, want 0", failure.observedRejoin)
		}
	case <-time.After(testwait.Bound):
		t.Fatal("delayed ACK refusal was not published")
	}
	controller.wait() // The terminal task is gone before registration returns.
	releaseRegistrationOnce.Do(func() { close(releaseRegistration) })
	waitRemovalACKDelay(t, heartbeatClock) // Registration and post-increment delivery finished.
	waitRemovalACKDelay(t, heartbeatClock) // The queued sequence-0 refusal was coalesced.
	cancel()
	reviewWait(t, "OCI heartbeat loop stopped", loopDone)
	select {
	case failure := <-failures:
		t.Fatalf("OCI recovery returned a fatal refusal: %v", failure.err)
	default:
	}
	_, found, err := controller.outbox.spool.removalIntent(t.Context(), pending.jobID)
	if err != nil || found || calls.Load() != 3 || registrations.Load() != 2 || session.rejoinSequence.Load() != 1 {
		t.Fatalf("OCI rejoin stranded row=%t err=%v ACKs=%d registrations=%d sequence=%d, want false/nil/3/2/1", found, err, calls.Load(), registrations.Load(), session.rejoinSequence.Load())
	}
	if scans.Load() != 1 || purges.Load() != 1 {
		t.Fatalf("OCI rejoin repeated removal work: scans=%d purges=%d, want 1/1", scans.Load(), purges.Load())
	}
	if !session.capabilities.snapshot().Capabilities["kind:oci"] {
		t.Fatal("OCI rejoin did not restore the functional capability")
	}
}

func TestRemovalAcknowledgementOCIResumptionSkipsConsumedSnapshot(t *testing.T) {
	var calls atomic.Int32
	session, _, clock, pending, _ := newRemovalACKHandoffFixture(t, func(r *http.Request) (*http.Response, bool) {
		if !strings.HasSuffix(r.URL.Path, "/removal-acknowledgement") {
			return nil, false
		}
		switch calls.Add(1) {
		case 1:
			return nil, true
		case 2:
			return removalACKHandoffResponse(contract.ErrorNodeNotRegistered), true
		default:
			return removalACKHandoffResponse(""), true
		}
	})
	controller := session.removals
	session.ociBootBarrier = readyOCIBootBarrier{}
	session.capabilities.probe = capabilityProbeFunc(func(context.Context) (CapabilityProbeResult, error) {
		return CapabilityProbeResult{Capabilities: map[string]bool{
			"kind:oci": true, "runtime_handler:io.containerd.runc.v2": true,
		}}, nil
	})
	if err := controller.resume(t.Context()); err != nil {
		t.Fatal(err)
	}
	clock.Advance(waitRemovalACKDelay(t, clock))
	controller.wait() // The refusal leaves a marker and no live delivery.
	snapshotted, release, resumed := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	resume := controller.resumeAcknowledgement
	controller.resumeAcknowledgement = func(ctx context.Context, removal localRemoval) (bool, error) {
		close(snapshotted)
		select {
		case <-release:
		case <-ctx.Done():
		}
		return resume(ctx, removal)
	}
	var resumeErr error
	go func() { defer close(resumed); resumeErr = controller.resumeAcknowledgements(t.Context()) }()
	defer func() {
		releaseOnce.Do(func() { close(release) })
		reviewWait(t, "snapshot resumption stopped", resumed)
	}()
	reviewWait(t, "marker snapshotted before admission", snapshotted)
	// The real OCI recovery path admits and completes the same removal while
	// the other pass still holds its snapshot. No task remains to deduplicate.
	if err := session.recoverOCIRuntimePublished(t.Context(), nil); err != nil {
		t.Fatal(err)
	}
	controller.mu.Lock()
	markers, tasks := len(controller.ackResumptions), len(controller.ackTasks)
	controller.mu.Unlock()
	_, found, err := controller.outbox.spool.removalIntent(t.Context(), pending.jobID)
	if err != nil || found || calls.Load() != 3 || markers != 0 || tasks != 0 {
		t.Fatalf("concurrent OCI recovery: row=%t err=%v ACKs=%d markers=%d tasks=%d, want false/nil/3/0/0", found, err, calls.Load(), markers, tasks)
	}
	releaseOnce.Do(func() { close(release) })
	reviewWait(t, "stale snapshot resumed", resumed)
	if resumeErr != nil || calls.Load() != 3 {
		t.Fatalf("stale snapshot sent an extra ACK: calls=%d err=%v, want 3/nil", calls.Load(), resumeErr)
	}
}
