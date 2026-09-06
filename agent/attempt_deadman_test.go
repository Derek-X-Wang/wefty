package agent

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/l1"
	workloadrunner "github.com/Derek-X-Wang/wefty/runner"
)

func TestHelperDeadmanQueuesOnlyLiveAdmittedOCIRenewals(t *testing.T) {
	clock := newManualClock(time.Unix(10_000, 0))
	renewer := &recordingDeadmanRenewer{}
	claim := l1.Claim{Job: l1.Job{JobID: "job", Spec: contract.JobSpec{Kind: contract.JobKindOCI}}}
	var logs []string
	admission := newAttemptDeadmanAdmission(renewer, claim, clock, func(format string, args ...any) {
		logs = append(logs, fmt.Sprintf(format, args...))
	})
	first := l1.AttemptLease{LeaseTTL: 30 * time.Second}
	if err := admission.queue(first, clock.Now().Add(first.LeaseTTL)); err != nil {
		t.Fatal(err)
	}
	newer := first
	newer.LeaseTTL = time.Minute
	absoluteExpiry := clock.Now().Add(40 * time.Second)
	if err := admission.queue(newer, absoluteExpiry); err != nil {
		t.Fatal(err)
	}
	if renewer.calls != 0 {
		t.Fatalf("pre-admission OCI renewal calls=%d", renewer.calls)
	}
	clock.Advance(10 * time.Second)
	generation := workloadrunner.RuntimeGeneration{InstanceID: "helper-a", Generation: 7}
	if err := admission.admit(generation); err != nil {
		t.Fatal(err)
	}
	if renewer.calls != 1 || !renewer.expiresAt.Equal(absoluteExpiry) || renewer.generation != generation {
		t.Fatalf("first admitted OCI renewal calls=%d expires_at=%s generation=%+v", renewer.calls, renewer.expiresAt, renewer.generation)
	}

	admission.terminate()
	if err := admission.queue(first, clock.Now().Add(first.LeaseTTL)); err != nil {
		t.Fatal(err)
	}
	if err := admission.admit(generation); err != nil {
		t.Fatal(err)
	}
	if renewer.calls != 1 {
		t.Fatalf("terminal admission refreshed helper deadman: calls=%d", renewer.calls)
	}

	renewer.err = &AttemptDeadmanGenerationMismatchError{
		Expected: generation,
		Observed: workloadrunner.RuntimeGeneration{InstanceID: "helper-b", Generation: 8},
	}
	mismatched := newAttemptDeadmanAdmission(renewer, claim, clock, func(format string, args ...any) {
		logs = append(logs, fmt.Sprintf(format, args...))
	})
	if err := mismatched.admit(generation); err != nil {
		t.Fatal(err)
	}
	if err := mismatched.queue(first, clock.Now().Add(first.LeaseTTL)); err != nil {
		t.Fatalf("session-generation mismatch was not dropped: %v", err)
	}
	if !strings.Contains(strings.Join(logs, "\n"), "reason=session_generation_mismatch") {
		t.Fatalf("session-generation mismatch lacked typed evidence: %q", logs)
	}
	if err := mismatched.queue(first, clock.Now().Add(first.LeaseTTL)); err != nil {
		t.Fatalf("terminal mismatch gate returned a later renewal error: %v", err)
	}
	if renewer.calls != 2 {
		t.Fatalf("session-generation mismatch left renewal gate open: calls=%d", renewer.calls)
	}
}

func TestHelperDeadmanDropsStopAndRestartDirectives(t *testing.T) {
	renewer := &recordingDeadmanRenewer{}
	claim := l1.Claim{Job: l1.Job{JobID: "job", Spec: contract.JobSpec{Kind: contract.JobKindOCI}}}
	clock := newManualClock(time.Unix(11_000, 0))
	generation := workloadrunner.RuntimeGeneration{InstanceID: "helper", Generation: 1}
	admission := newAttemptDeadmanAdmission(renewer, claim, clock, nil)
	if err := admission.admit(generation); err != nil {
		t.Fatal(err)
	}
	for _, directive := range []l1.AttemptDirective{l1.AttemptDirectiveStop, l1.AttemptDirectiveRestart} {
		lease := l1.AttemptLease{LeaseTTL: time.Minute, Directive: directive}
		if err := admission.queue(lease, clock.Now().Add(lease.LeaseTTL)); err != nil {
			t.Fatalf("directive %q: %v", directive, err)
		}
	}
	if renewer.calls != 0 {
		t.Fatalf("stop/restart directives reached helper deadman queue: calls=%d", renewer.calls)
	}
}

func TestHelperDeadmanAdmissionFailsLoudlyWhenUnwired(t *testing.T) {
	claim := l1.Claim{Job: l1.Job{JobID: "job", Spec: contract.JobSpec{Kind: contract.JobKindOCI}}}
	lease := l1.AttemptLease{LeaseTTL: time.Second}
	generation := workloadrunner.RuntimeGeneration{InstanceID: "helper", Generation: 1}
	var missing *attemptDeadmanAdmission
	if err := missing.queue(lease, time.Now().Add(time.Second)); err == nil {
		t.Fatal("nil admission queue succeeded")
	}
	if err := missing.admit(generation); err == nil {
		t.Fatal("nil admission admit succeeded")
	}
	unwired := newAttemptDeadmanAdmission(nil, claim, systemClock{}, nil)
	if err := unwired.admit(generation); err == nil {
		t.Fatal("OCI admission without a renewer succeeded")
	}
}

type recordingDeadmanRenewer struct {
	mu          sync.Mutex
	renewed     chan struct{}
	renewedOnce sync.Once
	calls       int
	expiresAt   time.Time
	generation  workloadrunner.RuntimeGeneration
	err         error
}

func (renewer *recordingDeadmanRenewer) QueueSuccessfulRenewal(_ l1.Claim, expiresAt time.Time, generation workloadrunner.RuntimeGeneration) error {
	renewer.mu.Lock()
	defer renewer.mu.Unlock()
	renewer.calls++
	renewer.expiresAt = expiresAt
	renewer.generation = generation
	if renewer.renewed != nil {
		renewer.renewedOnce.Do(func() { close(renewer.renewed) })
	}
	return renewer.err
}

func newRecordingDeadmanRenewer() *recordingDeadmanRenewer {
	return &recordingDeadmanRenewer{renewed: make(chan struct{})}
}

func (renewer *recordingDeadmanRenewer) waitForRenewal(t *testing.T) {
	t.Helper()
	select {
	case <-renewer.renewed:
	case <-time.After(5 * time.Second):
		t.Fatal("OCI helper admission did not produce a deadman renewal")
	}
}

func (renewer *recordingDeadmanRenewer) snapshot() (int, time.Time, workloadrunner.RuntimeGeneration) {
	renewer.mu.Lock()
	defer renewer.mu.Unlock()
	return renewer.calls, renewer.expiresAt, renewer.generation
}

func readyOCIHelperGeneration() workloadrunner.RuntimeGeneration {
	helperSession, _ := (readyOCIBootBarrier{}).Generation()
	return workloadrunner.RuntimeGeneration{
		InstanceID: helperSession.HelperInstanceID,
		Generation: helperSession.SessionGeneration,
	}
}

func admitReadyOCIHelper(request workloadrunner.Request) error {
	if request.OCIHelperAdmitted == nil {
		return errors.New("OCI helper admission callback is not wired")
	}
	return request.OCIHelperAdmitted(readyOCIHelperGeneration())
}
