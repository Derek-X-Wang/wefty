package agent

import (
	"fmt"
	"strings"
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
	if renewer.calls != 1 || renewer.ttl != 30*time.Second || renewer.generation != generation {
		t.Fatalf("first admitted OCI renewal calls=%d ttl=%s generation=%+v", renewer.calls, renewer.ttl, renewer.generation)
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
	calls      int
	ttl        time.Duration
	generation workloadrunner.RuntimeGeneration
	err        error
}

func (renewer *recordingDeadmanRenewer) QueueSuccessfulRenewal(_ l1.Claim, ttl time.Duration, generation workloadrunner.RuntimeGeneration) error {
	renewer.calls++
	renewer.ttl = ttl
	renewer.generation = generation
	return renewer.err
}
