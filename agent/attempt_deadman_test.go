package agent

import (
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/l1"
)

func TestHelperDeadmanQueuesOnlySuccessfulAdmittedOCIRenewals(t *testing.T) {
	renewer := &recordingDeadmanRenewer{}
	claim := l1.Claim{Job: l1.Job{JobID: "job", Spec: contract.JobSpec{Kind: contract.JobKindOCI}}}
	lease := l1.AttemptLease{LeaseTTL: 30 * time.Second}
	admission := newAttemptDeadmanAdmission(renewer, claim)
	if err := admission.queue(lease); err != nil {
		t.Fatal(err)
	}
	newer := lease
	newer.LeaseTTL = time.Minute
	if err := admission.queue(newer); err != nil {
		t.Fatal(err)
	}
	if renewer.calls != 0 {
		t.Fatalf("pre-admission OCI renewal calls=%d", renewer.calls)
	}
	if err := admission.admit(); err != nil {
		t.Fatal(err)
	}
	if renewer.calls != 1 || renewer.ttl != newer.LeaseTTL {
		t.Fatalf("first admitted OCI renewal calls=%d ttl=%s", renewer.calls, renewer.ttl)
	}
	lease.Directive = l1.AttemptDirectiveStop
	if err := admission.queue(lease); err != nil {
		t.Fatal(err)
	}
	lease.Directive = l1.AttemptDirectiveRestart
	if err := admission.queue(lease); err != nil {
		t.Fatal(err)
	}
	claim.Job.Spec.Kind = contract.JobKindProcess
	lease.Directive = ""
	processAdmission := newAttemptDeadmanAdmission(renewer, claim)
	if err := processAdmission.admit(); err != nil {
		t.Fatal(err)
	}
	if err := processAdmission.queue(lease); err != nil {
		t.Fatal(err)
	}
	if renewer.calls != 1 {
		t.Fatalf("non-live renewal evidence refreshed helper deadman: calls=%d", renewer.calls)
	}
}

type recordingDeadmanRenewer struct {
	calls int
	ttl   time.Duration
}

func (renewer *recordingDeadmanRenewer) QueueSuccessfulRenewal(_ l1.Claim, ttl time.Duration) error {
	renewer.calls++
	renewer.ttl = ttl
	return nil
}
