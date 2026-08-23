package agent

import (
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/l1"
)

func TestHelperDeadmanQueuesOnlySuccessfulLiveOCIRenewals(t *testing.T) {
	renewer := &recordingDeadmanRenewer{}
	claim := l1.Claim{Job: l1.Job{JobID: "job", Spec: contract.JobSpec{Kind: contract.JobKindOCI}}}
	lease := l1.AttemptLease{LeaseTTL: 30 * time.Second}
	if err := queueHelperDeadman(renewer, claim, lease); err != nil {
		t.Fatal(err)
	}
	if renewer.calls != 1 || renewer.ttl != lease.LeaseTTL {
		t.Fatalf("successful OCI renewal calls=%d ttl=%s", renewer.calls, renewer.ttl)
	}
	lease.Directive = l1.AttemptDirectiveStop
	if err := queueHelperDeadman(renewer, claim, lease); err != nil {
		t.Fatal(err)
	}
	lease.Directive = l1.AttemptDirectiveRestart
	if err := queueHelperDeadman(renewer, claim, lease); err != nil {
		t.Fatal(err)
	}
	claim.Job.Spec.Kind = contract.JobKindProcess
	lease.Directive = ""
	if err := queueHelperDeadman(renewer, claim, lease); err != nil {
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
