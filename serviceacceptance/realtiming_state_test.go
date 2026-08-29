package serviceacceptance

import (
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/l1"
)

func restartPendingObserved(job l1.Job) bool {
	if job.State != contract.JobQueued || job.Status != "restart-pending" || job.NextRestartAt == nil {
		return false
	}
	if job.Spec.PublishedPort == nil {
		return job.Ready == nil
	}
	return job.Ready != nil && !*job.Ready
}

func TestRestartPendingObservationDistinguishesPortlessAndPublishedReadiness(t *testing.T) {
	next := time.Now()
	ready := false
	port := 8080
	for _, test := range []struct {
		name string
		job  l1.Job
		want bool
	}{
		{name: "portless OCI has no publication readiness", job: l1.Job{State: contract.JobQueued, Status: "restart-pending", ServiceJob: &l1.ServiceJob{NextRestartAt: &next}}, want: true},
		{name: "published service is explicitly unavailable", job: l1.Job{State: contract.JobQueued, Status: "restart-pending", ServiceJob: &l1.ServiceJob{NextRestartAt: &next, Ready: &ready}, Spec: contract.JobSpec{PublishedPort: &port}}, want: true},
		{name: "published service cannot omit readiness", job: l1.Job{State: contract.JobQueued, Status: "restart-pending", ServiceJob: &l1.ServiceJob{NextRestartAt: &next}, Spec: contract.JobSpec{PublishedPort: &port}}, want: false},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := restartPendingObserved(test.job); got != test.want {
				t.Fatalf("restartPendingObserved() = %t, want %t", got, test.want)
			}
		})
	}
}
