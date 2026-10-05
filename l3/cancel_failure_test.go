package l3

import (
	"encoding/json"
	"testing"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/l1"
)

func TestCanceledJobFailureTakesPrecedence(t *testing.T) {
	for _, evidence := range []string{
		``,
		`,"attempts":[{"attempt_id":"old","state":"failed","result":{"exit_code":0}}]`,
		`,"attempts":[{"attempt_id":"old","node_id":"node","state":"lost","late_result":{"kind":"observation","result":{"exit_code":0},"late":true}}]`,
	} {
		t.Run(evidence, func(t *testing.T) {
			s, _, _ := recoveryStore(t)
			run, _ := dispatchedRecoveryRun(t, s, "canceled")
			var job l1.Job
			if err := json.Unmarshal([]byte(`{"state":"failed","outcome":"canceled","failure_reason":"older prestart failure"`+evidence+`}`), &job); err != nil {
				t.Fatal(err)
			}
			if got := JobFailureReason(job); got != "the L1 job was canceled" {
				t.Fatalf("reason=%q", got)
			}
			client := &fixedJobClient{jobs: map[string]l1.Job{run.JobID: job}}
			settled := reconcileUntilTerminal(t, s, client, run.RunID)
			if settled.Status != contract.RunFailed || settled.FailureReason != "the L1 job was canceled" {
				t.Fatalf("settled=%#v", settled)
			}
		})
	}
}
