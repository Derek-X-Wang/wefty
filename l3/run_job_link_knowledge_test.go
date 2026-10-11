package l3

import (
	"context"
	"testing"

	"github.com/Derek-X-Wang/wefty/contract"
)

func TestRunJobLinkKnowledge(t *testing.T) {
	s, _, clock := recoveryStore(t)
	attempt := clock.Now()
	regression := func(id string) *contract.APIError {
		return &contract.APIError{Code: contract.ErrorNotFound, Details: map[string]any{"reason": l1RegressedReason, "l1_job_id": id}}
	}
	tests := []struct {
		name    string
		fixture linkKnowledgeFixture
		state   linkKnowledgeState
		id      string
		invalid bool
	}{
		{name: "unknown", state: linkUnknown},
		{name: "attempted", fixture: linkKnowledgeFixture{attemptedAt: &attempt}, state: linkAttemptedUnconfirmed},
		{name: "refused-submit-still-unconfirmed", fixture: linkKnowledgeFixture{attemptedAt: &attempt, diagnostic: &contract.APIError{Code: contract.ErrorForbidden}}, state: linkAttemptedUnconfirmed},
		{name: "linked", fixture: linkKnowledgeFixture{runJob: linkID("job"), outboxJob: linkID("job"), acknowledged: true, attemptedAt: &attempt}, state: linkLinked, id: "job"},
		{name: "terminal-linked-settled-flag", fixture: linkKnowledgeFixture{runJob: linkID("job"), outboxJob: linkID("job"), settled: true}, state: linkLinked, id: "job"},
		{name: "legacy-acknowledged", fixture: linkKnowledgeFixture{outboxJob: linkID("job"), acknowledged: true, attemptedAt: &attempt}, state: linkLinked, id: "job"},
		{name: "missing-settled", fixture: linkKnowledgeFixture{settled: true, attemptedAt: &attempt}, state: linkMissingSettled},
		{name: "legacy-acknowledged-later-missing", fixture: linkKnowledgeFixture{outboxJob: linkID("job"), acknowledged: true, settled: true, attemptedAt: &attempt}, state: linkRegressed, id: "job"},
		{name: "legacy-acknowledged-missing-diagnostic", fixture: linkKnowledgeFixture{outboxJob: linkID("job"), acknowledged: true, diagnostic: &contract.APIError{Details: map[string]any{"reason": dispatchNotFoundReason}}}, state: linkRegressed, id: "job"},
		{name: "missing-contradicts-outbox", fixture: linkKnowledgeFixture{outboxJob: linkID("job"), settled: true, attemptedAt: &attempt}, state: linkMissingSettled, id: "job"},
		{name: "regressed-precedes-linked", fixture: linkKnowledgeFixture{runJob: linkID("job"), outboxJob: linkID("job"), diagnostic: regression("job")}, state: linkRegressed, id: "job"},
		{name: "legacy-regressed-outbox-only", fixture: linkKnowledgeFixture{outboxJob: linkID("job"), diagnostic: regression("job")}, state: linkRegressed, id: "job"},
		{name: "legacy-regressed-diagnostic-only", fixture: linkKnowledgeFixture{diagnostic: regression("job")}, state: linkRegressed, id: "job"},
		{name: "regressed-outbox-without-diagnostic-id", fixture: linkKnowledgeFixture{outboxJob: linkID("job"), diagnostic: &contract.APIError{Details: map[string]any{"reason": l1RegressedReason}}}, state: linkRegressed, id: "job"},
		{name: "empty-run-id", fixture: linkKnowledgeFixture{runJob: linkID("")}, invalid: true},
		{name: "empty-outbox-id", fixture: linkKnowledgeFixture{outboxJob: linkID("")}, invalid: true},
		{name: "empty-diagnostic-id", fixture: linkKnowledgeFixture{diagnostic: regression("")}, invalid: true},
		{name: "conflicting-run-outbox", fixture: linkKnowledgeFixture{runJob: linkID("one"), outboxJob: linkID("two")}, invalid: true},
		{name: "conflicting-diagnostic-outbox", fixture: linkKnowledgeFixture{outboxJob: linkID("one"), diagnostic: regression("two")}, invalid: true},
		{name: "regressed-without-original-id", fixture: linkKnowledgeFixture{diagnostic: &contract.APIError{Details: map[string]any{"reason": l1RegressedReason}}}, invalid: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			run, _, err := s.CreateRun(context.Background(), CreateRunInput{Actor: "test", IdempotencyKey: test.name, Request: inlineRunRequest("exit 0\n")})
			if err != nil {
				t.Fatal(err)
			}
			buildLinkKnowledge(t, s, run.RunID, test.fixture)
			got, err := s.runJobLink(context.Background(), run.RunID)
			if test.invalid {
				if err == nil {
					t.Fatalf("invalid shape decoded: %+v", got)
				}
				return
			}
			if err != nil || got.state != test.state || got.jobID != test.id {
				t.Fatalf("knowledge=%+v err=%v, want %s/%q", got, err, test.state, test.id)
			}
		})
	}
}
