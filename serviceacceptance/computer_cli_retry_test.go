package serviceacceptance

import (
	"encoding/json"
	"errors"
	"slices"
	"testing"

	"github.com/Derek-X-Wang/wefty/contract"
)

type computerCLIAttempt struct {
	stdout, stderr string
	err            error
}

// retryCommittedComputerMutation runs one Computer CLI command and repeats it
// once when the CLI reports a committed change whose post-change read was
// unavailable. L1 commits the change, and the CLI then exits 13 with
// `unavailable`, `retryable: true` and `details.mutation_applied: true`: the
// product tells the caller to repeat the same idempotent request. The repeat
// sends the same arguments, so it carries the same idempotency key. Any other
// failure, and any command without an idempotency key, returns unchanged.
func retryCommittedComputerMutation(logf func(string, ...any), arguments []string, run func() computerCLIAttempt) computerCLIAttempt {
	attempt := run()
	if attempt.err == nil || !slices.Contains(arguments, "--idempotency-key") || !committedMutationReadUnavailable(attempt.stderr) {
		return attempt
	}
	logf("Computer CLI %v: change committed but its post-change read was unavailable; repeating once with the same idempotency key", arguments)
	return run()
}

func committedMutationReadUnavailable(stderr string) bool {
	var response contract.ErrorResponse
	if json.Unmarshal([]byte(stderr), &response) != nil {
		return false
	}
	applied, _ := response.Error.Details["mutation_applied"].(bool)
	return response.Error.Code == contract.ErrorUnavailable && response.Error.Retryable && applied
}

func TestRetryCommittedComputerMutation(t *testing.T) {
	const expired = `{"error":{"code":"unavailable","message":"Computer change committed but its post-change read is unavailable","retryable":true,"details":{"mutation_applied":true,"read_reason":"read_snapshot_expired"}}}`
	failure := errors.New("exit status 13")
	keyed := []string{"services", "create", "--computer", "--idempotency-key", "k"}
	for _, test := range []struct {
		name      string
		arguments []string
		first     computerCLIAttempt
		wantCalls int
		wantErr   bool
	}{
		{"committed create retries once and recovers", keyed, computerCLIAttempt{stderr: expired, err: failure}, 2, false},
		{"success is not repeated", keyed, computerCLIAttempt{stdout: `{}`}, 1, false},
		{"unavailable without applied mutation", keyed, computerCLIAttempt{stderr: `{"error":{"code":"unavailable","retryable":true,"details":{"mutation_applied":false}}}`, err: failure}, 1, true},
		{"unavailable without mutation detail", keyed, computerCLIAttempt{stderr: `{"error":{"code":"unavailable","retryable":true}}`, err: failure}, 1, true},
		{"not retryable", keyed, computerCLIAttempt{stderr: `{"error":{"code":"unavailable","retryable":false,"details":{"mutation_applied":true}}}`, err: failure}, 1, true},
		{"other code", keyed, computerCLIAttempt{stderr: `{"error":{"code":"internal","retryable":true,"details":{"mutation_applied":true}}}`, err: failure}, 1, true},
		{"unparseable stderr", keyed, computerCLIAttempt{stderr: `wefty: boom`, err: failure}, 1, true},
		{"no idempotency key", []string{"services", "create"}, computerCLIAttempt{stderr: expired, err: failure}, 1, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			calls := 0
			got := retryCommittedComputerMutation(t.Logf, test.arguments, func() computerCLIAttempt {
				calls++
				if calls == 1 {
					return test.first
				}
				return computerCLIAttempt{stdout: `{"computer_id":"c"}`}
			})
			if calls != test.wantCalls || (got.err != nil) != test.wantErr {
				t.Fatalf("calls=%d err=%v, want calls=%d err=%v", calls, got.err, test.wantCalls, test.wantErr)
			}
		})
	}

	t.Run("second committed refusal is not retried again", func(t *testing.T) {
		calls := 0
		got := retryCommittedComputerMutation(t.Logf, keyed, func() computerCLIAttempt {
			calls++
			return computerCLIAttempt{stderr: expired, err: failure}
		})
		if calls != 2 || got.err == nil {
			t.Fatalf("calls=%d err=%v, want 2 calls and the second refusal", calls, got.err)
		}
	})
}
