package agent

import (
	"strings"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/l1"
)

// attemptCredentialFollowsDispatchAuthority ties WEFTY_ATTEMPT_TOKEN delivery
// to the same declaration that governs WEFTY_RUN_TOKEN.
//
// The product question "does the attempt credential go the same way as the run
// token?" was answered provisionally yes, so both deliveries sit behind this
// one switch. If the answer changes, set it to false: the attempt credential
// then reaches every one-shot L3 dispatched exactly as it does today, and the
// run token stays opt-in. Nothing else in the agent has to move.
const attemptCredentialFollowsDispatchAuthority = true

// dispatchedByLedger reports whether L3 dispatched this job. The run execution
// context exists only then, so it is also the boundary of the opt-in: a job
// submitted straight to L1 has no run to declare anything about, and its
// attempt credential — the whole point of ADR-0006 — is unaffected.
func dispatchedByLedger(spec contract.JobSpec) bool {
	return strings.TrimSpace(spec.Execution.Env[contract.EnvL3Endpoint]) != ""
}

// withholdWorkloadCredentials removes the in-job credentials from the
// environment the workload is about to receive, unless the run declared at
// submit that it dispatches child work.
//
// A job that only reports needs neither: it writes its steps, envelopes and
// gates into the run mailbox and the node agent publishes them with the run
// token it holds. Handing such a job a bearer anyway exposes the credential to
// everything the job executes under the same OS identity, which is the boundary
// this default closes.
//
// It returns the values it withheld so the caller keeps redacting them. A
// credential that is no longer in the environment is still a credential that
// must never reach a log sink.
func withholdWorkloadCredentials(execution *contract.ExecutionSpec, claim l1.Claim) []string {
	spec := claim.Job.Spec
	if spec.Class != contract.JobClassOneShot || !dispatchedByLedger(spec) {
		return nil
	}
	if contract.DeclaresDispatchAuthority(spec.Labels) {
		return nil
	}
	names := []string{contract.EnvRunToken}
	if attemptCredentialFollowsDispatchAuthority {
		names = append(names, contract.EnvAttemptToken)
	}
	var withheld []string
	for _, name := range names {
		// Both the dispatched spec and the environment assembled for this
		// attempt are named: the bridge may have replaced a submitted value,
		// and it may never have placed the authoritative one at all.
		withheld = appendCredential(withheld, spec.Execution.SensitiveEnv[name])
		withheld = appendCredential(withheld, execution.SensitiveEnv[name])
	}
	if attemptCredentialFollowsDispatchAuthority {
		withheld = appendCredential(withheld, claim.AttemptToken)
	}
	if len(execution.SensitiveEnv) > 0 {
		// The claim's map is shared with the stored job, so copy before
		// deleting rather than mutating state the rest of the attempt reads.
		execution.SensitiveEnv = cloneEnvironment(execution.SensitiveEnv)
		for _, name := range names {
			delete(execution.SensitiveEnv, name)
		}
	}
	return withheld
}

func appendCredential(values []string, value string) []string {
	if value == "" {
		return values
	}
	for _, existing := range values {
		if existing == value {
			return values
		}
	}
	return append(values, value)
}
