package agent

import (
	"os"

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

// withheldWorkloadCredentialNames are the reserved names this attempt removes
// from a workload's environment when its run did not declare dispatch
// authority.
func withheldWorkloadCredentialNames() []string {
	names := []string{contract.EnvRunToken}
	if attemptCredentialFollowsDispatchAuthority {
		names = append(names, contract.EnvAttemptToken)
	}
	return names
}

// withholdWorkloadCredentials removes the in-job credentials from the
// environment the workload is about to receive when L3 marked this job as one
// that reports through its run mailbox instead of holding a credential.
//
// A job that only reports needs neither: it writes its steps, envelopes and
// gates into the run mailbox and the node agent publishes them with the run
// token it holds. Handing such a job a bearer anyway exposes the credential to
// everything the job executes under the same OS identity, which is the boundary
// this default closes.
//
// The decision is read from L3's own label, never inferred from the job's
// environment: a process JobSpec may legally carry any environment name, so
// treating WEFTY_L3_ENDPOINT as proof of L3 provenance would strip the attempt
// credential from a direct-L1 job that merely names it.
//
// It returns the values it withheld so the caller keeps redacting them. A
// credential that is no longer in the environment — or that never belonged to
// this job at all — is still a credential that must never reach a log sink.
func withholdWorkloadCredentials(execution *contract.ExecutionSpec, claim l1.Claim) []string {
	spec := claim.Job.Spec
	// The node agent's own environment is not this job's. If the operator
	// exported a credential into the agent's process, a runtime that seeds a
	// child from os.Environ() would hand it to the workload, so its value is
	// named for redaction on every attempt regardless of what is delivered.
	// Runtimes strip these names from their base environment; this is the
	// second half of that guarantee, not a substitute for it.
	var withheld []string
	for _, name := range ambientCredentialNames {
		withheld = appendCredential(withheld, os.Getenv(name))
	}
	if spec.Class != contract.JobClassOneShot || !contract.WithholdsWorkloadCredentials(spec.Labels) {
		return withheld
	}
	names := withheldWorkloadCredentialNames()
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

// ambientCredentialNames is the reserved credential set, in the fixed order the
// redactor collects it.
var ambientCredentialNames = []string{
	contract.EnvRunToken, contract.EnvAttemptToken, contract.EnvComputerToken,
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
