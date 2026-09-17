package agent

import (
	"os"
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

// submittedByRunLedger reports L1's own answer to "did the run ledger submit
// this job?".
//
// The submitter is derived by L1 from the authenticated Fabric identity of the
// caller that created the job and stored on the job; no request body can set
// it. That is what makes it usable as a credential control, unlike the job's
// environment, which a direct-L1 submitter may fill with anything including
// WEFTY_L3_ENDPOINT.
//
// A job spawned through an attempt credential inherits its root's originating
// submitter, so a descendant of an L3 run would otherwise read as an L3
// dispatch. L3 submits only root jobs, so a job with a parent never counts.
func submittedByRunLedger(claim l1.Claim, runLedgerNodeID string) bool {
	runLedgerNodeID = strings.TrimSpace(runLedgerNodeID)
	if runLedgerNodeID == "" {
		// An unset value means the default, never "match nothing". Treating it
		// as the latter would turn one missing configuration line into a
		// silently absent credential control.
		runLedgerNodeID = contract.DefaultRunLedgerNodeID
	}
	if claim.Job.ParentJobID != "" {
		return false
	}
	return strings.TrimSpace(claim.Job.OriginatingSubmitter) == runLedgerNodeID
}

// withholdsWorkloadCredentials is the whole delivery rule, in one place.
//
// Belt and braces, because no single signal survives a rolling upgrade. The
// negative label alone fails open: an older L3, or a job queued before the
// upgrade, omits it and a new agent would hand a reporting run both
// credentials. The positive label alone fails closed in the other direction:
// an agent that predates it withholds from a run that declared dispatch
// authority and the run's child dispatch breaks. L1's provenance bit closes
// the first gap without a wire change, and sending both labels closes the
// second.
//
// Forging is not a way in. A submitter cannot set the provenance bit at all,
// and a direct-L1 job carries neither label, so it keeps its credentials by
// construction; the most a forged withhold label achieves is deleting the
// forger's own job's credentials.
func withholdsWorkloadCredentials(claim l1.Claim, runLedgerNodeID string) bool {
	spec := claim.Job.Spec
	if spec.Class != contract.JobClassOneShot {
		return false
	}
	if contract.WithholdsWorkloadCredentials(spec.Labels) {
		return true
	}
	return submittedByRunLedger(claim, runLedgerNodeID) && !contract.DeclaresDispatchAuthority(spec.Labels)
}

// withheldWorkloadCredentialNames are the reserved names an attempt removes
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
// environment the workload is about to receive, unless its run declared
// dispatch authority.
//
// A job that only reports needs neither: it writes its steps, envelopes and
// gates into the run mailbox and the node agent publishes them with the run
// token it holds. Handing such a job a bearer anyway exposes the credential to
// everything the job executes under the same OS identity, which is the boundary
// this default closes.
//
// It returns the values it withheld so the caller keeps redacting them. A
// credential that is no longer in the environment — or that never belonged to
// this job at all — is still a credential that must never reach a log sink.
func withholdWorkloadCredentials(execution *contract.ExecutionSpec, claim l1.Claim, runLedgerNodeID string) []string {
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
	if !withholdsWorkloadCredentials(claim, runLedgerNodeID) {
		return withheld
	}
	names := withheldWorkloadCredentialNames()
	for _, name := range names {
		// Both the dispatched spec and the environment assembled for this
		// attempt are named: the bridge may have replaced a submitted value,
		// and it may never have placed the authoritative one at all.
		withheld = appendCredential(withheld, claim.Job.Spec.Execution.SensitiveEnv[name])
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
