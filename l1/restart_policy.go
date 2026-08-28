package l1

import (
	"crypto/rand"
	"math/big"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
)

const (
	maxServiceRestartDelay     = 30 * time.Second
	minimumServiceRestartDelay = 500 * time.Millisecond
)

type failureClassification uint8

const (
	failureTerminal failureClassification = iota
	failureRestartable
	failureInfrastructure
)

// failureClassifications is deliberately owned by L1: agents report
// facts, while the durable control plane owns restart policy. Unknown codes
// and every code absent from this table are terminal.
var failureClassifications = map[contract.SpawnFailureCode]failureClassification{
	contract.SpawnFailureStartupReadinessTimeout:  failureRestartable,
	contract.SpawnFailurePublishedListener:        failureInfrastructure,
	contract.SpawnFailureRuntimeUnavailable:       failureInfrastructure,
	contract.SpawnFailureImageUnavailable:         failureTerminal,
	contract.SpawnFailureImageNotFound:            failureTerminal,
	contract.SpawnFailureImageManifestInvalid:     failureTerminal,
	contract.SpawnFailureImagePlatformUnsupported: failureTerminal,
	contract.SpawnFailureInsufficientDisk:         failureTerminal,
}

var runtimeFailureClassifications = map[contract.RuntimeFailureCode]failureClassification{
	contract.RuntimeFailureUnavailable: failureInfrastructure,
}

func classifySpawnFailure(code contract.SpawnFailureCode) failureClassification {
	return failureClassifications[code]
}

// IsRestartableSpawnFailure reports whether service policy may retry a coded
// pre-execution failure. It is fail-closed for unknown future codes.
func IsRestartableSpawnFailure(code contract.SpawnFailureCode) bool {
	return classifySpawnFailure(code) == failureRestartable
}

func classifyRuntimeFailure(code contract.RuntimeFailureCode) failureClassification {
	return runtimeFailureClassifications[code]
}

func prestartRetryDelay(retryCount int, jitter func(time.Duration) time.Duration) time.Duration {
	if retryCount < 1 {
		retryCount = 1
	}
	delay := time.Second
	for i := 1; i < retryCount && delay < maxServiceRestartDelay; i++ {
		delay *= 2
		if delay > maxServiceRestartDelay {
			delay = maxServiceRestartDelay
		}
	}
	delay = jitter(delay)
	if delay > maxServiceRestartDelay {
		return maxServiceRestartDelay
	}
	return delay
}

// defaultRestartJitter draws uniformly from 80% through 120% of the nominal
// delay. The caller applies the effective lease cap after the draw so jitter
// can never silently exceed configured authority timing.
func defaultRestartJitter(delay time.Duration) time.Duration {
	minimum := delay * 8 / 10
	maximum := delay * 12 / 10
	width := maximum - minimum
	if width <= 0 {
		return delay
	}
	draw, err := rand.Int(rand.Reader, big.NewInt(int64(width)+1))
	if err != nil {
		panic("l1: crypto/rand failed while computing restart jitter: " + err.Error())
	}
	return minimum + time.Duration(draw.Int64())
}

func serviceRestartDelay(restartStreak int, leaseDuration time.Duration, jitter func(time.Duration) time.Duration) time.Duration {
	// restartStreak is one-based after the accepted termination. The first
	// failure waits 1s, then the pinned sequence doubles through 30s.
	exponent := restartStreak - 1
	if exponent < 0 {
		exponent = 0
	}
	nominal := time.Second
	for range exponent {
		if nominal >= maxServiceRestartDelay/2 {
			nominal = maxServiceRestartDelay
			break
		}
		nominal *= 2
	}
	if nominal > maxServiceRestartDelay {
		nominal = maxServiceRestartDelay
	}
	if nominal > leaseDuration {
		nominal = leaseDuration
	}
	delay := jitter(nominal)
	if delay > leaseDuration {
		delay = leaseDuration
	}
	if delay < minimumServiceRestartDelay {
		delay = minimumServiceRestartDelay
	}
	return delay
}

type serviceCompletionPolicy struct {
	jobState             contract.JobState
	attemptState         contract.AttemptState
	restartStreak        int
	lifetimeRestartCount int
	nextRestartNS        *int64
	lastFailure          []byte
	updateLastFailure    bool
}

func (s *Store) classifyServiceCompletion(job Job, result ProcessResult, quiescenceEvidence RuntimeQuiescenceEvidence, lastFailureJSON []byte, now time.Time) serviceCompletionPolicy {
	policy := serviceCompletionPolicy{
		jobState:             contract.JobFailed,
		attemptState:         completionStatesAttempt(result),
		restartStreak:        job.RestartStreak,
		lifetimeRestartCount: job.LifetimeRestartCount,
	}
	if job.DesiredState == contract.ServiceDesiredStopped || job.State == contract.JobStopping {
		if !validRuntimeQuiescenceEvidence(quiescenceEvidence) {
			policy.lastFailure = lastFailureJSON
			policy.updateLastFailure = true
			return policy
		}
		policy.jobState = contract.JobStopped
		return policy
	}

	restartable := false
	infrastructure := false
	switch {
	case result.SpawnError != nil:
		classification := classifySpawnFailure(result.SpawnError.Code)
		if result.SpawnError.Code == contract.SpawnFailureRuntimeUnavailable && job.Spec.Kind != contract.JobKindOCI {
			classification = failureTerminal
		}
		switch classification {
		case failureRestartable:
			restartable = true
		case failureInfrastructure:
			infrastructure = true
		}
	case result.OutputError != "":
		// Genuine output failures are terminal. Expected service spool eviction
		// never reaches this mapper.
	case result.ExitCode != nil:
		restartable = true
	case result.Signal != "":
		if result.TerminationCause == contract.TerminationCauseSpontaneous {
			restartable = true
		} else {
			infrastructure = true
		}
	case result.RuntimeFailure != nil:
		infrastructure = job.Spec.Kind == contract.JobKindOCI &&
			classifyRuntimeFailure(result.RuntimeFailure.Code) == failureInfrastructure
	}

	if infrastructure {
		policy.jobState = contract.JobQueued
		policy.lifetimeRestartCount++
		nextRestart := now.Add(prestartRetryDelay(policy.lifetimeRestartCount, s.restartJitter))
		nextRestartNS := nextRestart.UnixNano()
		policy.nextRestartNS = &nextRestartNS
		return policy
	}
	if !restartable {
		policy.lastFailure = lastFailureJSON
		policy.updateLastFailure = true
		return policy
	}

	policy.restartStreak++
	policy.lastFailure = lastFailureJSON
	policy.updateLastFailure = true
	if job.Spec.MaxRestartStreak != nil && policy.restartStreak >= *job.Spec.MaxRestartStreak {
		return policy
	}
	policy.jobState = contract.JobQueued
	policy.lifetimeRestartCount++
	nextRestart := now.Add(serviceRestartDelay(policy.restartStreak, s.leaseDuration, s.restartJitter))
	nextRestartNS := nextRestart.UnixNano()
	policy.nextRestartNS = &nextRestartNS
	return policy
}

func validRuntimeQuiescenceEvidence(evidence RuntimeQuiescenceEvidence) bool {
	switch evidence {
	case RuntimeQuiescenceAttempt, RuntimeQuiescenceNoRuntime, RuntimeQuiescenceOCISweep,
		RuntimeQuiescencePriorBootGuardian, RuntimeQuiescencePriorBootOCISweep:
		return true
	default:
		return false
	}
}

func completionStatesAttempt(result ProcessResult) contract.AttemptState {
	if result.ExitCode != nil && *result.ExitCode == 0 {
		return contract.AttemptSucceeded
	}
	return contract.AttemptFailed
}
