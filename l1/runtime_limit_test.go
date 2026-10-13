package l1

import (
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
)

func TestJobLimitSecondsBounds(t *testing.T) {
	for _, field := range []string{"max_runtime_seconds", "idle_timeout_seconds", "completion_timeout_seconds"} {
		for _, seconds := range []int{-1, 9223372037, 0, 9223372036} {
			t.Run(fmt.Sprintf("%s/%d", field, seconds), func(t *testing.T) {
				h := newIntegrationHarness(t, nil)
				client := h.client(fabric.Identity{NodeID: "app", Tags: []string{DefaultClientPrincipalTag}})
				limits := &contract.JobLimits{}
				switch field {
				case "max_runtime_seconds":
					limits.MaxRuntimeSeconds = seconds
				case "idle_timeout_seconds":
					limits.IdleTimeoutSeconds = seconds
				case "completion_timeout_seconds":
					limits.CompletionTimeoutSeconds = seconds
				}
				spec := validJobSpec("bounds-go", nil)
				spec.Limits = limits
				_, _, err := h.store.CreateJob(t.Context(), spec)
				invalid := seconds < 0 || seconds > 9223372036
				if invalid {
					if errorCode(err) != contract.ErrorInvalidRequest || !strings.Contains(err.Error(), field) {
						t.Errorf("Go submission=%v, want invalid_request naming %s", err, field)
					}
				} else if err != nil {
					t.Fatalf("valid Go seconds refused: %v", err)
				}
				spec.DispatchKey = "bounds-http"
				status, _, body := h.do(client, http.MethodPost, "/v1/jobs", spec)
				want := http.StatusCreated
				if invalid {
					want = http.StatusBadRequest
				}
				if status != want {
					t.Errorf("HTTP submission=%d %s, want %d", status, body, want)
				}
			})
		}
	}
}

func TestServiceReachedRuntimeLimitKeepsAgentInterruptionRouting(t *testing.T) {
	for _, kind := range []string{"process", "oci"} {
		for _, restart := range []string{"always", "on-failure", "never"} {
			for _, initiator := range []contract.TerminationCause{contract.TerminationCauseAgent, contract.TerminationCauseGuardian} {
				t.Run(kind+"/"+restart+"/"+string(initiator), func(t *testing.T) {
					h, client, agent, node, job, claim := neverFixtureWith(t, kind, func(spec *contract.JobSpec) {
						spec.Restart = restart
						spec.Limits = &contract.JobLimits{MaxRuntimeSeconds: 1}
					})
					neverStarted(t, h, agent, job, claim)
					zero := 0
					result := ProcessResult{ExitCode: &zero, LimitReached: contract.LimitReachedMaxRuntime}
					completed := neverComplete(t, h, agent, job, claim, CompletionRequest{Result: result, TerminationInitiator: initiator})
					status, _, body := h.do(client, http.MethodGet, "/v1/jobs/"+job.JobID+"?class=service", nil)
					if status != http.StatusOK {
						t.Fatalf("read=%d %s", status, body)
					}
					completed = decodeJob(t, body)
					if len(completed.Attempts) != 1 {
						t.Fatalf("attempts=%+v", completed.Attempts)
					}
					want := contract.JobQueued
					if restart == "never" {
						want = contract.JobFailed
					}
					if completed.State != want || completed.Attempts[0].State != contract.AttemptSucceeded {
						t.Errorf("completion=%+v attempts=%+v", completed, completed.Attempts)
					}
					if completed.RestartStreak != job.RestartStreak {
						t.Errorf("restart streak=%d, want unchanged %d", completed.RestartStreak, job.RestartStreak)
					}
					wantLifetime := job.LifetimeRestartCount
					if restart != "never" {
						wantLifetime++
					}
					if completed.LifetimeRestartCount != wantLifetime {
						t.Errorf("lifetime restart count=%d, want %d", completed.LifetimeRestartCount, wantLifetime)
					}
					if completed.Attempts[0].Result == nil || completed.Attempts[0].Result.ExitCode == nil || *completed.Attempts[0].Result.ExitCode != 0 || completed.Attempts[0].Result.LimitReached != contract.LimitReachedMaxRuntime {
						t.Fatal("payload exit or stop evidence lost")
					}
					if restart == "on-failure" {
						h.clock.Advance(2 * time.Second)
						next := claimRestartService(t, h, agent, node)
						neverStarted(t, h, agent, job, next)
						zero := 0
						clean := neverComplete(t, h, agent, job, next, CompletionRequest{Result: ProcessResult{ExitCode: &zero}})
						if clean.State != contract.JobStopped || clean.FailureReason != "" {
							t.Errorf("clean stop retained prior limit failure: %+v", clean)
						}
					}
				})
			}
		}
	}
}
