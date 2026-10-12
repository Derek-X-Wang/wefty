package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/l1"
)

func TestMutationWaitCallerStopAfterWindowDeadline(t *testing.T) {
	for _, loop := range []string{"service", "storage", "revocation"} {
		t.Run(loop, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			reads := 0
			transport := storageRoundTripFunc(func(r *http.Request) (*http.Response, error) {
				reads++
				<-r.Context().Done()
				if !errors.Is(r.Context().Err(), context.DeadlineExceeded) {
					t.Fatalf("read must reach the window deadline before caller stop: %v", r.Context().Err())
				}
				cancel()
				return nil, r.Context().Err()
			})
			client := &apiClient{name: "L1", client: &http.Client{Transport: &requireMarkedReads{t: t, base: transport}}}
			clients := &apiClients{l1: client}
			var err error
			switch loop {
			case "service":
				_, err = waitForService(ctx, clients, l1.Job{JobID: "svc", State: contract.JobRunning},
					100*time.Millisecond, time.Millisecond, "stopped", serviceIsQuiescent)
			case "storage":
				_, _, err = waitForBackupOperation(ctx, clients, "computer-1", "backup-1",
					storageWaitFlags{timeout: 100 * time.Millisecond, pollInterval: time.Millisecond})
			case "revocation":
				clients, _ = revokeWaitStub(t, func(int32) func(http.ResponseWriter) {
					return func(http.ResponseWriter) { t.Error("transport must supply the observation failure") }
				})
				base := clients.l1.client.Transport
				clients.l1.client.Transport = storageRoundTripFunc(func(r *http.Request) (*http.Response, error) {
					if strings.Contains(r.URL.Path, "/revocations/") {
						return client.client.Transport.RoundTrip(r)
					}
					return base.RoundTrip(r)
				})
				var stdout, stderr bytes.Buffer
				err = executeComputerGrant(ctx, clients, true, revokeWaitArgs("100ms", "1ms"), &stdout, &stderr, true)
			}
			if reads != 1 || !errors.Is(err, context.Canceled) || commandExitCode(err) != exitFailure {
				t.Fatalf("caller stop after deadline: reads=%d exit=%d err=%v, want one read and local exit 1",
					reads, commandExitCode(err), err)
			}
		})
	}
}

// Cancel after reading the caller's error, before the decision reads the child
// window's error. This pins the cancellation race without scheduler timing.
type cancelAfterErrCheckContext struct {
	context.Context
	cancel context.CancelFunc
}

func (ctx cancelAfterErrCheckContext) Err() error {
	err := ctx.Context.Err()
	ctx.cancel()
	return err
}

func TestWaitDecisionCallerStopsBetweenContextChecks(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	window, closeWindow := openWaitWindow(ctx, time.Second)
	defer closeWindow()
	caller := cancelAfterErrCheckContext{Context: ctx, cancel: cancel}
	err := &apiResponseError{APIError: contract.APIError{Code: contract.ErrorUnavailable, Retryable: true}}
	var last error
	if verdict := decideWaitFailure(caller, window, err, &last); verdict != waitCallerStopped {
		t.Fatalf("caller=%v window=%v verdict=%v, want caller stop", ctx.Err(), window.Err(), verdict)
	}
}

func TestMutationWaitDoesNotRetryOtherFailures(t *testing.T) {
	for _, answer := range []struct {
		name      string
		code      contract.ErrorCode
		retryable bool
		exit      int
	}{
		{"transport", "", false, exitUnavailable},
		{"internal", contract.ErrorInternal, true, exitUnavailable},
		{"run_ledger_unavailable", contract.ErrorRunLedgerUnavailable, true, exitUnavailable},
		{"unavailable", contract.ErrorUnavailable, false, exitUnavailable},
	} {
		for _, loop := range []string{"storage", "service", "revocation", "provenance"} {
			t.Run(loop+"/"+answer.name, func(t *testing.T) {
				reads := 0
				transport := storageRoundTripFunc(func(r *http.Request) (*http.Response, error) {
					reads++
					if answer.code == "" {
						return nil, errors.New("broken observation transport")
					}
					body, _ := json.Marshal(contract.ErrorResponse{Error: contract.APIError{
						Code: answer.code, Message: "final observation answer", Retryable: answer.retryable,
						RequestID: "no-retry-answer"}})
					return &http.Response{StatusCode: http.StatusServiceUnavailable, Header: make(http.Header),
						Body: io.NopCloser(bytes.NewReader(body)), Request: r}, nil
				})
				client := &apiClient{name: "L1", client: &http.Client{Transport: &requireMarkedReads{t: t, base: transport}}}
				clients := &apiClients{l1: client}
				var err error
				switch loop {
				case "storage":
					_, _, err = waitForBackupOperation(t.Context(), clients, "computer-1", "backup-1", retryableWaitFlags)
				case "service":
					_, err = waitForService(t.Context(), clients, l1.Job{JobID: "svc", State: contract.JobRunning},
						time.Second, time.Millisecond, "stopped", serviceIsQuiescent)
				case "provenance":
					output := storageMutationOutput{Observation: &storageWaitObservation{
						waitDeadline: time.Now().Add(time.Second), waitPollInterval: time.Millisecond}}
					err = attachStorageProvenance(t.Context(), clients, "computer-1", &output, nil)
				case "revocation":
					clients, _ = revokeWaitStub(t, func(int32) func(http.ResponseWriter) {
						return func(http.ResponseWriter) { t.Error("transport must supply the answer") }
					})
					base := clients.l1.client.Transport
					clients.l1.client.Transport = storageRoundTripFunc(func(r *http.Request) (*http.Response, error) {
						if strings.Contains(r.URL.Path, "/revocations/") {
							return client.client.Transport.RoundTrip(r)
						}
						return base.RoundTrip(r)
					})
					var stdout, stderr bytes.Buffer
					err = executeComputerGrant(t.Context(), clients, true, revokeWaitArgs("1s", "1ms"), &stdout, &stderr, true)
				}
				wantExit := answer.exit
				if loop == "revocation" {
					wantExit = exitFailure
				}
				if err == nil || reads != 1 || commandExitCode(err) != wantExit {
					t.Fatalf("final failure: reads=%d exit=%d err=%v, want one read and exit %d", reads, commandExitCode(err), err, wantExit)
				}
				var refusal *apiResponseError
				if loop == "revocation" {
					if !errors.As(err, &refusal) || refusal.APIError.Code != contract.ErrorRevocationObservationFailed || !refusal.APIError.Retryable ||
						refusal.APIError.Details["mutation_applied"] != true || refusal.APIError.Details["observation_state"] != "failed" {
						t.Fatalf("revocation failure lost its classification or applied evidence: %v", err)
					}
				} else if answer.code != "" && (!errors.As(err, &refusal) || refusal.APIError.Code != answer.code ||
					refusal.APIError.Retryable != answer.retryable || refusal.APIError.RequestID != "no-retry-answer") {
					t.Fatalf("typed answer changed: %v", err)
				}
			})
		}
	}
}

func TestStorageWaitRejectsCompletionProcessedAfterDeadline(t *testing.T) {
	client := &apiClient{name: "L1", client: &http.Client{Transport: &requireMarkedReads{t: t,
		base: storageRoundTripFunc(func(r *http.Request) (*http.Response, error) {
			return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Request: r,
				Body: revokeDeadlineCloseBody{Reader: strings.NewReader(`{"operation":{"backup_id":"backup-1","status":"completed"}}`), ctx: r.Context()}}, nil
		})}}}
	last, observation, err := waitForBackupOperation(t.Context(), &apiClients{l1: client}, "computer-1", "backup-1",
		storageWaitFlags{timeout: 100 * time.Millisecond, pollInterval: time.Millisecond})
	if commandExitCode(err) != exitMutationWaitTimeout || observation.Status != "failed" ||
		last.Operation == nil || last.Operation.Status != "completed" {
		t.Fatalf("late completion must retain payload but time out: %+v / %+v / %v", last, observation, err)
	}
}

func TestMutationWaitSuccessfulPendingReadClearsLastAnswer(t *testing.T) {
	for _, loop := range []string{"storage", "service", "revocation"} {
		t.Run(loop, func(t *testing.T) {
			reads := 0
			transport := storageRoundTripFunc(func(r *http.Request) (*http.Response, error) {
				reads++
				if reads == 1 {
					return (cancelAfterDecodedAnswerTransport{cancel: func() {}, status: http.StatusServiceUnavailable,
						answer: contract.APIError{Code: contract.ErrorUnavailable, Retryable: true, Message: "obsolete transient answer"}}).RoundTrip(r)
				}
				if reads > 2 {
					<-r.Context().Done()
					return nil, r.Context().Err()
				}
				body := `{"operation":{"backup_id":"backup-1","status":"planned"}}`
				if loop == "service" {
					body = `{"job_id":"svc","state":"running"}`
				}
				if loop == "revocation" {
					body = `{"computer_id":"computer-1","subject_fabric_id":"fabric-1","subject_user_id":"person-1","policy_revision":2,"state":"pending"}`
				}
				return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body)), Request: r}, nil
			})
			client := &apiClient{name: "L1", client: &http.Client{Transport: &requireMarkedReads{t: t, base: transport}}}
			clients := &apiClients{l1: client}
			var err error
			var detail string
			switch loop {
			case "storage":
				_, _, err = waitForBackupOperation(t.Context(), clients, "computer-1", "backup-1", storageWaitFlags{timeout: 300 * time.Millisecond, pollInterval: time.Millisecond})
			case "service":
				_, err = waitForService(t.Context(), clients, l1.Job{JobID: "svc", State: contract.JobRunning}, 300*time.Millisecond, time.Millisecond, "stopped", serviceIsQuiescent)
			case "revocation":
				clients, _ = revokeWaitStub(t, func(int32) func(http.ResponseWriter) {
					return func(http.ResponseWriter) { t.Error("transport must supply the answer") }
				})
				base := clients.l1.client.Transport
				clients.l1.client.Transport = storageRoundTripFunc(func(r *http.Request) (*http.Response, error) {
					if strings.Contains(r.URL.Path, "/revocations/") {
						return client.client.Transport.RoundTrip(r)
					}
					return base.RoundTrip(r)
				})
				var stdout, stderr bytes.Buffer
				err = executeComputerGrant(t.Context(), clients, true, revokeWaitArgs("300ms", "1ms"), &stdout, &stderr, true)
				var refusal *apiResponseError
				if errors.As(err, &refusal) {
					detail, _ = refusal.APIError.Details["observation_failure"].(string)
				}
			}
			if reads != 3 || commandExitCode(err) != exitMutationWaitTimeout || strings.Contains(err.Error()+detail, "obsolete transient answer") {
				t.Fatalf("good pending read did not clear transient: reads=%d err=%v detail=%q", reads, err, detail)
			}
		})
	}
}

func TestRevokeWaitCallerStopAfterDecodedAnswer(t *testing.T) {
	for _, code := range []contract.ErrorCode{contract.ErrorUnavailable, contract.ErrorForbidden, contract.ErrorNotFound} {
		for _, jsonOutput := range []bool{true, false} {
			t.Run(string(code)+"/json="+map[bool]string{true: "true", false: "false"}[jsonOutput], func(t *testing.T) {
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				clients, _ := revokeWaitStub(t, func(int32) func(http.ResponseWriter) {
					return func(http.ResponseWriter) { t.Error("transport must supply the answer") }
				})
				base := clients.l1.client.Transport
				transport := cancelAfterDecodedAnswerTransport{cancel: cancel, status: http.StatusServiceUnavailable,
					answer: contract.APIError{Code: code, Message: "decoded answer", Retryable: code == contract.ErrorUnavailable}}
				clients.l1.client.Transport = storageRoundTripFunc(func(r *http.Request) (*http.Response, error) {
					if strings.Contains(r.URL.Path, "/revocations/") {
						return transport.RoundTrip(r)
					}
					return base.RoundTrip(r)
				})
				var stdout, stderr bytes.Buffer
				err := executeComputerGrant(ctx, clients, jsonOutput, revokeWaitArgs("1s", "1ms"), &stdout, &stderr, true)
				writeCommandError(&stderr, err, jsonOutput)
				if !errors.Is(err, context.Canceled) || commandExitCode(err) != exitFailure || !strings.Contains(stderr.String(), "revocation was applied but observation failed") {
					t.Fatalf("decoded %s must yield local caller stop: %v / %s", code, err, stderr.String())
				}
				if jsonOutput {
					var envelope contract.ErrorResponse
					var result l1.ComputerGrantMutationResult
					if json.Unmarshal(stderr.Bytes(), &envelope) != nil || envelope.Error.Code != contract.ErrorInternal || envelope.Error.Retryable ||
						json.Unmarshal(stdout.Bytes(), &result) != nil || !result.MutationApplied || result.ObservationState != "failed" ||
						result.ObservationFailure != context.Canceled.Error() || result.LastObservedRevocation == nil || result.LastObservedRevocation.State != l1.ComputerPolicyRevocationPending {
						t.Fatalf("caller stop lost receipt or local classification: %s / %s", stdout.String(), stderr.String())
					}
				} else if !strings.Contains(stdout.String(), "MUTATION APPLIED") || !strings.Contains(stdout.String(), "\ttrue\tcomputer-1:fabric-1:person-1:pending:2") {
					t.Fatalf("human output lost receipt: %s", stdout.String())
				}
			})
		}
	}
}
