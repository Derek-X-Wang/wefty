package oci

import (
	"context"
	"errors"
	"io"
	"reflect"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	workloadrunner "github.com/Derek-X-Wang/wefty/runner"
	"github.com/Derek-X-Wang/wefty/runner/ocihelper"
)

func TestCancelOCIExactAttemptTermination(t *testing.T) {
	for _, tc := range []struct {
		name                    string
		ignore, failed, already bool
		startedRefused          bool
	}{
		{name: "TERM_handled"}, {name: "TERM_ignored", ignore: true}, {name: "TERM_unconfirmed", failed: true}, {name: "already_exited", already: true}, {name: "cancel_before_durable_Started", startedRefused: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			engine := &adapterTestEngine{watchSignals: make(chan ocihelper.Signal, 2), termExitCode: intPointer(0), ignoreTERM: tc.ignore, termUnconfirmed: tc.failed, termRacesSelfExit: tc.already}
			adapter, closeAdapter := startAdapterTestServer(t, engine)
			defer closeAdapter()
			defer func() {
				select {
				case engine.watchSignals <- ocihelper.SignalKILL:
				default:
				}
			}()
			request := adapterTestRequest()
			request.TerminationGrace = 20 * time.Millisecond
			// The production one-shot request uses CallerLifetime, unlike services.
			request.Authority.WorkloadClass = contract.JobClassOneShot
			request.LifetimeBoundary = workloadrunner.CallerLifetime
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			ready := make(chan struct{})
			if tc.startedRefused {
				request.OCIStarted = func(context.Context, workloadrunner.OCIImageObservation) error {
					cancel()
					close(ready)
					return errors.New("cancellation refused Started")
				}
			} else {
				request.Started = func() { close(ready) }
			}
			resultCh := make(chan workloadrunner.Result, 1)
			errorCh := make(chan error, 1)
			go func() { result, err := adapter.Run(ctx, request, nil); resultCh <- result; errorCh <- err }()
			select {
			case <-ready:
			case <-time.After(5 * time.Second):
				t.Fatal("helper never admitted attempt")
			}
			cancel()
			var result workloadrunner.Result
			select {
			case result = <-resultCh:
			case <-time.After(5 * time.Second):
				t.Fatal("cancellation did not join termination")
			}
			err := <-errorCh
			if tc.startedRefused {
				if err == nil || result.Outcome.SpawnError == nil || result.Outcome.SpawnError.Code != contract.SpawnFailureProcessRequest {
					t.Fatalf("pre-start evidence=%+v %v", result, err)
				}
			} else {
				if err != nil {
					t.Fatal(err)
				}
				if tc.ignore {
					if result.Outcome.Signal != "killed" || result.Outcome.TerminationCause != contract.TerminationCauseAgent {
						t.Fatalf("forced termination=%+v", result)
					}
				} else if result.Outcome.ExitCode == nil || *result.Outcome.ExitCode != 0 || (result.Outcome.TerminationInitiator == contract.TerminationCauseAgent) != (!tc.failed && !tc.already) {
					t.Fatalf("confirmed-delivery provenance=%+v", result)
				}
			}
			engine.mu.Lock()
			signals := append([]ocihelper.Signal(nil), engine.signals...)
			authority := engine.lastRun.Authority
			deletes := engine.runtimeDeletes
			engine.mu.Unlock()
			want := []ocihelper.Signal{ocihelper.SignalTERM}
			if tc.ignore {
				want = append(want, ocihelper.SignalKILL)
			}
			if !reflect.DeepEqual(signals, want) || authority != HelperAuthority(request.Authority) {
				t.Fatalf("exact stop authority=%+v signals=%v want=%v", authority, signals, want)
			}
			if deletes != 0 {
				t.Fatal("deleted before caller could capture handoff result")
			}
			receipt, err := adapter.ReapAndVerify(t.Context(), workloadrunner.ReapRequest{Authority: request.Authority})
			if err != nil || !receipt.RuntimeQuiesced {
				t.Fatalf("reap=%+v %v", receipt, err)
			}
		})
	}
}

// The helper keeps shared image work independent of a caller that stops
// waiting. Admission, by contrast, is attempt-owned and must join its reap.
func TestCancelOCIStartupPlumbing(t *testing.T) {
	for _, phase := range []string{"image_preparation", "helper_admission"} {
		t.Run(phase, func(t *testing.T) {
			engine := &cancelStartupEngine{adapterTestEngine: &adapterTestEngine{}, phase: phase, entered: make(chan struct{}), release: make(chan struct{}), reaped: make(chan ocihelper.AttemptAuthority, 1)}
			adapter, closeAdapter := startAdapterTestServer(t, engine)
			defer closeAdapter()
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			request := adapterTestRequest()
			resultCh := make(chan workloadrunner.Result, 1)
			go func() { result, _ := adapter.Run(ctx, request, nil); resultCh <- result }()
			select {
			case <-engine.entered:
			case <-time.After(5 * time.Second):
				t.Fatal("startup edge never entered")
			}
			cancel()
			var result workloadrunner.Result
			select {
			case result = <-resultCh:
			case <-time.After(5 * time.Second):
				t.Fatal("startup cancel did not release agent")
			}
			close(engine.release)
			if result.Outcome.SpawnError == nil || result.Outcome.Signal != "" || result.Outcome.TerminationInitiator != "" {
				t.Fatalf("pre-start cancellation invented payload termination=%+v", result)
			}
			if phase == "helper_admission" {
				select {
				case authority := <-engine.reaped:
					if authority != HelperAuthority(request.Authority) {
						t.Fatalf("startup reaped wrong attempt=%+v", authority)
					}
				case <-time.After(5 * time.Second):
					t.Fatal("helper did not join failed admission reap")
				}
			}
			receipt, err := adapter.ReapAndVerify(t.Context(), workloadrunner.ReapRequest{Authority: request.Authority})
			if err != nil || !receipt.RuntimeQuiesced {
				t.Fatalf("startup reap=%+v %v", receipt, err)
			}
			engine.mu.Lock()
			run := engine.lastRun
			signals := append([]ocihelper.Signal(nil), engine.signals...)
			engine.mu.Unlock()
			if phase == "image_preparation" && run.Authority.AttemptID != "" {
				t.Fatalf("image cancellation started workload=%+v", run.Authority)
			}
			if len(signals) != 0 {
				t.Fatalf("signal before confirmed payload admission=%v", signals)
			}
		})
	}
}

type cancelStartupEngine struct {
	*adapterTestEngine
	phase            string
	entered, release chan struct{}
	reaped           chan ocihelper.AttemptAuthority
}

func (e *cancelStartupEngine) EnsureImage(ctx context.Context, r ocihelper.EnsureImageRequest, archive io.Reader, emit func(ocihelper.EnsureImageEvent) error) error {
	if e.phase == "image_preparation" {
		close(e.entered)
		select {
		case <-e.release:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return e.adapterTestEngine.EnsureImage(ctx, r, archive, emit)
}
func (e *cancelStartupEngine) Run(ctx context.Context, r ocihelper.RunRequest) (ocihelper.RunResponse, error) {
	if e.phase == "helper_admission" {
		close(e.entered)
		<-ctx.Done()
		return ocihelper.RunResponse{}, ctx.Err()
	}
	return e.adapterTestEngine.Run(ctx, r)
}
func (e *cancelStartupEngine) ReapAttempt(ctx context.Context, authority ocihelper.AttemptAuthority) error {
	e.reaped <- authority
	return e.adapterTestEngine.ReapAttempt(ctx, authority)
}
