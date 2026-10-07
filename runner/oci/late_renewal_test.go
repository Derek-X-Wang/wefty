package oci

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	workloadrunner "github.com/Derek-X-Wang/wefty/runner"
	"github.com/Derek-X-Wang/wefty/runner/ocihelper"
)

type lateRenewalEngine struct {
	*adapterTestEngine
	targetWatching, neighbourWatching, release chan struct{}
}

func (engine *lateRenewalEngine) Watch(ctx context.Context, request ocihelper.WatchRequest, emit func(ocihelper.WatchEvent) error) error {
	if request.Authority.AttemptID == "attempt" {
		close(engine.targetWatching)
		<-ctx.Done()
		return ctx.Err()
	}
	close(engine.neighbourWatching)
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-engine.release:
	}
	return emit(ocihelper.WatchEvent{Kind: ocihelper.WatchComplete, Result: &ocihelper.WatchResponse{ExitCode: intPointer(0)}})
}

func TestAdapterLateRenewalLosesOnlyExactAttempt(t *testing.T) {
	for _, class := range []string{contract.JobClassOneShot, contract.JobClassService} {
		t.Run(class, func(t *testing.T) {
			engine := &lateRenewalEngine{adapterTestEngine: &adapterTestEngine{}, targetWatching: make(chan struct{}), neighbourWatching: make(chan struct{}), release: make(chan struct{})}
			adapter, barrier, _, stop := startAdapterTestServerWithSnapshots(t, engine, ImagePolicy{})
			defer stop()
			defer close(engine.release)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			session, err := barrier.Session()
			if err != nil {
				t.Fatal(err)
			}
			generation := session.Handshake().SessionGeneration
			target := adapterTestRequest()
			target.InitialDeadman = time.Minute
			target.Authority.WorkloadClass = class
			neighbour := adapterTestRequest()
			neighbour.InitialDeadman = time.Minute
			neighbour.Authority.JobID, neighbour.Authority.AttemptID, neighbour.Authority.FencingToken, neighbour.Authority.WorkloadClass = "neighbour", "neighbour", "neighbour", contract.JobClassService
			target.OCIRuntimeUnavailable = func(workloadrunner.RuntimeGeneration) { t.Error("attempt loss embargoed OCI") }
			neighbour.OCIRuntimeUnavailable = target.OCIRuntimeUnavailable
			targetDone, neighbourDone := make(chan error, 1), make(chan error, 1)
			go func() { _, err := adapter.Run(ctx, target, nil); targetDone <- err }()
			go func() { _, err := adapter.Run(ctx, neighbour, nil); neighbourDone <- err }()
			for _, entered := range []chan struct{}{engine.targetWatching, engine.neighbourWatching} {
				select {
				case <-entered:
				case <-time.After(5 * time.Second):
					t.Fatal("Watch not entered")
				}
			}
			authority := HelperAuthority(target.Authority)
			if response, err := session.Delete(t.Context(), ocihelper.DeleteRequest{Authority: authority}); err != nil || !response.Deleted {
				t.Fatalf("Delete=%+v err=%v", response, err)
			}
			if err := session.QueueAttemptRenewal(authority, time.Minute); err != nil {
				t.Fatal(err)
			}
			select {
			case err := <-targetDone:
				var lost *ocihelper.AttemptLostError
				if !errors.As(err, &lost) || lost.Authority != authority {
					t.Fatalf("target returned %v", err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("refused attempt remained running")
			}
			select {
			case err := <-neighbourDone:
				t.Fatalf("neighbour ended: %v", err)
			default:
			}
			if session.HealthError() != nil || session.Handshake().SessionGeneration != generation {
				t.Fatal("helper session was lost")
			}
			if receipt, err := adapter.ReapAndVerify(t.Context(), workloadrunner.ReapRequest{Authority: target.Authority}); err != nil || !receipt.RuntimeQuiesced {
				t.Fatalf("reap=%+v err=%v", receipt, err)
			}
			cancel()
			select {
			case <-neighbourDone:
			case <-time.After(5 * time.Second):
				t.Fatal("neighbour did not join")
			}
		})
	}
}
