package main

import (
	"bytes"
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/l1"
	limarunner "github.com/Derek-X-Wang/wefty/runner/lima"
)

type capabilityProbeAdapterStub struct {
	err   error
	calls *int
}

func (stub capabilityProbeAdapterStub) Probe(context.Context, string, string, string, string, time.Duration) error {
	if stub.calls != nil {
		*stub.calls++
	}
	return stub.err
}

func TestOCIProbePublishesComputerOnlyAfterExactHelperProbe(t *testing.T) {
	for _, test := range []struct {
		name         string
		probeErr     error
		wantComputer bool
		wantErr      bool
	}{
		{name: "supporting helper", wantComputer: true},
		{name: "lost helper", probeErr: errors.New("helper session lost"), wantErr: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			probe := ociCapabilityProbe{adapter: capabilityProbeAdapterStub{err: test.probeErr}}
			result, err := probe.Probe(t.Context())
			if (err != nil) != test.wantErr {
				t.Fatalf("probe error = %v, want error %t", err, test.wantErr)
			}
			if result.Capabilities["computer"] != test.wantComputer {
				t.Fatalf("computer capability = %t, want %t in %+v", result.Capabilities["computer"], test.wantComputer, result)
			}
			if test.wantErr && len(result.MissingCapabilities) != 1 {
				t.Fatalf("lost helper result = %+v", result)
			}
		})
	}
}

func TestOCIProbeFailsClosedBeforeAdapterWhenIntentIsUnavailable(t *testing.T) {
	for _, test := range []struct {
		name   string
		intent limarunner.IntentSource
	}{
		{name: "missing", intent: limarunner.FileIntentSource{Path: filepath.Join(t.TempDir(), "missing.json")}},
		{name: "disabled", intent: limarunner.IntentSourceFunc(func(context.Context) (limarunner.OCIIntent, error) {
			return limarunner.OCIIntent{Version: limarunner.OCIIntentVersion, Revision: 2, Enabled: false, UpdatedAt: time.Now()}, nil
		})},
		{name: "malformed", intent: limarunner.IntentSourceFunc(func(context.Context) (limarunner.OCIIntent, error) {
			return limarunner.OCIIntent{}, errors.New("invalid marker")
		})},
	} {
		t.Run(test.name, func(t *testing.T) {
			calls := 0
			probe := ociCapabilityProbe{adapter: capabilityProbeAdapterStub{calls: &calls}, intent: test.intent}
			result, err := probe.Probe(t.Context())
			if err == nil || calls != 0 || result.ReasonCode != contract.CapabilityReasonOCIIntentDisabled {
				t.Fatalf("result=%+v calls=%d err=%v", result, calls, err)
			}
		})
	}
}

func TestConsoleOutputIsAttributedAndSerialized(t *testing.T) {
	assertConsoleOutputIsAttributedAndSerialized(t)
}

func assertConsoleOutputIsAttributedAndSerialized(t *testing.T) {
	t.Helper()
	var stdout, stderr bytes.Buffer
	claimOne := l1.Claim{Job: l1.Job{JobID: "job-one"}, Lease: l1.AttemptLease{AttemptID: "attempt-one"}}
	claimTwo := l1.Claim{Job: l1.Job{JobID: "job-two"}, Lease: l1.AttemptLease{AttemptID: "attempt-two"}}
	sinkOne := newConsoleOutputSink(&stdout, &stderr, claimOne)
	sinkTwo := newConsoleOutputSink(&stdout, &stderr, claimTwo)
	done := make(chan error, 2)
	go func() {
		done <- sinkOne.WriteOutput(context.Background(), contract.LogEvent{Stream: contract.LogStdout, Bytes: []byte("alpha\n")})
	}()
	go func() {
		done <- sinkTwo.WriteOutput(context.Background(), contract.LogEvent{Stream: contract.LogStdout, Bytes: []byte("beta\n")})
	}()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	one := "[job=job-one attempt=attempt-one stream=stdout] alpha\n"
	two := "[job=job-two attempt=attempt-two stream=stdout] beta\n"
	if got := stdout.String(); got != one+two && got != two+one {
		t.Fatalf("concurrent console output = %q, want two intact attributed records", got)
	}
	if stderr.Len() != 0 {
		t.Fatalf("stderr = %q, want empty", stderr.String())
	}
}
