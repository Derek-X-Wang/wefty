package ocihelper

import (
	"errors"
	"slices"
	"testing"
)

var errFakeContainerdTaskNotFound = errors.New("containerd task not found")

type portableFakeContainerdTask struct {
	errors  []error
	signals []Signal
}

func (task *portableFakeContainerdTask) Kill(signal Signal) error {
	task.signals = append(task.signals, signal)
	if len(task.errors) == 0 {
		return nil
	}
	err := task.errors[0]
	task.errors = task.errors[1:]
	return err
}

func TestSignalDeliveryRecordsOnlySuccessfulContainerdKill(t *testing.T) {
	task := &portableFakeContainerdTask{errors: []error{nil, errFakeContainerdTaskNotFound}}
	var delivered Signal
	var cause string
	record := func(signal Signal, terminationCause string) {
		delivered = signal
		cause = terminationCause
	}
	deliver := func(signal Signal) error {
		err := task.Kill(signal)
		if errors.Is(err, errFakeContainerdTaskNotFound) {
			return ErrTaskAlreadyTerminated
		}
		return err
	}

	if err := deliverSignalAndRecord(SignalTERM, "agent", func() error { return deliver(SignalTERM) }, record); err != nil {
		t.Fatal(err)
	}
	if err := deliverSignalAndRecord(SignalKILL, "agent", func() error { return deliver(SignalKILL) }, record); !errors.Is(err, ErrTaskAlreadyTerminated) {
		t.Fatalf("raced KILL = %v, want ErrTaskAlreadyTerminated", err)
	}
	result := terminalResultFromSignalDelivery(143, nil, delivered, cause, false, false)
	if result.Signal != SignalTERM || result.TerminationCause != "agent" || result.ExitCode != nil {
		t.Fatalf("TERM-delivered, KILL-raced result = %+v", result)
	}
	if !slices.Equal(task.signals, []Signal{SignalTERM, SignalKILL}) {
		t.Fatalf("containerd signals = %v, want TERM then KILL", task.signals)
	}

	// This is the exact outcome produced by the forbidden mutation that records
	// KILL before task.Kill succeeds: exit 143 can no longer be attributed to
	// the last delivered signal, so Watch must fall back to ordinary exit code.
	mutated := terminalResultFromSignalDelivery(143, nil, SignalKILL, "agent", false, false)
	if mutated.Signal != "" || mutated.ExitCode == nil || *mutated.ExitCode != 143 {
		t.Fatalf("premature-KILL-record outcome = %+v, want plain exit 143", mutated)
	}
}
