//go:build darwin || linux

package process

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

func TestGuardianStartedContractAndAgentDisconnectReapPayloadGroup(t *testing.T) {
	agentEndpoint, guardianEndpoint, err := newGuardianSocketPair()
	if err != nil {
		t.Fatal(err)
	}
	defer agentEndpoint.Close()

	var stdout synchronizedBuffer
	done := make(chan error, 1)
	go func() {
		done <- serveGuardian(guardianEndpoint, &stdout, os.Stderr)
		_ = guardianEndpoint.Close()
	}()
	encoder := json.NewEncoder(agentEndpoint)
	decoder := json.NewDecoder(agentEndpoint)
	if err := encoder.Encode(guardianTestStart("spawn-child")); err != nil {
		t.Fatal(err)
	}
	started, err := decodeGuardianStatus(decoder)
	if err != nil {
		t.Fatal(err)
	}
	if started.Type != guardianMessageStarted || started.PID != started.ProcessGroupID {
		t.Fatalf("started status = %#v, want distinct payload process group", started)
	}
	outputDeadline := time.Now().Add(2 * time.Second)
	for stdout.Len() == 0 && time.Now().Before(outputDeadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if stdout.Len() == 0 {
		t.Fatal("payload did not start its descendant before disconnect")
	}
	if err := agentEndpoint.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("serveGuardian() error = %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("guardian did not reap after agent endpoint closed")
	}

	childPIDText, _, _ := strings.Cut(stdout.String(), "\n")
	childPID, err := strconv.Atoi(strings.TrimSpace(childPIDText))
	if err != nil {
		t.Fatalf("parse descendant PID from %q: %v", stdout.String(), err)
	}
	waitForPIDGone(t, started.PID)
	waitForPIDGone(t, childPID)
}

func TestGuardianExplicitStopReportsStructuredExit(t *testing.T) {
	agentEndpoint, guardianEndpoint, err := newGuardianSocketPair()
	if err != nil {
		t.Fatal(err)
	}
	defer agentEndpoint.Close()
	defer guardianEndpoint.Close()

	done := make(chan error, 1)
	go func() { done <- serveGuardian(guardianEndpoint, os.Stdout, os.Stderr) }()
	encoder := json.NewEncoder(agentEndpoint)
	decoder := json.NewDecoder(agentEndpoint)
	if err := encoder.Encode(guardianTestStart("hang")); err != nil {
		t.Fatal(err)
	}
	started, err := decodeGuardianStatus(decoder)
	if err != nil {
		t.Fatal(err)
	}
	if started.Type != guardianMessageStarted {
		t.Fatalf("first status type = %q, want %q", started.Type, guardianMessageStarted)
	}
	if err := encoder.Encode(guardianControlMessage{Type: guardianMessageStop}); err != nil {
		t.Fatal(err)
	}
	exited, err := decodeGuardianStatus(decoder)
	if err != nil {
		t.Fatal(err)
	}
	if exited.Result.Signal == "" || exited.Result.TerminationCause != "guardian" {
		t.Fatalf("exit result = %#v, want guardian signal termination", exited.Result)
	}
	if err := <-done; err != nil {
		t.Fatalf("serveGuardian() error = %v", err)
	}
	waitForPIDGone(t, started.PID)
}

func TestGuardianPassesRawStreamsUnmodified(t *testing.T) {
	agentEndpoint, guardianEndpoint, err := newGuardianSocketPair()
	if err != nil {
		t.Fatal(err)
	}
	defer agentEndpoint.Close()
	defer guardianEndpoint.Close()

	var stdout synchronizedBuffer
	done := make(chan error, 1)
	go func() { done <- serveGuardian(guardianEndpoint, &stdout, os.Stderr) }()
	encoder := json.NewEncoder(agentEndpoint)
	decoder := json.NewDecoder(agentEndpoint)
	if err := encoder.Encode(guardianTestStart("raw-output")); err != nil {
		t.Fatal(err)
	}
	if _, err := decodeGuardianStatus(decoder); err != nil {
		t.Fatal(err)
	}
	exited, err := decodeGuardianStatus(decoder)
	if err != nil {
		t.Fatal(err)
	}
	if exited.Result.ExitCode == nil || *exited.Result.ExitCode != 0 {
		t.Fatalf("exit result = %#v, want zero", exited.Result)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	want := append(bytes.Repeat([]byte{'x'}, 70*1024), []byte("partial-without-newline")...)
	want = append(want, 0xff, 0xfe, 0x00, '\n')
	if !bytes.Equal(stdout.Bytes(), want) {
		t.Fatalf("guardian stdout length = %d, want exact %d raw bytes", stdout.Len(), len(want))
	}
}

func TestDecodeGuardianStatusRejectsMalformedStartedMessage(t *testing.T) {
	_, err := decodeGuardianStatus(json.NewDecoder(strings.NewReader(`{"type":"started"}`)))
	if err == nil {
		t.Fatal("malformed started status was accepted")
	}
}

func guardianTestStart(mode string) guardianControlMessage {
	return guardianControlMessage{Type: guardianMessageStart, Start: &guardianStart{
		Path: helperPath, Args: []string{"process-helper", mode}, Directory: os.TempDir(),
		Environment: os.Environ(), TerminationGrace: 100 * time.Millisecond,
	}}
}

func waitForPIDGone(t *testing.T, pid int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if err := syscall.Kill(pid, 0); errors.Is(err, syscall.ESRCH) {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("process %d is still alive", pid)
}

type synchronizedBuffer struct {
	mu     sync.Mutex
	buffer bytes.Buffer
}

func (buffer *synchronizedBuffer) Write(payload []byte) (int, error) {
	buffer.mu.Lock()
	defer buffer.mu.Unlock()
	return buffer.buffer.Write(payload)
}

func (buffer *synchronizedBuffer) String() string {
	buffer.mu.Lock()
	defer buffer.mu.Unlock()
	return buffer.buffer.String()
}

func (buffer *synchronizedBuffer) Bytes() []byte {
	buffer.mu.Lock()
	defer buffer.mu.Unlock()
	return append([]byte(nil), buffer.buffer.Bytes()...)
}

func (buffer *synchronizedBuffer) Len() int {
	buffer.mu.Lock()
	defer buffer.mu.Unlock()
	return buffer.buffer.Len()
}
