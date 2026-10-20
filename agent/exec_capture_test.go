package agent

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
)

type captureRoundTrip func(*http.Request) (*http.Response, error)

func (f captureRoundTrip) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

type progressCaptureBody struct {
	ctx     context.Context
	clock   *manualClock
	bytes   string
	entered chan struct{}
	stall   bool
}

func (b *progressCaptureBody) Read(p []byte) (int, error) {
	if b.entered != nil {
		close(b.entered)
		b.entered = nil
	}
	if b.stall {
		<-b.ctx.Done()
		return 0, b.ctx.Err()
	}
	if b.bytes == "" {
		return 0, io.EOF
	}
	b.clock.Advance(10 * time.Second)
	select {
	case <-b.ctx.Done():
		return 0, b.ctx.Err()
	case <-time.After(20 * time.Millisecond):
	}
	p[0] = b.bytes[0]
	b.bytes = b.bytes[1:]
	return 1, nil
}
func (b *progressCaptureBody) Close() error { return nil }
func captureSpec(payload string) contract.JobSpec {
	hash := sha256.Sum256([]byte(payload))
	raw, _ := json.Marshal(map[string]any{"exec_version": 2, "capture_sha256": hex.EncodeToString(hash[:]), "capture_size_bytes": len(payload)})
	return contract.JobSpec{Labels: map[string]string{contract.LabelRunParams: string(raw)}}
}
func TestExecCaptureProgressOutlivesAppenderBudget(t *testing.T) {
	clock := newManualClock(mailboxTestClockOrigin)
	appender := &httpRunLedgerAppender{client: &http.Client{Timeout: 50 * time.Millisecond, Transport: captureRoundTrip(func(r *http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 200, Body: &progressCaptureBody{ctx: r.Context(), clock: clock, bytes: "slow"}, Header: make(http.Header)}, nil
	})}}
	mailbox, _, _ := newTestMailbox(t, appender, `{}`)
	mailbox.clock = clock
	ok, err := seedExecCapture(t.Context(), captureSpec("slow"), mailbox)
	if err != nil || !ok {
		t.Fatalf("progressing transfer failed: %v", err)
	}
	if clock.Now().Sub(mailboxTestClockOrigin) <= 30*time.Second {
		t.Fatal("probe did not exceed old budget")
	}
}
func TestExecCaptureIdleBodyFails(t *testing.T) {
	clock := newManualClock(mailboxTestClockOrigin)
	entered := make(chan struct{})
	appender := &httpRunLedgerAppender{client: &http.Client{Timeout: 30 * time.Second, Transport: captureRoundTrip(func(r *http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 200, Body: &progressCaptureBody{ctx: r.Context(), clock: clock, stall: true, entered: entered}, Header: make(http.Header)}, nil
	})}}
	mailbox, _, _ := newTestMailbox(t, appender, `{}`)
	mailbox.clock = clock
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() { _, e := seedExecCapture(ctx, captureSpec("stall"), mailbox); done <- e }()
	<-entered
	clock.Advance(31 * time.Second)
	select {
	case e := <-done:
		if e == nil || !strings.Contains(e.Error(), "idle") {
			t.Fatalf("idle result=%v", e)
		}
	case <-time.After(time.Second):
		t.Fatal("stalled capture did not expire")
	}
}
func TestExecCaptureRetriesInterruptedDownload(t *testing.T) {
	attempts := 0
	appender := &httpRunLedgerAppender{client: &http.Client{Transport: captureRoundTrip(func(r *http.Request) (*http.Response, error) {
		attempts++
		body := io.NopCloser(strings.NewReader("good"))
		if attempts == 1 {
			body = io.NopCloser(strings.NewReader("g"))
		}
		return &http.Response{StatusCode: 200, Body: body, Header: make(http.Header)}, nil
	})}}
	mailbox, _, _ := newTestMailbox(t, appender, `{}`)
	mailbox.clock = systemClock{}
	ok, e := seedExecCapture(t.Context(), captureSpec("good"), mailbox)
	if e != nil || !ok || attempts != 2 {
		t.Fatalf("retry result=%v ok=%v attempts=%d", e, ok, attempts)
	}
	b, e := os.ReadFile(filepath.Join(mailbox.directory, "capture.bundle"))
	if e != nil || string(b) != "good" {
		t.Fatal(fmt.Sprint(string(b), e))
	}
}

func TestExecCapturePreparationRemovesStaleAttemptFiles(t *testing.T) {
	appender := &httpRunLedgerAppender{client: &http.Client{Transport: captureRoundTrip(func(r *http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader("good")), Header: make(http.Header)}, nil
	})}}
	mailbox, _, _ := newTestMailbox(t, appender, `{}`)
	os.WriteFile(filepath.Join(mailbox.directory, "tmp/capture.bundle"), []byte("stale"), 0600)
	os.Mkdir(filepath.Join(mailbox.directory, "wefty-exec-work"), 0700)
	os.WriteFile(filepath.Join(mailbox.directory, "wefty-exec-work/old"), []byte("old"), 0600)
	if _, e := seedExecCapture(t.Context(), captureSpec("good"), mailbox); e != nil {
		t.Fatal(e)
	}
	if _, e := os.Stat(filepath.Join(mailbox.directory, "wefty-exec-work")); !os.IsNotExist(e) {
		t.Fatal("stale work directory survived preparation")
	}
}
func TestExecCaptureCleanupSurvivesDetachedMailbox(t *testing.T) {
	appender := &httpRunLedgerAppender{client: &http.Client{Transport: captureRoundTrip(func(r *http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader("good")), Header: make(http.Header)}, nil
	})}}
	mailbox, _, _ := newTestMailbox(t, appender, `{}`)
	if _, e := seedExecCapture(t.Context(), captureSpec("good"), mailbox); e != nil {
		t.Fatal(e)
	}
	mailbox.detached.Store(true)
	mailbox.closeRoots()
	if _, e := os.Stat(filepath.Join(mailbox.directory, "capture.bundle")); !os.IsNotExist(e) {
		t.Fatal("bundle retained after roots closed")
	}
}
