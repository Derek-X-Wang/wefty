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
	"syscall"
	"testing"
	"testing/iotest"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/internal/execsweep"
	"github.com/Derek-X-Wang/wefty/internal/testwait"
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
		return &http.Response{StatusCode: 200, ContentLength: 4, Body: &progressCaptureBody{ctx: r.Context(), clock: clock, bytes: "slow"}, Header: make(http.Header)}, nil
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
		return &http.Response{StatusCode: 200, ContentLength: 5, Body: &progressCaptureBody{ctx: r.Context(), clock: clock, stall: true, entered: entered}, Header: make(http.Header)}, nil
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
	case <-time.After(testwait.Bound):
		t.Fatal("stalled capture did not expire")
	}
}
func TestExecCaptureRetriesInterruptedDownload(t *testing.T) {
	for _, tc := range []struct {
		name      string
		declared  int64
		readError error
	}{
		{"declared_early_eof", 4, nil},
		{"declared_unexpected_eof", 4, io.ErrUnexpectedEOF},
		{"unknown_unexpected_eof", -1, io.ErrUnexpectedEOF},
		{"unknown_connection_reset", -1, syscall.ECONNRESET},
	} {
		t.Run(tc.name, func(t *testing.T) {
			attempts := 0
			appender := &httpRunLedgerAppender{client: &http.Client{Transport: captureRoundTrip(func(r *http.Request) (*http.Response, error) {
				attempts++
				var body io.Reader = strings.NewReader("good")
				if attempts == 1 {
					body = strings.NewReader("g")
					if tc.readError != nil {
						body = io.MultiReader(body, iotest.ErrReader(tc.readError))
					}
				}
				// Only the first transfer is interrupted; the next fetch
				// recovers both declared-length and chunked responses.
				return &http.Response{StatusCode: 200, ContentLength: tc.declared, Body: io.NopCloser(body), Header: make(http.Header)}, nil
			})}}
			mailbox, _, _ := newTestMailbox(t, appender, `{}`)
			clock := &captureRetryClock{manualClock: newManualClock(mailboxTestClockOrigin)}
			mailbox.clock = clock
			ok, err := seedExecCapture(t.Context(), captureSpec("good"), mailbox)
			if err != nil || !ok || attempts != 2 || len(clock.waits) != 1 || clock.waits[0] != time.Second {
				t.Fatalf("retry result=%v ok=%v attempts=%d waits=%v", err, ok, attempts, clock.waits)
			}
			b, err := os.ReadFile(filepath.Join(mailbox.directory, "capture.bundle"))
			if err != nil || string(b) != "good" {
				t.Fatal(fmt.Sprint(string(b), err))
			}
		})
	}
}

func execCaptureTempAllocation(t *testing.T, storage string) string {
	t.Helper()
	allocation, err := os.MkdirTemp(os.Getenv("WEFTY_EXEC_TEMP_ROOT"), "wefty-run-data.")
	if err != nil {
		t.Fatal(err)
	}
	allocation, err = filepath.EvalSymlinks(allocation)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(allocation) })
	if err := os.MkdirAll(storage, 0700); err != nil {
		t.Fatal(err)
	}
	for path, value := range map[string]string{
		filepath.Join(storage, "tmp-dir"):  allocation + "\n",
		filepath.Join(allocation, "owner"): storage + "\n",
	} {
		if err := os.WriteFile(path, []byte(value), 0600); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.Mkdir(filepath.Join(allocation, "tmp"), 0700); err != nil {
		t.Fatal(err)
	}
	return allocation
}

func sweepExecCaptureTemp(t *testing.T, allocation, ownerRoot string) {
	t.Helper()
	// Model an orphan past the grace period; live cleanup may already remove it.
	old := time.Now().Add(-2 * time.Hour)
	if err := os.Chtimes(allocation, old, old); err != nil && !os.IsNotExist(err) {
		t.Fatal(err)
	}
	if err := execsweep.SweepTempDirs(t.Context(), []string{ownerRoot}); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(allocation); !os.IsNotExist(err) {
		t.Fatalf("orphan temp survived cleanup: %v", err)
	}
}

func TestExecCapturePreparationRemovesStaleAttemptFiles(t *testing.T) {
	appender := &httpRunLedgerAppender{client: &http.Client{Transport: captureRoundTrip(func(r *http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 200, ContentLength: 4, Body: io.NopCloser(strings.NewReader("good")), Header: make(http.Header)}, nil
	})}}
	mailbox, _, _ := newTestMailbox(t, appender, `{}`)
	os.WriteFile(filepath.Join(mailbox.directory, "tmp/capture.bundle"), []byte("stale"), 0600)
	os.Mkdir(filepath.Join(mailbox.directory, "wefty-exec-work"), 0700)
	os.WriteFile(filepath.Join(mailbox.directory, "wefty-exec-work/old"), []byte("old"), 0600)
	allocation := execCaptureTempAllocation(t, filepath.Join(mailbox.directory, "wefty-exec-work"))
	if _, e := seedExecCapture(t.Context(), captureSpec("good"), mailbox); e != nil {
		t.Fatal(e)
	}
	if _, e := os.Stat(filepath.Join(mailbox.directory, "wefty-exec-work")); !os.IsNotExist(e) {
		t.Fatal("stale work directory survived preparation")
	}
	sweepExecCaptureTemp(t, allocation, mailbox.directory)
}
func TestExecCaptureCleanupSurvivesDetachedMailbox(t *testing.T) {
	appender := &httpRunLedgerAppender{client: &http.Client{Transport: captureRoundTrip(func(r *http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 200, ContentLength: 4, Body: io.NopCloser(strings.NewReader("good")), Header: make(http.Header)}, nil
	})}}
	mailbox, _, _ := newTestMailbox(t, appender, `{}`)
	if _, e := seedExecCapture(t.Context(), captureSpec("good"), mailbox); e != nil {
		t.Fatal(e)
	}
	allocation := execCaptureTempAllocation(t, filepath.Join(mailbox.directory, "wefty-exec-work"))
	mailbox.detached.Store(true)
	mailbox.closeRoots()
	if _, e := os.Stat(filepath.Join(mailbox.directory, "capture.bundle")); !os.IsNotExist(e) {
		t.Fatal("bundle retained after roots closed")
	}
	waitExecCaptureTrash(t, mailbox.directory)
	sweepExecCaptureTemp(t, allocation, mailbox.directory)
}
