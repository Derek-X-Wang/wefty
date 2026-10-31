package agent

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"
)

// Only retry waits advance time. Network inactivity timers stay armed until
// the operation stops them, just as in production.
type captureRetryClock struct {
	*manualClock
	waits  []time.Duration
	onWait func()
}

func (c *captureRetryClock) NewTimer(d time.Duration) Timer {
	timer := c.manualClock.NewTimer(d)
	if d != execCaptureIdleTimeout {
		c.waits = append(c.waits, d)
		if c.onWait != nil {
			c.onWait()
		} else {
			c.Advance(d)
		}
	}
	return timer
}

func TestExecCaptureFetchOutlastsL3Restart(t *testing.T) {
	for _, recover := range []bool{true, false} {
		t.Run(fmt.Sprint(recover), func(t *testing.T) {
			clock := &captureRetryClock{manualClock: newManualClock(mailboxTestClockOrigin)}
			attempts := 0
			appender := &httpRunLedgerAppender{client: &http.Client{Transport: captureRoundTrip(func(r *http.Request) (*http.Response, error) {
				attempts++
				if recover && clock.Now().Sub(mailboxTestClockOrigin) >= 20*time.Second {
					return &http.Response{StatusCode: 200, ContentLength: 4, Body: io.NopCloser(strings.NewReader("good")), Header: make(http.Header)}, nil
				}
				if attempts%2 == 1 {
					return nil, errors.New("L3 restarting: connection refused")
				}
				return &http.Response{StatusCode: 503, Body: io.NopCloser(strings.NewReader("restarting")), Header: make(http.Header)}, nil
			})}}
			mailbox, _, _ := newTestMailbox(t, appender, `{}`)
			mailbox.clock = clock
			ok, err := seedExecCapture(t.Context(), captureSpec("good"), mailbox)
			if ok != recover || (err == nil) != recover {
				t.Errorf("recovery=%v: ok=%v err=%v", recover, ok, err)
			}
			want := []time.Duration{time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second, 15 * time.Second}
			if attempts != 6 || !slices.Equal(clock.waits, want) || clock.Now().Sub(mailboxTestClockOrigin) != 30*time.Second {
				t.Fatalf("attempts=%d waits=%v elapsed=%v", attempts, clock.waits, clock.Now().Sub(mailboxTestClockOrigin))
			}
		})
	}
}

func TestExecCaptureFetchDoesNotRetryPermanentFailures(t *testing.T) {
	for _, tc := range []struct {
		name   string
		status int
		body   string
	}{
		{"bad_request", 400, ""}, {"unauthorized", 401, ""}, {"forbidden", 403, ""},
		{"not_found", 404, ""}, {"timeout", 408, ""}, {"conflict", 409, ""},
		{"too_large", 413, ""}, {"rate_limited", 429, ""},
		{"oversize", 200, "good!"}, {"hash_mismatch", 200, "evil"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			clock := &captureRetryClock{manualClock: newManualClock(mailboxTestClockOrigin)}
			attempts := 0
			appender := &httpRunLedgerAppender{client: &http.Client{Transport: captureRoundTrip(func(r *http.Request) (*http.Response, error) {
				attempts++
				return &http.Response{StatusCode: tc.status, ContentLength: 4, Body: io.NopCloser(strings.NewReader(tc.body)), Header: make(http.Header)}, nil
			})}}
			mailbox, _, _ := newTestMailbox(t, appender, `{}`)
			mailbox.clock = clock
			if ok, err := seedExecCapture(t.Context(), captureSpec("good"), mailbox); ok || err == nil || attempts != 1 || len(clock.waits) != 0 {
				t.Fatalf("ok=%v err=%v attempts=%d waits=%v", ok, err, attempts, clock.waits)
			}
		})
	}
}

func TestExecCaptureFetchRejectsDeclaredLengthMismatch(t *testing.T) {
	for _, tc := range []struct {
		name     string
		declared int64
		body     string
	}{
		{"empty_stored_file", 0, ""},
		{"truncated_stored_file", 1, "g"},
		{"larger_declared_file", 5, "good"},
		{"short_unknown_length", -1, "g"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			clock := &captureRetryClock{manualClock: newManualClock(mailboxTestClockOrigin)}
			attempts := 0
			appender := &httpRunLedgerAppender{client: &http.Client{Transport: captureRoundTrip(func(r *http.Request) (*http.Response, error) {
				attempts++
				return &http.Response{StatusCode: 200, ContentLength: tc.declared, Body: io.NopCloser(strings.NewReader(tc.body)), Header: make(http.Header)}, nil
			})}}
			mailbox, _, _ := newTestMailbox(t, appender, `{}`)
			mailbox.clock = clock
			ok, err := seedExecCapture(t.Context(), captureSpec("good"), mailbox)
			if ok || err == nil || !strings.Contains(err.Error(), "size mismatch") || attempts != 1 || len(clock.waits) != 0 {
				t.Fatalf("ok=%v err=%v attempts=%d waits=%v", ok, err, attempts, clock.waits)
			}
			for _, name := range []string{"capture.bundle", "tmp/capture.bundle"} {
				if _, err := mailbox.root.Stat(name); !os.IsNotExist(err) {
					t.Errorf("refused transfer retained %s: %v", name, err)
				}
			}
		})
	}
}

func TestExecCaptureFetchCancelsDuringBackoff(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	clock := &captureRetryClock{manualClock: newManualClock(mailboxTestClockOrigin), onWait: cancel}
	attempts := 0
	appender := &httpRunLedgerAppender{client: &http.Client{Transport: captureRoundTrip(func(r *http.Request) (*http.Response, error) {
		attempts++
		return nil, errors.New("L3 restarting")
	})}}
	mailbox, _, _ := newTestMailbox(t, appender, `{}`)
	mailbox.clock = clock
	if ok, err := seedExecCapture(ctx, captureSpec("good"), mailbox); ok || !errors.Is(err, context.Canceled) || attempts != 1 {
		t.Fatalf("ok=%v err=%v attempts=%d", ok, err, attempts)
	}
}

func TestExecCaptureKilledWrapperCleanupDoesNotWaitForDeletion(t *testing.T) {
	appender := &httpRunLedgerAppender{client: &http.Client{Transport: captureRoundTrip(func(r *http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 200, ContentLength: 4, Body: io.NopCloser(strings.NewReader("good")), Header: make(http.Header)}, nil
	})}}
	mailbox, _, _ := newTestMailbox(t, appender, `{}`)
	type deletion struct {
		root *os.Root
		name string
	}
	entered := make(chan deletion, 1)
	release := make(chan struct{})
	finished := make(chan struct{})
	result := make(chan error, 1)
	unblock := sync.OnceFunc(func() { close(release) })
	defer unblock()
	// Hold the real Root.RemoveAll behind a handshake, without holding the
	// retirement step. This makes the cleanup/report/deletion order explicit.
	removeAll := func(root *os.Root, name string) error {
		defer close(finished)
		entered <- deletion{root, name}
		<-release
		err := root.RemoveAll(name)
		result <- err
		return err
	}
	if _, err := seedExecCaptureWithRemoval(t.Context(), captureSpec("good"), mailbox, removeAll); err != nil {
		t.Fatal(err)
	}
	storage := filepath.Join(mailbox.directory, "wefty-exec-work")
	if err := os.MkdirAll(filepath.Join(storage, "work"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(storage, "work", "leftover"), []byte("large checkout"), 0600); err != nil {
		t.Fatal(err)
	}
	// A killed wrapper leaves work behind. Exercise its actual deferred
	// cleanup, including the detached-publisher path with already closed roots.
	returned := make(chan struct{})
	go func() {
		mailbox.detached.Store(true)
		mailbox.closeRoots()
		mailbox.removeExecCapture()
		close(returned)
	}()
	t.Cleanup(func() {
		unblock()
		select {
		case <-finished:
		case <-time.After(2 * time.Second):
			t.Error("background deletion did not finish after release")
		}
	})
	var pending deletion
	select {
	case pending = <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("cleanup did not start background deletion")
	}
	select {
	case <-returned:
	case <-time.After(2 * time.Second):
		t.Fatal("completion waited for recursive deletion")
	}
	select {
	case <-finished:
		t.Fatal("deletion was not blocked when cleanup returned")
	default:
	}
	if !strings.HasPrefix(pending.name, ".wefty-trash-wefty-exec-work-") {
		t.Fatalf("unexpected trash entry %q", pending.name)
	}
	if _, err := os.Stat(storage); !os.IsNotExist(err) {
		t.Fatalf("checkout was not retired: %v", err)
	}
	if data, err := pending.root.ReadFile(pending.name + "/work/leftover"); err != nil || string(data) != "large checkout" {
		t.Fatalf("retained root closed or trash deleted before release: data=%q error=%v", data, err)
	}
	if _, err := os.Stat(filepath.Join(mailbox.directory, "capture.bundle")); !os.IsNotExist(err) {
		t.Fatalf("bundle retained: %v", err)
	}
	unblock()
	select {
	case err := <-result:
		if err != nil {
			t.Fatalf("background deletion failed: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("background deletion did not finish after release")
	}
	waitExecCaptureTrash(t, mailbox.directory)
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := pending.root.Stat("."); errors.Is(err, os.ErrClosed) {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("background deletion did not close its retained root")
}

func TestExecCaptureCleanupIgnoresReplacedMailboxPath(t *testing.T) {
	appender := &httpRunLedgerAppender{client: &http.Client{Transport: captureRoundTrip(func(r *http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 200, ContentLength: 4, Body: io.NopCloser(strings.NewReader("good")), Header: make(http.Header)}, nil
	})}}
	mailbox, _, _ := newTestMailbox(t, appender, `{}`)
	if _, err := seedExecCapture(t.Context(), captureSpec("good"), mailbox); err != nil {
		t.Fatal(err)
	}
	if err := mailbox.root.MkdirAll("wefty-exec-work/work", 0700); err != nil {
		t.Fatal(err)
	}
	if err := mailbox.root.WriteFile("wefty-exec-work/work/own", []byte("run checkout"), 0600); err != nil {
		t.Fatal(err)
	}
	outside := t.TempDir()
	sentinel := filepath.Join(outside, "wefty-exec-work", "sentinel")
	if err := os.Mkdir(filepath.Dir(sentinel), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(sentinel, []byte("outside the run"), 0600); err != nil {
		t.Fatal(err)
	}
	moved := mailbox.directory + "-moved"
	if err := os.Rename(mailbox.directory, moved); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, mailbox.directory); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		waitExecCaptureTrash(t, moved)
		waitExecCaptureTrash(t, outside)
	})
	// This is the killed-wrapper path, including finalization closing the
	// publisher's handles before its deferred cleanup is called again.
	mailbox.detached.Store(true)
	mailbox.closeRoots()
	mailbox.removeExecCapture()
	if _, err := os.Lstat(filepath.Join(moved, "wefty-exec-work")); !os.IsNotExist(err) {
		t.Errorf("the run's own checkout was not retired: %v", err)
	}
	waitExecCaptureTrash(t, moved)
	waitExecCaptureTrash(t, outside)
	if data, err := os.ReadFile(sentinel); err != nil || string(data) != "outside the run" {
		t.Fatalf("cleanup touched the outside sentinel: data=%q error=%v", data, err)
	}
}

func waitExecCaptureTrash(t *testing.T, directory string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		entries, err := os.ReadDir(directory)
		if err != nil {
			t.Fatal(err)
		}
		pending := false
		for _, entry := range entries {
			pending = pending || strings.HasPrefix(entry.Name(), ".wefty-trash-") || strings.HasPrefix(entry.Name(), ".wefty-delete-")
		}
		if !pending {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("background deletion did not finish under %s", directory)
}

// oversizeErrReader returns more than the capture's size and a transport error
// from the same Read, as a chunked response can.
type oversizeErrReader struct{ done bool }

func (r *oversizeErrReader) Read(p []byte) (int, error) {
	if r.done {
		return 0, io.EOF
	}
	r.done = true
	return copy(p, "goodX"), errors.New("connection reset by peer")
}

func TestExecCaptureOversizeWithReadErrorStops(t *testing.T) {
	clock := &captureRetryClock{manualClock: newManualClock(mailboxTestClockOrigin)}
	attempts := 0
	appender := &httpRunLedgerAppender{client: &http.Client{Transport: captureRoundTrip(func(r *http.Request) (*http.Response, error) {
		attempts++
		return &http.Response{StatusCode: 200, ContentLength: -1, Body: io.NopCloser(&oversizeErrReader{}), Header: make(http.Header)}, nil
	})}}
	mailbox, _, _ := newTestMailbox(t, appender, `{}`)
	mailbox.clock = clock
	ok, err := seedExecCapture(t.Context(), captureSpec("good"), mailbox)
	if ok || err == nil || attempts != 1 || len(clock.waits) != 0 {
		t.Fatalf("ok=%v err=%v attempts=%d waits=%v", ok, err, attempts, clock.waits)
	}
}
