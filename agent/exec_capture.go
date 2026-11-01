package agent

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
)

const execCaptureIdleTimeout = 30 * time.Second

var errExecCaptureIdle = errors.New("exec capture fetch idle deadline exceeded")

// Each network operation has an inactivity budget, not a whole-transfer budget.
// Cancellation closes the transport read; joining prevents a late timer from
// cancelling the next operation after this one made progress.
func captureWithIdle[T any](ctx context.Context, clock Clock, cancel context.CancelCauseFunc, operation func() (T, error)) (T, error) {
	timer := clock.NewTimer(execCaptureIdleTimeout)
	done, joined := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(joined)
		select {
		case <-timer.C():
			cancel(errExecCaptureIdle)
		case <-ctx.Done():
		case <-done:
		}
	}()
	value, err := operation()
	close(done)
	stopTimer(timer)
	<-joined
	if cause := context.Cause(ctx); cause != nil {
		return value, cause
	}
	return value, err
}

type execCaptureProgressReader struct {
	ctx    context.Context
	clock  Clock
	cancel context.CancelCauseFunc
	body   io.Reader
}

func (r execCaptureProgressReader) Read(p []byte) (int, error) {
	return captureWithIdle(r.ctx, r.clock, r.cancel, func() (int, error) { return r.body.Read(p) })
}

// Use the existing authenticated Fabric transport without the appender's 30 s
// total HTTP budget. The attempt context and per-operation idle timer bound it.
func seedExecCapture(ctx context.Context, spec contract.JobSpec, mailbox *runMailbox) (bool, error) {
	return seedExecCaptureWithRemoval(ctx, spec, mailbox, (*os.Root).RemoveAll)
}

func seedExecCaptureWithRemoval(ctx context.Context, spec contract.JobSpec, mailbox *runMailbox, removeAll func(*os.Root, string) error) (bool, error) {
	var ref struct {
		Version int    `json:"exec_version"`
		SHA256  string `json:"capture_sha256"`
		Size    int64  `json:"capture_size_bytes"`
	}
	if json.Unmarshal([]byte(spec.Labels[contract.LabelRunParams]), &ref) != nil || ref.Version != 2 {
		return false, nil
	}
	hash, e := hex.DecodeString(ref.SHA256)
	if e != nil || len(hash) != sha256.Size || strings.ToLower(ref.SHA256) != ref.SHA256 || ref.Size <= 0 {
		return false, fmt.Errorf("exec capture SHA-256 or size is invalid")
	}
	appender, ok := mailbox.appender.(*httpRunLedgerAppender)
	if !ok {
		return false, fmt.Errorf("exec capture fetch is unavailable")
	}
	// A retry or retained mailbox may contain an interrupted earlier attempt.
	// These diagnostics belong to stale input, never the new Attempt's evidence.
	mailbox.sweepExecCaptureRoot(ctx, mailbox.root, true)
	for _, name := range []string{"tmp/capture.bundle", "capture.bundle", "wefty-exec-work"} {
		if e = mailbox.root.RemoveAll(name); e != nil {
			return false, fmt.Errorf("remove stale exec input: %w", e)
		}
	}
	client := &http.Client{Transport: appender.client.Transport, CheckRedirect: appender.client.CheckRedirect, Jar: appender.client.Jar}
	var last error
	// Six attempts at 0, 1, 3, 7, 15 and 30 seconds let an L3 restart finish.
	// Cap the final exponential step at 15 s to bound total backoff to 30 s;
	// progressing downloads retain their independent inactivity budget.
	for _, delay := range []time.Duration{0, time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second, 15 * time.Second} {
		if delay > 0 {
			timer := mailbox.clock.NewTimer(delay)
			select {
			case <-ctx.Done():
				stopTimer(timer)
				return false, ctx.Err()
			case <-timer.C():
			}
			stopTimer(timer)
		}
		retry, err := downloadExecCapture(ctx, client, mailbox, ref.SHA256, ref.Size)
		if err == nil {
			// This handle stays owned by capture cleanup even if finalization transfers
			// the publisher's roots or closes them before the attempt's deferred cleanup.
			root, e := mailbox.root.OpenRoot(".")
			if e != nil {
				return false, e
			}
			var once sync.Once
			mailbox.captureSweep = func(ctx context.Context) []contract.StoppedProcess {
				return mailbox.sweepExecCaptureRoot(ctx, root, false)
			}
			mailbox.captureCleanup = func() {
				once.Do(func() {
					_ = root.Remove("capture.bundle")
					_ = root.Remove("tmp/capture.bundle")
					trash := ".wefty-trash-wefty-exec-work-" + rand.Text()
					if err := root.Rename("wefty-exec-work", trash); err != nil {
						root.Close()
						if !errors.Is(err, os.ErrNotExist) {
							mailbox.log("agent: retire exec capture checkout: %v", err)
						}
						return
					}
					// Keep both retirement and deletion anchored to the retained
					// directory. Never resolve the mailbox's pathname again: the
					// workload may have moved it and replaced it with a symlink.
					// Only the deleter owns this handle after cleanup returns.
					go func() {
						defer root.Close()
						if err := removeAll(root, trash); err != nil {
							mailbox.log("agent: delete exec capture trash: %v", err)
						}
					}()
				})
			}
			return true, nil
		}
		last = err
		if !retry || ctx.Err() != nil {
			return false, err
		}
	}
	return false, last
}
func downloadExecCapture(parent context.Context, client *http.Client, mailbox *runMailbox, hash string, expected int64) (bool, error) {
	ctx, cancel := context.WithCancelCause(parent)
	defer cancel(nil)
	request, e := http.NewRequestWithContext(ctx, http.MethodGet, "http://wefty.invalid/v1/runs/"+url.PathEscape(mailbox.runID)+"/exec-capture", nil)
	if e != nil {
		return false, e
	}
	request.Header.Set("Authorization", "Bearer "+mailbox.runToken)
	response, e := captureWithIdle(ctx, mailbox.clock, cancel, func() (*http.Response, error) { return client.Do(request) })
	if e != nil {
		if response != nil {
			response.Body.Close()
		}
		return !errors.Is(e, errExecCaptureIdle), fmt.Errorf("fetch exec capture: %w", e)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return response.StatusCode >= 500, fmt.Errorf("fetch exec capture: HTTP %d", response.StatusCode)
	}
	// L3 declares the stored file's size. A different length is corrupt
	// stored input, not an interrupted transfer that another fetch can repair.
	if response.ContentLength >= 0 && response.ContentLength != expected {
		return false, fmt.Errorf("exec capture size mismatch: declared %d bytes, expected %d bytes", response.ContentLength, expected)
	}
	file, e := mailbox.root.OpenFile("tmp/capture.bundle", os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if e != nil {
		return false, fmt.Errorf("stage exec capture: %w", e)
	}
	defer file.Close()
	defer mailbox.root.Remove("tmp/capture.bundle")
	digest := sha256.New()
	size, e := io.Copy(io.MultiWriter(file, digest), io.LimitReader(execCaptureProgressReader{ctx, mailbox.clock, cancel, response.Body}, expected+1))
	if e != nil {
		// Proven oversize stops even when the same read also failed.
		return size <= expected && (response.ContentLength < 0 || size < response.ContentLength) && !errors.Is(e, errExecCaptureIdle), fmt.Errorf("exec capture fetch failed: %w", e)
	}
	if size != expected {
		return size < response.ContentLength, fmt.Errorf("exec capture size mismatch: received %d bytes, expected %d bytes", size, expected)
	}
	if hex.EncodeToString(digest.Sum(nil)) != hash {
		return false, fmt.Errorf("exec capture SHA-256 mismatch")
	}
	if e = file.Close(); e != nil {
		return false, fmt.Errorf("close exec capture: %w", e)
	}
	if e = mailbox.root.Rename("tmp/capture.bundle", "capture.bundle"); e != nil {
		return false, fmt.Errorf("commit exec capture: %w", e)
	}
	return false, nil
}
