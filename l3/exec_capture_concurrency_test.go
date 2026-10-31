package l3

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/internal/testwait"
)

func TestExecCaptureSlowDownloadDoesNotBlockLedger(t *testing.T) {
	h := newIntegrationHarness(t)
	payload := bytes.Repeat([]byte("bundle"), 1<<20)
	hash := uploadExecFixture(t, h, payload)
	run := h.submit(execCaptureRunRequest(hash, len(payload)), "slow-download")
	reconciler, e := NewReconciler(h.l3Store, h.l1Client, ReconcilerConfig{})
	if e != nil {
		t.Fatal(e)
	}
	if e = reconciler.ReconcileOnce(t.Context()); e != nil {
		t.Fatal(e)
	}
	tokens := claimRunTokens(t, h, 1)
	request, _ := http.NewRequest(http.MethodGet, "http://wefty.invalid/v1/runs/"+run.RunID+"/exec-capture", nil)
	request.Header.Set("Authorization", "Bearer "+tokens[run.RunID])
	response, e := h.client(fabric.Identity{NodeID: "slow-reader"}, DefaultL3Address).Do(request)
	if e != nil {
		t.Fatal(e)
	}
	defer response.Body.Close()
	// Leave the multi-megabyte body unread. The server cannot finish this copy
	// into the bounded HTTP/Fabric buffers.
	for name, check := range map[string]func(context.Context) error{
		"reconcile": reconciler.ReconcileOnce,
		"upload": func(ctx context.Context) error {
			p := []byte("another capture")
			hash := sha256.Sum256(p)
			r, _ := http.NewRequestWithContext(ctx, http.MethodPost, "http://wefty.invalid/v1/exec-captures/"+hex.EncodeToString(hash[:]), bytes.NewReader(p))
			res, e := h.caller.Do(r)
			if e != nil {
				return e
			}
			defer res.Body.Close()
			if res.StatusCode != 201 {
				return fmt.Errorf("status=%d", res.StatusCode)
			}
			return nil
		},
	} {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			defer cancel()
			done := make(chan error, 1)
			go func() { done <- check(ctx) }()
			select {
			case e := <-done:
				if e != nil {
					t.Error(e)
				}
			case <-ctx.Done():
				t.Error("slow download blocked ledger")
			}
		})
	}
}
func TestExecCaptureBusyCleanupNeverWaitsInReconcile(t *testing.T) {
	h := newIntegrationHarness(t)
	reconciler, e := NewReconciler(h.l3Store, h.l1Client, ReconcilerConfig{})
	if e != nil {
		t.Fatal(e)
	}
	h.l3Store.execCaptureMu.Lock()
	ctx, cancel := context.WithTimeout(t.Context(), 200*time.Millisecond)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- reconciler.ReconcileOnce(ctx) }()
	select {
	case e := <-done:
		if e != nil {
			t.Error(e)
		}
		h.l3Store.execCaptureMu.Unlock()
	case <-time.After(testwait.Bound):
		t.Error("reconciler waited for cleanup lock")
		h.l3Store.execCaptureMu.Unlock()
		<-done
	}
}

func TestExecCaptureConcurrentUploadsReserveTotalCapacity(t *testing.T) {
	h := newIntegrationHarness(t)
	h.l3Store.execCaptureMax = 8
	h.l3Store.execCaptureTotal = 10
	reader, writer := io.Pipe()
	defer reader.Close()
	defer writer.Close()
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseUpload := func() { releaseOnce.Do(func() { close(release) }) }
	defer releaseUpload()
	go func() { writer.Write([]byte("a")); <-release; writer.Write([]byte("bcdefgh")); writer.Close() }()
	request, _ := http.NewRequest(http.MethodPost, "http://wefty.invalid/v1/exec-captures/"+strings.Repeat("a", 64), reader)
	request.ContentLength = 8
	done := make(chan error, 1)
	go func() {
		res, e := h.caller.Do(request)
		if e == nil {
			res.Body.Close()
			if res.StatusCode != 400 {
				e = fmt.Errorf("injected bad digest status=%d", res.StatusCode)
			}
		}
		done <- e
	}()
	deadline := time.Now().Add(3 * time.Second)
	for {
		entries, _ := os.ReadDir(filepath.Join(filepath.Dir(h.l3Path), "exec-captures"))
		staged := false
		for _, entry := range entries {
			if strings.HasPrefix(entry.Name(), ".upload-") {
				staged = true
			}
		}
		if staged {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("upload did not stage")
		}
		time.Sleep(time.Millisecond)
	}
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	payload := []byte("12345678")
	hash := sha256.Sum256(payload)
	second, _ := http.NewRequestWithContext(ctx, http.MethodPost, "http://wefty.invalid/v1/exec-captures/"+hex.EncodeToString(hash[:]), bytes.NewReader(payload))
	res, e := h.caller.Do(second)
	if e != nil {
		t.Fatal(e)
	}
	body, _ := io.ReadAll(res.Body)
	res.Body.Close()
	if res.StatusCode != 409 || !strings.Contains(string(body), "8 bytes held") {
		t.Errorf("reserved-cap response=%d %s", res.StatusCode, body)
	}
	// Complete the first stream with a bad digest, releasing its reservation.
	releaseUpload()
	if e := <-done; e != nil {
		t.Fatal(e)
	}
	uploadExecFixture(t, h, payload)
}
