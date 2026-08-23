package ocihelper

import (
	"context"
	"errors"
	"runtime"
	"sync/atomic"
	"testing"
	"time"
)

func testImageOperationKey(digest string) imageOperationKey {
	return imageOperationKey{Namespace: ContainerdNamespace, Digest: digest, Platform: "linux/amd64", Snapshotter: DefaultSnapshotter}
}

func TestImageOperationLeaderCancellationDoesNotCancelSharedWork(t *testing.T) {
	group := newImageOperationGroup()
	started := make(chan struct{})
	release := make(chan struct{})
	var calls atomic.Int32
	run := func(ctx context.Context) (EnsureImageResponse, error) {
		if calls.Add(1) == 1 {
			close(started)
		}
		select {
		case <-release:
			return EnsureImageResponse{TopLevelDigest: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", PlatformDigest: "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"}, nil
		case <-ctx.Done():
			return EnsureImageResponse{}, ctx.Err()
		}
	}

	leaderContext, cancelLeader := context.WithCancel(t.Context())
	key := testImageOperationKey("sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	leaderDone := make(chan error, 1)
	go func() {
		_, err := group.Do(leaderContext, key, time.Minute, run)
		leaderDone <- err
	}()
	<-started

	waiterDone := make(chan error, 1)
	go func() {
		_, err := group.Do(t.Context(), key, time.Minute, run)
		waiterDone <- err
	}()
	for {
		group.mu.Lock()
		flight := group.flights[key]
		joined := flight != nil && flight.waiters == 2
		group.mu.Unlock()
		if joined {
			break
		}
		runtime.Gosched()
	}
	cancelLeader()
	if err := <-leaderDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("leader error = %v, want context cancellation", err)
	}
	close(release)
	if err := <-waiterDone; err != nil {
		t.Fatalf("shared waiter failed after leader cancellation: %v", err)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("shared operation calls = %d, want 1", got)
	}
}

func TestImageOperationSessionCancellationIsBounded(t *testing.T) {
	group := newImageOperationGroup()
	started := make(chan struct{})
	release := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = group.Do(t.Context(), testImageOperationKey("bounded"), time.Minute, func(context.Context) (EnsureImageResponse, error) {
			close(started)
			<-release
			return EnsureImageResponse{}, nil
		})
	}()
	<-started
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Millisecond)
	defer cancel()
	if err := group.CancelAll(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("bounded cancellation error = %v, want deadline", err)
	}
	close(release)
	<-done
}
