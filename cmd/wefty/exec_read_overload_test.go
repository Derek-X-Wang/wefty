package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/fabric/plain"
)

func setExecReadWindows(t *testing.T, restart, overload, poll time.Duration) {
	t.Helper()
	oldRestart, oldOverload, oldPoll := execReadRecoveryBudget, execReadOverloadBudget, execPollInterval
	execReadRecoveryBudget, execReadOverloadBudget, execPollInterval = restart, overload, poll
	t.Cleanup(func() {
		execReadRecoveryBudget, execReadOverloadBudget, execPollInterval = oldRestart, oldOverload, oldPoll
	})
}

// A busy service (#1629) outlasts the restart window, and its reads are spaced
// further apart each time. An unreachable one still ends at the restart window.
func TestExecReadOutlastsBusyServiceButNotUnreachableOne(t *testing.T) {
	busy := &apiResponseError{Service: "L3", StatusCode: 503, APIError: contract.APIError{Code: contract.ErrorUnavailable, Retryable: true}}
	unreachable := &unavailableError{cause: errors.New("connection refused")}

	t.Run("busy answers grow the gap", func(t *testing.T) {
		setExecReadWindows(t, time.Minute, time.Minute, time.Second)
		var gaps []time.Duration
		clients := &apiClients{wait: func(_ context.Context, gap time.Duration) error {
			gaps = append(gaps, gap)
			return nil
		}}
		reads := 0
		notices := 0
		value, err := readExec(t.Context(), clients, func() { notices++ }, func(context.Context) (int, error) {
			if reads++; reads <= 6 {
				return 0, busy
			}
			return 7, nil
		})
		want := []time.Duration{time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second, 10 * time.Second, 10 * time.Second}
		// The restart window has not passed, so the read stays quiet.
		if err != nil || value != 7 || !slices.Equal(gaps, want) || notices != 0 {
			t.Fatalf("busy read: value=%d err=%v gaps=%v notices=%d", value, err, gaps, notices)
		}
	})

	for name, failure := range map[string]func(context.Context) error{
		"busy answer": func(context.Context) error { return busy },
		"no answer": func(ctx context.Context) error {
			<-ctx.Done()
			return &unavailableError{cause: ctx.Err()}
		},
	} {
		t.Run(name+" outlasts the restart window", func(t *testing.T) {
			setExecReadWindows(t, 50*time.Millisecond, time.Minute, 5*time.Millisecond)
			started := time.Now()
			notices := 0
			value, err := readExec(t.Context(), &apiClients{wait: waitForContext}, func() { notices++ }, func(ctx context.Context) (int, error) {
				if time.Since(started) < 250*time.Millisecond {
					return 0, failure(ctx)
				}
				return 7, nil
			})
			if err != nil || value != 7 || notices != 1 {
				t.Fatalf("read gave up on a busy service: value=%d err=%v notices=%d", value, err, notices)
			}
		})
		t.Run(name+" ends at the overload window", func(t *testing.T) {
			setExecReadWindows(t, 50*time.Millisecond, 250*time.Millisecond, 5*time.Millisecond)
			started := time.Now()
			_, err := readExec(t.Context(), &apiClients{wait: waitForContext}, func() {}, func(ctx context.Context) (int, error) {
				return 0, failure(ctx)
			})
			if err == nil || time.Since(started) < 250*time.Millisecond {
				t.Fatalf("persistent loss: err=%v after %v", err, time.Since(started))
			}
		})
		// A read that does not follow a live run, such as the drain after a
		// known verdict, gives a busy service the restart window only.
		t.Run(name+" ends at the restart window when not following", func(t *testing.T) {
			setExecReadWindows(t, 50*time.Millisecond, time.Minute, 5*time.Millisecond)
			started := time.Now()
			_, err := readExec(t.Context(), &apiClients{wait: waitForContext}, nil, func(ctx context.Context) (int, error) {
				return 0, failure(ctx)
			})
			if err == nil || time.Since(started) > 10*time.Second {
				t.Fatalf("read without a follow: err=%v after %v", err, time.Since(started))
			}
		})
	}

	// The restart window starts at the first unreachable failure in a row,
	// not at the start of a read that was busy before.
	t.Run("unreachable after busy still gets the restart window", func(t *testing.T) {
		setExecReadWindows(t, 200*time.Millisecond, time.Minute, 5*time.Millisecond)
		started := time.Now()
		sawUnreachable := false
		value, err := readExec(t.Context(), &apiClients{wait: waitForContext}, func() {}, func(context.Context) (int, error) {
			switch elapsed := time.Since(started); {
			case elapsed < 300*time.Millisecond:
				return 0, busy
			case !sawUnreachable:
				sawUnreachable = true
				return 0, unreachable
			}
			return 7, nil
		})
		if err != nil || value != 7 || !sawUnreachable {
			t.Fatalf("unreachable after busy: value=%d err=%v unreachable=%v", value, err, sawUnreachable)
		}
	})

	t.Run("unreachable ends at the restart window", func(t *testing.T) {
		setExecReadWindows(t, 50*time.Millisecond, time.Minute, 5*time.Millisecond)
		var gaps []time.Duration
		clients := &apiClients{wait: func(ctx context.Context, gap time.Duration) error {
			gaps = append(gaps, gap)
			return waitForContext(ctx, gap)
		}}
		started := time.Now()
		notices := 0
		_, err := readExec(t.Context(), clients, func() { notices++ }, func(context.Context) (int, error) { return 0, unreachable })
		if !errors.Is(err, unreachable) || time.Since(started) > 10*time.Second || notices != 0 {
			t.Fatalf("unreachable read: err=%v after %v notices=%d", err, time.Since(started), notices)
		}
		for _, gap := range gaps {
			if gap != 5*time.Millisecond {
				t.Fatalf("unreachable read backed off: %v", gaps)
			}
		}
	})
}

// The shipped binary keeps following a run whose log reads are refused for
// longer than the restart window, and reports the command's own exit (#1629).
// Before, it left with exit 13 while the run went on.
func TestExecOutlastsBusyLogReadsFromRealBinary(t *testing.T) {
	binary := buildWefty(t)
	stack := newExecStack(t, execRealBinaryIdleLimit)
	t.Chdir(execTestRepo(t))
	const busyFor = 45 * time.Second
	if busyFor <= execReadRecoveryBudget {
		t.Fatalf("busy period %v must exceed the restart window %v", busyFor, execReadRecoveryBudget)
	}
	upstream, _ := url.Parse("http://wefty.invalid")
	proxy := httputil.NewSingleHostReverseProxy(upstream)
	proxy.Transport = stack.clients.l3.client.Transport
	var mu sync.Mutex
	var busySince time.Time
	refused := 0
	front := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/logs") {
			mu.Lock()
			if busySince.IsZero() {
				busySince = time.Now()
			}
			busy := time.Since(busySince) < busyFor
			if busy {
				refused++
			}
			mu.Unlock()
			if busy {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusServiceUnavailable)
				fmt.Fprint(w, `{"error":{"code":"unavailable","message":"busy","retryable":true}}`)
				return
			}
		}
		proxy.ServeHTTP(w, r)
	}))
	listener, err := plain.NewNetwork().NewFabric(fabric.Identity{NodeID: "busy-proxy"}).Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	_ = front.Listener.Close()
	front.Listener = listener
	front.Start()
	t.Cleanup(front.Close)
	started := time.Now()
	code, out := runWefty(t, binary, 3*time.Minute, "--l1="+stack.l1Address, "--l3="+strings.TrimPrefix(front.URL, "http://"),
		"--node-config="+stack.clients.nodeConfigPath, "exec", "--", "/bin/sh", "-c", "printf 'tail-output\\n'; exit 7")
	mu.Lock()
	defer mu.Unlock()
	if code != 10 || strings.Count(out, "tail-output\n") != 1 || strings.Contains(out, "lost observation") ||
		strings.Count(out, "wefty: control plane busy; still following run ") != 1 || time.Since(started) < busyFor || refused < 2 {
		t.Fatalf("busy log reads: code=%d refused=%d after %v output=%s", code, refused, time.Since(started), out)
	}
}
