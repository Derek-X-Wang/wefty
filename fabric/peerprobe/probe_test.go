package peerprobe_test

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/fabric/peerprobe"
	"github.com/Derek-X-Wang/wefty/fabric/plain"
)

type probeFabric struct {
	fabric.Fabric
	dial  func(context.Context, string, string) (net.Conn, error)
	who   func(context.Context, string) (fabric.Identity, error)
	trust fabric.PersonIdentityTrust
}

func (f probeFabric) Dial(ctx context.Context, n, a string) (net.Conn, error) {
	return f.dial(ctx, n, a)
}
func (f probeFabric) WhoIs(ctx context.Context, a string) (fabric.Identity, error) {
	return f.who(ctx, a)
}
func (f probeFabric) PersonIdentityTrust() fabric.PersonIdentityTrust { return f.trust }

func TestPeerProbeCoalescesAndCancelsWaiters(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	var calls atomic.Int32
	f := probeFabric{dial: func(ctx context.Context, _, _ string) (net.Conn, error) {
		calls.Add(1)
		close(entered)
		select {
		case <-release:
		case <-ctx.Done():
		}
		return nil, errors.New("private diagnostic")
	}}
	p := peerprobe.New(f, "peer-target", "enrolled-peer")
	done := make(chan struct{})
	go func() { defer close(done); p.Check(t.Context()) }()
	<-entered
	// All these calls overlap the initiating request. Canceling their waits
	// must neither start another dial nor cancel the initiating request.
	var wg sync.WaitGroup
	for range 32 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithCancel(t.Context())
			cancel()
			result := p.Check(ctx)
			if result.Reachable || result.ErrorClass != "unavailable" {
				t.Errorf("canceled waiter=%+v", result)
			}
		}()
	}
	wg.Wait()
	if calls.Load() != 1 {
		t.Fatalf("outbound dials=%d", calls.Load())
	}
	close(release)
	<-done
}

func TestPeerProbeAuthenticatedTargetAndMismatch(t *testing.T) {
	for _, actual := range []string{"enrolled-peer", "wrong-peer"} {
		t.Run(actual, func(t *testing.T) {
			var requests atomic.Int32
			f := probeFabric{trust: fabric.PersonIdentityAuthenticated, who: func(context.Context, string) (fabric.Identity, error) { return fabric.Identity{NodeID: actual}, nil }}
			f.dial = func(ctx context.Context, network, address string) (net.Conn, error) {
				if address != "enrolled-peer" || network != "tcp" {
					t.Errorf("dial used %q %q", network, address)
				}
				a, b := net.Pipe()
				go func() {
					defer b.Close()
					request, err := http.ReadRequest(bufio.NewReader(b))
					if err != nil {
						return
					}
					requests.Add(1)
					if request.Method != "GET" || request.URL.Path != "/v1/peer/ping" {
						t.Errorf("request=%s %s", request.Method, request.URL)
					}
					_, _ = io.WriteString(b, "HTTP/1.1 200 OK\r\nContent-Length: 11\r\n\r\n{\"ok\":true}")
				}()
				return a, nil
			}
			result := peerprobe.New(f, "wefty://untrusted-default", "enrolled-peer").Check(t.Context())
			if result.Reachable != (actual == "enrolled-peer") {
				t.Fatalf("result=%+v", result)
			}
			if actual == "wrong-peer" && (result.ErrorClass != "forbidden" || requests.Load() != 0) {
				t.Fatalf("wrong peer received request: %+v requests=%d", result, requests.Load())
			}
		})
	}
}

func TestPeerProbeBoundedDialAndBody(t *testing.T) {
	for _, mode := range []string{"dial", "body", "redirect", "missing ping", "invalid pong", "oversized"} {
		t.Run(mode, func(t *testing.T) {
			f := probeFabric{who: func(context.Context, string) (fabric.Identity, error) { return fabric.Identity{NodeID: "peer"}, nil }}
			f.dial = func(ctx context.Context, _, _ string) (net.Conn, error) {
				if mode == "dial" {
					<-ctx.Done()
					return nil, ctx.Err()
				}
				a, b := net.Pipe()
				go func() {
					defer b.Close()
					if _, err := http.ReadRequest(bufio.NewReader(b)); err != nil {
						return
					}
					switch mode {
					case "body":
						_, _ = io.WriteString(b, "HTTP/1.1 200 OK\r\nContent-Length: 11\r\n\r\n")
						<-ctx.Done()
					case "redirect":
						_, _ = io.WriteString(b, "HTTP/1.1 302 Found\r\nLocation: http://other.invalid\r\nContent-Length: 0\r\n\r\n")
					case "missing ping":
						_, _ = io.WriteString(b, "HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\n\r\n")
					case "invalid pong":
						_, _ = io.WriteString(b, "HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\n{}")
					case "oversized":
						_, _ = io.WriteString(b, "HTTP/1.1 200 OK\r\nContent-Length: 5000\r\n\r\n"+strings.Repeat("x", 5000))
					}
				}()
				return a, nil
			}
			started := time.Now()
			result := peerprobe.New(f, "target", "peer").Check(t.Context())
			expected := "unavailable"
			if mode == "missing ping" {
				expected = "not_found"
			}
			if mode == "invalid pong" {
				expected = "internal"
			}
			if result.Reachable || result.ErrorClass != expected || time.Since(started) > 3*time.Second {
				t.Fatalf("%s: %+v duration=%s", mode, result, time.Since(started))
			}
		})
	}
}

func TestPeerProbePlainConcurrentHealthyRequests(t *testing.T) {
	n := plain.NewNetwork()
	peer := n.NewFabric(fabric.Identity{NodeID: "peer"})
	observer := n.NewFabric(fabric.Identity{NodeID: "observer"})
	ln, err := peer.Listen("tcp", "wefty://run-ledger")
	if err != nil {
		t.Fatal(err)
	}
	entered, release := make(chan struct{}), make(chan struct{})
	var pings atomic.Int32
	server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if pings.Add(1) == 1 {
			close(entered)
		}
		<-release
		_, _ = io.WriteString(w, "{\"ok\":true}")
	})}
	defer server.Close()
	go server.Serve(ln)
	p := peerprobe.New(observer, "wefty://run-ledger", "peer")
	leader := make(chan struct{})
	go func() {
		defer close(leader)
		if !p.Check(t.Context()).Reachable {
			t.Error("leader unreachable")
		}
	}()
	<-entered
	var waiters sync.WaitGroup
	for range 24 {
		waiters.Add(1)
		go func() {
			defer waiters.Done()
			ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
			defer cancel()
			result := p.Check(ctx)
			if result.Reachable || result.ErrorClass != "unavailable" {
				t.Errorf("waiter=%+v", result)
			}
		}()
	}
	waiters.Wait()
	close(release)
	<-leader
	if pings.Load() != 1 {
		t.Fatalf("pings=%d", pings.Load())
	}
}

func TestPeerProbeFirstCallerCancellationDoesNotSpoilWaiter(t *testing.T) {
	requested := make(chan struct{})
	var calls atomic.Int32
	f := probeFabric{who: func(context.Context, string) (fabric.Identity, error) { return fabric.Identity{NodeID: "peer"}, nil }}
	f.dial = func(ctx context.Context, _, _ string) (net.Conn, error) {
		calls.Add(1)
		a, b := net.Pipe()
		go func() {
			defer b.Close()
			if _, err := http.ReadRequest(bufio.NewReader(b)); err != nil {
				return
			}
			close(requested)
			timer := time.NewTimer(100 * time.Millisecond)
			defer timer.Stop()
			select {
			case <-timer.C:
			case <-ctx.Done():
				return
			}
			_, _ = io.WriteString(b, "HTTP/1.1 200 OK\r\nContent-Length: 11\r\n\r\n{\"ok\":true}")
		}()
		return a, nil
	}
	p := peerprobe.New(f, "peer-target", "peer")
	firstCtx, cancel := context.WithCancel(t.Context())
	defer cancel()
	first := make(chan bool, 1)
	go func() { first <- p.Check(firstCtx).Reachable }()
	select {
	case <-requested:
	case <-time.After(time.Second):
		t.Fatal("first caller did not start its ping")
	}
	time.AfterFunc(10*time.Millisecond, cancel)
	waiter := p.Check(t.Context())
	if !waiter.Reachable || waiter.ErrorClass != "" {
		t.Fatalf("first caller cancellation spoiled waiter: %+v", waiter)
	}
	select {
	case reachable := <-first:
		if reachable {
			t.Error("cancelled first caller ignored cancellation")
		}
	case <-time.After(time.Second):
		t.Fatal("cancelled caller did not return")
	}
	if calls.Load() != 1 {
		t.Fatalf("shared check dialed %d times", calls.Load())
	}
}
