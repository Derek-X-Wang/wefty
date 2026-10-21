// Package peerprobe observes one service peer through its owner's Fabric.
// It never opens a database or changes authority.
package peerprobe

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
)

const Timeout = 2 * time.Second

type observation struct {
	done   chan struct{}
	result contract.PeerHealth
}

type Probe struct {
	fabric            fabric.Fabric
	address, expected string
	mu                sync.Mutex
	active            *observation
}

func New(f fabric.Fabric, address, expected string) *Probe {
	return &Probe{fabric: f, address: address, expected: expected}
}

// Check coalesces overlapping requests. Each caller can cancel its own wait;
// the shared check has its own two-second ceiling and survives any caller's
// cancellation. Results are not cached between calls.
func (p *Probe) Check(ctx context.Context) contract.PeerHealth {
	ctx, cancel := context.WithTimeout(ctx, Timeout)
	defer cancel()
	if ctx.Err() != nil {
		return failed("unavailable")
	}
	p.mu.Lock()
	pending := p.active
	if pending == nil {
		pending = &observation{done: make(chan struct{})}
		p.active = pending
		shared, stop := context.WithTimeout(context.WithoutCancel(ctx), Timeout)
		go func() {
			defer stop()
			result := p.check(shared)
			p.mu.Lock()
			pending.result = result
			p.active = nil
			close(pending.done)
			p.mu.Unlock()
		}()
	}
	p.mu.Unlock()
	select {
	case <-pending.done:
		return pending.result
	case <-ctx.Done():
		return failed("unavailable")
	}
}

func failed(class string) contract.PeerHealth {
	return contract.PeerHealth{ErrorClass: class, CheckedAt: time.Now().UTC()}
}

func (p *Probe) check(ctx context.Context) contract.PeerHealth {
	address := p.address
	// An authenticated network must resolve the exact enrolled opaque ID. The
	// adapter owns that resolution; no service name or legacy address can replace
	// it. Plain's in-process evidence remains explicitly self-asserted.
	if trust, ok := p.fabric.(fabric.PersonIdentityTrustProvider); ok && trust.PersonIdentityTrust() == fabric.PersonIdentityAuthenticated {
		address = p.expected
	}
	if p.fabric == nil || address == "" || p.expected == "" {
		return failed("unavailable")
	}
	conn, err := p.fabric.Dial(ctx, "tcp", address)
	if err != nil {
		return failed("unavailable")
	}
	defer conn.Close()
	stop := context.AfterFunc(ctx, func() { _ = conn.Close() })
	defer stop()
	if deadline, ok := ctx.Deadline(); ok {
		if conn.SetDeadline(deadline) != nil {
			return failed("unavailable")
		}
	}
	peer, err := p.fabric.WhoIs(ctx, conn.RemoteAddr().String())
	if err != nil {
		return failed("unavailable")
	}
	if peer.NodeID != p.expected {
		return failed("forbidden")
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://peer.invalid/v1/peer/ping", nil)
	if err != nil {
		return failed("internal")
	}
	request.Close = true
	if err := request.Write(conn); err != nil {
		return failed("unavailable")
	}
	// Bound headers and body together. ReadResponse follows no redirects and
	// this connection carries exactly one request.
	response, err := http.ReadResponse(bufio.NewReader(io.LimitReader(conn, 4097)), request)
	if err != nil {
		return failed("unavailable")
	}
	defer response.Body.Close()
	switch response.StatusCode {
	case http.StatusOK:
	case http.StatusUnauthorized, http.StatusForbidden:
		return failed("forbidden")
	case http.StatusNotFound:
		return failed("not_found")
	default:
		return failed("unavailable")
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, 4097))
	if err != nil || len(body) > 4096 {
		return failed("unavailable")
	}
	var pong contract.PeerPong
	if json.Unmarshal(body, &pong) != nil || !pong.OK {
		return failed("internal")
	}
	if ctx.Err() != nil {
		return failed("unavailable")
	}
	return contract.PeerHealth{Reachable: true, CheckedAt: time.Now().UTC()}
}
