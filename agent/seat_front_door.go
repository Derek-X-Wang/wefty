package agent

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"sync"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
)

// One node listener carries all Seat features. Attempts register a feature mux
// and bind only the Seat IDs supplied by their fenced L1 wanted list.
type seatFrontDoor struct {
	mu        sync.RWMutex
	fabric    fabric.Fabric
	endpoint  string
	done      chan struct{}
	serveErr  error
	computers map[string]*seatFrontDoorRegistration
	seats     map[string]*seatFrontDoorRegistration
}
type seatFrontDoorRegistration struct {
	door              *seatFrontDoor
	ctx               context.Context
	computer, attempt string
	handler           http.Handler
}

var seatFrontDoorPath = regexp.MustCompile(`^/v1/seats/(seat_[0-9a-f]{32})(?:/|$)`)

func newSeatFrontDoor(f fabric.Fabric) *seatFrontDoor {
	return &seatFrontDoor{fabric: f, computers: map[string]*seatFrontDoorRegistration{}, seats: map[string]*seatFrontDoorRegistration{}}
}
func startSeatFrontDoor(ctx context.Context, f fabric.Fabric, node string) (*seatFrontDoor, func(), error) {
	address := "wefty://node/" + node
	if local, ok := f.(fabric.LocalServiceAddressProvider); ok {
		address = local.LocalServiceAddress()
	}
	listener, err := f.Listen("tcp", address)
	if err != nil {
		return nil, nil, fmt.Errorf("listen on Seat front door: %w", err)
	}
	port, err := listenerPort(listener.Addr())
	if err != nil {
		listener.Close()
		return nil, nil, err
	}
	door := newSeatFrontDoor(f)
	door.endpoint = (&url.URL{Scheme: "ws", Host: net.JoinHostPort(f.ConnectHost(), strconv.Itoa(port)), Path: contract.ComputerDisplayWebSocketPath}).String()
	server := &http.Server{Handler: door, BaseContext: func(net.Listener) context.Context { return ctx }}
	door.done = make(chan struct{})
	go func() {
		defer close(door.done)
		if err := server.Serve(listener); !errors.Is(err, http.ErrServerClosed) {
			door.serveErr = err
		}
	}()
	return door, func() { _ = server.Close(); <-door.done }, nil
}
func (d *seatFrontDoor) register(ctx context.Context, computer, attempt string, h http.Handler) (*seatFrontDoorRegistration, error) {
	if d == nil || ctx.Err() != nil || h == nil {
		return nil, errors.New("Seat front door or Attempt unavailable")
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if old := d.computers[computer]; old != nil && old.ctx.Err() == nil {
		return nil, errors.New("Computer already has a Seat front-door Attempt")
	}
	r := &seatFrontDoorRegistration{door: d, ctx: ctx, computer: computer, attempt: attempt, handler: h}
	d.computers[computer] = r
	return r, nil
}
func (r *seatFrontDoorRegistration) bind(ids []string) error {
	d := r.door
	d.mu.Lock()
	defer d.mu.Unlock()
	if r.ctx.Err() != nil || d.computers[r.computer] != r {
		return errors.New("stale Seat front-door Attempt")
	}
	for _, id := range ids {
		if old := d.seats[id]; old != nil && old != r && old.ctx.Err() == nil {
			return errors.New("Seat already belongs to another front-door Attempt")
		}
	}
	for id, old := range d.seats {
		if old == r {
			delete(d.seats, id)
		}
	}
	for _, id := range ids {
		d.seats[id] = r
	}
	return nil
}
func (r *seatFrontDoorRegistration) close() {
	d := r.door
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.computers[r.computer] == r {
		delete(d.computers, r.computer)
	}
	for id, old := range d.seats {
		if old == r {
			delete(d.seats, id)
		}
	}
}
func (d *seatFrontDoor) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	match := seatFrontDoorPath.FindStringSubmatch(r.URL.Path)
	if len(match) != 2 {
		http.NotFound(w, r)
		return
	}
	d.mu.RLock()
	registration := d.seats[match[1]]
	d.mu.RUnlock()
	if registration == nil || registration.ctx.Err() != nil {
		http.NotFound(w, r)
		return
	}
	if d.fabric != nil {
		if _, err := d.fabric.WhoIs(r.Context(), r.RemoteAddr); err != nil {
			http.Error(w, "Fabric identity unavailable", http.StatusUnauthorized)
			return
		}
	}
	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()
	stop := context.AfterFunc(registration.ctx, cancel)
	defer stop()
	registration.handler.ServeHTTP(w, r.WithContext(ctx))
}
