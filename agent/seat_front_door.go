package agent

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/internal/screentiming"
	"github.com/Derek-X-Wang/wefty/l1"
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
	publicationMu     *sync.Mutex
	seatContexts      map[string]context.Context
	seatCancels       map[string]context.CancelFunc
}

var seatFrontDoorPath = regexp.MustCompile(`^/v1/seats/(seat_[0-9a-f]{32})(?:/|$)`)

func newSeatFrontDoor(f fabric.Fabric) *seatFrontDoor {
	return &seatFrontDoor{fabric: f, computers: map[string]*seatFrontDoorRegistration{}, seats: map[string]*seatFrontDoorRegistration{}}
}
func startSeatFrontDoor(ctx context.Context, f fabric.Fabric, node string) (*seatFrontDoor, func(), error) {
	var address string
	if local, ok := f.(fabric.LocalServiceAddressProvider); ok {
		address = local.LocalServiceAddress()
	} else {
		// Clients dial the published endpoint, not this private registration name.
		// Retiring and replacement node agent instances may share one plain network.
		address = "wefty://node/" + node + "-seat-" + rand.Text()
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

// #1254: Run owns retries and the recovered server in the same child scope.
// The first listen stays synchronous; retries cannot hold up non-Seat work.
func (a *Agent) startSeatFrontDoorRecovery(ctx context.Context, cancelRun context.CancelFunc) <-chan error {
	stopped := make(chan error, 1)
	a.setSeatFrontDoor(nil)
	if a.fabric == nil {
		stopped <- nil
		return stopped
	}
	door, closeDoor, err := startSeatFrontDoor(ctx, a.fabric, a.registration.NodeID)
	if err != nil {
		a.log("Seat front door unavailable; node retries in the background: %v", err)
	} else {
		a.setSeatFrontDoor(door)
	}
	go func() {
		delay := time.Second
		recovering := err != nil
		for err != nil {
			timer := a.clock.NewTimer(delay)
			select {
			case <-ctx.Done():
				stopTimer(timer)
				stopped <- nil
				return
			case <-timer.C():
			}
			stopTimer(timer)
			if ctx.Err() != nil {
				stopped <- nil
				return
			}
			door, closeDoor, err = startSeatFrontDoor(ctx, a.fabric, a.registration.NodeID)
			delay = min(delay*2, 30*time.Second)
		}
		if ctx.Err() == nil {
			if recovering {
				a.setSeatFrontDoor(door)
				a.log("Seat front door opened after retry")
			}
			select {
			case <-door.done:
				cancelRun()
			case <-ctx.Done():
			}
		}
		// Listen has no context argument. Even a listener returned after Run
		// cancellation belongs to this worker and must be closed and joined.
		closeDoor()
		stopped <- door.serveErr
	}()
	return stopped
}

func (a *Agent) currentSeatFrontDoor() *seatFrontDoor {
	a.seatDoorMu.RLock()
	defer a.seatDoorMu.RUnlock()
	return a.seatDoor
}

func (a *Agent) setSeatFrontDoor(door *seatFrontDoor) {
	a.seatDoorMu.Lock()
	defer a.seatDoorMu.Unlock()
	a.seatDoor = door
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
	publicationMu := &sync.Mutex{}
	if old := d.computers[computer]; old != nil {
		publicationMu = old.publicationMu
	}
	r := &seatFrontDoorRegistration{door: d, ctx: ctx, computer: computer, attempt: attempt, handler: h, publicationMu: publicationMu, seatContexts: map[string]context.Context{}, seatCancels: map[string]context.CancelFunc{}}
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
	wanted := map[string]bool{}
	for _, id := range ids {
		wanted[id] = true
	}
	for id, old := range d.seats {
		if old == r && !wanted[id] {
			r.seatCancels[id]()
			delete(r.seatContexts, id)
			delete(r.seatCancels, id)
			delete(d.seats, id)
		}
	}
	for _, id := range ids {
		if d.seats[id] != r {
			r.seatContexts[id], r.seatCancels[id] = context.WithCancel(r.ctx)
		}
		d.seats[id] = r
	}
	return nil
}
func (r *seatFrontDoorRegistration) close() {
	r.publicationMu.Lock()
	defer r.publicationMu.Unlock()
	d := r.door
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.computers[r.computer] == r {
		delete(d.computers, r.computer)
	}
	for id, old := range d.seats {
		if old == r {
			r.seatCancels[id]()
			delete(d.seats, id)
		}
	}
}
func (d *seatFrontDoor) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	timing := screentiming.Start("node_fabric")
	timing("http_received")
	match := seatFrontDoorPath.FindStringSubmatch(r.URL.Path)
	if len(match) != 2 {
		http.NotFound(w, r)
		return
	}
	// Person viewing and its sideband share the published door. Check
	// the HTTP host before any page or browser-readable capability can escape.
	suffix := strings.TrimPrefix(r.URL.Path, "/v1/seats/"+match[1])
	if strings.HasPrefix(suffix, "/person/") && !seatViewerHostAllowed(r.Host, d.endpoint) {
		writeComputerControlError(w, http.StatusForbidden, contract.APIError{Code: contract.ErrorForbidden, Message: "Seat viewer host is not allowed"}, nil)
		return
	}
	d.mu.RLock()
	registration := d.seats[match[1]]
	var seatCtx context.Context
	if registration != nil {
		seatCtx = registration.seatContexts[match[1]]
	}
	d.mu.RUnlock()
	if registration == nil || registration.ctx.Err() != nil {
		http.NotFound(w, r)
		return
	}
	if r.Header.Get(l1.SeatCredentialHeader) == "" && !strings.HasPrefix(strings.TrimPrefix(r.URL.Path, "/v1/seats/"+match[1]), "/person/") {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusUnauthorized)
		_ = json.NewEncoder(w).Encode(contract.ErrorResponse{Error: contract.APIError{Code: contract.ErrorUnauthorized, Message: "Seat credential is required"}})
		return
	}
	if d.fabric != nil {
		if _, err := d.fabric.WhoIs(r.Context(), r.RemoteAddr); err != nil {
			status := http.StatusServiceUnavailable
			message := "Fabric identity could not be verified"
			if errors.Is(err, fabric.ErrIdentityNotFound) {
				status = http.StatusUnauthorized
				message = "Fabric identity could not be authenticated"
			}
			http.Error(w, message, status)
			return
		}
	}
	timing("fabric_accepted")
	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()
	stop := context.AfterFunc(seatCtx, cancel)
	defer stop()
	ctx = context.WithValue(ctx, seatAuthorityContextKey{}, seatCtx)
	registration.handler.ServeHTTP(w, r.WithContext(ctx))
}

// Serialize only publication I/O, never registry or Attempt locks. Replacements
// share this gate and recheck registration authority before each remote call.
func (r *seatFrontDoorRegistration) publish(ctx context.Context, ready bool, endpoint string, send func(context.Context, bool, string) error) error {
	r.publicationMu.Lock()
	defer r.publicationMu.Unlock()
	r.door.mu.RLock()
	current := r.door.computers[r.computer] == r
	r.door.mu.RUnlock()
	if !current || ready && r.ctx.Err() != nil {
		return errStaleSeatPublication
	}
	return send(ctx, ready, endpoint)
}

var errStaleSeatPublication = errors.New("stale Seat front-door Attempt")
