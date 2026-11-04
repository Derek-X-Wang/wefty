package agent

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/internal/screentiming"
	"github.com/Derek-X-Wang/wefty/l1"
)

type seatFrontDoorAuthentication struct {
	control interface {
		AuthenticateSeatScreen(context.Context, string, string) error
		AuthenticateSeatCopy(context.Context, string, string) error
	}
	policy interface {
		Revision() int64
		CheckAdministrator(fabric.Identity) (bool, int64)
	}
	logf func(string, ...any)
}

// One node listener carries all Seat features. Attempts register a feature mux
// and bind only the Seat IDs supplied by their fenced L1 wanted list.
type seatFrontDoor struct {
	authentication seatFrontDoorAuthentication
	mu             sync.RWMutex
	fabric         fabric.Fabric
	endpoint       string
	done           chan struct{}
	serveErr       error
	computers      map[string]*seatFrontDoorRegistration
	seats          map[string]*seatFrontDoorRegistration
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
func startSeatFrontDoor(ctx context.Context, f fabric.Fabric, node string, authentication seatFrontDoorAuthentication) (*seatFrontDoor, func(), error) {
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
	door.authentication = authentication
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
	authentication := seatFrontDoorAuthentication{control: a.session.client, policy: a.ComputerPolicy(), logf: a.log}
	door, closeDoor, err := startSeatFrontDoor(ctx, a.fabric, a.registration.NodeID, authentication)
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
			door, closeDoor, err = startSeatFrontDoor(ctx, a.fabric, a.registration.NodeID, authentication)
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
		d.logRefusal(seatDoorPathKind(r.Header.Get(l1.SeatCredentialHeader) == ""), fabric.Identity{}, contract.ErrorForbidden)
		writeComputerControlError(w, http.StatusForbidden, contract.APIError{Code: contract.ErrorForbidden, Message: "Seat viewer host is not allowed"}, nil)
		return
	}
	person := strings.HasPrefix(suffix, "/person/") && r.Header.Get(l1.SeatCredentialHeader) == ""
	var identity fabric.Identity
	if r.Header.Get(l1.SeatCredentialHeader) == "" && !person && !seatCopyRoute.MatchString(r.URL.Path) {
		d.logRefusal("ai_agent", identity, contract.ErrorUnauthorized)
		writeSeatScreenError(w, &ProtocolError{StatusCode: http.StatusUnauthorized, APIError: contract.APIError{Code: contract.ErrorUnauthorized, Message: "Seat credential is required"}})
		return
	}
	if d.fabric != nil {
		var err error
		identity, err = d.fabric.WhoIs(r.Context(), r.RemoteAddr)
		if err != nil {
			status := http.StatusServiceUnavailable
			message := "Fabric identity could not be verified"
			code := contract.ErrorUnavailable
			if errors.Is(err, fabric.ErrIdentityNotFound) {
				status = http.StatusUnauthorized
				message = "Fabric identity could not be authenticated"
				code = contract.ErrorUnauthorized
			}
			d.logRefusal(seatDoorPathKind(person), fabric.Identity{}, code)
			http.Error(w, message, status)
			return
		}
	}
	r = r.WithContext(context.WithValue(r.Context(), seatTransportLoggerKey{}, d.authentication.logf))
	d.mu.RLock()
	registration := d.seats[match[1]]
	var seatCtx context.Context
	if registration != nil {
		seatCtx = registration.seatContexts[match[1]]
	}
	d.mu.RUnlock()
	if registration == nil || registration.ctx.Err() != nil {
		d.serveUnbound(w, r, match[1], person, identity)
		return
	}
	timing("fabric_accepted")
	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()
	stop := context.AfterFunc(seatCtx, cancel)
	defer stop()
	ctx = context.WithValue(ctx, seatAuthorityContextKey{}, seatCtx)
	registration.handler.ServeHTTP(w, r.WithContext(ctx))
}

func seatDoorPathKind(person bool) string {
	if person {
		return "person"
	}
	return "ai_agent"
}

// An unbound ID has no Computer, Attempt or Seat log for durable evidence.
// Never include the requested Seat ID, credential or upstream error text here.
func (d *seatFrontDoor) logRefusal(path string, identity fabric.Identity, code contract.ErrorCode) {
	if d.authentication.logf != nil {
		d.authentication.logf("Seat front door refusal path=%s fabric=%q node=%q user=%q device=%q code=%s", path, identity.FabricID, identity.NodeID, identity.UserID, identity.DeviceID, code)
	}
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

// The context carries only the node logger, never request identifiers or secrets.
type seatTransportLoggerKey struct{}

func logSeatTransportFailure(ctx context.Context, stage string, err error) {
	logf, _ := ctx.Value(seatTransportLoggerKey{}).(func(string, ...any))
	var transport *url.Error
	if logf == nil || !errors.As(err, &transport) {
		return
	}
	// Classify typed causes instead of printing error text: even an inner error
	// can contain the Seat URL, credential, requested ID or another private path.
	cause := "transport failure"
	var errno syscall.Errno
	var network net.Error
	var dns *net.DNSError
	switch {
	case errors.Is(err, context.Canceled):
		cause = "canceled"
	case errors.Is(err, context.DeadlineExceeded):
		cause = "deadline exceeded"
	case errors.As(err, &errno):
		cause = errno.Error()
	case errors.As(err, &dns) && dns.IsNotFound:
		cause = "DNS name not found"
	case errors.As(err, &network) && network.Timeout():
		cause = "timeout"
	case errors.Is(err, io.EOF):
		cause = "EOF"
	case errors.Is(err, io.ErrUnexpectedEOF):
		cause = "unexpected EOF"
	default:
		cause = fmt.Sprintf("transport failure (%T)", transport.Err)
	}
	logf("Seat front door transport stage=%q cause=%q", stage, cause[:min(len(cause), 96)])
}
