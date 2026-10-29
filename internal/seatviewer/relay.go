// Package seatviewer serves an owner's browser over the owner's Fabric connection.
package seatviewer

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/internal/takeover"
	"github.com/coder/websocket"
)

// Relay owns the loopback listener and all upstream connections, including upgrades.
type Relay struct {
	URL                        string
	origin, base, prefix       string
	secret, cookie, cookieName string

	target      *url.URL
	ctx         context.Context
	cancel      context.CancelFunc
	server      *http.Server
	transport   *http.Transport
	proxy       *httputil.ReverseProxy
	mu          sync.Mutex
	exchanged   bool
	viewed      bool
	connections map[*relayConn]struct{}
	reason      error
	once        sync.Once
	endOnce     sync.Once
	done        chan struct{}
}

// Start requires the already authorized viewer-target URL. #1336: browsers
// cannot send Fabric's identity preface; only this process dials as the owner.
func Start(ctx context.Context, owner fabric.Fabric, target *url.URL) (*Relay, error) {
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(ctx)
	relay := &Relay{ctx: ctx, cancel: cancel, base: strings.TrimSuffix(target.Path, "/viewer"), secret: rand.Text(), cookie: rand.Text(), cookieName: "wefty_viewer_" + rand.Text(), connections: map[*relayConn]struct{}{}, done: make(chan struct{})}
	relay.prefix = "/" + rand.Text()
	relay.target = target
	relay.origin = "http://" + listener.Addr().String()
	relay.URL = relay.origin + relay.prefix + "/?secret=" + relay.secret
	relay.transport = &http.Transport{DialContext: func(ctx context.Context, network, _ string) (net.Conn, error) {
		ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		stop := context.AfterFunc(relay.ctx, cancel)
		defer stop()
		conn, err := owner.Dial(ctx, network, target.Host)
		if err != nil {
			return nil, err
		}
		tracked := &relayConn{Conn: conn, relay: relay}
		relay.mu.Lock()
		if relay.ctx.Err() != nil {
			relay.mu.Unlock()
			conn.Close()
			return nil, relay.ctx.Err()
		}
		relay.connections[tracked] = struct{}{}
		relay.mu.Unlock()
		return tracked, nil
	}}
	relay.proxy = &httputil.ReverseProxy{
		Transport: relay.transport,
		ErrorLog:  log.New(io.Discard, "", 0),
		Rewrite: func(p *httputil.ProxyRequest) {
			p.Out.URL.Scheme, p.Out.URL.Host = target.Scheme, target.Host
			p.Out.URL.Path = strings.TrimPrefix(p.In.URL.Path, relay.prefix)
			p.Out.URL.RawPath = ""
			p.Out.Host = target.Host
			p.Out.Header.Del("Cookie")
			p.Out.Header.Del("X-Wefty-Seat-Credential")
			if p.In.Header.Get("Origin") != "" {
				p.Out.Header.Set("Origin", target.Scheme+"://"+target.Host)
			}
		},
		ErrorHandler: func(w http.ResponseWriter, _ *http.Request, _ error) {
			http.Error(w, "Seat front door is unavailable", http.StatusBadGateway)
		},
		ModifyResponse: func(response *http.Response) error {
			trace := response.Request.Context().Value(traceKey{}).(*responseTrace)
			if location := response.Header.Get("Location"); strings.HasPrefix(location, relay.base+"/") {
				response.Header.Set("Location", relay.prefix+location)
			}
			if response.StatusCode == http.StatusGone {
				response.Body = &terminalBody{ReadCloser: response.Body, trace: trace}
			}
			return nil
		},
	}
	relay.server = &http.Server{Handler: relay, ReadHeaderTimeout: 5 * time.Second, ErrorLog: log.New(io.Discard, "", 0)}
	go func() {
		if err := relay.server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			relay.Stop(errors.New("Seat viewer relay stopped serving"))
		}
	}()
	go func() {
		select {
		case <-ctx.Done():
			relay.Stop(errors.New("Seat viewer stopped"))
		case <-relay.done:
		}
	}()
	return relay, nil
}

func (r *Relay) Done() <-chan struct{} { return r.done }
func (r *Relay) Err() error            { r.mu.Lock(); defer r.mu.Unlock(); return r.reason }
func (r *Relay) Close()                { r.Stop(errors.New("Seat viewer relay stopped")) }

// End leaves one status-poll interval for the page to read the door's typed
// ended/cap reason before the local listener disappears (#1336).
func (r *Relay) End(reason error) {
	r.recordReason(reason)
	r.endOnce.Do(func() {
		r.mu.Lock()
		viewed := r.viewed
		r.mu.Unlock()
		if !viewed {
			r.Stop(reason)
			return
		}
		go func() {
			timer := time.NewTimer(DrainTimeout) // Three-second page poll plus a round-trip margin.
			defer timer.Stop()
			select {
			case <-r.ctx.Done():
			case <-timer.C:
				r.Stop(nil)
			}
		}()
	})
}

// Stop closes upgraded streams too: http.Server.Close alone leaves them alive.
func (r *Relay) Stop(reason error) {
	r.recordReason(reason)
	r.once.Do(func() {
		r.mu.Lock()
		r.cancel()
		connections := make([]*relayConn, 0, len(r.connections))
		for conn := range r.connections {
			connections = append(connections, conn)
		}
		r.mu.Unlock()
		r.server.Close()
		r.transport.CloseIdleConnections()
		for _, conn := range connections {
			conn.Close()
		}
		close(r.done)
	})
}

func equalSecret(a, b string) bool { return subtle.ConstantTimeCompare([]byte(a), []byte(b)) == 1 }

// cleanPath refuses dot segments and escaped path bytes, so the forwarded path
// stays under the prefix the relay checked.
func cleanPath(u *url.URL) bool {
	if u.RawPath != "" {
		return false
	}
	clean := path.Clean(u.Path)
	if strings.HasSuffix(u.Path, "/") && clean != "/" {
		clean += "/"
	}
	return clean == u.Path
}

func (r *Relay) ServeHTTP(w http.ResponseWriter, request *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Referrer-Policy", "no-referrer")
	if request.Host != strings.TrimPrefix(r.origin, "http://") {
		http.Error(w, "Viewer host is not allowed", http.StatusForbidden)
		return
	}
	if !strings.HasPrefix(request.URL.Path, r.prefix+"/") || !cleanPath(request.URL) {
		http.Error(w, "Viewer path is not allowed", http.StatusForbidden)
		return
	}
	if request.Method == http.MethodGet && request.URL.Path == r.prefix+"/" {
		query, err := url.ParseQuery(request.URL.RawQuery)
		r.mu.Lock()
		accepted := err == nil && len(query) == 1 && len(query["secret"]) == 1 && equalSecret(query.Get("secret"), r.secret) && !r.exchanged
		if accepted {
			r.exchanged = true
		}
		r.mu.Unlock()
		if !accepted {
			http.Error(w, "Viewer secret is required", http.StatusForbidden)
			return
		}
		http.SetCookie(w, &http.Cookie{Name: r.cookieName, Value: r.cookie, Path: r.prefix + "/", HttpOnly: true, SameSite: http.SameSiteStrictMode})
		http.Redirect(w, request, r.prefix+r.base+"/viewer", http.StatusSeeOther)
		return
	}
	cookie, err := request.Cookie(r.cookieName)
	if err != nil || !equalSecret(cookie.Value, r.cookie) {
		http.Error(w, "Viewer cookie is required", http.StatusForbidden)
		return
	}
	if !strings.HasPrefix(request.URL.Path, r.prefix+r.base+"/") {
		http.NotFound(w, request)
		return
	}
	upgrade := request.Header.Get("Upgrade") != "" || headerHasToken(request.Header, "Connection", "upgrade") || strings.HasSuffix(request.URL.Path, "/viewer-session") || strings.HasSuffix(request.URL.Path, "/websockify")
	if (upgrade || request.Method != http.MethodGet && request.Method != http.MethodHead) && request.Header.Get("Origin") != r.origin {
		http.Error(w, "Viewer origin is not allowed", http.StatusForbidden)
		return
	}
	if upgrade && !validBrowserUpgrade(w, request) {
		return
	}
	trace := &responseTrace{}
	request = request.WithContext(context.WithValue(request.Context(), traceKey{}, trace))
	upgraded := false
	if upgrade {
		upgraded = r.serveSocket(w, request, trace)
	} else {
		r.proxy.ServeHTTP(w, request)
	}
	reason := trace.reason
	if reason != nil {
		if upgraded {
			r.End(reason)
		} else {
			_ = http.NewResponseController(w).Flush()
			r.Stop(reason)
		}
	}
}

type relayConn struct {
	net.Conn
	relay *Relay
}

func (c *relayConn) Close() error {
	err := c.Conn.Close()
	c.relay.mu.Lock()
	delete(c.relay.connections, c)
	c.relay.mu.Unlock()
	return err
}

type traceKey struct{}
type responseTrace struct{ reason error }

func (t *responseTrace) end(failure contract.ComputerControlErrorResponse) {
	t.reason = &takeover.ActionError{APIError: failure.Error, Receipt: failure.Receipt}
}

type terminalBody struct {
	io.ReadCloser
	trace *responseTrace
	body  bytes.Buffer
}

func (b *terminalBody) Read(p []byte) (int, error) {
	n, err := b.ReadCloser.Read(p)
	if b.body.Len()+n <= 64<<10 {
		b.body.Write(p[:n])
	}
	return n, err
}
func (b *terminalBody) Close() error {
	var reply contract.ComputerControlErrorResponse
	if json.Unmarshal(b.body.Bytes(), &reply) == nil && (reply.Error.Code == "seat_ended" || reply.Error.Code == contract.ErrorTakeoverSessionEnded) {
		b.trace.end(reply)
	}
	return b.ReadCloser.Close()
}

// Bridge messages without buffering RFB payloads. Reading close frames here is
// necessary: a raw HTTP upgrade proxy can wait for a TCP half-close after the
// door has already ended its WebSocket session.
func (r *Relay) serveSocket(w http.ResponseWriter, request *http.Request, trace *responseTrace) (upgraded bool) {
	ctx, cancel := context.WithCancel(r.ctx)
	defer cancel()
	stop := context.AfterFunc(request.Context(), cancel)
	defer stop()
	// Scheme and host are the door's; the path passed cleanPath under this Seat's prefix.
	target := url.URL{Scheme: r.target.Scheme, Host: r.target.Host, Path: strings.TrimPrefix(request.URL.Path, r.prefix), RawQuery: request.URL.RawQuery}
	protocols := strings.Split(request.Header.Get("Sec-WebSocket-Protocol"), ",")
	for i := range protocols {
		protocols[i] = strings.TrimSpace(protocols[i])
	}
	upstream, response, err := websocket.Dial(ctx, target.String(), &websocket.DialOptions{
		HTTPClient:   &http.Client{Transport: r.transport, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }},
		HTTPHeader:   http.Header{"Origin": []string{r.target.Scheme + "://" + r.target.Host}},
		Subprotocols: protocols,
	})
	if err != nil {
		if response == nil {
			http.Error(w, "Seat front door is unavailable", http.StatusBadGateway)
			return
		}
		defer response.Body.Close()
		for key, values := range response.Header {
			if key != "Content-Length" && key != "Transfer-Encoding" {
				w.Header()[key] = values
			}
		}
		data, _ := io.ReadAll(response.Body) // Dial retains at most 1024 bytes after a rejected upgrade.
		w.Header().Set("Content-Length", strconv.Itoa(len(data)))
		w.WriteHeader(response.StatusCode)
		body := &terminalBody{ReadCloser: io.NopCloser(bytes.NewReader(data)), trace: trace}
		_, _ = io.Copy(w, body)
		body.Close()
		return
	}
	defer upstream.CloseNow()
	downstream, err := websocket.Accept(w, request, &websocket.AcceptOptions{Subprotocols: []string{upstream.Subprotocol()}})
	if err != nil {
		return
	}
	defer downstream.CloseNow()
	upgraded = true
	r.mu.Lock()
	r.viewed = true
	r.mu.Unlock()
	upstream.SetReadLimit(-1)
	downstream.SetReadLimit(-1)
	type result struct {
		fromDoor bool
		err      error
	}
	finished := make(chan result, 2)
	copyMessages := func(dst, src *websocket.Conn, fromDoor bool) {
		for {
			kind, reader, err := src.Reader(ctx)
			if err != nil {
				finished <- result{fromDoor, err}
				return
			}
			writer, err := dst.Writer(ctx, kind)
			if err == nil {
				_, err = io.Copy(writer, reader)
				closeErr := writer.Close()
				if err == nil {
					err = closeErr
				}
			}
			if err != nil {
				finished <- result{false, err}
				return
			}
		}
	}
	go copyMessages(downstream, upstream, true)
	go copyMessages(upstream, downstream, false)
	first := <-finished
	if first.fromDoor {
		trace.end(contract.ComputerControlErrorResponse{Error: contract.APIError{Code: contract.ErrorTakeoverSessionEnded, Message: "Seat viewer session ended; reopen the viewer to reconnect"}})
		var closeError websocket.CloseError
		if errors.As(first.err, &closeError) && closeError.Code != websocket.StatusNoStatusRcvd && closeError.Code != websocket.StatusAbnormalClosure {
			_ = downstream.Close(closeError.Code, closeError.Reason)
		}
	}
	cancel()
	downstream.CloseNow()
	upstream.CloseNow()
	<-finished
	return
}

// DrainTimeout permits one page status poll and two seconds for the round trip.
const DrainTimeout = 5 * time.Second

func seatEnded(err error) bool {
	var action *takeover.ActionError
	return errors.As(err, &action) && action.APIError.Code == "seat_ended"
}

// A later Seat end supersedes a session close already waiting for its drain.
func (r *Relay) recordReason(reason error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	priority := func(err error) int {
		if seatEnded(err) {
			return 2
		}
		var action *takeover.ActionError
		if errors.As(err, &action) {
			return 1
		}
		return 0
	}
	var action *takeover.ActionError
	if reason != nil && (r.reason == nil || priority(reason) > priority(r.reason) || priority(reason) == 1 && priority(r.reason) == 1 && errors.As(reason, &action) && action.Receipt != nil) {
		r.reason = reason
	}
}

// The pinned websocket.Accept checks these RFC 6455 fields only after dialing
// would have created a person session. Validate them at the browser boundary.
func validBrowserUpgrade(w http.ResponseWriter, r *http.Request) bool {
	status := 0
	switch {
	case !r.ProtoAtLeast(1, 1), !headerHasToken(r.Header, "Connection", "upgrade"), !headerHasToken(r.Header, "Upgrade", "websocket"):
		status = http.StatusUpgradeRequired
	case r.Method != http.MethodGet:
		status = http.StatusMethodNotAllowed
	case r.Header.Get("Sec-WebSocket-Version") != "13":
		status = http.StatusBadRequest
	default:
		keys := r.Header.Values("Sec-WebSocket-Key")
		if len(keys) != 1 {
			status = http.StatusBadRequest
		} else if data, err := base64.StdEncoding.DecodeString(strings.TrimSpace(keys[0])); err != nil || len(data) != 16 {
			status = http.StatusBadRequest
		}
	}
	if status != 0 {
		http.Error(w, "Invalid browser WebSocket handshake", status)
		return false
	}
	return true
}

func headerHasToken(h http.Header, name, token string) bool {
	for _, value := range h.Values(name) {
		for _, part := range strings.Split(value, ",") {
			if strings.EqualFold(strings.TrimSpace(part), token) {
				return true
			}
		}
	}
	return false
}
