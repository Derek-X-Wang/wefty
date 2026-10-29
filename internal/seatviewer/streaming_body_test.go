package seatviewer

import (
	"bytes"
	"context"
	"io"
	"net"
	"net/http"
	"net/http/cookiejar"
	"net/http/httptest"
	"net/http/httptrace"
	"net/http/httputil"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/fabric"
)

type responseFirstOwner struct {
	fabric.Fabric
	headers <-chan struct{}
	ctx     context.Context
}

func (o responseFirstOwner) Dial(ctx context.Context, network, address string) (net.Conn, error) {
	conn, err := (&net.Dialer{}).DialContext(ctx, network, address)
	if err != nil {
		return nil, err
	}
	return &responseFirstConn{Conn: conn, headers: o.headers, ctx: o.ctx}, nil
}

type responseFirstConn struct {
	net.Conn
	headers <-chan struct{}
	ctx     context.Context
}

func (c *responseFirstConn) Write(p []byte) (int, error) {
	n, err := c.Conn.Write(p)
	// The body is on the wire. Hold its Write return until the downstream has
	// started receiving the response, before Transport's final body EOF read.
	if err == nil && bytes.Equal(p, []byte("{}")) {
		select {
		case <-c.headers:
		case <-c.ctx.Done():
			return n, c.ctx.Err()
		}
	}
	return n, err
}

func TestRelayStreamsResponseWhileRequestWriterFinishes(t *testing.T) {
	for _, tc := range []struct {
		name, method string
		body         bool
	}{{"GET-body", "GET", true}, {"POST-body", "POST", true}, {"browser-GET", "GET", false}} {
		t.Run(tc.name, func(t *testing.T) {
			payload := bytes.Repeat([]byte("asset"), 1<<18)
			requestFinished := make(chan struct{})
			node := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_, _ = w.Write(payload[:32<<10])
				_ = http.NewResponseController(w).Flush()
				// Keep the response open until the request writer finishes. This
				// avoids Transport's unrelated idle-connection write-wait limit.
				select {
				case <-requestFinished:
					_, _ = w.Write(payload[32<<10:])
				case <-r.Context().Done():
				}
			}))
			defer node.Close()
			headers := make(chan struct{})
			var headersOnce sync.Once
			release := func() { headersOnce.Do(func() { close(headers) }) }
			defer release()
			ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
			defer cancel()
			target, _ := url.Parse(node.URL + "/v1/seats/seat_test/person/viewer")
			relay, err := Start(ctx, responseFirstOwner{headers: headers, ctx: ctx}, target)
			if err != nil {
				t.Fatal(err)
			}
			defer relay.Close()
			wrote := make(chan error, 1)
			rewrite := relay.proxy.Rewrite
			relay.proxy.Rewrite = func(p *httputil.ProxyRequest) {
				rewrite(p)
				p.Out = p.Out.WithContext(httptrace.WithClientTrace(p.Out.Context(), &httptrace.ClientTrace{WroteRequest: func(info httptrace.WroteRequestInfo) {
					wrote <- info.Err
					close(requestFinished)
				}}))
			}
			jar, _ := cookiejar.New(nil)
			client := &http.Client{Jar: jar, Timeout: 10 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
			response, err := client.Get(relay.URL)
			if err != nil {
				t.Fatal("bootstrap failed")
			}
			_, _ = io.Copy(io.Discard, response.Body)
			response.Body.Close()
			traceCtx := httptrace.WithClientTrace(ctx, &httptrace.ClientTrace{GotFirstResponseByte: release})
			var input io.Reader
			if tc.body {
				input = bytes.NewReader([]byte("{}"))
			}
			request, _ := http.NewRequestWithContext(traceCtx, tc.method, relay.origin+relay.prefix+target.Path, input)
			request.Header.Set("Origin", relay.origin)
			response, err = client.Do(request)
			if err != nil {
				t.Fatalf("stream response did not start (%T)", err)
			}
			body, readErr := io.ReadAll(response.Body)
			response.Body.Close()
			var writeErr error
			select {
			case writeErr = <-wrote:
			case <-ctx.Done():
				t.Fatal("upstream request writer did not finish")
			}
			if readErr != nil || writeErr != nil || !bytes.Equal(body, payload) {
				t.Fatalf("truncated response: got %d/%d bytes; response error=%v; request writer error=%v", len(body), len(payload), readErr, writeErr)
			}
		})
	}
}
