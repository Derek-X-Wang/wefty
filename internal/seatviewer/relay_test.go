package seatviewer

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/cookiejar"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/internal/takeover"
)

type localOwner struct{ fabric.Fabric }

func (localOwner) Dial(ctx context.Context, network, address string) (net.Conn, error) {
	return (&net.Dialer{}).DialContext(ctx, network, address)
}

func TestRelayFailedUpgradeBodyLength(t *testing.T) {
	node := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", "4096")
		w.WriteHeader(403)
		io.WriteString(w, strings.Repeat("x", 4096))
	}))
	defer node.Close()
	target, _ := url.Parse(node.URL + "/v1/seats/seat_test/person/viewer")
	relay, err := Start(t.Context(), localOwner{}, target)
	if err != nil {
		t.Fatal(err)
	}
	defer relay.Close()
	jar, _ := cookiejar.New(nil)
	browser := &http.Client{Jar: jar, Timeout: 10 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	bootstrap, err := browser.Get(relay.URL)
	if err != nil {
		t.Fatal("bootstrap failed")
	}
	bootstrap.Body.Close()
	request, _ := http.NewRequestWithContext(t.Context(), "GET", relay.origin+relay.prefix+relay.base+"/viewer-session", nil)
	request.Header = http.Header{"Connection": []string{"Upgrade"}, "Upgrade": []string{"websocket"}, "Origin": []string{relay.origin}, "Sec-Websocket-Version": []string{"13"}, "Sec-Websocket-Key": []string{"MTIzNDU2Nzg5MDEyMzQ1Ng=="}}
	response, err := browser.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil || response.StatusCode != 403 || len(body) != 1024 || response.ContentLength != 1024 {
		t.Fatalf("rejected upgrade: status=%d length=%d/%d err=%v", response.StatusCode, len(body), response.ContentLength, err)
	}
}

func TestRelaySeatEndWinsDrain(t *testing.T) {
	target, _ := url.Parse("http://127.0.0.1:1/v1/seats/seat_test/person/viewer")
	relay, err := Start(t.Context(), localOwner{}, target)
	if err != nil {
		t.Fatal(err)
	}
	defer relay.Close()
	relay.mu.Lock()
	relay.viewed = true
	relay.mu.Unlock()
	relay.End(&takeover.ActionError{APIError: contract.APIError{Code: contract.ErrorTakeoverSessionEnded, Message: "Reconnect"}})
	relay.End(&takeover.ActionError{APIError: contract.APIError{Code: "seat_ended", Message: "This Seat has ended"}})
	select {
	case <-relay.Done():
	case <-time.After(DrainTimeout + 15*time.Second):
		t.Fatal("drain did not stop")
	}
	var terminal *takeover.ActionError
	if !errors.As(relay.Err(), &terminal) || terminal.APIError.Code != "seat_ended" {
		t.Fatal("session close replaced Seat end")
	}
}

func TestRelayCleanPathRefusesEscapes(t *testing.T) {
	for raw, want := range map[string]bool{
		"/p/v1/seats/s/person/viewer":            true,
		"/p/v1/seats/s/person/viewer/":           true,
		"/p/v1/seats/s/person/../../other":       false,
		"/p/v1/seats/s/person/./viewer":          false,
		"/p/v1/seats/s/person//viewer":           false,
		"/p/v1/seats/s/person/a%2F..%2F..%2Fx":   false,
		"/p/v1/seats/s/person/viewer/index.html": true,
	} {
		u, err := url.Parse("http://127.0.0.1:1" + raw)
		if err != nil {
			t.Fatal(err)
		}
		if got := cleanPath(u); got != want {
			t.Errorf("cleanPath(%q) = %v, want %v", raw, got, want)
		}
	}
}
