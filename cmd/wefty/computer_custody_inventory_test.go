package main

import (
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
)

func TestComputerCLICustodyWalksEveryPage(t *testing.T) {
	calls := 0
	clients := &apiClients{l1: &apiClient{name: "L1", client: &http.Client{Transport: storageRoundTripFunc(func(r *http.Request) (*http.Response, error) {
		calls++
		expected := ""
		next := "page/1"
		if calls == 2 {
			expected = "page/1"
			next = "page+2"
		}
		if calls == 3 {
			expected = "page+2"
			next = ""
		}
		if calls > 3 || r.URL.Query().Get("cursor") != expected {
			t.Fatalf("cursor=%s calls=%d", r.URL.RawQuery, calls)
		}
		body := fmt.Sprintf(`{"custody_exports":[{"export_id":"event-%d","status":"available"}],"next_cursor":%q}`, calls, next)
		return &http.Response{StatusCode: 200, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(body))}, nil
	})}}}
	exports, err := clients.listComputerCustodyExports(t.Context(), "computer")
	if err != nil || calls != 3 || len(exports) != 3 || exports[2].ExportID != "event-3" {
		t.Fatalf("inventory=%+v calls=%d err=%v", exports, calls, err)
	}
}
