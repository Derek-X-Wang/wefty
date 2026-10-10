package l1

import (
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/Derek-X-Wang/wefty/contract"
)

// walkNodePages follows next_cursor through /v1/nodes until the terminal page,
// so the result is the whole filtered list rather than one adaptive page.
func walkNodePages(t *testing.T, h *integrationHarness, client *http.Client, query string) nodePageWire {
	t.Helper()
	combined := nodePageWire{Nodes: []Node{}}
	cursor := ""
	for pageNumber := 0; ; pageNumber++ {
		if pageNumber > 250 {
			t.Fatal("node paging did not terminate")
		}
		path := "/v1/nodes?" + query
		if cursor != "" {
			path += "&cursor=" + url.QueryEscape(cursor)
		}
		status, _, body := h.do(client, http.MethodGet, path, nil)
		if status != http.StatusOK {
			t.Fatalf("list nodes status = %d body=%s", status, body)
		}
		var page nodePageWire
		if err := json.Unmarshal(body, &page); err != nil {
			t.Fatal(err)
		}
		if page.Nodes == nil {
			t.Fatalf("null nodes: %s", body)
		}
		combined.Nodes = append(combined.Nodes, page.Nodes...)
		cursor = page.NextCursor
		if cursor == "" {
			return combined
		}
	}
}

// walkLogPages polls a job-log endpoint and follows the cursor until a poll
// returns no events, so the result is the whole log rather than one page.
func walkLogPages(t *testing.T, h *integrationHarness, client *http.Client, path string) LogPage {
	t.Helper()
	combined := LogPage{Events: []contract.LogEvent{}}
	cursor := ""
	for poll := 0; ; poll++ {
		if poll > 250 {
			t.Fatal("log paging did not terminate")
		}
		withCursor := path
		if cursor != "" {
			separator := "?"
			if strings.Contains(path, "?") {
				separator = "&"
			}
			withCursor += separator + "cursor=" + url.QueryEscape(cursor)
		}
		status, _, body := h.do(client, http.MethodGet, withCursor, nil)
		if status != http.StatusOK {
			t.Fatalf("get job logs status = %d body=%s", status, body)
		}
		var page LogPage
		if err := json.Unmarshal(body, &page); err != nil {
			t.Fatal(err)
		}
		combined.Events = append(combined.Events, page.Events...)
		combined.Truncation = page.Truncation
		if len(page.Events) == 0 {
			return combined
		}
		cursor = page.NextCursor
	}
}

// walkStoreJobLogs walks GetJobLogs from the beginning until an empty poll so
// the result spans every stored row without depending on one page's cutoff.
func walkStoreJobLogs(t *testing.T, h *integrationHarness, jobID string) LogPage {
	t.Helper()
	var combined LogPage
	var cursor string
	for poll := 0; ; poll++ {
		if poll > 250 {
			t.Fatal("store log paging did not terminate")
		}
		page, err := h.store.GetJobLogs(context.Background(), jobID, cursor, MaxLogPageLimit)
		if err != nil {
			t.Fatal(err)
		}
		combined.Events = append(combined.Events, page.Events...)
		combined.Truncation = page.Truncation
		combined.NextCursor = page.NextCursor
		if len(page.Events) == 0 {
			return combined
		}
		cursor = page.NextCursor
	}
}
