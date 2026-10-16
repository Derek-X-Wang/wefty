package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/l1"
)

func TestExecQueuedLoadReason(t *testing.T) {
	previous := followWaitingNoticeAfter
	followWaitingNoticeAfter = 0
	t.Cleanup(func() { followWaitingNoticeAfter = previous })
	for _, tc := range []struct {
		name        string
		load, limit float64
		loadErr     error
		occupied    int
		busy        bool
	}{
		{name: "at limit", load: 18, limit: 18, occupied: 1, busy: true},
		{name: "above limit", load: 18.96, limit: 18, occupied: 1, busy: true},
		{name: "below limit", load: 17, limit: 18, occupied: 1},
		{name: "unreadable", limit: 18, occupied: 1, loadErr: errors.New("unreadable")},
		{name: "first run", load: 99, limit: 18},
		{name: "disabled", load: 99, occupied: 1},
	} {
		for _, jsonOutput := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/json=%v", tc.name, jsonOutput), func(t *testing.T) {
				h := newStatusHarness(t)
				node := readyNode("local")
				node.AuthoritativeTags = []string{"wefty:node:local"}
				node.OneshotOccupancy = tc.occupied
				h.nodes = l1.NodeList{Nodes: []l1.Node{node}}
				clients := h.clients()
				clients.readLoad = func() (float64, error) { return tc.load, tc.loadErr }
				clients.oneshotLoadLimit = &tc.limit
				clients.l3.client.Transport = storageRoundTripFunc(func(r *http.Request) (*http.Response, error) {
					body := `{}`
					if r.URL.Path == "/v1/runs/waiting" {
						body = `{"run_id":"waiting","status":"queued"}`
					}
					return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body))}, nil
				})
				stopped := errors.New("stop observation")
				clients.wait = func(context.Context, time.Duration) error { return stopped }
				var out, stderr bytes.Buffer
				_, err := observeExecRunOutput(t.Context(), clients, "waiting", "local", jsonOutput, &out, &stderr)
				if !errors.Is(err, stopped) {
					t.Fatal(err)
				}
				reason := "waiting for a node to start it"
				if tc.busy {
					reason = fmt.Sprintf("this machine is busy (load %.2f, limit %.2f), so it starts when the load drops", tc.load, tc.limit)
				}
				want := "wefty: run waiting is queued; " + reason + "\n"
				if stderr.String() != want {
					t.Errorf("stderr=%q want=%q", stderr.String(), want)
				}
				if jsonOutput {
					var event map[string]any
					if e := json.Unmarshal(out.Bytes(), &event); e != nil || event["type"] != "heartbeat" || event["reason"] != reason || event["text"] != want {
						t.Errorf("JSON=%s decode=%v", &out, e)
					}
				} else if out.Len() != 0 {
					t.Errorf("stdout=%s", &out)
				}
			})
		}
	}
}

func TestExecWaitingReasonChangesAreThrottled(t *testing.T) {
	h := newStatusHarness(t)
	node := readyNode("local")
	node.AuthoritativeTags = []string{"wefty:node:local"}
	node.OneshotOccupancy = 1
	h.nodes = l1.NodeList{Nodes: []l1.Node{node}}
	clients := h.clients()
	load, limit := 19.0, 18.0
	reads := 0
	clients.readLoad = func() (float64, error) { reads++; return load, nil }
	clients.oneshotLoadLimit = &limit
	waiting := execWaitingAnnouncer{nodeID: "local"}
	var out bytes.Buffer
	start := time.Date(2026, 10, 16, 0, 0, 0, 0, time.UTC)
	for _, step := range []struct {
		seconds      int
		load         float64
		lines, reads int
	}{
		{0, 19, 1, 1},
		{30, 17, 1, 1}, // No early repeat even though the reason changed.
		{60, 17, 2, 2},
		{120, 17, 2, 3}, // A minute passing alone never repeats a line.
		{180, 19, 3, 4},
		{240, 21, 3, 5}, // A changed load is still the same busy reason.
		{300, 17, 4, 6},
	} {
		load = step.load
		if err := waiting.announce(t.Context(), clients, io.Discard, &out, "waiting", "queued", false, start.Add(time.Duration(step.seconds)*time.Second)); err != nil {
			t.Fatal(err)
		}
		if strings.Count(out.String(), "\n") != step.lines || reads != step.reads {
			t.Fatalf("at %ds lines=%d reads=%d output=%s", step.seconds, strings.Count(out.String(), "\n"), reads, &out)
		}
	}
}
